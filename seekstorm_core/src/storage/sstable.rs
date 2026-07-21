//! SSTable（Sorted-String Table）：磁盘上的有序键值表。
//!
//! 文件布局（大端序）：
//! ```text
//! [Data block 0][Data block 1]...   ← 每个 block ~4 KiB，含若干 (key, value) 条目
//! [Index block]                      ← 元数据 + 每 block 的 (first_key, offset)
//! [Footer: 16 bytes]                 ← index_offset:u64 + magic "SKST_END"
//! ```
//!
//! Index block 布局：
//! ```text
//! magic: "SKST" (4)
//! version: u32 (4)
//! sstable_id: u32 (4)
//! entry_count: u32 (4)
//! num_blocks: u32 (4)
//! min_key: 21 bytes
//! max_key: 21 bytes
//! for each block: first_key(21) + block_offset:u64(8) = 29 bytes
//! ```
//!
//! 数据块条目布局：`key_bytes(21) + value_bytes(变长)`，其中 value 编码见 [`LsmValue`]。

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;

use super::io_backend::IoBackend;
use super::lsm::{LsmKey, LsmValue};

/// 数据块目标大小（字节）。
const BLOCK_SIZE: usize = 4096;

/// 文件魔数。
const MAGIC: &[u8; 4] = b"SKST";
const FOOTER_MAGIC: &[u8; 8] = b"SKST_END";
const VERSION: u32 = 1;

const FOOTER_LEN: usize = 16;
const INDEX_HEADER_LEN: usize = 4 + 4 + 4 + 4 + 4 + LsmKey::ENCODED_LEN + LsmKey::ENCODED_LEN;
const BLOCK_INDEX_ENTRY_LEN: usize = LsmKey::ENCODED_LEN + 8;

/// SSTable 句柄。打开时加载 index 到内存。
pub struct SSTable {
    pub id: u32,
    pub path: PathBuf,
    pub index: Vec<(LsmKey, u64)>,
    pub entry_count: u32,
    pub min_key: LsmKey,
    pub max_key: LsmKey,
    pub index_offset: u64,
    io: Arc<dyn IoBackend>,
}

impl SSTable {
    /// 打开现有 SSTable，读取 index 与元数据。
    pub async fn open(path: PathBuf, io: Arc<dyn IoBackend>) -> Result<Self> {
        let file_size = io.file_size(&path).await?;
        if file_size < FOOTER_LEN as u64 {
            anyhow::bail!("SSTable {:?} too small: {} bytes", path, file_size);
        }
        // Read footer.
        let mut footer = [0u8; FOOTER_LEN];
        let _ = io
            .read_at(&path, file_size - FOOTER_LEN as u64, &mut footer)
            .await?;
        let index_offset = u64::from_be_bytes(footer[0..8].try_into().unwrap());
        if &footer[8..16] != FOOTER_MAGIC {
            anyhow::bail!("SSTable {:?} footer magic mismatch", path);
        }
        // Read index block.
        let index_len = file_size - FOOTER_LEN as u64 - index_offset;
        let mut index_buf = vec![0u8; index_len as usize];
        let _ = io.read_at(&path, index_offset, &mut index_buf).await?;
        if index_buf.len() < INDEX_HEADER_LEN {
            anyhow::bail!("SSTable {:?} index too short", path);
        }
        if &index_buf[0..4] != MAGIC {
            anyhow::bail!("SSTable {:?} index magic mismatch", path);
        }
        let version = u32::from_be_bytes(index_buf[4..8].try_into().unwrap());
        if version != VERSION {
            anyhow::bail!("SSTable {:?} unsupported version {}", path, version);
        }
        let id = u32::from_be_bytes(index_buf[8..12].try_into().unwrap());
        let entry_count = u32::from_be_bytes(index_buf[12..16].try_into().unwrap());
        let num_blocks = u32::from_be_bytes(index_buf[16..20].try_into().unwrap());
        let min_key = LsmKey::decode(&index_buf[20..20 + LsmKey::ENCODED_LEN])?;
        let max_key = LsmKey::decode(
            &index_buf[20 + LsmKey::ENCODED_LEN..20 + 2 * LsmKey::ENCODED_LEN],
        )?;
        let mut index = Vec::with_capacity(num_blocks as usize);
        let mut p = INDEX_HEADER_LEN;
        for _ in 0..num_blocks {
            if p + BLOCK_INDEX_ENTRY_LEN > index_buf.len() {
                anyhow::bail!("SSTable {:?} index entry truncated", path);
            }
            let key = LsmKey::decode(&index_buf[p..p + LsmKey::ENCODED_LEN])?;
            let offset = u64::from_be_bytes(
                index_buf[p + LsmKey::ENCODED_LEN..p + BLOCK_INDEX_ENTRY_LEN]
                    .try_into()
                    .unwrap(),
            );
            index.push((key, offset));
            p += BLOCK_INDEX_ENTRY_LEN;
        }
        Ok(Self {
            id,
            path,
            index,
            entry_count,
            min_key,
            max_key,
            index_offset,
            io,
        })
    }

    /// 查找 `(ns, partition, doc_id)` 的最新版本。查询 key 的 `lsn` 字段被忽略。
    /// 返回 `Ok(None)` 表示不存在；返回 `Ok(Some(Tombstone))` 表示已被删除。
    pub async fn get(&self, key: &LsmKey) -> Result<Option<LsmValue>> {
        let lo = LsmKey {
            namespace: key.namespace,
            partition_or_segment: key.partition_or_segment,
            doc_id: key.doc_id,
            lsn: 0,
        };
        let hi = LsmKey {
            namespace: key.namespace,
            partition_or_segment: key.partition_or_segment,
            doc_id: key.doc_id,
            lsn: u64::MAX,
        };
        if self.index.is_empty() || self.min_key > hi || self.max_key < lo {
            return Ok(None);
        }
        // 找到 first_key <= lo 的最大 block。
        let start = match self.index.binary_search_by(|(first_key, _)| first_key.cmp(&lo)) {
            Ok(i) => i,
            Err(i) => i.saturating_sub(1),
        };
        let mut best: Option<LsmValue> = None;
        let mut i = start;
        while i < self.index.len() {
            let (first_key, offset) = &self.index[i];
            if *first_key > hi {
                break;
            }
            let block_end = if i + 1 < self.index.len() {
                self.index[i + 1].1
            } else {
                self.index_offset
            };
            let block_len = block_end - offset;
            let mut buf = vec![0u8; block_len as usize];
            self.io.read_at(&self.path, *offset, &mut buf).await?;
            let mut pos = 0;
            while pos < buf.len() {
                if pos + LsmKey::ENCODED_LEN > buf.len() {
                    break;
                }
                let entry_key = LsmKey::decode(&buf[pos..pos + LsmKey::ENCODED_LEN])?;
                pos += LsmKey::ENCODED_LEN;
                let (value, consumed) = LsmValue::decode_with_len(&buf[pos..])?;
                pos += consumed;
                if entry_key.namespace == key.namespace
                    && entry_key.partition_or_segment == key.partition_or_segment
                    && entry_key.doc_id == key.doc_id
                {
                    best = Some(value);
                }
            }
            i += 1;
        }
        Ok(best)
    }

    /// 范围扫描 `(ns, partition)` 下所有条目，按 doc_id 去重保留最高 lsn 版本。
    pub async fn scan_prefix(&self, ns: u8, partition: u32) -> Result<Vec<(LsmKey, LsmValue)>> {
        let lo = LsmKey {
            namespace: ns,
            partition_or_segment: partition,
            doc_id: 0,
            lsn: 0,
        };
        let hi = LsmKey {
            namespace: ns,
            partition_or_segment: partition,
            doc_id: u64::MAX,
            lsn: u64::MAX,
        };
        if self.index.is_empty() || self.min_key > hi || self.max_key < lo {
            return Ok(Vec::new());
        }
        let start = match self.index.binary_search_by(|(first_key, _)| first_key.cmp(&lo)) {
            Ok(i) => i,
            Err(i) => i.saturating_sub(1),
        };
        let mut results: std::collections::HashMap<u64, (LsmKey, LsmValue)> =
            std::collections::HashMap::new();
        let mut i = start;
        while i < self.index.len() {
            let (first_key, offset) = &self.index[i];
            if *first_key > hi {
                break;
            }
            let block_end = if i + 1 < self.index.len() {
                self.index[i + 1].1
            } else {
                self.index_offset
            };
            let block_len = block_end - offset;
            let mut buf = vec![0u8; block_len as usize];
            self.io.read_at(&self.path, *offset, &mut buf).await?;
            let mut pos = 0;
            while pos < buf.len() {
                if pos + LsmKey::ENCODED_LEN > buf.len() {
                    break;
                }
                let entry_key = LsmKey::decode(&buf[pos..pos + LsmKey::ENCODED_LEN])?;
                pos += LsmKey::ENCODED_LEN;
                let (value, consumed) = LsmValue::decode_with_len(&buf[pos..])?;
                pos += consumed;
                if entry_key.namespace == ns && entry_key.partition_or_segment == partition {
                    results.insert(entry_key.doc_id, (entry_key, value));
                }
            }
            i += 1;
        }
        let mut v: Vec<_> = results.into_values().collect();
        v.sort_by(|(a, _), (b, _)| a.cmp(b));
        Ok(v)
    }

    /// 删除 SSTable 文件。
    pub async fn delete_file(&self) -> Result<()> {
        self.io.delete_file(&self.path).await
    }

    /// 返回条目数。
    pub fn entry_count(&self) -> u32 {
        self.entry_count
    }

    /// 全量扫描所有条目（不做去重），按 key 升序返回。compaction 用。
    pub async fn iter_all_raw(&self) -> Result<Vec<(LsmKey, LsmValue)>> {
        let mut results = Vec::new();
        if self.index.is_empty() {
            return Ok(results);
        }
        for i in 0..self.index.len() {
            let (_first_key, offset) = &self.index[i];
            let block_end = if i + 1 < self.index.len() {
                self.index[i + 1].1
            } else {
                self.index_offset
            };
            let block_len = block_end - offset;
            let mut buf = vec![0u8; block_len as usize];
            self.io.read_at(&self.path, *offset, &mut buf).await?;
            let mut pos = 0;
            while pos < buf.len() {
                if pos + LsmKey::ENCODED_LEN > buf.len() {
                    break;
                }
                let entry_key = LsmKey::decode(&buf[pos..pos + LsmKey::ENCODED_LEN])?;
                pos += LsmKey::ENCODED_LEN;
                let (value, consumed) = LsmValue::decode_with_len(&buf[pos..])?;
                pos += consumed;
                results.push((entry_key, value));
            }
        }
        Ok(results)
    }
}

/// SSTable 写入器。按 key 升序写入条目，达到 BLOCK_SIZE 触发 flush。
/// 调用方必须保证 key 升序（否则 `scan_prefix` 与 `get` 的二分查找会失效）。
pub struct SStableWriter {
    id: u32,
    path: PathBuf,
    io: Arc<dyn IoBackend>,
    current_block: Vec<u8>,
    current_block_first_key: Option<LsmKey>,
    block_index: Vec<(LsmKey, u64)>,
    current_offset: u64,
    entry_count: u32,
    min_key: Option<LsmKey>,
    max_key: Option<LsmKey>,
}

impl SStableWriter {
    pub fn new(id: u32, path: PathBuf, io: Arc<dyn IoBackend>) -> Self {
        Self {
            id,
            path,
            io,
            current_block: Vec::new(),
            current_block_first_key: None,
            block_index: Vec::new(),
            current_offset: 0,
            entry_count: 0,
            min_key: None,
            max_key: None,
        }
    }

    /// 写入一条 (key, value)。key 必须严格大于已写入的所有 key。
    pub async fn write(&mut self, key: LsmKey, value: LsmValue) -> Result<()> {
        if self.min_key.is_none() {
            self.min_key = Some(key.clone());
        }
        self.max_key = Some(key.clone());
        if self.current_block_first_key.is_none() {
            self.current_block_first_key = Some(key.clone());
        }
        let key_bytes = key.encode();
        let value_bytes = value.encode();
        self.current_block.extend_from_slice(&key_bytes);
        self.current_block.extend_from_slice(&value_bytes);
        self.entry_count += 1;
        if self.current_block.len() >= BLOCK_SIZE {
            self.flush_block().await?;
        }
        Ok(())
    }

    /// 已写入条目数。
    pub fn entry_count(&self) -> u32 {
        self.entry_count
    }

    async fn flush_block(&mut self) -> Result<()> {
        if self.current_block.is_empty() {
            return Ok(());
        }
        let first_key = self
            .current_block_first_key
            .take()
            .ok_or_else(|| anyhow::anyhow!("flush_block: first_key missing"))?;
        let offset = self.current_offset;
        self.io
            .write_at(&self.path, offset, &self.current_block)
            .await?;
        self.block_index.push((first_key, offset));
        self.current_offset += self.current_block.len() as u64;
        self.current_block.clear();
        Ok(())
    }

    /// 完成写入：flush 剩余 block，写 index block 与 footer，返回打开的 SSTable 句柄。
    pub async fn finish(mut self) -> Result<SSTable> {
        self.flush_block().await?;
        let min_key = self
            .min_key
            .clone()
            .ok_or_else(|| anyhow::anyhow!("finish: empty SSTable (min_key missing)"))?;
        let max_key = self
            .max_key
            .clone()
            .ok_or_else(|| anyhow::anyhow!("finish: empty SSTable (max_key missing)"))?;

        let index_offset = self.current_offset;
        let mut index_buf = Vec::with_capacity(INDEX_HEADER_LEN + BLOCK_INDEX_ENTRY_LEN * self.block_index.len());
        index_buf.extend_from_slice(MAGIC);
        index_buf.extend_from_slice(&VERSION.to_be_bytes());
        index_buf.extend_from_slice(&self.id.to_be_bytes());
        index_buf.extend_from_slice(&self.entry_count.to_be_bytes());
        index_buf.extend_from_slice(&(self.block_index.len() as u32).to_be_bytes());
        index_buf.extend_from_slice(&min_key.encode());
        index_buf.extend_from_slice(&max_key.encode());
        for (first_key, offset) in &self.block_index {
            index_buf.extend_from_slice(&first_key.encode());
            index_buf.extend_from_slice(&offset.to_be_bytes());
        }
        self.io
            .write_at(&self.path, index_offset, &index_buf)
            .await?;

        let footer_offset = index_offset + index_buf.len() as u64;
        let mut footer = Vec::with_capacity(FOOTER_LEN);
        footer.extend_from_slice(&index_offset.to_be_bytes());
        footer.extend_from_slice(FOOTER_MAGIC);
        self.io
            .write_at(&self.path, footer_offset, &footer)
            .await?;
        self.io.fsync(&self.path).await?;

        SSTable::open(self.path, self.io).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::io_backend::{select_backend, IoBackendKind};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_sstable_write_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sstable-0001.bin");
        let io: Arc<dyn IoBackend> = select_backend(IoBackendKind::AsyncFs);

        let mut writer = SStableWriter::new(1, path.clone(), io.clone());
        for i in 0..1000u64 {
            writer
                .write(LsmKey::doc(i), LsmValue::Data(format!("value-{}", i).into_bytes()))
                .await
                .unwrap();
        }
        let sst = writer.finish().await.unwrap();
        assert_eq!(sst.entry_count, 1000);

        for i in 0..1000u64 {
            let v = sst.get(&LsmKey::doc(i)).await.unwrap().unwrap();
            assert!(matches!(v, LsmValue::Data(ref b) if b == &format!("value-{}", i).into_bytes()),
                "doc {} mismatch", i);
        }
        assert!(sst.get(&LsmKey::doc(1001)).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_sstable_scan_prefix() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sstable-scan.bin");
        let io: Arc<dyn IoBackend> = select_backend(IoBackendKind::AsyncFs);

        let mut writer = SStableWriter::new(2, path.clone(), io.clone());
        // 写入混合 namespace（按 key 升序）。
        writer.write(LsmKey::doc(1), LsmValue::Data(b"d1".to_vec())).await.unwrap();
        writer.write(LsmKey::doc(2), LsmValue::Data(b"d2".to_vec())).await.unwrap();
        writer.write(LsmKey::vec(5, 10), LsmValue::Data(b"v5-10".to_vec())).await.unwrap();
        writer.write(LsmKey::vec(5, 11), LsmValue::Data(b"v5-11".to_vec())).await.unwrap();
        writer.write(LsmKey::vec(7, 20), LsmValue::Data(b"v7-20".to_vec())).await.unwrap();
        let sst = writer.finish().await.unwrap();

        let r = sst.scan_prefix(crate::NS_VEC, 5).await.unwrap();
        assert_eq!(r.len(), 2);
        let r = sst.scan_prefix(crate::NS_VEC, 7).await.unwrap();
        assert_eq!(r.len(), 1);
        let r = sst.scan_prefix(crate::NS_DOC, 0).await.unwrap();
        assert_eq!(r.len(), 2);
        let r = sst.scan_prefix(crate::NS_VEC, 999).await.unwrap();
        assert_eq!(r.len(), 0);
    }

    #[tokio::test]
    async fn test_sstable_tombstone() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sstable-tomb.bin");
        let io: Arc<dyn IoBackend> = select_backend(IoBackendKind::AsyncFs);

        let mut writer = SStableWriter::new(3, path.clone(), io.clone());
        writer.write(LsmKey::doc(1), LsmValue::Data(b"v1".to_vec())).await.unwrap();
        writer.write(LsmKey::doc(2), LsmValue::Tombstone).await.unwrap();
        let sst = writer.finish().await.unwrap();

        let v1 = sst.get(&LsmKey::doc(1)).await.unwrap().unwrap();
        assert!(matches!(v1, LsmValue::Data(ref b) if b == b"v1"));
        let v2 = sst.get(&LsmKey::doc(2)).await.unwrap().unwrap();
        assert!(matches!(v2, LsmValue::Tombstone));
    }
}
