//! LSM 引擎：MemTable + SSTable + WAL + Manifest 的门面与协调器。
//!
//! 本文件包含：
//! - [`LsmKey`] / [`LsmValue`]：键值数据结构及序列化。
//! - [`LsmConfig`]：引擎配置。
//! - [`MemTable`]：基于 `crossbeam_skiplist::SkipMap` 的内存表。
//! - [`LsmEngine`]：门面（任务 1.6 实现）。

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use crossbeam_skiplist::SkipMap;

use super::io_backend::IoBackendKind;
use super::wal::WalSync;

/// 命名空间常量。键的首字节，决定条目归属。
pub const NS_DOC: u8 = 0x01;
pub const NS_VEC: u8 = 0x02;
pub const NS_META: u8 = 0x03;
/// 词法索引 posting 命名空间。key: (field_id, term_hash, doc_id)。
pub const NS_LEXICAL_POSTING: u8 = 0x04;
/// 词法索引统计信息（total_docs 等）。
pub const NS_LEXICAL_STATS: u8 = 0x05;

/// LSM 键。
///
/// 排序序：`(namespace, partition_or_segment, doc_id, lsn)` 全升序。
/// 同一 `(ns, partition, doc_id)` 的多个版本在 SSTable 与 MemTable 中连续存放，
/// 最高 `lsn` 排在最后。`get` 与 `scan_prefix` 据此取最新版本。
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct LsmKey {
    pub namespace: u8,
    pub partition_or_segment: u32,
    pub doc_id: u64,
    pub lsn: u64,
}

impl LsmKey {
    /// DocStore 命名空间下的查找键（partition=0, lsn=0）。
    pub fn doc(doc_id: u64) -> Self {
        Self {
            namespace: NS_DOC,
            partition_or_segment: 0,
            doc_id,
            lsn: 0,
        }
    }

    /// VectorPart 命名空间下的查找键（lsn=0）。
    pub fn vec(partition: u32, doc_id: u64) -> Self {
        Self {
            namespace: NS_VEC,
            partition_or_segment: partition,
            doc_id,
            lsn: 0,
        }
    }

    /// Meta 命名空间下的查找键。`name` 通过 `DefaultHasher` 哈希到 `doc_id`。
    pub fn meta(name: &str) -> Self {
        let mut hasher = DefaultHasher::new();
        name.hash(&mut hasher);
        Self {
            namespace: NS_META,
            partition_or_segment: 0,
            doc_id: hasher.finish(),
            lsn: 0,
        }
    }

    /// 编码后的固定长度（21 字节：1+4+8+8）。
    pub const ENCODED_LEN: usize = 21;

    /// 编码为大端字节序（用于 SSTable 持久化与排序）。
    pub fn encode(&self) -> [u8; Self::ENCODED_LEN] {
        let mut buf = [0u8; Self::ENCODED_LEN];
        buf[0] = self.namespace;
        buf[1..5].copy_from_slice(&self.partition_or_segment.to_be_bytes());
        buf[5..13].copy_from_slice(&self.doc_id.to_be_bytes());
        buf[13..21].copy_from_slice(&self.lsn.to_be_bytes());
        buf
    }

    /// 从 21 字节缓冲解码。
    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() != Self::ENCODED_LEN {
            anyhow::bail!(
                "LsmKey decode: expected {} bytes, got {}",
                Self::ENCODED_LEN,
                buf.len()
            );
        }
        Ok(Self {
            namespace: buf[0],
            partition_or_segment: u32::from_be_bytes(buf[1..5].try_into().unwrap()),
            doc_id: u64::from_be_bytes(buf[5..13].try_into().unwrap()),
            lsn: u64::from_be_bytes(buf[13..21].try_into().unwrap()),
        })
    }
}

impl Ord for LsmKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.encode().cmp(&other.encode())
    }
}

impl PartialOrd for LsmKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// LSM 值：数据或墓碑。
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LsmValue {
    /// 字节数据。
    Data(Vec<u8>),
    /// 删除标记。Compaction 时若为最新版本，会丢弃整组历史版本。
    Tombstone,
}

impl LsmValue {
    pub(crate) const TYPE_DATA: u8 = 0;
    pub(crate) const TYPE_TOMBSTONE: u8 = 1;

    /// 编码为字节：`[type:u8][len:u32 BE][data]` 或 `[type:u8]`（Tombstone）。
    pub fn encode(&self) -> Vec<u8> {
        match self {
            LsmValue::Data(b) => {
                let mut v = Vec::with_capacity(1 + 4 + b.len());
                v.push(Self::TYPE_DATA);
                v.extend_from_slice(&(b.len() as u32).to_be_bytes());
                v.extend_from_slice(b);
                v
            }
            LsmValue::Tombstone => vec![Self::TYPE_TOMBSTONE],
        }
    }

    /// 从字节解码。
    pub fn decode(buf: &[u8]) -> Result<Self> {
        let (v, _) = Self::decode_with_len(buf)?;
        Ok(v)
    }

    /// 解码并返回消费的字节数（SSTable 扫描时需要）。
    pub fn decode_with_len(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.is_empty() {
            anyhow::bail!("LsmValue decode: empty buffer");
        }
        match buf[0] {
            Self::TYPE_DATA => {
                if buf.len() < 5 {
                    anyhow::bail!("LsmValue decode: Data too short");
                }
                let len = u32::from_be_bytes(buf[1..5].try_into().unwrap()) as usize;
                if buf.len() < 5 + len {
                    anyhow::bail!(
                        "LsmValue decode: expected {} bytes payload, got {}",
                        len,
                        buf.len() - 5
                    );
                }
                Ok((LsmValue::Data(buf[5..5 + len].to_vec()), 5 + len))
            }
            Self::TYPE_TOMBSTONE => Ok((LsmValue::Tombstone, 1)),
            other => anyhow::bail!("LsmValue decode: unknown type {}", other),
        }
    }

    /// 序列化后字节数（用于 MemTable 大小估算）。
    pub fn encoded_len(&self) -> usize {
        match self {
            LsmValue::Data(b) => 1 + 4 + b.len(),
            LsmValue::Tombstone => 1,
        }
    }
}

/// LSM 引擎配置。
#[derive(Clone, Debug)]
pub struct LsmConfig {
    /// MemTable 触发 flush 的字节阈值。默认 64 MiB。
    pub memtable_max_bytes: u64,
    /// SSTable 数量达到此阈值时触发 compaction。默认 4。
    pub sstable_compact_threshold: usize,
    /// WAL 同步策略。
    pub wal_sync: WalSync,
    /// I/O 后端类型。
    pub io_backend: IoBackendKind,
}

impl Default for LsmConfig {
    fn default() -> Self {
        Self {
            memtable_max_bytes: 64 * 1024 * 1024,
            sstable_compact_threshold: 4,
            wal_sync: WalSync::EveryCommit,
            io_backend: IoBackendKind::AsyncFs,
        }
    }
}

/// MemTable：基于 `SkipMap` 的有序内存表。
///
/// 写入并发安全：`SkipMap` 支持并发插入；`bytes` 用 `AtomicU64` 估算大小。
pub struct MemTable {
    table: SkipMap<LsmKey, LsmValue>,
    bytes: AtomicU64,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            table: SkipMap::new(),
            bytes: AtomicU64::new(0),
        }
    }

    /// 插入或覆盖一条记录。
    pub fn insert(&self, key: LsmKey, value: LsmValue) {
        let entry_bytes = (LsmKey::ENCODED_LEN + value.encoded_len()) as u64;
        self.table.insert(key, value);
        self.bytes.fetch_add(entry_bytes, Ordering::Relaxed);
    }

    /// 查找 `(ns, partition, doc_id)` 的最新版本。查询 key 的 `lsn` 字段被忽略。
    pub fn get(&self, key: &LsmKey) -> Option<LsmValue> {
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
        let mut last: Option<LsmValue> = None;
        for entry in self.table.range(lo..=hi) {
            last = Some(entry.value().clone());
        }
        last
    }

    /// 范围扫描 `(ns, partition)` 下所有条目，按 key 升序返回。
    pub fn range_scan(&self, ns: u8, partition: u32) -> Vec<(LsmKey, LsmValue)> {
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
        self.table
            .range(lo..=hi)
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    /// 估算当前 MemTable 字节数（含 key+value 序列化开销）。
    pub fn approx_bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    /// 消费 MemTable，返回有序迭代器。
    pub fn into_iter(self) -> impl Iterator<Item = (LsmKey, LsmValue)> {
        self.table.into_iter()
    }

    /// 借用迭代（克隆 key/value）。flush_memtable 用。
    pub fn iter(&self) -> impl Iterator<Item = (LsmKey, LsmValue)> + '_ {
        self.table
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
    }

    /// 是否为空。
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

use std::collections::HashSet;
use std::path::Path;

use super::io_backend::{select_backend, IoBackend};
use super::manifest::{Manifest, SSTableMeta};
use super::sstable::{SSTable, SStableWriter};
use super::wal::{Wal, WalOp};

use tokio::sync::{Mutex as TokioMutex, RwLock};

/// LSM 引擎门面。协调 MemTable、WAL、SSTable 列表、Manifest。
pub struct LsmEngine {
    /// 活跃 MemTable（读多写少，用 RwLock 包 Arc 实现读时无锁拷贝）。
    memtable: RwLock<Arc<MemTable>>,
    /// 刷盘中的 MemTable（双 buffer）。
    flushing: RwLock<Option<Arc<MemTable>>>,
    /// SSTable 列表（按生成顺序，越后越新）。
    sstables: RwLock<Vec<Arc<SSTable>>>,
    /// 预写日志。
    wal: Arc<Wal>,
    /// I/O 后端。
    io: Arc<dyn IoBackend>,
    /// 配置。
    config: LsmConfig,
    /// compaction 互斥锁。
    compaction_lock: TokioMutex<()>,
    /// 下一个 SSTable id。
    next_sstable_id: AtomicU64,
    /// 数据目录。
    dir: std::path::PathBuf,
    /// 索引清单。
    manifest: RwLock<Manifest>,
}

impl LsmEngine {
    /// 打开（或创建）LSM 引擎。
    ///
    /// 恢复流程：加载 manifest → 打开已有 SSTable → 重放 WAL 到新 MemTable。
    pub async fn open(dir: &Path, config: LsmConfig) -> Result<Self> {
        std::fs::create_dir_all(dir)
            .map_err(|e| anyhow::anyhow!("create_dir_all {:?}: {}", dir, e))?;

        let io: Arc<dyn IoBackend> = select_backend(config.io_backend);
        let wal_path = dir.join("wal.log");
        let wal = Wal::open(wal_path.clone(), io.clone(), config.wal_sync.clone()).await?;

        let manifest_path = dir.join("manifest.json");
        let mut manifest = if manifest_path.exists() {
            Manifest::load(&manifest_path).await?
        } else {
            Manifest::new(now_secs(), "default".into(), wal_path.clone())
        };

        // Open existing SSTables.
        let mut sstables: Vec<Arc<SSTable>> = Vec::new();
        for meta in &manifest.sstables {
            let sst = SSTable::open(meta.path.clone(), io.clone()).await?;
            sstables.push(Arc::new(sst));
        }

        let max_sst_id = manifest.sstables.iter().map(|m| m.id).max().unwrap_or(0) as u64;
        let memtable = Arc::new(MemTable::new());

        // Replay WAL into MemTable.
        let records = wal.recover().await?;
        for rec in records {
            match rec.op {
                WalOp::Put { key, value } => {
                    memtable.insert(key, value);
                }
                WalOp::BatchPut { entries } => {
                    for (mut key, value) in entries {
                        key.lsn = rec.lsn;
                        memtable.insert(key, value);
                    }
                }
                WalOp::Delete { namespace, doc_id } => {
                    let key = LsmKey {
                        namespace,
                        partition_or_segment: 0,
                        doc_id,
                        lsn: rec.lsn,
                    };
                    memtable.insert(key, LsmValue::Tombstone);
                }
                WalOp::PartitionSplit { .. } => {
                    // TODO Phase 4: 分区分裂。
                }
                WalOp::Checkpoint { last_applied_lsn } => {
                    manifest.last_checkpoint_lsn = last_applied_lsn;
                }
            }
        }

        Ok(Self {
            memtable: RwLock::new(memtable),
            flushing: RwLock::new(None),
            sstables: RwLock::new(sstables),
            wal: Arc::new(wal),
            io,
            config,
            compaction_lock: TokioMutex::new(()),
            next_sstable_id: AtomicU64::new(max_sst_id + 1),
            dir: dir.to_path_buf(),
            manifest: RwLock::new(manifest),
        })
    }

    /// 插入 / 更新。返回分配的 lsn。
    pub async fn put(&self, key: LsmKey, value: LsmValue) -> Result<u64> {
        let lsn = self
            .wal
            .append(WalOp::Put {
                key: key.clone(),
                value: value.clone(),
            })
            .await?;
        let mut key = key;
        key.lsn = lsn;
        let memtable = self.memtable.read().await.clone();
        memtable.insert(key, value);
        if memtable.approx_bytes() > self.config.memtable_max_bytes {
            self.maybe_flush().await?;
        }
        Ok(lsn)
    }

    /// 批量插入 / 更新。返回起始 lsn。所有条目共享起始 LSN。
    pub async fn batch_put(&self, entries: Vec<(LsmKey, LsmValue)>) -> Result<u64> {
        if entries.is_empty() {
            return Ok(self.wal.next_lsn());
        }

        // 计算总字节数用于 flush 检查
        let total_bytes: u64 = entries
            .iter()
            .map(|(k, v)| (LsmKey::ENCODED_LEN + v.encoded_len()) as u64)
            .sum();

        let start_lsn = self.wal.append_batch(entries.clone()).await?;

        // 批量插入 MemTable
        let memtable = self.memtable.read().await.clone();
        for (mut key, value) in entries {
            key.lsn = start_lsn;
            memtable.insert(key, value);
        }

        // 检查是否需要 flush（使用总字节数估算）
        let current_bytes = memtable.approx_bytes();
        if current_bytes + total_bytes > self.config.memtable_max_bytes {
            self.maybe_flush().await?;
        }

        Ok(start_lsn)
    }

    /// 删除（墓碑）。返回分配的 lsn。
    pub async fn delete(&self, namespace: u8, doc_id: u64) -> Result<u64> {
        let lsn = self
            .wal
            .append(WalOp::Delete { namespace, doc_id })
            .await?;
        let key = LsmKey {
            namespace,
            partition_or_segment: 0,
            doc_id,
            lsn,
        };
        let memtable = self.memtable.read().await.clone();
        memtable.insert(key, LsmValue::Tombstone);
        if memtable.approx_bytes() > self.config.memtable_max_bytes {
            self.maybe_flush().await?;
        }
        Ok(lsn)
    }

    /// 查找 `(ns, partition, doc_id)` 的最新版本。查询 key 的 `lsn` 字段被忽略。
    pub async fn get(&self, key: &LsmKey) -> Result<Option<LsmValue>> {
        // [1] MemTable
        let memtable = self.memtable.read().await.clone();
        if let Some(v) = memtable.get(key) {
            return Ok(Some(v));
        }

        // [2] flushing MemTable
        if let Some(flushing) = self.flushing.read().await.as_ref() {
            if let Some(v) = flushing.get(key) {
                return Ok(Some(v));
            }
        }

        // [3] SSTable（新 → 旧）
        let sstables = self.sstables.read().await;
        for sst in sstables.iter().rev() {
            if let Some(v) = sst.get(key).await? {
                return Ok(Some(v));
            }
        }

        Ok(None)
    }

    /// 范围扫描 `(ns, partition)` 下所有条目，按 doc_id 去重保留最新版本。
    pub async fn scan_prefix(
        &self,
        namespace: u8,
        partition: u32,
    ) -> Result<Vec<(LsmKey, LsmValue)>> {
        let mut results = Vec::new();
        let mut seen_keys: HashSet<u64> = HashSet::new();

        let memtable = self.memtable.read().await.clone();
        for (k, v) in memtable.range_scan(namespace, partition) {
            if seen_keys.insert(k.doc_id) {
                results.push((k, v));
            }
        }

        if let Some(flushing) = self.flushing.read().await.as_ref() {
            for (k, v) in flushing.range_scan(namespace, partition) {
                if seen_keys.insert(k.doc_id) {
                    results.push((k, v));
                }
            }
        }

        let sstables = self.sstables.read().await;
        for sst in sstables.iter().rev() {
            for (k, v) in sst.scan_prefix(namespace, partition).await? {
                if seen_keys.insert(k.doc_id) {
                    results.push((k, v));
                }
            }
        }

        Ok(results)
    }

    /// 触发条件 flush：双 MemTable 切换 + 同步刷盘。
    /// Phase 1 采用同步刷盘（不 spawn），简化错误传播与测试时序。
    async fn maybe_flush(&self) -> Result<()> {
        let old_memtable = {
            let mut active = self.memtable.write().await;
            if active.is_empty() {
                return Ok(());
            }
            let new_memtable = Arc::new(MemTable::new());
            std::mem::replace(&mut *active, new_memtable)
        };

        *self.flushing.write().await = Some(old_memtable.clone());
        let sstable = self.flush_memtable(&old_memtable).await?;
        self.sstables.write().await.push(Arc::new(sstable));
        *self.flushing.write().await = None;

        if self.sstables.read().await.len() >= self.config.sstable_compact_threshold {
            self.trigger_compaction().await?;
        }
        Ok(())
    }

    /// 强制 flush 当前 MemTable（无视大小阈值）。测试用。
    pub async fn force_flush(&self) -> Result<()> {
        let old_memtable = {
            let mut active = self.memtable.write().await;
            if active.is_empty() {
                return Ok(());
            }
            let new_memtable = Arc::new(MemTable::new());
            std::mem::replace(&mut *active, new_memtable)
        };

        *self.flushing.write().await = Some(old_memtable.clone());
        let sstable = self.flush_memtable(&old_memtable).await?;
        self.sstables.write().await.push(Arc::new(sstable));
        *self.flushing.write().await = None;
        Ok(())
    }

    async fn flush_memtable(&self, memtable: &Arc<MemTable>) -> Result<SSTable> {
        let id = self.next_sstable_id();
        let path = self.dir.join(format!("sstable-{:04}.bin", id));
        let mut writer = SStableWriter::new(id as u32, path.clone(), self.io.clone());
        // MemTable 的 SkipMap 已按 key 升序，iter 保持顺序。
        for (key, value) in memtable.iter() {
            writer.write(key, value).await?;
        }
        let sst = writer.finish().await?;

        let meta = SSTableMeta {
            id: id as u32,
            path: path.clone(),
            entry_count: sst.entry_count,
            min_key_bytes: sst.min_key.encode(),
            max_key_bytes: sst.max_key.encode(),
        };
        {
            let mut m = self.manifest.write().await;
            m.add_sstable(meta);
        }
        self.persist_manifest().await?;

        Ok(sst)
    }

    /// 触发 compaction（若 SSTable 数达到阈值）。
    async fn trigger_compaction(&self) -> Result<()> {
        let _guard = self.compaction_lock.lock().await;
        let sstables = self.sstables.read().await.clone();
        if sstables.len() < self.config.sstable_compact_threshold {
            return Ok(());
        }
        self.run_compaction(sstables).await
    }

    /// 强制 compaction 所有 SSTable（无视阈值）。测试用。
    pub async fn force_compact(&self) -> Result<()> {
        let _guard = self.compaction_lock.lock().await;
        let sstables = self.sstables.read().await.clone();
        if sstables.is_empty() {
            return Ok(());
        }
        self.run_compaction(sstables).await
    }

    async fn run_compaction(&self, sstables: Vec<Arc<SSTable>>) -> Result<()> {
        let merged_opt = self.compact_sstables(&sstables).await?;

        let old_ids: Vec<u32> = sstables.iter().map(|s| s.id).collect();

        match merged_opt {
            Some(merged) => {
                let new_meta = SSTableMeta {
                    id: merged.id,
                    path: merged.path.clone(),
                    entry_count: merged.entry_count,
                    min_key_bytes: merged.min_key.encode(),
                    max_key_bytes: merged.max_key.encode(),
                };
                {
                    let mut current = self.sstables.write().await;
                    *current = vec![Arc::new(merged)];
                }
                {
                    let mut m = self.manifest.write().await;
                    m.replace_sstables(old_ids, new_meta);
                }
            }
            None => {
                // 全部被 tombstone 清空。
                {
                    let mut current = self.sstables.write().await;
                    *current = Vec::new();
                }
                {
                    let mut m = self.manifest.write().await;
                    let empty = SSTableMeta {
                        id: 0,
                        path: std::path::PathBuf::new(),
                        entry_count: 0,
                        min_key_bytes: [0; 21],
                        max_key_bytes: [0; 21],
                    };
                    m.replace_sstables(old_ids, empty);
                    // 移除占位空 meta。
                    m.sstables.retain(|s| s.entry_count > 0 || !s.path.as_os_str().is_empty());
                }
            }
        }

        self.persist_manifest().await?;

        // 删除旧 SSTable 文件（manifest 持久化后）。
        for sst in &sstables {
            let _ = sst.delete_file().await;
        }
        Ok(())
    }

    /// 多路归并 + 去重：按 `(ns, partition, doc_id)` 分组取最高 lsn 版本；
    /// 若最新版本是 Tombstone，丢弃整组（含底层 Data）。
    /// 返回 `None` 表示合并后无任何条目（全为墓碑）。
    async fn compact_sstables(&self, sstables: &[Arc<SSTable>]) -> Result<Option<SSTable>> {
        // Phase 1：收集所有条目 + sort + 单趟去重。O(N log N)。
        let mut all_entries: Vec<(LsmKey, LsmValue)> = Vec::new();
        for sst in sstables {
            all_entries.extend(sst.iter_all_raw().await?);
        }
        if all_entries.is_empty() {
            return Ok(None);
        }
        all_entries.sort_by(|(a, _), (b, _)| a.cmp(b));

        let new_id = self.next_sstable_id();
        let path = self.dir.join(format!("sstable-{:04}.bin", new_id));
        let mut writer = SStableWriter::new(new_id as u32, path.clone(), self.io.clone());

        let mut current_group: Option<(LsmKey, LsmValue)> = None;
        for (key, value) in all_entries {
            let group_changed = current_group.as_ref().map_or(true, |(k, _)| {
                k.namespace != key.namespace
                    || k.partition_or_segment != key.partition_or_segment
                    || k.doc_id != key.doc_id
            });
            if group_changed {
                if let Some((k, v)) = current_group.take() {
                    if !matches!(v, LsmValue::Tombstone) {
                        writer.write(k, v).await?;
                    }
                }
                current_group = Some((key, value));
            } else {
                // 同组：覆盖（升序遍历，后出现的 lsn 更大）。
                current_group = Some((key, value));
            }
        }
        if let Some((k, v)) = current_group.take() {
            if !matches!(v, LsmValue::Tombstone) {
                writer.write(k, v).await?;
            }
        }

        if writer.entry_count() == 0 {
            return Ok(None);
        }
        let sst = writer.finish().await?;
        Ok(Some(sst))
    }

    async fn persist_manifest(&self) -> Result<()> {
        let manifest_path = self.dir.join("manifest.json");
        self.manifest.read().await.save(&manifest_path).await?;
        self.io.sync_dir(&self.dir).await?;
        Ok(())
    }

    fn next_sstable_id(&self) -> u64 {
        self.next_sstable_id.fetch_add(1, Ordering::SeqCst)
    }

    /// 当前 SSTable 数量（测试与观测用）。
    pub async fn sstable_count(&self) -> usize {
        self.sstables.read().await.len()
    }

    /// 当前 MemTable 估算字节数。
    pub async fn memtable_bytes(&self) -> u64 {
        self.memtable.read().await.approx_bytes()
    }
}

fn now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
