//! 预写日志（Write-Ahead Log）。
//!
//! 记录格式（小端序无关，统一用大端序便于跨平台一致）：
//! ```text
//! [len:u32 BE][lsn:u64 BE][op:u8][payload:bytes][crc32:u32 BE]
//! ```
//! - `len` = 后续所有字节数（`lsn + op + payload + crc32` = 13 + payload_len）。
//! - `crc32` 覆盖 `lsn + op + payload`（即 `body[0..body_len-4]`）。
//!
//! `lsn` 由 [`Wal::append`] 内部分配（`next_lsn.fetch_add`）。`WalOp::Put` 中 `key.lsn`
//! 在传入时为 0，由 `append` 覆盖为实际 lsn；`recover` 时同样用记录的 lsn 还原 key。

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;

use super::io_backend::IoBackend;
use super::lsm::{LsmKey, LsmValue};

/// WAL 同步策略。
#[derive(Clone, Debug)]
pub enum WalSync {
    /// 每次 commit 都 fsync。默认。
    EveryCommit,
    /// 周期性 fsync。
    Periodic(Duration),
    /// 不主动 fsync（仅依赖 OS 页缓存）。
    None,
}

impl Default for WalSync {
    fn default() -> Self {
        Self::EveryCommit
    }
}

/// WAL 操作类型码。
const OP_PUT: u8 = 0;
const OP_DELETE: u8 = 1;
const OP_PARTITION_SPLIT: u8 = 2;
const OP_CHECKPOINT: u8 = 3;

/// WAL 操作记录。
#[derive(Clone, Debug)]
pub enum WalOp {
    /// 插入 / 更新。`key.lsn` 在 `append` 时会被覆盖。
    Put { key: LsmKey, value: LsmValue },
    /// 删除（墓碑）。
    Delete { namespace: u8, doc_id: u64 },
    /// 分区分裂。
    PartitionSplit { old: u32, new: u32, moved: u64 },
    /// 检查点（标记已 apply 到该 lsn）。
    Checkpoint { last_applied_lsn: u64 },
}

impl WalOp {
    fn op_code(&self) -> u8 {
        match self {
            WalOp::Put { .. } => OP_PUT,
            WalOp::Delete { .. } => OP_DELETE,
            WalOp::PartitionSplit { .. } => OP_PARTITION_SPLIT,
            WalOp::Checkpoint { .. } => OP_CHECKPOINT,
        }
    }

    fn encode_payload(&self) -> Vec<u8> {
        match self {
            WalOp::Put { key, value } => {
                let mut buf = Vec::with_capacity(LsmKey::ENCODED_LEN + value.encoded_len());
                buf.extend_from_slice(&key.encode());
                buf.extend_from_slice(&value.encode());
                buf
            }
            WalOp::Delete { namespace, doc_id } => {
                let mut buf = Vec::with_capacity(9);
                buf.push(*namespace);
                buf.extend_from_slice(&doc_id.to_be_bytes());
                buf
            }
            WalOp::PartitionSplit { old, new, moved } => {
                let mut buf = Vec::with_capacity(16);
                buf.extend_from_slice(&old.to_be_bytes());
                buf.extend_from_slice(&new.to_be_bytes());
                buf.extend_from_slice(&moved.to_be_bytes());
                buf
            }
            WalOp::Checkpoint { last_applied_lsn } => {
                last_applied_lsn.to_be_bytes().to_vec()
            }
        }
    }

    fn decode(op_code: u8, lsn: u64, payload: &[u8]) -> Result<WalOp> {
        match op_code {
            OP_PUT => {
                if payload.len() < LsmKey::ENCODED_LEN {
                    anyhow::bail!("Put payload too short: {}", payload.len());
                }
                let mut key = LsmKey::decode(&payload[..LsmKey::ENCODED_LEN])?;
                key.lsn = lsn;
                let value = LsmValue::decode(&payload[LsmKey::ENCODED_LEN..])?;
                Ok(WalOp::Put { key, value })
            }
            OP_DELETE => {
                if payload.len() < 9 {
                    anyhow::bail!("Delete payload too short: {}", payload.len());
                }
                let namespace = payload[0];
                let doc_id = u64::from_be_bytes(payload[1..9].try_into().unwrap());
                Ok(WalOp::Delete { namespace, doc_id })
            }
            OP_PARTITION_SPLIT => {
                if payload.len() < 16 {
                    anyhow::bail!("PartitionSplit payload too short: {}", payload.len());
                }
                let old = u32::from_be_bytes(payload[0..4].try_into().unwrap());
                let new = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                let moved = u64::from_be_bytes(payload[8..16].try_into().unwrap());
                Ok(WalOp::PartitionSplit { old, new, moved })
            }
            OP_CHECKPOINT => {
                if payload.len() < 8 {
                    anyhow::bail!("Checkpoint payload too short: {}", payload.len());
                }
                let last_applied_lsn = u64::from_be_bytes(payload[0..8].try_into().unwrap());
                Ok(WalOp::Checkpoint { last_applied_lsn })
            }
            other => anyhow::bail!("unknown WalOp code {}", other),
        }
    }
}

/// 一条已恢复的 WAL 记录。
#[derive(Clone, Debug)]
pub struct WalRecord {
    pub lsn: u64,
    pub op: WalOp,
    /// 该记录在文件中占用的总字节数（含 len 字段）。
    pub total_len: usize,
}

/// 预写日志。
pub struct Wal {
    path: PathBuf,
    io: Arc<dyn IoBackend>,
    sync_policy: WalSync,
    current_size: AtomicU64,
    rotate_size: u64,
    next_lsn: AtomicU64,
}

impl Wal {
    /// 打开（或创建）WAL。
    pub async fn open(
        path: PathBuf,
        io: Arc<dyn IoBackend>,
        sync_policy: WalSync,
    ) -> Result<Self> {
        if !path.exists() {
            io.create_file(&path).await?;
        }
        let size = io.file_size(&path).await?;
        // 初步 next_lsn 设为 0；recover() 会根据已有记录推进。
        Ok(Self {
            path,
            io,
            sync_policy,
            current_size: AtomicU64::new(size),
            rotate_size: 64 * 1024 * 1024,
            next_lsn: AtomicU64::new(0),
        })
    }

    /// 追加一条记录，返回分配的 lsn。
    pub async fn append(&self, op: WalOp) -> Result<u64> {
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        let op_code = op.op_code();
        let payload = op.encode_payload();
        let record = encode_record(lsn, op_code, &payload);

        let offset = self
            .current_size
            .fetch_add(record.len() as u64, Ordering::SeqCst);
        self.io.write_at(&self.path, offset, &record).await?;

        match self.sync_policy {
            WalSync::EveryCommit => {
                self.io.fsync(&self.path).await?;
            }
            WalSync::Periodic(_) => {
                // TODO: 批量 fsync。
            }
            WalSync::None => {}
        }

        // 简易 rotate 触发（不主动清理旧 WAL，留给上层 manifest 处理）。
        if self.current_size.load(Ordering::SeqCst) > self.rotate_size {
            let _ = self.rotate().await;
        }

        Ok(lsn)
    }

    /// 主动 fsync。
    pub async fn fsync(&self) -> Result<()> {
        self.io.fsync(&self.path).await
    }

    /// 扫描整个 WAL，返回所有完整记录。最后一条若截断则跳过。
    /// 同时推进 `next_lsn` 至已恢复最大 lsn + 1。
    pub async fn recover(&self) -> Result<Vec<WalRecord>> {
        let size = self.io.file_size(&self.path).await? as usize;
        if size == 0 {
            return Ok(Vec::new());
        }
        let mut buf = vec![0u8; size];
        let n = self.io.read_at(&self.path, 0, &mut buf).await?;
        buf.truncate(n);

        let mut records = Vec::new();
        let mut offset = 0;
        let mut max_lsn = 0u64;
        while offset < buf.len() {
            match parse_record(&buf[offset..]) {
                Ok((rec, consumed)) => {
                    if rec.lsn > max_lsn {
                        max_lsn = rec.lsn;
                    }
                    records.push(rec);
                    offset += consumed;
                }
                Err(_) => {
                    // 尾部不完整记录：忽略（崩溃时未完整写入）。
                    break;
                }
            }
        }
        self.next_lsn.store(max_lsn + 1, Ordering::SeqCst);
        Ok(records)
    }

    /// 轮转：将当前 WAL 重命名为 `*.old`，新建空 WAL。
    pub async fn rotate(&self) -> Result<()> {
        let old_path = self.path.with_extension("wal.old");
        // 先 invalidate 句柄缓存，避免 rename 后句柄悬空。
        self.io.delete_file(&self.path).await.ok();
        if self.path.exists() {
            std::fs::rename(&self.path, &old_path)
                .map_err(|e| anyhow::anyhow!("rotate rename {:?}: {}", self.path, e))?;
        }
        self.io.create_file(&self.path).await?;
        self.current_size.store(0, Ordering::SeqCst);
        Ok(())
    }

    /// 截断 WAL 至指定 lsn（含）之前。用于 checkpoint 后回收空间。
    pub async fn truncate_to(&self, lsn: u64) -> Result<()> {
        let records = self.recover().await?;
        let mut offset = 0u64;
        for rec in &records {
            if rec.lsn >= lsn {
                break;
            }
            offset += rec.total_len as u64;
        }
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&self.path)
            .map_err(|e| anyhow::anyhow!("truncate open {:?}: {}", self.path, e))?;
        f.set_len(offset)
            .map_err(|e| anyhow::anyhow!("set_len {:?}: {}", self.path, e))?;
        self.current_size.store(offset, Ordering::SeqCst);
        Ok(())
    }

    /// 当前下一 lsn（仅用于观测）。
    pub fn next_lsn(&self) -> u64 {
        self.next_lsn.load(Ordering::SeqCst)
    }
}

/// 编码一条 WAL 记录。
fn encode_record(lsn: u64, op_code: u8, payload: &[u8]) -> Vec<u8> {
    let body_len = 8 + 1 + payload.len() + 4; // lsn + op + payload + crc32
    let mut buf = Vec::with_capacity(4 + body_len);
    buf.extend_from_slice(&(body_len as u32).to_be_bytes());
    buf.extend_from_slice(&lsn.to_be_bytes());
    buf.push(op_code);
    buf.extend_from_slice(payload);
    let crc = crc32fast::hash(&buf[4..4 + 8 + 1 + payload.len()]);
    buf.extend_from_slice(&crc.to_be_bytes());
    buf
}

/// 解析一条记录。返回 `(record, consumed_bytes)`。若数据不完整或 CRC 校验失败返回 `Err`。
fn parse_record(buf: &[u8]) -> Result<(WalRecord, usize)> {
    if buf.len() < 4 {
        anyhow::bail!("incomplete len field");
    }
    let body_len = u32::from_be_bytes(buf[0..4].try_into().unwrap()) as usize;
    let total_len = 4 + body_len;
    if buf.len() < total_len {
        anyhow::bail!("incomplete body");
    }
    let body = &buf[4..total_len];
    // body 至少 13 字节：lsn(8) + op(1) + crc(4)。
    if body_len < 13 {
        anyhow::bail!("body too short: {}", body_len);
    }
    let lsn = u64::from_be_bytes(body[0..8].try_into().unwrap());
    let op_code = body[8];
    let payload = &body[9..body_len - 4];
    let crc_stored = u32::from_be_bytes(body[body_len - 4..].try_into().unwrap());
    let crc_computed = crc32fast::hash(&body[0..body_len - 4]);
    if crc_stored != crc_computed {
        anyhow::bail!("CRC mismatch at lsn={}", lsn);
    }
    let op = WalOp::decode(op_code, lsn, payload)?;
    Ok((WalRecord { lsn, op, total_len }, total_len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::io_backend::{select_backend, AsyncFsBackend, IoBackendKind};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_wal_append_recover() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let io: Arc<dyn IoBackend> = select_backend(IoBackendKind::AsyncFs);
        let wal = Wal::open(path.clone(), io.clone(), WalSync::EveryCommit)
            .await
            .unwrap();

        let lsn1 = wal
            .append(WalOp::Put {
                key: LsmKey::doc(1),
                value: LsmValue::Data(b"v1".to_vec()),
            })
            .await
            .unwrap();
        let lsn2 = wal
            .append(WalOp::Put {
                key: LsmKey::doc(2),
                value: LsmValue::Data(b"v2".to_vec()),
            })
            .await
            .unwrap();
        assert_eq!(lsn1, 0);
        assert_eq!(lsn2, 1);

        let records = wal.recover().await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].lsn, 0);
        assert_eq!(records[1].lsn, 1);
        assert!(matches!(records[0].op, WalOp::Put { .. }));
        assert_eq!(wal.next_lsn(), 2);
    }

    #[tokio::test]
    async fn test_wal_recover_after_drop() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let io: Arc<dyn IoBackend> = Arc::new(AsyncFsBackend::new());

        {
            let wal = Wal::open(path.clone(), io.clone(), WalSync::EveryCommit)
                .await
                .unwrap();
            wal.append(WalOp::Put {
                key: LsmKey::doc(1),
                value: LsmValue::Data(b"v1".to_vec()),
            })
            .await
            .unwrap();
            wal.append(WalOp::Delete { namespace: 1, doc_id: 5 })
                .await
                .unwrap();
        }

        // 重开并恢复
        let wal = Wal::open(path.clone(), io.clone(), WalSync::EveryCommit)
            .await
            .unwrap();
        let records = wal.recover().await.unwrap();
        assert_eq!(records.len(), 2);
        assert!(matches!(records[0].op, WalOp::Put { .. }));
        assert!(matches!(records[1].op, WalOp::Delete { .. }));
    }

    #[tokio::test]
    async fn test_wal_truncate() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let io: Arc<dyn IoBackend> = Arc::new(AsyncFsBackend::new());
        let wal = Wal::open(path.clone(), io.clone(), WalSync::EveryCommit)
            .await
            .unwrap();
        wal.append(WalOp::Put {
            key: LsmKey::doc(1),
            value: LsmValue::Data(b"v1".to_vec()),
        })
        .await
        .unwrap();
        wal.append(WalOp::Put {
            key: LsmKey::doc(2),
            value: LsmValue::Data(b"v2".to_vec()),
        })
        .await
        .unwrap();
        wal.append(WalOp::Put {
            key: LsmKey::doc(3),
            value: LsmValue::Data(b"v3".to_vec()),
        })
        .await
        .unwrap();

        wal.truncate_to(2).await.unwrap();
        let records = wal.recover().await.unwrap();
        assert_eq!(records.len(), 2); // lsn 0, 1 保留
    }
}
