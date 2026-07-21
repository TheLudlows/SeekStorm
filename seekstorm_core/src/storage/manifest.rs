//! 索引元数据清单（Manifest）。
//!
//! 记录一个索引的：身份信息、SSTable 列表、WAL 路径、最近 checkpoint lsn 等。
//! 序列化为 JSON 写入 `manifest.json`。

use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// SSTable 在 manifest 中的条目。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SSTableMeta {
    pub id: u32,
    pub path: PathBuf,
    pub entry_count: u32,
    pub min_key_bytes: [u8; 21],
    pub max_key_bytes: [u8; 21],
}

/// 索引清单。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub index_id: u64,
    pub index_name: String,
    pub sstables: Vec<SSTableMeta>,
    pub wal_path: PathBuf,
    pub last_checkpoint_lsn: u64,
    pub created_at: u64,
    pub updated_at: u64,
}

impl Manifest {
    /// 创建新清单。
    pub fn new(index_id: u64, index_name: String, wal_path: PathBuf) -> Self {
        let now = now_secs();
        Self {
            index_id,
            index_name,
            sstables: Vec::new(),
            wal_path,
            last_checkpoint_lsn: 0,
            created_at: now,
            updated_at: now,
        }
    }

    /// 序列化并写入 `path`（原子：先写 `path.tmp` 再 rename）。
    pub async fn save(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_vec_pretty(self)?;
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, &json)
            .map_err(|e| anyhow::anyhow!("manifest write {:?}: {}", tmp, e))?;
        std::fs::rename(&tmp, path)
            .map_err(|e| anyhow::anyhow!("manifest rename {:?}: {}", path, e))?;
        Ok(())
    }

    /// 从 `path` 读取并反序列化。
    pub async fn load(path: &Path) -> Result<Self> {
        let buf = std::fs::read(path)
            .map_err(|e| anyhow::anyhow!("manifest read {:?}: {}", path, e))?;
        let m: Manifest = serde_json::from_slice(&buf)
            .map_err(|e| anyhow::anyhow!("manifest parse {:?}: {}", path, e))?;
        Ok(m)
    }

    /// 添加一个 SSTable 元数据。
    pub fn add_sstable(&mut self, meta: SSTableMeta) {
        self.sstables.push(meta);
        self.updated_at = now_secs();
    }

    /// 用一个新的 SSTable 替换一组旧 SSTable（用于 compaction）。
    /// `old` 中每个 id 若不存在则跳过。
    pub fn replace_sstables(&mut self, old: Vec<u32>, new: SSTableMeta) {
        let old_set: std::collections::HashSet<u32> = old.into_iter().collect();
        self.sstables.retain(|m| !old_set.contains(&m.id));
        self.sstables.push(new);
        self.updated_at = now_secs();
    }
}

fn now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_manifest_save_load() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("manifest.json");
        let mut m = Manifest::new(42, "test-index".into(), dir.path().join("test.wal"));
        m.add_sstable(SSTableMeta {
            id: 1,
            path: dir.path().join("sstable-1.bin"),
            entry_count: 100,
            min_key_bytes: [0; 21],
            max_key_bytes: [0xff; 21],
        });
        m.save(&path).await.unwrap();

        let m2 = Manifest::load(&path).await.unwrap();
        assert_eq!(m2.index_id, 42);
        assert_eq!(m2.index_name, "test-index");
        assert_eq!(m2.sstables.len(), 1);
        assert_eq!(m2.sstables[0].id, 1);
        assert_eq!(m2.sstables[0].entry_count, 100);
    }

    #[tokio::test]
    async fn test_manifest_replace() {
        let dir = tempdir().unwrap();
        let mut m = Manifest::new(1, "idx".into(), dir.path().join("w.wal"));
        for i in 1..=4 {
            m.add_sstable(SSTableMeta {
                id: i,
                path: dir.path().join(format!("sstable-{}.bin", i)),
                entry_count: i * 10,
                min_key_bytes: [0; 21],
                max_key_bytes: [0xff; 21],
            });
        }
        assert_eq!(m.sstables.len(), 4);

        m.replace_sstables(vec![1, 2, 3, 4], SSTableMeta {
            id: 5,
            path: dir.path().join("sstable-5.bin"),
            entry_count: 100,
            min_key_bytes: [0; 21],
            max_key_bytes: [0xff; 21],
        });
        assert_eq!(m.sstables.len(), 1);
        assert_eq!(m.sstables[0].id, 5);
    }
}
