//! Index 子系统（Phase 2 §2.3）：在 LSM 与 Schema 之上提供文档 CRUD。
//!
//! [`Index`] 是用户访问一个索引的入口。它持有：
//! - [`LsmEngine`]：负责 DocStore / VectorPart / Meta 三个命名空间的持久化。
//! - [`DynamicSchema`]：按需推断字段、序列化到 Meta 命名空间。
//! - [`Manifest`]：索引元数据（SSTable 列表、checkpoint lsn）。
//! - `next_doc_id`：单调递增的文档 id 分配器。
//!
//! Phase 3 通过 `OnceLock` 接入 [`LexicalEngine`]（基于 tantivy 分词器），Phase 4-5 接入 [`VectorEngine`]。

pub mod document;
pub mod vector;

pub use document::SchemalessDoc;
pub use vector::VectorEngine;

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::lexical::LexicalEngine;
use crate::schema::DynamicSchema;
use crate::storage::lsm::{LsmConfig, LsmEngine, LsmKey, LsmValue, NS_DOC};
use crate::storage::manifest::Manifest;

/// 文档 id 类型。
pub type DocId = u64;

/// 索引配置。
#[derive(Clone, Debug)]
pub struct IndexConfig {
    /// 索引名（仅用于显示与 manifest）。
    pub name: String,
    /// 底层 LSM 配置。
    pub lsm: LsmConfig,
    /// 索引 id（0 表示启动时自动分配）。
    pub index_id: u64,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            name: "default".into(),
            lsm: LsmConfig::default(),
            index_id: 0,
        }
    }
}

/// Meta 命名空间下记录 next_doc_id 的键名。
const META_KEY_NEXT_DOC_ID: &str = "next_doc_id";
/// Meta 命名空间下记录 schema 快照的键名。
const META_KEY_SCHEMA: &str = "schema";

/// 索引。一个索引对应磁盘上一个目录。
pub struct Index {
    pub lsm: Arc<LsmEngine>,
    pub schema: Arc<RwLock<DynamicSchema>>,
    pub manifest: RwLock<Manifest>,
    pub config: IndexConfig,
    next_doc_id: AtomicU64,

    /// Phase 3 接入：tantovy 词法引擎。
    pub lexical: OnceLock<Arc<LexicalEngine>>,
    /// Phase 4-5 接入：SPFresh LIRE 向量引擎。
    pub vector: OnceLock<Arc<VectorEngine>>,
}

impl Index {
    /// 创建新索引。目录必须为空或不存在。
    pub async fn create(path: &Path, config: IndexConfig) -> Result<Self> {
        if path.exists() && std::fs::read_dir(path)?.next().is_some() {
            anyhow::bail!("create: directory {:?} is not empty", path);
        }
        Self::open_inner(path, config, true).await
    }

    /// 打开已有索引。会重放 WAL、加载 schema 快照、恢复 next_doc_id。
    pub async fn open(path: &Path) -> Result<Self> {
        let config = IndexConfig::default();
        Self::open_inner(path, config, false).await
    }

    async fn open_inner(path: &Path, config: IndexConfig, is_create: bool) -> Result<Self> {
        let lsm = LsmEngine::open(path, config.lsm.clone()).await?;
        let lsm = Arc::new(lsm);

        // 加载或初始化 schema。
        let schema = match lsm.get(&LsmKey::meta(META_KEY_SCHEMA)).await? {
            Some(LsmValue::Data(bytes)) => DynamicSchema::deserialize(&bytes).await?,
            _ => DynamicSchema::new(),
        };
        let schema = Arc::new(RwLock::new(schema));

        // 恢复 next_doc_id：取 checkpoint 与 DocStore 最大 doc_id+1 的较大者。
        let checkpoint_next = match lsm.get(&LsmKey::meta(META_KEY_NEXT_DOC_ID)).await? {
            Some(LsmValue::Data(bytes)) => {
                if bytes.len() >= 8 {
                    u64::from_be_bytes(bytes[..8].try_into().unwrap())
                } else {
                    0
                }
            }
            _ => 0,
        };
        let existing = lsm.scan_prefix(NS_DOC, 0).await?;
        let max_doc_id = existing
            .iter()
            .map(|(k, _)| k.doc_id)
            .max()
            .unwrap_or(0);
        // next_doc_id = max(checkpoint, max_doc_id+1, 1)
        let next_doc_id = checkpoint_next.max(max_doc_id + 1).max(1);

        // 构造 manifest。Phase 2 简化：复用 LSM 内部 manifest 的概念，
        // 这里仅保留一份索引级元数据供后续阶段扩展。
        let manifest = Manifest::new(
            if config.index_id != 0 {
                config.index_id
            } else if is_create {
                now_secs()
            } else {
                0
            },
            config.name.clone(),
            path.join("wal.log"),
        );

        // 初始化词法引擎（Phase 3）
        let lexical = LexicalEngine::new(lsm.clone(), schema.clone()).await?;

        Ok(Self {
            lsm,
            schema,
            manifest: RwLock::new(manifest),
            config,
            next_doc_id: AtomicU64::new(next_doc_id),
            lexical: OnceLock::from(Arc::new(lexical)),
            vector: OnceLock::new(),
        })
    }

    /// 插入文档。返回分配的 doc_id。
    ///
    /// 流程（批量写入优化）：
    /// 1. Schema 推断（冲突即报错）
    /// 2. 分配 doc_id
    /// 3. 序列化文档
    /// 4. 收集所有 LSM entries（DocStore + 词法索引 + 向量索引）
    /// 5. 批量写入 LSM（单次 WAL fsync）
    /// 6. 内存状态更新（lexical total_docs, vector partition counts）
    /// 7. Schema 变更同步（若有）
    pub async fn add_document(&self, doc: SchemalessDoc) -> Result<DocId> {
        // [1] Schema 推断
        let changes = {
            let schema = self.schema.read().await;
            schema.infer_and_add(&doc).await?
        };

        // [2] 分配 doc_id
        let doc_id = self.next_doc_id.fetch_add(1, Ordering::SeqCst);

        // [3] 序列化文档
        let doc_bytes = doc.to_bytes()?;

        // [4] 收集批量写入项
        let mut batch_entries = Vec::new();

        // 4a) 文档存储
        batch_entries.push((
            LsmKey::doc(doc_id),
            LsmValue::Data(doc_bytes)
        ));

        // 4b) 词法索引 postings
        if let Some(lexical) = self.lexical.get() {
            let lexical_entries = lexical.prepare_add_document(doc_id, &doc).await?;
            batch_entries.extend(lexical_entries);
        }

        // 4c) 向量索引（Phase 4）
        let vector_partition_ids = if let Some(vector) = self.vector.get() {
            let (vec_entries, partition_ids) = vector.prepare_add_document(doc_id, &doc).await?;
            batch_entries.extend(vec_entries);
            Some(partition_ids)
        } else {
            None
        };

        // 4d) Schema 变更同步（若有）
        let mut meta_entry = None;
        if !changes.is_empty() {
            let schema_snapshot = {
                let schema = self.schema.read().await;
                schema.serialize().await
            };
            meta_entry = Some((
                LsmKey::meta(META_KEY_SCHEMA),
                LsmValue::Data(schema_snapshot)
            ));
        }

        // [5] 批量写入 LSM（包含 Meta）
        if let Some(meta) = meta_entry {
            batch_entries.push(meta);
        }
        self.lsm.batch_put(batch_entries).await?;

        // [6] 内存状态更新（不涉及 LSM）
        // 词法引擎：已在 prepare_add_document 中更新 total_docs
        // 向量引擎：更新分区计数
        if let Some(partition_ids) = vector_partition_ids {
            if let Some(vector) = self.vector.get() {
                for partition_id in partition_ids {
                    vector.update_partition_count(doc_id, partition_id).await;
                }
            }
        }

        Ok(doc_id)
    }

    /// 按 doc_id 查询文档。返回 `None` 表示不存在或已被删除（墓碑）。
    pub async fn get_document(&self, doc_id: DocId) -> Result<Option<SchemalessDoc>> {
        let value = self.lsm.get(&LsmKey::doc(doc_id)).await?;
        match value {
            Some(LsmValue::Data(bytes)) => Ok(Some(SchemalessDoc::from_bytes(&bytes)?)),
            Some(LsmValue::Tombstone) => Ok(None),
            None => Ok(None),
        }
    }

    /// 删除文档（写墓碑）。Phase 7 增加崩溃恢复语义。
    pub async fn delete_document(&self, doc_id: DocId) -> Result<()> {
        self.lsm.delete(NS_DOC, doc_id).await?;
        Ok(())
    }

    /// 提交：确保 MemTable flush + WAL fsync。
    /// Phase 3 后：同时提交词法引擎的 total_docs 统计。
    pub async fn commit(&self) -> Result<()> {
        // 持久化 next_doc_id（崩溃恢复时用作下限）。
        let current = self.next_doc_id.load(Ordering::SeqCst);
        let bytes = current.to_be_bytes();
        self.lsm
            .put(
                LsmKey::meta(META_KEY_NEXT_DOC_ID),
                LsmValue::Data(bytes.to_vec()),
            )
            .await?;

        // 提交词法引擎统计（Phase 3）
        if let Some(lexical) = self.lexical.get() {
            lexical.commit().await?;
        }

        // 触发 flush 让最近写入落到 SSTable。
        self.lsm.force_flush().await?;
        Ok(())
    }

    /// 词法搜索（Phase 3）。
    pub async fn lexical_search(&self, query: &str, limit: usize) -> Result<Vec<crate::lexical::ScoredDoc>> {
        if let Some(lexical) = self.lexical.get() {
            lexical.search(query, limit).await
        } else {
            Ok(Vec::new())
        }
    }
}

fn now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// 用于在 Meta 命名空间序列化 next_doc_id 的小工具（公开给测试）。
#[derive(Serialize, Deserialize)]
pub struct DocIdCheckpoint {
    pub next_doc_id: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_create_and_add_document() {
        let dir = tempdir().unwrap();
        let idx = Index::create(dir.path(), IndexConfig::default()).await.unwrap();
        let doc = SchemalessDoc::from_json(r#"{"title":"hello"}"#).unwrap();
        let doc_id = idx.add_document(doc).await.unwrap();
        assert!(doc_id > 0);

        let got = idx.get_document(doc_id).await.unwrap().unwrap();
        assert_eq!(got.get("title").unwrap(), &serde_json::json!("hello"));
    }

    #[tokio::test]
    async fn test_reopen_preserves_docs() {
        let dir = tempdir().unwrap();
        let doc_id = {
            let idx = Index::create(dir.path(), IndexConfig::default()).await.unwrap();
            let doc = SchemalessDoc::from_json(r#"{"title":"first"}"#).unwrap();
            let id = idx.add_document(doc).await.unwrap();
            idx.commit().await.unwrap();
            id
        };

        let idx = Index::open(dir.path()).await.unwrap();
        let got = idx.get_document(doc_id).await.unwrap().unwrap();
        assert_eq!(got.get("title").unwrap(), &serde_json::json!("first"));
    }

    #[tokio::test]
    async fn test_delete_document() {
        let dir = tempdir().unwrap();
        let idx = Index::create(dir.path(), IndexConfig::default()).await.unwrap();
        let doc = SchemalessDoc::from_json(r#"{"title":"x"}"#).unwrap();
        let doc_id = idx.add_document(doc).await.unwrap();

        idx.delete_document(doc_id).await.unwrap();
        let got = idx.get_document(doc_id).await.unwrap();
        assert!(got.is_none(), "expected None after delete, got {:?}", got);
    }
}
