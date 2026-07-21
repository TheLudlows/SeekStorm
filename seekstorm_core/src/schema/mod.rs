//! 动态 Schema 子系统（Phase 2）。
//!
//! 在 LSM 之上提供按需字段推断：文档写入时若出现新字段，自动加入 schema；
//! 已存在字段则进行类型兼容性检查，冲突时返回错误。
//!
//! 持久化：通过 [`DynamicSchema::serialize`] / [`DynamicSchema::deserialize`]
//! 与 LSM Meta 命名空间交互。Schema 变更由 [`Index`]（Phase 2 §2.3）同步写回。

pub mod field;
pub mod inference;

pub use field::{FieldChange, FieldId, FieldMeta, FieldType, TokenizerType, VectorSimilarity};
pub use inference::infer_field_type;

use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::Result;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

/// 动态 schema。字段按名称查找，按插入顺序持久化。
pub struct DynamicSchema {
    fields: RwLock<IndexMap<String, FieldMeta>>,
    next_field_id: AtomicU32,
}

/// 序列化载体：把 `fields` 按插入顺序写成数组。
#[derive(Serialize, Deserialize)]
struct SchemaSnapshot {
    next_field_id: u32,
    fields: Vec<FieldMeta>,
}

impl DynamicSchema {
    /// 创建空 schema。
    pub fn new() -> Self {
        Self {
            fields: RwLock::new(IndexMap::new()),
            next_field_id: AtomicU32::new(0),
        }
    }

    /// 推断文档字段并加入 schema。返回本次新增字段列表（已有字段不重复加入）。
    /// 若同名字段类型与既有 schema 不兼容，返回错误。
    pub async fn infer_and_add(&self, doc: &super::index::document::SchemalessDoc) -> Result<Vec<FieldChange>> {
        let mut changes = Vec::new();
        let mut fields = self.fields.write().await;
        for (name, value) in doc.fields() {
            let inferred = infer_field_type(value);
            if let Some(existing) = fields.get(name) {
                if !existing.data_type.compatible(&inferred) {
                    anyhow::bail!(
                        "schema conflict on field '{}': existing {:?} vs new {:?}",
                        name,
                        existing.data_type,
                        inferred
                    );
                }
                continue;
            }
            let id = self.next_field_id.fetch_add(1, Ordering::SeqCst);
            let meta = FieldMeta::new(id, name.clone(), inferred);
            fields.insert(name.clone(), meta.clone());
            changes.push(FieldChange {
                name: name.clone(),
                meta,
            });
        }
        Ok(changes)
    }

    /// 按名查找字段元数据（克隆返回，避免持锁）。
    pub async fn get_field(&self, name: &str) -> Option<FieldMeta> {
        self.fields.read().await.get(name).cloned()
    }

    /// 当前所有字段元数据快照（按插入顺序）。
    pub async fn snapshot(&self) -> Vec<FieldMeta> {
        self.fields.read().await.values().cloned().collect()
    }

    /// 序列化为字节（JSON）。用于写入 LSM Meta 命名空间。
    pub async fn serialize(&self) -> Vec<u8> {
        let fields = self.fields.read().await;
        let snap = SchemaSnapshot {
            next_field_id: self.next_field_id.load(Ordering::SeqCst),
            fields: fields.values().cloned().collect(),
        };
        serde_json::to_vec(&snap).unwrap_or_else(|_| Vec::new())
    }

    /// 从字节反序列化。
    pub async fn deserialize(bytes: &[u8]) -> Result<Self> {
        let snap: SchemaSnapshot = serde_json::from_slice(bytes)?;
        let mut map: IndexMap<String, FieldMeta> = IndexMap::with_capacity(snap.fields.len());
        for meta in snap.fields {
            map.insert(meta.name.clone(), meta);
        }
        Ok(Self {
            fields: RwLock::new(map),
            next_field_id: AtomicU32::new(snap.next_field_id),
        })
    }

    /// 字段数（测试用）。
    pub async fn field_count(&self) -> usize {
        self.fields.read().await.len()
    }
}

impl Default for DynamicSchema {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::document::SchemalessDoc;

    #[tokio::test]
    async fn test_infer_and_add_basic() {
        let schema = DynamicSchema::new();
        let doc = SchemalessDoc::from_json(r#"{"title":"hello","count":42,"tags":["a","b"]}"#).unwrap();
        let changes = schema.infer_and_add(&doc).await.unwrap();
        assert_eq!(changes.len(), 3);

        let title = schema.get_field("title").await.unwrap();
        assert!(matches!(title.data_type, FieldType::Text));

        let count = schema.get_field("count").await.unwrap();
        assert!(matches!(count.data_type, FieldType::I64));

        let tags = schema.get_field("tags").await.unwrap();
        assert!(matches!(tags.data_type, FieldType::Array(_)));
    }

    #[tokio::test]
    async fn test_infer_and_add_no_duplicate() {
        let schema = DynamicSchema::new();
        let doc1 = SchemalessDoc::from_json(r#"{"title":"a"}"#).unwrap();
        let changes1 = schema.infer_and_add(&doc1).await.unwrap();
        assert_eq!(changes1.len(), 1);

        let doc2 = SchemalessDoc::from_json(r#"{"title":"b","body":"c"}"#).unwrap();
        let changes2 = schema.infer_and_add(&doc2).await.unwrap();
        assert_eq!(changes2.len(), 1); // title 已存在，只新增 body
        assert_eq!(changes2[0].name, "body");
    }

    #[tokio::test]
    async fn test_serialize_deserialize_roundtrip() {
        let schema = DynamicSchema::new();
        let doc = SchemalessDoc::from_json(r#"{"title":"x","count":1}"#).unwrap();
        schema.infer_and_add(&doc).await.unwrap();
        let bytes = schema.serialize().await;

        let restored = DynamicSchema::deserialize(&bytes).await.unwrap();
        assert_eq!(restored.field_count().await, 2);
        assert!(restored.get_field("title").await.is_some());
        assert!(restored.get_field("count").await.is_some());
    }
}
