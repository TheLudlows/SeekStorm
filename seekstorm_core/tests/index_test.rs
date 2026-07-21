//! Phase 2 集成测试：Schemaless + 文档存储。
//!
//! 对应 V4_PHASED_IMPLEMENTATION.md §Phase 2 测试用例。

use seekstorm_core::{DynamicSchema, FieldType, Index, IndexConfig, SchemalessDoc};
use tempfile::tempdir;

#[tokio::test]
async fn test_schemaless_inference() {
    let schema = DynamicSchema::new();
    let doc =
        SchemalessDoc::from_json(r#"{"title":"hello","count":42,"tags":["a","b"]}"#).unwrap();
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
async fn test_add_and_get_document() {
    let dir = tempdir().unwrap();
    let index = Index::create(dir.path(), IndexConfig::default())
        .await
        .unwrap();
    let doc_id = index
        .add_document(SchemalessDoc::from_json(r#"{"title":"hello"}"#).unwrap())
        .await
        .unwrap();

    let doc = index.get_document(doc_id).await.unwrap().unwrap();
    assert_eq!(doc.get("title").unwrap(), &serde_json::json!("hello"));
}

#[tokio::test]
async fn test_schemaless_different_fields() {
    let dir = tempdir().unwrap();
    let index = Index::create(dir.path(), IndexConfig::default())
        .await
        .unwrap();
    index
        .add_document(SchemalessDoc::from_json(r#"{"title":"a"}"#).unwrap())
        .await
        .unwrap();
    index
        .add_document(SchemalessDoc::from_json(r#"{"body":"b","tags":["x"]}"#).unwrap())
        .await
        .unwrap();

    let schema = index.schema.read().await;
    assert!(schema.get_field("title").await.is_some());
    assert!(schema.get_field("body").await.is_some());
    assert!(schema.get_field("tags").await.is_some());
}

#[tokio::test]
async fn test_type_conflict() {
    let dir = tempdir().unwrap();
    let index = Index::create(dir.path(), IndexConfig::default())
        .await
        .unwrap();
    index
        .add_document(SchemalessDoc::from_json(r#"{"count":42}"#).unwrap())
        .await
        .unwrap();

    let result = index
        .add_document(SchemalessDoc::from_json(r#"{"count":"text"}"#).unwrap())
        .await;
    assert!(result.is_err(), "expected type conflict error, got Ok");

    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("schema conflict") && err.contains("count"),
        "expected conflict on 'count', got: {}",
        err
    );
}

#[tokio::test]
async fn test_schema_persisted_to_meta_namespace() {
    // 验收标准：Schema 变更持久化到 Meta 命名空间。
    let dir = tempdir().unwrap();
    {
        let index = Index::create(dir.path(), IndexConfig::default())
            .await
            .unwrap();
        index
            .add_document(SchemalessDoc::from_json(r#"{"title":"x","count":1}"#).unwrap())
            .await
            .unwrap();
        index.commit().await.unwrap();
    }

    // 重开后 schema 应已恢复。
    let index = Index::open(dir.path()).await.unwrap();
    let schema = index.schema.read().await;
    assert!(schema.get_field("title").await.is_some());
    assert!(schema.get_field("count").await.is_some());
}

#[tokio::test]
async fn test_doc_id_monotonic_after_reopen() {
    // 重开后 next_doc_id 应大于已存在的最大 doc_id。
    let dir = tempdir().unwrap();
    let first_id = {
        let index = Index::create(dir.path(), IndexConfig::default())
            .await
            .unwrap();
        let id = index
            .add_document(SchemalessDoc::from_json(r#"{"x":1}"#).unwrap())
            .await
            .unwrap();
        index.commit().await.unwrap();
        id
    };

    let index = Index::open(dir.path()).await.unwrap();
    let second_id = index
        .add_document(SchemalessDoc::from_json(r#"{"x":2}"#).unwrap())
        .await
        .unwrap();
    assert!(
        second_id > first_id,
        "expected second_id > first_id ({} > {})",
        second_id,
        first_id
    );
}

#[tokio::test]
async fn test_vector_field_inferred() {
    // 附加测试：向量字段被推断为 Vector(dim, Cosine)。
    let dir = tempdir().unwrap();
    let index = Index::create(dir.path(), IndexConfig::default())
        .await
        .unwrap();
    index
        .add_document(
            SchemalessDoc::from_json(r#"{"title":"x","vec":[1.0,2.0,3.0,4.0]}"#).unwrap(),
        )
        .await
        .unwrap();

    let schema = index.schema.read().await;
    let vec_meta = schema.get_field("vec").await.unwrap();
    match &vec_meta.data_type {
        FieldType::Vector(dim, sim) => {
            assert_eq!(*dim, 4);
            assert!(matches!(sim, seekstorm_core::VectorSimilarity::Cosine));
        }
        other => panic!("expected Vector, got {:?}", other),
    }
    assert!(vec_meta.index_vector);
    assert_eq!(vec_meta.vector_dim, Some(4));
}

#[tokio::test]
async fn test_create_rejects_non_empty_dir() {
    let dir = tempdir().unwrap();
    let idx = Index::create(dir.path(), IndexConfig::default())
        .await
        .unwrap();
    idx.add_document(SchemalessDoc::from_json(r#"{"a":1}"#).unwrap())
        .await
        .unwrap();

    let result = Index::create(dir.path(), IndexConfig::default()).await;
    assert!(result.is_err(), "expected create to reject non-empty dir");
}
