//! Phase 3 集成测试：词法索引（tantivy 分词器 + 自研 LSM 倒排）。
//!
//! 验证：
//! - 插入文档后词法检索可用
//! - 删除后检索结果正确（lazy 删除）
//! - 重启后索引可用
//! - 多字段/多 term 查询

use seekstorm_core::{Index, IndexConfig, SchemalessDoc};
use tempfile::tempdir;

#[tokio::test]
async fn test_lexical_basic_search() {
    let dir = tempdir().unwrap();
    let index = Index::create(dir.path(), IndexConfig::default()).await.unwrap();

    index
        .add_document(SchemalessDoc::from_json(r#"{"title":"hello world"}"#).unwrap())
        .await
        .unwrap();
    index
        .add_document(SchemalessDoc::from_json(r#"{"title":"foo bar"}"#).unwrap())
        .await
        .unwrap();
    index.commit().await.unwrap();

    let results = index.lexical_search("hello", 10).await.unwrap();
    assert_eq!(results.len(), 1, "expected 1 result for 'hello'");
    assert_eq!(results[0].doc_id, 1);
}

#[tokio::test]
async fn test_lexical_field_query() {
    let dir = tempdir().unwrap();
    let index = Index::create(dir.path(), IndexConfig::default()).await.unwrap();

    index
        .add_document(SchemalessDoc::from_json(r#"{"title":"hello","body":"world"}"#).unwrap())
        .await
        .unwrap();
    index
        .add_document(SchemalessDoc::from_json(r#"{"title":"world","body":"hello"}"#).unwrap())
        .await
        .unwrap();
    index.commit().await.unwrap();

    // 指定字段查询
    let results = index.lexical_search("title:hello", 10).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].doc_id, 1);

    let results = index.lexical_search("body:hello", 10).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].doc_id, 2);
}

#[tokio::test]
async fn test_lexical_delete_lazy() {
    let dir = tempdir().unwrap();
    let index = Index::create(dir.path(), IndexConfig::default()).await.unwrap();

    let id1 = index
        .add_document(SchemalessDoc::from_json(r#"{"title":"hello"}"#).unwrap())
        .await
        .unwrap();
    let id2 = index
        .add_document(SchemalessDoc::from_json(r#"{"title":"hello"}"#).unwrap())
        .await
        .unwrap();
    index.commit().await.unwrap();

    // 删除 id1
    index.delete_document(id1).await.unwrap();
    index.commit().await.unwrap();

    // 检索应只返回 id2（lazy 删除过滤）
    let results = index.lexical_search("hello", 10).await.unwrap();
    assert_eq!(results.len(), 1, "expected 1 result after delete");
    assert_eq!(results[0].doc_id, id2);
    assert_ne!(results[0].doc_id, id1);
}

#[tokio::test]
async fn test_lexical_reopen_persistence() {
    let dir = tempdir().unwrap();
    {
        let index = Index::create(dir.path(), IndexConfig::default()).await.unwrap();
        index
            .add_document(SchemalessDoc::from_json(r#"{"title":"persisted"}"#).unwrap())
            .await
            .unwrap();
        index.commit().await.unwrap();
    }

    // 重启后索引可用
    let index = Index::open(dir.path()).await.unwrap();
    let results = index.lexical_search("persisted", 10).await.unwrap();
    assert_eq!(results.len(), 1);
}

#[tokio::test]
async fn test_lexical_multiple_terms() {
    let dir = tempdir().unwrap();
    let index = Index::create(dir.path(), IndexConfig::default()).await.unwrap();

    index
        .add_document(SchemalessDoc::from_json(r#"{"title":"hello world foo"}"#).unwrap())
        .await
        .unwrap();
    index
        .add_document(SchemalessDoc::from_json(r#"{"title":"hello bar"}"#).unwrap())
        .await
        .unwrap();
    index
        .add_document(SchemalessDoc::from_json(r#"{"title":"world baz"}"#).unwrap())
        .await
        .unwrap();
    index.commit().await.unwrap();

    // 多 term AND 查询：只匹配同时包含 hello 和 world 的文档
    let results = index.lexical_search("hello world", 10).await.unwrap();
    assert_eq!(results.len(), 1, "expected 1 doc with both 'hello' and 'world'");
    assert_eq!(results[0].doc_id, 1);
}

#[tokio::test]
async fn test_lexical_scoring() {
    let dir = tempdir().unwrap();
    let index = Index::create(dir.path(), IndexConfig::default()).await.unwrap();

    // doc 1: "hello" 出现 1 次
    index
        .add_document(SchemalessDoc::from_json(r#"{"title":"hello"}"#).unwrap())
        .await
        .unwrap();
    // doc 2: "hello" 出现 3 次（更高 score）
    index
        .add_document(SchemalessDoc::from_json(r#"{"title":"hello hello hello"}"#).unwrap())
        .await
        .unwrap();
    index.commit().await.unwrap();

    let results = index.lexical_search("hello", 10).await.unwrap();
    assert_eq!(results.len(), 2);
    // doc 2 应排在前面（TF 更高）
    assert_eq!(results[0].doc_id, 2);
    assert!(results[0].score > results[1].score);
}

#[tokio::test]
async fn test_lexical_case_insensitive() {
    let dir = tempdir().unwrap();
    let index = Index::create(dir.path(), IndexConfig::default()).await.unwrap();

    index
        .add_document(SchemalessDoc::from_json(r#"{"title":"Hello World"}"#).unwrap())
        .await
        .unwrap();
    index.commit().await.unwrap();

    // 默认分词器应做 lowercase
    let results = index.lexical_search("hello", 10).await.unwrap();
    assert_eq!(results.len(), 1);

    let results = index.lexical_search("HELLO", 10).await.unwrap();
    assert_eq!(results.len(), 1);
}
