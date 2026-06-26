//! index.rs 功能的集成测试模块
//!
//! 覆盖范围：
//! - 索引的创建与打开（create_index / open_index）
//! - 分词器类型（TokenizerType）各变体
//! - 词干提取器（Stemmer）
//! - 停用词（Stop words）与高频词（Frequent words）
//! - Ngram 索引
//! - 同义词（Synonyms）：双向与单向
//! - 文档生命周期：索引、获取、删除
//! - 提交（Commit）行为与计数器
//! - 清空索引（Clear index）
//! - 文档压缩（DocumentCompression）变体
//! - 词法相似度（LexicalSimilarity）变体
//! - 访问类型（AccessType）：Ram vs Mmap
//! - 短语查询、交集查询、排除查询
//! - 实时搜索（Realtime search，提交前可搜索）
//! - 数值字段类型
//! - version() 函数
//! - 空文档集与无结果搜索
//!
//! 注意：
//! - 使用独立的索引目录，避免与现有测试冲突
//! - 测试必须顺序执行（RUST_TEST_THREADS=1，见 .cargo/config.toml）

// 引入提交（Commit）trait，用于将未提交的文档持久化到索引
use seekstorm::commit::Commit;
// 引入索引核心类型和操作 trait
use seekstorm::index::{
    AccessType, Close, Clustering, DeleteDocument, DocumentCompression, FileType, FrequentwordType,
    IndexDocument, IndexDocuments, IndexMetaObject, LexicalSimilarity, NgramSet, StemmerType,
    StopwordType, Synonym, TokenizerType, create_index, open_index,
};
// 引入搜索相关类型
use seekstorm::search::{QueryRewriting, QueryType, ResultType, Search, SearchMode};
use std::{fs, path::Path};

/// 测试索引的存储路径（独立目录，避免与其他测试冲突）
const TEST_INDEX_PATH: &str = "tests/index_test_index_rs/";

/// 构建默认的索引元数据对象
///
/// 配置说明：
/// - `lexical_similarity: Bm25f` — 使用 BM25F 评分算法（字段加权 BM25）
/// - `tokenizer: UnicodeAlphanumeric` — Unicode 字母数字分词器（支持多语言）
/// - `stemmer: None` — 不使用词干提取
/// - `stop_words: None` — 不使用停用词过滤
/// - `frequent_words: English` — 使用英文高频词列表（用于 ngram 加速）
/// - `ngram_indexing: NgramFF | NgramFFF` — 启用二元和三元 ngram 索引
/// - `document_compression: Snappy` — 使用 Snappy 压缩存储文档
/// - `access_type: Mmap` — 使用内存映射文件方式访问索引数据
fn default_meta() -> IndexMetaObject {
    IndexMetaObject {
        id: 0,
        name: "test_index".into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::UnicodeAlphanumeric,
        stemmer: StemmerType::None,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::English,
        ngram_indexing: NgramSet::NgramFF as u8 | NgramSet::NgramFFF as u8,
        document_compression: DocumentCompression::Snappy,
        access_type: AccessType::Mmap,
        spelling_correction: None,
        query_completion: None,
        clustering: Clustering::None,
        inference: seekstorm::vector::Inference::None,
    }
}

/// 构建默认的索引 Schema
///
/// 字段定义：
/// - `title`：Text 类型，存储原文，建立词法索引，标记为最长字段（longest=true，用于标题高亮等）
/// - `body`：Text 类型，存储原文，建立词法索引
/// - `url`：Text 类型，不存储原文，不建立词法索引（仅作为元数据）
fn default_schema() -> Vec<seekstorm::index::SchemaField> {
    let schema_json = r#"
    [{"field":"title","field_type":"Text","store":true,"index_lexical":true,"longest":true},
    {"field":"body","field_type":"Text","store":true,"index_lexical":true},
    {"field":"url","field_type":"Text","store":false,"index_lexical":false}]"#;
    serde_json::from_str(schema_json).unwrap()
}

// ============================================================
// 1. 索引创建与打开基础功能
// ============================================================

/// 测试使用默认参数创建索引
///
/// 验证点：
/// - 索引元数据（id、name）正确保存
/// - 默认分片数等于 CPU 核心数
/// - 初始已索引文档数为 0
#[tokio::test]
async fn test_create_index_default() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    // create_index 参数：路径、元数据、Schema、同义词列表、种子端口、是否实时、自定义分片数
    let index_arc = create_index(
        index_path,
        default_meta(),
        &default_schema(),
        &Vec::new(),
        11,
        false,
        None, // None 表示分片数默认为 CPU 核心数
    )
    .await
    .unwrap();

    let index = index_arc.read().await;
    assert_eq!(index.meta.id, 0);
    assert_eq!(index.meta.name, "test_index");
    assert_eq!(index.shard_count().await, num_cpus::get());
    assert_eq!(index.indexed_doc_count().await, 0);

    index_arc.close().await;
}

/// 测试创建索引时指定自定义分片数
///
/// 验证点：
/// - 传入 Some(2) 后，分片数应为 2 而非默认的 CPU 核心数
#[tokio::test]
async fn test_create_index_custom_shard_count() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let index_arc = create_index(
        index_path,
        default_meta(),
        &default_schema(),
        &Vec::new(),
        11,
        false,
        Some(2), // 自定义分片数为 2
    )
    .await
    .unwrap();

    assert_eq!(index_arc.read().await.shard_count().await, 2);
    index_arc.close().await;
}

/// 测试关闭后重新打开索引时，元数据能够正确持久化
///
/// 验证点：
/// - 创建索引时设置 id=42, name="persist_test"
/// - 关闭索引后重新打开
/// - 元数据 id 和 name 仍然正确
#[tokio::test]
async fn test_open_index_persists_meta() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let meta = IndexMetaObject {
        id: 42,
        name: "persist_test".into(),
        ..default_meta()
    };

    let index_arc = create_index(
        index_path,
        meta,
        &default_schema(),
        &Vec::new(),
        11,
        false,
        None,
    )
    .await
    .unwrap();
    index_arc.close().await;

    // 重新打开已关闭的索引
    let reopened = open_index(index_path).await.unwrap();
    let index = reopened.read().await;
    assert_eq!(index.meta.id, 42);
    assert_eq!(index.meta.name, "persist_test");

    reopened.close().await;
}

/// 测试打开不存在的索引路径时应返回错误
///
/// 验证点：
/// - 对不存在的路径调用 open_index 应返回 Err
#[tokio::test]
async fn test_open_nonexistent_index_fails() {
    let index_path = Path::new("tests/nonexistent_index/");
    let result = open_index(index_path).await;
    assert!(result.is_err());
}

// ============================================================
// 2. 分词器类型变体测试
// ============================================================

/// 测试 AsciiAlphabetic 分词器
///
/// 行为：仅保留 ASCII 字母，去除数字和非字母字符
/// 验证点：
/// - "Hello World 123" → 仅索引 "hello" 和 "world"，数字 "123" 被去除
/// - 搜索 "hello" 能找到文档
/// - 搜索 "123" 找不到文档
#[tokio::test]
async fn test_tokenizer_ascii_alphabetic() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let meta = IndexMetaObject {
        tokenizer: TokenizerType::AsciiAlphabetic,
        ..default_meta()
    };
    let index_arc = create_index(index_path, meta, &default_schema(), &Vec::new(), 11, false, None)
        .await
        .unwrap();

    let doc = serde_json::from_str(r#"{"title":"Hello World 123","body":"test","url":"u1"}"#).unwrap();
    index_arc.index_document(doc, FileType::None).await;
    index_arc.commit().await;

    // "123" 被 AsciiAlphabetic 分词器去除，只有 "hello" 和 "world" 被索引
    let result = index_arc
        .search("hello".into(), None, QueryType::Union, SearchMode::Lexical, false, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 1);

    let result = index_arc
        .search("123".into(), None, QueryType::Union, SearchMode::Lexical, false, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 0);

    index_arc.close().await;
}

/// 测试 Whitespace 分词器
///
/// 行为：仅以空白字符为分隔符，连字符等标点不会拆分 token
/// 验证点：
/// - "Hello-World" 被视为一个完整 token（不在连字符处拆分）
/// - 搜索 "hello-world" 能找到文档
#[tokio::test]
async fn test_tokenizer_whitespace() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let meta = IndexMetaObject {
        tokenizer: TokenizerType::Whitespace,
        ..default_meta()
    };
    let index_arc = create_index(index_path, meta, &default_schema(), &Vec::new(), 11, false, None)
        .await
        .unwrap();

    let doc = serde_json::from_str(r#"{"title":"Hello-World","body":"test","url":"u1"}"#).unwrap();
    index_arc.index_document(doc, FileType::None).await;
    index_arc.commit().await;

    // Whitespace 分词器将 "Hello-World" 视为一个完整 token（不在连字符处拆分）
    let result = index_arc
        .search("hello-world".into(), None, QueryType::Union, SearchMode::Lexical, false, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 1);

    index_arc.close().await;
}

/// 测试 UnicodeAlphanumericFolded 分词器
///
/// 行为：Unicode 字母数字分词 + Unicode 折叠（如变音符号去除）
/// 验证点：
/// - "café" 被折叠为 "cafe"
/// - 搜索 "cafe"（无变音符号）仍能找到包含 "café" 的文档
#[tokio::test]
async fn test_tokenizer_unicode_alphanumeric_folded() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let meta = IndexMetaObject {
        tokenizer: TokenizerType::UnicodeAlphanumericFolded,
        ..default_meta()
    };
    let index_arc = create_index(index_path, meta, &default_schema(), &Vec::new(), 11, false, None)
        .await
        .unwrap();

    // "café" 应被折叠为 "cafe"，搜索 "cafe" 也能匹配
    let doc = serde_json::from_str(r#"{"title":"café au lait","body":"test","url":"u1"}"#).unwrap();
    index_arc.index_document(doc, FileType::None).await;
    index_arc.commit().await;

    let result = index_arc
        .search("cafe".into(), None, QueryType::Union, SearchMode::Lexical, false, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 1);

    index_arc.close().await;
}

// ============================================================
// 3. 词干提取器（Stemmer）
// ============================================================

/// 测试英文词干提取器
///
/// 行为：英文词干提取将词汇还原为词根形式
/// 验证点：
/// - "running" → 词干 "run"，搜索 "run" 可匹配
/// - "cats" → 词干 "cat"，搜索 "cat" 可匹配
#[tokio::test]
async fn test_stemmer_english() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let meta = IndexMetaObject {
        stemmer: StemmerType::English,
        ..default_meta()
    };
    let index_arc = create_index(index_path, meta, &default_schema(), &Vec::new(), 11, false, None)
        .await
        .unwrap();

    let doc = serde_json::from_str(r#"{"title":"running cats","body":"test","url":"u1"}"#).unwrap();
    index_arc.index_document(doc, FileType::None).await;
    index_arc.commit().await;

    // "running" 词干为 "run"；"cats" 词干为 "cat"
    let result = index_arc
        .search("run".into(), None, QueryType::Union, SearchMode::Lexical, false, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 1);

    let result = index_arc
        .search("cat".into(), None, QueryType::Union, SearchMode::Lexical, false, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 1);

    index_arc.close().await;
}

// ============================================================
// 4. 停用词与高频词
// ============================================================

/// 测试英文停用词过滤
///
/// 行为：启用英文停用词后，常见虚词（如 "the"、"a"、"is"）不会被索引
/// 验证点：
/// - "the" 是英文停用词，搜索应返回 0 结果
/// - "quick" 不是停用词，搜索应返回 1 结果
#[tokio::test]
async fn test_stop_words_english() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let meta = IndexMetaObject {
        stop_words: StopwordType::English,
        ..default_meta()
    };
    let index_arc = create_index(index_path, meta, &default_schema(), &Vec::new(), 11, false, None)
        .await
        .unwrap();

    let doc = serde_json::from_str(r#"{"title":"the quick fox","body":"test","url":"u1"}"#).unwrap();
    index_arc.index_document(doc, FileType::None).await;
    index_arc.commit().await;

    // "the" 是停用词，搜索应返回 0 结果
    let result = index_arc
        .search("the".into(), None, QueryType::Union, SearchMode::Lexical, false, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 0);

    // "quick" 不是停用词，搜索应返回 1 结果
    let result = index_arc
        .search("quick".into(), None, QueryType::Union, SearchMode::Lexical, false, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 1);

    index_arc.close().await;
}

/// 测试自定义停用词
///
/// 行为：用户可指定自定义停用词列表
/// 验证点：
/// - "apple" 和 "banana" 是自定义停用词，搜索应返回 0 结果
/// - "orange" 不在停用词列表中，搜索应返回 1 结果
#[tokio::test]
async fn test_stop_words_custom() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let meta = IndexMetaObject {
        stop_words: StopwordType::Custom {
            terms: vec!["apple".into(), "banana".into()],
        },
        ..default_meta()
    };
    let index_arc = create_index(index_path, meta, &default_schema(), &Vec::new(), 11, false, None)
        .await
        .unwrap();

    let doc = serde_json::from_str(r#"{"title":"apple orange banana","body":"test","url":"u1"}"#).unwrap();
    index_arc.index_document(doc, FileType::None).await;
    index_arc.commit().await;

    // 自定义停用词 "apple" 和 "banana" 被过滤
    let result = index_arc
        .search("apple".into(), None, QueryType::Union, SearchMode::Lexical, false, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 0);

    let result = index_arc
        .search("orange".into(), None, QueryType::Union, SearchMode::Lexical, false, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 1);

    index_arc.close().await;
}

/// 测试高频词设为 None 的行为
///
/// 行为：禁用高频词列表后，所有词都可被正常索引和搜索
/// 验证点：
/// - 没有高频词过滤时，"the" 作为普通词被索引
/// - 搜索 "the" 应返回 1 结果
#[tokio::test]
async fn test_frequent_words_none() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let meta = IndexMetaObject {
        frequent_words: FrequentwordType::None,
        ngram_indexing: NgramSet::NgramFF as u8,
        ..default_meta()
    };
    let index_arc = create_index(index_path, meta, &default_schema(), &Vec::new(), 11, false, None)
        .await
        .unwrap();

    let doc = serde_json::from_str(r#"{"title":"the quick fox","body":"test","url":"u1"}"#).unwrap();
    index_arc.index_document(doc, FileType::None).await;
    index_arc.commit().await;

    // 无高频词列表时，"the" 作为普通词被索引，搜索返回 1 结果
    let result = index_arc
        .search("the".into(), None, QueryType::Union, SearchMode::Lexical, false, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 1);

    index_arc.close().await;
}

// ============================================================
// 5. Ngram 索引
// ============================================================

/// 测试仅启用 SingleTerm 模式的 ngram 索引
///
/// 行为：SingleTerm 仅支持单词条搜索，不构建多词 ngram
/// 验证点：
/// - 单词条搜索 "quick" 仍能正常工作
#[tokio::test]
async fn test_ngram_single_term_only() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let meta = IndexMetaObject {
        ngram_indexing: NgramSet::SingleTerm as u8,
        ..default_meta()
    };
    let index_arc = create_index(index_path, meta, &default_schema(), &Vec::new(), 11, false, None)
        .await
        .unwrap();

    let doc = serde_json::from_str(r#"{"title":"the quick fox","body":"test","url":"u1"}"#).unwrap();
    index_arc.index_document(doc, FileType::None).await;
    index_arc.commit().await;

    // SingleTerm 模式下，单词条搜索应正常工作
    let result = index_arc
        .search("quick".into(), None, QueryType::Union, SearchMode::Lexical, false, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 1);

    index_arc.close().await;
}

/// 测试启用所有 ngram 类型的索引
///
/// Ngram 类型说明：
/// - NgramFF：前缀+前缀（两词前缀 ngram）
/// - NgramFR：前缀+后缀
/// - NgramRF：后缀+前缀
/// - NgramFFF：三词前缀 ngram
/// - NgramRFF、NgramFFR、NgramFRF：混合三词 ngram
///
/// 验证点：
/// - 启用全量 ngram 后，短语搜索 "the quick" 可通过 F+F ngram 匹配
#[tokio::test]
async fn test_ngram_all_types() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let meta = IndexMetaObject {
        ngram_indexing: NgramSet::NgramFF as u8
            | NgramSet::NgramFR as u8
            | NgramSet::NgramRF as u8
            | NgramSet::NgramFFF as u8
            | NgramSet::NgramRFF as u8
            | NgramSet::NgramFFR as u8
            | NgramSet::NgramFRF as u8,
        ..default_meta()
    };
    let index_arc = create_index(index_path, meta, &default_schema(), &Vec::new(), 11, false, None)
        .await
        .unwrap();

    let doc = serde_json::from_str(r#"{"title":"the quick brown fox","body":"test","url":"u1"}"#).unwrap();
    index_arc.index_document(doc, FileType::None).await;
    index_arc.commit().await;

    // 短语搜索 "the quick" 应通过 F+F ngram 匹配
    let result = index_arc
        .search("\"the quick\"".into(), None, QueryType::Phrase, SearchMode::Lexical, false, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 1);

    index_arc.close().await;
}

// ============================================================
// 6. 同义词（Synonyms）
// ============================================================

/// 测试双向同义词（multiway=true）
///
/// 行为：双向同义词中，所有词互相等价
/// 验证点：
/// - 文档包含 "avenue"，搜索 "street" 也能找到（street ↔ avenue ↔ road）
/// - 搜索 "road" 也能找到同一文档
#[tokio::test]
async fn test_synonyms_multiway() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    // 定义双向同义词组：street、avenue、road 互相等价
    let synonyms = vec![Synonym {
        terms: vec!["street".into(), "avenue".into(), "road".into()],
        multiway: true,
    }];

    let index_arc = create_index(
        index_path,
        default_meta(),
        &default_schema(),
        &synonyms,
        11,
        false,
        None,
    )
    .await
    .unwrap();

    let doc = serde_json::from_str(r#"{"title":"fifth avenue","body":"test","url":"u1"}"#).unwrap();
    index_arc.index_document(doc, FileType::None).await;
    index_arc.commit().await;

    // 双向同义词：搜索 "street" 能找到包含 "avenue" 的文档
    let result = index_arc
        .search("street".into(), None, QueryType::Union, SearchMode::Lexical, false, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 1);

    // 搜索 "road" 也能找到同一文档
    let result = index_arc
        .search("road".into(), None, QueryType::Union, SearchMode::Lexical, false, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 1);

    index_arc.close().await;
}

/// 测试单向同义词（multiway=false）
///
/// 行为：单向同义词中，只有列表中的后续词可扩展到前面的词
/// 验证点：
/// - 搜索 "street" 能找到包含 "avenue" 的文档（street → avenue 方向扩展）
/// - 直接搜索 "avenue" 也能找到文档（原始词直接匹配）
#[tokio::test]
async fn test_synonyms_oneway() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    // 定义单向同义词组：只有 street → avenue/road 方向扩展
    let synonyms = vec![Synonym {
        terms: vec!["street".into(), "avenue".into(), "road".into()],
        multiway: false,
    }];

    let index_arc = create_index(
        index_path,
        default_meta(),
        &default_schema(),
        &synonyms,
        11,
        false,
        None,
    )
    .await
    .unwrap();

    let doc = serde_json::from_str(r#"{"title":"fifth avenue","body":"test","url":"u1"}"#).unwrap();
    index_arc.index_document(doc, FileType::None).await;
    index_arc.commit().await;

    // 单向：搜索 "street" 可扩展到 "avenue"，能找到文档
    let result = index_arc
        .search("street".into(), None, QueryType::Union, SearchMode::Lexical, false, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 1);

    // 直接搜索 "avenue" 仍能找到文档（原始词直接匹配，无需同义词扩展）
    let result = index_arc
        .search("avenue".into(), None, QueryType::Union, SearchMode::Lexical, false, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 1);

    index_arc.close().await;
}

// ============================================================
// 7. 文档生命周期：索引、获取、删除
// ============================================================

/// 测试索引单篇文档并通过 get_document 获取
///
/// 验证点：
/// - 文档被正确索引并存储
/// - 通过 doc_id=0 获取文档，title 和 body 字段值与原始输入一致
#[tokio::test]
async fn test_index_and_get_document() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let index_arc = create_index(index_path, default_meta(), &default_schema(), &Vec::new(), 11, false, None)
        .await
        .unwrap();

    let doc = serde_json::from_str(r#"{"title":"hello world","body":"doc body text","url":"u1"}"#).unwrap();
    index_arc.index_document(doc, FileType::None).await;
    index_arc.commit().await;

    // 通过 doc_id 获取文档内容
    let index = index_arc.read().await;
    let result = index
        .get_document(0, false, &None, &std::collections::HashSet::new(), &Vec::new())
        .await
        .unwrap();

    let title = result.get("title").unwrap().to_owned();
    assert_eq!(serde_json::from_value::<String>(title).unwrap(), "hello world");

    let body = result.get("body").unwrap().to_owned();
    assert_eq!(serde_json::from_value::<String>(body).unwrap(), "doc body text");

    index_arc.close().await;
}

/// 测试批量索引多篇文档
///
/// 验证点：
/// - 使用 index_documents 批量索引 3 篇文档
/// - indexed_doc_count 应为 3
#[tokio::test]
async fn test_index_multiple_documents() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let index_arc = create_index(index_path, default_meta(), &default_schema(), &Vec::new(), 11, false, None)
        .await
        .unwrap();

    let docs_json = r#"
    [{"title":"doc alpha","body":"first document","url":"u1"},
    {"title":"doc beta","body":"second document","url":"u2"},
    {"title":"doc gamma","body":"third document","url":"u3"}]"#;
    let docs: Vec<seekstorm::index::Document> = serde_json::from_str(docs_json).unwrap();
    index_arc.index_documents(docs).await;
    index_arc.commit().await;

    assert_eq!(index_arc.read().await.indexed_doc_count().await, 3);

    index_arc.close().await;
}

/// 测试删除文档
///
/// 验证点：
/// - 索引文档后，indexed_doc_count 为 1
/// - 删除文档（doc_id=0）后，current_doc_count 应为 0
/// - 搜索 "content" 应返回 0 结果（文档已被删除）
#[tokio::test]
async fn test_delete_document() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let index_arc = create_index(index_path, default_meta(), &default_schema(), &Vec::new(), 11, false, None)
        .await
        .unwrap();

    let doc = serde_json::from_str(r#"{"title":"to be deleted","body":"content","url":"u1"}"#).unwrap();
    index_arc.index_document(doc, FileType::None).await;
    index_arc.commit().await;

    assert_eq!(index_arc.read().await.indexed_doc_count().await, 1);

    // 删除 doc_id=0 的文档
    index_arc.delete_document(0).await;

    // 删除后，当前文档数应为 0
    assert_eq!(index_arc.read().await.current_doc_count().await, 0);

    // 搜索已删除文档的内容应返回 0 结果
    let result = index_arc
        .search("content".into(), None, QueryType::Union, SearchMode::Lexical, false, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 0);

    index_arc.close().await;
}

// ============================================================
// 8. 提交（Commit）与计数器行为
// ============================================================

/// 测试 commit 前后的文档计数器变化
///
/// 验证点：
/// - 索引文档但未提交时，uncommitted_doc_count >= 1
/// - 提交后，committed_doc_count >= 1
#[tokio::test]
async fn test_commit_persists_documents() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let index_arc = create_index(index_path, default_meta(), &default_schema(), &Vec::new(), 11, false, None)
        .await
        .unwrap();

    let doc = serde_json::from_str(r#"{"title":"persist test","body":"body","url":"u1"}"#).unwrap();
    index_arc.index_document(doc, FileType::None).await;

    // 提交前，未提交文档数 >= 1
    let uncommitted = index_arc.read().await.uncommitted_doc_count().await;
    assert!(uncommitted >= 1);

    index_arc.commit().await;

    // 提交后，已提交文档数 >= 1
    let committed = index_arc.read().await.committed_doc_count().await;
    assert!(committed >= 1);

    index_arc.close().await;
}

/// 测试索引和删除操作后的计数器一致性
///
/// 验证点：
/// - 索引 3 篇文档后：indexed_doc_count=3, current_doc_count=3
/// - 删除 1 篇后：indexed_doc_count 仍为 3（不减），current_doc_count=2（= indexed - deleted）
#[tokio::test]
async fn test_doc_counters_after_operations() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let index_arc = create_index(index_path, default_meta(), &default_schema(), &Vec::new(), 11, false, None)
        .await
        .unwrap();

    // 索引 3 篇文档
    let docs_json = r#"
    [{"title":"a","body":"body a","url":"u1"},
    {"title":"b","body":"body b","url":"u2"},
    {"title":"c","body":"body c","url":"u3"}]"#;
    let docs: Vec<seekstorm::index::Document> = serde_json::from_str(docs_json).unwrap();
    index_arc.index_documents(docs).await;
    index_arc.commit().await;

    assert_eq!(index_arc.read().await.indexed_doc_count().await, 3);
    assert_eq!(index_arc.read().await.current_doc_count().await, 3);

    // 删除 doc_id=1 的文档
    index_arc.delete_document(1).await;

    // indexed_doc_count 不减少（累计计数），current_doc_count = indexed - deleted
    assert_eq!(index_arc.read().await.indexed_doc_count().await, 3);
    assert_eq!(index_arc.read().await.current_doc_count().await, 2);

    index_arc.close().await;
}

/// 测试关闭并重新打开索引后数据仍然可用
///
/// 验证点：
/// - 索引文档并提交后关闭索引
/// - 重新打开后 indexed_doc_count 仍为 1
/// - 搜索 "persistent" 仍能找到文档
#[tokio::test]
async fn test_reopen_index_preserves_data() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let index_arc = create_index(index_path, default_meta(), &default_schema(), &Vec::new(), 11, false, None)
        .await
        .unwrap();

    let doc = serde_json::from_str(r#"{"title":"persistent doc","body":"body","url":"u1"}"#).unwrap();
    index_arc.index_document(doc, FileType::None).await;
    index_arc.commit().await;

    // 关闭后重新打开索引
    index_arc.close().await;
    let reopened = open_index(Path::new(TEST_INDEX_PATH)).await.unwrap();

    assert_eq!(reopened.read().await.indexed_doc_count().await, 1);

    // 重新打开后搜索仍能正常工作
    let result = reopened
        .search("persistent".into(), None, QueryType::Union, SearchMode::Lexical, false, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 1);

    reopened.close().await;
}

// ============================================================
// 9. 清空索引
// ============================================================

/// 测试清空索引后可重新索引新文档
///
/// 验证点：
/// - 索引 2 篇文档后，indexed_doc_count=2
/// - 调用 clear_index() 后，indexed_doc_count=0
/// - 清空后仍可索引新文档，indexed_doc_count=1
#[tokio::test]
async fn test_clear_index() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let index_arc = create_index(index_path, default_meta(), &default_schema(), &Vec::new(), 11, false, None)
        .await
        .unwrap();

    let docs_json = r#"
    [{"title":"a","body":"body a","url":"u1"},
    {"title":"b","body":"body b","url":"u2"}]"#;
    let docs: Vec<seekstorm::index::Document> = serde_json::from_str(docs_json).unwrap();
    index_arc.index_documents(docs).await;
    index_arc.commit().await;

    assert_eq!(index_arc.read().await.indexed_doc_count().await, 2);

    // 清空索引
    index_arc.write().await.clear_index().await;
    assert_eq!(index_arc.read().await.indexed_doc_count().await, 0);

    // 清空后索引新文档
    let doc = serde_json::from_str(r#"{"title":"new doc","body":"new body","url":"u3"}"#).unwrap();
    index_arc.index_document(doc, FileType::None).await;
    index_arc.commit().await;

    assert_eq!(index_arc.read().await.indexed_doc_count().await, 1);

    index_arc.close().await;
}

// ============================================================
// 10. 文档压缩变体
// ============================================================

/// 测试 Zstd 压缩存储文档
///
/// 验证点：
/// - 使用 Zstd 压缩后，文档仍可正确存储和检索
/// - get_document 返回的 title 字段值正确
#[tokio::test]
async fn test_document_compression_zstd() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let meta = IndexMetaObject {
        document_compression: DocumentCompression::Zstd,
        ..default_meta()
    };
    let index_arc = create_index(index_path, meta, &default_schema(), &Vec::new(), 11, false, None)
        .await
        .unwrap();

    let doc = serde_json::from_str(r#"{"title":"zstd test","body":"compressed body","url":"u1"}"#).unwrap();
    index_arc.index_document(doc, FileType::None).await;
    index_arc.commit().await;

    let index = index_arc.read().await;
    let result = index
        .get_document(0, false, &None, &std::collections::HashSet::new(), &Vec::new())
        .await
        .unwrap();
    let title = serde_json::from_value::<String>(result.get("title").unwrap().to_owned()).unwrap();
    assert_eq!(title, "zstd test");

    index_arc.close().await;
}

/// 测试无压缩存储文档
///
/// 验证点：
/// - 不使用压缩时，文档仍可正确存储和检索
/// - get_document 返回的 title 字段值正确
#[tokio::test]
async fn test_document_compression_none() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let meta = IndexMetaObject {
        document_compression: DocumentCompression::None,
        ..default_meta()
    };
    let index_arc = create_index(index_path, meta, &default_schema(), &Vec::new(), 11, false, None)
        .await
        .unwrap();

    let doc = serde_json::from_str(r#"{"title":"no compression","body":"raw body","url":"u1"}"#).unwrap();
    index_arc.index_document(doc, FileType::None).await;
    index_arc.commit().await;

    let index = index_arc.read().await;
    let result = index
        .get_document(0, false, &None, &std::collections::HashSet::new(), &Vec::new())
        .await
        .unwrap();
    let title = serde_json::from_value::<String>(result.get("title").unwrap().to_owned()).unwrap();
    assert_eq!(title, "no compression");

    index_arc.close().await;
}

// ============================================================
// 11. 词法相似度变体
// ============================================================

/// 测试 Bm25fProximity 评分算法
///
/// 行为：BM25F Proximity 在 BM25F 基础上增加了词距（proximity）加权，
///       查询词在文档中越接近，评分越高
/// 验证点：
/// - 使用 Bm25fProximity 模式创建索引，搜索仍能正常返回结果
#[tokio::test]
async fn test_lexical_similarity_bm25f_proximity() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let meta = IndexMetaObject {
        lexical_similarity: LexicalSimilarity::Bm25fProximity,
        ..default_meta()
    };
    let index_arc = create_index(index_path, meta, &default_schema(), &Vec::new(), 11, false, None)
        .await
        .unwrap();

    let doc = serde_json::from_str(r#"{"title":"proximity test","body":"body","url":"u1"}"#).unwrap();
    index_arc.index_document(doc, FileType::None).await;
    index_arc.commit().await;

    let result = index_arc
        .search("proximity".into(), None, QueryType::Union, SearchMode::Lexical, false, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 1);

    index_arc.close().await;
}

// ============================================================
// 12. 访问类型：AccessType::Ram
// ============================================================

/// 测试纯内存访问模式（AccessType::Ram）
///
/// 行为：与 Mmap 模式不同，Ram 模式将索引数据完全加载到内存中
/// 验证点：
/// - 使用 Ram 模式创建索引并索引文档后，搜索仍能正常工作
#[tokio::test]
async fn test_access_type_ram() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let meta = IndexMetaObject {
        access_type: AccessType::Ram,
        ..default_meta()
    };
    let index_arc = create_index(index_path, meta, &default_schema(), &Vec::new(), 11, false, None)
        .await
        .unwrap();

    let doc = serde_json::from_str(r#"{"title":"ram access","body":"body","url":"u1"}"#).unwrap();
    index_arc.index_document(doc, FileType::None).await;
    index_arc.commit().await;

    let result = index_arc
        .search("ram".into(), None, QueryType::Union, SearchMode::Lexical, false, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 1);

    index_arc.close().await;
}

// ============================================================
// 13. 短语查询（Phrase Query）
// ============================================================

/// 测试短语查询的词序敏感性
///
/// 行为：短语查询要求查询词在文档中按指定顺序相邻出现
/// 验证点：
/// - 短语 "new york" 只匹配 "new york city"（doc_id=0）
/// - 不匹配 "york new"（词序相反，doc_id=1）
#[tokio::test]
async fn test_phrase_query() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let meta = IndexMetaObject {
        ngram_indexing: NgramSet::NgramFF as u8 | NgramSet::NgramFFF as u8,
        ..default_meta()
    };
    let index_arc = create_index(index_path, meta, &default_schema(), &Vec::new(), 11, false, None)
        .await
        .unwrap();

    let docs_json = r#"
    [{"title":"new york city","body":"body1","url":"u1"},
    {"title":"york new","body":"body2","url":"u2"}]"#;
    let docs: Vec<seekstorm::index::Document> = serde_json::from_str(docs_json).unwrap();
    index_arc.index_documents(docs).await;
    index_arc.commit().await;

    // 短语 "new york" 只匹配 doc1（"new york city"），不匹配 doc2（"york new"）
    let result = index_arc
        .search("\"new york\"".into(), None, QueryType::Phrase, SearchMode::Lexical, false, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 1);
    assert_eq!(result.results[0].doc_id, 0);

    index_arc.close().await;
}

// ============================================================
// 14. 交集查询（AND / Intersection Query）
// ============================================================

/// 测试交集查询：要求所有查询词同时出现在同一文档中
///
/// 验证点：
/// - "+alpha +beta" 只匹配同时包含 "alpha" 和 "beta" 的文档（doc_id=0）
/// - 不匹配只包含其中一个词的文档
#[tokio::test]
async fn test_intersection_query() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let index_arc = create_index(index_path, default_meta(), &default_schema(), &Vec::new(), 11, false, None)
        .await
        .unwrap();

    let docs_json = r#"
    [{"title":"alpha beta","body":"body1","url":"u1"},
    {"title":"alpha gamma","body":"body2","url":"u2"},
    {"title":"beta gamma","body":"body3","url":"u3"}]"#;
    let docs: Vec<seekstorm::index::Document> = serde_json::from_str(docs_json).unwrap();
    index_arc.index_documents(docs).await;
    index_arc.commit().await;

    // "+alpha +beta" 只匹配同时包含两者的 doc1
    let result = index_arc
        .search("+alpha +beta".into(), None, QueryType::Intersection, SearchMode::Lexical, false, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 1);
    assert_eq!(result.results[0].doc_id, 0);

    index_arc.close().await;
}

// ============================================================
// 15. 排除查询（Not Query / 排除词）
// ============================================================

/// 测试排除查询：使用减号排除包含指定词的文档
///
/// 验证点：
/// - "alpha -beta" 匹配包含 "alpha" 但不包含 "beta" 的文档（doc_id=1）
/// - 排除了同时包含 "alpha" 和 "beta" 的文档（doc_id=0）
#[tokio::test]
async fn test_not_query() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let index_arc = create_index(index_path, default_meta(), &default_schema(), &Vec::new(), 11, false, None)
        .await
        .unwrap();

    let docs_json = r#"
    [{"title":"alpha beta","body":"body1","url":"u1"},
    {"title":"alpha gamma","body":"body2","url":"u2"}]"#;
    let docs: Vec<seekstorm::index::Document> = serde_json::from_str(docs_json).unwrap();
    index_arc.index_documents(docs).await;
    index_arc.commit().await;

    // "alpha -beta" 只匹配 doc2（含 alpha 但不含 beta）
    let result = index_arc
        .search("alpha -beta".into(), None, QueryType::Union, SearchMode::Lexical, false, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 1);
    assert_eq!(result.results[0].doc_id, 1);

    index_arc.close().await;
}

// ============================================================
// 16. 实时搜索（提交前可搜索）
// ============================================================

/// 测试实时搜索：在 commit 之前也能搜索到文档
///
/// 行为：search() 的 realtime 参数控制是否搜索未提交的文档
/// 验证点：
/// - 索引文档但未提交时，realtime=true 的搜索能找到文档（结果数为 1）
/// - realtime=false 的搜索找不到未提交的文档（结果数为 0）
#[tokio::test]
async fn test_realtime_search_before_commit() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let index_arc = create_index(index_path, default_meta(), &default_schema(), &Vec::new(), 11, false, None)
        .await
        .unwrap();

    let doc = serde_json::from_str(r#"{"title":"realtime doc","body":"body","url":"u1"}"#).unwrap();
    index_arc.index_document(doc, FileType::None).await;

    // 未提交时，realtime=true 的搜索能找到文档
    let result = index_arc
        .search("realtime".into(), None, QueryType::Union, SearchMode::Lexical, true, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 1);

    // realtime=false 时，未提交的文档不会被搜索到
    let result = index_arc
        .search("realtime".into(), None, QueryType::Union, SearchMode::Lexical, false, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 0);

    index_arc.commit().await;
    index_arc.close().await;
}

// ============================================================
// 17. Schema 中的字段类型变体
// ============================================================

/// 测试数值字段类型（如 U32）在 Schema 中的使用
///
/// 验证点：
/// - Schema 包含 U32 类型字段 "count" 时，索引创建和文档索引正常
/// - 数值字段不需要建立词法索引（index_lexical=false）
/// - indexed_doc_count 正确反映已索引文档数
#[tokio::test]
async fn test_numeric_field_types() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let schema_json = r#"
    [{"field":"title","field_type":"Text","store":true,"index_lexical":true,"longest":true},
    {"field":"count","field_type":"U32","store":true,"index_lexical":false},
    {"field":"url","field_type":"Text","store":false,"index_lexical":false}]"#;
    let schema = serde_json::from_str(schema_json).unwrap();

    let index_arc = create_index(index_path, default_meta(), &schema, &Vec::new(), 11, false, None)
        .await
        .unwrap();

    let doc = serde_json::from_str(r#"{"title":"numeric test","count":42,"url":"u1"}"#).unwrap();
    index_arc.index_document(doc, FileType::None).await;
    index_arc.commit().await;

    assert_eq!(index_arc.read().await.indexed_doc_count().await, 1);

    index_arc.close().await;
}

// ============================================================
// 18. version() 函数
// ============================================================

/// 测试 version() 函数返回非空版本号
///
/// 验证点：
/// - seekstorm::index::version() 返回的版本字符串不为空
#[test]
fn test_version_not_empty() {
    let v = seekstorm::index::version();
    assert!(!v.is_empty());
}

// ============================================================
// 19. 空文档集
// ============================================================

/// 测试索引空文档列表不会导致错误
///
/// 验证点：
/// - 传入空的 Vec<Document> 给 index_documents 不会 panic
/// - indexed_doc_count 保持为 0
#[tokio::test]
async fn test_index_empty_documents() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let index_arc = create_index(index_path, default_meta(), &default_schema(), &Vec::new(), 11, false, None)
        .await
        .unwrap();

    // 索引空文档列表
    let empty_docs: Vec<seekstorm::index::Document> = Vec::new();
    index_arc.index_documents(empty_docs).await;
    index_arc.commit().await;

    assert_eq!(index_arc.read().await.indexed_doc_count().await, 0);

    index_arc.close().await;
}

// ============================================================
// 20. 搜索无匹配结果
// ============================================================

/// 测试搜索不存在的词时返回空结果
///
/// 验证点：
/// - 搜索索引中不存在的词，result_count_total 为 0
/// - results 列表为空
#[tokio::test]
async fn test_search_no_results() {
    let index_path = Path::new(TEST_INDEX_PATH);
    let _ = fs::remove_dir_all(index_path);

    let index_arc = create_index(index_path, default_meta(), &default_schema(), &Vec::new(), 11, false, None)
        .await
        .unwrap();

    let doc = serde_json::from_str(r#"{"title":"hello world","body":"test body","url":"u1"}"#).unwrap();
    index_arc.index_document(doc, FileType::None).await;
    index_arc.commit().await;

    let result = index_arc
        .search("nonexistent_term_xyz".into(), None, QueryType::Union, SearchMode::Lexical, false, 0, 10, ResultType::TopkCount, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), QueryRewriting::SearchOnly)
        .await;
    assert_eq!(result.result_count_total, 0);
    assert!(result.results.is_empty());

    index_arc.close().await;
}
