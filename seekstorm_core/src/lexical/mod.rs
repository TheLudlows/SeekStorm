//! 词法引擎：基于 tantivy 分词器 + 自研 LSM 倒排索引。
//!
//! 不使用 tantivy 的 Directory trait，postings 直接写入 LSM（NS_LEXICAL_POSTING）。
//! 与文档写入同 batch，保证原子性。

pub mod analyzer;
pub mod posting;
pub mod query;

pub use posting::Posting;
pub use query::{parse_query, LexicalQuery, TermQuery};

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use tantivy::tokenizer::TextAnalyzer;
use tokio::sync::RwLock;

use crate::schema::{DynamicSchema, FieldId, FieldType};
use crate::storage::lsm::{LsmEngine, LsmKey, LsmValue, NS_LEXICAL_POSTING, NS_LEXICAL_STATS};

/// BM25 参数。
pub struct Bm25Params {
    /// 词频饱和参数（默认 1.2）。
    pub k1: f64,
    /// 文档长度归一化参数（默认 0.75）。
    pub b: f64,
}

impl Default for Bm25Params {
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

/// 评分文档。
#[derive(Clone, Debug)]
pub struct ScoredDoc {
    pub doc_id: u64,
    pub score: f64,
}

/// 词法引擎。
pub struct LexicalEngine {
    lsm: Arc<LsmEngine>,
    schema: Arc<RwLock<DynamicSchema>>,
    /// 分词器缓存（按 FieldId）。
    tokenizers: RwLock<HashMap<FieldId, TextAnalyzer>>,
    /// BM25 参数。
    bm25: Bm25Params,
    /// 已索引文档数（内存计数器，commit 时持久化）。
    total_docs: AtomicU64,
}

impl LexicalEngine {
    /// 创建词法引擎。
    pub async fn new(lsm: Arc<LsmEngine>, schema: Arc<RwLock<DynamicSchema>>) -> Result<Self> {
        let total_docs = Self::read_total_docs(&lsm).await?;
        Ok(Self {
            lsm,
            schema,
            tokenizers: RwLock::new(HashMap::new()),
            bm25: Bm25Params::default(),
            total_docs: AtomicU64::new(total_docs),
        })
    }

    /// 准备文档的 posting entries（不实际写入 LSM）。
    /// 返回 `Vec<(LsmKey, LsmValue)>` 供上层批量写入。
    /// 同时递增 total_docs 内存计数器。
    pub async fn prepare_add_document(
        &self,
        doc_id: u64,
        doc: &crate::index::SchemalessDoc,
    ) -> Result<Vec<(LsmKey, LsmValue)>> {
        // 递增 total_docs（内存计数器）
        self.total_docs.fetch_add(1, Ordering::SeqCst);
        let schema = self.schema.read().await;
        let mut entries = Vec::new();

        for (field_name, value) in doc.fields() {
            let Some(meta) = schema.get_field(field_name).await else { continue };
            if !meta.index_lexical { continue }
            let serde_json::Value::String(text) = value else { continue };

            // 取分词器
            let mut analyzer = self.get_or_create_analyzer(meta.id, &meta.tokenizer).await?;

            // 分词 → 按 term 聚合 positions
            let mut term_positions: HashMap<String, Vec<u32>> = HashMap::new();
            let mut stream = analyzer.token_stream(text);
            let mut pos = 0u32;
            while let Some(token) = stream.next() {
                term_positions.entry(token.text.clone()).or_default().push(pos);
                pos += 1;
            }

            // 每个 (field, term, doc) 生成一个 entry
            for (term, positions) in term_positions {
                let partition = hash_field_term(meta.id, &term);
                let key = LsmKey {
                    namespace: NS_LEXICAL_POSTING,
                    partition_or_segment: partition,
                    doc_id,
                    lsn: 0, // LSN 由上层批量写入时设置
                };
                let posting = Posting::new(meta.id, term, doc_id, positions);
                let value = posting.encode()?;
                entries.push((key, LsmValue::Data(value)));
            }
        }

        Ok(entries)
    }


    /// 搜索。
    pub async fn search(&self, query_str: &str, top_k: usize) -> Result<Vec<ScoredDoc>> {
        let query = parse_query(query_str)?;
        let schema = self.schema.read().await;
        let total_docs = self.total_docs.load(Ordering::SeqCst) as f64;

        // 用 analyzer 处理查询词（lowercase 等）
        let mut processed_terms = Vec::new();
        for term_q in &query.terms {
            let fields_to_search = if let Some(field_name) = &term_q.field {
                if let Some(meta) = schema.get_field(field_name).await {
                    vec![meta]
                } else {
                    continue;
                }
            } else {
                schema.snapshot().await
                    .into_iter()
                    .filter(|m| matches!(m.data_type, FieldType::Text) && m.index_lexical)
                    .collect()
            };

            for meta in fields_to_search {
                let mut analyzer = self.get_or_create_analyzer(meta.id, &meta.tokenizer).await?;
                let mut stream = analyzer.token_stream(&term_q.text);
                while let Some(token) = stream.next() {
                    processed_terms.push((meta.clone(), token.text.clone()));
                }
            }
        }

        // 多 term 查询：取交集
        let mut term_doc_sets: Vec<std::collections::HashSet<u64>> = Vec::new();
        let mut term_scores: Vec<HashMap<u64, f64>> = Vec::new();

        for (meta, term_text) in &processed_terms {
            let partition = hash_field_term(meta.id, term_text);
            let entries = self.lsm.scan_prefix(NS_LEXICAL_POSTING, partition).await?;

            let df = entries.len() as f64;
            let idf = if df > 0.0 {
                ((total_docs - df + 0.5) / (df + 0.5) + 1.0).ln()
            } else {
                0.0
            };

            let mut doc_set = std::collections::HashSet::new();
            let mut scores = HashMap::new();

            for (key, value) in entries {
                let bytes = match value {
                    LsmValue::Data(b) => b,
                    _ => continue,
                };
                let posting = Posting::decode(&bytes)?;

                // Lazy 删除过滤
                match self.lsm.get(&LsmKey::doc(key.doc_id)).await? {
                    Some(LsmValue::Tombstone) | None => continue,
                    _ => {}
                }

                doc_set.insert(key.doc_id);

                // BM25（简化版）
                let tf = posting.term_freq as f64;
                let score = idf * (tf * (self.bm25.k1 + 1.0)) / (tf + self.bm25.k1);
                *scores.entry(key.doc_id).or_default() += score;
            }

            term_doc_sets.push(doc_set);
            term_scores.push(scores);
        }

        // 取交集：只保留出现在所有 term 结果中的 doc_id
        let mut scored: HashMap<u64, f64> = HashMap::new();
        if !term_doc_sets.is_empty() {
            let first_set = &term_doc_sets[0];
            for doc_id in first_set {
                let in_all = term_doc_sets.iter().all(|set| set.contains(doc_id));
                if in_all {
                    let total_score = term_scores.iter().map(|s| s.get(doc_id).copied().unwrap_or(0.0)).sum();
                    scored.insert(*doc_id, total_score);
                }
            }
        }

        let mut results: Vec<_> = scored
            .into_iter()
            .map(|(doc_id, score)| ScoredDoc { doc_id, score })
            .collect();
        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        results.truncate(top_k);
        Ok(results)
    }

    /// 提交：持久化 total_docs 到 NS_LEXICAL_STATS。
    pub async fn commit(&self) -> Result<()> {
        let total_docs = self.total_docs.load(Ordering::SeqCst);
        let key = LsmKey {
            namespace: NS_LEXICAL_STATS,
            partition_or_segment: 0,
            doc_id: 0,
            lsn: 0,
        };
        let value = total_docs.to_be_bytes().to_vec();
        self.lsm.put(key, LsmValue::Data(value)).await?;
        Ok(())
    }

    /// 获取或创建分词器。
    async fn get_or_create_analyzer(
        &self,
        field_id: FieldId,
        tokenizer_type: &Option<crate::schema::TokenizerType>,
    ) -> Result<TextAnalyzer> {
        let mut tokenizers = self.tokenizers.write().await;
        if let Some(analyzer) = tokenizers.get(&field_id) {
            return Ok(analyzer.clone());
        }
        let tt = tokenizer_type.clone().unwrap_or(crate::schema::TokenizerType::Default);
        let analyzer = analyzer::build_analyzer(&tt)?;
        tokenizers.insert(field_id, analyzer.clone());
        Ok(analyzer)
    }

    /// 从 NS_LEXICAL_STATS 读取 total_docs。
    async fn read_total_docs(lsm: &LsmEngine) -> Result<u64> {
        let key = LsmKey {
            namespace: NS_LEXICAL_STATS,
            partition_or_segment: 0,
            doc_id: 0,
            lsn: 0,
        };
        match lsm.get(&key).await? {
            Some(LsmValue::Data(bytes)) => {
                if bytes.len() >= 8 {
                    Ok(u64::from_be_bytes(bytes[..8].try_into()?))
                } else {
                    Ok(0)
                }
            }
            _ => Ok(0),
        }
    }
}

/// 计算 (field_id, term) 的哈希值作为 LSM partition。
fn hash_field_term(field_id: FieldId, term: &str) -> u32 {
    use std::hash::{Hash, Hasher};
    let mut hasher = ahash::AHasher::default();
    field_id.hash(&mut hasher);
    term.hash(&mut hasher);
    hasher.finish() as u32
}
