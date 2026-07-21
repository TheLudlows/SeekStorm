//! 字段元数据与类型定义。
//!
//! [`FieldMeta`] 描述单个字段的名称、类型、索引选项。 [`FieldType`]
//! 覆盖标量、向量与数组三种形态。 [`DynamicSchema`]（见 [`mod`）
//! 持有 `IndexMap<String, FieldMeta>`，按字段名查找并按插入顺序持久化。

use serde::{Deserialize, Serialize};

use super::inference::infer_field_type;

/// 字段 id。Schema 内单调递增。
pub type FieldId = u32;

/// 向量相似度度量。Phase 4 接入向量引擎时使用，Phase 2 仅作为元数据持久化。
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum VectorSimilarity {
    /// 余弦相似度（默认）。
    Cosine,
    /// 欧氏距离（L2）。
    L2,
    /// 内积（Inner Product）。
    InnerProduct,
}

/// 词法分析器类型。Phase 3 接入 tantivy 时使用，Phase 2 仅作为元数据持久化。
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TokenizerType {
    /// 默认分词（小写 + Unicode 分词）。
    Default,
    /// 原文（不分词，用于精确匹配 / ID 字段）。
    Raw,
    /// N-gram（前缀匹配）。
    Ngram { n: u8 },
}

/// 字段类型。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FieldType {
    /// 文本（可被词法索引）。
    Text,
    /// 64 位有符号整数。
    I64,
    /// 64 位无符号整数。
    U64,
    /// 64 位浮点。
    F64,
    /// 布尔。
    Bool,
    /// RFC 3339 日期时间。
    DateTime,
    /// 字节序列（二进制，不索引）。
    Bytes,
    /// 向量。`Vector(dim, similarity)`。
    Vector(usize, VectorSimilarity),
    /// 数组。元素类型由内部 `Box<FieldType>` 描述。
    Array(Box<FieldType>),
}

impl FieldType {
    /// 类型判等（忽略 Vector 的 dim，仅比较相似度类别；
    /// 其它类型严格相等）。用于 Schema 推断时的类型冲突检测。
    pub fn compatible(&self, other: &FieldType) -> bool {
        match (self, other) {
            (FieldType::Vector(_, a), FieldType::Vector(_, b)) => a == b,
            (FieldType::Array(a), FieldType::Array(b)) => a.compatible(b),
            (a, b) => a.discriminant() == b.discriminant(),
        }
    }

    /// 类型 tag（用于冲突诊断日志）。
    fn discriminant(&self) -> u8 {
        match self {
            FieldType::Text => 0,
            FieldType::I64 => 1,
            FieldType::U64 => 2,
            FieldType::F64 => 3,
            FieldType::Bool => 4,
            FieldType::DateTime => 5,
            FieldType::Bytes => 6,
            FieldType::Vector(_, _) => 7,
            FieldType::Array(_) => 8,
        }
    }

    /// 是否为向量类型。
    pub fn is_vector(&self) -> bool {
        matches!(self, FieldType::Vector(_, _))
    }
}

/// 字段元数据。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FieldMeta {
    pub id: FieldId,
    pub name: String,
    pub data_type: FieldType,
    /// 是否建立词法倒排索引。默认与字段类型相关：Text/DateTime 默认 true。
    pub indexed: bool,
    /// 是否在 DocStore 中存储原始值（用于查询时回填）。默认 true。
    pub stored: bool,
    /// 是否参与词法索引（tantovy）。仅对 Text/DateTime/Array<Text> 有意义。
    pub index_lexical: bool,
    /// 是否参与向量索引。
    pub index_vector: bool,
    /// 向量维度（仅当 `data_type` 为 `Vector` 时有效）。
    pub vector_dim: Option<usize>,
    /// 词法分析器（仅当 `index_lexical = true` 时有效）。
    pub tokenizer: Option<TokenizerType>,
}

impl FieldMeta {
    /// 根据推断出的类型与字段名构造默认元数据。
    pub fn new(id: FieldId, name: String, data_type: FieldType) -> Self {
        let is_text = matches!(data_type, FieldType::Text);
        let is_vector = data_type.is_vector();
        let vector_dim = match &data_type {
            FieldType::Vector(d, _) => Some(*d),
            _ => None,
        };
        let indexed = is_text || matches!(data_type, FieldType::DateTime);
        let stored = true;
        let index_lexical = is_text;
        let index_vector = is_vector;
        let tokenizer = if is_text {
            Some(TokenizerType::Default)
        } else {
            None
        };
        Self {
            id,
            name,
            data_type,
            indexed,
            stored,
            index_lexical,
            index_vector,
            vector_dim,
            tokenizer,
        }
    }
}

/// Schema 变更事件。`infer_and_add` 返回本次新增的字段列表。
#[derive(Clone, Debug)]
pub struct FieldChange {
    pub name: String,
    pub meta: FieldMeta,
}

/// 给定 JSON 值，推断对应字段类型（委托给 [`inference::infer_field_type`]）。
pub fn infer(value: &serde_json::Value) -> FieldType {
    infer_field_type(value)
}
