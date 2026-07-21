//! SeekStorm v4 核心库。
//!
//! 顶层模块按子系统组织：
//! - [`storage`]：简易 LSM 引擎（MemTable + SSTable + WAL + Manifest）。
//! - [`schema`]：动态 schema 与类型推断（Phase 2）。
//! - [`index`]：Schemaless 文档 CRUD 与索引入口（Phase 2）。
//! - [`lexical`]：词法引擎，基于 tantivy 分词器 + 自研 LSM 倒排索引（Phase 3）。
//! - 后续阶段会加入 `vector`（SPFresh LIRE）/ `query` / `search`。

pub mod index;
pub mod lexical;
pub mod schema;
pub mod storage;
pub mod vector;

// ---- Phase 1 re-exports ----
pub use storage::io_backend::{select_backend, AsyncFsBackend, IoBackend, IoBackendKind};
pub use storage::lsm::{
    LsmConfig, LsmEngine, LsmKey, LsmValue, MemTable, NS_DOC, NS_META, NS_VEC,
};
pub use storage::manifest::{Manifest, SSTableMeta};
pub use storage::sstable::{SSTable, SStableWriter};
pub use storage::wal::{Wal, WalOp, WalSync};

// ---- Phase 2 re-exports ----
pub use index::{DocId, Index, IndexConfig, SchemalessDoc, VectorEngine as VectorEngineOld};
pub use schema::{
    DynamicSchema, FieldChange, FieldId, FieldMeta, FieldType, TokenizerType, VectorSimilarity,
};

// ---- Phase 3 re-exports ----
pub use lexical::{LexicalEngine, ScoredDoc};

// ---- Phase 4 re-exports ----
pub use vector::{
    F32Quantization, IvfIndex, partition::Partition, partition::PartitionId, Quantization,
    QuantizationType, ScoredResult, VectorConfig, VectorEngine,
};
