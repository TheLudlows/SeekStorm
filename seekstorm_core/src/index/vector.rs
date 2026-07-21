//! 向量引擎类型别名（Phase 4）。
//!
//! Phase 4 之前是占位符，现在指向真实的 VectorEngine。

pub use crate::vector::{VectorConfig, VectorEngine as VectorEngineImpl};

/// 向量引擎类型别名，保持与 Phase 2 兼容。
pub type VectorEngine = VectorEngineImpl;