//! 向量引擎占位（Phase 4-5 接入 SPFresh LIRE）。
//!
//! Phase 2 仅定义空壳 `VectorEngine`，使 [`super::Index`] 字段类型可编译。
//! Phase 4 会接入 IVF 基础结构，Phase 5 接入 LIRE 实时更新协议。

use std::marker::PhantomData;

/// 向量引擎占位。Phase 4-5 替换为真实实现。
pub struct VectorEngine {
    _phantom: PhantomData<()>,
}

impl VectorEngine {
    /// Phase 4-5 在此接入 IVF + LIRE。
    pub fn placeholder() -> Self {
        Self { _phantom: PhantomData }
    }
}
