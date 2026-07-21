//! 向量量化（Phase 4）。
//!
//! Phase 4 实现 F32Quantization，Phase 5 扩展 ScalarQuantization 和 TurboQuant。

use std::fmt;

use crate::vector::similarity::compute_similarity;

/// 量化类型。
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum QuantizationType {
    /// 不量化，直接存储 f32。
    F32,
    /// Phase 5 实现。
    ScalarQuantizationI8,
    /// Phase 5 实现。
    TurboQuantI8,
}

/// 量化 trait。
pub trait Quantization: Send + Sync {
    fn encode(&self, vector: &[f32]) -> Vec<u8>;
    fn decode(&self, bytes: &[u8]) -> Vec<f32>;
    fn similarity(&self, a: &[u8], b: &[u8], sim: crate::vector::VectorSimilarity) -> f32;
    fn dim(&self) -> usize;
    fn bytes_per_vector(&self) -> usize;
}

/// F32 量化（不量化，直接存储）。
pub struct F32Quantization {
    pub dim: usize,
    pub similarity: crate::vector::VectorSimilarity,
}

impl F32Quantization {
    pub fn new(dim: usize, similarity: crate::vector::VectorSimilarity) -> Self {
        Self { dim, similarity }
    }
}

impl Quantization for F32Quantization {
    fn encode(&self, vector: &[f32]) -> Vec<u8> {
        bytemuck::cast_slice(vector).to_vec()
    }

    fn decode(&self, bytes: &[u8]) -> Vec<f32> {
        bytemuck::cast_slice(bytes).to_vec()
    }

    fn similarity(&self, a: &[u8], b: &[u8], sim: crate::vector::VectorSimilarity) -> f32
    {
        let va: &[f32] = bytemuck::cast_slice(a);
        let vb: &[f32] = bytemuck::cast_slice(b);
        compute_similarity(va, vb, sim)
    }

    fn dim(&self) -> usize {
        self.dim
    }

    fn bytes_per_vector(&self) -> usize {
        self.dim * 4
    }
}

impl fmt::Debug for dyn Quantization {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Quantization")
            .field("dim", &self.dim())
            .field("bytes_per_vector", &self.bytes_per_vector())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_f32_quantization_roundtrip() {
        let dim = 128;
        let quant = F32Quantization::new(dim, crate::vector::VectorSimilarity::Cosine);
        let original: Vec<f32> = (0..dim).map(|i| i as f32 / dim as f32).collect();

        let encoded = quant.encode(&original);
        let decoded = quant.decode(&encoded);

        assert_eq!(decoded.len(), dim);
        for i in 0..dim {
            assert!((decoded[i] - original[i]).abs() < 1e-6);
        }
    }

    #[test]
    fn test_f32_bytes_per_vector() {
        let quant = F32Quantization::new(64, crate::vector::VectorSimilarity::Cosine);
        assert_eq!(quant.bytes_per_vector(), 64 * 4);
    }

    #[test]
    fn test_f32_similarity_cosine() {
        let quant = F32Quantization::new(3, crate::vector::VectorSimilarity::Cosine);

        let v1 = vec![1.0f32, 0.0, 0.0];
        let v2 = vec![1.0f32, 0.0, 0.0];
        let v3 = vec![0.0f32, 1.0, 0.0];

        let e1 = quant.encode(&v1);
        let e2 = quant.encode(&v2);
        let e3 = quant.encode(&v3);

        let sim12 = quant.similarity(&e1, &e2, crate::vector::VectorSimilarity::Cosine);
        let sim13 = quant.similarity(&e1, &e3, crate::vector::VectorSimilarity::Cosine);

        assert!(sim12 > 0.99, "same vectors should be similar");
        assert!(sim13.abs() < 0.01, "orthogonal vectors should have near-zero similarity");
    }
}