//! 相似度计算（Phase 4）。
//!
//! 支持 Cosine、Dot、Euclidean 三种相似度度量。
//! Phase 4 实现标量版本，SIMD 加速留待后续优化。

use std::cmp::Ordering;

/// 相似度度量。
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum VectorSimilarity {
    /// 余弦相似度（方向）。
    Cosine,
    /// 点积/内积（方向 + 大小）。
    Dot,
    /// 欧氏距离的负值（越大越相似）。
    Euclidean,
}

/// 计算两个向量的相似度。
pub fn compute_similarity(a: &[f32], b: &[f32], sim: VectorSimilarity) -> f32 {
    match sim {
        VectorSimilarity::Cosine => cosine_similarity(a, b),
        VectorSimilarity::Dot => dot_product(a, b),
        VectorSimilarity::Euclidean => -euclidean_distance_squared(a, b),
    }
}

/// 余弦相似度。
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot = dot_product(a, b);
    let norm_a = norm_l2(a);
    let norm_b = norm_l2(b);

    if norm_a < 1e-10 || norm_b < 1e-10 {
        return 0.0;
    }

    dot / (norm_a * norm_b)
}

/// 点积（内积）。
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

/// L2 范数。
pub fn norm_l2(v: &[f32]) -> f32 {
    v.iter().map(|x| x * x).sum::<f32>().sqrt()
}

/// 欧氏距离的平方。
pub fn euclidean_distance_squared(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| {
            let diff = x - y;
            diff * diff
        })
        .sum()
}

/// 欧氏距离。
pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    euclidean_distance_squared(a, b).sqrt()
}

/// 找到距离查询向量最近的质心。
///
/// 返回 (质心索引, 相似度)。
pub fn find_nearest_centroid(
    query: &[f32],
    centroids: &[Vec<f32>],
    sim: VectorSimilarity,
) -> (usize, f32) {
    centroids
        .iter()
        .enumerate()
        .map(|(i, c)| (i, compute_similarity(query, c, sim)))
        .max_by(|(_, s1), (_, s2)| {
            s1.partial_cmp(s2)
                .unwrap_or(if s1.is_nan() { Ordering::Less } else { Ordering::Greater })
        })
        .unwrap()
}

/// SIMD 优化预留（Phase 4 不实现）。
#[cfg(target_arch = "x86_64")]
pub fn cosine_similarity_avx2(_a: &[f32], _b: &[f32]) -> f32 {
    // TODO: 从 v3 复用 AVX2 实现
    unimplemented!("SIMD not implemented in Phase 4")
}

#[cfg(target_arch = "aarch64")]
pub fn cosine_similarity_neon(_a: &[f32], _b: &[f32]) -> f32 {
    // TODO: 从 v3 复用 NEON 实现
    unimplemented!("SIMD not implemented in Phase 4")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dot_product() {
        let v1 = vec![1.0, 2.0, 3.0];
        let v2 = vec![4.0, 5.0, 6.0];
        assert_eq!(dot_product(&v1, &v2), 1.0 * 4.0 + 2.0 * 5.0 + 3.0 * 6.0);
    }

    #[test]
    fn test_norm_l2() {
        let v = vec![3.0, 4.0];
        assert_eq!(norm_l2(&v), 5.0);
    }

    #[test]
    fn test_euclidean_distance() {
        let v1 = vec![1.0, 2.0, 3.0];
        let v2 = vec![4.0, 6.0, 8.0];
        let dist = euclidean_distance(&v1, &v2);
        assert_eq!(dist, 7.0710678); // sqrt((3^2 + 4^2 + 5^2)) = sqrt(50)
    }

    #[test]
    fn test_cosine_similarity_identical() {
        let v = vec![1.0, 2.0, 3.0];
        assert!(cosine_similarity(&v, &v) > 0.99);
    }

    #[test]
    fn test_cosine_similarity_orthogonal() {
        let v1 = vec![1.0, 0.0, 0.0];
        let v2 = vec![0.0, 1.0, 0.0];
        assert!(cosine_similarity(&v1, &v2).abs() < 1e-6);
    }

    #[test]
    fn test_compute_similarity_cosine() {
        let v1 = vec![1.0, 0.0];
        let v2 = vec![1.0, 0.0];
        let v3 = vec![0.0, 1.0];

        let sim12 = compute_similarity(&v1, &v2, VectorSimilarity::Cosine);
        let sim13 = compute_similarity(&v1, &v3, VectorSimilarity::Cosine);

        assert!(sim12 > 0.99);
        assert!(sim13.abs() < 0.01);
    }

    #[test]
    fn test_compute_similarity_dot() {
        let v1 = vec![1.0, 2.0];
        let v2 = vec![3.0, 4.0];
        let sim = compute_similarity(&v1, &v2, VectorSimilarity::Dot);
        assert_eq!(sim, 11.0);
    }

    #[test]
    fn test_compute_similarity_euclidean() {
        let v1 = vec![0.0, 0.0];
        let v2 = vec![3.0, 4.0];
        let sim = compute_similarity(&v1, &v2, VectorSimilarity::Euclidean);
        assert_eq!(sim, -25.0); // -(3^2 + 4^2) = -25
    }

    #[test]
    fn test_find_nearest_centroid() {
        // 使用 Dot 相似度（考虑大小）而不是 Cosine（仅方向）
        // 因为 [1,0] 和 [0.5,0] 的 Cosine 相似度都是 1.0（方向相同）
        let query = vec![1.0, 0.0];
        let centroids = vec![
            vec![1.0, 0.0],
            vec![0.0, 1.0],
            vec![0.5, 0.0],
        ];

        let (idx, sim) = find_nearest_centroid(&query, &centroids, VectorSimilarity::Dot);
        assert_eq!(idx, 0);
        assert!(sim > 0.99);
    }

    #[test]
    fn test_find_nearest_centroid_dot() {
        let query = vec![2.0, 0.0];
        let centroids = vec![
            vec![1.0, 0.0],
            vec![3.0, 0.0],
            vec![0.0, 1.0],
        ];

        let (idx, sim) = find_nearest_centroid(&query, &centroids, VectorSimilarity::Dot);
        assert_eq!(idx, 1); // [3, 0] dot [2, 0] = 6, highest
        assert_eq!(sim, 6.0);
    }
}