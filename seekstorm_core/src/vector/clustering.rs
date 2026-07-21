//! K-Medoid PAM 聚类算法（Phase 4）。
//!
//! 从 v3 的 clustering.rs 简化复制，用于初始质心计算。

use crate::vector::similarity::{compute_similarity, VectorSimilarity};
use rand::seq::SliceRandom;

/// K-Medoid PAM 聚类算法。
///
/// # 参数
/// - `vectors`: 输入向量列表
/// - `k`: 聚类数量
///
/// # 返回
/// - `Vec<Vec<f32>>`: k 个质心向量
pub fn k_medoid_pam(vectors: &[Vec<f32>], k: usize) -> Vec<Vec<f32>> {
    if k == 0 || vectors.is_empty() {
        return Vec::new();
    }
    if k == 1 {
        // 返回全局平均
        let _dim = vectors[0].len();
        let avg = average_vector(vectors);
        return vec![avg];
    }
    if k >= vectors.len() {
        // 每个向量都是质心
        return vectors.to_vec();
    }

    // 1. 随机选择 k 个初始 medoids
    let mut medoids: Vec<usize> = (0..vectors.len()).collect();
    medoids.shuffle(&mut rand::rng());
    medoids.truncate(k);

    // 2. 迭代优化
    let max_iterations = 10;
    let mut converged = false;

    for _iteration in 0..max_iterations {
        if converged {
            break;
        }

        // 2.1 将每个向量分配到最近的 medoid
        let mut assignments: Vec<(usize, f32)> = Vec::with_capacity(vectors.len());
        for (_vi, vector) in vectors.iter().enumerate() {
            let mut best_medoid = 0;
            let mut best_similarity = f32::MIN;

            for (mi, &medoid_idx) in medoids.iter().enumerate() {
                let sim = compute_similarity(vector, &vectors[medoid_idx], VectorSimilarity::Cosine);
                if sim > best_similarity {
                    best_medoid = mi;
                    best_similarity = sim;
                }
            }

            assignments.push((best_medoid, best_similarity));
        }

        // 2.2 为每个簇重新选择 medoid
        let mut new_medoids: Vec<usize> = vec![0; k];
        let mut cluster_sizes: Vec<usize> = vec![0; k];

        for (_vi, &(cluster_idx, _)) in assignments.iter().enumerate() {
            cluster_sizes[cluster_idx] += 1;
        }

        // 为每个簇找到最大化总相似度的 medoid
        for cluster_idx in 0..k {
            let mut best_medoid = medoids[cluster_idx];
            let mut best_total = f32::MIN;

            // 优化：只考虑分配到该簇的向量
            let cluster_vectors: Vec<usize> = assignments
                .iter()
                .enumerate()
                .filter(|(_, (ci, _))| *ci == cluster_idx)
                .map(|(vi, _)| vi)
                .collect();

            if cluster_vectors.is_empty() {
                // 簇为空，保持原 medoid
                new_medoids[cluster_idx] = medoids[cluster_idx];
                continue;
            }

            for &candidate in &cluster_vectors {
                let mut total_sim = 0.0;
                for &other in &cluster_vectors {
                    total_sim += compute_similarity(
                        &vectors[candidate],
                        &vectors[other],
                        VectorSimilarity::Cosine,
                    );
                }
                if total_sim > best_total {
                    best_total = total_sim;
                    best_medoid = candidate;
                }
            }

            new_medoids[cluster_idx] = best_medoid;
        }

        // 检查是否收敛
        if new_medoids == medoids {
            converged = true;
        }

        medoids = new_medoids;
    }

    // 返回质心向量
    medoids
        .iter()
        .map(|&idx| vectors[idx].clone())
        .collect()
}

/// 计算向量列表的平均向量。
fn average_vector(vectors: &[Vec<f32>]) -> Vec<f32> {
    let dim = vectors[0].len();
    let mut avg = vec![0.0f32; dim];

    for vector in vectors {
        for (i, &val) in vector.iter().enumerate() {
            avg[i] += val;
        }
    }

    let count = vectors.len() as f32;
    for val in avg.iter_mut() {
        *val /= count;
    }

    avg
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_k_medoid_pam_k1() {
        let vectors: Vec<Vec<f32>> = vec![
            vec![1.0, 0.0],
            vec![1.0, 0.1],
            vec![0.0, 1.0],
            vec![0.0, 1.1],
        ];
        let centroids = k_medoid_pam(&vectors, 1);
        assert_eq!(centroids.len(), 1);

        // 质心应该接近 [0.5, 0.525]
        let centroid = &centroids[0];
        assert!((centroid[0] - 0.5).abs() < 0.1);
        assert!((centroid[1] - 0.525).abs() < 0.1);
    }

    #[test]
    fn test_k_medoid_pam_k2() {
        let vectors: Vec<Vec<f32>> = vec![
            vec![1.0, 0.0, 0.0],
            vec![1.0, 0.1, 0.0],
            vec![1.0, 0.0, 0.1],
            vec![0.0, 1.0, 0.0],
            vec![0.0, 1.1, 0.0],
            vec![0.0, 1.0, 0.1],
        ];
        let centroids = k_medoid_pam(&vectors, 2);
        assert_eq!(centroids.len(), 2);

        // 验证两个质心分别接近不同的簇中心（顺序不确定）
        let has_large_x = centroids.iter().any(|c| c[0] > 0.5);
        let has_small_x = centroids.iter().any(|c| c[0] < 0.5);
        assert!(has_large_x, "should have a centroid with x > 0.5");
        assert!(has_small_x, "should have a centroid with x < 0.5");
    }

    #[test]
    fn test_k_medoid_pam_k_equals_vectors() {
        let vectors: Vec<Vec<f32>> = vec![
            vec![1.0, 0.0],
            vec![0.0, 1.0],
        ];
        let centroids = k_medoid_pam(&vectors, 2);
        assert_eq!(centroids.len(), 2);
    }

    #[test]
    fn test_k_medoid_pam_k_exceeds_vectors() {
        let vectors: Vec<Vec<f32>> = vec![
            vec![1.0, 0.0],
            vec![0.0, 1.0],
        ];
        let centroids = k_medoid_pam(&vectors, 10);
        assert_eq!(centroids.len(), 2);
    }

    #[test]
    fn test_average_vector() {
        let vectors: Vec<Vec<f32>> = vec![
            vec![1.0, 2.0],
            vec![3.0, 4.0],
            vec![5.0, 6.0],
        ];
        let avg = average_vector(&vectors);
        assert_eq!(avg, vec![3.0, 4.0]);
    }
}