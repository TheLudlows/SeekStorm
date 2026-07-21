//! Phase 4 向量引擎集成测试。
//!
//! 对应 V4_PHASED_IMPLEMENTATION.md §Phase 4 测试用例。

use seekstorm_core::{
    F32Quantization, IvfIndex, LsmConfig, LsmEngine, Partition, ScoredResult, VectorConfig,
    VectorEngine, VectorSimilarity, VectorSimilarityInternal,
};
use tempfile::tempdir;

#[tokio::test]
async fn test_quantization_f32_roundtrip() {
    let dim = 128;
    let quant = F32Quantization::new(dim, VectorSimilarityInternal::Cosine);
    let original: Vec<f32> = (0..dim).map(|i| i as f32 / dim as f32).collect();

    let encoded = quant.encode(&original);
    let decoded = quant.decode(&encoded);

    assert_eq!(decoded.len(), dim);
    for i in 0..dim {
        assert!((decoded[i] - original[i]).abs() < 1e-6);
    }
}

#[tokio::test]
async fn test_f32_bytes_per_vector() {
    let quant = F32Quantization::new(64, VectorSimilarityInternal::Cosine);
    assert_eq!(quant.bytes_per_vector(), 64 * 4);
}

#[tokio::test]
async fn test_f32_similarity_cosine() {
    let quant = F32Quantization::new(3, VectorSimilarityInternal::Cosine);

    let v1 = vec![1.0f32, 0.0, 0.0];
    let v2 = vec![1.0f32, 0.0, 0.0];
    let v3 = vec![0.0f32, 1.0, 0.0];

    let e1 = quant.encode(&v1);
    let e2 = quant.encode(&v2);
    let e3 = quant.encode(&v3);

    let sim12 = quant.similarity(&e1, &e2, VectorSimilarityInternal::Cosine);
    let sim13 = quant.similarity(&e1, &e3, VectorSimilarityInternal::Cosine);

    assert!(sim12 > 0.99, "same vectors should be similar");
    assert!(sim13.abs() < 0.01, "orthogonal vectors should have near-zero similarity");
}

#[tokio::test]
async fn test_partition_operations() {
    let centroid = vec![0.5, 0.5, 0.5];
    let partition = Partition::new(0, centroid.clone());

    // 添加向量
    partition.add_vector(100).await;
    assert_eq!(partition.vector_count().await, 1);

    partition.add_vector(200).await;
    assert_eq!(partition.vector_count().await, 2);

    // 删除向量
    assert!(!partition.is_tombstoned(100).await);
    partition.delete_vector(100).await;
    assert!(partition.is_tombstoned(100).await);

    // 检查质心
    assert_eq!(partition.centroid().await, centroid);
}

#[tokio::test]
async fn test_ivf_index_add_partition() {
    let ivf = IvfIndex::new();

    let pid1 = ivf.add_partition(vec![0.0; 10]).await;
    let pid2 = ivf.add_partition(vec![1.0; 10]).await;

    assert_eq!(pid1, 0);
    assert_eq!(pid2, 1);
    assert_eq!(ivf.partition_count().await, 2);

    // 获取分区
    let p1 = ivf.get_partition(pid1).await;
    assert!(p1.is_some());
    assert_eq!(p1.unwrap().id, pid1);
}

#[tokio::test]
async fn test_vector_engine_basic() {
    let dir = tempdir().unwrap();
    let lsm = LsmEngine::open(dir.path(), LsmConfig::default())
        .await
        .unwrap();
    let lsm = std::sync::Arc::new(lsm);

    let mut cfg = VectorConfig::default();
    cfg.vector_dim = 64;
    cfg.nprobe = 1;

    let engine = VectorEngine::new(lsm.clone(), cfg).await.unwrap();

    // 初始化质心
    let samples: Vec<Vec<f32>> = (0..100).map(|i| vec![i as f32 / 100.0; 64]).collect();
    engine.initialize_centroids(&samples).await.unwrap();

    assert!(engine.is_initialized());

    // 插入向量
    for (i, vec) in samples.iter().enumerate() {
        engine.add_vector(i as u64, vec).await.unwrap();
    }

    // 检索
    let results = engine.search(&samples[0], 10).await.unwrap();
    assert!(!results.is_empty());
    assert_eq!(results[0].doc_id, 0);
    assert!(results[0].score > 0.0);
}

#[tokio::test]
async fn test_vector_engine_delete() {
    let dir = tempdir().unwrap();
    let lsm = LsmEngine::open(dir.path(), LsmConfig::default())
        .await
        .unwrap();
    let lsm = std::sync::Arc::new(lsm);

    let mut cfg = VectorConfig::default();
    cfg.vector_dim = 32;
    cfg.nprobe = 1;

    let engine = VectorEngine::new(lsm.clone(), cfg).await.unwrap();

    let samples: Vec<Vec<f32>> = (0..10).map(|i| vec![i as f32; 32]).collect();
    engine.initialize_centroids(&samples).await.unwrap();

    // 插入
    for (i, vec) in samples.iter().enumerate() {
        engine.add_vector(i as u64, vec).await.unwrap();
    }

    // 删除 doc_id = 5
    let pid = engine.find_nearest_partition(&samples[5]).await.unwrap();
    engine.delete_vector(5, pid).await.unwrap();

    // 检索不包含已删除
    let results = engine.search(&samples[5], 10).await.unwrap();
    assert!(!results.iter().any(|r| r.doc_id == 5));
}

#[tokio::test]
async fn test_vector_partition_count() {
    let dir = tempdir().unwrap();
    let lsm = LsmEngine::open(dir.path(), LsmConfig::default())
        .await
        .unwrap();
    let lsm = std::sync::Arc::new(lsm);

    let mut cfg = VectorConfig::default();
    cfg.vector_dim = 16;

    let engine = VectorEngine::new(lsm.clone(), cfg).await.unwrap();

    let samples: Vec<Vec<f32>> = (0..200).map(|i| vec![i as f32 / 200.0; 16]).collect();
    engine.initialize_centroids(&samples).await.unwrap();

    assert!(engine.partition_count().await > 0);
}

#[tokio::test]
async fn test_vector_engine_not_initialized() {
    let dir = tempdir().unwrap();
    let lsm = LsmEngine::open(dir.path(), LsimConfig::default())
        .await
        .unwrap();
    let lsm = std::sync::Arc::new(lsm);

    let engine = VectorEngine::new(lsm, VectorConfig::default()).await.unwrap();

    // 未初始化时搜索应失败
    let result = engine.search(&vec![0.0; 16], 10).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not initialized"));
}

#[tokio::test]
async fn test_vector_dimension_mismatch() {
    let dir = tempdir().unwrap();
    let lsm = LsmEngine::open(dir.path(), LsmConfig::default())
        .await
        .unwrap();
    let lsm = std::sync::Arc::new(lsm);

    let mut cfg = VectorConfig::default();
    cfg.vector_dim = 64;

    let engine = VectorEngine::new(lsm, VectorConfig::default()).await.unwrap();

    let samples: Vec<Vec<f32>> = (0..10).map(|i| vec![i as f32; 64]).collect();
    engine.initialize_centroids(&samples).await.unwrap();

    // 插入错误维度的向量
    let wrong_dim = vec![0.0; 32];
    let result = engine.add_vector(0, &wrong_dim).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("dimension mismatch"));
}

#[tokio::test]
async fn test_k_medoid_pam() {
    use seekstorm_core::vector::clustering;

    let vectors: Vec<Vec<f32>> = vec![
        vec![1.0, 0.0],
        vec![1.0, 0.1],
        vec![0.0, 1.0],
        vec![0.0, 1.1],
        vec![1.0, 1.0],
        vec![0.0, 0.0],
    ];

    let centroids = clustering::k_medoid_pam(&vectors, 2);
    assert_eq!(centroids.len(), 2);

    // 质心应该在 [1, ~0.3] 和 [0, ~1.05] 附近
    assert!(centroids[0][0] > 0.5, "first centroid should have larger x");
    assert!(centroids[1][1] > 0.5, "second centroid should have larger y");
}

#[tokio::test]
async fn test_find_nearest_centroid() {
    use seekstorm_core::vector::similarity;

    let centroids = vec![
        vec![1.0, 0.0, 0.0],
        vec![0.0, 1.0, 0.0],
        vec![0.0, 0.0, 1.0],
    ];

    let (idx, sim) = similarity::find_nearest_centroid(
        &vec![1.0, 0.1, 0.0],
        &centroids,
        VectorSimilarityInternal::Cosine,
    );

    assert_eq!(idx, 0);
    assert!(sim > 0.9);
}

#[tokio::test]
async fn test_cosine_similarity() {
    use seekstorm_core::vector::similarity;

    let v1 = vec![1.0, 0.0, 0.0];
    let v2 = vec![1.0, 0.0, 0.0];
    let v3 = vec![0.0, 1.0, 0.0];

    assert!(similarity::cosine_similarity(&v1, &v2) > 0.99);
    assert!(similarity::cosine_similarity(&v1, &v3).abs() < 0.01);
}

#[tokio::test]
async fn test_vector_similarity_from_schema() {
    use seekstorm_core::schema::VectorSimilarity as SchemaSim;

    let sim = VectorSimilarity::from(SchemaSim::Cosine);
    assert_eq!(sim, VectorSimilarityInternal::Cosine);

    let sim = VectorSimilarity::from(SchemaSim::L2);
    assert_eq!(sim, VectorSimilarity::Euclidean);

    let sim = VectorSimilarity::from(SchemaSim::InnerProduct);
    assert_eq!(sim, VectorSimilarity::Dot);
}