//! 向量引擎（Phase 4）：基于 IVF 的 ANN 检索。
//!
//! 本模块实现：
//! - 向量量化 (Quantization)
//! - 相似度计算 (Similarity) + SIMD 加速
//! - K-Medoid PAM 聚类
//! - IVF 分区管理 (Partition + IvfIndex)
//! - VectorEngine 门面
//!
//! 元素类型：f32（用户已确认）
//! 存储命名空间：NS_VEC (0x02)

pub mod clustering;
pub mod ivf_index;
pub mod partition;
pub mod quantization;
pub mod similarity;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::RwLock;

use crate::index::SchemalessDoc;
use crate::schema::VectorSimilarity as SchemaVectorSimilarity;
use crate::storage::lsm::{LsmEngine, LsmKey, LsmValue, NS_VEC};

pub use ivf_index::{CentroidTable, IvfIndex};
pub use partition::{PartitionId, VectorPartitionId};
pub use quantization::{F32Quantization, Quantization, QuantizationType};
use similarity::VectorSimilarity;

/// 向量搜索结果（带评分）。
#[derive(Clone, Debug, PartialEq)]
pub struct ScoredResult {
    pub doc_id: u64,
    pub score: f32,
}

/// 向量引擎配置。
#[derive(Clone, Debug)]
pub struct VectorConfig {
    /// 向量维度。
    pub vector_dim: usize,
    /// 相似度度量。
    pub similarity: VectorSimilarity,
    /// 量化策略。
    pub quantization: QuantizationType,
    /// 搜索时探测的分区数（nprobe）。
    pub nprobe: usize,
    /// 分区最大向量数（Phase 5 LIRE 使用）。
    pub max_partition_size: u64,
}

impl Default for VectorConfig {
    fn default() -> Self {
        Self {
            vector_dim: 128,
            similarity: VectorSimilarity::Cosine,
            quantization: QuantizationType::F32,
            nprobe: 10,
            max_partition_size: 10000,
        }
    }
}

/// 向量引擎（Phase 4 门面）。
pub struct VectorEngine {
    lsm: Arc<LsmEngine>,
    ivf: RwLock<Arc<IvfIndex>>,
    quantization: Arc<dyn Quantization>,
    config: VectorConfig,
    initialized: AtomicBool,
}

impl VectorEngine {
    /// 创建新的向量引擎（需要先调用 `initialize_centroids`）。
    pub async fn new(lsm: Arc<LsmEngine>, config: VectorConfig) -> Result<Self> {
        let quantization: Arc<dyn Quantization> = match config.quantization {
            QuantizationType::F32 => {
                Arc::new(F32Quantization::new(config.vector_dim, config.similarity))
            }
            QuantizationType::ScalarQuantizationI8 | QuantizationType::TurboQuantI8 => {
                anyhow::bail!("QuantizationType not yet implemented in Phase 4")
            }
        };

        Ok(Self {
            lsm,
            ivf: RwLock::new(Arc::new(IvfIndex::new())),
            quantization,
            config,
            initialized: AtomicBool::new(false),
        })
    }

    /// 使用样本向量初始化质心。
    pub async fn initialize_centroids(&self, sample_vectors: &[Vec<f32>]) -> Result<()> {
        if sample_vectors.is_empty() {
            anyhow::bail!("sample_vectors must not be empty");
        }

        // k = sqrt(n) + 1
        let k = (sample_vectors.len() as f64).sqrt() as usize + 1;

        // K-Medoid PAM 聚类
        let centroids = clustering::k_medoid_pam(sample_vectors, k);

        // 添加分区到 IVF 索引
        let ivf = self.ivf.write().await;
        let mut centroid_table = CentroidTable::new();

        for centroid in centroids {
            let pid = ivf.add_partition(centroid.clone()).await;
            centroid_table.add(pid, centroid);
        }

        // 更新质心表
        *ivf.centroid_table.write().await = centroid_table;

        self.initialized.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// 准备向量写入的 entry（不实际写入 LSM）。
    /// 返回 `Vec<(LsmKey, LsmValue)>` 供上层批量写入。
    pub async fn prepare_add_vector(
        &self,
        doc_id: u64,
        vector: &[f32],
    ) -> Result<(Vec<(LsmKey, LsmValue)>, VectorPartitionId)> {
        if !self.initialized.load(Ordering::SeqCst) {
            anyhow::bail!("VectorEngine not initialized, call initialize_centroids first");
        }

        if vector.len() != self.config.vector_dim {
            anyhow::bail!(
                "Vector dimension mismatch: expected {}, got {}",
                self.config.vector_dim,
                vector.len()
            );
        }

        // 找到最近分区
        let partition_id = self.find_nearest_partition(vector).await?;

        // 量化
        let quantized = self.quantization.encode(vector);
        let key = LsmKey::vec(partition_id, doc_id);

        Ok((vec![(key, LsmValue::Data(quantized))], partition_id))
    }

    /// 从文档中准备所有向量字段的 entries。
    /// 返回 `(entries, partition_ids)` 供上层批量写入。
    pub async fn prepare_add_document(
        &self,
        doc_id: u64,
        doc: &SchemalessDoc,
    ) -> Result<(Vec<(LsmKey, LsmValue)>, Vec<VectorPartitionId>)> {
        let mut entries = Vec::new();
        let mut partition_ids = Vec::new();

        for (_field_name, value) in doc.fields() {
            let Some(vector) = self.extract_vector(value) else { continue };
            let (vec_entries, partition_id) = self.prepare_add_vector(doc_id, &vector).await?;
            entries.extend(vec_entries);
            partition_ids.push(partition_id);
        }

        Ok((entries, partition_ids))
    }

    /// 更新分区中的向量计数（内存操作）。
    pub async fn update_partition_count(&self, doc_id: u64, partition_id: VectorPartitionId) {
        let ivf = self.ivf.read().await;
        let partitions = ivf.partitions.read().await;
        if let Some(partition) = partitions.get(&partition_id) {
            partition.add_vector(doc_id).await;
        }
    }

    /// 从 Value 中提取向量。
    fn extract_vector(&self, value: &serde_json::Value) -> Option<Vec<f32>> {
        match value {
            serde_json::Value::Array(arr) => {
                if !arr.is_empty() && arr.iter().all(|v| v.is_number()) {
                    Some(
                        arr.iter()
                            .map(|v| {
                                v.as_f64()
                                    .map(|f| f as f32)
                                    .unwrap_or_else(|| v.as_i64().map(|i| i as f32).unwrap_or(0.0))
                            })
                            .collect(),
                    )
                } else {
                    None
                }
            }
            _ => None,
        }
    }


    /// 搜索向量（穷举 nprobe 个分区）。
    pub async fn search(&self, query: &[f32], top_k: usize) -> Result<Vec<ScoredResult>> {
        if !self.initialized.load(Ordering::SeqCst) {
            anyhow::bail!("VectorEngine not initialized");
        }

        if query.len() != self.config.vector_dim {
            anyhow::bail!(
                "Query vector dimension mismatch: expected {}, got {}",
                self.config.vector_dim,
                query.len()
            );
        }

        let ivf = self.ivf.read().await;
        let centroid_table = ivf.centroid_table.read().await;

        // 1. 找到最近的 nprobe 个分区
        let probe_partitions =
            centroid_table.find_nearest(query, self.config.nprobe, self.config.similarity);

        // 2. 穷举扫描这些分区
        let mut candidates = Vec::new();

        for &pid in &probe_partitions {
            // 从 LSM 扫描分区
            let vectors = self.lsm.scan_prefix(NS_VEC, pid).await?;
            for (key, value) in vectors {
                // 跳过墓碑
                let partitions = ivf.partitions.read().await;
                if let Some(partition) = partitions.get(&pid) {
                    if partition.is_tombstoned(key.doc_id).await {
                        continue;
                    }
                }

                // 解码向量
                let decoded = if let LsmValue::Data(bytes) = value {
                    self.quantization.decode(&bytes)
                } else {
                    continue;
                };

                // 计算相似度
                let query_quantized = self.quantization.encode(query);
                let score = self.quantization.similarity(
                    &query_quantized,
                    &self.quantization.encode(&decoded),
                    self.config.similarity,
                );

                candidates.push(ScoredResult {
                    doc_id: key.doc_id,
                    score,
                });
            }
        }

        // 3. 排序并返回 top_k
        candidates.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(candidates.into_iter().take(top_k).collect())
    }

    /// 查找距离查询向量最近的分区。
    async fn find_nearest_partition(&self, vector: &[f32]) -> Result<PartitionId> {
        let ivf = self.ivf.read().await;
        let centroid_table = ivf.centroid_table.read().await;

        let (idx, _) = similarity::find_nearest_centroid(
            vector,
            &centroid_table
                .entries
                .iter()
                .map(|(_, c)| c.clone())
                .collect::<Vec<_>>(),
            self.config.similarity,
        );

        // idx 是 entries 中的索引，需要获取实际的分区 ID
        let pid = centroid_table
            .entries
            .get(idx)
            .map(|(id, _)| *id)
            .ok_or_else(|| anyhow::anyhow!("No centroids available"))?;

        Ok(pid)
    }

    /// 获取分区总数。
    pub async fn partition_count(&self) -> usize {
        let ivf = self.ivf.read().await;
        ivf.partitions.read().await.len()
    }

    /// 检查是否已初始化。
    pub fn is_initialized(&self) -> bool {
        self.initialized.load(Ordering::SeqCst)
    }
}

/// Schema VectorSimilarity 到 internal VectorSimilarity 的转换。
impl From<SchemaVectorSimilarity> for VectorSimilarity {
    fn from(sim: SchemaVectorSimilarity) -> Self {
        match sim {
            SchemaVectorSimilarity::Cosine => Self::Cosine,
            SchemaVectorSimilarity::L2 => Self::Euclidean,
            SchemaVectorSimilarity::InnerProduct => Self::Dot,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_vector_similarity_from_schema() {
        assert_eq!(
            VectorSimilarity::from(SchemaVectorSimilarity::Cosine),
            VectorSimilarity::Cosine
        );
        assert_eq!(
            VectorSimilarity::from(SchemaVectorSimilarity::L2),
            VectorSimilarity::Euclidean
        );
        assert_eq!(
            VectorSimilarity::from(SchemaVectorSimilarity::InnerProduct),
            VectorSimilarity::Dot
        );
    }
}