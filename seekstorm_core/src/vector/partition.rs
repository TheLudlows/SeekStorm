//! IVF 分区管理（Phase 4）。
//!
//! 每个分区包含一个质心和向量计数，以及墓碑集合。

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;

/// 分区 ID 类型。
pub type PartitionId = u32;

/// 向量分区 ID 类型别名（与 PartitionId 相同）。
pub type VectorPartitionId = PartitionId;

/// IVF 分区。
pub struct Partition {
    pub id: PartitionId,
    pub centroid: RwLock<Vec<f32>>,
    pub vector_count: AtomicU64,
    pub tombstones: RwLock<HashSet<u64>>,
}

impl Partition {
    /// 创建新分区。
    pub fn new(id: PartitionId, centroid: Vec<f32>) -> Self {
        Self {
            id,
            centroid: RwLock::new(centroid),
            vector_count: AtomicU64::new(0),
            tombstones: RwLock::new(HashSet::new()),
        }
    }

    /// 添加向量（计数）。
    pub async fn add_vector(&self, _doc_id: u64) {
        self.vector_count.fetch_add(1, Ordering::SeqCst);
    }

    /// 删除向量（写墓碑）。
    pub async fn delete_vector(&self, doc_id: u64) {
        self.tombstones.write().await.insert(doc_id);
    }

    /// 获取向量计数。
    pub async fn vector_count(&self) -> u64 {
        self.vector_count.load(Ordering::SeqCst)
    }

    /// 检查向量是否被删除。
    pub async fn is_tombstoned(&self, doc_id: u64) -> bool {
        self.tombstones.read().await.contains(&doc_id)
    }

    /// 更新质心。
    pub async fn update_centroid(&self, new_centroid: Vec<f32>) {
        *self.centroid.write().await = new_centroid;
    }

    /// 获取质心副本。
    pub async fn centroid(&self) -> Vec<f32> {
        self.centroid.read().await.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_partition_new() {
        let centroid = vec![0.5, 0.5, 0.5];
        let partition = Partition::new(0, centroid.clone());
        assert_eq!(partition.id, 0);
        assert_eq!(partition.vector_count().await, 0);
        assert_eq!(partition.tombstones.read().await.len(), 0);
        assert_eq!(partition.centroid().await, centroid);
    }

    #[tokio::test]
    async fn test_partition_add_vector() {
        let partition = Partition::new(1, vec![0.0; 10]);
        partition.add_vector(100).await;
        assert_eq!(partition.vector_count().await, 1);

        partition.add_vector(200).await;
        assert_eq!(partition.vector_count().await, 2);
    }

    #[tokio::test]
    async fn test_partition_delete_vector() {
        let partition = Partition::new(2, vec![0.0; 10]);
        partition.add_vector(100).await;
        assert!(!partition.is_tombstoned(100).await);

        partition.delete_vector(100).await;
        assert!(partition.is_tombstoned(100).await);
    }

    #[tokio::test]
    async fn test_partition_update_centroid() {
        let partition = Partition::new(3, vec![0.0, 0.0]);
        assert_eq!(partition.centroid().await, vec![0.0, 0.0]);

        partition.update_centroid(vec![1.0, 2.0]).await;
        assert_eq!(partition.centroid().await, vec![1.0, 2.0]);
    }
}