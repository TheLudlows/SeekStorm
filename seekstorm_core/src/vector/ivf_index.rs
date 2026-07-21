//! IVF 索引管理（Phase 4）。
//!
//! 管理分区集合和质心表，支持分区查找和扫描。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::RwLock;

use crate::storage::lsm::{LsmEngine, LsmValue, NS_VEC};
use crate::vector::partition::{Partition, PartitionId};
use crate::vector::similarity::VectorSimilarity;

/// 质心表。
pub struct CentroidTable {
    pub entries: Vec<(PartitionId, Vec<f32>)>,
}

impl CentroidTable {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// 添加质心。
    pub fn add(&mut self, partition_id: PartitionId, centroid: Vec<f32>) {
        self.entries.push((partition_id, centroid));
    }

    /// 找到距离查询向量最近的分区。
    pub fn find_nearest(
        &self,
        query: &[f32],
        nprobe: usize,
        sim: VectorSimilarity,
    ) -> Vec<PartitionId> {
        if self.entries.is_empty() {
            return Vec::new();
        }

        // 计算所有相似度
        let mut scored: Vec<(PartitionId, f32)> = self
            .entries
            .iter()
            .map(|(id, c)| (*id, crate::vector::similarity::compute_similarity(query, c, sim)))
            .collect();

        // 排序（降序）
        scored.sort_by(|(_, s1), (_, s2)| {
            s2.partial_cmp(s1)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // 返回前 nprobe 个分区 ID
        scored
            .into_iter()
            .take(nprobe)
            .map(|(id, _)| id)
            .collect()
    }
}

/// IVF 索引。
pub struct IvfIndex {
    pub partitions: RwLock<HashMap<PartitionId, Arc<Partition>>>,
    pub centroid_table: RwLock<CentroidTable>,
    pub next_partition_id: AtomicU32,
}

impl IvfIndex {
    pub fn new() -> Self {
        Self {
            partitions: RwLock::new(HashMap::new()),
            centroid_table: RwLock::new(CentroidTable::new()),
            next_partition_id: AtomicU32::new(0),
        }
    }

    /// 添加新分区。
    pub async fn add_partition(&self, centroid: Vec<f32>) -> PartitionId {
        let pid = self.next_partition_id.fetch_add(1, Ordering::SeqCst);
        let partition = Arc::new(Partition::new(pid, centroid));

        let mut partitions = self.partitions.write().await;
        partitions.insert(pid, partition);
        pid
    }

    /// 扫描分区，返回所有向量（doc_id, quantized_bytes）。
    pub async fn scan_partition(
        &self,
        partition_id: PartitionId,
        lsm: &LsmEngine,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let entries = lsm.scan_prefix(NS_VEC, partition_id).await?;

        let mut results = Vec::new();
        for (key, value) in entries {
            if let LsmValue::Data(bytes) = value {
                results.push((key.doc_id, bytes));
            }
        }

        Ok(results)
    }

    /// 获取分区总数。
    pub async fn partition_count(&self) -> usize {
        self.partitions.read().await.len()
    }

    /// 获取分区引用。
    pub async fn get_partition(&self, partition_id: PartitionId) -> Option<Arc<Partition>> {
        self.partitions.read().await.get(&partition_id).cloned()
    }

    /// 更新分区质心表。
    pub async fn update_centroid_table(&self, new_table: CentroidTable) {
        *self.centroid_table.write().await = new_table;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_centroid_table_new() {
        let table = CentroidTable::new();
        assert_eq!(table.entries.len(), 0);
    }

    #[test]
    fn test_centroid_table_add() {
        let mut table = CentroidTable::new();
        table.add(0, vec![0.5, 0.5]);
        table.add(1, vec![1.0, 0.0]);

        assert_eq!(table.entries.len(), 2);
        assert_eq!(table.entries[0].0, 0);
        assert_eq!(table.entries[1].0, 1);
    }

    #[test]
    fn test_centroid_table_find_nearest() {
        let mut table = CentroidTable::new();
        table.add(0, vec![1.0, 0.0]);
        table.add(1, vec![0.0, 1.0]);
        table.add(2, vec![0.5, 0.0]);

        let query = vec![1.0, 0.1];
        let nearest = table.find_nearest(&query, 2, VectorSimilarity::Cosine);

        assert_eq!(nearest.len(), 2);
        // [1, 0] 应该最接近 [1, 0.1]
        assert!(nearest.contains(&0));
    }

    #[tokio::test]
    async fn test_ivf_index_new() {
        let ivf = IvfIndex::new();
        assert_eq!(ivf.partition_count().await, 0);
    }

    #[tokio::test]
    async fn test_ivf_index_add_partition() {
        let ivf = IvfIndex::new();
        let pid = ivf.add_partition(vec![0.0; 10]).await;
        assert_eq!(pid, 0);

        let pid2 = ivf.add_partition(vec![1.0; 10]).await;
        assert_eq!(pid2, 1);

        assert_eq!(ivf.partition_count().await, 2);
    }

    #[tokio::test]
    async fn test_ivf_index_get_partition() {
        let ivf = IvfIndex::new();
        let pid = ivf.add_partition(vec![0.0; 10]).await;

        let partition = ivf.get_partition(pid).await;
        assert!(partition.is_some());
        assert_eq!(partition.unwrap().id, pid);

        assert!(ivf.get_partition(999).await.is_none());
    }

    #[tokio::test]
    async fn test_ivf_index_scan_partition_empty() {
        let dir = tempdir().unwrap();
        let lsm = LsmEngine::open(dir.path(), Default::default())
            .await
            .unwrap();
        let ivf = IvfIndex::new();

        let results = ivf.scan_partition(0, &lsm).await.unwrap();
        assert_eq!(results.len(), 0);
    }
}