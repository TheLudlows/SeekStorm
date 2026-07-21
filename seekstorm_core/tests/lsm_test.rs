//! Phase 1 集成测试：验证 LsmEngine 的 put/get/delete/scan/compaction/WAL 恢复。
//!
//! 对应 V4_PHASED_IMPLEMENTATION.md §Phase 1 测试用例。

use seekstorm_core::{
    IoBackendKind, LsmConfig, LsmEngine, LsmKey, LsmValue, NS_DOC, NS_VEC, WalSync,
};
use tempfile::tempdir;

#[tokio::test]
async fn test_put_get_basic() {
    let dir = tempdir().unwrap();
    let lsm = LsmEngine::open(dir.path(), LsmConfig::default())
        .await
        .unwrap();
    let key = LsmKey {
        namespace: 0x01,
        partition_or_segment: 0,
        doc_id: 1,
        lsn: 0,
    };
    lsm.put(key.clone(), LsmValue::Data(b"hello".to_vec()))
        .await
        .unwrap();
    let v = lsm.get(&key).await.unwrap();
    assert!(
        matches!(v, Some(LsmValue::Data(ref b)) if b == b"hello"),
        "expected Data(hello), got {:?}",
        v
    );
}

#[tokio::test]
async fn test_tombstone() {
    let dir = tempdir().unwrap();
    let lsm = LsmEngine::open(dir.path(), LsmConfig::default())
        .await
        .unwrap();
    lsm.put(LsmKey::doc(1), LsmValue::Data(b"hello".to_vec()))
        .await
        .unwrap();
    lsm.delete(0x01, 1).await.unwrap();
    let v = lsm.get(&LsmKey::doc(1)).await.unwrap();
    assert!(
        matches!(v, Some(LsmValue::Tombstone)),
        "expected Tombstone, got {:?}",
        v
    );
}

#[tokio::test]
async fn test_compaction_drops_tombstone() {
    let mut cfg = LsmConfig::default();
    cfg.memtable_max_bytes = 1024; // 小阈值触发 flush
    cfg.sstable_compact_threshold = 2;
    let dir = tempdir().unwrap();
    let lsm = LsmEngine::open(dir.path(), cfg).await.unwrap();

    lsm.put(LsmKey::doc(1), LsmValue::Data(b"v1".to_vec()))
        .await
        .unwrap();
    lsm.force_flush().await.unwrap();
    lsm.delete(0x01, 1).await.unwrap();
    lsm.force_flush().await.unwrap();
    lsm.force_compact().await.unwrap();

    let v = lsm.get(&LsmKey::doc(1)).await.unwrap();
    assert!(v.is_none(), "tombstone 已被清理，got {:?}", v);
}

#[tokio::test]
async fn test_scan_prefix() {
    let dir = tempdir().unwrap();
    let lsm = LsmEngine::open(dir.path(), LsmConfig::default())
        .await
        .unwrap();
    lsm.put(LsmKey::vec(5, 1), LsmValue::Data(b"v1".to_vec()))
        .await
        .unwrap();
    lsm.put(LsmKey::vec(5, 2), LsmValue::Data(b"v2".to_vec()))
        .await
        .unwrap();
    lsm.put(LsmKey::vec(7, 3), LsmValue::Data(b"v3".to_vec()))
        .await
        .unwrap();

    let results = lsm.scan_prefix(NS_VEC, 5).await.unwrap();
    assert_eq!(results.len(), 2, "expected 2, got {}", results.len());
    let results = lsm.scan_prefix(NS_VEC, 7).await.unwrap();
    assert_eq!(results.len(), 1);
    let results = lsm.scan_prefix(NS_VEC, 999).await.unwrap();
    assert_eq!(results.len(), 0);
}

#[tokio::test]
async fn test_wal_recovery() {
    let dir = tempdir().unwrap();
    {
        let lsm = LsmEngine::open(dir.path(), LsmConfig::default())
            .await
            .unwrap();
        lsm.put(LsmKey::doc(1), LsmValue::Data(b"v1".to_vec()))
            .await
            .unwrap();
        lsm.put(LsmKey::doc(2), LsmValue::Data(b"v2".to_vec()))
            .await
            .unwrap();
        // 显式 drop 触发 WAL 关闭（EveryCommit 已 fsync）。
        drop(lsm);
    }

    // 重启后恢复
    let lsm = LsmEngine::open(dir.path(), LsmConfig::default())
        .await
        .unwrap();
    let v1 = lsm.get(&LsmKey::doc(1)).await.unwrap();
    let v2 = lsm.get(&LsmKey::doc(2)).await.unwrap();
    assert!(v1.is_some(), "doc 1 should exist after recovery");
    assert!(v2.is_some(), "doc 2 should exist after recovery");
    assert!(
        matches!(v1, Some(LsmValue::Data(ref b)) if b == b"v1"),
        "doc 1 value mismatch: {:?}",
        v1
    );
    assert!(
        matches!(v2, Some(LsmValue::Data(ref b)) if b == b"v2"),
        "doc 2 value mismatch: {:?}",
        v2
    );
}

#[tokio::test]
async fn test_wal_recovery_with_tombstone() {
    // 额外测试：删除后重启，墓碑应仍可见。
    let dir = tempdir().unwrap();
    {
        let lsm = LsmEngine::open(dir.path(), LsmConfig::default())
            .await
            .unwrap();
        lsm.put(LsmKey::doc(10), LsmValue::Data(b"hello".to_vec()))
            .await
            .unwrap();
        lsm.delete(NS_DOC, 10).await.unwrap();
        drop(lsm);
    }

    let lsm = LsmEngine::open(dir.path(), LsmConfig::default())
        .await
        .unwrap();
    let v = lsm.get(&LsmKey::doc(10)).await.unwrap();
    assert!(
        matches!(v, Some(LsmValue::Tombstone)),
        "tombstone should survive recovery, got {:?}",
        v
    );
}

#[tokio::test]
async fn test_overwrite_latest_wins() {
    // 额外测试：同一 key 多次写入，get 返回最新版本。
    let dir = tempdir().unwrap();
    let lsm = LsmEngine::open(dir.path(), LsmConfig::default())
        .await
        .unwrap();
    lsm.put(LsmKey::doc(1), LsmValue::Data(b"v1".to_vec()))
        .await
        .unwrap();
    lsm.put(LsmKey::doc(1), LsmValue::Data(b"v2".to_vec()))
        .await
        .unwrap();
    lsm.put(LsmKey::doc(1), LsmValue::Data(b"v3".to_vec()))
        .await
        .unwrap();
    let v = lsm.get(&LsmKey::doc(1)).await.unwrap().unwrap();
    assert!(matches!(v, LsmValue::Data(ref b) if b == b"v3"));
}

#[tokio::test]
async fn test_flush_then_get() {
    // 额外测试：flush 后从 SSTable 读取。
    let dir = tempdir().unwrap();
    let lsm = LsmEngine::open(dir.path(), LsmConfig::default())
        .await
        .unwrap();
    for i in 0..50u64 {
        lsm.put(LsmKey::doc(i), LsmValue::Data(format!("v{}", i).into_bytes()))
            .await
            .unwrap();
    }
    lsm.force_flush().await.unwrap();
    assert!(lsm.sstable_count().await >= 1);
    for i in 0..50u64 {
        let v = lsm.get(&LsmKey::doc(i)).await.unwrap().unwrap();
        assert!(
            matches!(v, LsmValue::Data(ref b) if b == &format!("v{}", i).into_bytes()),
            "doc {} mismatch after flush",
            i
        );
    }
}

#[tokio::test]
async fn test_compaction_merges_duplicates() {
    // 额外测试：同一 key 多次 flush 后 compaction，应只保留最新版本。
    let mut cfg = LsmConfig::default();
    cfg.sstable_compact_threshold = 2;
    let dir = tempdir().unwrap();
    let lsm = LsmEngine::open(dir.path(), cfg).await.unwrap();

    lsm.put(LsmKey::doc(1), LsmValue::Data(b"v1".to_vec()))
        .await
        .unwrap();
    lsm.force_flush().await.unwrap();
    lsm.put(LsmKey::doc(1), LsmValue::Data(b"v2".to_vec()))
        .await
        .unwrap();
    lsm.force_flush().await.unwrap();
    lsm.force_compact().await.unwrap();

    assert_eq!(lsm.sstable_count().await, 1);
    let v = lsm.get(&LsmKey::doc(1)).await.unwrap().unwrap();
    assert!(matches!(v, LsmValue::Data(ref b) if b == b"v2"));
}

#[tokio::test]
async fn test_wal_sync_none_still_recovers_from_memtable() {
    // 即使 WalSync::None（依赖 OS 页缓存），重新打开时也尝试从 WAL 重放。
    let mut cfg = LsmConfig::default();
    cfg.wal_sync = WalSync::None;
    let dir = tempdir().unwrap();
    {
        let lsm = LsmEngine::open(dir.path(), cfg.clone()).await.unwrap();
        lsm.put(LsmKey::doc(42), LsmValue::Data(b"answer".to_vec()))
            .await
            .unwrap();
        drop(lsm);
    }
    let lsm = LsmEngine::open(dir.path(), cfg).await.unwrap();
    // WalSync::None 下 OS 可能未刷盘，但通常小数据仍在页缓存中。
    let v = lsm.get(&LsmKey::doc(42)).await.unwrap();
    assert!(v.is_some(), "expected recovery with WalSync::None");
}

#[tokio::test]
async fn test_io_backend_kind_default() {
    // 确认默认配置使用 AsyncFs 后端。
    let cfg = LsmConfig::default();
    assert!(matches!(cfg.io_backend, IoBackendKind::AsyncFs));
}
