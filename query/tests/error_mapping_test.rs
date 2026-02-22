use std::sync::Arc;
use query::QueryEngine;
use alayasiki_core::error::{ErrorCode, AlayasikiError};
use storage::repo::Repository;
use tempfile::tempdir;

#[tokio::test]
async fn test_error_mapping_standard_categories() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("error_test.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());
    let engine = QueryEngine::new(repo);

    // Case 1: INVALID_ARGUMENT (Invalid top_k)
    let bad_json = r#"{"query": "test", "top_k": 0}"#; // 0 is invalid top_k in validate()
    let result = engine.execute_json(bad_json).await;
    match result {
        Err(err) => {
            assert_eq!(err.error_code(), ErrorCode::InvalidArgument);
            let response = err.to_response();
            assert_eq!(response.error_code, Some(ErrorCode::InvalidArgument));
            assert!(response.answer.unwrap().contains("invalid query"));
        },
        Ok(_) => panic!("Expected error for top_k=0"),
    }

    // Case 2: NOT_FOUND (Snapshot not found)
    let missing_snap = r#"{"query": "test", "snapshot_id": "wal-lsn-999999"}"#;
    let result = engine.execute_json(missing_snap).await;
    match result {
        Err(err) => {
            assert_eq!(err.error_code(), ErrorCode::NotFound);
            let response = err.to_response();
            assert_eq!(response.error_code, Some(ErrorCode::NotFound));
            assert!(response.answer.unwrap().contains("snapshot_id"));
        },
        Ok(_) => panic!("Expected error for missing snapshot"),
    }

    // Check metrics
    let metrics = engine.metrics();
    assert_eq!(metrics.total_queries, 2);
}

#[tokio::test]
async fn test_metrics_p95_p99_calculation() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("metrics_test.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());
    let engine = QueryEngine::new(repo);

    let query = r#"{"query": "test"}"#;
    for _ in 0..10 {
        let _ = engine.execute_json(query).await;
    }

    let metrics = engine.metrics();
    assert_eq!(metrics.total_queries, 10);
    assert!(metrics.p50 > 0 || metrics.history_count > 0);
    assert!(metrics.p95 >= metrics.p50);
    assert!(metrics.p99 >= metrics.p95);
}

#[tokio::test]
async fn test_slm_and_gpu_metrics() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("slm_metrics_test.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());
    let engine = QueryEngine::new(repo);

    let metrics_collector = engine.metrics_collector();
    metrics_collector.record_slm_extraction(0.95);
    metrics_collector.record_slm_extraction(0.85);
    metrics_collector.set_gpu_usage(1024);

    let snapshot = engine.metrics();
    assert_eq!(snapshot.avg_extraction_confidence, 0.9);
    assert_eq!(snapshot.gpu_vram_usage_mb, 1024);
}
