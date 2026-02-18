use std::sync::Arc;

use alayasiki_core::embedding::deterministic_embedding;
use alayasiki_core::model::Node;
use query::{QueryEngine, QueryError, QueryRequest};
use storage::repo::Repository;
use tempfile::tempdir;

const MODEL_ID: &str = "embedding-default-v1";
const DIMS: usize = 8;

#[tokio::test]
async fn pinned_snapshot_uses_historical_state() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("time_travel_snapshot_view.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    repo.put_node(Node::new(
        1,
        deterministic_embedding("energy policy baseline", MODEL_ID, DIMS),
        "baseline evidence".to_string(),
    ))
    .await
    .unwrap();
    let snapshot_id = repo.current_snapshot_id().await;

    repo.put_node(Node::new(
        2,
        deterministic_embedding("energy policy updated", MODEL_ID, DIMS),
        "new evidence after snapshot".to_string(),
    ))
    .await
    .unwrap();

    let engine = QueryEngine::new(repo);

    let pinned = QueryRequest::parse_json(&format!(
        r#"{{
            "query":"energy policy updated",
            "mode":"evidence",
            "search_mode":"local",
            "top_k":10,
            "snapshot_id":"{}"
        }}"#,
        snapshot_id
    ))
    .unwrap();
    let latest = QueryRequest::parse_json(
        r#"{
            "query":"energy policy updated",
            "mode":"evidence",
            "search_mode":"local",
            "top_k":10
        }"#,
    )
    .unwrap();

    let pinned_response = engine.execute(pinned).await.unwrap();
    let latest_response = engine.execute(latest).await.unwrap();

    let pinned_ids: Vec<u64> = pinned_response
        .evidence
        .nodes
        .iter()
        .map(|n| n.id)
        .collect();
    let latest_ids: Vec<u64> = latest_response
        .evidence
        .nodes
        .iter()
        .map(|n| n.id)
        .collect();

    assert!(pinned_ids.contains(&1));
    assert!(!pinned_ids.contains(&2));
    assert!(latest_ids.contains(&2));
    assert_eq!(
        pinned_response.snapshot_id.as_deref(),
        Some(snapshot_id.as_str())
    );
}

#[tokio::test]
async fn unknown_snapshot_id_returns_not_found() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("time_travel_snapshot_not_found.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());
    repo.put_node(Node::new(
        1,
        deterministic_embedding("energy policy baseline", MODEL_ID, DIMS),
        "baseline evidence".to_string(),
    ))
    .await
    .unwrap();

    let engine = QueryEngine::new(repo);
    let request = QueryRequest::parse_json(
        r#"{
            "query":"energy policy",
            "mode":"evidence",
            "search_mode":"local",
            "top_k":5,
            "snapshot_id":"wal-lsn-99"
        }"#,
    )
    .unwrap();

    let err = engine.execute(request).await.unwrap_err();
    assert!(matches!(err, QueryError::NotFound(_)));
}
