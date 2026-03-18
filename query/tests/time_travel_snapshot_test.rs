use std::sync::Arc;

use alayasiki_core::embedding::deterministic_embedding;
use alayasiki_core::model::Node;
use chrono::{Duration as ChronoDuration, Utc};
use query::{QueryEngine, QueryError, QueryRequest};
use storage::community::CommunitySummary;
use storage::repo::Repository;
use tempfile::tempdir;
use tokio::time::{sleep, Duration};

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

#[tokio::test]
async fn time_travel_resolves_rfc3339_to_historical_snapshot() {
    let before_repo = (Utc::now() - ChronoDuration::seconds(1)).to_rfc3339();
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("time_travel_rfc3339_resolution.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    repo.put_node(Node::new(
        1,
        deterministic_embedding("baseline trend", MODEL_ID, DIMS),
        "baseline evidence".to_string(),
    ))
    .await
    .unwrap();
    let first_snapshot_id = repo.current_snapshot_id().await;
    let as_of_first = Utc::now().to_rfc3339();

    sleep(Duration::from_millis(1_100)).await;

    repo.put_node(Node::new(
        2,
        deterministic_embedding("future leaked trend", MODEL_ID, DIMS),
        "future evidence".to_string(),
    ))
    .await
    .unwrap();

    let engine = QueryEngine::new(repo);

    let historical = QueryRequest::parse_json(&format!(
        r#"{{
            "query":"trend",
            "mode":"evidence",
            "search_mode":"local",
            "top_k":10,
            "time_travel":"{}"
        }}"#,
        as_of_first
    ))
    .unwrap();
    let before_creation = QueryRequest::parse_json(&format!(
        r#"{{
            "query":"trend",
            "mode":"evidence",
            "search_mode":"local",
            "top_k":10,
            "time_travel":"{}"
        }}"#,
        before_repo
    ))
    .unwrap();

    let historical_response = engine.execute(historical).await.unwrap();
    let before_creation_err = engine.execute(before_creation).await.unwrap_err();

    let historical_ids: Vec<u64> = historical_response
        .evidence
        .nodes
        .iter()
        .map(|n| n.id)
        .collect();

    assert_eq!(
        historical_response.snapshot_id.as_deref(),
        Some(first_snapshot_id.as_str())
    );
    assert_eq!(
        historical_response.time_travel.as_deref(),
        Some(as_of_first.as_str())
    );
    assert!(historical_ids.contains(&1));
    assert!(!historical_ids.contains(&2));
    assert!(matches!(before_creation_err, QueryError::NotFound(_)));
}

#[tokio::test]
async fn global_time_travel_uses_versioned_community_summary_without_future_leakage() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("global_time_travel_versioned_summary.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    repo.put_node(Node::new(
        1,
        deterministic_embedding("baseline trend", MODEL_ID, DIMS),
        "baseline evidence".to_string(),
    ))
    .await
    .unwrap();
    let as_of_first = Utc::now().to_rfc3339();

    sleep(Duration::from_millis(1_100)).await;

    repo.put_node(Node::new(
        2,
        deterministic_embedding("future leaked trend", MODEL_ID, DIMS),
        "future evidence".to_string(),
    ))
    .await
    .unwrap();

    let summaries = vec![
        CommunitySummary {
            level: 0,
            community_id: 0,
            top_nodes: vec![1],
            summary: "Global synthesis: baseline trend summary".to_string(),
            snapshot_lsn_range: Some((1, 1)),
        },
        CommunitySummary {
            level: 0,
            community_id: 1,
            top_nodes: vec![2],
            summary: "Global synthesis: leaked future trend summary".to_string(),
            snapshot_lsn_range: Some((2, 2)),
        },
    ];
    let engine = QueryEngine::new(repo).with_community_summaries(summaries);

    let request = QueryRequest::parse_json(&format!(
        r#"{{
            "query":"trend",
            "mode":"answer",
            "search_mode":"global",
            "top_k":5,
            "time_travel":"{}"
        }}"#,
        as_of_first
    ))
    .unwrap();

    let response = engine.execute(request).await.unwrap();

    let answer = response.answer.unwrap_or_default();
    assert!(
        answer.contains("baseline trend summary"),
        "versioned summary for the resolved snapshot should be used"
    );
    assert!(
        !answer.contains("leaked future trend summary"),
        "future summary must not bleed into time-travel responses"
    );
    assert!(
        response.evidence.nodes.iter().all(|node| node.id != 2),
        "historical time-travel evidence must exclude post-snapshot nodes"
    );
}
