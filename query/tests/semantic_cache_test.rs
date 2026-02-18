use std::sync::Arc;

use alayasiki_core::model::{Edge, Node};
use query::{QueryEngine, QueryRequest};
use storage::repo::Repository;
use tempfile::TempDir;

async fn seeded_repo() -> (TempDir, Arc<Repository>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal_path = dir.path().join("semantic_cache.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.expect("repo open"));

    let mut toyota = Node::new(
        1,
        vec![1.0, 0.0],
        "Toyota expands EV production and battery partnerships".to_string(),
    );
    toyota
        .metadata
        .insert("entity_type".to_string(), "Company".to_string());

    let mut honda = Node::new(
        2,
        vec![0.9, 0.1],
        "Honda updates EV roadmap with new battery procurement".to_string(),
    );
    honda
        .metadata
        .insert("entity_type".to_string(), "Company".to_string());

    repo.put_node(toyota).await.expect("put node toyota");
    repo.put_node(honda).await.expect("put node honda");
    repo.put_edge(Edge::new(1, 2, "competitor_of", 0.8))
        .await
        .expect("put edge");

    (dir, repo)
}

#[tokio::test]
async fn semantic_cache_hits_for_semantically_equivalent_query() {
    let (_dir, repo) = seeded_repo().await;
    let engine = QueryEngine::new(repo);

    let first_request = QueryRequest::parse_json(
        r#"{
            "query": "Toyota EV strategy in 2024",
            "mode": "evidence",
            "search_mode": "local",
            "top_k": 5,
            "traversal": {"depth": 2}
        }"#,
    )
    .expect("first request parse");
    let second_request = QueryRequest::parse_json(
        r#"{
            "query": "2024 Toyota EV strategy overview",
            "mode": "evidence",
            "search_mode": "local",
            "top_k": 5,
            "traversal": {"depth": 2}
        }"#,
    )
    .expect("second request parse");

    let first = engine.execute(first_request).await.expect("first execute");
    assert!(!first
        .explain
        .steps
        .iter()
        .any(|step| step == "semantic_cache_hit"));

    let second = engine
        .execute(second_request)
        .await
        .expect("second execute");
    assert!(second
        .explain
        .steps
        .iter()
        .any(|step| step == "semantic_cache_hit"));

    assert_eq!(first.snapshot_id, second.snapshot_id);
    assert_eq!(first.evidence.nodes, second.evidence.nodes);
    assert_eq!(first.evidence.edges, second.evidence.edges);
}

#[tokio::test]
async fn semantic_cache_does_not_cross_snapshot_boundaries() {
    let (_dir, repo) = seeded_repo().await;
    let engine = QueryEngine::new(repo.clone());

    let request = QueryRequest::parse_json(
        r#"{
            "query": "Toyota EV strategy",
            "mode": "evidence",
            "search_mode": "local",
            "top_k": 5
        }"#,
    )
    .expect("request parse");

    let first = engine
        .execute(request.clone())
        .await
        .expect("first execute");
    assert!(!first
        .explain
        .steps
        .iter()
        .any(|step| step == "semantic_cache_hit"));

    repo.put_node(Node::new(
        3,
        vec![0.7, 0.3],
        "Policy update unrelated to automotive strategy".to_string(),
    ))
    .await
    .expect("put noise node");

    let second = engine.execute(request).await.expect("second execute");
    assert!(!second
        .explain
        .steps
        .iter()
        .any(|step| step == "semantic_cache_hit"));

    assert_ne!(first.snapshot_id, second.snapshot_id);
}
