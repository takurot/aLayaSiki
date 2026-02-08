use std::sync::Arc;

use alayasiki_core::model::{Edge, Node};
use query::{QueryEngine, QueryMode, QueryPlanner, QueryRequest, SearchMode};
use storage::repo::Repository;
use tempfile::TempDir;

async fn seeded_repo() -> (TempDir, Arc<Repository>) {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("query.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    let mut toyota = Node::new(
        1,
        vec![1.0, 0.0],
        "Toyota expands EV production and battery partnerships".to_string(),
    );
    toyota
        .metadata
        .insert("entity_type".to_string(), "Company".to_string());
    toyota
        .metadata
        .insert("timestamp".to_string(), "2024-02-10".to_string());
    toyota
        .metadata
        .insert("source".to_string(), "s3://corp/toyota".to_string());

    let mut meta = Node::new(
        2,
        vec![0.92, 0.08],
        "Meta shifts strategy after EV headset market pressure".to_string(),
    );
    meta.metadata
        .insert("entity_type".to_string(), "Company".to_string());
    meta.metadata
        .insert("timestamp".to_string(), "2024-05-12".to_string());
    meta.metadata
        .insert("source".to_string(), "s3://corp/meta".to_string());

    let mut policy = Node::new(
        3,
        vec![0.05, 0.95],
        "Government policy introduces battery recycling standards".to_string(),
    );
    policy
        .metadata
        .insert("entity_type".to_string(), "Policy".to_string());
    policy
        .metadata
        .insert("timestamp".to_string(), "2022-07-01".to_string());
    policy
        .metadata
        .insert("source".to_string(), "s3://policy/recycle".to_string());

    repo.put_node(toyota).await.unwrap();
    repo.put_node(meta).await.unwrap();
    repo.put_node(policy).await.unwrap();

    repo.put_edge(Edge::new(1, 2, "competitor_of", 0.9))
        .await
        .unwrap();
    repo.put_edge(Edge::new(2, 3, "influenced_by", 0.6))
        .await
        .unwrap();

    (dir, repo)
}

#[test]
fn test_json_dsl_parser_defaults_and_validation() {
    let request = QueryRequest::parse_json(r#"{"query":"トヨタのEV戦略"}"#).unwrap();
    assert_eq!(request.top_k, 20);
    assert_eq!(request.mode, QueryMode::Answer);
    assert_eq!(request.search_mode, SearchMode::Auto);
    assert_eq!(request.traversal.depth, 1);
    assert!(request.validate().is_ok());

    let invalid = QueryRequest::parse_json(r#"{"query":"x","top_k":0}"#).unwrap();
    assert!(invalid.validate().is_err());

    let unknown_mode = QueryRequest::parse_json(r#"{"query":"x","search_mode":"unknown"}"#);
    assert!(unknown_mode.is_err());
}

#[test]
fn test_query_planner_auto_mode_chooses_global_for_theme_queries() {
    let request = QueryRequest::parse_json(
        r#"{"query":"このデータセットの主要テーマを総括して","search_mode":"auto"}"#,
    )
    .unwrap();

    let plan = QueryPlanner::plan(&request);
    assert_eq!(plan.effective_search_mode, SearchMode::Global);
    assert_eq!(
        plan.steps,
        vec!["vector_search", "graph_expansion", "context_pruning"]
    );
}

#[tokio::test]
async fn test_query_mode_switch_between_answer_and_evidence() {
    let (_dir, repo) = seeded_repo().await;
    let engine = QueryEngine::new(repo);

    let answer_request = QueryRequest::parse_json(
        r#"{
            "query": "ToyotaとMetaの差分は?",
            "mode": "answer",
            "search_mode": "local",
            "top_k": 5,
            "traversal": {"depth": 2}
        }"#,
    )
    .unwrap();

    let answer_response = engine.execute(answer_request).await.unwrap();
    assert!(answer_response.answer.is_some());
    assert!(!answer_response.evidence.nodes.is_empty());

    let evidence_request = QueryRequest::parse_json(
        r#"{
            "query": "ToyotaとMetaの差分は?",
            "mode": "evidence",
            "search_mode": "local",
            "top_k": 5,
            "traversal": {"depth": 2}
        }"#,
    )
    .unwrap();

    let evidence_response = engine.execute(evidence_request).await.unwrap();
    assert!(evidence_response.answer.is_none());
    assert!(!evidence_response.evidence.nodes.is_empty());
}

#[tokio::test]
async fn test_query_engine_returns_explain_plan_with_anchors_paths_and_exclusions() {
    let (_dir, repo) = seeded_repo().await;
    let engine = QueryEngine::new(repo);

    let request = QueryRequest::parse_json(
        r#"{
            "query": "トヨタの競合と背景要因",
            "mode": "evidence",
            "search_mode": "local",
            "top_k": 3,
            "traversal": {"depth": 2},
            "filters": {
                "relation_type": ["competitor_of"]
            }
        }"#,
    )
    .unwrap();

    let response = engine.execute(request).await.unwrap();
    assert_eq!(
        response.explain.steps,
        vec!["vector_search", "graph_expansion", "context_pruning"]
    );
    assert!(!response.explain.anchors.is_empty());
    assert!(!response.explain.expansion_paths.is_empty());
    assert!(!response.explain.exclusions.is_empty());

    assert!(!response.evidence.edges.is_empty());
    assert!(response
        .evidence
        .edges
        .iter()
        .all(|edge| edge.relation == "competitor_of"));
}

#[tokio::test]
async fn test_query_engine_applies_entity_and_time_range_filters() {
    let (_dir, repo) = seeded_repo().await;
    let engine = QueryEngine::new(repo);

    let request = QueryRequest::parse_json(
        r#"{
            "query": "EV戦略の比較",
            "mode": "evidence",
            "search_mode": "local",
            "top_k": 10,
            "traversal": {"depth": 3},
            "filters": {
                "entity_type": ["Company"],
                "time_range": {"from": "2024-01-01", "to": "2024-12-31"}
            }
        }"#,
    )
    .unwrap();

    let response = engine.execute(request).await.unwrap();
    let node_ids: Vec<u64> = response.evidence.nodes.iter().map(|n| n.id).collect();

    assert!(node_ids.contains(&1));
    assert!(node_ids.contains(&2));
    assert!(!node_ids.contains(&3));
    assert!(response
        .explain
        .exclusions
        .iter()
        .any(|ex| ex.node_id == Some(3)));
}
