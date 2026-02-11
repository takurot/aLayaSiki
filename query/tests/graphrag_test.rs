// PR-08: GraphRAG Inference Pipeline Tests (TDD - Red phase)

use std::sync::Arc;

use alayasiki_core::embedding::deterministic_embedding;
use alayasiki_core::model::{Edge, Node};
use query::engine::QueryEngine;
use query::graphrag::{compute_groundedness, GroundednessInput};
use query::{QueryRequest, SearchMode};
use storage::community::{CommunityEngine, CommunitySummary, DeterministicSummarizer};
use storage::repo::Repository;
use tempfile::TempDir;

const DIMS: usize = 8;
const MODEL_ID: &str = "embedding-default-v1";

/// Build a rich repo with multiple communities for GraphRAG tests.
/// Community A: Toyota(1), Honda(2), BYD(3) — EV companies
/// Community B: FDA(4), EPA(5) — regulatory bodies
/// Community C: MIT(6), Stanford(7) — universities
/// Edges create a clear community structure.
async fn graphrag_repo() -> (TempDir, Arc<Repository>, Vec<CommunitySummary>) {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("graphrag.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    // Community A: EV companies
    let mut toyota = Node::new(
        1,
        deterministic_embedding("EV production", MODEL_ID, DIMS),
        "Toyota leads EV production with new battery technology".to_string(),
    );
    toyota
        .metadata
        .insert("entity_type".to_string(), "Company".to_string());
    toyota
        .metadata
        .insert("source".to_string(), "report/toyota.pdf".to_string());

    let mut honda = Node::new(
        2,
        deterministic_embedding("EV strategy", MODEL_ID, DIMS),
        "Honda announces partnership for solid-state batteries".to_string(),
    );
    honda
        .metadata
        .insert("entity_type".to_string(), "Company".to_string());
    honda
        .metadata
        .insert("source".to_string(), "report/honda.pdf".to_string());

    let mut byd = Node::new(
        3,
        deterministic_embedding("EV market expansion", MODEL_ID, DIMS),
        "BYD expands to European EV market with affordable models".to_string(),
    );
    byd.metadata
        .insert("entity_type".to_string(), "Company".to_string());
    byd.metadata
        .insert("source".to_string(), "report/byd.pdf".to_string());

    // Community B: Regulators
    let mut fda = Node::new(
        4,
        deterministic_embedding("battery safety regulation", MODEL_ID, DIMS),
        "FDA updates battery safety standards for consumer electronics".to_string(),
    );
    fda.metadata
        .insert("entity_type".to_string(), "Regulator".to_string());
    fda.metadata
        .insert("source".to_string(), "gov/fda.pdf".to_string());

    let mut epa = Node::new(
        5,
        deterministic_embedding("emission standards", MODEL_ID, DIMS),
        "EPA tightens emission standards accelerating EV adoption".to_string(),
    );
    epa.metadata
        .insert("entity_type".to_string(), "Regulator".to_string());
    epa.metadata
        .insert("source".to_string(), "gov/epa.pdf".to_string());

    // Community C: Universities
    let mut mit = Node::new(
        6,
        deterministic_embedding("battery research", MODEL_ID, DIMS),
        "MIT publishes breakthrough in lithium-sulfur battery research".to_string(),
    );
    mit.metadata
        .insert("entity_type".to_string(), "University".to_string());
    mit.metadata
        .insert("source".to_string(), "acad/mit.pdf".to_string());

    let mut stanford = Node::new(
        7,
        deterministic_embedding("autonomous driving", MODEL_ID, DIMS),
        "Stanford advances autonomous driving with new sensor fusion".to_string(),
    );
    stanford
        .metadata
        .insert("entity_type".to_string(), "University".to_string());
    stanford
        .metadata
        .insert("source".to_string(), "acad/stanford.pdf".to_string());

    // Insert all nodes
    for node in [toyota, honda, byd, fda, epa, mit, stanford] {
        repo.put_node(node).await.unwrap();
    }

    // Intra-community edges (strong links)
    repo.put_edge(Edge::new(1, 2, "competitor_of", 0.9))
        .await
        .unwrap();
    repo.put_edge(Edge::new(1, 3, "competitor_of", 0.85))
        .await
        .unwrap();
    repo.put_edge(Edge::new(2, 3, "competitor_of", 0.8))
        .await
        .unwrap();
    repo.put_edge(Edge::new(4, 5, "collaborates_with", 0.7))
        .await
        .unwrap();
    repo.put_edge(Edge::new(6, 7, "collaborates_with", 0.75))
        .await
        .unwrap();

    // Inter-community edges (weak links)
    repo.put_edge(Edge::new(5, 1, "regulates", 0.5))
        .await
        .unwrap();
    repo.put_edge(Edge::new(6, 1, "supplies_research", 0.4))
        .await
        .unwrap();

    // Build community summaries using CommunityEngine
    let graph = {
        let index = repo.hyper_index.read().await;
        index.graph_index.clone()
    };
    let mut community_engine = CommunityEngine::new(graph);
    community_engine.rebuild_hierarchy(2, &DeterministicSummarizer);
    let summaries = community_engine.summaries().to_vec();

    (dir, repo, summaries)
}

// ---------------------------------------------------------------------------
// 1. Vector Search Anchor Identification
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_vector_search_identifies_anchors_by_similarity() {
    let (_dir, repo, summaries) = graphrag_repo().await;
    let engine = QueryEngine::new(repo).with_community_summaries(summaries);

    let request = QueryRequest::parse_json(
        r#"{
            "query": "EV production",
            "mode": "evidence",
            "search_mode": "local",
            "top_k": 3
        }"#,
    )
    .unwrap();

    let response = engine.execute(request).await.unwrap();

    // Anchors must be present and sorted by score (highest first)
    assert!(!response.explain.anchors.is_empty());
    let anchor_scores: Vec<f32> = response.explain.anchors.iter().map(|a| a.score).collect();
    for w in anchor_scores.windows(2) {
        assert!(w[0] >= w[1], "anchors should be ordered by score desc");
    }
}

// ---------------------------------------------------------------------------
// 2. Graph Expansion Hop Traversal
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_graph_expansion_discovers_multi_hop_nodes() {
    let (_dir, repo, summaries) = graphrag_repo().await;
    let engine = QueryEngine::new(repo).with_community_summaries(summaries);

    let request = QueryRequest::parse_json(
        r#"{
            "query": "EV production",
            "mode": "evidence",
            "search_mode": "local",
            "top_k": 10,
            "traversal": {"depth": 2}
        }"#,
    )
    .unwrap();

    let response = engine.execute(request).await.unwrap();

    // With depth=2 starting from Toyota (node 1), we should reach
    // competitors (hop 1) and regulators/universities (hop 2)
    assert!(
        response.evidence.nodes.iter().any(|n| n.hop >= 1),
        "should discover nodes beyond hop 0"
    );
    assert!(
        !response.explain.expansion_paths.is_empty(),
        "expansion paths should be recorded"
    );
}

// ---------------------------------------------------------------------------
// 3. Context Pruning
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_context_pruning_removes_noise_nodes() {
    let (_dir, repo, summaries) = graphrag_repo().await;
    let engine = QueryEngine::new(repo).with_community_summaries(summaries);

    let request = QueryRequest::parse_json(
        r#"{
            "query": "EV production",
            "mode": "evidence",
            "search_mode": "local",
            "top_k": 2,
            "traversal": {"depth": 3}
        }"#,
    )
    .unwrap();

    let response = engine.execute(request).await.unwrap();

    // top_k = 2 means at most 2 evidence nodes, rest should be pruned
    assert!(
        response.evidence.nodes.len() <= 2,
        "context pruning should enforce top_k limit"
    );
    // Pruned nodes should appear in exclusions
    assert!(
        response
            .explain
            .exclusions
            .iter()
            .any(|ex| ex.reason == "pruned_by_top_k"),
        "pruned nodes should be recorded in exclusions"
    );
}

// ---------------------------------------------------------------------------
// 4. Local Search: Entity-centric (1-2 hop)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_local_search_centers_on_entity_with_shallow_hops() {
    let (_dir, repo, summaries) = graphrag_repo().await;
    let engine = QueryEngine::new(repo).with_community_summaries(summaries);

    let request = QueryRequest::parse_json(
        r#"{
            "query": "Toyota battery partnerships",
            "mode": "answer",
            "search_mode": "local",
            "top_k": 5,
            "traversal": {"depth": 2}
        }"#,
    )
    .unwrap();

    let response = engine.execute(request).await.unwrap();

    assert_eq!(response.explain.effective_search_mode, SearchMode::Local);
    assert!(response.answer.is_some());
    // Local search should center around the entity most similar to query
    assert!(!response.evidence.nodes.is_empty());
    // All nodes should be within 2 hops of an anchor
    for node in &response.evidence.nodes {
        assert!(
            node.hop <= 2,
            "local search should stay within traversal depth"
        );
    }
}

// ---------------------------------------------------------------------------
// 5. Global Search: Community Summaries + Map-Reduce
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_global_search_uses_community_summaries_for_answer() {
    let (_dir, repo, summaries) = graphrag_repo().await;
    assert!(
        !summaries.is_empty(),
        "test setup must produce community summaries"
    );

    let engine = QueryEngine::new(repo).with_community_summaries(summaries);

    let request = QueryRequest::parse_json(
        r#"{
            "query": "What are the major themes in this dataset?",
            "mode": "answer",
            "search_mode": "global",
            "top_k": 10,
            "traversal": {"depth": 2}
        }"#,
    )
    .unwrap();

    let response = engine.execute(request).await.unwrap();

    assert_eq!(response.explain.effective_search_mode, SearchMode::Global);
    // Global search must produce an answer that includes community-level information
    let answer = response.answer.unwrap();
    assert!(!answer.is_empty());
    // The explain plan should indicate community-based search was used
    assert!(
        response
            .explain
            .steps
            .iter()
            .any(|s| s.contains("community")),
        "global search explain should mention community-based processing"
    );
}

#[tokio::test]
async fn test_global_search_without_community_data_falls_back_to_expanded_vector() {
    let (_dir, repo, _summaries) = graphrag_repo().await;

    // No community summaries provided
    let engine = QueryEngine::new(repo);

    let request = QueryRequest::parse_json(
        r#"{
            "query": "What are the overall trends?",
            "mode": "answer",
            "search_mode": "global",
            "top_k": 10
        }"#,
    )
    .unwrap();

    let response = engine.execute(request).await.unwrap();

    // Should still produce a valid response via expanded vector search
    assert!(response.answer.is_some());
    assert!(
        response
            .explain
            .exclusions
            .iter()
            .any(|ex| ex.reason.contains("no_community_data")),
        "should note absence of community data in exclusions"
    );
}

// ---------------------------------------------------------------------------
// 6. DRIFT Search: Iterative Feedback Loop
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_drift_search_expands_dynamically_when_evidence_insufficient() {
    let (_dir, repo, summaries) = graphrag_repo().await;
    let engine = QueryEngine::new(repo).with_community_summaries(summaries);

    let request = QueryRequest::parse_json(
        r#"{
            "query": "How does university research influence EV regulation?",
            "mode": "answer",
            "search_mode": "drift",
            "top_k": 10,
            "traversal": {"depth": 1}
        }"#,
    )
    .unwrap();

    let response = engine.execute(request).await.unwrap();

    assert_eq!(response.explain.effective_search_mode, SearchMode::Drift);
    assert!(response.answer.is_some());
    // DRIFT should expand beyond the initial traversal depth
    assert!(
        response.explain.steps.iter().any(|s| s.contains("drift")),
        "drift search explain should mention iterative expansion"
    );
    // Evidence should include nodes from multiple communities
    // (universities and regulators are linked through the graph)
    assert!(
        response.evidence.nodes.len() >= 2,
        "drift should discover cross-community evidence"
    );
}

#[tokio::test]
async fn test_drift_search_terminates_within_max_iterations() {
    let (_dir, repo, summaries) = graphrag_repo().await;
    let engine = QueryEngine::new(repo).with_community_summaries(summaries);

    let request = QueryRequest::parse_json(
        r#"{
            "query": "nonexistent topic with no matching nodes",
            "mode": "evidence",
            "search_mode": "drift",
            "top_k": 5,
            "traversal": {"depth": 1}
        }"#,
    )
    .unwrap();

    let response = engine.execute(request).await.unwrap();

    // Even with drift, the search should terminate gracefully
    assert_eq!(response.explain.effective_search_mode, SearchMode::Drift);
    // Should not hang or loop forever — response is returned
}

// ---------------------------------------------------------------------------
// 7. Groundedness Scoring
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_groundedness_score_reflects_evidence_quality() {
    let (_dir, repo, summaries) = graphrag_repo().await;
    let engine = QueryEngine::new(repo).with_community_summaries(summaries);

    // Query with strong matches
    let strong_req = QueryRequest::parse_json(
        r#"{
            "query": "EV production",
            "mode": "answer",
            "search_mode": "local",
            "top_k": 5,
            "traversal": {"depth": 2}
        }"#,
    )
    .unwrap();

    let strong_res = engine.execute(strong_req).await.unwrap();

    // Query with weak matches
    let weak_req = QueryRequest::parse_json(
        r#"{
            "query": "quantum computing in healthcare",
            "mode": "answer",
            "search_mode": "local",
            "top_k": 5,
            "traversal": {"depth": 1}
        }"#,
    )
    .unwrap();

    let weak_res = engine.execute(weak_req).await.unwrap();

    assert!(
        strong_res.groundedness > weak_res.groundedness,
        "groundedness should be higher for strong matches ({}) vs weak matches ({})",
        strong_res.groundedness,
        weak_res.groundedness
    );
    assert!(
        strong_res.groundedness > 0.0,
        "strong match should have non-zero groundedness"
    );
    assert!(
        strong_res.groundedness <= 1.0,
        "groundedness must be in [0, 1]"
    );
    assert!(
        weak_res.groundedness <= 1.0,
        "groundedness must be in [0, 1]"
    );
}

#[test]
fn test_compute_groundedness_unit() {
    // High similarity evidence → high groundedness
    let high = compute_groundedness(&GroundednessInput {
        query: "EV production battery",
        evidence_scores: &[0.95, 0.88, 0.72],
        evidence_count: 3,
        source_diversity: 3,
        has_graph_support: true,
    });
    assert!(
        high > 0.5,
        "well-grounded evidence should score high: {high}"
    );

    // Low similarity evidence → low groundedness
    let low = compute_groundedness(&GroundednessInput {
        query: "unrelated topic",
        evidence_scores: &[0.1, 0.05],
        evidence_count: 2,
        source_diversity: 1,
        has_graph_support: false,
    });
    assert!(
        low < high,
        "weak evidence should score lower: {low} < {high}"
    );

    // No evidence → zero
    let zero = compute_groundedness(&GroundednessInput {
        query: "anything",
        evidence_scores: &[],
        evidence_count: 0,
        source_diversity: 0,
        has_graph_support: false,
    });
    assert_eq!(zero, 0.0, "no evidence means zero groundedness");
}

// ---------------------------------------------------------------------------
// 8. Vector-only Fallback
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_graphrag_fallback_to_vector_only_when_graph_yields_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("fallback.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    // Insert nodes WITHOUT edges — graph expansion won't help
    repo.put_node(Node::new(
        1,
        deterministic_embedding("EV production", MODEL_ID, DIMS),
        "Toyota EV production analysis".to_string(),
    ))
    .await
    .unwrap();
    repo.put_node(Node::new(
        2,
        deterministic_embedding("EV market", MODEL_ID, DIMS),
        "Global EV market report".to_string(),
    ))
    .await
    .unwrap();

    let engine = QueryEngine::new(repo);

    let request = QueryRequest::parse_json(
        r#"{
            "query": "EV production",
            "mode": "answer",
            "search_mode": "local",
            "top_k": 5,
            "traversal": {"depth": 2}
        }"#,
    )
    .unwrap();

    let response = engine.execute(request).await.unwrap();

    // Even without graph data, we should get vector-based results
    assert!(response.answer.is_some());
    assert!(!response.evidence.nodes.is_empty());
    // The response should indicate that fallback was used
    assert!(
        response.explain.steps.iter().any(|s| s.contains("vector"))
            || response
                .explain
                .exclusions
                .iter()
                .any(|ex| ex.reason.contains("vector_only_fallback")
                    || ex.reason.contains("no_graph_expansion")),
        "should indicate vector-only results when graph yields nothing"
    );
}

#[tokio::test]
async fn test_fallback_preserves_evidence_from_vector_search() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("fallback_evidence.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    // Strong vector match, no edges
    let mut node = Node::new(
        1,
        deterministic_embedding("EV production", MODEL_ID, DIMS),
        "Important EV production data".to_string(),
    );
    node.metadata
        .insert("source".to_string(), "report/ev.pdf".to_string());
    repo.put_node(node).await.unwrap();

    let engine = QueryEngine::new(repo);

    let request = QueryRequest::parse_json(
        r#"{
            "query": "EV production",
            "mode": "evidence",
            "search_mode": "local",
            "top_k": 5,
            "model_id": "embedding-default-v1"
        }"#,
    )
    .unwrap();

    let response = engine.execute(request).await.unwrap();

    // Vector-only results should still have proper evidence
    assert_eq!(response.evidence.nodes.len(), 1);
    assert_eq!(response.evidence.nodes[0].id, 1);
    assert!(!response.citations.is_empty());
    assert!(response.groundedness > 0.0);
}
