// PR-09: Output Specification Tests (Evidence/Provenance/Citation)
// TDD Red phase: Tests for the output specification requirements.

use std::sync::Arc;

use alayasiki_core::embedding::deterministic_embedding;
use alayasiki_core::model::{Edge, Node};
use query::engine::{Provenance, QueryEngine};
use query::QueryRequest;
use storage::community::{CommunityEngine, CommunitySummary, DeterministicSummarizer};
use storage::repo::Repository;
use tempfile::TempDir;

const DIMS: usize = 8;
const MODEL_ID: &str = "embedding-default-v1";

/// Build a repository with provenance metadata on nodes and edges.
async fn provenance_repo() -> (TempDir, Arc<Repository>, Vec<CommunitySummary>) {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("provenance.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    // Node 1: Toyota with full provenance metadata
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
    toyota.metadata.insert(
        "extraction_model_id".to_string(),
        "slm-triplex-v1".to_string(),
    );
    toyota
        .metadata
        .insert("snapshot_id".to_string(), "snap-2024-06-01".to_string());
    toyota.metadata.insert(
        "ingested_at".to_string(),
        "2024-06-01T10:00:00Z".to_string(),
    );
    toyota
        .metadata
        .insert("confidence".to_string(), "0.92".to_string());

    // Node 2: Honda with partial provenance
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
    honda.metadata.insert(
        "extraction_model_id".to_string(),
        "slm-triplex-v1".to_string(),
    );

    // Node 3: BYD with minimal provenance
    let mut byd = Node::new(
        3,
        deterministic_embedding("EV market expansion", MODEL_ID, DIMS),
        "BYD expands to European EV market with affordable models".to_string(),
    );
    byd.metadata
        .insert("entity_type".to_string(), "Company".to_string());
    byd.metadata
        .insert("source".to_string(), "report/byd.pdf".to_string());

    for node in [toyota, honda, byd] {
        repo.put_node(node).await.unwrap();
    }

    // Edges with metadata
    let mut edge12 = Edge::new(1, 2, "competitor_of", 0.9);
    edge12.metadata.insert(
        "extraction_model_id".to_string(),
        "slm-triplex-v1".to_string(),
    );
    edge12
        .metadata
        .insert("snapshot_id".to_string(), "snap-2024-06-01".to_string());

    let edge13 = Edge::new(1, 3, "competitor_of", 0.85);

    repo.put_edge(edge12).await.unwrap();
    repo.put_edge(edge13).await.unwrap();
    repo.put_edge(Edge::new(2, 3, "competitor_of", 0.8))
        .await
        .unwrap();

    // Build community summaries
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
// 1. Evidence サブグラフ返却形式
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_evidence_nodes_include_provenance_and_confidence() {
    let (_dir, repo, summaries) = provenance_repo().await;
    let engine = QueryEngine::new(repo).with_community_summaries(summaries);

    let request = QueryRequest::parse_json(
        r#"{
            "query": "EV production",
            "mode": "evidence",
            "search_mode": "local",
            "top_k": 5,
            "traversal": {"depth": 2}
        }"#,
    )
    .unwrap();

    let response = engine.execute(request).await.unwrap();

    // All evidence nodes must have provenance
    for node in &response.evidence.nodes {
        assert!(
            node.provenance.source.is_some() || node.provenance.extraction_model_id.is_some(),
            "evidence node {} must carry provenance (source or model)",
            node.id
        );
    }

    // Toyota (node 1) should have full provenance
    if let Some(toyota) = response.evidence.nodes.iter().find(|n| n.id == 1) {
        let prov = &toyota.provenance;
        assert_eq!(
            prov.source.as_deref(),
            Some("report/toyota.pdf"),
            "source provenance must be preserved"
        );
        assert_eq!(
            prov.extraction_model_id.as_deref(),
            Some("slm-triplex-v1"),
            "extraction model must be preserved"
        );
        assert_eq!(
            prov.snapshot_id.as_deref(),
            Some("snap-2024-06-01"),
            "snapshot_id provenance must be preserved"
        );
        assert_eq!(
            prov.ingested_at.as_deref(),
            Some("2024-06-01T10:00:00Z"),
            "ingested_at provenance must be preserved"
        );
        assert!(
            toyota.confidence > 0.0,
            "confidence should be positive for Toyota"
        );
    }
}

#[tokio::test]
async fn test_evidence_edges_include_provenance_and_confidence() {
    let (_dir, repo, summaries) = provenance_repo().await;
    let engine = QueryEngine::new(repo).with_community_summaries(summaries);

    let request = QueryRequest::parse_json(
        r#"{
            "query": "EV production",
            "mode": "evidence",
            "search_mode": "local",
            "top_k": 5,
            "traversal": {"depth": 2}
        }"#,
    )
    .unwrap();

    let response = engine.execute(request).await.unwrap();

    // All edges should have a confidence value (weight as confidence)
    for edge in &response.evidence.edges {
        assert!(
            edge.confidence > 0.0,
            "edge {}→{} should have positive confidence",
            edge.source,
            edge.target
        );
    }

    // Edges should carry provenance when available
    let has_provenance_edge = response
        .evidence
        .edges
        .iter()
        .any(|e| e.provenance.extraction_model_id.is_some() || e.provenance.source.is_some());
    // At least the edge 1→2 should have provenance
    assert!(
        has_provenance_edge,
        "at least one edge should have provenance metadata"
    );
}

#[tokio::test]
async fn test_evidence_subgraph_is_self_consistent() {
    let (_dir, repo, summaries) = provenance_repo().await;
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
    let node_ids: std::collections::HashSet<u64> =
        response.evidence.nodes.iter().map(|n| n.id).collect();

    // All edges must reference nodes present in the evidence subgraph
    for edge in &response.evidence.edges {
        assert!(
            node_ids.contains(&edge.source),
            "edge source {} must be in evidence nodes",
            edge.source
        );
        assert!(
            node_ids.contains(&edge.target),
            "edge target {} must be in evidence nodes",
            edge.target
        );
    }
}

// ---------------------------------------------------------------------------
// 2. Provenance/Confidence を返却に含める
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_provenance_struct_fields_are_populated_from_metadata() {
    let (_dir, repo, _summaries) = provenance_repo().await;
    let engine = QueryEngine::new(repo);

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

    // Check that provenance has all optional fields
    let provenances: Vec<&Provenance> = response
        .evidence
        .nodes
        .iter()
        .map(|n| &n.provenance)
        .collect();

    assert!(
        !provenances.is_empty(),
        "should have at least one evidence node"
    );

    // Verify serialization roundtrip of Provenance
    let json = serde_json::to_string(&response).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    let evidence = parsed.get("evidence").unwrap();
    let first_node = &evidence["nodes"][0];
    assert!(
        first_node.get("provenance").is_some(),
        "provenance should appear in JSON output"
    );
    assert!(
        first_node.get("confidence").is_some(),
        "confidence should appear in JSON output"
    );
}

#[tokio::test]
async fn test_confidence_reflects_metadata_when_available() {
    let (_dir, repo, _summaries) = provenance_repo().await;
    let engine = QueryEngine::new(repo);

    let request = QueryRequest::parse_json(
        r#"{
            "query": "EV production",
            "mode": "evidence",
            "search_mode": "local",
            "top_k": 5,
            "traversal": {"depth": 2}
        }"#,
    )
    .unwrap();

    let response = engine.execute(request).await.unwrap();

    // Toyota (node 1) has explicit confidence=0.92 in metadata
    if let Some(toyota) = response.evidence.nodes.iter().find(|n| n.id == 1) {
        assert!(
            (toyota.confidence - 0.92).abs() < 0.01,
            "confidence should be 0.92 from metadata, got {}",
            toyota.confidence
        );
    }

    // BYD (node 3) has no explicit confidence — should fall back to score
    if let Some(byd) = response.evidence.nodes.iter().find(|n| n.id == 3) {
        assert!(
            byd.confidence > 0.0,
            "nodes without explicit confidence should still have a positive value"
        );
    }
}

// ---------------------------------------------------------------------------
// 3. Citation 形式（source + span）を定義・返却
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_citations_include_node_id_and_confidence() {
    let (_dir, repo, _summaries) = provenance_repo().await;
    let engine = QueryEngine::new(repo);

    let request = QueryRequest::parse_json(
        r#"{
            "query": "EV production",
            "mode": "evidence",
            "search_mode": "local",
            "top_k": 5,
            "traversal": {"depth": 2}
        }"#,
    )
    .unwrap();

    let response = engine.execute(request).await.unwrap();

    assert!(!response.citations.is_empty(), "should have citations");

    for citation in &response.citations {
        assert!(
            !citation.source.is_empty(),
            "citation source must not be empty"
        );
        assert!(
            citation.span[1] > citation.span[0],
            "citation span end must be > start"
        );
        assert!(citation.node_id > 0, "citation must reference a node");
        assert!(
            citation.confidence > 0.0,
            "citation must have positive confidence"
        );
    }
}

#[tokio::test]
async fn test_citations_span_covers_actual_data_range() {
    let (_dir, repo, _summaries) = provenance_repo().await;
    let engine = QueryEngine::new(repo);

    let request = QueryRequest::parse_json(
        r#"{
            "query": "EV production",
            "mode": "evidence",
            "search_mode": "local",
            "top_k": 5,
            "traversal": {"depth": 2}
        }"#,
    )
    .unwrap();

    let response = engine.execute(request).await.unwrap();

    for citation in &response.citations {
        // span[1] should not exceed the data length of the corresponding node
        if let Some(node) = response
            .evidence
            .nodes
            .iter()
            .find(|n| n.id == citation.node_id)
        {
            assert!(
                citation.span[1] <= node.data.len(),
                "citation span end ({}) must not exceed node data length ({})",
                citation.span[1],
                node.data.len()
            );
        }
    }
}

// ---------------------------------------------------------------------------
// 4. time_travel / snapshot_id の優先順序を反映
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_time_travel_field_accepted_in_query_request() {
    // YYYY-MM-DD format
    let request = QueryRequest::parse_json(
        r#"{
            "query": "EV production",
            "time_travel": "2024-06-01"
        }"#,
    )
    .unwrap();

    assert_eq!(request.time_travel.as_deref(), Some("2024-06-01"));
    assert!(request.validate().is_ok());

    // RFC3339 format
    let request_rfc = QueryRequest::parse_json(
        r#"{
            "query": "EV production",
            "time_travel": "2024-06-01T10:00:00Z"
        }"#,
    )
    .unwrap();

    assert_eq!(
        request_rfc.time_travel.as_deref(),
        Some("2024-06-01T10:00:00Z")
    );
    assert!(request_rfc.validate().is_ok());
}

#[tokio::test]
async fn test_time_travel_validation_rejects_invalid_format() {
    let request = QueryRequest::parse_json(
        r#"{
            "query": "EV production",
            "time_travel": "not-a-date"
        }"#,
    )
    .unwrap();

    assert!(
        request.validate().is_err(),
        "invalid time_travel format should fail validation"
    );
}

#[tokio::test]
async fn test_snapshot_id_takes_priority_over_time_travel() {
    let (_dir, repo, _summaries) = provenance_repo().await;
    let engine = QueryEngine::new(repo);

    // Both snapshot_id and time_travel provided — snapshot_id wins
    let request = QueryRequest::parse_json(
        r#"{
            "query": "EV production",
            "mode": "evidence",
            "search_mode": "local",
            "top_k": 5,
            "snapshot_id": "snap-explicit",
            "time_travel": "2024-01-01"
        }"#,
    )
    .unwrap();

    let response = engine.execute(request).await.unwrap();

    assert_eq!(
        response.snapshot_id.as_deref(),
        Some("snap-explicit"),
        "snapshot_id must take priority over time_travel"
    );
}

#[tokio::test]
async fn test_time_travel_reflected_in_response_when_no_snapshot_id() {
    let (_dir, repo, _summaries) = provenance_repo().await;
    let engine = QueryEngine::new(repo);

    let request = QueryRequest::parse_json(
        r#"{
            "query": "EV production",
            "mode": "evidence",
            "search_mode": "local",
            "top_k": 5,
            "time_travel": "2024-06-01"
        }"#,
    )
    .unwrap();

    let response = engine.execute(request).await.unwrap();

    // When time_travel is provided without snapshot_id, the resolved snapshot_id
    // should be included in the response (current snapshot since we don't have
    // actual time-travel yet, but the field should be populated)
    assert!(
        response.snapshot_id.is_some(),
        "snapshot_id should be resolved when time_travel is provided"
    );
    // time_travel should be reflected in the response
    assert_eq!(
        response.time_travel.as_deref(),
        Some("2024-06-01"),
        "time_travel should be reflected in response"
    );
}

// ---------------------------------------------------------------------------
// 5. latency_ms in response
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_response_includes_latency_ms() {
    let (_dir, repo, _summaries) = provenance_repo().await;
    let engine = QueryEngine::new(repo);

    let request = QueryRequest::parse_json(
        r#"{
            "query": "EV production",
            "mode": "answer",
            "search_mode": "local",
            "top_k": 5
        }"#,
    )
    .unwrap();

    let response = engine.execute(request).await.unwrap();

    // latency_ms should reflect actual execution time (u64 is always >= 0)
    // Just verify the field exists and is accessible
    let _latency = response.latency_ms;
}

// ---------------------------------------------------------------------------
// 6. JSON output structure matches SPEC
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_response_json_matches_spec_structure() {
    let (_dir, repo, summaries) = provenance_repo().await;
    let engine = QueryEngine::new(repo).with_community_summaries(summaries);

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

    // Serialize to JSON and verify spec fields exist
    let json = serde_json::to_string(&response).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    // Required top-level fields per SPEC section 4.1
    assert!(parsed.get("answer").is_some(), "answer field required");
    assert!(parsed.get("evidence").is_some(), "evidence field required");
    assert!(
        parsed.get("citations").is_some(),
        "citations field required"
    );
    assert!(
        parsed.get("groundedness").is_some(),
        "groundedness field required"
    );
    assert!(parsed.get("model_id").is_some(), "model_id field required");
    assert!(
        parsed.get("snapshot_id").is_some(),
        "snapshot_id field required"
    );
    assert!(
        parsed.get("latency_ms").is_some(),
        "latency_ms field required per SPEC 4.1"
    );

    // Evidence subgraph structure
    let evidence = parsed.get("evidence").unwrap();
    assert!(evidence.get("nodes").is_some(), "evidence.nodes required");
    assert!(evidence.get("edges").is_some(), "evidence.edges required");

    // Evidence node structure
    if let Some(nodes) = evidence["nodes"].as_array() {
        if let Some(node) = nodes.first() {
            assert!(node.get("id").is_some());
            assert!(node.get("data").is_some());
            assert!(node.get("score").is_some());
            assert!(node.get("hop").is_some());
            assert!(node.get("provenance").is_some());
            assert!(node.get("confidence").is_some());
        }
    }

    // Citation structure
    if let Some(citations) = parsed["citations"].as_array() {
        if let Some(citation) = citations.first() {
            assert!(citation.get("source").is_some());
            assert!(citation.get("span").is_some());
            assert!(citation.get("node_id").is_some());
            assert!(citation.get("confidence").is_some());
        }
    }
}

// ---------------------------------------------------------------------------
// 7. Edge update boundary cases (review feedback)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_edge_update_with_empty_metadata_clears_old_provenance() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("edge_clear.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    repo.put_node(Node::new(
        1,
        deterministic_embedding("A", MODEL_ID, DIMS),
        "NodeA".to_string(),
    ))
    .await
    .unwrap();
    repo.put_node(Node::new(
        2,
        deterministic_embedding("B", MODEL_ID, DIMS),
        "NodeB".to_string(),
    ))
    .await
    .unwrap();

    // Insert edge WITH provenance metadata
    let mut edge_v1 = Edge::new(1, 2, "links", 0.9);
    edge_v1.metadata.insert(
        "extraction_model_id".to_string(),
        "old-model-v1".to_string(),
    );
    repo.put_edge(edge_v1).await.unwrap();

    // Verify provenance is stored
    let meta = repo.get_edge_metadata(1, 2, "links").await;
    assert_eq!(meta.get("extraction_model_id").unwrap(), "old-model-v1");

    // Update edge with EMPTY metadata — must clear old provenance
    let edge_v2 = Edge::new(1, 2, "links", 0.95);
    repo.put_edge(edge_v2).await.unwrap();

    let meta_after = repo.get_edge_metadata(1, 2, "links").await;
    assert!(
        meta_after.is_empty(),
        "empty metadata update must clear stale provenance, got: {:?}",
        meta_after
    );
}

#[tokio::test]
async fn test_edge_update_replaces_provenance_not_appends() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("edge_replace.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    repo.put_node(Node::new(
        1,
        deterministic_embedding("A", MODEL_ID, DIMS),
        "NodeA".to_string(),
    ))
    .await
    .unwrap();
    repo.put_node(Node::new(
        2,
        deterministic_embedding("B", MODEL_ID, DIMS),
        "NodeB".to_string(),
    ))
    .await
    .unwrap();

    // V1: old model
    let mut edge_v1 = Edge::new(1, 2, "links", 0.8);
    edge_v1
        .metadata
        .insert("extraction_model_id".to_string(), "old-model".to_string());
    edge_v1
        .metadata
        .insert("source".to_string(), "old-source.pdf".to_string());
    repo.put_edge(edge_v1).await.unwrap();

    // V2: new model, different metadata
    let mut edge_v2 = Edge::new(1, 2, "links", 0.95);
    edge_v2.metadata.insert(
        "extraction_model_id".to_string(),
        "new-model-v2".to_string(),
    );
    // Note: no "source" key in v2
    repo.put_edge(edge_v2).await.unwrap();

    let meta = repo.get_edge_metadata(1, 2, "links").await;
    assert_eq!(
        meta.get("extraction_model_id").unwrap(),
        "new-model-v2",
        "provenance must be fully replaced"
    );
    assert!(
        !meta.contains_key("source"),
        "old 'source' key must not persist after replacement"
    );
}

#[tokio::test]
async fn test_edge_upsert_updates_weight_in_graph_index() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("edge_upsert.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    repo.put_node(Node::new(
        1,
        deterministic_embedding("A", MODEL_ID, DIMS),
        "NodeA".to_string(),
    ))
    .await
    .unwrap();
    repo.put_node(Node::new(
        2,
        deterministic_embedding("B", MODEL_ID, DIMS),
        "NodeB".to_string(),
    ))
    .await
    .unwrap();

    // Insert edge with weight 0.5
    repo.put_edge(Edge::new(1, 2, "links", 0.5)).await.unwrap();

    // Upsert same edge with weight 0.9
    repo.put_edge(Edge::new(1, 2, "links", 0.9)).await.unwrap();

    // Graph index should have exactly 1 edge (not 2)
    let index = repo.hyper_index.read().await;
    let neighbors = index.graph_index.neighbors(1);
    let matching: Vec<_> = neighbors
        .iter()
        .filter(|(t, r, _)| *t == 2 && r == "links")
        .collect();
    assert_eq!(
        matching.len(),
        1,
        "upsert should produce exactly one edge, not duplicate"
    );
    assert!(
        (matching[0].2 - 0.9).abs() < f32::EPSILON,
        "weight should be updated to 0.9, got {}",
        matching[0].2
    );
}

#[tokio::test]
async fn test_edge_upsert_survives_wal_replay() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("edge_upsert_replay.wal");

    // 1. Insert then update edge
    {
        let repo = Repository::open(&wal_path).await.unwrap();
        repo.put_node(Node::new(1, vec![1.0], "N1".to_string()))
            .await
            .unwrap();
        repo.put_node(Node::new(2, vec![2.0], "N2".to_string()))
            .await
            .unwrap();

        let mut edge_v1 = Edge::new(1, 2, "links", 0.5);
        edge_v1
            .metadata
            .insert("extraction_model_id".to_string(), "old".to_string());
        repo.put_edge(edge_v1).await.unwrap();

        let mut edge_v2 = Edge::new(1, 2, "links", 0.95);
        edge_v2
            .metadata
            .insert("extraction_model_id".to_string(), "new".to_string());
        repo.put_edge(edge_v2).await.unwrap();
    }

    // 2. Reopen — replay must produce upserted state
    {
        let repo = Repository::open(&wal_path).await.unwrap();

        let index = repo.hyper_index.read().await;
        let neighbors = index.graph_index.neighbors(1);
        let matching: Vec<_> = neighbors
            .iter()
            .filter(|(t, r, _)| *t == 2 && r == "links")
            .collect();
        assert_eq!(matching.len(), 1, "replay must upsert, not append");
        assert!(
            (matching[0].2 - 0.95).abs() < f32::EPSILON,
            "replayed weight should be 0.95"
        );

        let meta = repo.get_edge_metadata(1, 2, "links").await;
        assert_eq!(
            meta.get("extraction_model_id").unwrap(),
            "new",
            "replayed provenance should be the latest"
        );
    }
}
