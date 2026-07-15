// Issue #52: Comprehensive integration tests for multi-hop GraphRAG traversal
// and DRIFT search over a realistic, deterministic knowledge graph.
//
// The fixture models a real-world AI/semiconductor supply chain with directed
// multi-hop chains, a competitor cycle, and a disconnected node. These tests
// verify the "Retrieve Reasoned" promise (SPEC §2.2 / §3) by asserting exact
// multi-hop reachability, BFS path reconstruction, DRIFT iterative expansion,
// determinism, and robust handling of cycles, disconnected components,
// relation filtering, and top_k pruning.

use std::sync::Arc;

use alayasiki_core::embedding::deterministic_embedding;
use alayasiki_core::model::{Edge, Node};
use query::dsl::{QueryMode, Traversal};
use query::engine::QueryEngine;
use query::{QueryRequest, SearchMode};
use storage::repo::Repository;
use tempfile::TempDir;

const DIMS: usize = 8;
const MODEL_ID: &str = "embedding-default-v1";

// --- Fixture entity ids -----------------------------------------------------
const OPENAI: u64 = 1;
const NVIDIA: u64 = 2;
const TSMC: u64 = 3;
const ASML: u64 = 4;
const ZEISS: u64 = 5;
const MICROSOFT: u64 = 6;
const APPLE: u64 = 7;
const ARM: u64 = 8;
const GRAPHCORE: u64 = 9;
const SAMSUNG: u64 = 10; // intentionally disconnected (no edges)

// Distinctive data texts. A query equal to a node's data text produces an
// exact embedding match (cosine similarity 1.0), guaranteeing that node is the
// single highest-ranked vector anchor when top_k == 1.
const OPENAI_TEXT: &str = "OpenAI trains large language models on GPU clusters";
const NVIDIA_TEXT: &str = "NVIDIA designs GPUs for AI training";
const TSMC_TEXT: &str = "TSMC manufactures advanced semiconductor wafers";
const ASML_TEXT: &str = "ASML produces EUV lithography machines";
const ZEISS_TEXT: &str = "Zeiss supplies precision optics for EUV lithography";
const SAMSUNG_TEXT: &str = "Samsung foundry produces legacy process nodes";

/// Build a deterministic AI/semiconductor supply-chain knowledge graph.
///
/// Directed edges form a deep chain from OpenAI down to Zeiss, a competitor
/// cycle (NVIDIA <-> Graphcore), and Apple as a source-only node (its edge
/// points *into* TSMC, so Apple is unreachable from the OpenAI chain).
async fn supply_chain_repo() -> (TempDir, Arc<Repository>) {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("multihop.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    let node = |id: u64, data: &str, entity_type: &str, source: &str| {
        let mut n = Node::new(
            id,
            deterministic_embedding(data, MODEL_ID, DIMS),
            data.to_string(),
        );
        n.metadata
            .insert("entity_type".to_string(), entity_type.to_string());
        n.metadata.insert("source".to_string(), source.to_string());
        n
    };

    for n in [
        node(OPENAI, OPENAI_TEXT, "Company", "news/openai.txt"),
        node(NVIDIA, NVIDIA_TEXT, "Company", "news/nvidia.txt"),
        node(TSMC, TSMC_TEXT, "Company", "news/tsmc.txt"),
        node(ASML, ASML_TEXT, "Company", "news/asml.txt"),
        node(ZEISS, ZEISS_TEXT, "Company", "news/zeiss.txt"),
        node(
            MICROSOFT,
            "Microsoft Azure hosts OpenAI models",
            "Company",
            "news/microsoft.txt",
        ),
        node(
            APPLE,
            "Apple designs M-series silicon",
            "Company",
            "news/apple.txt",
        ),
        node(
            ARM,
            "ARM licenses RISC instruction sets",
            "Company",
            "news/arm.txt",
        ),
        node(
            GRAPHCORE,
            "Graphcore builds AI accelerator chips",
            "Company",
            "news/graphcore.txt",
        ),
        node(SAMSUNG, SAMSUNG_TEXT, "Company", "news/samsung.txt"),
    ] {
        repo.put_node(n).await.unwrap();
    }

    // Deep directed chain: OpenAI -> NVIDIA -> TSMC -> ASML -> Zeiss
    repo.put_edge(Edge::new(OPENAI, NVIDIA, "uses_gpus", 0.9))
        .await
        .unwrap();
    repo.put_edge(Edge::new(NVIDIA, TSMC, "fabricated_by", 0.85))
        .await
        .unwrap();
    repo.put_edge(Edge::new(TSMC, ASML, "uses_equipment", 0.8))
        .await
        .unwrap();
    repo.put_edge(Edge::new(ASML, ZEISS, "optics_from", 0.75))
        .await
        .unwrap();

    // Branches from OpenAI / NVIDIA
    repo.put_edge(Edge::new(OPENAI, MICROSOFT, "backed_by", 0.6))
        .await
        .unwrap();
    repo.put_edge(Edge::new(NVIDIA, ARM, "licenses_ip", 0.5))
        .await
        .unwrap();

    // Competitor cycle: NVIDIA <-> Graphcore
    repo.put_edge(Edge::new(NVIDIA, GRAPHCORE, "competes_with", 0.7))
        .await
        .unwrap();
    repo.put_edge(Edge::new(GRAPHCORE, NVIDIA, "competes_with", 0.7))
        .await
        .unwrap();

    // Apple points INTO TSMC (source-only); unreachable from the OpenAI chain.
    repo.put_edge(Edge::new(APPLE, TSMC, "fabricated_by", 0.8))
        .await
        .unwrap();

    // SAMSUNG (10) intentionally has no edges.

    (dir, repo)
}

/// Build an evidence-mode request anchored on `query` with a single vector
/// anchor (top_k == 1) and the requested traversal depth.
fn anchored_request(query: &str, depth: u8, search_mode: SearchMode) -> QueryRequest {
    QueryRequest {
        query: query.to_string(),
        mode: QueryMode::Evidence,
        traversal: Traversal {
            depth,
            relation_types: Vec::new(),
        },
        top_k: 1,
        search_mode,
        ..QueryRequest::default()
    }
}

/// Collect expansion-path target ids from a response.
fn path_targets(response: &query::QueryResponse) -> Vec<u64> {
    let mut ids: Vec<u64> = response
        .explain
        .expansion_paths
        .iter()
        .map(|p| p.target_id)
        .collect();
    ids.sort_unstable();
    ids
}

/// Find the reconstructed path to `target`, if any.
fn path_to(response: &query::QueryResponse, target: u64) -> Option<Vec<u64>> {
    response
        .explain
        .expansion_paths
        .iter()
        .find(|p| p.target_id == target)
        .map(|p| p.path.clone())
}

fn assert_excluded_with_reason(response: &query::QueryResponse, needle: &str) {
    assert!(
        response
            .explain
            .exclusions
            .iter()
            .any(|r| r.reason.contains(needle)),
        "expected an exclusion containing '{needle}', got {:?}",
        response
            .explain
            .exclusions
            .iter()
            .map(|r| &r.reason)
            .collect::<Vec<_>>()
    );
}

// ---------------------------------------------------------------------------
// 1. Multi-hop traversal correctness (Local search, single controlled anchor)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn local_depth_1_reaches_only_direct_neighbors() {
    let (_dir, repo) = supply_chain_repo().await;
    let engine = QueryEngine::new(repo);

    let response = engine
        .execute(anchored_request(OPENAI_TEXT, 1, SearchMode::Local))
        .await
        .unwrap();

    assert_eq!(response.explain.anchors.len(), 1);
    assert_eq!(response.explain.anchors[0].node_id, OPENAI);

    // depth 1: NVIDIA and Microsoft are the only direct successors of OpenAI.
    assert_eq!(path_targets(&response), vec![NVIDIA, MICROSOFT]);
}

#[tokio::test]
async fn local_depth_2_reaches_two_hop_neighbors() {
    let (_dir, repo) = supply_chain_repo().await;
    let engine = QueryEngine::new(repo);

    let response = engine
        .execute(anchored_request(OPENAI_TEXT, 2, SearchMode::Local))
        .await
        .unwrap();

    // hop1: NVIDIA, Microsoft ; hop2: TSMC, ARM, Graphcore (via NVIDIA).
    assert_eq!(
        path_targets(&response),
        vec![NVIDIA, TSMC, MICROSOFT, ARM, GRAPHCORE]
    );
}

#[tokio::test]
async fn local_depth_3_and_4_reach_deep_chain() {
    let (_dir, repo) = supply_chain_repo().await;
    let engine = QueryEngine::new(repo);

    let depth3 = engine
        .execute(anchored_request(OPENAI_TEXT, 3, SearchMode::Local))
        .await
        .unwrap();
    assert!(
        path_targets(&depth3).contains(&ASML),
        "depth 3 must reach ASML (OpenAI->NVIDIA->TSMC->ASML)"
    );

    let depth4 = engine
        .execute(anchored_request(OPENAI_TEXT, 4, SearchMode::Local))
        .await
        .unwrap();
    assert!(
        path_targets(&depth4).contains(&ZEISS),
        "depth 4 must reach Zeiss (OpenAI->NVIDIA->TSMC->ASML->Zeiss)"
    );
}

#[tokio::test]
async fn bfs_reconstructs_exact_multi_hop_paths() {
    let (_dir, repo) = supply_chain_repo().await;
    let engine = QueryEngine::new(repo);

    let response = engine
        .execute(anchored_request(OPENAI_TEXT, 4, SearchMode::Local))
        .await
        .unwrap();

    assert_eq!(
        path_to(&response, NVIDIA).as_deref(),
        Some(&[OPENAI, NVIDIA][..])
    );
    assert_eq!(
        path_to(&response, TSMC).as_deref(),
        Some(&[OPENAI, NVIDIA, TSMC][..])
    );
    assert_eq!(
        path_to(&response, ASML).as_deref(),
        Some(&[OPENAI, NVIDIA, TSMC, ASML][..])
    );
    assert_eq!(
        path_to(&response, ZEISS).as_deref(),
        Some(&[OPENAI, NVIDIA, TSMC, ASML, ZEISS][..])
    );
    assert_eq!(
        path_to(&response, MICROSOFT).as_deref(),
        Some(&[OPENAI, MICROSOFT][..])
    );
}

// ---------------------------------------------------------------------------
// 2. Cycle handling: traversal terminates and assigns minimum hops
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cycle_does_not_cause_infinite_traversal() {
    let (_dir, repo) = supply_chain_repo().await;
    let engine = QueryEngine::new(repo);

    // NVIDIA <-> Graphcore is a 2-cycle. depth 3 must terminate and not loop.
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        engine.execute(anchored_request(NVIDIA_TEXT, 3, SearchMode::Local)),
    )
    .await
    .expect("multi-hop traversal over a cycle must terminate")
    .unwrap();

    // Graphcore is reached once at hop 1; NVIDIA is not re-added via the cycle.
    let targets = path_targets(&response);
    assert!(targets.contains(&GRAPHCORE));
    assert!(
        !response
            .explain
            .expansion_paths
            .iter()
            .any(|p| p.target_id == NVIDIA),
        "anchor must not be re-discovered through a cycle"
    );
    // Deep chain from NVIDIA is still reachable: ASML (hop2) and Zeiss (hop3).
    assert!(targets.contains(&ASML));
    assert!(targets.contains(&ZEISS));
}

// ---------------------------------------------------------------------------
// 3. Disconnected components are never reached across the void
// ---------------------------------------------------------------------------

#[tokio::test]
async fn disconnected_node_is_unreachable_from_connected_anchor() {
    let (_dir, repo) = supply_chain_repo().await;
    let engine = QueryEngine::new(repo);

    let response = engine
        .execute(anchored_request(OPENAI_TEXT, 4, SearchMode::Local))
        .await
        .unwrap();

    let targets = path_targets(&response);
    assert!(
        !targets.contains(&SAMSUNG),
        "disconnected Samsung must never be reached"
    );
    assert!(
        !targets.contains(&APPLE),
        "source-only Apple (no incoming edges) must be unreachable"
    );
}

#[tokio::test]
async fn isolated_anchor_returns_only_itself() {
    let (_dir, repo) = supply_chain_repo().await;
    let engine = QueryEngine::new(repo);

    let response = engine
        .execute(anchored_request(SAMSUNG_TEXT, 2, SearchMode::Local))
        .await
        .unwrap();

    assert_eq!(response.explain.anchors.len(), 1);
    assert_eq!(response.explain.anchors[0].node_id, SAMSUNG);
    assert!(response.explain.expansion_paths.is_empty());
    // No graph support when the anchor has no neighbors.
    assert!(response.evidence.edges.is_empty());
}

// ---------------------------------------------------------------------------
// 4. Relation-type filtering prunes traversal by relation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn relation_filter_excludes_disallowed_edges() {
    let (_dir, repo) = supply_chain_repo().await;
    let engine = QueryEngine::new(repo);

    // Allow only the supply-chain relations; exclude backed_by / licenses_ip /
    // competes_with.
    let request = QueryRequest {
        query: OPENAI_TEXT.to_string(),
        mode: QueryMode::Evidence,
        traversal: Traversal {
            depth: 4,
            relation_types: vec![
                "uses_gpus".to_string(),
                "fabricated_by".to_string(),
                "uses_equipment".to_string(),
                "optics_from".to_string(),
            ],
        },
        top_k: 1,
        search_mode: SearchMode::Local,
        ..QueryRequest::default()
    };

    let response = engine.execute(request).await.unwrap();

    // Only NVIDIA (uses_gpus), TSMC (fabricated_by), ASML (uses_equipment),
    // Zeiss (optics_from) are reachable along allowed relations.
    assert_eq!(path_targets(&response), vec![NVIDIA, TSMC, ASML, ZEISS]);

    // Microsoft / ARM / Graphcore were filtered out with explicit reasons.
    assert_excluded_with_reason(&response, "relation_filtered:backed_by");
    assert_excluded_with_reason(&response, "relation_filtered:licenses_ip");
    assert_excluded_with_reason(&response, "relation_filtered:competes_with");
}

// ---------------------------------------------------------------------------
// 5. top_k pruning caps the evidence set while still traversing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn top_k_prunes_evidence_but_keeps_full_expansion_paths() {
    let (_dir, repo) = supply_chain_repo().await;
    let engine = QueryEngine::new(repo);

    // top_k == 1 -> a single vector anchor and a single evidence node, yet the
    // full BFS tree is still surfaced via expansion_paths.
    let response = engine
        .execute(anchored_request(OPENAI_TEXT, 2, SearchMode::Local))
        .await
        .unwrap();

    assert_eq!(response.evidence.nodes.len(), 1);
    assert_eq!(response.evidence.nodes[0].id, OPENAI);
    assert_excluded_with_reason(&response, "pruned_by_top_k");
    assert_eq!(
        path_targets(&response),
        vec![NVIDIA, TSMC, MICROSOFT, ARM, GRAPHCORE]
    );
}

// ---------------------------------------------------------------------------
// 6. DRIFT search: mode selection, iterative expansion, coverage
// ---------------------------------------------------------------------------

#[tokio::test]
async fn drift_search_selects_drift_mode_and_iterative_steps() {
    let (_dir, repo) = supply_chain_repo().await;
    let engine = QueryEngine::new(repo);

    let request = QueryRequest {
        query: OPENAI_TEXT.to_string(),
        mode: QueryMode::Evidence,
        traversal: Traversal {
            depth: 1,
            relation_types: Vec::new(),
        },
        top_k: 5,
        search_mode: SearchMode::Drift,
        ..QueryRequest::default()
    };

    let response = engine.execute(request).await.unwrap();

    assert_eq!(
        response.explain.effective_search_mode,
        SearchMode::Drift,
        "explicit drift mode must be honored"
    );
    assert!(
        response
            .explain
            .steps
            .iter()
            .any(|s| s == "drift_iterative_expansion"),
        "drift must record its iterative expansion step: {:?}",
        response.explain.steps
    );
    assert!(
        response
            .explain
            .steps
            .iter()
            .any(|s| s == "graph_expansion"),
        "drift must still perform graph expansion: {:?}",
        response.explain.steps
    );
    assert!(!response.evidence.nodes.is_empty());
    assert!(
        response.evidence.nodes.iter().any(|n| n.id == OPENAI),
        "drift must include the query anchor in evidence"
    );
}

#[tokio::test]
async fn drift_covers_at_least_as_much_evidence_as_shallow_local() {
    let (_dir, repo) = supply_chain_repo().await;

    let base = || QueryRequest {
        query: OPENAI_TEXT.to_string(),
        mode: QueryMode::Evidence,
        traversal: Traversal {
            depth: 1,
            relation_types: Vec::new(),
        },
        top_k: 5,
        ..QueryRequest::default()
    };

    // Use independent engines so each computes against its own semantic cache.
    let local_engine = QueryEngine::new(repo.clone());
    let drift_engine = QueryEngine::new(repo);

    let local_req = QueryRequest {
        search_mode: SearchMode::Local,
        ..base()
    };
    let drift_req = QueryRequest {
        search_mode: SearchMode::Drift,
        ..base()
    };

    let local_resp = local_engine.execute(local_req).await.unwrap();
    let drift_resp = drift_engine.execute(drift_req).await.unwrap();

    assert!(
        drift_resp.evidence.nodes.len() >= local_resp.evidence.nodes.len(),
        "DRIFT iterative expansion (depth up to 6) must cover at least as much \
         evidence as depth-1 Local: drift={} local={}",
        drift_resp.evidence.nodes.len(),
        local_resp.evidence.nodes.len()
    );
}

// ---------------------------------------------------------------------------
// 7. Auto mode falls back to DRIFT when Local yields insufficient evidence
// ---------------------------------------------------------------------------

#[tokio::test]
async fn auto_mode_falls_back_to_drift_for_insufficient_local_evidence() {
    let (_dir, repo) = supply_chain_repo().await;
    let engine = QueryEngine::new(repo);

    // top_k == 1 -> Local returns a single evidence node (< 2), which triggers
    // the documented Auto -> DRIFT fallback.
    let response = engine
        .execute(anchored_request(OPENAI_TEXT, 1, SearchMode::Auto))
        .await
        .unwrap();

    assert_eq!(
        response.explain.effective_search_mode,
        SearchMode::Drift,
        "Auto must fall back to DRIFT when Local evidence is insufficient"
    );
    assert_excluded_with_reason(
        &response,
        "auto_fallback_to_drift_due_to_insufficient_evidence",
    );
}

// ---------------------------------------------------------------------------
// 8. Reproducibility: identical requests yield identical reasoned results
// ---------------------------------------------------------------------------

#[tokio::test]
async fn identical_requests_are_reproducible_across_engines() {
    let (_dir, repo) = supply_chain_repo().await;

    // Two independent engines (independent semantic caches) over the same repo.
    let engine_a = QueryEngine::new(repo.clone());
    let engine_b = QueryEngine::new(repo);

    let make_req = || QueryRequest {
        query: OPENAI_TEXT.to_string(),
        mode: QueryMode::Evidence,
        traversal: Traversal {
            depth: 4,
            relation_types: Vec::new(),
        },
        top_k: 8,
        search_mode: SearchMode::Drift,
        ..QueryRequest::default()
    };

    let a = engine_a.execute(make_req()).await.unwrap();
    let b = engine_b.execute(make_req()).await.unwrap();

    assert_eq!(
        a.explain.effective_search_mode,
        b.explain.effective_search_mode
    );

    // Evidence nodes: same ids, hops, and scores (order-independent).
    let mut na = a.evidence.nodes.clone();
    let mut nb = b.evidence.nodes.clone();
    na.sort_by_key(|n| n.id);
    nb.sort_by_key(|n| n.id);
    assert_eq!(na.len(), nb.len(), "evidence node count must match");
    for (x, y) in na.iter().zip(nb.iter()) {
        assert_eq!(x.id, y.id, "node id must match");
        assert_eq!(x.hop, y.hop, "hop for node {} must match", x.id);
        assert!(
            (x.score - y.score).abs() < 1e-6,
            "score for node {} must match: {} vs {}",
            x.id,
            x.score,
            y.score
        );
    }

    // Expansion paths must reconstruct identically (compare order-independently).
    let mut pa = a.explain.expansion_paths.clone();
    let mut pb = b.explain.expansion_paths.clone();
    pa.sort_by(|x, y| {
        x.target_id
            .cmp(&y.target_id)
            .then_with(|| x.path.cmp(&y.path))
    });
    pb.sort_by(|x, y| {
        x.target_id
            .cmp(&y.target_id)
            .then_with(|| x.path.cmp(&y.path))
    });
    assert_eq!(pa, pb, "expansion paths must be identical");

    // Groundedness is a pure function of evidence and must match.
    assert!(
        (a.groundedness - b.groundedness).abs() < 1e-6,
        "groundedness must be reproducible: {} vs {}",
        a.groundedness,
        b.groundedness
    );
}
