use alayasiki_core::ingest::IngestionRequest;
use alayasiki_core::model::{Edge, Node};
use alayasiki_runtime::RuntimeBuilder;
use std::collections::HashMap;
use std::time::Duration;

async fn global(
    runtime: &alayasiki_runtime::AlayasikiRuntime,
    snapshot: Option<&str>,
) -> query::QueryResponse {
    let mut request = query::QueryRequest::parse_json(
        r#"{"query":"Rust company overview","search_mode":"global","mode":"answer","top_k":50}"#,
    )
    .unwrap();
    request.snapshot_id = snapshot.map(str::to_string);
    runtime.query().execute(request).await.unwrap()
}

#[tokio::test]
async fn standard_runtime_maintains_global_communities_across_mutation_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = RuntimeBuilder::new(dir.path()).start().await.unwrap();
    runtime
        .ingestion()
        .ingest(IngestionRequest::text(
            "Rust company acquired startup".into(),
            HashMap::new(),
        ))
        .await
        .unwrap();
    runtime.wait_for_jobs(Duration::from_secs(5)).await.unwrap();
    let summaries = runtime.refresh_communities().await.unwrap();
    assert!(!summaries.is_empty());
    assert!(summaries.iter().any(|s| s.summary.contains("Rust")));
    let before = runtime.repository().current_snapshot_id().await;
    let initial = global(&runtime, None).await;
    assert!(initial
        .explain
        .steps
        .iter()
        .any(|s| s == "community_map_reduce"));
    assert!(initial.answer.as_deref().unwrap().contains("Community"));
    let count = runtime.community_rebuild_count();
    runtime.refresh_communities().await.unwrap();
    assert_eq!(runtime.community_rebuild_count(), count);

    let repo = runtime.repository();
    repo.put_node(Node::new(1001, vec![], "Future graph content".into()))
        .await
        .unwrap();
    repo.put_node(Node::new(1002, vec![], "Future companion".into()))
        .await
        .unwrap();
    repo.put_edge(Edge {
        source: 1001,
        target: 1002,
        relation: "related".into(),
        weight: 1.0,
        metadata: HashMap::new(),
    })
    .await
    .unwrap();
    let updated = runtime.refresh_communities().await.unwrap();
    assert!(updated.iter().any(|s| s.summary.contains("Future")));
    assert_eq!(runtime.community_rebuild_count(), count + 1);
    let pinned = global(&runtime, Some(&before)).await;
    assert!(pinned
        .explain
        .steps
        .iter()
        .any(|s| s == "community_map_reduce"));
    assert!(!pinned.answer.as_deref().unwrap().contains("Future"));
    assert!(!pinned
        .evidence
        .nodes
        .iter()
        .any(|n| n.id == 1001 || n.id == 1002));
    runtime.shutdown().await.unwrap();
    let runtime = RuntimeBuilder::new(dir.path()).start().await.unwrap();
    assert_eq!(runtime.refresh_communities().await.unwrap(), updated);
    let pinned_again = global(&runtime, Some(&before)).await;
    assert_eq!(pinned.answer, pinned_again.answer);
    runtime.repository().delete_node(1001).await.unwrap();
    assert!(!runtime
        .refresh_communities()
        .await
        .unwrap()
        .iter()
        .any(|s| s.summary.contains("Future graph")));
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn tenant_communities_exclude_foreign_nodes_and_cross_tenant_edges() {
    use alayasiki_core::auth::{Authorizer, Principal, ResourceContext};
    let dir = tempfile::tempdir().unwrap();
    let runtime = RuntimeBuilder::new(dir.path())
        .extraction_enabled(false)
        .start()
        .await
        .unwrap();
    for (id, tenant, text) in [
        (1, "a", "Rust public overview"),
        (2, "a", "Rust partner"),
        (3, "b", "Rust secretforeign"),
        (4, "b", "Rust secretcompanion"),
    ] {
        let mut node = Node::new(id, vec![], text.into());
        node.metadata.insert("tenant".into(), tenant.into());
        runtime.repository().put_node(node).await.unwrap();
    }
    for (source, target) in [(1, 2), (3, 4), (1, 3)] {
        runtime
            .repository()
            .put_edge(Edge {
                source,
                target,
                relation: "related".into(),
                weight: 1.0,
                metadata: HashMap::new(),
            })
            .await
            .unwrap();
    }
    let request = query::QueryRequest::parse_json(
        r#"{"query":"Rust overview","search_mode":"global","mode":"answer"}"#,
    )
    .unwrap();
    for (tenant, allowed, forbidden) in [
        ("a", "public", "secretforeign"),
        ("b", "secretforeign", "public"),
    ] {
        let principal = Principal::new("reader", tenant).with_roles(["reader"]);
        let response = runtime
            .query()
            .execute_authorized(
                request.clone(),
                &principal,
                &Authorizer::default(),
                &ResourceContext::new(tenant),
            )
            .await
            .unwrap();
        assert!(response
            .explain
            .steps
            .iter()
            .any(|s| s == "community_map_reduce"));
        assert!(response.answer.as_deref().unwrap().contains(allowed));
        assert!(!response.answer.as_deref().unwrap().contains(forbidden));
        assert!(response.evidence.nodes.iter().all(|n| if tenant == "a" {
            n.id <= 2
        } else {
            n.id >= 3
        }));
    }
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn empty_graph_falls_back_and_pinned_graph_survives_cache_eviction() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = RuntimeBuilder::new(dir.path())
        .extraction_enabled(false)
        .start()
        .await
        .unwrap();
    let empty = global(&runtime, None).await;
    assert!(empty
        .explain
        .steps
        .iter()
        .any(|s| s == "global_fallback_no_community_data"));
    runtime
        .repository()
        .put_node(Node::new(1, vec![], "Rust first".into()))
        .await
        .unwrap();
    runtime
        .repository()
        .put_node(Node::new(2, vec![], "Rust second".into()))
        .await
        .unwrap();
    runtime
        .repository()
        .put_edge(Edge {
            source: 1,
            target: 2,
            relation: "related".into(),
            weight: 1.0,
            metadata: HashMap::new(),
        })
        .await
        .unwrap();
    let pinned = runtime.repository().current_snapshot_id().await;
    let expected = runtime.refresh_communities().await.unwrap();
    for id in 100..135 {
        runtime
            .repository()
            .put_node(Node::new(id, vec![], format!("version {id}")))
            .await
            .unwrap();
        runtime.refresh_communities().await.unwrap();
    }
    let response = global(&runtime, Some(&pinned)).await;
    assert!(response
        .explain
        .steps
        .iter()
        .any(|s| s == "community_map_reduce"));
    assert!(!response.answer.as_deref().unwrap().contains("version"));
    assert!(expected.iter().all(|s| s.snapshot_lsn_range.is_some()));
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn filtered_and_retention_queries_never_return_excluded_community_text() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = RuntimeBuilder::new(dir.path())
        .extraction_enabled(false)
        .start()
        .await
        .unwrap();
    for (id, kind, text, retention) in [
        (1, "Company", "Rust allowed", None),
        (2, "Person", "Rust forbiddenperson", Some("0")),
    ] {
        let mut node = Node::new(
            id,
            alayasiki_core::embedding::deterministic_embedding(text, "embedding-default-v1", 768),
            text.into(),
        );
        node.metadata.insert("entity_type".into(), kind.into());
        if let Some(retention) = retention {
            node.metadata
                .insert("retention_until_unix".into(), retention.into());
        }
        runtime.repository().put_node(node).await.unwrap();
    }
    runtime
        .repository()
        .put_edge(Edge {
            source: 2,
            target: 1,
            relation: "related".into(),
            weight: 1.0,
            metadata: HashMap::new(),
        })
        .await
        .unwrap();
    let retained = global(&runtime, None).await;
    assert!(retained
        .answer
        .as_deref()
        .unwrap_or_default()
        .contains("allowed"));
    assert!(!retained
        .answer
        .as_deref()
        .unwrap_or_default()
        .contains("forbiddenperson"));
    let snapshot = runtime.repository().current_snapshot_id().await;
    let mut request = query::QueryRequest::parse_json(
        r#"{"query":"Rust overview","search_mode":"global","filters":{"entity_type":["Company"]}}"#,
    )
    .unwrap();
    request.snapshot_id = Some(snapshot);
    let filtered = runtime.query().execute(request).await.unwrap();
    assert!(filtered
        .answer
        .as_deref()
        .unwrap_or_default()
        .contains("allowed"));
    assert!(!filtered
        .answer
        .as_deref()
        .unwrap_or_default()
        .contains("forbiddenperson"));
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn retention_changes_are_respected_without_a_new_graph_version() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = RuntimeBuilder::new(dir.path())
        .extraction_enabled(false)
        .start()
        .await
        .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    for (id, text, deadline) in [
        (1, "Rust allowed", None),
        (2, "Rust transientsecret", Some(now + 2)),
    ] {
        let mut node = Node::new(
            id,
            alayasiki_core::embedding::deterministic_embedding(text, "embedding-default-v1", 768),
            text.into(),
        );
        if let Some(deadline) = deadline {
            node.metadata
                .insert("retention_until_unix".into(), deadline.to_string());
        }
        runtime.repository().put_node(node).await.unwrap();
    }
    runtime
        .repository()
        .put_edge(Edge {
            source: 2,
            target: 1,
            relation: "related".into(),
            weight: 1.0,
            metadata: HashMap::new(),
        })
        .await
        .unwrap();
    let snapshot = runtime.repository().current_snapshot_id().await;
    let response = global(&runtime, None).await;
    assert!(response.evidence.nodes.iter().any(|n| n.id == 2));
    tokio::time::sleep(Duration::from_secs(3)).await;
    let response = global(&runtime, None).await;
    assert!(!response
        .answer
        .as_deref()
        .unwrap_or_default()
        .contains("transientsecret"));
    assert!(!response.evidence.nodes.iter().any(|n| n.id == 2));
    assert_eq!(runtime.repository().current_snapshot_id().await, snapshot);
    runtime.shutdown().await.unwrap();
}
