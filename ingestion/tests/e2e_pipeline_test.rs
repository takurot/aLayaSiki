use std::collections::HashMap;
use std::sync::Arc;

use alayasiki_core::ingest::IngestionRequest;
use ingestion::processor::IngestionPipeline;
use jobs::queue::{ChannelJobQueue, JobQueue};
use jobs::worker::Worker;
use query::{QueryEngine, QueryRequest};
use slm::ner::MockEntityExtractor;
use storage::community::{CommunityEngine, DeterministicSummarizer};
use storage::repo::Repository;
use tempfile::tempdir;
use tokio::sync::mpsc;

#[tokio::test]
async fn test_e2e_ingest_to_query_with_filters_and_citations() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("e2e_ingest_query.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());
    let pipeline = IngestionPipeline::new(repo.clone());

    let mut company_meta = HashMap::new();
    company_meta.insert("source".to_string(), "report/toyota-2024.md".to_string());
    company_meta.insert("entity_type".to_string(), "Company".to_string());
    company_meta.insert("timestamp".to_string(), "2024-03-10".to_string());

    let mut policy_meta = HashMap::new();
    policy_meta.insert("source".to_string(), "policy/ev-2022.md".to_string());
    policy_meta.insert("entity_type".to_string(), "Policy".to_string());
    policy_meta.insert("timestamp".to_string(), "2022-06-01".to_string());

    pipeline
        .ingest(IngestionRequest::Text {
            content: "Toyota expands EV battery partnerships in 2024.".to_string(),
            metadata: company_meta,
            idempotency_key: Some("e2e-doc-company".to_string()),
            model_id: Some("embedding-default-v1".to_string()),
        })
        .await
        .unwrap();

    pipeline
        .ingest(IngestionRequest::Text {
            content: "Government policy update for EV recycling in 2022.".to_string(),
            metadata: policy_meta,
            idempotency_key: Some("e2e-doc-policy".to_string()),
            model_id: Some("embedding-default-v1".to_string()),
        })
        .await
        .unwrap();

    let engine = QueryEngine::new(repo);
    let request = QueryRequest::parse_json(
        r#"{
            "query": "Toyota EV partnerships",
            "mode": "evidence",
            "search_mode": "local",
            "top_k": 10,
            "model_id": "embedding-default-v1",
            "filters": {
                "entity_type": ["Company"],
                "time_range": { "from": "2024-01-01", "to": "2024-12-31" }
            }
        }"#,
    )
    .unwrap();

    let response = engine.execute(request).await.unwrap();

    assert!(!response.evidence.nodes.is_empty());
    assert!(response
        .evidence
        .nodes
        .iter()
        .all(|node| node.data.contains("Toyota")));
    assert!(response
        .explain
        .exclusions
        .iter()
        .any(|ex| ex.reason == "entity_type_filtered" || ex.reason == "time_range_filtered"));
    assert!(!response.citations.is_empty());
    assert!(response.citations[0]
        .source
        .contains("report/toyota-2024.md"));
    assert_eq!(response.model_id.as_deref(), Some("embedding-default-v1"));
    assert!(response.snapshot_id.is_some());
}

#[tokio::test]
async fn test_e2e_query_is_reproducible_with_fixed_model_and_snapshot() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("e2e_repro.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());
    let pipeline = IngestionPipeline::new(repo.clone());

    let mut metadata = HashMap::new();
    metadata.insert("source".to_string(), "report/byd-2024.md".to_string());
    metadata.insert("entity_type".to_string(), "Company".to_string());
    metadata.insert("timestamp".to_string(), "2024-05-01".to_string());

    pipeline
        .ingest(IngestionRequest::Text {
            content: "BYD expands EV production with new battery facilities.".to_string(),
            metadata,
            idempotency_key: Some("e2e-repro-doc".to_string()),
            model_id: Some("embedding-default-v1".to_string()),
        })
        .await
        .unwrap();

    let snapshot_id = repo.current_snapshot_id().await;
    let engine = QueryEngine::new(repo);
    let pinned_request = QueryRequest::parse_json(&format!(
        r#"{{
            "query": "BYD EV production",
            "mode": "evidence",
            "search_mode": "local",
            "top_k": 5,
            "model_id": "embedding-default-v1",
            "snapshot_id": "{}"
        }}"#,
        snapshot_id
    ))
    .unwrap();

    let first = engine.execute(pinned_request.clone()).await.unwrap();

    // Mutate repository after taking snapshot. A pinned query should still
    // report the fixed snapshot_id, while an unpinned query moves forward.
    let mut extra_metadata = HashMap::new();
    extra_metadata.insert("source".to_string(), "report/noise-2026.md".to_string());
    extra_metadata.insert("entity_type".to_string(), "Policy".to_string());
    extra_metadata.insert("timestamp".to_string(), "2026-01-01".to_string());
    pipeline
        .ingest(IngestionRequest::Text {
            content: "Unrelated policy update for emissions reporting.".to_string(),
            metadata: extra_metadata,
            idempotency_key: Some("e2e-repro-extra-doc".to_string()),
            model_id: Some("embedding-default-v1".to_string()),
        })
        .await
        .unwrap();

    let second = engine.execute(pinned_request).await.unwrap();
    let latest_unpinned = engine
        .execute(
            QueryRequest::parse_json(
                r#"{
                    "query": "BYD EV production",
                    "mode": "evidence",
                    "search_mode": "local",
                    "top_k": 5,
                    "model_id": "embedding-default-v1"
                }"#,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let first_ids: Vec<u64> = first.evidence.nodes.iter().map(|n| n.id).collect();
    let second_ids: Vec<u64> = second.evidence.nodes.iter().map(|n| n.id).collect();

    assert!(
        first_ids.iter().all(|id| second_ids.contains(id)),
        "pinned snapshot query should retain previously observed evidence IDs"
    );
    assert_eq!(first.model_id, second.model_id);
    assert_eq!(first.snapshot_id, second.snapshot_id);
    assert_eq!(first.snapshot_id.as_deref(), Some(snapshot_id.as_str()));
    assert_ne!(latest_unpinned.snapshot_id, first.snapshot_id);
}

#[tokio::test]
async fn test_e2e_full_graphrag_flow_with_global_and_drift() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("e2e_graphrag.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    // 1. Setup Job Queue and Worker for asynchronous extraction
    let (tx, rx) = mpsc::channel(100);
    let job_queue = Arc::new(ChannelJobQueue::new(tx));
    let extractor = Arc::new(MockEntityExtractor::new().with_keywords(vec![
        ("Tesla".to_string(), "Company".to_string()),
        ("BYD".to_string(), "Company".to_string()),
    ]));
    let worker = Worker::new(rx, repo.clone(), extractor);
    let _worker_handle = tokio::spawn(async move {
        worker.run().await;
    });

    let mut pipeline = IngestionPipeline::new(repo.clone());
    pipeline.set_job_queue(job_queue);

    // 2. Ingest some documents that will trigger entity extraction
    pipeline
        .ingest(IngestionRequest::Text {
            content: "Tesla and BYD are major players in the global EV market.".to_string(),
            metadata: HashMap::from([("source".to_string(), "market_report.txt".to_string())]),
            idempotency_key: Some("doc-1".to_string()),
            model_id: Some("embedding-default-v1".to_string()),
        })
        .await
        .unwrap();

    pipeline
        .ingest(IngestionRequest::Text {
            content: "China's BYD has overtaken Tesla in volume for battery production.".to_string(),
            metadata: HashMap::from([("source".to_string(), "byd_news.txt".to_string())]),
            idempotency_key: Some("doc-2".to_string()),
            model_id: Some("embedding-default-v1".to_string()),
        })
        .await
        .unwrap();

    // 3. Wait for job worker to finish processing (Extraction & Edge Creation)
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // 4. Manually trigger community summary generation (simulating periodic background task)
    let summarizer = DeterministicSummarizer;
    let graph = repo.graph_index().await;
    let summaries = {
        let mut community_engine = CommunityEngine::new(graph);
        community_engine.rebuild_hierarchy(2, &summarizer);
        community_engine.summaries().to_vec()
    };

    let engine = QueryEngine::new(repo).with_community_summaries(summaries);

    // 5. Test Global Search (Map-Reduce over community summaries)
    let global_request = QueryRequest::parse_json(
        r#"{
            "query": "L0-C0",
            "search_mode": "global",
            "mode": "answer"
        }"#,
    )
    .unwrap();
    let global_response = engine.execute(global_request).await.unwrap();
    assert!(global_response.answer.is_some());
    assert!(global_response.answer.as_ref().unwrap().contains("Global synthesis"));
    assert!(global_response
        .explain
        .steps
        .contains(&"community_map_reduce".to_string()));

    // 6. Test DRIFT Search (Dynamic iterative expansion)
    let drift_request = QueryRequest::parse_json(
        r#"{
            "query": "Who is overtaking Tesla?",
            "search_mode": "drift",
            "mode": "answer",
            "top_k": 5
        }"#,
    )
    .unwrap();
    let drift_response = engine.execute(drift_request).await.unwrap();
    assert!(drift_response.answer.is_some());
    assert!(drift_response.answer.unwrap().contains("Answer synthesized"));
    assert!(drift_response.evidence.nodes.len() >= 1);
    assert!(drift_response
        .explain
        .steps
        .contains(&"drift_iterative_expansion".to_string()));

    // 7. Verify graph support in groundedness
    assert!(global_response.groundedness > 0.0);
    assert!(drift_response.groundedness > 0.0);
    // Drift should find BYD via "mentions" edge from Tesla anchor
    assert!(drift_response.evidence.edges.len() >= 1);
}
