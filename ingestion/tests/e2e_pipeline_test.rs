use std::collections::HashMap;
use std::sync::Arc;

use alayasiki_core::ingest::IngestionRequest;
use ingestion::processor::IngestionPipeline;
use query::{QueryEngine, QueryRequest};
use storage::repo::Repository;
use tempfile::tempdir;

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
    let request = QueryRequest::parse_json(&format!(
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

    let first = engine.execute(request.clone()).await.unwrap();
    let second = engine.execute(request).await.unwrap();

    let first_ids: Vec<u64> = first.evidence.nodes.iter().map(|n| n.id).collect();
    let second_ids: Vec<u64> = second.evidence.nodes.iter().map(|n| n.id).collect();

    assert_eq!(first_ids, second_ids);
    assert_eq!(first.citations, second.citations);
    assert_eq!(first.model_id, second.model_id);
    assert_eq!(first.snapshot_id, second.snapshot_id);
}
