use std::sync::Arc;

use alayasiki_core::audit::{AuditOperation, AuditOutcome, InMemoryAuditSink};
use alayasiki_core::auth::{Authorizer, Principal, ResourceContext};
use alayasiki_core::embedding::deterministic_embedding;
use alayasiki_core::model::Node;
use query::{QueryEngine, QueryRequest};
use storage::repo::Repository;
use tempfile::tempdir;

async fn build_repo() -> Arc<Repository> {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("query_audit.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    repo.put_node(Node::new(
        1,
        deterministic_embedding("EV strategy", "embedding-default-v1", 8),
        "Toyota expands EV strategy".to_string(),
    ))
    .await
    .unwrap();

    repo
}

#[tokio::test]
async fn query_records_audit_event_with_model_id() {
    let repo = build_repo().await;
    let sink = Arc::new(InMemoryAuditSink::default());
    let engine = QueryEngine::new(repo).with_audit_sink(sink.clone());

    let request = QueryRequest::parse_json(
        r#"{
            "query":"EV strategy",
            "mode":"evidence",
            "search_mode":"local",
            "top_k":1,
            "model_id":"embedding-default-v1"
        }"#,
    )
    .unwrap();

    engine.execute(request).await.unwrap();

    let events = sink.events().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].operation, AuditOperation::Query);
    assert_eq!(events[0].outcome, AuditOutcome::Succeeded);
    assert_eq!(events[0].model_id.as_deref(), Some("embedding-default-v1"));
    assert!(events[0].snapshot_id.is_some());
}

#[tokio::test]
async fn query_authorized_records_denied_audit_event() {
    let repo = build_repo().await;
    let sink = Arc::new(InMemoryAuditSink::default());
    let engine = QueryEngine::new(repo).with_audit_sink(sink.clone());

    let request = QueryRequest::parse_json(
        r#"{
            "query":"EV strategy",
            "mode":"evidence",
            "search_mode":"local",
            "top_k":1
        }"#,
    )
    .unwrap();

    let principal = Principal::new("ingestor-1", "acme").with_roles(["ingestor"]);
    let authorizer = Authorizer::default();
    let resource = ResourceContext::new("acme");

    let result = engine
        .execute_authorized(request, &principal, &authorizer, &resource)
        .await;
    assert!(result.is_err());

    let events = sink.events().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].operation, AuditOperation::Query);
    assert_eq!(events[0].outcome, AuditOutcome::Denied);
    assert_eq!(events[0].actor.as_deref(), Some("ingestor-1"));
    assert_eq!(events[0].tenant.as_deref(), Some("acme"));
    assert!(events[0].metadata.contains_key("error"));
}
