use alayasiki_core::audit::{AuditOperation, AuditOutcome, InMemoryAuditSink};
use alayasiki_core::auth::{Authorizer, Principal, ResourceContext};
use alayasiki_core::ingest::IngestionRequest;
use ingestion::processor::IngestionPipeline;
use std::collections::HashMap;
use std::sync::Arc;
use storage::repo::Repository;
use tempfile::tempdir;

#[tokio::test]
async fn ingest_records_audit_event_with_model_id() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("ingest_audit.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    let sink = Arc::new(InMemoryAuditSink::default());
    let mut pipeline = IngestionPipeline::new(repo);
    pipeline.set_audit_sink(sink.clone());

    let request = IngestionRequest::Text {
        content: "audit trail text".to_string(),
        metadata: HashMap::new(),
        idempotency_key: None,
        model_id: Some("embedding-audit-v1".to_string()),
    };

    pipeline.ingest(request).await.unwrap();

    let events = sink.events().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].operation, AuditOperation::Ingest);
    assert_eq!(events[0].outcome, AuditOutcome::Succeeded);
    assert_eq!(events[0].model_id.as_deref(), Some("embedding-audit-v1"));
}

#[tokio::test]
async fn ingest_authorized_records_denied_audit_event() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("ingest_audit_denied.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    let sink = Arc::new(InMemoryAuditSink::default());
    let mut pipeline = IngestionPipeline::new(repo);
    pipeline.set_audit_sink(sink.clone());

    let request = IngestionRequest::Text {
        content: "unauthorized ingestion".to_string(),
        metadata: HashMap::new(),
        idempotency_key: None,
        model_id: None,
    };
    let principal = Principal::new("reader-1", "acme").with_roles(["reader"]);
    let authorizer = Authorizer::default();
    let resource = ResourceContext::new("acme");

    let result = pipeline
        .ingest_authorized(request, &principal, &authorizer, &resource)
        .await;
    assert!(result.is_err());

    let events = sink.events().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].operation, AuditOperation::Ingest);
    assert_eq!(events[0].outcome, AuditOutcome::Denied);
    assert_eq!(events[0].actor.as_deref(), Some("reader-1"));
    assert_eq!(events[0].tenant.as_deref(), Some("acme"));
    assert!(events[0].metadata.contains_key("error"));
}
