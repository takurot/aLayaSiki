use std::sync::Arc;

use alayasiki_core::audit::{
    AuditError, AuditEvent, AuditFailurePolicy, AuditOperation, AuditOutcome, AuditSink,
    InMemoryAuditSink,
};
use alayasiki_core::auth::{Authorizer, Principal, ResourceContext};
use alayasiki_core::embedding::deterministic_embedding;
use alayasiki_core::model::Node;
use query::{QueryEngine, QueryRequest};
use storage::repo::Repository;
use tempfile::tempdir;

/// An `AuditSink` that always fails, used to exercise audit failure
/// policies without depending on real I/O failure conditions.
#[derive(Default)]
struct FailingAuditSink;

impl AuditSink for FailingAuditSink {
    fn record(&self, _event: AuditEvent) -> Result<(), AuditError> {
        Err(AuditError::LockPoisoned)
    }
}

fn sample_query_request() -> QueryRequest {
    QueryRequest::parse_json(
        r#"{
            "query":"EV strategy",
            "mode":"evidence",
            "search_mode":"local",
            "top_k":1,
            "model_id":"embedding-default-v1"
        }"#,
    )
    .unwrap()
}

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

#[tokio::test]
async fn query_strict_policy_surfaces_audit_failure_instead_of_success() {
    let repo = build_repo().await;
    let engine = QueryEngine::new(repo)
        .with_audit_sink(Arc::new(FailingAuditSink))
        .with_audit_failure_policy(AuditFailurePolicy::Strict);

    let result = engine.execute(sample_query_request()).await;

    assert!(
        result.is_err(),
        "strict policy must not report success when the audit write failed"
    );
    assert_eq!(engine.audit_health().failure_count(), 1);
    assert!(engine.audit_health().is_degraded());
}

#[tokio::test]
async fn query_best_effort_policy_continues_and_records_degraded_health() {
    let repo = build_repo().await;
    let engine = QueryEngine::new(repo)
        .with_audit_sink(Arc::new(FailingAuditSink))
        .with_audit_failure_policy(AuditFailurePolicy::BestEffort);

    let result = engine.execute(sample_query_request()).await;

    assert!(
        result.is_ok(),
        "best-effort policy must let the operation continue despite audit loss"
    );
    assert_eq!(engine.audit_health().failure_count(), 1);
    assert!(engine.audit_health().is_degraded());
}

#[tokio::test]
async fn query_authorized_strict_policy_surfaces_audit_failure_on_denial() {
    let repo = build_repo().await;
    let engine = QueryEngine::new(repo)
        .with_audit_sink(Arc::new(FailingAuditSink))
        .with_audit_failure_policy(AuditFailurePolicy::Strict);

    let principal = Principal::new("ingestor-1", "acme").with_roles(["ingestor"]);
    let authorizer = Authorizer::default();
    let resource = ResourceContext::new("acme");

    let result = engine
        .execute_authorized(sample_query_request(), &principal, &authorizer, &resource)
        .await;

    assert!(result.is_err(), "denied operation must remain an error");
    assert_eq!(engine.audit_health().failure_count(), 1);
}
