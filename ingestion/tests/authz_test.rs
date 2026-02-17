use alayasiki_core::auth::{Authorizer, AuthzError, Principal, ResourceContext};
use alayasiki_core::ingest::IngestionRequest;
use ingestion::processor::{IngestionError, IngestionPipeline};
use std::collections::HashMap;
use std::sync::Arc;
use storage::repo::Repository;
use tempfile::tempdir;

fn sample_request() -> IngestionRequest {
    IngestionRequest::Text {
        content: "Authorized ingestion text".to_string(),
        metadata: HashMap::new(),
        idempotency_key: None,
        model_id: None,
    }
}

#[tokio::test]
async fn ingest_authorized_allows_ingestor_role() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("auth_allow.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());
    let pipeline = IngestionPipeline::new(repo.clone());

    let principal = Principal::new("user-1", "acme").with_roles(["ingestor"]);
    let authorizer = Authorizer::default();
    let resource = ResourceContext::new("acme");

    let ids = pipeline
        .ingest_authorized(sample_request(), &principal, &authorizer, &resource)
        .await
        .unwrap();

    assert!(!ids.is_empty());
    assert!(!repo.list_node_ids().await.is_empty());
}

#[tokio::test]
async fn ingest_authorized_denies_reader_role() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("auth_deny_role.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());
    let pipeline = IngestionPipeline::new(repo.clone());

    let principal = Principal::new("user-1", "acme").with_roles(["reader"]);
    let authorizer = Authorizer::default();
    let resource = ResourceContext::new("acme");

    let err = pipeline
        .ingest_authorized(sample_request(), &principal, &authorizer, &resource)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        IngestionError::Unauthorized(AuthzError::PermissionDenied { .. })
    ));
    assert!(repo.list_node_ids().await.is_empty());
}

#[tokio::test]
async fn ingest_authorized_enforces_abac_attributes() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("auth_deny_abac.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());
    let pipeline = IngestionPipeline::new(repo.clone());

    let principal = Principal::new("user-1", "acme")
        .with_roles(["ingestor"])
        .with_attribute("department", "finance");
    let authorizer = Authorizer::default();
    let resource = ResourceContext::new("acme").require_attribute("department", "research");

    let err = pipeline
        .ingest_authorized(sample_request(), &principal, &authorizer, &resource)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        IngestionError::Unauthorized(AuthzError::AttributeMismatch { .. })
    ));
    assert!(repo.list_node_ids().await.is_empty());
}
