use std::sync::Arc;

use alayasiki_core::auth::{Authorizer, AuthzError, Principal, ResourceContext};
use alayasiki_core::embedding::deterministic_embedding;
use alayasiki_core::model::Node;
use query::{QueryEngine, QueryError, QueryRequest};
use storage::repo::Repository;
use tempfile::tempdir;

async fn build_engine() -> (Arc<Repository>, QueryEngine) {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("query_authz.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    repo.put_node(Node::new(
        1,
        deterministic_embedding("EV strategy", "embedding-default-v1", 8),
        "Toyota expands EV strategy".to_string(),
    ))
    .await
    .unwrap();

    let engine = QueryEngine::new(repo.clone());
    (repo, engine)
}

#[tokio::test]
async fn execute_authorized_allows_reader_role() {
    let (_repo, engine) = build_engine().await;
    let principal = Principal::new("user-1", "acme").with_roles(["reader"]);
    let authorizer = Authorizer::default();
    let resource = ResourceContext::new("acme");

    let request = QueryRequest::parse_json(
        r#"{
            "query":"EV strategy",
            "mode":"evidence",
            "search_mode":"local",
            "top_k":3
        }"#,
    )
    .unwrap();

    let response = engine
        .execute_authorized(request, &principal, &authorizer, &resource)
        .await
        .unwrap();

    assert!(!response.evidence.nodes.is_empty());
}

#[tokio::test]
async fn execute_authorized_denies_ingestor_role() {
    let (_repo, engine) = build_engine().await;
    let principal = Principal::new("user-1", "acme").with_roles(["ingestor"]);
    let authorizer = Authorizer::default();
    let resource = ResourceContext::new("acme");

    let request = QueryRequest::parse_json(
        r#"{
            "query":"EV strategy",
            "mode":"evidence",
            "search_mode":"local",
            "top_k":3
        }"#,
    )
    .unwrap();

    let err = engine
        .execute_authorized(request, &principal, &authorizer, &resource)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        QueryError::Unauthorized(AuthzError::PermissionDenied { .. })
    ));
}

#[tokio::test]
async fn execute_authorized_enforces_tenant_boundary() {
    let (_repo, engine) = build_engine().await;
    let principal = Principal::new("user-1", "acme").with_roles(["reader"]);
    let authorizer = Authorizer::default();
    let resource = ResourceContext::new("other-tenant");

    let request = QueryRequest::parse_json(
        r#"{
            "query":"EV strategy",
            "mode":"evidence",
            "search_mode":"local",
            "top_k":3
        }"#,
    )
    .unwrap();

    let err = engine
        .execute_authorized(request, &principal, &authorizer, &resource)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        QueryError::Unauthorized(AuthzError::TenantMismatch { .. })
    ));
}
