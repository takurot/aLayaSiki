use std::sync::Arc;

use alayasiki_core::auth::{Authorizer, AuthzError, JwtAuthenticator, Principal, ResourceContext};
use alayasiki_core::embedding::deterministic_embedding;
use alayasiki_core::model::Node;
use query::{QueryEngine, QueryError, QueryRequest};
use storage::community::CommunitySummary;
use storage::repo::Repository;
use tempfile::tempdir;

async fn build_engine() -> (Arc<Repository>, QueryEngine) {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("query_authz.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    let mut node = Node::new(
        1,
        deterministic_embedding("EV strategy", "embedding-default-v1", 8),
        "Toyota expands EV strategy".to_string(),
    );
    node.metadata
        .insert("tenant".to_string(), "acme".to_string());

    repo.put_node(node).await.unwrap();

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

#[tokio::test]
async fn execute_json_jwt_authorized_authenticates_before_parsing_query() {
    let (_repo, engine) = build_engine().await;
    let authenticator =
        JwtAuthenticator::new_hs256("jwt-secret", Some("alayasiki-auth"), Some("alayasiki-api"));
    let authorizer = Authorizer::default();
    let resource = ResourceContext::new("acme");

    let err = engine
        .execute_json_jwt_authorized(
            r#"{"query":"EV strategy","mode":"evidence","search_mode":"local","top_k":"broken"}"#,
            "not-a-jwt",
            &authenticator,
            &authorizer,
            &resource,
        )
        .await
        .unwrap_err();

    assert!(matches!(err, QueryError::Unauthenticated(_)));
}

#[tokio::test]
async fn execute_authorized_global_avoids_cross_tenant_summary_synthesis() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("query_authz_global_tenant_scope.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    let mut acme_node = Node::new(
        10,
        deterministic_embedding("shared market signal", "embedding-default-v1", 8),
        "Acme-only market signal".to_string(),
    );
    acme_node
        .metadata
        .insert("tenant".to_string(), "acme".to_string());

    let mut beta_node = Node::new(
        11,
        deterministic_embedding("shared market signal", "embedding-default-v1", 8),
        "Beta-only market signal".to_string(),
    );
    beta_node
        .metadata
        .insert("tenant".to_string(), "beta".to_string());

    repo.put_node(acme_node).await.unwrap();
    repo.put_node(beta_node).await.unwrap();

    let summaries = vec![CommunitySummary {
        level: 0,
        community_id: 1,
        top_nodes: vec![10, 11],
        summary: "Cross-tenant summary mentions beta confidential program".to_string(),
        snapshot_lsn_range: None,
    }];
    let engine = QueryEngine::new(repo).with_community_summaries(summaries);

    let principal = Principal::new("reader-1", "acme").with_roles(["reader"]);
    let authorizer = Authorizer::default();
    let resource = ResourceContext::new("acme");

    let request = QueryRequest::parse_json(
        r#"{
            "query":"shared market signal",
            "mode":"answer",
            "search_mode":"global",
            "top_k":5
        }"#,
    )
    .unwrap();

    let response = engine
        .execute_authorized(request, &principal, &authorizer, &resource)
        .await
        .unwrap();

    let answer = response.answer.unwrap_or_default();
    assert!(!answer.contains("Global synthesis"));
    assert!(!answer.contains("beta confidential"));
    assert!(response.evidence.nodes.iter().all(|node| node.id == 10));
    assert!(response
        .explain
        .exclusions
        .iter()
        .any(|x| x.reason == "global_summary_disabled_by_tenant_scope"));
}
