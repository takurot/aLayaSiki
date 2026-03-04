use alayasiki_core::auth::{Authorizer, Principal, ResourceContext};
use alayasiki_core::ingest::IngestionRequest;
use ingestion::processor::IngestionPipeline;
use query::dsl::{QueryRequest, SearchMode};
use query::engine::{QueryEngine, QueryError};
use std::collections::HashMap;
use std::sync::Arc;
use storage::repo::{RepoError, Repository};
use storage::wal::Wal;
use tempfile::tempdir;
use tokio::sync::Mutex;

#[tokio::test]
async fn test_session_graph_isolation_and_merging() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal");
    let wal = Wal::open(&wal_path).await.unwrap();
    let repo = Arc::new(Repository::new(Arc::new(Mutex::new(wal))));
    let pipeline = IngestionPipeline::new(repo.clone());
    let engine = QueryEngine::new(repo.clone());

    let principal = Principal::new("user1", "tenant1").with_roles(["admin"]);
    let authorizer = Authorizer::new();
    let resource = ResourceContext::new("tenant1");

    // 1. Ingest persistent node
    let req1 = IngestionRequest::text("Persistent context".to_string(), HashMap::new());
    let p_ids = pipeline
        .ingest_authorized(req1, &principal, &authorizer, &resource)
        .await
        .unwrap();
    let p_id = p_ids[0];

    // 2. Ingest session node
    let session_id = "session-123";
    let req2 = IngestionRequest::text("Volatile thinking".to_string(), HashMap::new());
    let s_ids = pipeline
        .ingest_to_session_authorized(session_id, req2, &principal, &authorizer, &resource)
        .await
        .unwrap();
    let s_id = s_ids[0];

    // 3. Query without session -> only persistent node found
    let query_req_no_session = QueryRequest {
        query: "Volatile thinking".to_string(),
        session_id: None,
        search_mode: SearchMode::Local,
        ..Default::default()
    };
    let resp_no_session = engine.execute(query_req_no_session).await.unwrap();
    assert!(resp_no_session.evidence.nodes.iter().all(|n| n.id != s_id));

    // 4. Query with session -> session node found
    let query_req_with_session = QueryRequest {
        query: "Volatile thinking".to_string(),
        session_id: Some(session_id.to_string()),
        search_mode: SearchMode::Local,
        ..Default::default()
    };
    let resp_with_session = engine.execute(query_req_with_session).await.unwrap();
    assert!(resp_with_session
        .evidence
        .nodes
        .iter()
        .any(|n| n.id == s_id));

    // 5. Query with session for persistent content -> persistent node found
    let query_req_p = QueryRequest {
        query: "Persistent context".to_string(),
        session_id: Some(session_id.to_string()),
        search_mode: SearchMode::Local,
        ..Default::default()
    };
    let resp_p = engine.execute(query_req_p).await.unwrap();
    assert!(resp_p.evidence.nodes.iter().any(|n| n.id == p_id));
}

#[tokio::test]
async fn test_session_promotion_to_persistent() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal");
    let wal = Wal::open(&wal_path).await.unwrap();
    let repo = Arc::new(Repository::new(Arc::new(Mutex::new(wal))));
    let pipeline = IngestionPipeline::new(repo.clone());
    let engine = QueryEngine::new(repo.clone());

    let principal = Principal::new("user1", "tenant1").with_roles(["admin"]);
    let authorizer = Authorizer::new();
    let resource = ResourceContext::new("tenant1");

    let session_id = "session-promote";
    let req = IngestionRequest::text("To be promoted".to_string(), HashMap::new());
    let s_ids = pipeline
        .ingest_to_session_authorized(session_id, req, &principal, &authorizer, &resource)
        .await
        .unwrap();
    let s_id = s_ids[0];

    // Promote
    repo.promote_session_to_persistent(session_id)
        .await
        .unwrap();

    // Now query WITHOUT session_id should find it
    let query_req = QueryRequest {
        query: "promoted".to_string(),
        session_id: None,
        search_mode: SearchMode::Local,
        ..Default::default()
    };
    let resp = engine.execute(query_req).await.unwrap();
    assert!(resp.evidence.nodes.iter().any(|n| n.id == s_id));

    // Session should be cleared
    assert!(repo.session_manager.get(session_id).is_none());
}

#[tokio::test]
async fn test_session_only_query_works_without_persistent_nodes() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal");
    let wal = Wal::open(&wal_path).await.unwrap();
    let repo = Arc::new(Repository::new(Arc::new(Mutex::new(wal))));
    let pipeline = IngestionPipeline::new(repo.clone());
    let engine = QueryEngine::new(repo.clone());

    let principal = Principal::new("user1", "tenant1").with_roles(["admin"]);
    let authorizer = Authorizer::new();
    let resource = ResourceContext::new("tenant1");

    let session_id = "session-only";
    pipeline
        .ingest_to_session_authorized(
            session_id,
            IngestionRequest::text("only in session memory".to_string(), HashMap::new()),
            &principal,
            &authorizer,
            &resource,
        )
        .await
        .unwrap();

    let response = engine
        .execute(QueryRequest {
            query: "session memory".to_string(),
            session_id: Some(session_id.to_string()),
            search_mode: SearchMode::Local,
            ..Default::default()
        })
        .await
        .unwrap();

    assert!(response
        .evidence
        .nodes
        .iter()
        .any(|node| node.data.contains("only in session memory")));
}

#[tokio::test]
async fn test_session_owner_isolation_denies_cross_user_access() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal");
    let wal = Wal::open(&wal_path).await.unwrap();
    let repo = Arc::new(Repository::new(Arc::new(Mutex::new(wal))));
    let pipeline = IngestionPipeline::new(repo.clone());
    let engine = QueryEngine::new(repo.clone());

    let authorizer = Authorizer::new();
    let resource = ResourceContext::new("tenant1");
    let owner = Principal::new("owner", "tenant1").with_roles(["admin"]);
    let other_user = Principal::new("other", "tenant1").with_roles(["admin"]);

    let session_id = "owned-session";
    pipeline
        .ingest_to_session_authorized(
            session_id,
            IngestionRequest::text("private scratchpad".to_string(), HashMap::new()),
            &owner,
            &authorizer,
            &resource,
        )
        .await
        .unwrap();

    let denied = engine
        .execute_authorized(
            QueryRequest {
                query: "scratchpad".to_string(),
                session_id: Some(session_id.to_string()),
                search_mode: SearchMode::Local,
                ..Default::default()
            },
            &other_user,
            &authorizer,
            &resource,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        denied,
        QueryError::Repository(RepoError::SessionAccessDenied(_))
    ));
}
