use alayasiki_api::{router, ApiState, ServerConfig};
use alayasiki_core::{auth::JwtClaims, ingest::IngestionRequest};
use alayasiki_runtime::RuntimeBuilder;
use jsonwebtoken::{encode, EncodingKey, Header};
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc};
fn token(tenant: &str, role: &str) -> String {
    encode(
        &Header::default(),
        &JwtClaims {
            sub: "test".into(),
            tenant: tenant.into(),
            roles: vec![role.into()],
            scope: None,
            attributes: HashMap::new(),
            iss: Some("test".into()),
            aud: Some("api".into()),
            exp: usize::MAX / 2,
            nbf: None,
            iat: None,
        },
        &EncodingKey::from_secret(b"test-secret-at-least-32-bytes-long"),
    )
    .unwrap()
}
#[tokio::test]
async fn http_ingest_query_and_tenant_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let config = ServerConfig::new(
        "test-secret-at-least-32-bytes-long".into(),
        "test".into(),
        "api".into(),
    )
    .unwrap();
    let runtime = RuntimeBuilder::new(dir.path()).start().await.unwrap();
    let runtime = Arc::new(runtime);
    let state = ApiState::new(runtime.clone(), config, dir.path().join("audit.jsonl")).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router(state)).await.unwrap() });
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .get(format!("{base}/health"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        client
            .post(format!("{base}/v1/query"))
            .json(&json!({"query":"Alice"}))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    let mut request =
        IngestionRequest::text("Alice startup builds Rust databases".into(), HashMap::new());
    if let IngestionRequest::Text {
        idempotency_key, ..
    } = &mut request
    {
        *idempotency_key = Some("same-key".into());
    }
    let mut ids = Vec::new();
    for tenant in ["a", "b"] {
        let result: Value = client
            .post(format!("{base}/v1/ingest"))
            .bearer_auth(token(tenant, "admin"))
            .json(&request)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        ids.push(result["data"]["node_ids"].clone());
        let response: Value = client
            .post(format!("{base}/v1/query"))
            .bearer_auth(token(tenant, "admin"))
            .json(&json!({"query":"Alice", "search_mode":"local"}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response["success"], true);
        assert!(!response["data"]["evidence"]["nodes"]
            .as_array()
            .unwrap()
            .is_empty());
    }
    assert_ne!(ids[0], ids[1]);
    runtime
        .repository()
        .put_node(alayasiki_core::model::Node {
            id: 42,
            embedding: vec![],
            data: "expired".into(),
            metadata: HashMap::from([
                ("tenant".into(), "a".into()),
                ("retention_until_unix".into(), "1".into()),
            ]),
        })
        .await
        .unwrap();
    runtime
        .wait_for_jobs(std::time::Duration::from_secs(5))
        .await
        .unwrap();
    let mut entity_ids = Vec::new();
    for tenant in ["a", "b"] {
        let graph: Value = client
            .get(format!("{base}/v1/graph"))
            .bearer_auth(token(tenant, "admin"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let nodes = graph["data"]["nodes"].as_array().unwrap();
        assert!(nodes
            .iter()
            .all(|node| node["id"].is_string() && node["metadata"]["tenant"] == tenant));
        let entities: Vec<_> = nodes
            .iter()
            .filter(|node| node["metadata"]["type"] == "entity")
            .map(|node| node["id"].as_str().unwrap().to_owned())
            .collect();
        assert!(!entities.is_empty());
        entity_ids.push(entities);
        let page: Value = client
            .get(format!("{base}/v1/graph?limit=1"))
            .bearer_auth(token(tenant, "admin"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(page["data"]["nodes"].as_array().unwrap().len(), 1);
        assert!(page["data"]["next_after"].is_string());
    }
    assert!(entity_ids[0].iter().all(|id| !entity_ids[1].contains(id)));
    assert_eq!(
        client
            .get(format!("{base}/metrics"))
            .bearer_auth(token("a", "admin"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        client
            .post(format!("{base}/v1/query"))
            .bearer_auth("bad.jwt")
            .json(&json!({"query":"Alice"}))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        client
            .post(format!("{base}/v1/query"))
            .bearer_auth(token("a", "admin"))
            .json(&json!({"query":""}))
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    for (mime, name, content, metadata) in [
        (
            "application/pdf",
            "dummy.pdf",
            include_bytes!("../../ingestion/tests/assets/dummy.pdf").to_vec(),
            HashMap::new(),
        ),
        (
            "image/png",
            "diagram.png",
            vec![1, 2, 3],
            HashMap::from([("ocr_text".into(), "Alice image diagram".into())]),
        ),
        (
            "audio/wav",
            "brief.wav",
            vec![1, 2, 3],
            HashMap::from([("transcript".into(), "Alice audio summary".into())]),
        ),
    ] {
        let request = IngestionRequest::file(name.into(), content, mime.into(), metadata);
        assert_eq!(
            client
                .post(format!("{base}/v1/ingest"))
                .bearer_auth(token("a", "admin"))
                .json(&request)
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
    }
    assert_eq!(
        client
            .post(format!("{base}/v1/ingest"))
            .bearer_auth(token("a", "admin"))
            .header("content-type", "application/json")
            .body("x".repeat(8 * 1024 * 1024 + 1))
            .send()
            .await
            .unwrap()
            .status(),
        413
    );

    assert_eq!(
        client
            .get(format!("{base}/metrics"))
            .bearer_auth(token("a", "reader"))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        client
            .post(format!("{base}/v1/ingest"))
            .bearer_auth(token("a", "reader"))
            .json(&request)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        client
            .post(format!("{base}/v1/query"))
            .bearer_auth(token("a", "admin"))
            .body("{")
            .header("content-type", "application/json")
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    let sdk = alayasiki_sdk::ClientBuilder::new()
        .connect_remote(&base, &token("a", "admin"))
        .unwrap();
    let result = sdk
        .ingest(IngestionRequest::text(
            "Remote SDK evidence".into(),
            HashMap::new(),
        ))
        .await
        .unwrap();
    assert!(!result.node_ids.is_empty());
    let response = sdk
        .query(query::QueryRequest {
            query: "Remote SDK evidence".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(!response.evidence.nodes.is_empty());
    server.abort();
    let _ = server.await;
    runtime
        .wait_for_jobs(std::time::Duration::from_secs(5))
        .await
        .unwrap();
    Arc::try_unwrap(runtime)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    let reopened = Arc::new(
        RuntimeBuilder::new(dir.path())
            .extraction_enabled(false)
            .start()
            .await
            .unwrap(),
    );
    let config = ServerConfig::new(
        "test-secret-at-least-32-bytes-long".into(),
        "test".into(),
        "api".into(),
    )
    .unwrap();
    let state = ApiState::new(reopened, config, dir.path().join("restart-audit.jsonl")).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let restart = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router(state)).await.unwrap() });
    let result: Value = client
        .post(format!("{restart}/v1/query"))
        .bearer_auth(token("a", "admin"))
        .json(&json!({"query":"Remote SDK evidence"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!result["data"]["evidence"]["nodes"]
        .as_array()
        .unwrap()
        .is_empty());
    server.abort();
}

#[tokio::test]
async fn limits_and_authentication_are_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = ServerConfig::new(
        "test-secret-at-least-32-bytes-long".into(),
        "test".into(),
        "api".into(),
    )
    .unwrap();
    config.requests_per_second = 6;
    config.max_body_bytes = 32;
    let runtime = Arc::new(
        RuntimeBuilder::new(dir.path())
            .extraction_enabled(false)
            .start()
            .await
            .unwrap(),
    );
    let state = ApiState::new(runtime, config, dir.path().join("audit.jsonl")).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router(state)).await.unwrap() });
    let client = reqwest::Client::new();
    let claims = JwtClaims {
        sub: "user".into(),
        tenant: "a".into(),
        roles: vec!["admin".into()],
        scope: None,
        attributes: HashMap::new(),
        iss: Some("test".into()),
        aud: Some("api".into()),
        exp: 1,
        nbf: None,
        iat: None,
    };
    let expired = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(b"test-secret-at-least-32-bytes-long"),
    )
    .unwrap();
    assert_eq!(
        client
            .get(format!("{base}/metrics"))
            .bearer_auth(expired)
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        client
            .post(format!("{base}/v1/query"))
            .bearer_auth(token("a", "admin"))
            .header("content-type", "application/json")
            .body("x".repeat(33))
            .send()
            .await
            .unwrap()
            .status(),
        413
    );
    assert_eq!(
        client
            .get(format!("{base}/v1/graph?limit=0"))
            .bearer_auth(token("a", "admin"))
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    assert_eq!(
        client
            .get(format!("{base}/health"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        client
            .get(format!("{base}/metrics"))
            .bearer_auth(token("a", "reader"))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        client
            .get(format!("{base}/v1/graph"))
            .bearer_auth(token("a", "ingestor"))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    let response = client.get(format!("{base}/health")).send().await.unwrap();
    assert_eq!(response.status(), 429);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["error"]["retryable"], true);
    assert!(std::fs::read_to_string(dir.path().join("audit.jsonl"))
        .unwrap()
        .contains("denied"));
    let records = std::fs::read_to_string(dir.path().join("audit.jsonl")).unwrap();
    assert_eq!(
        records
            .lines()
            .filter(|line| line.contains("test") && line.contains("denied"))
            .count(),
        2
    );
    server.abort();
}

#[test]
fn configuration_rejects_missing_or_weak_credentials() {
    assert!(ServerConfig::new("weak".into(), "issuer".into(), "audience".into()).is_err());
    assert!(ServerConfig::new("x".repeat(32), "".into(), "audience".into()).is_err());
    assert!(ServerConfig::new("x".repeat(32), "issuer".into(), "".into()).is_err());
}

#[tokio::test]
async fn extracted_entities_never_extend_source_retention() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(RuntimeBuilder::new(dir.path()).start().await.unwrap());
    let config = ServerConfig::new(
        "test-secret-at-least-32-bytes-long".into(),
        "test".into(),
        "api".into(),
    )
    .unwrap();
    let state = ApiState::new(runtime.clone(), config, dir.path().join("audit.jsonl")).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router(state)).await.unwrap() });
    let client = reqwest::Client::new();
    let expired = IngestionRequest::text(
        "Private startup retention source".into(),
        HashMap::from([("retention_until_unix".into(), "1".into())]),
    );
    assert_eq!(
        client
            .post(format!("{base}/v1/ingest"))
            .bearer_auth(token("a", "admin"))
            .json(&expired)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    runtime
        .wait_for_jobs(std::time::Duration::from_secs(5))
        .await
        .unwrap();
    let graph: Value = client
        .get(format!("{base}/v1/graph"))
        .bearer_auth(token("a", "admin"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        graph["data"]["nodes"].as_array().unwrap().is_empty(),
        "expired source must not leak derived entity text"
    );
    let query: Value = client
        .post(format!("{base}/v1/query"))
        .bearer_auth(token("a", "admin"))
        .json(&json!({"query":"startup","search_mode":"global"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(query["data"]["evidence"]["nodes"]
        .as_array()
        .unwrap()
        .is_empty());
    for metadata in [
        HashMap::from([("retention_until_unix".into(), u64::MAX.to_string())]),
        HashMap::new(),
    ] {
        let live = IngestionRequest::text(
            format!("Public startup source {}", metadata.len()),
            metadata,
        );
        assert_eq!(
            client
                .post(format!("{base}/v1/ingest"))
                .bearer_auth(token("a", "admin"))
                .json(&live)
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        runtime
            .wait_for_jobs(std::time::Duration::from_secs(5))
            .await
            .unwrap();
        let entity = runtime
            .repository()
            .get_nodes_by_ids(&runtime.repository().list_node_ids().await)
            .await
            .into_iter()
            .find(|node| {
                node.metadata
                    .get("type")
                    .is_some_and(|value| value == "entity")
            })
            .unwrap();
        assert_eq!(
            entity
                .metadata
                .get("retention_until_unix")
                .map(String::as_str),
            Some("1")
        );
    }
    // The reverse ordering must also tighten an initially unbounded entity.
    for metadata in [
        HashMap::new(),
        HashMap::from([("retention_until_unix".into(), "1".into())]),
    ] {
        let request = IngestionRequest::text(
            format!("Second tenant startup source {}", metadata.len()),
            metadata,
        );
        assert_eq!(
            client
                .post(format!("{base}/v1/ingest"))
                .bearer_auth(token("b", "admin"))
                .json(&request)
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        runtime
            .wait_for_jobs(std::time::Duration::from_secs(5))
            .await
            .unwrap();
    }
    let entity = runtime
        .repository()
        .get_nodes_by_ids(&runtime.repository().list_node_ids().await)
        .await
        .into_iter()
        .find(|node| {
            node.metadata
                .get("type")
                .is_some_and(|value| value == "entity")
                && node
                    .metadata
                    .get("tenant")
                    .is_some_and(|value| value == "b")
        })
        .unwrap();
    assert_eq!(
        entity
            .metadata
            .get("retention_until_unix")
            .map(String::as_str),
        Some("1")
    );
    server.abort();
}
