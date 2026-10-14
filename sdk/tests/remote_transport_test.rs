use alayasiki_core::ingest::IngestionRequest;
use alayasiki_sdk::{ClientBuilder, ClientError, RemoteTransport, RetryConfig};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

async fn server(responses: Vec<(u16, String)>, delay: Duration) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    tokio::spawn(async move {
        for (status, body) in responses {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = vec![0; 65536];
            let size = stream.read(&mut bytes).await.unwrap();
            let request = String::from_utf8_lossy(&bytes[..size]);
            assert!(request.starts_with("POST /v1/ingest "));
            assert!(request
                .to_lowercase()
                .contains("authorization: bearer secret-test-token"));
            count.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(delay).await;
            let reply = format!("HTTP/1.1 {status} Reply\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            let _ = stream.write_all(reply.as_bytes()).await;
        }
    });
    (url, calls)
}
fn request(keyed: bool) -> IngestionRequest {
    IngestionRequest::Text {
        content: "Rust".into(),
        metadata: HashMap::new(),
        idempotency_key: keyed.then(|| "key".into()),
        model_id: None,
        embedding_model_id: None,
        extraction_model_id: None,
    }
}
fn retry() -> RetryConfig {
    RetryConfig {
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(1),
        ..RetryConfig::default()
    }
}
fn success() -> String {
    r#"{"success":true,"data":{"node_ids":[42],"snapshot_id":"wal-lsn-1"}}"#.into()
}
fn failure(code: &str, retryable: bool) -> String {
    serde_json::json!({"success":false,"error":{"code":code,"message":"failed","retryable":retryable}}).to_string()
}

#[tokio::test]
async fn keyed_ingest_retries_transient_http_errors() {
    for status in [429, 503] {
        let (url, calls) = server(
            vec![
                (status, failure("RESOURCE_EXHAUSTED", true)),
                (200, success()),
            ],
            Duration::ZERO,
        )
        .await;
        let client = ClientBuilder::new()
            .retry_config(retry())
            .connect_remote(&url, "secret-test-token")
            .unwrap();
        assert_eq!(
            client.ingest(request(true)).await.unwrap().node_ids,
            vec![42]
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
#[tokio::test]
async fn unkeyed_ingest_and_permanent_errors_are_not_retried() {
    for (status, code, keyed) in [
        (503, "INTERNAL", false),
        (401, "UNAUTHENTICATED", true),
        (400, "INVALID_ARGUMENT", true),
    ] {
        let (url, calls) = server(vec![(status, failure(code, true))], Duration::ZERO).await;
        let client = ClientBuilder::new()
            .retry_config(retry())
            .connect_remote(&url, "secret-test-token")
            .unwrap();
        assert!(matches!(
            client.ingest(request(keyed)).await,
            Err(ClientError::Remote { .. })
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn malformed_and_oversized_responses_are_protocol_errors() {
    for body in [
        "not json".to_string(),
        "x".repeat(8 * 1024 * 1024 + 1),
        r#"{"success":false,"data":{"node_ids":[1],"snapshot_id":"wal-lsn-1"}}"#.into(),
    ] {
        let (url, _) = server(vec![(200, body)], Duration::ZERO).await;
        let client = ClientBuilder::new()
            .connect_remote(&url, "secret-test-token")
            .unwrap();
        assert!(matches!(
            client.ingest(request(true)).await,
            Err(ClientError::Protocol(_))
        ));
    }
}
#[tokio::test]
async fn remote_attempt_respects_client_deadline() {
    let (url, _) = server(vec![(200, success())], Duration::from_millis(100)).await;
    let client = ClientBuilder::new()
        .retry_config(RetryConfig {
            deadline: Some(Duration::from_millis(10)),
            ..retry()
        })
        .connect_remote(&url, "secret-test-token")
        .unwrap();
    assert!(matches!(
        client.ingest(request(true)).await,
        Err(ClientError::DeadlineExceeded)
    ));
}
#[test]
fn invalid_remote_endpoints_and_tokens_are_rejected() {
    for url in [
        "file:///tmp/test",
        "http://user:password@localhost",
        "http://localhost/query",
        "http://localhost?secret=yes",
        "http://localhost#secret",
        "invalid",
    ] {
        assert!(RemoteTransport::new(url, "secret-test-token").is_err());
    }
    assert!(RemoteTransport::new("http://localhost", "secret\nheader").is_err());
    assert!(RemoteTransport::new("http://localhost", "").is_err());
    assert!(RemoteTransport::new("http://localhost", "invalid token").is_err());
}

#[tokio::test]
async fn remote_configuration_conflicts_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let repo = Arc::new(
        storage::repo::Repository::open(dir.path().join("graph.wal"))
            .await
            .unwrap(),
    );
    assert!(ClientBuilder::new()
        .with_repo(repo)
        .connect_remote("http://localhost", "token")
        .is_err());
    assert!(ClientBuilder::new()
        .app_config(serde_json::from_value(serde_json::json!({"server":{"host":"127.0.0.1","port":8080},"storage":{"data_dir":"unused","wal_flush_interval_ms":100}})).unwrap())
        .connect_remote("http://localhost", "token")
        .is_err());
}
#[tokio::test]
async fn redirects_do_not_forward_credentials() {
    let destination = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let redirect = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", redirect.local_addr().unwrap());
    let location = format!("http://{}/stolen", destination.local_addr().unwrap());
    tokio::spawn(async move {
        let (mut socket, _) = redirect.accept().await.unwrap();
        let mut buf = [0; 65536];
        let received = socket.read(&mut buf).await.unwrap();
        assert!(received > 0);
        let response = format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        socket.write_all(response.as_bytes()).await.unwrap();
    });
    let client = ClientBuilder::new()
        .connect_remote(&url, "secret-test-token")
        .unwrap();
    assert!(matches!(
        client.ingest(request(true)).await,
        Err(ClientError::Protocol(_))
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), destination.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn query_retries_and_preserves_response() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        for status in [503, 200] {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = [0; 65536];
            let len = stream.read(&mut bytes).await.unwrap();
            assert!(String::from_utf8_lossy(&bytes[..len]).starts_with("POST /v1/query "));
            let body = if status == 503 {
                failure("INTERNAL", true)
            } else {
                serde_json::json!({"success":true,"data": {
                    "answer":"Rust", "evidence":{"nodes":[],"edges":[]}, "citations":[], "groundedness":1.0,
                    "explain":{"steps":["community_map_reduce"],"effective_search_mode":"global","anchors":[],"expansion_paths":[],"exclusions":[]},
                    "model_id":"test", "snapshot_id":"wal-lsn-1", "latency_ms":1
                }}).to_string()
            };
            let response = format!(
                "HTTP/1.1 {status} Reply\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });
    let client = ClientBuilder::new()
        .retry_config(retry())
        .connect_remote(&url, "secret-test-token")
        .unwrap();
    let query =
        query::QueryRequest::parse_json(r#"{"query":"Rust","search_mode":"global"}"#).unwrap();
    assert_eq!(
        client.query(query).await.unwrap().answer.as_deref(),
        Some("Rust")
    );
}
#[tokio::test]
async fn network_failure_is_sanitized_and_retryable() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let client = ClientBuilder::new()
        .retry_config(RetryConfig {
            max_attempts: 1,
            ..retry()
        })
        .connect_remote(&url, "secret-test-token")
        .unwrap();
    let error = client.ingest(request(true)).await.unwrap_err();
    assert!(matches!(error, ClientError::Network));
    assert!(error.is_retryable());
    assert!(!format!("{error:?}").contains("secret-test-token"));
}
