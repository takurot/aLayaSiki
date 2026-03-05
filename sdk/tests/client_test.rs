use std::collections::HashMap;
use std::io;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use alayasiki_core::ingest::IngestionRequest;
use alayasiki_sdk::client::{
    Client, ClientBuildError, ClientBuilder, ClientError, IngestResult, RetryConfig, SdkTransport,
};
use async_trait::async_trait;
use ingestion::processor::IngestionError;
use query::engine::{Anchor, Citation, EvidenceSubgraph, ExplainPlan};
use query::{QueryError, QueryRequest, QueryResponse, SearchMode};
use storage::repo::{RepoError, Repository};
use storage::wal::WalError;
use tempfile::tempdir;
use tokio::sync::Mutex;

#[tokio::test]
async fn test_client_ingest_and_query_roundtrip() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("sdk_client_roundtrip.wal");

    let client = ClientBuilder::default()
        .connect_in_process(&wal_path)
        .await
        .unwrap();

    let mut metadata = HashMap::new();
    metadata.insert("source".to_string(), "sdk/test.md".to_string());
    metadata.insert("entity_type".to_string(), "Company".to_string());
    metadata.insert("timestamp".to_string(), "2025-02-01".to_string());

    let ingest = client
        .ingest(IngestionRequest::Text {
            content: "Tesla expands battery production in 2025".to_string(),
            metadata,
            idempotency_key: Some("sdk-roundtrip-1".to_string()),
            model_id: Some("embedding-default-v1".to_string()),
        })
        .await
        .unwrap();

    assert!(!ingest.node_ids.is_empty());
    assert!(ingest.snapshot_id.starts_with("wal-lsn-"));

    let request = QueryRequest::parse_json(
        r#"{
            "query": "Tesla battery production",
            "mode": "evidence",
            "search_mode": "local",
            "top_k": 5,
            "model_id": "embedding-default-v1"
        }"#,
    )
    .unwrap();

    let response = client.query(request).await.unwrap();
    assert!(!response.evidence.nodes.is_empty());
    assert!(response
        .evidence
        .nodes
        .iter()
        .any(|node| node.data.contains("battery production")));
}

#[tokio::test]
async fn test_client_retries_retryable_ingest_error() {
    let transport = Arc::new(FlakyIngestTransport::new(2));
    let client = Client::new(
        transport.clone(),
        RetryConfig {
            max_attempts: 4,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            backoff_multiplier: 2.0,
        },
    );

    let result = client
        .ingest(IngestionRequest::text(
            "retry me".to_string(),
            HashMap::new(),
        ))
        .await
        .unwrap();

    assert_eq!(result.node_ids, vec![42]);
    assert_eq!(result.snapshot_id, "wal-lsn-99");
    assert_eq!(transport.attempts(), 3);
}

#[tokio::test]
async fn test_client_does_not_retry_non_retryable_ingest_error() {
    let transport = Arc::new(NonRetryableIngestTransport::default());
    let client = Client::new(
        transport.clone(),
        RetryConfig {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            backoff_multiplier: 2.0,
        },
    );

    let err = client
        .ingest(IngestionRequest::text(
            "no retry".to_string(),
            HashMap::new(),
        ))
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        ClientError::Ingestion(IngestionError::UnsupportedType(_))
    ));
    assert_eq!(transport.attempts(), 1);
}

#[tokio::test]
async fn test_client_retries_retryable_query_error() {
    let transport = Arc::new(FlakyQueryTransport::new(2));
    let client = Client::new(
        transport.clone(),
        RetryConfig {
            max_attempts: 4,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            backoff_multiplier: 2.0,
        },
    );

    let response = client
        .query(
            QueryRequest::parse_json(
                r#"{
                "query": "retry query path",
                "mode": "evidence",
                "search_mode": "local"
            }"#,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.answer.as_deref(), Some("ok"));
    assert_eq!(transport.attempts(), 3);
}

struct FlakyIngestTransport {
    failures_remaining: Mutex<usize>,
    attempts: AtomicUsize,
}

impl FlakyIngestTransport {
    fn new(failures_before_success: usize) -> Self {
        Self {
            failures_remaining: Mutex::new(failures_before_success),
            attempts: AtomicUsize::new(0),
        }
    }

    fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl SdkTransport for FlakyIngestTransport {
    async fn ingest(&self, _request: IngestionRequest) -> Result<IngestResult, ClientError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);

        let mut remaining = self.failures_remaining.lock().await;
        if *remaining > 0 {
            *remaining -= 1;
            return Err(retryable_ingest_error());
        }

        Ok(IngestResult {
            node_ids: vec![42],
            snapshot_id: "wal-lsn-99".to_string(),
        })
    }

    async fn query(&self, _request: QueryRequest) -> Result<QueryResponse, ClientError> {
        Err(ClientError::Query(QueryError::InvalidQuery(
            "query not supported in this mock".to_string(),
        )))
    }
}

#[derive(Default)]
struct NonRetryableIngestTransport {
    attempts: AtomicUsize,
}

impl NonRetryableIngestTransport {
    fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl SdkTransport for NonRetryableIngestTransport {
    async fn ingest(&self, _request: IngestionRequest) -> Result<IngestResult, ClientError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Err(ClientError::Ingestion(IngestionError::UnsupportedType(
            "application/x-binary".to_string(),
        )))
    }

    async fn query(&self, _request: QueryRequest) -> Result<QueryResponse, ClientError> {
        Err(ClientError::Query(QueryError::InvalidQuery(
            "query not supported in this mock".to_string(),
        )))
    }
}

struct FlakyQueryTransport {
    failures_remaining: Mutex<usize>,
    attempts: AtomicUsize,
}

impl FlakyQueryTransport {
    fn new(failures_before_success: usize) -> Self {
        Self {
            failures_remaining: Mutex::new(failures_before_success),
            attempts: AtomicUsize::new(0),
        }
    }

    fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl SdkTransport for FlakyQueryTransport {
    async fn ingest(&self, _request: IngestionRequest) -> Result<IngestResult, ClientError> {
        Err(ClientError::Ingestion(IngestionError::UnsupportedType(
            "ingest not supported in this mock".to_string(),
        )))
    }

    async fn query(&self, _request: QueryRequest) -> Result<QueryResponse, ClientError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);

        let mut remaining = self.failures_remaining.lock().await;
        if *remaining > 0 {
            *remaining -= 1;
            let io_err = io::Error::new(io::ErrorKind::WouldBlock, "transient wal failure");
            return Err(ClientError::Query(QueryError::Repository(RepoError::Wal(
                WalError::Io(io_err),
            ))));
        }

        Ok(QueryResponse {
            answer: Some("ok".to_string()),
            evidence: EvidenceSubgraph {
                nodes: vec![],
                edges: vec![],
            },
            citations: Vec::<Citation>::new(),
            groundedness: 0.0,
            explain: ExplainPlan {
                steps: vec!["mock".to_string()],
                effective_search_mode: SearchMode::Local,
                anchors: Vec::<Anchor>::new(),
                expansion_paths: vec![],
                exclusions: vec![],
            },
            model_id: Some("embedding-default-v1".to_string()),
            snapshot_id: Some("wal-lsn-1".to_string()),
            time_travel: None,
            latency_ms: 0,
            error_code: None,
        })
    }
}

fn retryable_ingest_error() -> ClientError {
    let io_err = io::Error::new(io::ErrorKind::WouldBlock, "transient wal failure");
    ClientError::Ingestion(IngestionError::Storage(RepoError::Wal(WalError::Io(
        io_err,
    ))))
}

#[tokio::test]
async fn test_client_builder_from_repo_works() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("sdk_client_from_repo.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    let client = ClientBuilder::default().with_repo(repo).build().unwrap();
    let response = client
        .query(
            QueryRequest::parse_json(
                r#"{
                    "query": "no data yet",
                    "mode": "evidence",
                    "search_mode": "local"
                }"#,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.evidence.nodes.is_empty());
}

#[tokio::test]
async fn test_client_builder_rejects_conflicting_repo_and_wal_path() {
    let dir = tempdir().unwrap();
    let repo_wal = dir.path().join("sdk_repo.wal");
    let connect_wal = dir.path().join("sdk_connect.wal");
    let repo = Arc::new(Repository::open(&repo_wal).await.unwrap());

    let err = ClientBuilder::default()
        .with_repo(repo)
        .connect_in_process(&connect_wal)
        .await;

    assert!(matches!(
        err,
        Err(ClientBuildError::ConflictingRepositoryAndWalPath)
    ));
}
