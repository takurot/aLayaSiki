use std::collections::HashMap;
use std::io;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use alayasiki_core::ingest::IngestionRequest;
use alayasiki_sdk::client::{
    Client, ClientBuildError, ClientBuilder, ClientError, IngestResult, RetryConfig, SdkTransport,
};
use async_trait::async_trait;
use ingestion::processor::IngestionError;
use query::engine::{Anchor, Citation, EvidenceSubgraph, ExplainPlan};
use query::{QueryError, QueryRequest, QueryResponse, SearchMode};
use storage::repo::{RepoError, Repository};
use storage::snapshot::SnapshotError;
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
            embedding_model_id: None,
            extraction_model_id: None,
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
            deadline: None,
            retry_non_idempotent_ingestion: false,
        },
    );

    let result = client
        .ingest(IngestionRequest::Text {
            content: "retry me".to_string(),
            metadata: HashMap::new(),
            idempotency_key: Some("retry-me-key".to_string()),
            model_id: None,
            embedding_model_id: None,
            extraction_model_id: None,
        })
        .await
        .unwrap();

    assert_eq!(result.node_ids, vec![42]);
    assert_eq!(result.snapshot_id, "wal-lsn-99");
    assert_eq!(transport.attempts(), 3);
}

#[tokio::test]
async fn test_client_does_not_retry_ingest_without_idempotency_key() {
    let transport = Arc::new(FlakyIngestTransport::new(2));
    let client = Client::new(
        transport.clone(),
        RetryConfig {
            max_attempts: 4,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            backoff_multiplier: 2.0,
            deadline: None,
            retry_non_idempotent_ingestion: false,
        },
    );

    let err = client
        .ingest(IngestionRequest::text(
            "retry me".to_string(),
            HashMap::new(),
        ))
        .await
        .unwrap_err();

    assert!(matches!(err, ClientError::Ingestion(_)));
    assert_eq!(
        transport.attempts(),
        1,
        "non-idempotent ingest requests must not be retried"
    );
}

#[tokio::test]
async fn test_client_retries_non_idempotent_ingest_when_opted_in() {
    let transport = Arc::new(FlakyIngestTransport::new(2));
    let client = Client::new(
        transport.clone(),
        RetryConfig {
            max_attempts: 4,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            backoff_multiplier: 2.0,
            deadline: None,
            retry_non_idempotent_ingestion: true,
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
    assert_eq!(transport.attempts(), 3);
}

#[tokio::test]
async fn test_client_respects_deadline_and_stops_retrying() {
    let transport = Arc::new(FlakyIngestTransport::new(usize::MAX - 1));
    let deadline = Duration::from_millis(60);
    let client = Client::new(
        transport.clone(),
        RetryConfig {
            max_attempts: usize::MAX,
            initial_backoff: Duration::from_millis(20),
            max_backoff: Duration::from_millis(20),
            backoff_multiplier: 1.0,
            deadline: Some(deadline),
            retry_non_idempotent_ingestion: true,
        },
    );

    let start = Instant::now();
    let err = client
        .ingest(IngestionRequest::text(
            "retry me".to_string(),
            HashMap::new(),
        ))
        .await
        .unwrap_err();
    let elapsed = start.elapsed();

    assert!(matches!(
        err,
        ClientError::Ingestion(_) | ClientError::DeadlineExceeded
    ));
    // Bound deterministically on wall-clock time rather than attempt count:
    // each attempt resolves near-instantly in this mock, so total elapsed
    // time is bounded above by `deadline + max_backoff` (the largest single
    // delay that can be taken once the deadline check passes), independent
    // of how the random jitter happens to land on any given run.
    assert!(
        elapsed < deadline + Duration::from_millis(20) + Duration::from_millis(100),
        "deadline should have stopped retries deterministically, got {:?}",
        elapsed
    );
}

#[tokio::test]
async fn test_client_deadline_bounds_a_hung_in_flight_attempt() {
    let transport = Arc::new(HangingIngestTransport::new(Duration::from_secs(5)));
    let deadline = Duration::from_millis(50);
    let client = Client::new(
        transport.clone(),
        RetryConfig {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(5),
            backoff_multiplier: 1.0,
            deadline: Some(deadline),
            retry_non_idempotent_ingestion: true,
        },
    );

    let start = Instant::now();
    let err = client
        .ingest(IngestionRequest::text(
            "retry me".to_string(),
            HashMap::new(),
        ))
        .await
        .unwrap_err();
    let elapsed = start.elapsed();

    assert!(
        matches!(err, ClientError::DeadlineExceeded),
        "a single hung attempt must be bounded by the deadline, got {err:?}"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "a hung attempt should not stall the caller well past the deadline, got {:?}",
        elapsed
    );
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
            deadline: None,
            retry_non_idempotent_ingestion: false,
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
            deadline: None,
            retry_non_idempotent_ingestion: false,
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

#[tokio::test]
async fn test_dropping_ingest_future_does_not_abort_in_flight_attempt() {
    let transport = Arc::new(SlowIngestTransport::new(Duration::from_millis(80)));
    let client = Client::new(transport.clone(), RetryConfig::default());

    let fut = client.ingest(IngestionRequest::text(
        "keep going".to_string(),
        HashMap::new(),
    ));
    // Drop the caller-facing future almost immediately, well before the
    // transport call has had a chance to complete, to simulate the caller
    // cancelling (e.g. via an external timeout).
    let dropped_early = tokio::time::timeout(Duration::from_millis(1), fut)
        .await
        .is_err();
    assert!(
        dropped_early,
        "test setup expects the outer future to still be pending at drop time"
    );

    assert!(
        !transport.completed(),
        "attempt should not have finished yet at drop time"
    );

    // Give the spawned attempt time to run to completion independently of
    // the dropped caller future.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        transport.completed(),
        "spawned attempt should keep running to completion even after the caller's future is dropped"
    );
}

#[tokio::test]
async fn test_snapshot_io_error_is_retryable_and_retried() {
    let transport = Arc::new(FlakySnapshotIoQueryTransport::new(2));
    let client = Client::new(
        transport.clone(),
        RetryConfig {
            max_attempts: 4,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            backoff_multiplier: 2.0,
            deadline: None,
            retry_non_idempotent_ingestion: false,
        },
    );

    let response = client
        .query(
            QueryRequest::parse_json(
                r#"{
                "query": "retry on snapshot io",
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

#[tokio::test]
async fn test_idempotency_conflict_is_retryable_and_retried() {
    let transport = Arc::new(FlakyIdempotencyConflictIngestTransport::new(2));
    let client = Client::new(
        transport.clone(),
        RetryConfig {
            max_attempts: 4,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            backoff_multiplier: 2.0,
            deadline: None,
            retry_non_idempotent_ingestion: false,
        },
    );

    let result = client
        .ingest(IngestionRequest::Text {
            content: "retry on idempotency conflict".to_string(),
            metadata: HashMap::new(),
            idempotency_key: Some("conflict-key".to_string()),
            model_id: None,
            embedding_model_id: None,
            extraction_model_id: None,
        })
        .await
        .unwrap();

    assert_eq!(result.node_ids, vec![42]);
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

struct HangingIngestTransport {
    hang_for: Duration,
}

impl HangingIngestTransport {
    fn new(hang_for: Duration) -> Self {
        Self { hang_for }
    }
}

#[async_trait]
impl SdkTransport for HangingIngestTransport {
    async fn ingest(&self, _request: IngestionRequest) -> Result<IngestResult, ClientError> {
        tokio::time::sleep(self.hang_for).await;
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

struct SlowIngestTransport {
    delay: Duration,
    completed: AtomicBool,
}

impl SlowIngestTransport {
    fn new(delay: Duration) -> Self {
        Self {
            delay,
            completed: AtomicBool::new(false),
        }
    }

    fn completed(&self) -> bool {
        self.completed.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl SdkTransport for SlowIngestTransport {
    async fn ingest(&self, _request: IngestionRequest) -> Result<IngestResult, ClientError> {
        tokio::time::sleep(self.delay).await;
        self.completed.store(true, Ordering::SeqCst);
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

struct FlakySnapshotIoQueryTransport {
    failures_remaining: Mutex<usize>,
    attempts: AtomicUsize,
}

impl FlakySnapshotIoQueryTransport {
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
impl SdkTransport for FlakySnapshotIoQueryTransport {
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
            let io_err = io::Error::new(io::ErrorKind::WouldBlock, "transient snapshot io error");
            return Err(ClientError::Query(QueryError::Repository(
                RepoError::Snapshot(SnapshotError::Io(io_err)),
            )));
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

struct FlakyIdempotencyConflictIngestTransport {
    failures_remaining: Mutex<usize>,
    attempts: AtomicUsize,
}

impl FlakyIdempotencyConflictIngestTransport {
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
impl SdkTransport for FlakyIdempotencyConflictIngestTransport {
    async fn ingest(&self, _request: IngestionRequest) -> Result<IngestResult, ClientError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);

        let mut remaining = self.failures_remaining.lock().await;
        if *remaining > 0 {
            *remaining -= 1;
            return Err(ClientError::Ingestion(IngestionError::IdempotencyConflict(
                "conflict-key".to_string(),
            )));
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
