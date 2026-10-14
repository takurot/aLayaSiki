use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alayasiki_core::ingest::IngestionRequest;
use async_trait::async_trait;
use ingestion::processor::{IngestionError, IngestionPipeline};
use query::{QueryEngine, QueryError, QueryRequest, QueryResponse};
use storage::repo::{RepoError, Repository};
use storage::snapshot::SnapshotError;
use storage::wal::WalError;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IngestResult {
    pub node_ids: Vec<u64>,
    pub snapshot_id: String,
}

#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_attempts: usize,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub backoff_multiplier: f64,
    /// Overall wall-clock budget for a single operation's retry loop,
    /// including all attempts and backoff delays. Each in-flight attempt is
    /// bounded by the remaining budget via `tokio::time::timeout`, so a
    /// single hung attempt cannot stall the caller past the deadline.
    /// `None` means unbounded.
    pub deadline: Option<Duration>,
    /// By default, ingestion requests are only retried when they carry an
    /// `idempotency_key`, since retrying a request whose WAL append already
    /// succeeded (but whose ack was lost) would otherwise produce duplicate
    /// inserts. Set this to `true` to opt into retrying ingestion requests
    /// that have no idempotency key.
    pub retry_non_idempotent_ingestion: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(25),
            max_backoff: Duration::from_millis(250),
            backoff_multiplier: 2.0,
            deadline: None,
            retry_non_idempotent_ingestion: false,
        }
    }
}

impl RetryConfig {
    fn normalized(mut self) -> Self {
        if self.max_attempts == 0 {
            self.max_attempts = 1;
        }
        if self.initial_backoff > self.max_backoff {
            self.initial_backoff = self.max_backoff;
        }
        if self.backoff_multiplier < 1.0 {
            self.backoff_multiplier = 1.0;
        }
        self
    }

    /// Computes the backoff delay for the given retry attempt using
    /// exponential backoff with equal jitter: the result is sampled
    /// uniformly from `[computed_delay / 2, computed_delay]` to avoid
    /// thundering-herd retries across clients hitting the same transient
    /// error, while keeping a floor so retries against a momentarily
    /// contended resource (e.g. an idempotency lock) don't hot-spin at a
    /// near-zero delay.
    fn backoff_for_retry(&self, retry_number: usize) -> Duration {
        let retry_number = retry_number.max(1) as i32;
        let base_ms = self.initial_backoff.as_secs_f64() * 1_000.0;
        let max_ms = self.max_backoff.as_secs_f64() * 1_000.0;
        let delay_ms = (base_ms * self.backoff_multiplier.powi(retry_number - 1)).min(max_ms);
        let delay_ms = delay_ms.max(0.0);
        let half_ms = delay_ms / 2.0;
        let jittered_ms = half_ms + fastrand::f64() * half_ms;
        Duration::from_secs_f64(jittered_ms / 1_000.0)
    }
}

#[derive(Debug, Error)]
pub enum ClientBuildError {
    #[error("repository error: {0}")]
    Repository(#[from] RepoError),
    #[error("missing repository: call with_repo() or connect_in_process()")]
    MissingRepository,
    #[error("invalid remote endpoint or bearer token configuration")]
    InvalidRemoteConfiguration,
    #[error("remote connection cannot be combined with repository or application configuration")]
    ConflictingRemoteConfiguration,
    #[error("conflicting builder configuration: with_repo() cannot be combined with connect_in_process()")]
    ConflictingRepositoryAndWalPath,
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("ingestion failed: {0}")]
    Ingestion(#[from] IngestionError),
    #[error("query failed: {0}")]
    Query(#[from] QueryError),
    /// The retry loop's overall `RetryConfig::deadline` elapsed while an
    /// attempt was still in flight. The underlying attempt keeps running on
    /// its own spawned task (so the WAL/repository write is not torn down),
    /// but this caller can no longer observe its outcome.
    #[error("operation exceeded its retry deadline")]
    DeadlineExceeded,
    /// A retry attempt's task was cancelled (e.g. the Tokio runtime shut
    /// down) before it could report an outcome.
    #[error("retry attempt task was cancelled before completing")]
    AttemptCancelled,
    #[error("remote network request failed")]
    Network,
    #[error("remote API protocol error: {0}")]
    Protocol(String),
    #[error("remote request failed ({code}): {message}")]
    Remote {
        code: alayasiki_core::error::ErrorCode,
        message: String,
        retryable: bool,
    },
}

impl ClientError {
    pub fn is_retryable(&self) -> bool {
        match self {
            ClientError::Ingestion(IngestionError::Storage(repo_err))
            | ClientError::Query(QueryError::Repository(repo_err)) => {
                is_retryable_repo_error(repo_err)
            }
            ClientError::Network => true,
            ClientError::Remote { retryable, .. } => *retryable,
            // Idempotency-guarded ingestion that found a key already being
            // processed is a transient lock-contention condition, not a
            // permanent failure.
            ClientError::Ingestion(IngestionError::IdempotencyConflict(_)) => true,
            _ => false,
        }
    }
}

fn is_retryable_repo_error(err: &RepoError) -> bool {
    matches!(
        err,
        RepoError::Wal(WalError::Io(_)) | RepoError::Snapshot(SnapshotError::Io(_))
    )
}

#[allow(clippy::double_must_use)]
#[async_trait]
pub trait SdkTransport: Send + Sync {
    async fn ingest(&self, request: IngestionRequest) -> Result<IngestResult, ClientError>;
    async fn query(&self, request: QueryRequest) -> Result<QueryResponse, ClientError>;
}

pub struct InProcessTransport {
    repo: Arc<Repository>,
    ingestion_pipeline: IngestionPipeline,
    query_engine: QueryEngine,
}

impl InProcessTransport {
    pub async fn connect(wal_path: impl AsRef<Path>) -> Result<Self, RepoError> {
        let repo = Arc::new(Repository::open(wal_path).await?);
        Ok(Self::from_repo(repo))
    }

    pub fn from_repo(repo: Arc<Repository>) -> Self {
        Self {
            ingestion_pipeline: IngestionPipeline::new(repo.clone()),
            query_engine: QueryEngine::new(repo.clone()),
            repo,
        }
    }

    pub fn from_repo_with_config(
        repo: Arc<Repository>,
        config: &alayasiki_core::config::AppConfig,
    ) -> Self {
        Self {
            ingestion_pipeline: IngestionPipeline::with_config(repo.clone(), config),
            query_engine: QueryEngine::new(repo.clone()),
            repo,
        }
    }

    pub fn repository(&self) -> Arc<Repository> {
        self.repo.clone()
    }
}

#[async_trait]
impl SdkTransport for InProcessTransport {
    async fn ingest(&self, request: IngestionRequest) -> Result<IngestResult, ClientError> {
        let node_ids = self.ingestion_pipeline.ingest(request).await?;
        let snapshot_id = self.repo.current_snapshot_id().await;
        Ok(IngestResult {
            node_ids,
            snapshot_id,
        })
    }

    async fn query(&self, request: QueryRequest) -> Result<QueryResponse, ClientError> {
        let response = self.query_engine.execute(request).await?;
        Ok(response)
    }
}

pub struct Client {
    transport: Arc<dyn SdkTransport>,
    retry_config: RetryConfig,
}

impl Client {
    pub fn new(transport: Arc<dyn SdkTransport>, retry_config: RetryConfig) -> Self {
        Self {
            transport,
            retry_config: retry_config.normalized(),
        }
    }

    pub fn retry_config(&self) -> &RetryConfig {
        &self.retry_config
    }

    /// Note on cancellation: each retry attempt runs on its own spawned
    /// task so dropping the future returned by this method (e.g. via an
    /// external timeout) does not abort an in-flight WAL write. However,
    /// if the caller stops awaiting, or `RetryConfig::deadline` elapses
    /// mid-attempt (surfaced as `ClientError::DeadlineExceeded`), the
    /// spawned attempt's eventual `Ok`/`Err` outcome is discarded -- there
    /// is currently no channel for observing it after the fact.
    pub async fn ingest(&self, request: IngestionRequest) -> Result<IngestResult, ClientError> {
        // Retrying ingestion is only safe when the request is idempotent:
        // if the first attempt's WAL append succeeded but its ack was lost
        // (e.g. due to a transient IO error on the response path), a retry
        // without an idempotency key would re-insert the same content.
        let idempotent =
            request.idempotency_key().is_some() || self.retry_config.retry_non_idempotent_ingestion;
        let transport = self.transport.clone();
        self.execute_with_retry(idempotent, move || {
            let transport = transport.clone();
            let request = request.clone();
            async move { transport.ingest(request).await }
        })
        .await
    }

    pub async fn query(&self, request: QueryRequest) -> Result<QueryResponse, ClientError> {
        // Queries are read-only and therefore always safe to retry.
        let transport = self.transport.clone();
        self.execute_with_retry(true, move || {
            let transport = transport.clone();
            let request = request.clone();
            async move { transport.query(request).await }
        })
        .await
    }

    async fn execute_with_retry<T, Op, Fut>(
        &self,
        retries_allowed: bool,
        mut op: Op,
    ) -> Result<T, ClientError>
    where
        Op: FnMut() -> Fut,
        Fut: Future<Output = Result<T, ClientError>> + Send + 'static,
        T: Send + 'static,
    {
        let start = Instant::now();
        let mut attempts = 0usize;
        loop {
            attempts += 1;
            // Spawn each attempt onto its own task so that if the caller
            // drops this future (e.g. via timeout or cancellation) while an
            // attempt is in flight, the underlying operation still runs to
            // completion instead of being torn down mid-write. This avoids
            // leaving the WAL/repository in a state that a subsequent retry
            // cannot safely reason about.
            let handle = tokio::spawn(op());

            // Bound this attempt by whatever remains of the overall
            // deadline so a single hung attempt cannot stall the caller
            // indefinitely. The spawned task itself is not aborted on
            // timeout (that would reintroduce the mid-write teardown this
            // spawn is meant to avoid) -- it keeps running in the
            // background, but its outcome becomes unobservable to this
            // caller.
            let remaining = self
                .retry_config
                .deadline
                .map(|deadline| deadline.saturating_sub(start.elapsed()));
            let join_result = match remaining {
                Some(remaining) => match tokio::time::timeout(remaining, handle).await {
                    Ok(join_result) => join_result,
                    Err(_elapsed) => return Err(ClientError::DeadlineExceeded),
                },
                None => handle.await,
            };

            let result = match join_result {
                Ok(result) => result,
                Err(join_err) if join_err.is_panic() => {
                    std::panic::resume_unwind(join_err.into_panic())
                }
                Err(_cancelled) => return Err(ClientError::AttemptCancelled),
            };
            match result {
                Ok(value) => return Ok(value),
                Err(err) => {
                    let should_retry = retries_allowed
                        && err.is_retryable()
                        && attempts < self.retry_config.max_attempts;
                    if !should_retry {
                        return Err(err);
                    }

                    let retry_number = attempts;
                    let delay = self.retry_config.backoff_for_retry(retry_number);
                    if let Some(deadline) = self.retry_config.deadline {
                        if start.elapsed() + delay >= deadline {
                            return Err(err);
                        }
                    }
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }
}

#[derive(Default)]
pub struct ClientBuilder {
    retry_config: RetryConfig,
    repo: Option<Arc<Repository>>,
    app_config: Option<alayasiki_core::config::AppConfig>,
}

impl ClientBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn retry_config(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = retry_config;
        self
    }

    /// Apply runtime chunking settings; storage is still selected by the repo/WAL path.
    pub fn app_config(self, config: alayasiki_core::config::AppConfig) -> Self {
        Self {
            app_config: Some(config),
            ..self
        }
    }

    pub fn with_repo(mut self, repo: Arc<Repository>) -> Self {
        self.repo = Some(repo);
        self
    }

    pub fn build(self) -> Result<Client, ClientBuildError> {
        let repo = self.repo.ok_or(ClientBuildError::MissingRepository)?;
        let transport: Arc<dyn SdkTransport> = Arc::new(match self.app_config {
            Some(config) => InProcessTransport::from_repo_with_config(repo, &config),
            None => InProcessTransport::from_repo(repo),
        });
        Ok(Client::new(transport, self.retry_config))
    }

    /// Connect over HTTP. Retry and deadline policy is shared with in-process clients.
    pub fn connect_remote(
        self,
        base_url: &str,
        bearer_token: &str,
    ) -> Result<Client, ClientBuildError> {
        if self.repo.is_some() || self.app_config.is_some() {
            return Err(ClientBuildError::ConflictingRemoteConfiguration);
        }
        let transport = crate::RemoteTransport::new(base_url, bearer_token)?;
        Ok(Client::new(Arc::new(transport), self.retry_config))
    }

    pub async fn connect_in_process(
        self,
        wal_path: impl AsRef<Path>,
    ) -> Result<Client, ClientBuildError> {
        if self.repo.is_some() {
            return Err(ClientBuildError::ConflictingRepositoryAndWalPath);
        }
        let transport = match self.app_config {
            Some(config) => InProcessTransport::from_repo_with_config(
                Arc::new(Repository::open(wal_path).await?),
                &config,
            ),
            None => InProcessTransport::connect(wal_path).await?,
        };
        Ok(Client::new(Arc::new(transport), self.retry_config))
    }
}
