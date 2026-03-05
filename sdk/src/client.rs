use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use alayasiki_core::ingest::IngestionRequest;
use async_trait::async_trait;
use ingestion::processor::{IngestionError, IngestionPipeline};
use query::{QueryEngine, QueryError, QueryRequest, QueryResponse};
use storage::repo::{RepoError, Repository};
use storage::wal::WalError;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
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
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(25),
            max_backoff: Duration::from_millis(250),
            backoff_multiplier: 2.0,
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

    fn backoff_for_retry(&self, retry_number: usize) -> Duration {
        let retry_number = retry_number.max(1) as i32;
        let base_ms = self.initial_backoff.as_secs_f64() * 1_000.0;
        let max_ms = self.max_backoff.as_secs_f64() * 1_000.0;
        let delay_ms = (base_ms * self.backoff_multiplier.powi(retry_number - 1)).min(max_ms);
        Duration::from_secs_f64((delay_ms / 1_000.0).max(0.0))
    }
}

#[derive(Debug, Error)]
pub enum ClientBuildError {
    #[error("repository error: {0}")]
    Repository(#[from] RepoError),
    #[error("missing repository: call with_repo() or connect_in_process()")]
    MissingRepository,
    #[error("conflicting builder configuration: with_repo() cannot be combined with connect_in_process()")]
    ConflictingRepositoryAndWalPath,
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("ingestion failed: {0}")]
    Ingestion(#[from] IngestionError),
    #[error("query failed: {0}")]
    Query(#[from] QueryError),
}

impl ClientError {
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            ClientError::Ingestion(IngestionError::Storage(RepoError::Wal(WalError::Io(_))))
                | ClientError::Query(QueryError::Repository(RepoError::Wal(WalError::Io(_))))
        )
    }
}

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

    pub async fn ingest(&self, request: IngestionRequest) -> Result<IngestResult, ClientError> {
        let transport = self.transport.clone();
        self.execute_with_retry(move || {
            let transport = transport.clone();
            let request = request.clone();
            async move { transport.ingest(request).await }
        })
        .await
    }

    pub async fn query(&self, request: QueryRequest) -> Result<QueryResponse, ClientError> {
        let transport = self.transport.clone();
        self.execute_with_retry(move || {
            let transport = transport.clone();
            let request = request.clone();
            async move { transport.query(request).await }
        })
        .await
    }

    async fn execute_with_retry<T, Op, Fut>(&self, mut op: Op) -> Result<T, ClientError>
    where
        Op: FnMut() -> Fut,
        Fut: Future<Output = Result<T, ClientError>>,
    {
        let mut attempts = 0usize;
        loop {
            attempts += 1;
            match op().await {
                Ok(value) => return Ok(value),
                Err(err) => {
                    let should_retry =
                        err.is_retryable() && attempts < self.retry_config.max_attempts;
                    if !should_retry {
                        return Err(err);
                    }

                    let retry_number = attempts;
                    let delay = self.retry_config.backoff_for_retry(retry_number);
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
}

impl ClientBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn retry_config(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = retry_config;
        self
    }

    pub fn with_repo(mut self, repo: Arc<Repository>) -> Self {
        self.repo = Some(repo);
        self
    }

    pub fn build(self) -> Result<Client, ClientBuildError> {
        let repo = self.repo.ok_or(ClientBuildError::MissingRepository)?;
        let transport: Arc<dyn SdkTransport> = Arc::new(InProcessTransport::from_repo(repo));
        Ok(Client::new(transport, self.retry_config))
    }

    pub async fn connect_in_process(
        self,
        wal_path: impl AsRef<Path>,
    ) -> Result<Client, ClientBuildError> {
        if self.repo.is_some() {
            return Err(ClientBuildError::ConflictingRepositoryAndWalPath);
        }
        let transport = InProcessTransport::connect(wal_path).await?;
        Ok(Client::new(Arc::new(transport), self.retry_config))
    }
}
