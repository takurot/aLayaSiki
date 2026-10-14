mod community;
use query::engine::CommunitySummaryProvider;
// Standard autonomous composition. Low-level ingestion constructors remain task-free.
use ingestion::processor::IngestionPipeline;
use jobs::durable::{
    DeadLetterEntry, DurableJobQueue, DurableQueueConfig, JobQueueError, JobQueueStats,
};
use jobs::worker::Worker;
use query::engine::QueryEngine;
use slm::registry::{ModelRegistry, RegistryError};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use storage::repo::{RepoError, Repository};
use storage::wal::WalRecoveryMode;
use tokio::task::JoinHandle;

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("repository: {0}")]
    Repository(#[from] RepoError),
    #[error("durable extraction queue: {0}")]
    Queue(#[from] JobQueueError),
    #[error("model registry: {0}")]
    Registry(#[from] RegistryError),
    #[error("data directory: {0}")]
    Io(#[from] std::io::Error),
    #[error("extraction jobs did not settle before the deadline")]
    JobsTimeout,
    #[error("extraction worker stopped unexpectedly")]
    WorkerStopped,
}

type ConfigureIngestion = Box<dyn FnOnce(IngestionPipeline) -> IngestionPipeline + Send>;

pub struct RuntimeBuilder {
    data_dir: PathBuf,
    extraction_enabled: bool,
    registry: Option<(Arc<ModelRegistry>, String)>,
    queue_config: DurableQueueConfig,
    configure_ingestion: Option<ConfigureIngestion>,
    configure_query: Option<Box<dyn FnOnce(QueryEngine) -> QueryEngine + Send>>,
}

impl RuntimeBuilder {
    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        Self {
            data_dir: data_dir.as_ref().to_path_buf(),
            extraction_enabled: true,
            registry: None,
            configure_ingestion: None,
            configure_query: None,
            queue_config: DurableQueueConfig {
                recovery_mode: WalRecoveryMode::FailFast,
                ..Default::default()
            },
        }
    }

    /// Explicit vector-only fail-safe mode: no extraction queue or background task.
    pub fn extraction_enabled(self, enabled: bool) -> Self {
        Self {
            extraction_enabled: enabled,
            ..self
        }
    }

    pub fn with_registry(self, registry: Arc<ModelRegistry>, model_ref: impl Into<String>) -> Self {
        Self {
            registry: Some((registry, model_ref.into())),
            ..self
        }
    }

    pub fn with_queue_config(self, config: DurableQueueConfig) -> Self {
        Self {
            queue_config: config,
            ..self
        }
    }

    /// Configure policy, governance and audit hooks before the durable queue is attached.
    pub fn with_ingestion_config(
        self,
        configure: impl FnOnce(IngestionPipeline) -> IngestionPipeline + Send + 'static,
    ) -> Self {
        Self {
            configure_ingestion: Some(Box::new(configure)),
            ..self
        }
    }

    pub fn with_query_config(
        self,
        configure: impl FnOnce(QueryEngine) -> QueryEngine + Send + 'static,
    ) -> Self {
        Self {
            configure_query: Some(Box::new(configure)),
            ..self
        }
    }

    pub async fn start(self) -> Result<AlayasikiRuntime, RuntimeError> {
        tokio::fs::create_dir_all(&self.data_dir).await?;
        let repo = Arc::new(
            Repository::open_with_snapshots(
                self.data_dir.join("graph.wal"),
                self.data_dir.join("snapshots"),
            )
            .await?,
        );
        let pipeline = IngestionPipeline::new(repo.clone());
        let mut pipeline = match self.configure_ingestion {
            Some(configure) => configure(pipeline),
            None => pipeline,
        };
        let (queue, worker, stop) = if self.extraction_enabled {
            let (registry, model_ref) = match self.registry {
                Some(configured) => configured,
                None => {
                    let mut registry = ModelRegistry::new();
                    slm::lightweight::register_default_lightweight_models(&mut registry)?;
                    (Arc::new(registry), "triplex-lite@1.0.0".to_string())
                }
            };
            registry.resolve(&model_ref)?;
            pipeline.set_default_extraction_model_id(model_ref.clone());
            let (queue, rx) = DurableJobQueue::open_with_config(
                self.data_dir.join("jobs.wal"),
                self.queue_config,
            )
            .await?;
            let queue = Arc::new(queue);
            pipeline.set_job_queue(queue.clone());
            let worker = Worker::with_durable_registry(repo.clone(), registry, model_ref);
            let (stop, shutdown) = tokio::sync::watch::channel(false);
            let handle =
                tokio::spawn(worker.run_durable_until_shutdown(queue.clone(), rx, shutdown));
            (Some(queue), Some(handle), Some(stop))
        } else {
            (None, None, None)
        };
        let communities = Arc::new(community::RuntimeCommunities::default());
        let query = QueryEngine::new(repo.clone()).with_community_provider(communities.clone());
        let query = match self.configure_query {
            Some(configure) => configure(query),
            None => query,
        };
        let view = Arc::new(
            repo.load_snapshot_view(&repo.current_snapshot_id().await)
                .await?,
        );
        communities.summaries(view, None).await?;
        Ok(AlayasikiRuntime {
            repo,
            pipeline,
            query,
            queue,
            worker,
            stop,
            communities,
        })
    }
}

pub struct AlayasikiRuntime {
    communities: Arc<community::RuntimeCommunities>,
    repo: Arc<Repository>,
    pipeline: IngestionPipeline,
    query: QueryEngine,
    queue: Option<Arc<DurableJobQueue>>,
    worker: Option<JoinHandle<()>>,
    stop: Option<tokio::sync::watch::Sender<bool>>,
}

impl AlayasikiRuntime {
    pub fn repository(&self) -> &Arc<Repository> {
        &self.repo
    }
    pub fn ingestion(&self) -> &IngestionPipeline {
        &self.pipeline
    }
    pub fn query(&self) -> &QueryEngine {
        &self.query
    }

    /// Refreshes only when the durable graph version changes; all mutations,
    /// including extraction and direct repository writes, are detected by LSN.
    pub async fn refresh_communities(
        &self,
    ) -> Result<Vec<storage::community::CommunitySummary>, RuntimeError> {
        let snapshot = self.repo.current_snapshot_id().await;
        self.communities
            .summaries(
                Arc::new(self.repo.load_snapshot_view(&snapshot).await?),
                None,
            )
            .await
            .map_err(Into::into)
    }

    /// Resource summaries are rebuilt from currently visible tenant nodes so
    /// cached community text cannot outlive a source retention deadline.
    pub async fn refresh_communities_for_tenant(
        &self,
        tenant: &str,
    ) -> Result<Vec<storage::community::CommunitySummary>, RuntimeError> {
        let snapshot = self.repo.current_snapshot_id().await;
        let view = self.repo.load_snapshot_view(&snapshot).await?;
        Ok(community::tenant_resource_summaries(&view, tenant))
    }

    pub fn community_rebuild_count(&self) -> u64 {
        self.communities.rebuild_count()
    }

    pub async fn job_stats(&self) -> Option<JobQueueStats> {
        match &self.queue {
            Some(queue) => Some(queue.stats().await),
            None => None,
        }
    }

    pub async fn dead_letters(&self) -> Vec<DeadLetterEntry> {
        match &self.queue {
            Some(queue) => queue.dead_letters().await,
            None => Vec::new(),
        }
    }

    /// Waits for completion or DLQ settlement; inspect stats/DLQ for extraction failures.
    pub async fn wait_for_jobs(&self, deadline: Duration) -> Result<(), RuntimeError> {
        tokio::time::timeout(deadline, async {
            if let Some(queue) = &self.queue {
                while queue.stats().await.pending_depth > 0 {
                    if self.worker.as_ref().is_some_and(JoinHandle::is_finished) {
                        return Err(RuntimeError::WorkerStopped);
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
            Ok(())
        })
        .await
        .map_err(|_| RuntimeError::JobsTimeout)?
    }

    /// Stops at a job boundary and joins before flushing. Queued jobs replay on
    /// restart. Stop accepting requests first; a blocked extractor can delay shutdown.
    pub async fn shutdown(mut self) -> Result<(), RuntimeError> {
        if let Some(worker) = self.worker.take() {
            if let Some(stop) = &self.stop {
                let _ = stop.send(true);
            }
            worker.await.map_err(|_| RuntimeError::WorkerStopped)?;
        }
        self.repo.flush().await?;
        Ok(())
    }
}

impl Drop for AlayasikiRuntime {
    fn drop(&mut self) {
        if let Some(stop) = &self.stop {
            let _ = stop.send(true);
        }
    }
}
