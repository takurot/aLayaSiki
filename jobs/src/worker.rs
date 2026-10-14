use crate::durable::{DurableJobQueue, JobEnvelope};
use crate::queue::Job;
use sha2::{Digest, Sha256};
use slm::ner::EntityExtractor;
use slm::registry::ModelRegistry;
use std::sync::Arc;
use std::time::Instant;
use storage::repo::Repository;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

pub struct Worker {
    receiver: Option<mpsc::Receiver<Job>>,
    repo: Arc<Repository>,
    registry: Arc<ModelRegistry>,
    default_model_ref: String,
}

impl Worker {
    pub fn new(
        receiver: mpsc::Receiver<Job>,
        repo: Arc<Repository>,
        extractor: Arc<dyn EntityExtractor>,
    ) -> Self {
        let mut registry = ModelRegistry::new();
        registry
            .register("legacy-default", "1.0.0", extractor)
            .expect("legacy extractor registration must succeed");
        registry
            .activate("legacy-default", "1.0.0")
            .expect("legacy extractor activation must succeed");

        Self {
            receiver: Some(receiver),
            repo,
            registry: Arc::new(registry),
            default_model_ref: "legacy-default".to_string(),
        }
    }

    /// Construct a worker for the durable queue path. No `mpsc::Receiver<Job>` is
    /// required because [`Worker::run_durable`] consumes a `Receiver<JobEnvelope>`
    /// from the [`DurableJobQueue`] instead.
    pub fn new_durable(repo: Arc<Repository>, extractor: Arc<dyn EntityExtractor>) -> Self {
        let mut registry = ModelRegistry::new();
        registry
            .register("legacy-default", "1.0.0", extractor)
            .expect("legacy extractor registration must succeed");
        registry
            .activate("legacy-default", "1.0.0")
            .expect("legacy extractor activation must succeed");

        Self {
            receiver: None,
            repo,
            registry: Arc::new(registry),
            default_model_ref: "legacy-default".to_string(),
        }
    }

    pub fn with_registry(
        receiver: mpsc::Receiver<Job>,
        repo: Arc<Repository>,
        registry: Arc<ModelRegistry>,
        default_model_ref: impl Into<String>,
    ) -> Self {
        Self {
            receiver: Some(receiver),
            repo,
            registry,
            default_model_ref: default_model_ref.into(),
        }
    }

    pub fn with_durable_registry(
        repo: Arc<Repository>,
        registry: Arc<ModelRegistry>,
        default_model_ref: impl Into<String>,
    ) -> Self {
        Self {
            receiver: None,
            repo,
            registry,
            default_model_ref: default_model_ref.into(),
        }
    }

    pub async fn run(mut self) {
        info!("Worker started");
        let Some(mut receiver) = self.receiver.take() else {
            error!("Worker::run called without a receiver; use run_durable for the durable queue");
            return;
        };
        while let Some(job) = receiver.recv().await {
            match job {
                Job::ExtractEntities {
                    node_id,
                    content,
                    model_id,
                    snapshot_id,
                } => {
                    info!("Processing ExtractEntities for node {}", node_id);
                    if let Err(e) = self
                        .process_extraction(node_id, &content, &model_id, &snapshot_id)
                        .await
                    {
                        error!("Failed to process extraction for node {}: {}", node_id, e);
                    }
                }
            }
        }
        info!("Worker stopped");
    }

    /// Drive extraction from a [`DurableJobQueue`], acknowledging completion (after
    /// flushing the graph WAL so extracted nodes/edges are durable before the job is
    /// retired) or reporting failures for bounded retry / dead-lettering.
    pub async fn run_durable(self, queue: Arc<DurableJobQueue>, rx: mpsc::Receiver<JobEnvelope>) {
        let (_stop, shutdown) = tokio::sync::watch::channel(false);
        self.run_durable_until_shutdown(queue, rx, shutdown).await;
    }

    /// Stops between jobs, never cancelling an in-flight graph/WAL mutation.
    pub async fn run_durable_until_shutdown(
        self,
        queue: Arc<DurableJobQueue>,
        mut rx: mpsc::Receiver<JobEnvelope>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        info!("Durable worker started");
        loop {
            if *shutdown.borrow() {
                break;
            }
            let envelope = tokio::select! {
                biased;
                _ = shutdown.changed() => break,
                envelope = rx.recv() => match envelope { Some(envelope) => envelope, None => break },
            };
            let id = envelope.id;
            let started = Instant::now();
            match envelope.job {
                Job::ExtractEntities {
                    node_id,
                    content,
                    model_id,
                    snapshot_id,
                } => {
                    match self
                        .process_extraction(node_id, &content, &model_id, &snapshot_id)
                        .await
                    {
                        Ok(()) => {
                            if let Err(e) = self.repo.flush().await {
                                error!(
                                    "repo flush before completing job {} failed: {}; retrying",
                                    id, e
                                );
                                if let Err(fe) = queue.fail(id, format!("repo flush: {e}")).await {
                                    error!("fail({}) error: {}", id, fe);
                                }
                            } else if let Err(e) = queue.complete(id).await {
                                error!("complete({}) error: {}", id, e);
                            }
                        }
                        Err(e) => {
                            warn!("extraction for job {} failed: {}", id, e);
                            if let Err(fe) = queue.fail(id, e.to_string()).await {
                                error!("fail({}) error: {}", id, fe);
                            }
                        }
                    }
                }
            }
            debug!("job {} processed in {:?}", id, started.elapsed());
        }
        info!("Durable worker stopped");
    }

    async fn process_extraction(
        &self,
        node_id: u64,
        content: &str,
        model_ref: &str,
        snapshot_id: &str,
    ) -> anyhow::Result<()> {
        let source = self.repo.get_node(node_id).await?;
        let resolved = self
            .registry
            .resolve(model_ref)
            .or_else(|_| self.registry.resolve(&self.default_model_ref))?;
        let extraction_model_ref = format!("{}@{}", resolved.model_id, resolved.version);
        let entities = resolved.extractor.extract(content).await?;

        let tenant = source.metadata.get("tenant");
        let mut mutations = Vec::new();
        for entity in entities {
            // Stable ID generation for entity node using Sha256
            let mut hasher = Sha256::new();
            if let Some(tenant) = tenant {
                hasher.update(b"alayasiki-tenant-entity-v1");
                hasher.update((tenant.len() as u64).to_le_bytes());
                hasher.update(tenant.as_bytes());
            }
            hasher.update(entity.text.as_bytes());
            let digest = hasher.finalize();
            // Use first 8 bytes for u64 ID
            let target_id = u64::from_le_bytes([
                digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6],
                digest[7],
            ]);

            // Ensure Entity Node exists
            let mut entity_node = alayasiki_core::model::Node {
                id: target_id,
                embedding: vec![], // No embedding for purely symbolic entity node for now
                data: entity.text.clone(),
                metadata: std::collections::HashMap::from([
                    ("type".to_string(), "entity".to_string()),
                    ("label".to_string(), entity.label.clone()),
                    (
                        "extraction_model_id".to_string(),
                        extraction_model_ref.clone(),
                    ),
                    ("snapshot_id".to_string(), snapshot_id.to_string()),
                ]),
            };

            if let Some(tenant) = tenant {
                entity_node.metadata.insert("tenant".into(), tenant.clone());
            }
            // A tenant-wide entity may be mentioned by sources with different
            // policies. Keep the earliest deadline; a later mention must never
            // extend or clear retention inherited from an earlier source.
            let existing = match self.repo.get_node(target_id).await {
                Ok(node) => Some(node),
                Err(storage::repo::RepoError::NotFound) => None,
                Err(error) => return Err(error.into()),
            };
            let retention = [Some(&source), existing.as_ref()]
                .into_iter()
                .flatten()
                .filter_map(|node| node.metadata.get("retention_until_unix"))
                .filter_map(|value| value.parse::<u64>().ok())
                .min();
            if let Some(deadline) = retention {
                entity_node
                    .metadata
                    .insert("retention_until_unix".into(), deadline.to_string());
            }
            mutations.push(storage::repo::IndexMutation::PutNode(entity_node));

            // Create Edge
            let mut edge = alayasiki_core::model::Edge {
                source: node_id,
                target: target_id,
                relation: "mentions".to_string(),
                weight: entity.confidence,
                metadata: std::collections::HashMap::from([
                    (
                        "extraction_model_id".to_string(),
                        extraction_model_ref.clone(),
                    ),
                    ("snapshot_id".to_string(), snapshot_id.to_string()),
                ]),
            };

            if let Some(tenant) = tenant {
                edge.metadata.insert("tenant".into(), tenant.clone());
            }
            if let Some(retention) = source.metadata.get("retention_until_unix") {
                edge.metadata
                    .insert("retention_until_unix".into(), retention.clone());
            }
            mutations.push(storage::repo::IndexMutation::PutEdge(edge));
        }
        self.repo.apply_index_transaction(mutations).await?;
        Ok(())
    }
}
