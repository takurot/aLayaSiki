use crate::queue::Job;
use sha2::{Digest, Sha256};
use slm::ner::EntityExtractor;
use slm::registry::ModelRegistry;
use std::sync::Arc;
use storage::repo::Repository;
use tokio::sync::mpsc;
use tracing::{error, info};

pub struct Worker {
    receiver: mpsc::Receiver<Job>,
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
            receiver,
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
            receiver,
            repo,
            registry,
            default_model_ref: default_model_ref.into(),
        }
    }

    pub async fn run(mut self) {
        info!("Worker started");
        while let Some(job) = self.receiver.recv().await {
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

    async fn process_extraction(
        &self,
        node_id: u64,
        content: &str,
        model_ref: &str,
        snapshot_id: &str,
    ) -> anyhow::Result<()> {
        let resolved = self
            .registry
            .resolve(model_ref)
            .or_else(|_| self.registry.resolve(&self.default_model_ref))?;
        let extraction_model_ref = format!("{}@{}", resolved.model_id, resolved.version);
        let entities = resolved.extractor.extract(content).await?;

        for entity in entities {
            // Stable ID generation for entity node using Sha256
            let mut hasher = Sha256::new();
            hasher.update(entity.text.as_bytes());
            let digest = hasher.finalize();
            // Use first 8 bytes for u64 ID
            let target_id = u64::from_le_bytes([
                digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6],
                digest[7],
            ]);

            // Ensure Entity Node exists
            let entity_node = alayasiki_core::model::Node {
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

            if let Err(e) = self.repo.put_node(entity_node).await {
                error!("Failed to put entity node {}: {}", target_id, e);
                // Continue to try putting edge? Maybe edge will fail if node missing in some DBs,
                // but our Repo/HyperIndex might allow it. Better to log and proceed.
            }

            // Create Edge
            let edge = alayasiki_core::model::Edge {
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

            if let Err(e) = self.repo.put_edge(edge.clone()).await {
                error!("Failed to put edge: {}", e);
            } else {
                info!(
                    "Created edge from {} to {} ({})",
                    node_id, target_id, entity.text
                );
            }
        }
        Ok(())
    }
}
