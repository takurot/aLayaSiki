use crate::queue::Job;
use slm::ner::EntityExtractor;
use storage::repo::Repository;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, error};

pub struct Worker {
    receiver: mpsc::Receiver<Job>,
    repo: Arc<Repository>,
    extractor: Arc<dyn EntityExtractor>,
}

impl Worker {
    pub fn new(
        receiver: mpsc::Receiver<Job>,
        repo: Arc<Repository>,
        extractor: Arc<dyn EntityExtractor>,
    ) -> Self {
        Self {
            receiver,
            repo,
            extractor,
        }
    }

    pub async fn run(mut self) {
        info!("Worker started");
        while let Some(job) = self.receiver.recv().await {
            match job {
                Job::ExtractEntities { node_id, content } => {
                    info!("Processing ExtractEntities for node {}", node_id);
                    if let Err(e) = self.process_extraction(node_id, &content).await {
                        error!("Failed to process extraction for node {}: {}", node_id, e);
                    }
                }
            }
        }
        info!("Worker stopped");
    }

    async fn process_extraction(&self, node_id: u64, content: &str) -> anyhow::Result<()> {
        let entities = self.extractor.extract(content).await?;
        
        for entity in entities {
            // Mock ID generation for entity node
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            use std::hash::{Hash, Hasher};
            entity.text.hash(&mut hasher);
            let target_id = hasher.finish();
            
            // Create Edge
            let edge = alayasiki_core::model::Edge {
                source: node_id,
                target: target_id,
                relation: "mentions".to_string(),
                weight: entity.confidence,
                metadata: std::collections::HashMap::new(),
            };
            
            if let Err(e) = self.repo.put_edge(edge.clone()).await {
                 error!("Failed to put edge: {}", e);
            } else {
                 info!("Created edge from {} to {} ({})", node_id, target_id, entity.text);
            }
        }
        Ok(())
    }
}
