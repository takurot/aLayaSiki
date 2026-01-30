use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Job {
    ExtractEntities { node_id: u64, content: String },
}

#[async_trait::async_trait]
pub trait JobQueue: Send + Sync {
    async fn enqueue(&self, job: Job) -> anyhow::Result<()>;
}

/// Simple in-memory queue using Tokio channels
pub struct ChannelJobQueue {
    sender: mpsc::Sender<Job>,
}

impl ChannelJobQueue {
    pub fn new(sender: mpsc::Sender<Job>) -> Self {
        Self { sender }
    }
}

#[async_trait::async_trait]
impl JobQueue for ChannelJobQueue {
    async fn enqueue(&self, job: Job) -> anyhow::Result<()> {
        self.sender.send(job).await.map_err(|e| anyhow::anyhow!("Queue send error: {}", e))
    }
}
