use alayasiki_core::ingest::{IngestionRequest, ContentHash};
use alayasiki_core::model::Node;
use storage::repo::Repository;
use crate::chunker::{Chunker, SemanticChunker};
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum IngestionError {
    #[error("Storage error: {0}")]
    Storage(#[from] storage::repo::RepoError),
    #[error("Unsupported content type: {0}")]
    UnsupportedType(String),
}

pub struct IngestionPipeline {
    repo: Arc<Repository>,
    chunker: Box<dyn Chunker>,
}

impl IngestionPipeline {
    pub fn new(repo: Arc<Repository>) -> Self {
        Self {
            repo,
            chunker: Box::new(SemanticChunker::default()),
        }
    }

    pub fn with_chunker(repo: Arc<Repository>, chunker: Box<dyn Chunker>) -> Self {
        Self {
            repo,
            chunker,
        }
    }

    pub async fn ingest(&self, request: IngestionRequest) -> Result<Vec<u64>, IngestionError> {
        // 1. Check Idempotency (Content Hash)
        let _hash = request.content_hash();
        // TODO: potential check against existing hashes in DB to prevent re-processing?
        // For now, we assume we process everything but rely on ID determinism or just overwrite.
        
        // 2. Extract Text
        let (text, metadata) = match request {
            IngestionRequest::Text { content, metadata } => (content, metadata),
            IngestionRequest::File { mime_type, .. } => {
                return Err(IngestionError::UnsupportedType(mime_type)); 
                // TODO: Implement PDF extraction here
            }
        };

        // 3. Chunking
        let chunks = self.chunker.chunk(&text, metadata).await;

        // 4. Embedding & Storage (Node Creation)
        let mut node_ids = Vec::new();
        for (_i, mut chunk) in chunks.into_iter().enumerate() {
            // Stub for Embedding Generation (Random or Zero for now)
            // In real impl, we would call an SLM/Embedding API here.
            let embedding = vec![0.0; 768]; 
            chunk.embedding = Some(embedding.clone());

            // Generate deterministic ID or random?
            // Using UUID v4 hashed to u64 for now, or monotonic if we had a counter.
            // Let's use a combination of hash + chunk_index for stability? 
            // Or simple random for MVP. UUID v4 is safe enough for collisions in small scale.
            let chunk_id = Uuid::new_v4().as_u64_pair().0; 

            let node = Node {
                id: chunk_id,
                embedding,
                data: chunk.content,
                metadata: chunk.metadata,
            };

            self.repo.put_node(node).await?;
            node_ids.push(chunk_id);
        }

        Ok(node_ids)
    }
}
