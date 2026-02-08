use crate::chunker::{Chunker, ChunkingConfig, SemanticChunker};
use crate::embedding::{DeterministicEmbedder, Embedder};
use crate::extract::{detect_content_kind, extract_pdf_text, extract_utf8, ContentKind};
use crate::policy::{ContentPolicy, NoOpPolicy, PolicyError};
use alayasiki_core::ingest::{ContentHash, IngestionRequest};
use alayasiki_core::model::Node;
use dashmap::DashMap;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use storage::repo::Repository;
use thiserror::Error;

use jobs::queue::{Job, JobQueue};

#[derive(Error, Debug)]
pub enum IngestionError {
    #[error("Storage error: {0}")]
    Storage(#[from] storage::repo::RepoError),
    #[error("Unsupported content type: {0}")]
    UnsupportedType(String),
    #[error("Invalid UTF-8 content")]
    InvalidUtf8,
    #[error("Content extraction failed: {0}")]
    ExtractionFailed(String),
    #[error("Policy error: {0}")]
    Policy(#[from] PolicyError),
    #[error("Job Queue error: {0}")]
    JobQueue(#[from] anyhow::Error),
    #[error("Idempotency conflict: processing already in progress for key {0}")]
    IdempotencyConflict(String),
}

struct IdempotencyGuard {
    key: String,
    locks: Arc<DashMap<String, ()>>,
}

impl Drop for IdempotencyGuard {
    fn drop(&mut self) {
        self.locks.remove(&self.key);
    }
}

pub struct IngestionPipeline {
    repo: Arc<Repository>,
    chunker: Box<dyn Chunker>,
    embedder: Box<dyn Embedder>,
    policy: Box<dyn ContentPolicy>,
    default_model_id: String,
    default_extraction_model_id: String,
    // In-flight locks for idempotency keys
    locks: Arc<DashMap<String, ()>>,
    job_queue: Option<Arc<dyn JobQueue>>,
}

impl IngestionPipeline {
    pub fn new(repo: Arc<Repository>) -> Self {
        Self {
            repo,
            chunker: Box::new(SemanticChunker::default()),
            embedder: Box::new(DeterministicEmbedder::default()),
            policy: Box::new(NoOpPolicy),
            default_model_id: "embedding-default-v1".to_string(),
            default_extraction_model_id: "triplex-lite@1.0.0".to_string(),
            locks: Arc::new(DashMap::new()),
            job_queue: None,
        }
    }

    pub fn with_chunker(repo: Arc<Repository>, chunker: Box<dyn Chunker>) -> Self {
        Self {
            repo,
            chunker,
            embedder: Box::new(DeterministicEmbedder::default()),
            policy: Box::new(NoOpPolicy),
            // This line is intentionally left as is, as the diff did not include changes for `with_chunker`
            // and it's not using the `dedup` field anymore.
            // The `locks` field will be initialized by `new` or `with_components` if they were used.
            // For `with_chunker`, we'll add the default locks initialization.
            default_model_id: "embedding-default-v1".to_string(),
            default_extraction_model_id: "triplex-lite@1.0.0".to_string(),
            locks: Arc::new(DashMap::new()),
            job_queue: None,
        }
    }

    pub fn with_components(
        repo: Arc<Repository>,
        chunker: Box<dyn Chunker>,
        embedder: Box<dyn Embedder>,
        policy: Box<dyn ContentPolicy>,
        default_model_id: &str,
    ) -> Self {
        Self {
            repo,
            chunker,
            embedder,
            policy,
            default_model_id: default_model_id.to_string(),
            default_extraction_model_id: "triplex-lite@1.0.0".to_string(),
            locks: Arc::new(DashMap::new()),
            job_queue: None,
        }
    }

    pub fn set_job_queue(&mut self, queue: Arc<dyn JobQueue>) {
        self.job_queue = Some(queue);
    }

    pub async fn ingest(&self, request: IngestionRequest) -> Result<Vec<u64>, IngestionError> {
        let content_hash = request.content_hash();
        let idempotency_key = request.idempotency_key().map(|key| key.to_string());

        // LOCKING: Prevent concurrent processing of same key
        let lock_key = idempotency_key
            .clone()
            .unwrap_or_else(|| content_hash.clone());

        {
            if self.locks.contains_key(&lock_key) {
                return Err(IngestionError::IdempotencyConflict(lock_key));
            }
            self.locks.insert(lock_key.clone(), ());
        }
        // Created guard to remove lock on drop (RAII)
        let _guard = IdempotencyGuard {
            key: lock_key.clone(),
            locks: self.locks.clone(),
        };

        // 1. Check Persistent Idempotency
        if let Some(key) = idempotency_key.as_deref() {
            if let Some(ids) = self.repo.check_idempotency(key).await {
                return Ok(ids);
            }
        }
        if let Some(ids) = self.repo.check_idempotency(&content_hash).await {
            return Ok(ids);
        }

        let embedding_model_id = request
            .model_id()
            .unwrap_or(&self.default_model_id)
            .to_string();
        let extraction_model_id = request
            .model_id()
            .unwrap_or(&self.default_extraction_model_id)
            .to_string();

        let (text, mut metadata) = extract_request_text(request)?;
        metadata.insert("content_hash".to_string(), content_hash.clone());
        metadata.insert("model_id".to_string(), embedding_model_id.clone());
        if let Some(key) = &idempotency_key {
            metadata.insert("idempotency_key".to_string(), key.clone());
        }

        let text = self.policy.apply(&text)?;

        let chunks = self.chunker.chunk(&text, metadata).await;

        let mut node_ids = Vec::new();
        for (i, mut chunk) in chunks.into_iter().enumerate() {
            let embedding = self
                .embedder
                .embed(&chunk.content, &embedding_model_id)
                .await;
            chunk.embedding = Some(embedding.clone());

            let chunk_id = derive_chunk_id(&content_hash, i as u64);

            let chunk_content = chunk.content.clone();

            let node = Node {
                id: chunk_id,
                embedding,
                data: chunk.content,
                metadata: chunk.metadata,
            };

            self.repo.put_node(node).await?;
            node_ids.push(chunk_id);

            // Enqueue Job if queue is present
            if let Some(queue) = &self.job_queue {
                let snapshot_id = self.repo.current_snapshot_id().await;
                let job = Job::ExtractEntities {
                    node_id: chunk_id,
                    content: chunk_content,
                    model_id: extraction_model_id.clone(),
                    snapshot_id,
                };
                if let Err(e) = queue.enqueue(job).await {
                    // Best-effort: Log warning but continue ingestion to preserve idempotency
                    tracing::warn!("Failed to enqueue job for node {}: {}", chunk_id, e);
                }
            }
        }

        // 2. Record Idempotency persistently
        if let Some(key) = &idempotency_key {
            self.repo.record_idempotency(key, node_ids.clone()).await?;
        }
        self.repo
            .record_idempotency(&content_hash, node_ids.clone())
            .await?;

        // Guard will automatically remove lock on drop
        // self.locks.remove(&lock_key);

        Ok(node_ids)
    }
}

fn derive_chunk_id(content_hash: &str, index: u64) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(content_hash.as_bytes());
    hasher.update(index.to_le_bytes());
    let digest = hasher.finalize();
    u64::from_le_bytes([
        digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7],
    ])
}

fn extract_request_text(
    request: IngestionRequest,
) -> Result<(String, std::collections::HashMap<String, String>), IngestionError> {
    match request {
        IngestionRequest::Text {
            content, metadata, ..
        } => Ok((content, metadata)),
        IngestionRequest::File {
            filename,
            content,
            mime_type,
            mut metadata,
            ..
        } => {
            let kind = detect_content_kind(&mime_type, Some(&filename));
            metadata.insert("filename".to_string(), filename);
            metadata.insert("mime_type".to_string(), mime_type.clone());

            match kind {
                ContentKind::Text | ContentKind::Markdown | ContentKind::Json => {
                    let text = extract_utf8(&content).map_err(|_| IngestionError::InvalidUtf8)?;
                    Ok((text, metadata))
                }
                ContentKind::Pdf => {
                    if let Some(text) = extract_pdf_text(&content) {
                        Ok((text, metadata))
                    } else {
                        Err(IngestionError::ExtractionFailed("pdf".to_string()))
                    }
                }
                ContentKind::Unsupported => Err(IngestionError::UnsupportedType(mime_type)),
            }
        }
    }
}

#[allow(dead_code)]
pub fn default_chunker() -> Box<dyn Chunker> {
    Box::new(SemanticChunker::new(ChunkingConfig::default()))
}
