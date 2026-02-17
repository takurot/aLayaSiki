use crate::chunker::{Chunker, ChunkingConfig, SemanticChunker};
use crate::embedding::{DeterministicEmbedder, Embedder};
use crate::extract::{detect_content_kind, extract_pdf_text, extract_utf8, ContentKind};
use crate::policy::{ContentPolicy, NoOpPolicy, PolicyError};
use alayasiki_core::audit::{AuditEvent, AuditOperation, AuditOutcome, AuditSink};
use alayasiki_core::auth::{Action, Authorizer, AuthzError, Principal, ResourceContext};
use alayasiki_core::governance::{GovernanceError, GovernancePolicyStore};
use alayasiki_core::ingest::{ContentHash, IngestionRequest};
use alayasiki_core::model::Node;
use dashmap::DashMap;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
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
    #[error("Authorization error: {0}")]
    Unauthorized(#[from] AuthzError),
    #[error("Governance error: {0}")]
    Governance(#[from] GovernanceError),
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
    audit_sink: Option<Arc<dyn AuditSink>>,
    governance_policy_store: Option<Arc<dyn GovernancePolicyStore>>,
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
            audit_sink: None,
            governance_policy_store: None,
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
            audit_sink: None,
            governance_policy_store: None,
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
            audit_sink: None,
            governance_policy_store: None,
        }
    }

    pub fn set_job_queue(&mut self, queue: Arc<dyn JobQueue>) {
        self.job_queue = Some(queue);
    }

    pub fn set_audit_sink(&mut self, sink: Arc<dyn AuditSink>) {
        self.audit_sink = Some(sink);
    }

    pub fn set_governance_policy_store(&mut self, store: Arc<dyn GovernancePolicyStore>) {
        self.governance_policy_store = Some(store);
    }

    pub async fn ingest_authorized(
        &self,
        request: IngestionRequest,
        principal: &Principal,
        authorizer: &Authorizer,
        resource: &ResourceContext,
    ) -> Result<Vec<u64>, IngestionError> {
        let model_id = effective_ingest_model_id(&request, &self.default_model_id);
        if let Err(err) = authorizer.authorize(principal, Action::Ingest, resource) {
            self.emit_audit_event(build_audit_event(
                AuditOutcome::Denied,
                &model_id,
                Some(principal.subject.clone()),
                Some(principal.tenant.clone()),
                Some(err.to_string()),
            ));
            return Err(err.into());
        }

        let actor = Some(principal.subject.clone());
        let tenant = Some(principal.tenant.clone());
        self.ingest_with_audit(request, model_id, actor, tenant)
            .await
    }

    pub async fn ingest(&self, request: IngestionRequest) -> Result<Vec<u64>, IngestionError> {
        let model_id = effective_ingest_model_id(&request, &self.default_model_id);
        self.ingest_with_audit(request, model_id, None, None).await
    }

    async fn ingest_with_audit(
        &self,
        request: IngestionRequest,
        model_id: String,
        actor: Option<String>,
        tenant: Option<String>,
    ) -> Result<Vec<u64>, IngestionError> {
        let result = self.ingest_internal(request, tenant.as_deref()).await;
        let outcome = match &result {
            Ok(_) => AuditOutcome::Succeeded,
            Err(_) => AuditOutcome::Failed,
        };
        let error = result.as_ref().err().map(|err| err.to_string());
        self.emit_audit_event(build_audit_event(outcome, &model_id, actor, tenant, error));
        result
    }

    async fn ingest_internal(
        &self,
        request: IngestionRequest,
        tenant: Option<&str>,
    ) -> Result<Vec<u64>, IngestionError> {
        self.validate_governance_preflight(tenant, request.metadata())?;

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
        self.apply_governance(tenant, &mut metadata)?;

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

    fn validate_governance_preflight(
        &self,
        tenant: Option<&str>,
        metadata: &HashMap<String, String>,
    ) -> Result<(), IngestionError> {
        let (Some(policy_store), Some(tenant)) = (&self.governance_policy_store, tenant) else {
            return Ok(());
        };

        let Some(policy) = policy_store.get_policy(tenant)? else {
            return Ok(());
        };

        policy.ensure_residency(metadata.get("region").map(String::as_str))?;
        Ok(())
    }

    fn apply_governance(
        &self,
        tenant: Option<&str>,
        metadata: &mut HashMap<String, String>,
    ) -> Result<(), IngestionError> {
        let (Some(policy_store), Some(tenant)) = (&self.governance_policy_store, tenant) else {
            return Ok(());
        };

        let Some(policy) = policy_store.get_policy(tenant)? else {
            return Ok(());
        };

        policy.ensure_residency(metadata.get("region").map(String::as_str))?;
        metadata.insert("tenant".to_string(), tenant.to_string());
        metadata.insert(
            "residency_region".to_string(),
            policy.residency_region.clone(),
        );
        metadata.insert(
            "retention_until_unix".to_string(),
            policy
                .retention_deadline_unix(current_unix_timestamp())
                .to_string(),
        );
        if let Some(kms_key_id) = policy.kms_key_id() {
            metadata.insert("kms_key_id".to_string(), kms_key_id.to_string());
        }

        Ok(())
    }

    fn emit_audit_event(&self, event: AuditEvent) {
        if let Some(sink) = &self.audit_sink {
            let _ = sink.record(event);
        }
    }
}

fn effective_ingest_model_id(request: &IngestionRequest, default_model_id: &str) -> String {
    request.model_id().unwrap_or(default_model_id).to_string()
}

fn build_audit_event(
    outcome: AuditOutcome,
    model_id: &str,
    actor: Option<String>,
    tenant: Option<String>,
    error: Option<String>,
) -> AuditEvent {
    let mut event = AuditEvent::new(AuditOperation::Ingest, outcome);
    event.model_id = Some(model_id.to_string());
    event.actor = actor;
    event.tenant = tenant;
    if let Some(error) = error {
        event.metadata.insert("error".to_string(), error);
    }
    event
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

fn current_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn extract_request_text(
    request: IngestionRequest,
) -> Result<(String, HashMap<String, String>), IngestionError> {
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
