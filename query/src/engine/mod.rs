mod execution;
mod planning;
mod synthesis;

use crate::dsl::{QueryRequest, SearchMode};
use crate::semantic_cache::{SemanticCache, SemanticCacheConfig, SemanticCacheKey};
use alayasiki_core::audit::{AuditEvent, AuditOutcome, AuditSink};
use alayasiki_core::auth::{
    Action, AuthError, Authorizer, AuthzError, JwtAuthenticator, Principal, ResourceContext,
};
use alayasiki_core::error::{AlayasikiError, ErrorCode};
use alayasiki_core::metrics::{MetricsCollector, MetricsSnapshot};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Instant;
use storage::community::CommunitySummary;
use storage::repo::{RepoError, Repository, SnapshotView};
use storage::session::SessionOwner;
use thiserror::Error;
use tokio::sync::Mutex;

use synthesis::{build_query_audit_event, effective_query_model_id};

/// Provenance metadata attached to evidence items.
/// Captures the data lineage: where it came from and how it was extracted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Provenance {
    /// Original source document (e.g. "s3://bucket/file.pdf")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Model that extracted/processed this data
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extraction_model_id: Option<String>,
    /// Snapshot when this data was ingested
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<String>,
    /// Timestamp of ingestion (RFC3339)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ingested_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvidenceNode {
    pub id: u64,
    pub data: String,
    pub score: f32,
    pub hop: u8,
    pub provenance: Provenance,
    pub confidence: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvidenceEdge {
    pub source: u64,
    pub target: u64,
    pub relation: String,
    pub weight: f32,
    pub provenance: Provenance,
    pub confidence: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvidenceSubgraph {
    pub nodes: Vec<EvidenceNode>,
    pub edges: Vec<EvidenceEdge>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Citation {
    pub source: String,
    pub span: [usize; 2],
    pub node_id: u64,
    pub confidence: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Anchor {
    pub node_id: u64,
    pub score: f32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpansionPath {
    pub anchor_id: u64,
    pub target_id: u64,
    pub path: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExclusionReason {
    pub node_id: Option<u64>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExplainPlan {
    pub steps: Vec<String>,
    pub effective_search_mode: SearchMode,
    pub anchors: Vec<Anchor>,
    pub expansion_paths: Vec<ExpansionPath>,
    pub exclusions: Vec<ExclusionReason>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryResponse {
    pub answer: Option<String>,
    pub evidence: EvidenceSubgraph,
    pub citations: Vec<Citation>,
    pub groundedness: f32,
    pub explain: ExplainPlan,
    pub model_id: Option<String>,
    pub snapshot_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_travel: Option<String>,
    pub latency_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<ErrorCode>,
}

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("invalid query: {0}")]
    InvalidQuery(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("repository error: {0}")]
    Repository(#[from] RepoError),
    #[error("authorization error: {0}")]
    Unauthorized(#[from] AuthzError),
    #[error("authentication error: {0}")]
    Unauthenticated(#[from] AuthError),
}

impl AlayasikiError for QueryError {
    fn error_code(&self) -> ErrorCode {
        match self {
            QueryError::InvalidQuery(_) => ErrorCode::InvalidArgument,
            QueryError::NotFound(_) => ErrorCode::NotFound,
            QueryError::Repository(err) => err.error_code(),
            QueryError::Unauthorized(err) => err.error_code(),
            QueryError::Unauthenticated(err) => err.error_code(),
        }
    }
}

impl QueryError {
    pub fn to_response(&self) -> QueryResponse {
        QueryResponse {
            answer: Some(self.to_string()),
            evidence: EvidenceSubgraph {
                nodes: vec![],
                edges: vec![],
            },
            citations: vec![],
            groundedness: 0.0,
            explain: ExplainPlan {
                steps: vec!["error".to_string()],
                effective_search_mode: SearchMode::Auto,
                anchors: vec![],
                expansion_paths: vec![],
                exclusions: vec![],
            },
            model_id: None,
            snapshot_id: None,
            time_travel: None,
            latency_ms: 0,
            error_code: Some(self.error_code()),
        }
    }
}

pub struct QueryEngine {
    repo: Arc<Repository>,
    community_summaries: Vec<CommunitySummary>,
    audit_sink: Option<Arc<dyn AuditSink>>,
    semantic_cache: Arc<Mutex<SemanticCache<QueryResponse>>>,
    metrics: Arc<MetricsCollector>,
}

const DEFAULT_EMBEDDING_MODEL_ID: &str = "embedding-default-v1";
const UNICODE_NGRAM_SIZE: usize = 2;

#[derive(Debug, Clone)]
pub struct RankedNode {
    pub id: u64,
    pub data: String,
    pub score: f32,
    pub hop: u8,
    pub source: Option<String>,
    pub extraction_model_id: Option<String>,
    pub node_snapshot_id: Option<String>,
    pub ingested_at: Option<String>,
    pub confidence: f32,
}

/// Internal edge representation during query execution (before final output).
#[derive(Debug, Clone)]
pub struct InternalEdge {
    pub source: u64,
    pub target: u64,
    pub relation: String,
    pub weight: f32,
    pub provenance: Provenance,
    pub confidence: f32,
}

#[derive(Debug, Clone)]
pub struct ExecutionState {
    pub anchors: Vec<Anchor>,
    pub expansion_paths: Vec<ExpansionPath>,
    pub exclusions: Vec<ExclusionReason>,
    pub nodes: Vec<RankedNode>,
    pub edges: Vec<InternalEdge>,
}

#[derive(Clone)]
struct ResolvedSnapshot {
    snapshot_id: String,
    snapshot_lsn: u64,
    snapshot_view: Option<Arc<SnapshotView>>,
    time_travel: Option<String>,
    requires_versioned_summaries: bool,
}

impl QueryEngine {
    pub fn new(repo: Arc<Repository>) -> Self {
        Self {
            repo,
            community_summaries: Vec::new(),
            audit_sink: None,
            semantic_cache: Arc::new(Mutex::new(SemanticCache::with_config(
                SemanticCacheConfig::default(),
            ))),
            metrics: Arc::new(MetricsCollector::new(1000)),
        }
    }

    /// Attach pre-computed community summaries for global search support.
    pub fn with_community_summaries(mut self, summaries: Vec<CommunitySummary>) -> Self {
        self.community_summaries = summaries;
        self
    }

    pub fn with_audit_sink(mut self, sink: Arc<dyn AuditSink>) -> Self {
        self.audit_sink = Some(sink);
        self
    }

    pub fn with_semantic_cache_config(mut self, config: SemanticCacheConfig) -> Self {
        self.semantic_cache = Arc::new(Mutex::new(SemanticCache::with_config(config)));
        self
    }

    pub fn metrics(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }

    pub fn metrics_collector(&self) -> Arc<MetricsCollector> {
        self.metrics.clone()
    }

    pub async fn execute_json(&self, raw: &str) -> Result<QueryResponse, QueryError> {
        let request = QueryRequest::parse_json(raw)
            .map_err(|err| QueryError::InvalidQuery(err.to_string()))?;
        self.execute(request).await
    }

    pub async fn execute_json_authorized(
        &self,
        raw: &str,
        principal: &Principal,
        authorizer: &Authorizer,
        resource: &ResourceContext,
    ) -> Result<QueryResponse, QueryError> {
        let request = QueryRequest::parse_json(raw)
            .map_err(|err| QueryError::InvalidQuery(err.to_string()))?;
        self.execute_authorized(request, principal, authorizer, resource)
            .await
    }

    pub async fn execute_json_jwt_authorized(
        &self,
        raw: &str,
        bearer_token: &str,
        authenticator: &JwtAuthenticator,
        authorizer: &Authorizer,
        resource: &ResourceContext,
    ) -> Result<QueryResponse, QueryError> {
        let principal = self.authenticate_query_principal(
            bearer_token,
            authenticator,
            DEFAULT_EMBEDDING_MODEL_ID,
        )?;
        let request = QueryRequest::parse_json(raw)
            .map_err(|err| QueryError::InvalidQuery(err.to_string()))?;
        self.execute_authorized(request, &principal, authorizer, resource)
            .await
    }

    pub async fn execute_authorized(
        &self,
        request: QueryRequest,
        principal: &Principal,
        authorizer: &Authorizer,
        resource: &ResourceContext,
    ) -> Result<QueryResponse, QueryError> {
        let model_id = effective_query_model_id(&request);
        if let Err(err) = authorizer.authorize(principal, Action::Query, resource) {
            self.emit_audit_event(build_query_audit_event(
                AuditOutcome::Denied,
                &model_id,
                Some(principal.subject.clone()),
                Some(principal.tenant.clone()),
                None,
                Some(err.to_string()),
            ));
            return Err(err.into());
        }

        self.execute_with_audit(
            request,
            Some(principal.subject.clone()),
            Some(principal.tenant.clone()),
            Some(principal.tenant.clone()),
            Some(SessionOwner::new(
                principal.tenant.clone(),
                principal.subject.clone(),
            )),
        )
        .await
    }

    pub async fn execute_jwt_authorized(
        &self,
        request: QueryRequest,
        bearer_token: &str,
        authenticator: &JwtAuthenticator,
        authorizer: &Authorizer,
        resource: &ResourceContext,
    ) -> Result<QueryResponse, QueryError> {
        let model_id = effective_query_model_id(&request);
        let principal =
            self.authenticate_query_principal(bearer_token, authenticator, &model_id)?;

        self.execute_authorized(request, &principal, authorizer, resource)
            .await
    }

    fn authenticate_query_principal(
        &self,
        bearer_token: &str,
        authenticator: &JwtAuthenticator,
        model_id: &str,
    ) -> Result<Principal, QueryError> {
        authenticator.authenticate(bearer_token).map_err(|err| {
            self.emit_audit_event(build_query_audit_event(
                AuditOutcome::Denied,
                model_id,
                None,
                None,
                None,
                Some(err.to_string()),
            ));
            err.into()
        })
    }

    pub async fn execute(&self, request: QueryRequest) -> Result<QueryResponse, QueryError> {
        self.execute_with_audit(request, None, None, None, None)
            .await
    }

    async fn execute_with_audit(
        &self,
        request: QueryRequest,
        actor: Option<String>,
        tenant: Option<String>,
        tenant_scope: Option<String>,
        session_owner: Option<SessionOwner>,
    ) -> Result<QueryResponse, QueryError> {
        let start = Instant::now();
        let model_id = effective_query_model_id(&request);
        let result = self
            .execute_internal(request, start, tenant_scope, session_owner)
            .await;
        match &result {
            Ok(response) => {
                self.emit_audit_event(build_query_audit_event(
                    AuditOutcome::Succeeded,
                    &model_id,
                    actor,
                    tenant,
                    response.snapshot_id.clone(),
                    None,
                ));
            }
            Err(err) => {
                self.emit_audit_event(build_query_audit_event(
                    AuditOutcome::Failed,
                    &model_id,
                    actor,
                    tenant,
                    None,
                    Some(err.to_string()),
                ));
                self.metrics
                    .record_query(start.elapsed().as_micros() as u64, false);
            }
        }
        result
    }

    fn emit_audit_event(&self, event: AuditEvent) {
        if let Some(sink) = &self.audit_sink {
            let _ = sink.record(event);
        }
    }

    async fn lookup_semantic_cache(
        &self,
        key: &SemanticCacheKey,
        query: &str,
    ) -> Option<QueryResponse> {
        let mut cache = self.semantic_cache.lock().await;
        cache.lookup(key, query)
    }

    async fn insert_semantic_cache(
        &self,
        key: SemanticCacheKey,
        query: &str,
        response: QueryResponse,
    ) {
        let mut cache = self.semantic_cache.lock().await;
        cache.insert(key, query, response);
    }
}
