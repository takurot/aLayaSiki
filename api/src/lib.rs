//! Authenticated HTTP access to the standard durable runtime.
use alayasiki_core::{
    audit::{AuditEvent, AuditOperation, AuditOutcome, AuditSink, JsonlAuditSink},
    auth::{Action, Authorizer, JwtAuthenticator, Principal, ResourceContext},
    error::{AlayasikiError, ErrorCode},
    http::{ApiError, ApiResponse, IngestResponse},
    ingest::IngestionRequest,
};
use alayasiki_runtime::AlayasikiRuntime;
use axum::{
    extract::{rejection::JsonRejection, DefaultBodyLimit, Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension, Json, Router,
};
use query::QueryRequest;
use serde::Serialize;
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

pub struct ServerConfig {
    secret: String,
    issuer: String,
    audience: String,
    pub max_body_bytes: usize,
    pub max_concurrent_requests: usize,
    pub requests_per_second: u32,
}
impl ServerConfig {
    pub fn new(secret: String, issuer: String, audience: String) -> Result<Self, String> {
        if secret.len() < 32 || issuer.trim().is_empty() || audience.trim().is_empty() {
            return Err(
                "JWT secret must contain at least 32 bytes and issuer/audience must be nonempty"
                    .into(),
            );
        }
        Ok(Self {
            secret,
            issuer,
            audience,
            max_body_bytes: 8 * 1024 * 1024,
            max_concurrent_requests: 64,
            requests_per_second: 100,
        })
    }
    pub fn from_env() -> Result<Self, String> {
        let required = |name: &str| std::env::var(name).map_err(|_| format!("missing {name}"));
        Self::new(
            required("ALAYASIKI_JWT_SECRET")?,
            required("ALAYASIKI_JWT_ISSUER")?,
            required("ALAYASIKI_JWT_AUDIENCE")?,
        )
    }
}
pub struct ApiState {
    runtime: Arc<AlayasikiRuntime>,
    auth: JwtAuthenticator,
    authorizer: Authorizer,
    audit: Arc<dyn AuditSink>,
    limit: Arc<Semaphore>,
    rate: Mutex<(Instant, u32)>,
    config: ServerConfig,
}
impl ApiState {
    /// Call after the HTTP listener has stopped accepting requests. Detached
    /// durable operations retain their admission permit until completion.
    pub async fn drain(&self) -> Result<(), tokio::sync::AcquireError> {
        let _permits = self
            .limit
            .acquire_many(self.config.max_concurrent_requests as u32)
            .await?;
        Ok(())
    }

    pub fn new(
        runtime: Arc<AlayasikiRuntime>,
        config: ServerConfig,
        audit_path: impl AsRef<Path>,
    ) -> Result<Arc<Self>, std::io::Error> {
        if config.max_body_bytes == 0
            || config.max_concurrent_requests == 0
            || config.max_concurrent_requests > u32::MAX as usize
            || config.requests_per_second == 0
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "server limits must be positive and concurrency must fit u32",
            ));
        }
        let audit = JsonlAuditSink::open(audit_path).map_err(std::io::Error::other)?;
        Ok(Arc::new(Self {
            auth: JwtAuthenticator::new_hs256(
                &config.secret,
                Some(&config.issuer),
                Some(&config.audience),
            ),
            limit: Arc::new(Semaphore::new(config.max_concurrent_requests)),
            rate: Mutex::new((Instant::now(), 0)),
            config,
            runtime,
            authorizer: Authorizer::default(),
            audit: Arc::new(audit),
        }))
    }
}
fn success<T: Serialize>(data: T) -> Response {
    Json(ApiResponse {
        success: true,
        data: Some(data),
        error: None,
    })
    .into_response()
}
fn failure(status: StatusCode, code: ErrorCode, message: &str, retryable: bool) -> Response {
    (
        status,
        Json(ApiResponse::<()> {
            success: false,
            data: None,
            error: Some(ApiError {
                code,
                message: message.into(),
                retryable,
            }),
        }),
    )
        .into_response()
}
fn mapped(error: &impl AlayasikiError) -> Response {
    let code = error.error_code();
    let (status, message) = match code {
        ErrorCode::InvalidArgument => (StatusCode::BAD_REQUEST, "invalid request"),
        ErrorCode::Unauthenticated => (StatusCode::UNAUTHORIZED, "authentication required"),
        ErrorCode::PermissionDenied => (StatusCode::FORBIDDEN, "permission denied"),
        ErrorCode::NotFound => (StatusCode::NOT_FOUND, "resource not found"),
        ErrorCode::ResourceExhausted => (StatusCode::TOO_MANY_REQUESTS, "resource exhausted"),
        ErrorCode::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "internal error"),
    };
    failure(status, code, message, false)
}
pub fn router(state: Arc<ApiState>) -> Router {
    let protected = Router::new()
        .route("/v1/ingest", post(ingest))
        .route("/v1/query", post(query))
        .route("/metrics", get(metrics))
        .route("/v1/graph", get(graph))
        .route_layer(middleware::from_fn_with_state(state.clone(), authenticate));
    Router::new()
        .route(
            "/health",
            get(|| async { success(serde_json::json!({"status":"ok"})) }),
        )
        .merge(protected)
        .layer(DefaultBodyLimit::max(state.config.max_body_bytes))
        .layer(middleware::from_fn_with_state(state.clone(), admission))
        .with_state(state)
}
async fn admission(
    State(state): State<Arc<ApiState>>,
    mut request: Request,
    next: Next,
) -> Response {
    let permit = match state.limit.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            return failure(
                StatusCode::TOO_MANY_REQUESTS,
                ErrorCode::ResourceExhausted,
                "server busy",
                true,
            )
        }
    };
    let allowed = {
        let Ok(mut rate) = state.rate.lock() else {
            return failure(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::Internal,
                "internal error",
                false,
            );
        };
        if rate.0.elapsed() >= Duration::from_secs(1) {
            *rate = (Instant::now(), 0)
        };
        rate.1 = rate.1.saturating_add(1);
        rate.1 <= state.config.requests_per_second
    };
    if !allowed {
        return failure(
            StatusCode::TOO_MANY_REQUESTS,
            ErrorCode::ResourceExhausted,
            "rate limit exceeded",
            true,
        );
    }
    request.extensions_mut().insert(Arc::new(permit));
    next.run(request).await
}
async fn authenticate(
    State(state): State<Arc<ApiState>>,
    mut request: Request,
    next: Next,
) -> Response {
    let header = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let principal = match state.auth.authenticate(header) {
        Ok(p) => p,
        Err(_) => {
            let operation = if request.uri().path() == "/v1/ingest" {
                AuditOperation::Ingest
            } else {
                AuditOperation::Query
            };
            if state
                .audit
                .record(AuditEvent::new(operation, AuditOutcome::Denied))
                .is_err()
            {
                return failure(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ErrorCode::Internal,
                    "audit unavailable",
                    false,
                );
            }
            return failure(
                StatusCode::UNAUTHORIZED,
                ErrorCode::Unauthenticated,
                "authentication required",
                false,
            );
        }
    };
    request.extensions_mut().insert(principal);
    next.run(request).await
}
fn json_error(error: JsonRejection) -> Response {
    let status = if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
        StatusCode::PAYLOAD_TOO_LARGE
    } else {
        StatusCode::BAD_REQUEST
    };
    failure(
        status,
        ErrorCode::InvalidArgument,
        "invalid JSON payload",
        false,
    )
}
async fn ingest(
    State(state): State<Arc<ApiState>>,
    Extension(principal): Extension<Principal>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    payload: Result<Json<IngestionRequest>, JsonRejection>,
) -> Response {
    let Json(request) = match payload {
        Ok(p) => p,
        Err(e) => return json_error(e),
    };
    // Detached execution protects durable writes if the peer disconnects.
    match tokio::spawn(async move {
        let _permit = permit;
        let resource = ResourceContext::new(principal.tenant.clone());
        let result = state
            .runtime
            .ingestion()
            .ingest_authorized(request, &principal, &state.authorizer, &resource)
            .await;
        match result {
            Ok(node_ids) => success(IngestResponse {
                node_ids,
                snapshot_id: state.runtime.repository().current_snapshot_id().await,
            }),
            Err(ingestion::processor::IngestionError::IdempotencyConflict(_)) => failure(
                StatusCode::TOO_MANY_REQUESTS,
                ErrorCode::ResourceExhausted,
                "ingestion in progress",
                true,
            ),
            Err(e) => ingest_error(&e),
        }
    })
    .await
    {
        Ok(response) => response,
        Err(_) => failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::Internal,
            "internal error",
            false,
        ),
    }
}
async fn query(
    State(state): State<Arc<ApiState>>,
    Extension(principal): Extension<Principal>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    payload: Result<Json<QueryRequest>, JsonRejection>,
) -> Response {
    let Json(request) = match payload {
        Ok(p) => p,
        Err(e) => return json_error(e),
    };
    match tokio::spawn(async move {
        let _permit = permit;
        let resource = ResourceContext::new(principal.tenant.clone());
        match state
            .runtime
            .query()
            .execute_authorized(request, &principal, &state.authorizer, &resource)
            .await
        {
            Ok(response) => success(response),
            Err(query::QueryError::Repository(storage::repo::RepoError::Wal(
                storage::wal::WalError::Io(_),
            )))
            | Err(query::QueryError::Repository(storage::repo::RepoError::Snapshot(
                storage::snapshot::SnapshotError::Io(_),
            ))) => failure(
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorCode::Internal,
                "storage temporarily unavailable",
                true,
            ),
            Err(e) => mapped(&e),
        }
    })
    .await
    {
        Ok(response) => response,
        Err(_) => failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::Internal,
            "internal error",
            false,
        ),
    }
}
async fn metrics(
    State(state): State<Arc<ApiState>>,
    Extension(principal): Extension<Principal>,
) -> Response {
    if state
        .authorizer
        .authorize(
            &principal,
            Action::Admin,
            &ResourceContext::new(principal.tenant.clone()),
        )
        .is_err()
    {
        return authorization_denied(&state, &principal, "/metrics");
    }
    success(state.runtime.query().metrics())
}

fn ingest_error(error: &ingestion::processor::IngestionError) -> Response {
    use ingestion::processor::IngestionError;
    match error {
        IngestionError::Storage(storage::repo::RepoError::Wal(storage::wal::WalError::Io(_)))
        | IngestionError::Storage(storage::repo::RepoError::Snapshot(
            storage::snapshot::SnapshotError::Io(_),
        )) => failure(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::Internal,
            "storage temporarily unavailable",
            true,
        ),
        IngestionError::Unauthorized(e) => mapped(e),
        IngestionError::Unauthenticated(e) => mapped(e),
        IngestionError::UnsupportedType(_)
        | IngestionError::InvalidUtf8
        | IngestionError::ExtractionFailed(_)
        | IngestionError::Policy(_)
        | IngestionError::Governance(_) => failure(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidArgument,
            "invalid ingestion payload",
            false,
        ),
        _ => failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::Internal,
            "ingestion failed",
            false,
        ),
    }
}

#[derive(serde::Deserialize)]
struct GraphParams {
    limit: Option<usize>,
    after: Option<u64>,
}
async fn graph(
    State(state): State<Arc<ApiState>>,
    Extension(principal): Extension<Principal>,
    axum::extract::Query(params): axum::extract::Query<GraphParams>,
) -> Response {
    if state
        .authorizer
        .authorize(
            &principal,
            Action::Query,
            &ResourceContext::new(principal.tenant.clone()),
        )
        .is_err()
    {
        return authorization_denied(&state, &principal, "/v1/graph");
    }
    let limit = params.limit.unwrap_or(100);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    if limit == 0 || limit > 1000 {
        return failure(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidArgument,
            "limit must be between 1 and 1000",
            false,
        );
    }
    let repo = state.runtime.repository();
    let mut nodes = Vec::new();
    for id in repo
        .list_node_ids()
        .await
        .into_iter()
        .filter(|id| params.after.is_none_or(|after| *id > after))
    {
        if let Ok(node) = repo.get_node(id).await {
            if node.metadata.get("tenant") == Some(&principal.tenant)
                && node
                    .metadata
                    .get("retention_until_unix")
                    .and_then(|value| value.parse::<u64>().ok())
                    .is_none_or(|deadline| now < deadline)
            {
                nodes.push(node);
                if nodes.len() == limit {
                    break;
                }
            }
        }
    }
    let ids: std::collections::HashSet<_> = nodes.iter().map(|n| n.id).collect();
    let mut edges = Vec::new();
    for node in &nodes {
        for (target, relation, weight) in repo.neighbors_with_session_graph(node.id, None).await {
            if ids.contains(&target) {
                edges.push(serde_json::json!({"source":node.id.to_string(),"target":target.to_string(),"relation":relation,"weight":weight}));
            }
        }
    }
    edges.sort_by_key(|edge| edge.to_string());
    let next_after = nodes.last().map(|node| node.id.to_string());
    let nodes: Vec<_> = nodes
        .into_iter()
        .map(|n| serde_json::json!({"id":n.id.to_string(),"data":n.data,"metadata":n.metadata}))
        .collect();
    success(serde_json::json!({"nodes":nodes,"edges":edges,"next_after":next_after}))
}

fn authorization_denied(state: &ApiState, principal: &Principal, endpoint: &str) -> Response {
    let mut event = AuditEvent::new(AuditOperation::Query, AuditOutcome::Denied);
    event.actor = Some(principal.subject.clone());
    event.tenant = Some(principal.tenant.clone());
    event.metadata.insert("endpoint".into(), endpoint.into());
    if state.audit.record(event).is_err() {
        return failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::Internal,
            "audit unavailable",
            false,
        );
    }
    failure(
        StatusCode::FORBIDDEN,
        ErrorCode::PermissionDenied,
        "permission denied",
        false,
    )
}
