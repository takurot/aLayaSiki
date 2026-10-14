//! Legacy SSE transport: endpoint announcement plus POST-to-message stream.
use crate::{Connection, MAX_MESSAGE_BYTES};
use alayasiki_core::{audit::AuditSink, auth::Principal};
use alayasiki_runtime::AlayasikiRuntime;
use axum::{
    body::Bytes,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{get, post},
    Extension, Router,
};
use futures_core::Stream;
use serde::Deserialize;
use std::{
    collections::HashMap,
    convert::Infallible,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll},
};
use tokio::sync::{mpsc, Mutex as AsyncMutex, Semaphore};
use tokio_stream::wrappers::ReceiverStream;
const MAX_SESSIONS: usize = 128;
const QUEUE_CAPACITY: usize = 32;
struct Session {
    tenant: String,
    subject: String,
    connection: Arc<AsyncMutex<Connection>>,
    sender: mpsc::Sender<Event>,
}
pub struct SseState {
    runtime: Arc<AlayasikiRuntime>,
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    origins: Vec<String>,
    dispatch: Arc<Semaphore>,
    audit: Option<Arc<dyn AuditSink>>,
    budget: Arc<crate::SessionBudget>,
    closing: std::sync::atomic::AtomicBool,
}
impl SseState {
    pub fn new(runtime: Arc<AlayasikiRuntime>, origins: Vec<String>) -> Arc<Self> {
        Self::with_audit_sink(runtime, origins, None)
    }
    pub fn with_audit_sink(
        runtime: Arc<AlayasikiRuntime>,
        origins: Vec<String>,
        audit: Option<Arc<dyn AuditSink>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            runtime,
            audit,
            budget: Arc::new(crate::SessionBudget::default()),
            closing: std::sync::atomic::AtomicBool::new(false),
            sessions: Mutex::new(HashMap::new()),
            origins,
            dispatch: Arc::new(Semaphore::new(32)),
        })
    }
    pub fn close_connections(&self) {
        if let Ok(mut sessions) = self.sessions.lock() {
            self.closing
                .store(true, std::sync::atomic::Ordering::Release);
            sessions.clear();
        }
    }
    pub async fn drain(&self) -> Result<(), tokio::sync::AcquireError> {
        let _permits = self.dispatch.acquire_many(32).await?;
        Ok(())
    }
    fn allowed(&self, headers: &HeaderMap) -> bool {
        headers.get("origin").is_none_or(|origin| {
            origin
                .to_str()
                .is_ok_and(|origin| self.origins.iter().any(|allowed| allowed == origin))
        })
    }
}
pub fn router<S: Clone + Send + Sync + 'static>(state: Arc<SseState>) -> Router<S> {
    Router::new()
        .route("/mcp/sse", get(connect))
        .route("/mcp/messages", post(messages))
        .with_state(state)
}
struct EventStream {
    inner: ReceiverStream<Event>,
    state: Weak<SseState>,
    id: String,
}
impl Stream for EventStream {
    type Item = Result<Event, Infallible>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner)
            .poll_next(cx)
            .map(|event| event.map(Ok))
    }
}
impl Drop for EventStream {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            if let Ok(mut sessions) = state.sessions.lock() {
                sessions.remove(&self.id);
            }
        }
    }
}
async fn connect(
    State(state): State<Arc<SseState>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
) -> Response {
    if !state.allowed(&headers) {
        return (StatusCode::FORBIDDEN, "Origin denied").into_response();
    }
    let id = uuid::Uuid::new_v4().to_string();
    let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
    let endpoint = Event::default()
        .event("endpoint")
        .data(format!("/mcp/messages?session_id={id}"));
    if tx.try_send(endpoint).is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let connection = Connection::new(state.runtime.clone(), principal.clone())
        .with_session_budget(state.budget.clone());
    let connection = match &state.audit {
        Some(audit) => connection.with_audit_sink(audit.clone()),
        None => connection,
    };
    let session = Arc::new(Session {
        tenant: principal.tenant.clone(),
        subject: principal.subject.clone(),
        connection: Arc::new(AsyncMutex::new(connection)),
        sender: tx,
    });
    {
        let Ok(mut sessions) = state.sessions.lock() else {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        if state.closing.load(std::sync::atomic::Ordering::Acquire) {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
        if sessions.len() >= MAX_SESSIONS {
            return (StatusCode::TOO_MANY_REQUESTS, "MCP session limit reached").into_response();
        }
        sessions.insert(id.clone(), session);
    }
    Sse::new(EventStream {
        inner: ReceiverStream::new(rx),
        state: Arc::downgrade(&state),
        id,
    })
    .keep_alive(KeepAlive::default())
    .into_response()
}
#[derive(Deserialize)]
struct MessageParams {
    session_id: String,
}
async fn messages(
    State(state): State<Arc<SseState>>,
    Extension(principal): Extension<Principal>,
    admission: Option<Extension<Arc<tokio::sync::OwnedSemaphorePermit>>>,
    Query(params): Query<MessageParams>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !state.allowed(&headers) {
        return (StatusCode::FORBIDDEN, "Origin denied").into_response();
    }
    if body.len() > MAX_MESSAGE_BYTES {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    }
    let session = {
        let Ok(sessions) = state.sessions.lock() else {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        sessions.get(&params.session_id).cloned()
    };
    let Some(session) = session else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if session.tenant != principal.tenant || session.subject != principal.subject {
        return StatusCode::FORBIDDEN.into_response();
    }
    let output = match session.sender.clone().try_reserve_owned() {
        Ok(permit) => permit,
        Err(_) => return StatusCode::TOO_MANY_REQUESTS.into_response(),
    };
    let mut connection = match session.connection.clone().try_lock_owned() {
        Ok(connection) => connection,
        Err(_) => return StatusCode::TOO_MANY_REQUESTS.into_response(),
    };
    connection.principal = principal;
    // Complete the lifecycle notification before acknowledging its POST, so
    // the next request can immediately use the initialized connection.
    if let Ok(message) = serde_json::from_slice::<serde_json::Value>(&body) {
        if message["jsonrpc"] == "2.0"
            && message["method"] == "notifications/initialized"
            && message.get("id").is_none()
        {
            connection.dispatch(message).await;
            return StatusCode::ACCEPTED.into_response();
        }
    }
    let permit = match state.dispatch.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => return StatusCode::TOO_MANY_REQUESTS.into_response(),
    };
    tokio::spawn(async move {
        let _admission = admission;
        let _permit = permit;
        let response = match serde_json::from_slice(&body) {
            Ok(message) => connection.dispatch(message).await,
            Err(_) => Some(crate::protocol::error(
                serde_json::Value::Null,
                -32700,
                "Parse error",
            )),
        };
        if let Some(response) = response {
            let encoded = response.to_string();
            let encoded = if encoded.len() > MAX_MESSAGE_BYTES {
                crate::protocol::error(response["id"].clone(), -32603, "Response too large")
                    .to_string()
            } else {
                encoded
            };
            output.send(Event::default().event("message").data(encoded));
        }
    });
    StatusCode::ACCEPTED.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    async fn state() -> (tempfile::TempDir, Arc<SseState>, Principal) {
        let dir = tempfile::tempdir().unwrap();
        let runtime = Arc::new(
            alayasiki_runtime::RuntimeBuilder::new(dir.path())
                .extraction_enabled(false)
                .start()
                .await
                .unwrap(),
        );
        let state = SseState::new(runtime, vec!["https://trusted.example".into()]);
        (
            dir,
            state,
            Principal::new("agent", "local").with_roles(["admin"]),
        )
    }
    #[tokio::test]
    async fn full_output_queue_rejects_before_any_mutation() {
        let (_dir, state, principal) = state().await;
        let (tx, _rx) = mpsc::channel(1);
        tx.try_send(Event::default().data("occupied")).unwrap();
        state.sessions.lock().unwrap().insert(
            "test".into(),
            Arc::new(Session {
                tenant: principal.tenant.clone(),
                subject: principal.subject.clone(),
                connection: Arc::new(AsyncMutex::new(Connection::new(
                    state.runtime.clone(),
                    principal.clone(),
                ))),
                sender: tx,
            }),
        );
        let body=Bytes::from(serde_json::json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"alayasiki_ingest","arguments":{"content":"must not persist"}}}).to_string());
        let response = messages(
            State(state.clone()),
            Extension(principal),
            None,
            Query(MessageParams {
                session_id: "test".into(),
            }),
            HeaderMap::new(),
            body,
        )
        .await;
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(state.runtime.repository().list_node_ids().await.is_empty());
    }
    #[tokio::test]
    async fn sessions_are_bounded_and_stream_drop_removes_identity() {
        let (_dir, state, principal) = state().await;
        let mut streams = Vec::new();
        for _ in 0..MAX_SESSIONS {
            let response = connect(
                State(state.clone()),
                Extension(principal.clone()),
                HeaderMap::new(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            streams.push(response);
        }
        assert_eq!(
            connect(
                State(state.clone()),
                Extension(principal.clone()),
                HeaderMap::new()
            )
            .await
            .status(),
            StatusCode::TOO_MANY_REQUESTS
        );
        drop(streams);
        assert!(state.sessions.lock().unwrap().is_empty());
        let mut headers = HeaderMap::new();
        headers.insert("origin", "https://trusted.example".parse().unwrap());
        assert!(state.allowed(&headers));
        headers.insert("origin", "https://trusted.example.evil".parse().unwrap());
        assert!(!state.allowed(&headers));
        state.close_connections();
        assert_eq!(
            connect(State(state.clone()), Extension(principal), HeaderMap::new())
                .await
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        state.drain().await.unwrap();
    }
}
