use alayasiki_api::{router, ApiState, ServerConfig};
use alayasiki_core::audit::JsonlAuditSink;
use alayasiki_runtime::RuntimeBuilder;
use std::{path::PathBuf, sync::Arc};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ServerConfig::from_env()?;
    let data = PathBuf::from(std::env::var("ALAYASIKI_DATA_DIR").unwrap_or_else(|_| "data".into()));
    std::fs::create_dir_all(&data)?;
    let audit = Arc::new(JsonlAuditSink::open(data.join("audit.jsonl"))?);
    let ingest_audit = audit.clone();
    let runtime = Arc::new(
        RuntimeBuilder::new(&data)
            .with_ingestion_config(move |mut p| {
                p.set_audit_sink(ingest_audit);
                p
            })
            .with_query_config(move |q| q.with_audit_sink(audit))
            .start()
            .await?,
    );
    let state = ApiState::new(runtime.clone(), config, data.join("http-audit.jsonl"))?;
    let listener = tokio::net::TcpListener::bind(
        std::env::var("ALAYASIKI_BIND").unwrap_or_else(|_| "127.0.0.1:8080".into()),
    )
    .await?;
    axum::serve(listener, router(state.clone()))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    state.drain().await?;
    drop(state);
    Arc::try_unwrap(runtime)
        .map_err(|_| "runtime still has active requests")?
        .shutdown()
        .await?;
    Ok(())
}
