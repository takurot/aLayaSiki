use alayasiki_core::{audit::JsonlAuditSink, auth::Principal};
use alayasiki_mcp::{stdio, Connection};
use alayasiki_runtime::RuntimeBuilder;
use std::{path::PathBuf, sync::Arc};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let mut data = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                println!("alayasiki-mcp [--data-dir PATH]\nMCP 2024-11-05 stdio server. HTTP/SSE is hosted by alayasiki-api.");
                return Ok(());
            }
            "--version" | "-V" => {
                println!("alayasiki-mcp {}", env!("CARGO_PKG_VERSION"));
                return Ok(());
            }
            "--data-dir" => {
                data = Some(PathBuf::from(
                    args.next().ok_or("--data-dir requires a path")?,
                ));
            }
            _ => return Err("unknown command-line argument".into()),
        }
    }
    let data = match data.or_else(|| std::env::var_os("ALAYASIKI_DATA_DIR").map(PathBuf::from)) {
        Some(data) => data,
        None => PathBuf::from(
            std::env::var_os("HOME")
                .or_else(|| std::env::var_os("USERPROFILE"))
                .ok_or("set --data-dir or ALAYASIKI_DATA_DIR when the user home is unavailable")?,
        )
        .join(".alayasiki-mcp"),
    };
    std::fs::create_dir_all(&data)?;
    let audit = Arc::new(JsonlAuditSink::open(data.join("audit.jsonl"))?);
    let ingest_audit = audit.clone();
    let connection_audit = audit.clone();
    let runtime = Arc::new(
        RuntimeBuilder::new(&data)
            .with_ingestion_config(move |mut pipeline| {
                pipeline.set_audit_sink(ingest_audit);
                pipeline
            })
            .with_query_config(move |query| query.with_audit_sink(audit))
            .start()
            .await?,
    );
    let principal = Principal::new(
        std::env::var("ALAYASIKI_MCP_SUBJECT").unwrap_or_else(|_| "local-agent".into()),
        std::env::var("ALAYASIKI_MCP_TENANT").unwrap_or_else(|_| "local".into()),
    )
    .with_roles(["admin"]);
    if principal.tenant.trim().is_empty() || principal.subject.trim().is_empty() {
        return Err("MCP tenant and subject must be nonempty".into());
    }
    let result = stdio::run(
        tokio::io::BufReader::new(tokio::io::stdin()),
        tokio::io::stdout(),
        Connection::new(runtime.clone(), principal).with_audit_sink(connection_audit),
    )
    .await;
    Arc::try_unwrap(runtime)
        .map_err(|_| "active MCP connection during shutdown")?
        .shutdown()
        .await?;
    result?;
    Ok(())
}
