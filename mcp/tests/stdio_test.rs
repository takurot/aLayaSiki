use serde_json::{json, Value};
use std::process::Stdio;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command,
};
#[tokio::test]
async fn subprocess_stdio_handshake_tools_and_eof_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_alayasiki-mcp"))
        .arg("--data-dir")
        .arg(dir.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap()).lines();
    let init = json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"desktop","version":"1"}}});
    input
        .write_all(format!("{init}\n").as_bytes())
        .await
        .unwrap();
    let first: Value = serde_json::from_str(
        &tokio::time::timeout(std::time::Duration::from_secs(10), output.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(first["result"]["serverInfo"]["name"], "alayasiki-mcp");
    input.write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}\n").await.unwrap();
    let tools: Value = serde_json::from_str(&output.next_line().await.unwrap().unwrap()).unwrap();
    assert_eq!(tools["id"], 2);
    assert_eq!(tools["result"]["tools"].as_array().unwrap().len(), 4);
    input.write_all(b"{bad}\n").await.unwrap();
    let error: Value = serde_json::from_str(&output.next_line().await.unwrap().unwrap()).unwrap();
    assert_eq!(error["error"]["code"], -32700);
    drop(input);
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(10), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    assert!(output.next_line().await.unwrap().is_none());
}
#[tokio::test]
async fn help_and_version_do_not_open_database() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("must-not-exist");
    for flag in ["--help", "--version"] {
        let output = Command::new(env!("CARGO_BIN_EXE_alayasiki-mcp"))
            .arg(flag)
            .env("ALAYASIKI_DATA_DIR", &data)
            .output()
            .await
            .unwrap();
        assert!(output.status.success());
        assert!(String::from_utf8(output.stdout)
            .unwrap()
            .contains("alayasiki-mcp"));
        assert!(!data.exists());
    }
}

#[tokio::test]
async fn oversized_stdio_frame_recovers_at_next_newline() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = std::sync::Arc::new(
        alayasiki_runtime::RuntimeBuilder::new(dir.path())
            .extraction_enabled(false)
            .start()
            .await
            .unwrap(),
    );
    let connection = alayasiki_mcp::Connection::new(
        runtime,
        alayasiki_core::auth::Principal::new("agent", "local").with_roles(["admin"]),
    );
    let input = format!(
        "{}\n{{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"ping\"}}\n",
        "x".repeat(alayasiki_mcp::MAX_MESSAGE_BYTES + 1)
    );
    let mut output = Vec::new();
    alayasiki_mcp::stdio::run(BufReader::new(input.as_bytes()), &mut output, connection)
        .await
        .unwrap();
    let records: Vec<Value> = String::from_utf8(output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["error"]["code"], -32600);
    assert_eq!(records[1]["id"], 2);
}
