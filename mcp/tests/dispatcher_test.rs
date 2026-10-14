use alayasiki_core::auth::Principal;
use alayasiki_mcp::Connection;
use alayasiki_runtime::RuntimeBuilder;
use serde_json::{json, Value};
use std::sync::Arc;
async fn call(c: &mut Connection, id: u64, method: &str, params: Value) -> Value {
    c.dispatch(json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
        .await
        .unwrap()
}
#[tokio::test]
async fn initialize_tools_resources_and_session_isolation() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(RuntimeBuilder::new(dir.path()).start().await.unwrap());
    let mut c = Connection::new(
        runtime.clone(),
        Principal::new("agent", "a").with_roles(["admin"]),
    );
    assert_eq!(
        call(&mut c, 0, "tools/list", json!({})).await["error"]["code"],
        -32600
    );
    assert_eq!(call(&mut c,1,"initialize",json!({"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1"}})).await["result"]["protocolVersion"],"2024-11-05");
    assert!(c
        .dispatch(json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
        .await
        .is_none());
    assert_eq!(
        call(&mut c, 2, "tools/list", json!({})).await["result"]["tools"]
            .as_array()
            .unwrap()
            .len(),
        4
    );
    let ingest = call(
        &mut c,
        3,
        "tools/call",
        json!({"name":"alayasiki_ingest","arguments":{"content":"Rust startup evidence"}}),
    )
    .await;
    assert_eq!(ingest["result"]["isError"], false);
    let query = call(
        &mut c,
        4,
        "tools/call",
        json!({"name":"alayasiki_query","arguments":{"query":"Rust startup"}}),
    )
    .await;
    let result: Value =
        serde_json::from_str(query["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert!(!result["evidence"]["nodes"].as_array().unwrap().is_empty());
    assert_eq!(call(&mut c,5,"tools/call",json!({"name":"alayasiki_session_write","arguments":{"session_id":"reasoning","content":"Private thought"}})).await["result"]["isError"],false);
    assert_eq!(
        call(
            &mut c,
            6,
            "tools/call",
            json!({"name":"alayasiki_session_read","arguments":{"session_id":"reasoning"}})
        )
        .await["result"]["isError"],
        false
    );
    assert_eq!(
        call(&mut c, 7, "resources/list", json!({})).await["result"]["resources"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        call(&mut c, 8, "prompts/list", json!({})).await["result"]["prompts"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let mut foreign = Connection::new(runtime, Principal::new("other", "a").with_roles(["admin"]));
    call(&mut foreign,1,"initialize",json!({"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1"}})).await;
    foreign
        .dispatch(json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
        .await;
    assert_eq!(
        call(
            &mut foreign,
            2,
            "tools/call",
            json!({"name":"alayasiki_session_read","arguments":{"session_id":"reasoning"}})
        )
        .await["result"]["isError"],
        true
    );
}

#[tokio::test]
async fn protocol_errors_resources_prompts_and_tenant_summaries() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(RuntimeBuilder::new(dir.path()).start().await.unwrap());
    let mut c = Connection::new(
        runtime.clone(),
        Principal::new("agent", "a").with_roles(["admin"]),
    );
    assert_eq!(
        c.dispatch(json!([])).await.unwrap()["error"]["code"],
        -32600
    );
    assert_eq!(
        call(&mut c, 1, "initialize", json!({})).await["error"]["code"],
        -32602
    );
    assert_eq!(
        call(&mut c, 2, "ping", json!({})).await["result"],
        json!({})
    );
    call(&mut c,3,"initialize",json!({"protocolVersion":"2099-01-01","capabilities":{},"clientInfo":{"name":"test","version":"1"}})).await;
    c.dispatch(json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
        .await;
    assert_eq!(
        call(&mut c, 4, "unknown", json!({})).await["error"]["code"],
        -32601
    );
    assert_eq!(
        call(&mut c, 5, "tools/call", json!({"name":"unknown"})).await["error"]["code"],
        -32602
    );
    assert_eq!(
        call(
            &mut c,
            6,
            "tools/call",
            json!({"name":"alayasiki_ingest","arguments":{"content":42}})
        )
        .await["result"]["isError"],
        true
    );
    call(&mut c,7,"tools/call",json!({"name":"alayasiki_ingest","arguments":{"content":"Private startup company Rust", "metadata":{"retention_until_unix":"1"}}})).await;
    runtime
        .wait_for_jobs(std::time::Duration::from_secs(5))
        .await
        .unwrap();
    let resource = call(
        &mut c,
        8,
        "resources/read",
        json!({"uri":"alayasiki://communities"}),
    )
    .await;
    assert!(!resource.to_string().contains("Private"));
    assert_eq!(
        call(&mut c, 9, "resources/templates/list", json!({})).await["result"]["resourceTemplates"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        call(
            &mut c,
            10,
            "resources/read",
            json!({"uri":"alayasiki://sessions/missing"})
        )
        .await["error"]["code"],
        -32602
    );
    let prompt = call(
        &mut c,
        11,
        "prompts/get",
        json!({"name":"alayasiki_reasoning","arguments":{"question":"How does Rust work?"}}),
    )
    .await;
    assert!(prompt.to_string().contains("How does Rust work?"));
    assert_eq!(
        call(&mut c, 12, "prompts/get", json!({"name":"invalid"})).await["error"]["code"],
        -32602
    );
    assert_eq!(
        call(&mut c, 13, "initialize", json!({})).await["error"]["code"],
        -32600
    );
}

#[tokio::test]
async fn shared_session_quota_prevents_mutation_after_reconnect() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(
        RuntimeBuilder::new(dir.path())
            .extraction_enabled(false)
            .start()
            .await
            .unwrap(),
    );
    let budget = alayasiki_mcp::SessionBudget::new(1, 1, 1024);
    for (attempt, content) in [(0, "first memory"), (1, "second memory")] {
        let mut c = Connection::new(
            runtime.clone(),
            Principal::new("agent", "a").with_roles(["admin"]),
        )
        .with_session_budget(budget.clone());
        call(&mut c,1,"initialize",json!({"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1"}})).await;
        c.dispatch(json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
            .await;
        let result=call(&mut c,2,"tools/call",json!({"name":"alayasiki_session_write","arguments":{"session_id":"bounded","content":content}})).await;
        assert_eq!(result["result"]["isError"], attempt == 1);
    }
    let owner = storage::session::SessionOwner::new("a", "agent");
    let session = runtime
        .repository()
        .get_session_with_owner("bounded", Some(&owner))
        .unwrap()
        .unwrap();
    assert_eq!(session.nodes.len(), 1);
    assert_eq!(session.nodes.values().next().unwrap().data, "first memory");
}

#[tokio::test]
async fn expired_session_memory_is_reclaimed_before_quota_reuse() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(
        RuntimeBuilder::new(dir.path())
            .extraction_enabled(false)
            .start()
            .await
            .unwrap(),
    );
    {
        let mut session = runtime
            .repository()
            .session_manager
            .get_or_create("expired");
        session.expires_at = std::time::SystemTime::UNIX_EPOCH;
    }
    assert_eq!(runtime.repository().session_manager.len(), 1);
    runtime.repository().cleanup_expired_sessions();
    assert_eq!(runtime.repository().session_manager.len(), 0);
}

#[tokio::test]
async fn failed_audit_after_session_mutation_does_not_refund_quota() {
    struct FailingAudit;
    impl alayasiki_core::audit::AuditSink for FailingAudit {
        fn record(
            &self,
            _: alayasiki_core::audit::AuditEvent,
        ) -> Result<(), alayasiki_core::audit::AuditError> {
            Err(alayasiki_core::audit::AuditError::LockPoisoned)
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(
        RuntimeBuilder::new(dir.path())
            .extraction_enabled(false)
            .with_ingestion_config(|mut pipeline| {
                pipeline.set_audit_sink(Arc::new(FailingAudit));
                pipeline
            })
            .start()
            .await
            .unwrap(),
    );
    let budget = alayasiki_mcp::SessionBudget::new(1, 1, 1024);
    let mut c = Connection::new(
        runtime.clone(),
        Principal::new("agent", "a").with_roles(["admin"]),
    )
    .with_session_budget(budget);
    call(&mut c,1,"initialize",json!({"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1"}})).await;
    c.dispatch(json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
        .await;
    for (id, content) in [
        (2, "first persisted thought"),
        (3, "must not persist second thought"),
    ] {
        assert_eq!(call(&mut c,id,"tools/call",json!({"name":"alayasiki_session_write","arguments":{"session_id":"audit-failure","content":content}})).await["result"]["isError"],true);
    }
    let owner = storage::session::SessionOwner::new("a", "agent");
    let session = runtime
        .repository()
        .get_session_with_owner("audit-failure", Some(&owner))
        .unwrap()
        .unwrap();
    assert_eq!(session.nodes.len(), 1);
    assert_eq!(
        session.nodes.values().next().unwrap().data,
        "first persisted thought"
    );
}
