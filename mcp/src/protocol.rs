use alayasiki_core::{
    audit::{AuditEvent, AuditOperation, AuditOutcome, AuditSink},
    auth::Principal,
};
use alayasiki_runtime::AlayasikiRuntime;
use serde_json::{json, Value};
use std::sync::Arc;

pub struct Connection {
    pub(crate) runtime: Arc<AlayasikiRuntime>,
    pub(crate) principal: Principal,
    pub(crate) budget: Arc<crate::SessionBudget>,
    stage: u8,
    audit: Option<Arc<dyn AuditSink>>,
}
impl Connection {
    pub fn new(runtime: Arc<AlayasikiRuntime>, principal: Principal) -> Self {
        Self {
            runtime,
            principal,
            budget: Arc::new(crate::SessionBudget::default()),
            stage: 0,
            audit: None,
        }
    }
    pub fn with_session_budget(self, budget: Arc<crate::SessionBudget>) -> Self {
        Self { budget, ..self }
    }

    pub fn with_audit_sink(self, audit: Arc<dyn AuditSink>) -> Self {
        Self {
            audit: Some(audit),
            ..self
        }
    }
    pub(crate) fn record_denial(&self, endpoint: &str) -> Result<(), &'static str> {
        if let Some(sink) = &self.audit {
            let mut event = AuditEvent::new(AuditOperation::Query, AuditOutcome::Denied);
            event.actor = Some(self.principal.subject.clone());
            event.tenant = Some(self.principal.tenant.clone());
            event.metadata.insert("endpoint".into(), endpoint.into());
            sink.record(event).map_err(|_| "Audit unavailable")?;
        }
        Ok(())
    }
    pub async fn dispatch(&mut self, message: Value) -> Option<Value> {
        let id = message
            .get("id")
            .filter(|id| id.is_null() || id.is_string() || id.is_number())
            .cloned()
            .unwrap_or(Value::Null);
        let Some(object) = message.as_object() else {
            return Some(error(id, -32600, "Invalid request"));
        };
        if object.get("jsonrpc") != Some(&json!("2.0"))
            || !object.get("method").is_some_and(Value::is_string)
            || object
                .get("id")
                .is_some_and(|id| !id.is_null() && !id.is_string() && !id.is_number())
        {
            return Some(error(id, -32600, "Invalid request"));
        }
        let method = message["method"].as_str().unwrap();
        if !object.contains_key("id") {
            if method == "notifications/initialized" && self.stage == 1 {
                self.stage = 2;
            }
            return None;
        }
        let params = message.get("params").cloned().unwrap_or_else(|| json!({}));
        if !params.is_object() {
            return Some(error(id, -32602, "Invalid parameters"));
        }
        if method == "ping" {
            return Some(result(id, json!({})));
        }
        if method == "initialize" {
            if self.stage != 0 {
                return Some(error(id, -32600, "Already initialized"));
            }
            if !params["protocolVersion"].is_string()
                || !params["capabilities"].is_object()
                || !params["clientInfo"]["name"].is_string()
                || !params["clientInfo"]["version"].is_string()
            {
                return Some(error(id, -32602, "Invalid initialization parameters"));
            }
            self.stage = 1;
            return Some(result(
                id,
                json!({"protocolVersion":"2024-11-05","capabilities":{"tools":{},"resources":{},"prompts":{}},"serverInfo":{"name":"alayasiki-mcp","version":env!("CARGO_PKG_VERSION")}}),
            ));
        }
        if self.stage != 2 {
            return Some(error(id, -32600, "Connection is not initialized"));
        }
        let output = match method {
            "tools/list" => Ok(json!({"tools":crate::tools::definitions()})),
            "tools/call" => self.call_tool(&params).await,
            "resources/list" => Ok(
                json!({"resources":[{"uri":"alayasiki://communities","name":"Tenant community summaries","mimeType":"application/json"}]}),
            ),
            "resources/templates/list" => Ok(
                json!({"resourceTemplates":[{"uriTemplate":"alayasiki://sessions/{session_id}","name":"Owned session graph","mimeType":"application/json"}]}),
            ),
            "resources/read" => self.read_resource(&params).await,
            "prompts/list" => Ok(
                json!({"prompts":[{"name":"alayasiki_reasoning","description":"Ground reasoning in GraphRAG evidence","arguments":[{"name":"question","required":true}]}]}),
            ),
            "prompts/get" => {
                if params["name"] != "alayasiki_reasoning"
                    || !params["arguments"]["question"].is_string()
                {
                    Err((-32602, "Invalid prompt arguments"))
                } else {
                    Ok(
                        json!({"description":"Evidence-backed reasoning","messages":[{"role":"user","content":{"type":"text","text":format!("Use alayasiki_query to investigate {}. Cite returned evidence and provenance, identify missing support, and use session working memory for intermediate reasoning.",params["arguments"]["question"].as_str().unwrap())}}]}),
                    )
                }
            }
            _ => Err((-32601, "Method not found")),
        };
        Some(match output {
            Ok(value) => result(id, value),
            Err((code, message)) => error(id, code, message),
        })
    }
}
pub fn error(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}})
}
fn result(id: Value, value: Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"result":value})
}
