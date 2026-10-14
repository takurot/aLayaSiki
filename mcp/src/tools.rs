use crate::Connection;
use alayasiki_core::{
    auth::{Action, Authorizer, ResourceContext},
    ingest::IngestionRequest,
};
use serde_json::{json, Value};
use storage::session::SessionOwner;

pub fn definitions() -> Vec<Value> {
    let request = request_schema();
    let query = json!({"type":"object","properties":{"query":{"type":"string","minLength":1},"search_mode":{"enum":["local","global","drift","auto"]},"mode":{"enum":["evidence","answer"]},"top_k":{"type":"integer","minimum":1,"maximum":1000},"filters":{"type":"object"},"traversal":{"type":"object"},"model_id":{"type":"string"},"snapshot_id":{"type":"string"},"session_id":{"type":"string"},"time_travel":{"type":"string"}},"required":["query"]});
    let ingest = json!({"type":"object","properties":{"content":{"type":"string","minLength":1},"metadata":{"type":"object","additionalProperties":{"type":"string"}},"idempotency_key":{"type":"string"},"request":request},"anyOf":[{"required":["content"]},{"required":["request"]}]});
    vec![
        json!({"name":"alayasiki_query","description":"Query GraphRAG using the full query DSL, returning evidence and provenance citations","inputSchema":query}),
        json!({"name":"alayasiki_ingest","description":"Ingest text, Markdown, or canonical document payloads; use idempotency_key when retrying","inputSchema":ingest}),
        json!({"name":"alayasiki_session_read","description":"Read your owned TTL-backed session graph","inputSchema":{"type":"object","properties":{"session_id":{"type":"string","minLength":1,"maxLength":256}},"required":["session_id"]}}),
        json!({"name":"alayasiki_session_write","description":"Write bounded text, Markdown, or media hints to owned working memory (64 KiB content, 4 KiB metadata); use ingest for PDFs","inputSchema":{"type":"object","properties":{"session_id":{"type":"string","minLength":1,"maxLength":256},"content":{"type":"string","minLength":1},"request":request,"metadata":{"type":"object","additionalProperties":{"type":"string"}}},"required":["session_id"],"anyOf":[{"required":["content"]},{"required":["request"]}]}}),
    ]
}
impl Connection {
    pub(crate) async fn call_tool(&self, params: &Value) -> Result<Value, (i64, &'static str)> {
        let name = params["name"]
            .as_str()
            .ok_or((-32602, "Tool name required"))?;
        if !definitions().iter().any(|tool| tool["name"] == name) {
            return Err((-32602, "Unknown tool"));
        }
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        if !arguments.is_object() {
            return Err((-32602, "Invalid tool arguments"));
        }
        let result = self.execute_tool(name, &arguments).await;
        Ok(match result {
            Ok(value) => {
                json!({"content":[{"type":"text","text":value.to_string()}],"isError":false})
            }
            Err(message) => json!({"content":[{"type":"text","text":message}],"isError":true}),
        })
    }
    async fn execute_tool(&self, name: &str, args: &Value) -> Result<Value, &'static str> {
        let authorizer = Authorizer::default();
        let resource = ResourceContext::new(self.principal.tenant.clone());
        match name {
            "alayasiki_query" => {
                let request =
                    serde_json::from_value(args.clone()).map_err(|_| "Invalid query arguments")?;
                let response = self
                    .runtime
                    .query()
                    .execute_authorized(request, &self.principal, &authorizer, &resource)
                    .await
                    .map_err(|_| "Query failed or permission denied")?;
                serde_json::to_value(response).map_err(|_| "Response encoding failed")
            }
            "alayasiki_ingest" | "alayasiki_session_write" => {
                let request = ingest_request(args)?;
                let ids = if name == "alayasiki_session_write" {
                    return self
                        .write_session(session_id(args)?, request, &authorizer, &resource)
                        .await;
                } else {
                    self.runtime
                        .ingestion()
                        .ingest_authorized(request, &self.principal, &authorizer, &resource)
                        .await
                }
                .map_err(|_| "Ingestion failed or permission denied")?;
                Ok(
                    json!({"node_ids":ids,"snapshot_id":self.runtime.repository().current_snapshot_id().await}),
                )
            }
            "alayasiki_session_read" => {
                authorizer
                    .authorize(&self.principal, Action::Query, &resource)
                    .map_err(|_| {
                        self.record_denial("alayasiki_session_read")
                            .err()
                            .unwrap_or("Permission denied")
                    })?;
                self.session_graph(session_id(args)?)
            }
            _ => Err("Unknown tool"),
        }
    }
    async fn write_session(
        &self,
        session: &str,
        request: IngestionRequest,
        authorizer: &Authorizer,
        resource: &ResourceContext,
    ) -> Result<Value, &'static str> {
        let cost = session_cost(&request)?;
        let reservation = self.budget.reserve(
            self.runtime.repository(),
            (
                self.principal.tenant.clone(),
                self.principal.subject.clone(),
                session.to_string(),
            ),
            cost,
        )?;
        let result = self
            .runtime
            .ingestion()
            .ingest_to_session_authorized(session, request, &self.principal, authorizer, resource)
            .await;
        let owner = SessionOwner::new(
            self.principal.tenant.clone(),
            self.principal.subject.clone(),
        );
        // Strict audit failures can happen after mutations. A retained owned
        // graph means this attempt remains charged even if its result is Err.
        // This intentionally may overcharge failures against existing graphs.
        match self
            .runtime
            .repository()
            .session_expiry_with_owner(session, Some(&owner))
        {
            Ok(Some(expires)) => reservation.commit(expires)?,
            Ok(None) | Err(storage::repo::RepoError::SessionAccessDenied(_)) => {}
            Err(_) => reservation
                .commit(std::time::SystemTime::now() + std::time::Duration::from_secs(1800))?,
        }
        let ids = result.map_err(|_| "Ingestion failed or permission denied")?;
        Ok(
            json!({"node_ids":ids,"snapshot_id":self.runtime.repository().current_snapshot_id().await}),
        )
    }

    fn session_graph(&self, id: &str) -> Result<Value, &'static str> {
        let owner = SessionOwner::new(
            self.principal.tenant.clone(),
            self.principal.subject.clone(),
        );
        let session = self
            .runtime
            .repository()
            .get_session_with_owner(id, Some(&owner))
            .map_err(|_| {
                self.record_denial("alayasiki_session_read")
                    .err()
                    .unwrap_or("Session access denied")
            })?
            .ok_or("Session not found or expired")?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut nodes: Vec<_> = session
            .nodes
            .into_values()
            .filter(|node| {
                node.metadata
                    .get("retention_until_unix")
                    .and_then(|value| value.parse::<u64>().ok())
                    .is_none_or(|deadline| now < deadline)
            })
            .collect();
        nodes.sort_by_key(|node| node.id);
        let ids: std::collections::HashSet<_> = nodes.iter().map(|node| node.id).collect();
        let mut edges: Vec<_> = session
            .edges
            .into_iter()
            .filter(|edge| ids.contains(&edge.source) && ids.contains(&edge.target))
            .collect();
        edges.sort_by(|a, b| {
            (a.source, a.target, &a.relation).cmp(&(b.source, b.target, &b.relation))
        });
        let nodes: Vec<_> = nodes
            .into_iter()
            .map(|node| json!({"id":node.id,"data":node.data,"metadata":node.metadata}))
            .collect();
        let edges:Vec<_>=edges.into_iter().map(|edge|json!({"source":edge.source,"target":edge.target,"relation":edge.relation,"weight":edge.weight,"metadata":edge.metadata})).collect();
        Ok(json!({"session_id":id,"nodes":nodes,"edges":edges}))
    }
    pub(crate) async fn read_resource(&self, params: &Value) -> Result<Value, (i64, &'static str)> {
        let uri = params["uri"]
            .as_str()
            .ok_or((-32602, "Resource URI required"))?;
        Authorizer::default()
            .authorize(
                &self.principal,
                Action::Query,
                &ResourceContext::new(self.principal.tenant.clone()),
            )
            .map_err(|_| {
                (
                    -32602,
                    self.record_denial("resources/read")
                        .err()
                        .unwrap_or("Permission denied"),
                )
            })?;
        let value = if uri == "alayasiki://communities" {
            let summaries = self
                .runtime
                .refresh_communities_for_tenant(&self.principal.tenant)
                .await
                .map_err(|_| (-32603, "Resource unavailable"))?;
            let summaries:Vec<_>=summaries.into_iter().map(|summary|json!({"id":summary.community_id,"level":summary.level,"top_nodes":summary.top_nodes,"text":summary.summary})).collect();
            json!({"summaries":summaries})
        } else if let Some(id) = uri.strip_prefix("alayasiki://sessions/") {
            if id.is_empty() || id.len() > 256 {
                return Err((-32602, "Invalid session URI"));
            }
            self.session_graph(id)
                .map_err(|_| (-32602, "Session unavailable or permission denied"))?
        } else {
            return Err((-32602, "Unknown resource"));
        };
        Ok(json!({"contents":[{"uri":uri,"mimeType":"application/json","text":value.to_string()}]}))
    }
}
fn session_id(args: &Value) -> Result<&str, &'static str> {
    args["session_id"]
        .as_str()
        .filter(|id| !id.is_empty() && id.len() <= 256)
        .ok_or("Invalid session_id")
}
fn ingest_request(args: &Value) -> Result<IngestionRequest, &'static str> {
    if let Some(request) = args.get("request") {
        return serde_json::from_value(request.clone()).map_err(|_| "Invalid document request");
    }
    let content = args["content"]
        .as_str()
        .filter(|text| !text.trim().is_empty())
        .ok_or("Content required")?;
    let metadata =
        serde_json::from_value(args.get("metadata").cloned().unwrap_or_else(|| json!({})))
            .map_err(|_| "Invalid metadata")?;
    let mut request = IngestionRequest::text(content.into(), metadata);
    if let IngestionRequest::Text {
        idempotency_key, ..
    } = &mut request
    {
        if let Some(key) = args.get("idempotency_key") {
            *idempotency_key = Some(
                key.as_str()
                    .filter(|key| !key.is_empty())
                    .ok_or("Invalid idempotency_key")?
                    .into(),
            )
        }
    }
    Ok(request)
}

fn session_cost(request: &IngestionRequest) -> Result<usize, &'static str> {
    let (content_bytes, metadata) = match request {
        IngestionRequest::Text {
            content, metadata, ..
        } => (content.len(), metadata),
        IngestionRequest::File {
            content,
            metadata,
            mime_type,
            ..
        } => {
            // Working memory accepts bounded text/Markdown/JSON and metadata
            // media hints. Persisted ingest is the document/PDF entry point.
            let mime = mime_type
                .split(';')
                .next()
                .unwrap_or_default()
                .trim()
                .to_lowercase();
            if !(mime.starts_with("text/")
                || mime == "application/json"
                || mime.starts_with("image/")
                || mime.starts_with("audio/"))
            {
                return Err("Use alayasiki_ingest for documents; working memory requires text or media hints");
            }
            (content.len(), metadata)
        }
    };
    let metadata_bytes = serde_json::to_vec(metadata)
        .map_err(|_| "Invalid metadata")?
        .len();
    if content_bytes > 64 * 1024 || metadata_bytes > 4096 {
        return Err("Working memory write exceeds size limit");
    }
    serde_json::to_vec(request)
        .map(|bytes| bytes.len())
        .map_err(|_| "Invalid working memory payload")
}

fn request_schema() -> Value {
    let mut fields = json!({"content":{"type":"string","minLength":1},"metadata":{"type":"object","additionalProperties":{"type":"string"}},"idempotency_key":{"type":["string","null"]},"model_id":{"type":["string","null"]},"embedding_model_id":{"type":["string","null"]},"extraction_model_id":{"type":["string","null"]}});
    let text = json!({"type":"object","properties":fields,"required":["content","metadata"],"additionalProperties":false});
    fields["content"] =
        json!({"type":"array","items":{"type":"integer","minimum":0,"maximum":255}});
    fields["filename"] = json!({"type":"string"});
    fields["mime_type"] = json!({"type":"string"});
    let file = json!({"type":"object","properties":fields,"required":["filename","content","mime_type","metadata"],"additionalProperties":false});
    json!({"oneOf":[{"type":"object","properties":{"Text":text},"required":["Text"],"additionalProperties":false},{"type":"object","properties":{"File":file},"required":["File"],"additionalProperties":false}]})
}
