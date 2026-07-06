use super::{Citation, EvidenceNode, ExclusionReason, ExpansionPath, InternalEdge, RankedNode};
use alayasiki_core::audit::{AuditEvent, AuditOperation, AuditOutcome};
use alayasiki_core::model::Node;
use chrono::NaiveDate;
use std::collections::{BTreeSet, HashMap, HashSet};

pub(super) fn node_belongs_to_tenant(node: &Node, tenant_scope: &str) -> bool {
    node.metadata
        .get("tenant")
        .map(|tenant| tenant == tenant_scope)
        .unwrap_or(false)
}

pub(super) fn node_lexical_text(node: &Node) -> String {
    format!(
        "{} {}",
        node.data,
        node.metadata
            .values()
            .cloned()
            .collect::<Vec<_>>()
            .join(" ")
    )
}

pub(super) fn tokenize(text: &str) -> HashSet<String> {
    let mut out = HashSet::new();
    let mut buffer = String::new();

    for ch in text.chars().flat_map(|ch| ch.to_lowercase()) {
        if ch.is_alphanumeric() || ch == '_' {
            buffer.push(ch);
        } else if !buffer.is_empty() {
            out.insert(buffer.clone());
            buffer.clear();
        }
    }

    if !buffer.is_empty() {
        out.insert(buffer);
    }

    let unicode_tokens: Vec<String> = out
        .iter()
        .filter(|token| !token.is_ascii())
        .cloned()
        .collect();
    for token in unicode_tokens {
        for ngram in char_ngrams(&token, super::UNICODE_NGRAM_SIZE) {
            out.insert(ngram);
        }
    }

    out
}

fn char_ngrams(token: &str, n: usize) -> Vec<String> {
    let chars: Vec<char> = token.chars().collect();
    if chars.is_empty() || n == 0 {
        return Vec::new();
    }
    if chars.len() <= n {
        return vec![token.to_string()];
    }

    chars
        .windows(n)
        .map(|window| window.iter().collect::<String>())
        .collect()
}

pub(super) fn lexical_similarity(a: &HashSet<String>, b: &HashSet<String>) -> f32 {
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }

    let intersection = a.intersection(b).count() as f32;
    let denominator = a.len().max(b.len()) as f32;
    intersection / denominator
}

pub(super) fn collect_relation_filter(request: &super::QueryRequest) -> HashSet<&str> {
    request
        .filters
        .relation_type
        .iter()
        .map(|value| value.as_str())
        .chain(
            request
                .traversal
                .relation_types
                .iter()
                .map(|value| value.as_str()),
        )
        .collect()
}

pub(super) fn relation_is_allowed(relation: &str, relation_filter: &HashSet<&str>) -> bool {
    relation_filter.is_empty() || relation_filter.contains(relation)
}

pub(super) fn reconstruct_path(
    anchor_id: u64,
    target_id: u64,
    parents: &HashMap<u64, u64>,
) -> Option<Vec<u64>> {
    let mut path = vec![target_id];
    let mut current = target_id;

    while current != anchor_id {
        let parent = parents.get(&current)?;
        current = *parent;
        path.push(current);
    }

    path.reverse();
    Some(path)
}

pub(super) fn parse_time_range(
    request: &super::QueryRequest,
) -> Result<Option<(NaiveDate, NaiveDate)>, super::QueryError> {
    let Some(range) = &request.filters.time_range else {
        return Ok(None);
    };

    let from = NaiveDate::parse_from_str(&range.from, "%Y-%m-%d").map_err(|_| {
        super::QueryError::InvalidQuery("filters.time_range.from/to must be YYYY-MM-DD".to_string())
    })?;
    let to = NaiveDate::parse_from_str(&range.to, "%Y-%m-%d").map_err(|_| {
        super::QueryError::InvalidQuery("filters.time_range.from/to must be YYYY-MM-DD".to_string())
    })?;

    Ok(Some((from, to)))
}

pub(super) fn node_filter_exclusion_reason(
    node: &Node,
    entity_filter: &HashSet<&str>,
    time_range: Option<(NaiveDate, NaiveDate)>,
    retention_cutoff_unix: Option<u64>,
    tenant_scope: Option<&str>,
) -> Option<String> {
    if let Some(tenant_scope) = tenant_scope {
        if !node_belongs_to_tenant(node, tenant_scope) {
            return Some("tenant_filtered".to_string());
        }
    }

    if let Some(now_unix) = retention_cutoff_unix {
        if node_is_retention_expired(node, now_unix) {
            return Some("retention_expired".to_string());
        }
    }

    if !entity_filter.is_empty() {
        let entity_type = node.metadata.get("entity_type").map(|value| value.as_str());
        if entity_type
            .map(|value| !entity_filter.contains(value))
            .unwrap_or(true)
        {
            return Some("entity_type_filtered".to_string());
        }
    }

    if let Some((from, to)) = time_range {
        let timestamp = node
            .metadata
            .get("timestamp")
            .and_then(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d").ok());
        match timestamp {
            Some(value) if value >= from && value <= to => {}
            _ => return Some("time_range_filtered".to_string()),
        }
    }

    None
}

pub(super) fn node_passes_filters(
    node: &Node,
    entity_filter: &HashSet<&str>,
    time_range: Option<(NaiveDate, NaiveDate)>,
    retention_cutoff_unix: Option<u64>,
    tenant_scope: Option<&str>,
) -> bool {
    node_filter_exclusion_reason(
        node,
        entity_filter,
        time_range,
        retention_cutoff_unix,
        tenant_scope,
    )
    .is_none()
}

fn node_is_retention_expired(node: &Node, now_unix: u64) -> bool {
    node.metadata
        .get("retention_until_unix")
        .and_then(|raw| raw.parse::<u64>().ok())
        .is_some_and(|deadline| now_unix >= deadline)
}

fn current_unix_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(super) fn retention_cutoff_unix(request: &super::QueryRequest) -> Option<u64> {
    if request.snapshot_id.is_some() || request.time_travel.is_some() {
        None
    } else {
        Some(current_unix_timestamp())
    }
}

pub(super) fn effective_query_model_id(request: &super::QueryRequest) -> String {
    request
        .model_id
        .clone()
        .unwrap_or_else(|| super::DEFAULT_EMBEDDING_MODEL_ID.to_string())
}

pub(super) fn build_query_audit_event(
    outcome: AuditOutcome,
    model_id: &str,
    actor: Option<String>,
    tenant: Option<String>,
    snapshot_id: Option<String>,
    error: Option<String>,
) -> AuditEvent {
    let mut event = AuditEvent::new(AuditOperation::Query, outcome);
    event.model_id = Some(model_id.to_string());
    event.actor = actor;
    event.tenant = tenant;
    event.snapshot_id = snapshot_id;
    if let Some(error) = error {
        event.metadata.insert("error".to_string(), error);
    }
    event
}

pub(super) fn generate_answer(query: &str, nodes: &[EvidenceNode]) -> String {
    if nodes.is_empty() {
        return format!("No evidence found for query: {query}");
    }

    let snippets = nodes
        .iter()
        .take(3)
        .map(|node| node.data.as_str())
        .collect::<Vec<_>>()
        .join(" | ");

    format!(
        "Answer synthesized from {} evidence nodes: {}",
        nodes.len(),
        snippets
    )
}

pub(super) fn build_citations(nodes: &[RankedNode]) -> Vec<Citation> {
    let mut unique_sources = BTreeSet::new();
    let mut out = Vec::new();

    for node in nodes {
        let Some(source) = &node.source else {
            continue;
        };
        if !unique_sources.insert(source.clone()) {
            continue;
        }

        let end = node.data.len();
        out.push(Citation {
            source: source.clone(),
            span: [0, end],
            node_id: node.id,
            confidence: node.confidence,
        });
    }

    out
}

pub(super) fn dedup_edges(edges: Vec<InternalEdge>) -> Vec<InternalEdge> {
    let mut map: HashMap<(u64, u64, String), InternalEdge> = HashMap::new();
    for edge in edges {
        let key = (edge.source, edge.target, edge.relation.clone());
        map.insert(key, edge);
    }
    let mut out: Vec<InternalEdge> = map.into_values().collect();
    out.sort_by(|a, b| {
        a.source
            .cmp(&b.source)
            .then(a.target.cmp(&b.target))
            .then(a.relation.cmp(&b.relation))
    });
    out
}

pub(super) fn dedup_paths(mut paths: Vec<ExpansionPath>) -> Vec<ExpansionPath> {
    let mut seen = BTreeSet::new();
    paths.retain(|path| seen.insert((path.anchor_id, path.target_id, path.path.clone())));
    paths.sort_by(|a, b| {
        a.anchor_id
            .cmp(&b.anchor_id)
            .then(a.target_id.cmp(&b.target_id))
    });
    paths
}

pub(super) fn dedup_exclusions(mut exclusions: Vec<ExclusionReason>) -> Vec<ExclusionReason> {
    let mut seen = BTreeSet::new();
    exclusions.retain(|exclusion| seen.insert((exclusion.node_id, exclusion.reason.clone())));
    exclusions.sort_by(|a, b| a.node_id.cmp(&b.node_id).then(a.reason.cmp(&b.reason)));
    exclusions
}
