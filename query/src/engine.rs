use crate::dsl::{QueryMode, QueryRequest, SearchMode};
use crate::planner::{QueryPlan, QueryPlanner};
use alayasiki_core::embedding::deterministic_embedding;
use alayasiki_core::model::Node;
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use storage::repo::{RepoError, Repository};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvidenceNode {
    pub id: u64,
    pub data: String,
    pub score: f32,
    pub hop: u8,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvidenceEdge {
    pub source: u64,
    pub target: u64,
    pub relation: String,
    pub weight: f32,
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
}

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("invalid query: {0}")]
    InvalidQuery(String),
    #[error("repository error: {0}")]
    Repository(#[from] RepoError),
}

pub struct QueryEngine {
    repo: Arc<Repository>,
}

const DEFAULT_EMBEDDING_MODEL_ID: &str = "embedding-default-v1";
const UNICODE_NGRAM_SIZE: usize = 2;

#[derive(Debug, Clone)]
struct RankedNode {
    id: u64,
    data: String,
    score: f32,
    hop: u8,
    source: Option<String>,
}

#[derive(Debug, Clone)]
struct ExecutionState {
    anchors: Vec<Anchor>,
    expansion_paths: Vec<ExpansionPath>,
    exclusions: Vec<ExclusionReason>,
    nodes: Vec<RankedNode>,
    edges: Vec<EvidenceEdge>,
}

impl QueryEngine {
    pub fn new(repo: Arc<Repository>) -> Self {
        Self { repo }
    }

    pub async fn execute_json(&self, raw: &str) -> Result<QueryResponse, QueryError> {
        let request = QueryRequest::parse_json(raw)
            .map_err(|err| QueryError::InvalidQuery(err.to_string()))?;
        self.execute(request).await
    }

    pub async fn execute(&self, request: QueryRequest) -> Result<QueryResponse, QueryError> {
        request
            .validate()
            .map_err(|err| QueryError::InvalidQuery(err.to_string()))?;

        let effective_model_id = request
            .model_id
            .clone()
            .unwrap_or_else(|| DEFAULT_EMBEDDING_MODEL_ID.to_string());

        let mut plan = QueryPlanner::plan(&request);
        let mut state = self
            .execute_with_plan(&request, &plan, &effective_model_id)
            .await?;

        if request.search_mode == SearchMode::Auto
            && plan.effective_search_mode == SearchMode::Local
            && state.nodes.len() < 2
        {
            let mut drift_plan = plan.clone();
            drift_plan.effective_search_mode = SearchMode::Drift;
            drift_plan.expansion_depth = drift_plan.expansion_depth.saturating_add(1).min(8);

            let mut drift_state = self
                .execute_with_plan(&request, &drift_plan, &effective_model_id)
                .await?;
            drift_state.exclusions.push(ExclusionReason {
                node_id: None,
                reason: "auto_fallback_to_drift_due_to_insufficient_evidence".to_string(),
            });

            plan = drift_plan;
            state = drift_state;
        }

        let evidence_nodes: Vec<EvidenceNode> = state
            .nodes
            .iter()
            .map(|node| EvidenceNode {
                id: node.id,
                data: node.data.clone(),
                score: node.score,
                hop: node.hop,
            })
            .collect();

        let citations = build_citations(&state.nodes);
        let groundedness = if evidence_nodes.is_empty() {
            0.0
        } else {
            (evidence_nodes.len() as f32 / request.top_k as f32).min(1.0)
        };

        let answer = match request.mode {
            QueryMode::Evidence => None,
            QueryMode::Answer => Some(generate_answer(&request.query, &evidence_nodes)),
        };

        let snapshot_id = match request.snapshot_id.clone() {
            Some(snapshot_id) => Some(snapshot_id),
            None => Some(self.repo.current_snapshot_id().await),
        };

        Ok(QueryResponse {
            answer,
            evidence: EvidenceSubgraph {
                nodes: evidence_nodes,
                edges: state.edges,
            },
            citations,
            groundedness,
            explain: ExplainPlan {
                steps: plan.steps.iter().map(|step| step.to_string()).collect(),
                effective_search_mode: plan.effective_search_mode,
                anchors: state.anchors,
                expansion_paths: state.expansion_paths,
                exclusions: state.exclusions,
            },
            model_id: Some(effective_model_id),
            snapshot_id,
        })
    }

    async fn execute_with_plan(
        &self,
        request: &QueryRequest,
        plan: &QueryPlan,
        embedding_model_id: &str,
    ) -> Result<ExecutionState, QueryError> {
        let mut vector_hits = self
            .collect_vector_scores(request, plan, embedding_model_id)
            .await;
        if vector_hits.is_empty() {
            if let Some(node_id) = self.repo.list_node_ids().await.into_iter().next() {
                vector_hits.push((node_id, 0.0));
            }
        }

        if vector_hits.is_empty() {
            return Ok(ExecutionState {
                anchors: Vec::new(),
                expansion_paths: Vec::new(),
                exclusions: vec![ExclusionReason {
                    node_id: None,
                    reason: "no_nodes_available".to_string(),
                }],
                nodes: Vec::new(),
                edges: Vec::new(),
            });
        }

        let anchor_limit = plan.vector_top_k.min(vector_hits.len()).max(1);
        let mut anchors: Vec<Anchor> = vector_hits
            .iter()
            .take(anchor_limit)
            .map(|(node_id, score)| Anchor {
                node_id: *node_id,
                score: *score,
            })
            .collect();

        let relation_filter = collect_relation_filter(request);
        let mut candidate_hops: HashMap<u64, u8> = HashMap::new();
        let mut expansion_paths = Vec::new();
        let mut exclusions = Vec::new();
        let mut traversed_edges = Vec::new();

        {
            let index = self.repo.hyper_index.read().await;

            for anchor in &anchors {
                candidate_hops.entry(anchor.node_id).or_insert(0);

                let mut queue = VecDeque::new();
                let mut visited: HashMap<u64, u8> = HashMap::new();
                let mut parents: HashMap<u64, u64> = HashMap::new();

                queue.push_back(anchor.node_id);
                visited.insert(anchor.node_id, 0);

                while let Some(current_id) = queue.pop_front() {
                    let current_hop = *visited.get(&current_id).unwrap_or(&0);
                    if current_hop >= plan.expansion_depth {
                        continue;
                    }

                    for (target, relation, weight) in index.graph_index.neighbors(current_id) {
                        if !relation_is_allowed(relation.as_str(), &relation_filter) {
                            exclusions.push(ExclusionReason {
                                node_id: Some(*target),
                                reason: format!("relation_filtered:{}", relation),
                            });
                            continue;
                        }

                        traversed_edges.push(EvidenceEdge {
                            source: current_id,
                            target: *target,
                            relation: relation.clone(),
                            weight: *weight,
                        });

                        let next_hop = current_hop + 1;
                        let should_visit = visited
                            .get(target)
                            .map(|prev_hop| next_hop < *prev_hop)
                            .unwrap_or(true);

                        if should_visit {
                            visited.insert(*target, next_hop);
                            parents.insert(*target, current_id);
                            queue.push_back(*target);
                            candidate_hops
                                .entry(*target)
                                .and_modify(|hop| *hop = (*hop).min(next_hop))
                                .or_insert(next_hop);

                            if let Some(path) = reconstruct_path(anchor.node_id, *target, &parents)
                            {
                                expansion_paths.push(ExpansionPath {
                                    anchor_id: anchor.node_id,
                                    target_id: *target,
                                    path,
                                });
                            }
                        }
                    }
                }
            }
        }

        let candidate_ids: Vec<u64> = candidate_hops.keys().copied().collect();
        let fetched_nodes = self.repo.get_nodes_by_ids(&candidate_ids).await;
        let node_lookup: HashMap<u64, Node> = fetched_nodes
            .into_iter()
            .map(|node| (node.id, node))
            .collect();

        let anchor_scores: HashMap<u64, f32> = anchors
            .iter()
            .map(|anchor| (anchor.node_id, anchor.score))
            .collect();
        let query_tokens = tokenize(&request.query);
        let time_range = parse_time_range(request)?;
        let entity_filter: HashSet<&str> = request
            .filters
            .entity_type
            .iter()
            .map(|value| value.as_str())
            .collect();

        let mut ranked_nodes = Vec::new();
        for (node_id, hop) in candidate_hops {
            let Some(node) = node_lookup.get(&node_id) else {
                exclusions.push(ExclusionReason {
                    node_id: Some(node_id),
                    reason: "missing_node".to_string(),
                });
                continue;
            };

            if !entity_filter.is_empty() {
                let entity_type = node.metadata.get("entity_type").map(|value| value.as_str());
                if entity_type
                    .map(|value| !entity_filter.contains(value))
                    .unwrap_or(true)
                {
                    exclusions.push(ExclusionReason {
                        node_id: Some(node_id),
                        reason: "entity_type_filtered".to_string(),
                    });
                    continue;
                }
            }

            if let Some((from, to)) = time_range {
                let timestamp = node
                    .metadata
                    .get("timestamp")
                    .and_then(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d").ok());
                match timestamp {
                    Some(value) if value >= from && value <= to => {}
                    _ => {
                        exclusions.push(ExclusionReason {
                            node_id: Some(node_id),
                            reason: "time_range_filtered".to_string(),
                        });
                        continue;
                    }
                }
            }

            let lexical_score =
                lexical_similarity(&query_tokens, &tokenize(&node_lexical_text(node)));
            let anchor_score = anchor_scores.get(&node_id).copied().unwrap_or(0.0);
            let base_score = ((anchor_score * 0.8) + (lexical_score * 0.2))
                .max(lexical_score)
                .max(0.01);
            let score = base_score / (hop as f32 + 1.0);

            ranked_nodes.push(RankedNode {
                id: node_id,
                data: node.data.clone(),
                score,
                hop,
                source: node.metadata.get("source").cloned(),
            });
        }

        ranked_nodes.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(Ordering::Equal)
                .then(a.id.cmp(&b.id))
        });

        if ranked_nodes.len() > request.top_k {
            let pruned = ranked_nodes.split_off(request.top_k);
            for node in pruned {
                exclusions.push(ExclusionReason {
                    node_id: Some(node.id),
                    reason: "pruned_by_top_k".to_string(),
                });
            }
        }

        let selected_ids: HashSet<u64> = ranked_nodes.iter().map(|node| node.id).collect();

        let mut edges: Vec<EvidenceEdge> = traversed_edges
            .into_iter()
            .filter(|edge| {
                selected_ids.contains(&edge.source) && selected_ids.contains(&edge.target)
            })
            .filter(|edge| relation_is_allowed(edge.relation.as_str(), &relation_filter))
            .collect();

        edges = dedup_edges(edges);
        expansion_paths = dedup_paths(expansion_paths);
        exclusions = dedup_exclusions(exclusions);
        anchors.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(Ordering::Equal)
                .then(a.node_id.cmp(&b.node_id))
        });

        Ok(ExecutionState {
            anchors,
            expansion_paths,
            exclusions,
            nodes: ranked_nodes,
            edges,
        })
    }

    async fn collect_vector_scores(
        &self,
        request: &QueryRequest,
        plan: &QueryPlan,
        embedding_model_id: &str,
    ) -> Vec<(u64, f32)> {
        let Some(embedding_dim) = self.repo.embedding_dimension().await else {
            return Vec::new();
        };

        let query_embedding =
            deterministic_embedding(&request.query, embedding_model_id, embedding_dim);
        let vector_limit = match plan.effective_search_mode {
            SearchMode::Global => plan.vector_top_k.saturating_mul(2),
            _ => plan.vector_top_k,
        }
        .max(1);

        let index = self.repo.hyper_index.read().await;
        index.search_vector(&query_embedding, vector_limit)
    }
}

fn node_lexical_text(node: &Node) -> String {
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

fn tokenize(text: &str) -> HashSet<String> {
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
        for ngram in char_ngrams(&token, UNICODE_NGRAM_SIZE) {
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

fn lexical_similarity(a: &HashSet<String>, b: &HashSet<String>) -> f32 {
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }

    let intersection = a.intersection(b).count() as f32;
    let denominator = a.len().max(b.len()) as f32;
    intersection / denominator
}

fn collect_relation_filter(request: &QueryRequest) -> HashSet<&str> {
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

fn relation_is_allowed(relation: &str, relation_filter: &HashSet<&str>) -> bool {
    relation_filter.is_empty() || relation_filter.contains(relation)
}

fn reconstruct_path(
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

fn parse_time_range(request: &QueryRequest) -> Result<Option<(NaiveDate, NaiveDate)>, QueryError> {
    let Some(range) = &request.filters.time_range else {
        return Ok(None);
    };

    let from = NaiveDate::parse_from_str(&range.from, "%Y-%m-%d").map_err(|_| {
        QueryError::InvalidQuery("filters.time_range.from/to must be YYYY-MM-DD".to_string())
    })?;
    let to = NaiveDate::parse_from_str(&range.to, "%Y-%m-%d").map_err(|_| {
        QueryError::InvalidQuery("filters.time_range.from/to must be YYYY-MM-DD".to_string())
    })?;

    Ok(Some((from, to)))
}

fn generate_answer(query: &str, nodes: &[EvidenceNode]) -> String {
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

fn build_citations(nodes: &[RankedNode]) -> Vec<Citation> {
    let mut unique_sources = BTreeSet::new();
    let mut out = Vec::new();

    for node in nodes {
        let Some(source) = &node.source else {
            continue;
        };
        if !unique_sources.insert(source.clone()) {
            continue;
        }

        let end = node.data.len().min(80);
        out.push(Citation {
            source: source.clone(),
            span: [0, end],
        });
    }

    out
}

fn dedup_edges(mut edges: Vec<EvidenceEdge>) -> Vec<EvidenceEdge> {
    let mut seen = BTreeSet::new();
    edges.retain(|edge| seen.insert((edge.source, edge.target, edge.relation.clone())));
    edges.sort_by(|a, b| {
        a.source
            .cmp(&b.source)
            .then(a.target.cmp(&b.target))
            .then(a.relation.cmp(&b.relation))
    });
    edges
}

fn dedup_paths(mut paths: Vec<ExpansionPath>) -> Vec<ExpansionPath> {
    let mut seen = BTreeSet::new();
    paths.retain(|path| seen.insert((path.anchor_id, path.target_id, path.path.clone())));
    paths.sort_by(|a, b| {
        a.anchor_id
            .cmp(&b.anchor_id)
            .then(a.target_id.cmp(&b.target_id))
    });
    paths
}

fn dedup_exclusions(mut exclusions: Vec<ExclusionReason>) -> Vec<ExclusionReason> {
    let mut seen = BTreeSet::new();
    exclusions.retain(|exclusion| seen.insert((exclusion.node_id, exclusion.reason.clone())));
    exclusions.sort_by(|a, b| a.node_id.cmp(&b.node_id).then(a.reason.cmp(&b.reason)));
    exclusions
}
