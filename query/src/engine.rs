use crate::dsl::{QueryMode, QueryRequest, SearchMode};
use crate::graphrag::{
    compute_groundedness, map_community_summaries, reduce_community_summaries, GroundednessInput,
    DRIFT_EVIDENCE_THRESHOLD, DRIFT_MAX_ITERATIONS,
};
use crate::planner::{QueryPlan, QueryPlanner};
use alayasiki_core::audit::{AuditEvent, AuditOperation, AuditOutcome, AuditSink};
use alayasiki_core::auth::{Action, Authorizer, AuthzError, Principal, ResourceContext};
use alayasiki_core::embedding::deterministic_embedding;
use alayasiki_core::model::Node;
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;
use storage::community::CommunitySummary;
use storage::repo::{RepoError, Repository};
use thiserror::Error;

/// Provenance metadata attached to evidence items.
/// Captures the data lineage: where it came from and how it was extracted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Provenance {
    /// Original source document (e.g. "s3://bucket/file.pdf")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Model that extracted/processed this data
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extraction_model_id: Option<String>,
    /// Snapshot when this data was ingested
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<String>,
    /// Timestamp of ingestion (RFC3339)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ingested_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvidenceNode {
    pub id: u64,
    pub data: String,
    pub score: f32,
    pub hop: u8,
    pub provenance: Provenance,
    pub confidence: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvidenceEdge {
    pub source: u64,
    pub target: u64,
    pub relation: String,
    pub weight: f32,
    pub provenance: Provenance,
    pub confidence: f32,
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
    pub node_id: u64,
    pub confidence: f32,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_travel: Option<String>,
    pub latency_ms: u64,
}

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("invalid query: {0}")]
    InvalidQuery(String),
    #[error("repository error: {0}")]
    Repository(#[from] RepoError),
    #[error("authorization error: {0}")]
    Unauthorized(#[from] AuthzError),
}

pub struct QueryEngine {
    repo: Arc<Repository>,
    community_summaries: Vec<CommunitySummary>,
    audit_sink: Option<Arc<dyn AuditSink>>,
}

const DEFAULT_EMBEDDING_MODEL_ID: &str = "embedding-default-v1";
const UNICODE_NGRAM_SIZE: usize = 2;

#[derive(Debug, Clone)]
pub struct RankedNode {
    pub id: u64,
    pub data: String,
    pub score: f32,
    pub hop: u8,
    pub source: Option<String>,
    pub extraction_model_id: Option<String>,
    pub node_snapshot_id: Option<String>,
    pub ingested_at: Option<String>,
    pub confidence: f32,
}

/// Internal edge representation during query execution (before final output).
#[derive(Debug, Clone)]
pub struct InternalEdge {
    pub source: u64,
    pub target: u64,
    pub relation: String,
    pub weight: f32,
    pub provenance: Provenance,
    pub confidence: f32,
}

#[derive(Debug, Clone)]
pub struct ExecutionState {
    pub anchors: Vec<Anchor>,
    pub expansion_paths: Vec<ExpansionPath>,
    pub exclusions: Vec<ExclusionReason>,
    pub nodes: Vec<RankedNode>,
    pub edges: Vec<InternalEdge>,
}

impl QueryEngine {
    pub fn new(repo: Arc<Repository>) -> Self {
        Self {
            repo,
            community_summaries: Vec::new(),
            audit_sink: None,
        }
    }

    /// Attach pre-computed community summaries for global search support.
    pub fn with_community_summaries(mut self, summaries: Vec<CommunitySummary>) -> Self {
        self.community_summaries = summaries;
        self
    }

    pub fn with_audit_sink(mut self, sink: Arc<dyn AuditSink>) -> Self {
        self.audit_sink = Some(sink);
        self
    }

    pub async fn execute_json(&self, raw: &str) -> Result<QueryResponse, QueryError> {
        let request = QueryRequest::parse_json(raw)
            .map_err(|err| QueryError::InvalidQuery(err.to_string()))?;
        self.execute(request).await
    }

    pub async fn execute_json_authorized(
        &self,
        raw: &str,
        principal: &Principal,
        authorizer: &Authorizer,
        resource: &ResourceContext,
    ) -> Result<QueryResponse, QueryError> {
        let request = QueryRequest::parse_json(raw)
            .map_err(|err| QueryError::InvalidQuery(err.to_string()))?;
        self.execute_authorized(request, principal, authorizer, resource)
            .await
    }

    pub async fn execute_authorized(
        &self,
        request: QueryRequest,
        principal: &Principal,
        authorizer: &Authorizer,
        resource: &ResourceContext,
    ) -> Result<QueryResponse, QueryError> {
        let model_id = effective_query_model_id(&request);
        if let Err(err) = authorizer.authorize(principal, Action::Query, resource) {
            self.emit_audit_event(build_query_audit_event(
                AuditOutcome::Denied,
                &model_id,
                Some(principal.subject.clone()),
                Some(principal.tenant.clone()),
                None,
                Some(err.to_string()),
            ));
            return Err(err.into());
        }

        self.execute_with_audit(
            request,
            Some(principal.subject.clone()),
            Some(principal.tenant.clone()),
        )
        .await
    }

    pub async fn execute(&self, request: QueryRequest) -> Result<QueryResponse, QueryError> {
        self.execute_with_audit(request, None, None).await
    }

    async fn execute_with_audit(
        &self,
        request: QueryRequest,
        actor: Option<String>,
        tenant: Option<String>,
    ) -> Result<QueryResponse, QueryError> {
        let model_id = effective_query_model_id(&request);
        let result = self.execute_internal(request).await;
        match &result {
            Ok(response) => {
                self.emit_audit_event(build_query_audit_event(
                    AuditOutcome::Succeeded,
                    &model_id,
                    actor,
                    tenant,
                    response.snapshot_id.clone(),
                    None,
                ));
            }
            Err(err) => {
                self.emit_audit_event(build_query_audit_event(
                    AuditOutcome::Failed,
                    &model_id,
                    actor,
                    tenant,
                    None,
                    Some(err.to_string()),
                ));
            }
        }
        result
    }

    async fn execute_internal(&self, request: QueryRequest) -> Result<QueryResponse, QueryError> {
        let start = Instant::now();

        request
            .validate()
            .map_err(|err| QueryError::InvalidQuery(err.to_string()))?;

        let effective_model_id = request
            .model_id
            .clone()
            .unwrap_or_else(|| DEFAULT_EMBEDDING_MODEL_ID.to_string());

        let mut plan = QueryPlanner::plan(&request);

        // Dispatch to the appropriate search mode pipeline.
        let (state, plan, global_answer) = match plan.effective_search_mode {
            SearchMode::Global => {
                self.execute_global(&request, &mut plan, &effective_model_id)
                    .await?
            }
            SearchMode::Drift => {
                let (state, plan) = self
                    .execute_drift(&request, &mut plan, &effective_model_id)
                    .await?;
                (state, plan, None)
            }
            SearchMode::Local | SearchMode::Auto => {
                let (state, plan) = self
                    .execute_local_with_auto_fallback(&request, plan, &effective_model_id)
                    .await?;
                (state, plan, None)
            }
        };

        // Build evidence nodes with provenance from execution state.
        let evidence_nodes: Vec<EvidenceNode> = state
            .nodes
            .iter()
            .map(|node| EvidenceNode {
                id: node.id,
                data: node.data.clone(),
                score: node.score,
                hop: node.hop,
                provenance: Provenance {
                    source: node.source.clone(),
                    extraction_model_id: node.extraction_model_id.clone(),
                    snapshot_id: node.node_snapshot_id.clone(),
                    ingested_at: node.ingested_at.clone(),
                },
                confidence: node.confidence,
            })
            .collect();

        // Build evidence edges with provenance.
        let evidence_edges: Vec<EvidenceEdge> = state
            .edges
            .iter()
            .map(|edge| {
                // Look up edge metadata from the graph index via repo nodes
                // Edge confidence defaults to weight
                EvidenceEdge {
                    source: edge.source,
                    target: edge.target,
                    relation: edge.relation.clone(),
                    weight: edge.weight,
                    provenance: edge.provenance.clone(),
                    confidence: edge.confidence,
                }
            })
            .collect();

        let citations = build_citations(&state.nodes);

        // Improved groundedness scoring.
        let evidence_scores: Vec<f32> = state.nodes.iter().map(|n| n.score).collect();
        let source_diversity = {
            let sources: HashSet<&str> = state
                .nodes
                .iter()
                .filter_map(|n| n.source.as_deref())
                .collect();
            sources.len()
        };
        let has_graph_support = !evidence_edges.is_empty();
        let groundedness = compute_groundedness(&GroundednessInput {
            query: &request.query,
            evidence_scores: &evidence_scores,
            evidence_count: evidence_nodes.len(),
            source_diversity,
            has_graph_support,
        });

        // Generate answer based on mode.
        let answer = match request.mode {
            QueryMode::Evidence => None,
            QueryMode::Answer => {
                if let Some(global_ans) = global_answer {
                    Some(global_ans)
                } else {
                    Some(generate_answer(&request.query, &evidence_nodes))
                }
            }
        };

        // snapshot_id takes priority over time_travel (SPEC 4.1)
        let snapshot_id = match request.snapshot_id.clone() {
            Some(snapshot_id) => Some(snapshot_id),
            None => Some(self.repo.current_snapshot_id().await),
        };

        // Reflect time_travel in response when provided (without snapshot_id override)
        let time_travel = if request.snapshot_id.is_none() {
            request.time_travel.clone()
        } else {
            None
        };

        let latency_ms = start.elapsed().as_millis() as u64;

        Ok(QueryResponse {
            answer,
            evidence: EvidenceSubgraph {
                nodes: evidence_nodes,
                edges: evidence_edges,
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
            time_travel,
            latency_ms,
        })
    }

    fn emit_audit_event(&self, event: AuditEvent) {
        if let Some(sink) = &self.audit_sink {
            let _ = sink.record(event);
        }
    }

    // -----------------------------------------------------------------------
    // Local Search with Auto-fallback to DRIFT
    // -----------------------------------------------------------------------
    async fn execute_local_with_auto_fallback(
        &self,
        request: &QueryRequest,
        mut plan: QueryPlan,
        embedding_model_id: &str,
    ) -> Result<(ExecutionState, QueryPlan), QueryError> {
        let mut state = self
            .execute_with_plan(request, &plan, embedding_model_id)
            .await?;

        // Record vector-only fallback when graph expansion added nothing.
        if state.edges.is_empty() && !state.nodes.is_empty() {
            state.exclusions.push(ExclusionReason {
                node_id: None,
                reason: "no_graph_expansion_vector_only_fallback".to_string(),
            });
        }

        // Auto mode: fall back to DRIFT if local yields insufficient evidence.
        if request.search_mode == SearchMode::Auto
            && plan.effective_search_mode == SearchMode::Local
            && state.nodes.len() < 2
        {
            let (drift_state, drift_plan) = self
                .execute_drift(request, &mut plan, embedding_model_id)
                .await?;
            let mut drift_state = drift_state;
            drift_state.exclusions.push(ExclusionReason {
                node_id: None,
                reason: "auto_fallback_to_drift_due_to_insufficient_evidence".to_string(),
            });

            return Ok((drift_state, drift_plan));
        }

        Ok((state, plan))
    }

    // -----------------------------------------------------------------------
    // Global Search: Community Summary Map-Reduce
    // -----------------------------------------------------------------------
    async fn execute_global(
        &self,
        request: &QueryRequest,
        plan: &mut QueryPlan,
        embedding_model_id: &str,
    ) -> Result<(ExecutionState, QueryPlan, Option<String>), QueryError> {
        if self.community_summaries.is_empty() {
            // Fallback: no community data available — run expanded vector search.
            plan.steps = vec![
                "vector_search",
                "graph_expansion",
                "context_pruning",
                "global_fallback_no_community_data",
            ];
            let mut state = self
                .execute_with_plan(request, plan, embedding_model_id)
                .await?;
            state.exclusions.push(ExclusionReason {
                node_id: None,
                reason: "no_community_data_fallback_to_vector".to_string(),
            });
            return Ok((state, plan.clone(), None));
        }

        // Always build filtered evidence first so global synthesis can respect request filters.
        let mut state = self
            .execute_with_plan(request, plan, embedding_model_id)
            .await?;

        // MAP: Score community summaries.
        let ranked = map_community_summaries(&request.query, &self.community_summaries);
        let relation_filter = collect_relation_filter(request);
        let time_range = parse_time_range(request)?;
        let now_unix = current_unix_timestamp();
        let entity_filter: HashSet<&str> = request
            .filters
            .entity_type
            .iter()
            .map(|value| value.as_str())
            .collect();

        // Build lookup for top nodes referenced by summaries so we can enforce filters
        // before producing global synthesis.
        let all_top_node_ids: Vec<u64> = {
            let mut seen = HashSet::new();
            let mut out = Vec::new();
            for (summary, _) in &ranked {
                for node_id in &summary.top_nodes {
                    if seen.insert(*node_id) {
                        out.push(*node_id);
                    }
                }
            }
            out
        };
        let top_nodes = self.repo.get_nodes_by_ids(&all_top_node_ids).await;
        let top_node_lookup: HashMap<u64, Node> =
            top_nodes.into_iter().map(|node| (node.id, node)).collect();

        // Restrict summary candidates by relevance (>0) and request filters.
        // Relation filters are edge/path-level constraints and cannot be validated
        // purely from summary top nodes, so summary synthesis is disabled in that case.
        let relevant_ranked: Vec<(&CommunitySummary, f32)> = if relation_filter.is_empty() {
            ranked
                .into_iter()
                .filter(|(summary, score)| {
                    *score > 0.0
                        && summary.top_nodes.iter().any(|node_id| {
                            top_node_lookup.get(node_id).is_some_and(|node| {
                                node_passes_filters(node, &entity_filter, time_range, now_unix)
                            })
                        })
                })
                .collect()
        } else {
            Vec::new()
        };

        let max_communities = 5;
        let global_answer = if relevant_ranked.is_empty() {
            let reason = if relation_filter.is_empty() {
                "global_no_relevant_community_summary"
            } else {
                "global_summary_disabled_by_relation_filter"
            };
            state.exclusions.push(ExclusionReason {
                node_id: None,
                reason: reason.to_string(),
            });
            None
        } else {
            Some(reduce_community_summaries(
                &request.query,
                &relevant_ranked,
                max_communities,
            ))
        };

        plan.steps = vec![
            "vector_search",
            "community_map_reduce",
            "graph_expansion",
            "context_pruning",
        ];

        Ok((state, plan.clone(), global_answer))
    }

    // -----------------------------------------------------------------------
    // DRIFT Search: Dynamic Reasoning via Iterative Feedback and Traversals
    // -----------------------------------------------------------------------
    async fn execute_drift(
        &self,
        request: &QueryRequest,
        plan: &mut QueryPlan,
        embedding_model_id: &str,
    ) -> Result<(ExecutionState, QueryPlan), QueryError> {
        plan.effective_search_mode = SearchMode::Drift;

        let mut best_state: Option<ExecutionState> = None;
        let initial_depth = plan.expansion_depth;

        for iteration in 0..DRIFT_MAX_ITERATIONS {
            let mut iter_plan = plan.clone();
            // Each DRIFT iteration expands the search depth progressively.
            iter_plan.expansion_depth = (initial_depth + iteration as u8).min(8);
            // Increase vector candidates each round.
            iter_plan.vector_top_k = plan.vector_top_k.saturating_add(iteration * 2).min(50);

            let state = self
                .execute_with_plan(request, &iter_plan, embedding_model_id)
                .await?;

            let is_sufficient = state.nodes.len() >= DRIFT_EVIDENCE_THRESHOLD
                || (iteration > 0
                    && best_state
                        .as_ref()
                        .map(|prev| state.nodes.len() <= prev.nodes.len())
                        .unwrap_or(false));

            if state.nodes.len() > best_state.as_ref().map(|s| s.nodes.len()).unwrap_or(0) {
                best_state = Some(state);
            }

            if is_sufficient {
                break;
            }
        }

        let mut state = best_state.unwrap_or(ExecutionState {
            anchors: Vec::new(),
            expansion_paths: Vec::new(),
            exclusions: vec![ExclusionReason {
                node_id: None,
                reason: "drift_no_evidence_found".to_string(),
            }],
            nodes: Vec::new(),
            edges: Vec::new(),
        });

        plan.steps = vec![
            "vector_search",
            "drift_iterative_expansion",
            "graph_expansion",
            "context_pruning",
        ];

        // If drift still yielded nothing, note it in exclusions.
        if state.nodes.is_empty() {
            state.exclusions.push(ExclusionReason {
                node_id: None,
                reason: "drift_exhausted_no_evidence".to_string(),
            });
        }

        Ok((state, plan.clone()))
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

                        traversed_edges.push(InternalEdge {
                            source: current_id,
                            target: *target,
                            relation: relation.clone(),
                            weight: *weight,
                            provenance: Provenance::default(),
                            confidence: *weight,
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
        let now_unix = current_unix_timestamp();
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

            if let Some(reason) =
                node_filter_exclusion_reason(node, &entity_filter, time_range, now_unix)
            {
                exclusions.push(ExclusionReason {
                    node_id: Some(node_id),
                    reason,
                });
                continue;
            }

            let lexical_score =
                lexical_similarity(&query_tokens, &tokenize(&node_lexical_text(node)));
            let anchor_score = anchor_scores.get(&node_id).copied().unwrap_or(0.0);
            let base_score = ((anchor_score * 0.8) + (lexical_score * 0.2))
                .max(lexical_score)
                .max(0.01);
            let score = base_score / (hop as f32 + 1.0);

            // Extract confidence: prefer explicit metadata, fall back to score
            let confidence = node
                .metadata
                .get("confidence")
                .and_then(|v| v.parse::<f32>().ok())
                .unwrap_or(score);

            ranked_nodes.push(RankedNode {
                id: node_id,
                data: node.data.clone(),
                score,
                hop,
                source: node.metadata.get("source").cloned(),
                extraction_model_id: node.metadata.get("extraction_model_id").cloned(),
                node_snapshot_id: node.metadata.get("snapshot_id").cloned(),
                ingested_at: node.metadata.get("ingested_at").cloned(),
                confidence,
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

        let mut edges: Vec<InternalEdge> = traversed_edges
            .into_iter()
            .filter(|edge| {
                selected_ids.contains(&edge.source) && selected_ids.contains(&edge.target)
            })
            .filter(|edge| relation_is_allowed(edge.relation.as_str(), &relation_filter))
            .collect();

        // Enrich edges with provenance — single lock acquisition via bulk API
        {
            let edge_keys: Vec<(u64, u64, String)> = edges
                .iter()
                .map(|e| (e.source, e.target, e.relation.clone()))
                .collect();
            let all_meta = self.repo.get_edge_metadata_bulk(&edge_keys).await;
            for edge in &mut edges {
                let key = (edge.source, edge.target, edge.relation.clone());
                if let Some(meta) = all_meta.get(&key) {
                    edge.provenance = Provenance {
                        source: meta.get("source").cloned(),
                        extraction_model_id: meta.get("extraction_model_id").cloned(),
                        snapshot_id: meta.get("snapshot_id").cloned(),
                        ingested_at: meta.get("ingested_at").cloned(),
                    };
                }
            }
        }

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

fn node_filter_exclusion_reason(
    node: &Node,
    entity_filter: &HashSet<&str>,
    time_range: Option<(NaiveDate, NaiveDate)>,
    now_unix: u64,
) -> Option<String> {
    if node_is_retention_expired(node, now_unix) {
        return Some("retention_expired".to_string());
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

fn node_passes_filters(
    node: &Node,
    entity_filter: &HashSet<&str>,
    time_range: Option<(NaiveDate, NaiveDate)>,
    now_unix: u64,
) -> bool {
    node_filter_exclusion_reason(node, entity_filter, time_range, now_unix).is_none()
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

fn effective_query_model_id(request: &QueryRequest) -> String {
    request
        .model_id
        .clone()
        .unwrap_or_else(|| DEFAULT_EMBEDDING_MODEL_ID.to_string())
}

fn build_query_audit_event(
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

fn dedup_edges(edges: Vec<InternalEdge>) -> Vec<InternalEdge> {
    // Last-wins dedup: when the same (source, target, relation) appears multiple times,
    // keep the last occurrence which has the most up-to-date weight/provenance.
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
