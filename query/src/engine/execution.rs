use super::synthesis::{
    collect_relation_filter, dedup_edges, dedup_exclusions, dedup_paths, lexical_similarity,
    node_belongs_to_tenant, node_filter_exclusion_reason, node_lexical_text, node_passes_filters,
    parse_time_range, reconstruct_path, relation_is_allowed, retention_cutoff_unix, tokenize,
};
use super::{
    Anchor, ExclusionReason, ExecutionState, ExpansionPath, InternalEdge, Provenance, QueryError,
    QueryRequest, RankedNode, ResolvedSnapshot,
};
use crate::graphrag::{
    map_community_summaries, reduce_community_summaries, DRIFT_EVIDENCE_THRESHOLD,
    DRIFT_MAX_ITERATIONS,
};
use crate::planner::QueryPlan;
use alayasiki_core::embedding::deterministic_embedding;
use alayasiki_core::model::Node;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use storage::community::CommunitySummary;
use storage::repo::SnapshotView;
use storage::session::SessionGraph;

impl super::QueryEngine {
    pub(super) async fn execute_local_with_auto_fallback(
        &self,
        request: &QueryRequest,
        mut plan: QueryPlan,
        embedding_model_id: &str,
        snapshot_view: Option<&SnapshotView>,
        tenant_scope: Option<&str>,
        session: Option<&SessionGraph>,
    ) -> Result<(ExecutionState, QueryPlan), QueryError> {
        let mut state = self
            .execute_with_plan(
                request,
                &plan,
                embedding_model_id,
                snapshot_view,
                tenant_scope,
                session,
            )
            .await?;

        if state.edges.is_empty() && !state.nodes.is_empty() {
            state.exclusions.push(ExclusionReason {
                node_id: None,
                reason: "no_graph_expansion_vector_only_fallback".to_string(),
            });
        }

        if request.search_mode == crate::dsl::SearchMode::Auto
            && plan.effective_search_mode == crate::dsl::SearchMode::Local
            && state.nodes.len() < 2
        {
            let (drift_state, drift_plan) = self
                .execute_drift(
                    request,
                    &mut plan,
                    embedding_model_id,
                    snapshot_view,
                    tenant_scope,
                    session,
                )
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

    pub(super) async fn execute_global(
        &self,
        request: &QueryRequest,
        plan: &mut QueryPlan,
        embedding_model_id: &str,
        resolved_snapshot: &ResolvedSnapshot,
        tenant_scope: Option<&str>,
        session: Option<&SessionGraph>,
    ) -> Result<(ExecutionState, QueryPlan, Option<String>), QueryError> {
        let snapshot_view = resolved_snapshot.snapshot_view.as_deref();
        if tenant_scope.is_some() {
            plan.steps = vec![
                "vector_search",
                "graph_expansion",
                "context_pruning",
                "global_fallback_tenant_scoped",
            ];
            let mut state = self
                .execute_with_plan(
                    request,
                    plan,
                    embedding_model_id,
                    snapshot_view,
                    tenant_scope,
                    session,
                )
                .await?;
            state.exclusions.push(ExclusionReason {
                node_id: None,
                reason: "global_summary_disabled_by_tenant_scope".to_string(),
            });
            return Ok((state, plan.clone(), None));
        }

        if self.community_summaries.is_empty() {
            plan.steps = vec![
                "vector_search",
                "graph_expansion",
                "context_pruning",
                "global_fallback_no_community_data",
            ];
            let mut state = self
                .execute_with_plan(
                    request,
                    plan,
                    embedding_model_id,
                    snapshot_view,
                    tenant_scope,
                    session,
                )
                .await?;
            state.exclusions.push(ExclusionReason {
                node_id: None,
                reason: "no_community_data_fallback_to_vector".to_string(),
            });
            return Ok((state, plan.clone(), None));
        }

        let summary_candidates: Vec<CommunitySummary> = self
            .community_summaries
            .iter()
            .filter(|summary| {
                summary.is_visible_at_lsn(resolved_snapshot.snapshot_lsn)
                    && (!resolved_snapshot.requires_versioned_summaries
                        || summary.snapshot_lsn_range.is_some())
            })
            .cloned()
            .collect();
        if summary_candidates.is_empty() && resolved_snapshot.requires_versioned_summaries {
            plan.steps = vec![
                "vector_search",
                "graph_expansion",
                "context_pruning",
                "global_fallback_snapshot_pinned",
            ];
            let mut state = self
                .execute_with_plan(
                    request,
                    plan,
                    embedding_model_id,
                    snapshot_view,
                    tenant_scope,
                    session,
                )
                .await?;
            state.exclusions.push(ExclusionReason {
                node_id: None,
                reason: "global_summary_disabled_by_snapshot_pin".to_string(),
            });
            return Ok((state, plan.clone(), None));
        }

        let mut state = self
            .execute_with_plan(
                request,
                plan,
                embedding_model_id,
                snapshot_view,
                tenant_scope,
                session,
            )
            .await?;

        let ranked = map_community_summaries(&request.query, &summary_candidates);
        let relation_filter = collect_relation_filter(request);
        let time_range = parse_time_range(request)?;
        let retention_cutoff = retention_cutoff_unix(request);
        let entity_filter: HashSet<&str> = request
            .filters
            .entity_type
            .iter()
            .map(|value| value.as_str())
            .collect();

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
        let top_nodes = self
            .get_nodes_by_ids_from_source(&all_top_node_ids, snapshot_view, None)
            .await;
        let top_node_lookup: HashMap<u64, Node> =
            top_nodes.into_iter().map(|node| (node.id, node)).collect();

        let relevant_ranked: Vec<(&CommunitySummary, f32)> = if relation_filter.is_empty() {
            ranked
                .into_iter()
                .filter(|(summary, score)| {
                    *score > 0.0
                        && summary.top_nodes.iter().any(|node_id| {
                            top_node_lookup.get(node_id).is_some_and(|node| {
                                node_passes_filters(
                                    node,
                                    &entity_filter,
                                    time_range,
                                    retention_cutoff,
                                    tenant_scope,
                                )
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

    pub(super) async fn execute_drift(
        &self,
        request: &QueryRequest,
        plan: &mut QueryPlan,
        embedding_model_id: &str,
        snapshot_view: Option<&SnapshotView>,
        tenant_scope: Option<&str>,
        session: Option<&SessionGraph>,
    ) -> Result<(ExecutionState, QueryPlan), QueryError> {
        plan.effective_search_mode = crate::dsl::SearchMode::Drift;

        let mut best_state: Option<ExecutionState> = None;
        let initial_depth = plan.expansion_depth;

        for iteration in 0..DRIFT_MAX_ITERATIONS {
            let mut iter_plan = plan.clone();
            iter_plan.expansion_depth = (initial_depth + iteration as u8).min(8);
            iter_plan.vector_top_k = plan.vector_top_k.saturating_add(iteration * 2).min(50);

            let state = self
                .execute_with_plan(
                    request,
                    &iter_plan,
                    embedding_model_id,
                    snapshot_view,
                    tenant_scope,
                    session,
                )
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

        if state.nodes.is_empty() {
            state.exclusions.push(ExclusionReason {
                node_id: None,
                reason: "drift_exhausted_no_evidence".to_string(),
            });
        }

        Ok((state, plan.clone()))
    }

    pub(super) async fn execute_with_plan(
        &self,
        request: &QueryRequest,
        plan: &QueryPlan,
        embedding_model_id: &str,
        snapshot_view: Option<&SnapshotView>,
        tenant_scope: Option<&str>,
        session: Option<&SessionGraph>,
    ) -> Result<ExecutionState, QueryError> {
        let mut vector_hits = self
            .collect_vector_scores(
                request,
                plan,
                embedding_model_id,
                snapshot_view,
                tenant_scope,
                session,
            )
            .await;
        if vector_hits.is_empty() {
            if let Some(node_id) = self
                .list_node_ids_from_source(snapshot_view, session)
                .await
                .into_iter()
                .next()
            {
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

        if let Some(view) = snapshot_view {
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

                    for (target, relation, weight) in
                        view.neighbors_with_session(current_id, session)
                    {
                        if !relation_is_allowed(relation.as_str(), &relation_filter) {
                            exclusions.push(ExclusionReason {
                                node_id: Some(target),
                                reason: format!("relation_filtered:{}", relation),
                            });
                            continue;
                        }

                        traversed_edges.push(InternalEdge {
                            source: current_id,
                            target,
                            relation: relation.clone(),
                            weight,
                            provenance: Provenance::default(),
                            confidence: weight,
                        });

                        let next_hop = current_hop + 1;
                        let should_visit = visited
                            .get(&target)
                            .map(|prev_hop| next_hop < *prev_hop)
                            .unwrap_or(true);

                        if should_visit {
                            visited.insert(target, next_hop);
                            parents.insert(target, current_id);
                            queue.push_back(target);
                            candidate_hops
                                .entry(target)
                                .and_modify(|hop| *hop = (*hop).min(next_hop))
                                .or_insert(next_hop);

                            if let Some(path) = reconstruct_path(anchor.node_id, target, &parents) {
                                expansion_paths.push(ExpansionPath {
                                    anchor_id: anchor.node_id,
                                    target_id: target,
                                    path,
                                });
                            }
                        }
                    }
                }
            }
        } else {
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

                    for (target, relation, weight) in self
                        .repo
                        .neighbors_with_session_graph(current_id, session)
                        .await
                    {
                        if !relation_is_allowed(relation.as_str(), &relation_filter) {
                            exclusions.push(ExclusionReason {
                                node_id: Some(target),
                                reason: format!("relation_filtered:{}", relation),
                            });
                            continue;
                        }

                        traversed_edges.push(InternalEdge {
                            source: current_id,
                            target,
                            relation: relation.clone(),
                            weight,
                            provenance: Provenance::default(),
                            confidence: weight,
                        });

                        let next_hop = current_hop + 1;
                        let should_visit = visited
                            .get(&target)
                            .map(|prev_hop| next_hop < *prev_hop)
                            .unwrap_or(true);

                        if should_visit {
                            visited.insert(target, next_hop);
                            parents.insert(target, current_id);
                            queue.push_back(target);
                            candidate_hops
                                .entry(target)
                                .and_modify(|hop| *hop = (*hop).min(next_hop))
                                .or_insert(next_hop);

                            if let Some(path) = reconstruct_path(anchor.node_id, target, &parents) {
                                expansion_paths.push(ExpansionPath {
                                    anchor_id: anchor.node_id,
                                    target_id: target,
                                    path,
                                });
                            }
                        }
                    }
                }
            }
        }

        let candidate_ids: Vec<u64> = candidate_hops.keys().copied().collect();
        let fetched_nodes = self
            .get_nodes_by_ids_from_source(&candidate_ids, snapshot_view, session)
            .await;
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
        let retention_cutoff = retention_cutoff_unix(request);
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

            if let Some(reason) = node_filter_exclusion_reason(
                node,
                &entity_filter,
                time_range,
                retention_cutoff,
                tenant_scope,
            ) {
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

        {
            let edge_keys: Vec<(u64, u64, String)> = edges
                .iter()
                .map(|e| (e.source, e.target, e.relation.clone()))
                .collect();
            let all_meta = self
                .get_edge_metadata_bulk_from_source(&edge_keys, snapshot_view)
                .await;
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

    pub(super) async fn collect_vector_scores(
        &self,
        request: &QueryRequest,
        plan: &QueryPlan,
        embedding_model_id: &str,
        snapshot_view: Option<&SnapshotView>,
        tenant_scope: Option<&str>,
        session: Option<&SessionGraph>,
    ) -> Vec<(u64, f32)> {
        let embedding_dim = match snapshot_view {
            Some(view) => view.embedding_dimension(),
            None => self.repo.embedding_dimension().await,
        }
        .or_else(|| session.and_then(SessionGraph::embedding_dimension));
        let Some(embedding_dim) = embedding_dim else {
            return Vec::new();
        };

        let query_embedding =
            deterministic_embedding(&request.query, embedding_model_id, embedding_dim);
        let vector_limit = match plan.effective_search_mode {
            crate::dsl::SearchMode::Global => plan.vector_top_k.saturating_mul(2),
            _ => plan.vector_top_k,
        }
        .max(1);

        let raw_hits = match snapshot_view {
            Some(view) => view.search_vector_with_session(&query_embedding, vector_limit, session),
            None => {
                self.repo
                    .search_vector_with_session_graph(&query_embedding, vector_limit, session)
                    .await
            }
        };

        let Some(tenant) = tenant_scope else {
            return raw_hits;
        };

        let candidate_ids: Vec<u64> = raw_hits.iter().map(|(node_id, _)| *node_id).collect();
        let allowed_ids: HashSet<u64> = self
            .get_nodes_by_ids_from_source(&candidate_ids, snapshot_view, session)
            .await
            .into_iter()
            .filter(|node| node_belongs_to_tenant(node, tenant))
            .map(|node| node.id)
            .collect();

        raw_hits
            .into_iter()
            .filter(|(node_id, _)| allowed_ids.contains(node_id))
            .collect()
    }
}
