use super::synthesis::{build_citations, generate_answer};
use super::{
    EvidenceEdge, EvidenceNode, EvidenceSubgraph, Provenance, QueryError, QueryRequest,
    QueryResponse, ResolvedSnapshot, DEFAULT_EMBEDDING_MODEL_ID,
};
use crate::dsl::{QueryMode, SearchMode};
use crate::graphrag::compute_groundedness;
use crate::planner::QueryPlanner;
use crate::semantic_cache::SemanticCacheKey;
use alayasiki_core::model::Node;
use chrono::{DateTime, NaiveDate, Utc};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use storage::repo::{parse_wal_snapshot_lsn, RepoError, SnapshotView};
use storage::session::{SessionGraph, SessionOwner};

impl super::QueryEngine {
    pub(super) async fn execute_internal(
        &self,
        request: QueryRequest,
        start: Instant,
        tenant_scope: Option<String>,
        session_owner: Option<SessionOwner>,
    ) -> Result<QueryResponse, QueryError> {
        request
            .validate()
            .map_err(|err| QueryError::InvalidQuery(err.to_string()))?;

        let effective_model_id = request
            .model_id
            .clone()
            .unwrap_or_else(|| DEFAULT_EMBEDDING_MODEL_ID.to_string());
        let mut plan = QueryPlanner::plan(&request);
        let resolved_snapshot = self.resolve_snapshot(&request).await?;
        let tenant_scoped = tenant_scope.is_some();
        let cache_eligible = !tenant_scoped && request.session_id.is_none();

        let session_graph = match request.session_id.as_deref() {
            Some(session_id) => self
                .repo
                .get_session_with_owner(session_id, session_owner.as_ref())?,
            None => None,
        };
        let cache_key = SemanticCacheKey::from_request(
            &request,
            &effective_model_id,
            &resolved_snapshot.snapshot_id,
            plan.effective_search_mode,
        );

        if cache_eligible {
            if let Some(mut cached_response) =
                self.lookup_semantic_cache(&cache_key, &request.query).await
            {
                cached_response.latency_ms = start.elapsed().as_millis() as u64;
                if !cached_response
                    .explain
                    .steps
                    .iter()
                    .any(|step| step == crate::SEMANTIC_CACHE_HIT_STEP)
                {
                    cached_response
                        .explain
                        .steps
                        .insert(0, crate::SEMANTIC_CACHE_HIT_STEP.to_string());
                }

                self.metrics
                    .record_query(start.elapsed().as_micros() as u64, true);
                return Ok(cached_response);
            }
        }

        let (state, plan, global_answer) = match plan.effective_search_mode {
            SearchMode::Global => {
                self.execute_global(
                    &request,
                    &mut plan,
                    &effective_model_id,
                    &resolved_snapshot,
                    tenant_scope.as_deref(),
                    session_graph.as_ref(),
                )
                .await?
            }
            SearchMode::Drift => {
                let (state, plan) = self
                    .execute_drift(
                        &request,
                        &mut plan,
                        &effective_model_id,
                        resolved_snapshot.snapshot_view.as_deref(),
                        tenant_scope.as_deref(),
                        session_graph.as_ref(),
                    )
                    .await?;
                (state, plan, None)
            }
            SearchMode::Local | SearchMode::Auto => {
                let (state, plan) = self
                    .execute_local_with_auto_fallback(
                        &request,
                        plan,
                        &effective_model_id,
                        resolved_snapshot.snapshot_view.as_deref(),
                        tenant_scope.as_deref(),
                        session_graph.as_ref(),
                    )
                    .await?;
                (state, plan, None)
            }
        };

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

        let evidence_edges: Vec<EvidenceEdge> = state
            .edges
            .iter()
            .map(|edge| EvidenceEdge {
                source: edge.source,
                target: edge.target,
                relation: edge.relation.clone(),
                weight: edge.weight,
                provenance: edge.provenance.clone(),
                confidence: edge.confidence,
            })
            .collect();

        let citations = build_citations(&state.nodes);

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
        let groundedness = compute_groundedness(&crate::graphrag::GroundednessInput {
            query: &request.query,
            evidence_scores: &evidence_scores,
            evidence_count: evidence_nodes.len(),
            source_diversity,
            has_graph_support,
        });

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

        let latency_ms = start.elapsed().as_millis() as u64;

        let response = QueryResponse {
            answer,
            evidence: EvidenceSubgraph {
                nodes: evidence_nodes,
                edges: evidence_edges,
            },
            citations,
            groundedness,
            explain: super::ExplainPlan {
                steps: plan.steps.iter().map(|step| step.to_string()).collect(),
                effective_search_mode: plan.effective_search_mode,
                anchors: state.anchors,
                expansion_paths: state.expansion_paths,
                exclusions: state.exclusions,
            },
            model_id: Some(effective_model_id),
            snapshot_id: Some(resolved_snapshot.snapshot_id.clone()),
            time_travel: resolved_snapshot.time_travel.clone(),
            latency_ms,
            error_code: None,
        };

        self.metrics.record_query(
            start.elapsed().as_micros() as u64,
            response
                .explain
                .steps
                .iter()
                .any(|s| s == crate::SEMANTIC_CACHE_HIT_STEP),
        );

        if cache_eligible {
            self.insert_semantic_cache(cache_key, &request.query, response.clone())
                .await;
        }

        Ok(response)
    }

    async fn resolve_snapshot(
        &self,
        request: &QueryRequest,
    ) -> Result<ResolvedSnapshot, QueryError> {
        if let Some(snapshot_id) = request.snapshot_id.clone() {
            let snapshot_lsn = parse_wal_snapshot_lsn(&snapshot_id).ok_or_else(|| {
                QueryError::InvalidQuery(format!(
                    "snapshot_id must be wal-lsn-<lsn>: {snapshot_id}"
                ))
            })?;
            let snapshot_view = self.load_snapshot_view(&snapshot_id).await?;
            return Ok(ResolvedSnapshot {
                snapshot_id,
                snapshot_lsn,
                snapshot_view: Some(snapshot_view),
                time_travel: None,
                requires_versioned_summaries: true,
            });
        }

        if let Some(time_travel) = request.time_travel.as_deref() {
            let as_of_unix_ms = parse_time_travel_as_of_unix_ms(time_travel)?;
            let snapshot_id = self
                .repo
                .resolve_snapshot_id_at_or_before(as_of_unix_ms)
                .await
                .map_err(|err| match err {
                    RepoError::SnapshotNotFound(_) => {
                        QueryError::NotFound(format!("time_travel `{time_travel}`"))
                    }
                    other => QueryError::Repository(other),
                })?;
            let snapshot_lsn = parse_wal_snapshot_lsn(&snapshot_id).ok_or_else(|| {
                QueryError::InvalidQuery(format!(
                    "snapshot_id must be wal-lsn-<lsn>: {snapshot_id}"
                ))
            })?;
            let snapshot_view = self.load_snapshot_view(&snapshot_id).await?;
            return Ok(ResolvedSnapshot {
                snapshot_id,
                snapshot_lsn,
                snapshot_view: Some(snapshot_view),
                time_travel: Some(time_travel.to_string()),
                requires_versioned_summaries: true,
            });
        }

        let snapshot_id = self.repo.current_snapshot_id().await;
        let snapshot_lsn = parse_wal_snapshot_lsn(&snapshot_id).ok_or_else(|| {
            QueryError::InvalidQuery(format!("snapshot_id must be wal-lsn-<lsn>: {snapshot_id}"))
        })?;
        Ok(ResolvedSnapshot {
            snapshot_id,
            snapshot_lsn,
            snapshot_view: None,
            time_travel: None,
            requires_versioned_summaries: false,
        })
    }

    async fn load_snapshot_view(&self, snapshot_id: &str) -> Result<Arc<SnapshotView>, QueryError> {
        match self.repo.load_snapshot_view(snapshot_id).await {
            Ok(view) => Ok(Arc::new(view)),
            Err(RepoError::SnapshotNotFound(_)) => {
                Err(QueryError::NotFound(format!("snapshot_id `{snapshot_id}`")))
            }
            Err(RepoError::InvalidSnapshotId(_)) => Err(QueryError::InvalidQuery(format!(
                "snapshot_id must be wal-lsn-<lsn>: {snapshot_id}"
            ))),
            Err(err) => Err(QueryError::Repository(err)),
        }
    }

    pub(super) async fn list_node_ids_from_source(
        &self,
        snapshot_view: Option<&SnapshotView>,
        session: Option<&SessionGraph>,
    ) -> Vec<u64> {
        let mut out = match snapshot_view {
            Some(view) => view.list_node_ids(),
            None => self.repo.list_node_ids().await,
        };
        if let Some(session) = session {
            out.extend(session.nodes.keys().copied());
            out.sort_unstable();
            out.dedup();
        }
        out
    }

    pub(super) async fn get_nodes_by_ids_from_source(
        &self,
        ids: &[u64],
        snapshot_view: Option<&SnapshotView>,
        session: Option<&SessionGraph>,
    ) -> Vec<Node> {
        let mut results = Vec::with_capacity(ids.len());
        let mut remaining_ids = Vec::new();

        if let Some(session) = session {
            for id in ids {
                if let Some(node) = session.nodes.get(id) {
                    results.push(node.clone());
                } else {
                    remaining_ids.push(*id);
                }
            }
        } else {
            remaining_ids = ids.to_vec();
        }

        if !remaining_ids.is_empty() {
            let mut source_results = match snapshot_view {
                Some(view) => view.get_nodes_by_ids(&remaining_ids),
                None => self.repo.get_nodes_by_ids(&remaining_ids).await,
            };
            results.append(&mut source_results);
        }
        results
    }

    pub(super) async fn get_edge_metadata_bulk_from_source(
        &self,
        keys: &[(u64, u64, String)],
        snapshot_view: Option<&SnapshotView>,
    ) -> HashMap<(u64, u64, String), HashMap<String, String>> {
        match snapshot_view {
            Some(view) => view.get_edge_metadata_bulk(keys),
            None => self.repo.get_edge_metadata_bulk(keys).await,
        }
    }
}

fn parse_time_travel_as_of_unix_ms(input: &str) -> Result<i64, QueryError> {
    if let Ok(date) = NaiveDate::parse_from_str(input, "%Y-%m-%d") {
        return date
            .and_hms_milli_opt(23, 59, 59, 999)
            .map(|datetime| datetime.and_utc().timestamp_millis())
            .ok_or_else(|| {
                QueryError::InvalidQuery(
                    "time_travel must be YYYY-MM-DD or RFC3339 format".to_string(),
                )
            });
    }

    DateTime::parse_from_rfc3339(input)
        .map(|datetime| datetime.with_timezone(&Utc).timestamp_millis())
        .map_err(|_| {
            QueryError::InvalidQuery("time_travel must be YYYY-MM-DD or RFC3339 format".to_string())
        })
}
