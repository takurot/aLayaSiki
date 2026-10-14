//! Snapshot-scoped, deterministic community index. Rebuild on version changes;
//! bounded caching avoids recomputing clustering for repeated queries.
use query::engine::CommunitySummaryProvider;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use storage::community::{CommunityEngine, CommunitySummarizer, CommunitySummary};
use storage::index::AdjacencyGraph;
use storage::repo::{parse_wal_snapshot_lsn, RepoError, SnapshotView};
use tokio::sync::Mutex;

const MAX_CACHED_VERSIONS: usize = 32;
type CacheKey = (u64, Option<String>);

#[derive(Default)]
pub(crate) struct RuntimeCommunities {
    cache: Mutex<BTreeMap<CacheKey, Vec<CommunitySummary>>>,
    rebuilds: AtomicU64,
}

impl RuntimeCommunities {
    pub fn rebuild_count(&self) -> u64 {
        self.rebuilds.load(Ordering::Relaxed)
    }
}

struct TextSummarizer {
    labels: BTreeMap<u64, String>,
}

impl CommunitySummarizer for TextSummarizer {
    fn summarize(
        &self,
        level: usize,
        community_id: usize,
        node_ids: &[u64],
        _top_nodes: &[u64],
    ) -> String {
        let text: Vec<_> = node_ids
            .iter()
            .take(16)
            .filter_map(|id| self.labels.get(id))
            .map(String::as_str)
            .collect();
        format!(
            "L{level}-C{community_id}: {} nodes; {}",
            node_ids.len(),
            text.join(" | ")
        )
    }
}

#[async_trait::async_trait]
impl CommunitySummaryProvider for RuntimeCommunities {
    async fn summaries(
        &self,
        view: Arc<SnapshotView>,
        tenant: Option<&str>,
    ) -> Result<Vec<CommunitySummary>, RepoError> {
        let lsn = parse_wal_snapshot_lsn(view.snapshot_id())
            .ok_or(RepoError::InvalidSnapshotId(view.snapshot_id().to_string()))?;
        let key = (lsn, tenant.map(str::to_string));
        let mut cache = self.cache.lock().await;
        if let Some(summaries) = cache.get(&key) {
            return Ok(summaries.clone());
        }
        // Select ownership before clustering; cross-tenant edges never enter summaries.
        let labels: BTreeMap<_, _> = view
            .nodes()
            .filter(|node| {
                tenant.is_none_or(|tenant| {
                    node.metadata
                        .get("tenant")
                        .is_some_and(|owner| owner == tenant)
                })
            })
            .map(|node| (node.id, node.data.chars().take(240).collect::<String>()))
            .collect();
        let visible: BTreeSet<_> = labels.keys().copied().collect();
        let mut graph = AdjacencyGraph::new();
        for source in &visible {
            for (target, relation, weight) in view.neighbors(*source) {
                if visible.contains(&target) {
                    graph.add_edge(*source, target, relation, weight);
                }
            }
        }
        let mut engine = CommunityEngine::new(graph);
        engine.rebuild_hierarchy(3, &TextSummarizer { labels });
        let summaries: Vec<_> = engine
            .summaries()
            .iter()
            .cloned()
            .map(|summary| summary.with_snapshot_lsn_range(lsn, lsn))
            .collect();
        self.rebuilds.fetch_add(1, Ordering::Relaxed);
        if cache.len() >= MAX_CACHED_VERSIONS {
            if let Some(oldest) = cache.keys().next().cloned() {
                cache.remove(&oldest);
            }
        }
        cache.insert(key, summaries.clone());
        Ok(summaries)
    }
}

pub(crate) fn tenant_resource_summaries(
    view: &SnapshotView,
    tenant: &str,
) -> Vec<CommunitySummary> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let labels: BTreeMap<_, _> = view
        .nodes()
        .filter(|node| {
            node.metadata
                .get("tenant")
                .is_some_and(|owner| owner == tenant)
                && node
                    .metadata
                    .get("retention_until_unix")
                    .and_then(|deadline| deadline.parse::<u64>().ok())
                    .is_none_or(|deadline| now < deadline)
        })
        .map(|node| (node.id, node.data.chars().take(240).collect::<String>()))
        .collect();
    let mut graph = AdjacencyGraph::new();
    for source in labels.keys() {
        for (target, relation, weight) in view.neighbors(*source) {
            if labels.contains_key(&target) {
                graph.add_edge(*source, target, relation, weight);
            }
        }
    }
    let mut engine = CommunityEngine::new(graph);
    engine.rebuild_hierarchy(3, &TextSummarizer { labels });
    engine.summaries().to_vec()
}
