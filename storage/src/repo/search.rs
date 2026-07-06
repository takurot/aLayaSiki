use super::{EdgeMetaKey, SnapshotView};
use crate::session::SessionGraph;
use alayasiki_core::embedding::cosine_similarity;
use alayasiki_core::model::Node;
use std::collections::HashMap;

impl SnapshotView {
    pub fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    pub fn storage_capabilities(&self) -> &crate::tiering::StorageCapabilities {
        self.hyper_index.storage_capabilities()
    }

    pub fn list_node_ids(&self) -> Vec<u64> {
        let mut out: Vec<u64> = self.nodes.keys().copied().collect();
        out.sort_unstable();
        out
    }

    pub fn get_nodes_by_ids(&self, ids: &[u64]) -> Vec<Node> {
        let mut out: Vec<Node> = ids
            .iter()
            .filter_map(|id| self.nodes.get(id).cloned())
            .collect();
        out.sort_by_key(|node| node.id);
        out
    }

    pub fn embedding_dimension(&self) -> Option<usize> {
        self.nodes
            .values()
            .find_map(|node| (!node.embedding.is_empty()).then_some(node.embedding.len()))
    }

    pub fn search_vector(&self, query: &[f32], k: usize) -> Vec<(u64, f32)> {
        self.hyper_index.search_vector(query, k)
    }

    pub fn search_vector_with_session(
        &self,
        query: &[f32],
        k: usize,
        session: Option<&SessionGraph>,
    ) -> Vec<(u64, f32)> {
        let mut results = self.search_vector(query, k);
        if let Some(session) = session {
            let mut session_results: Vec<(u64, f32)> = session
                .nodes
                .values()
                .filter_map(|node| {
                    cosine_similarity(query, &node.embedding).map(|sim| (node.id, sim))
                })
                .collect();

            results.append(&mut session_results);
            results.sort_by(|a, b| {
                a.0.cmp(&b.0)
                    .then_with(|| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal))
            });
            results.dedup_by_key(|(id, _)| *id);
            results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            results.truncate(k);
        }
        results
    }

    pub fn neighbors(&self, node_id: u64) -> Vec<(u64, String, f32)> {
        self.hyper_index
            .graph_index
            .neighbors(node_id)
            .into_iter()
            .map(|(target, relation, weight)| (*target, relation.clone(), *weight))
            .collect()
    }

    pub fn neighbors_with_session(
        &self,
        node_id: u64,
        session: Option<&SessionGraph>,
    ) -> Vec<(u64, String, f32)> {
        let mut results = self.neighbors(node_id);
        if let Some(session) = session {
            for edge in &session.edges {
                if edge.source == node_id {
                    results.push((edge.target, edge.relation.clone(), edge.weight));
                }
            }
        }
        results
    }

    pub fn get_edge_metadata_bulk(
        &self,
        keys: &[(u64, u64, String)],
    ) -> HashMap<EdgeMetaKey, HashMap<String, String>> {
        keys.iter()
            .filter_map(|key| {
                self.edge_metadata
                    .get(key)
                    .map(|meta| (key.clone(), meta.clone()))
            })
            .collect()
    }
}
