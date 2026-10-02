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
        let results = self.search_vector(query, k);
        let Some(session) = session else {
            return results;
        };

        let mut merged: HashMap<u64, f32> = results.into_iter().collect();
        for node in session.nodes.values() {
            if let Some(sim) = cosine_similarity(query, &node.embedding) {
                merged.insert(node.id, sim);
            }
        }

        let mut merged: Vec<(u64, f32)> = merged.into_iter().collect();
        merged.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        merged.truncate(k);
        merged
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
        let Some(session) = session else {
            return self.neighbors(node_id);
        };

        let mut merged: HashMap<(u64, String), f32> = self
            .neighbors(node_id)
            .into_iter()
            .map(|(target, relation, weight)| ((target, relation), weight))
            .collect();

        for edge in &session.edges {
            if edge.source == node_id {
                merged.insert((edge.target, edge.relation.clone()), edge.weight);
            }
        }

        merged
            .into_iter()
            .map(|((target, relation), weight)| (target, relation, weight))
            .collect()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hyper_index::HyperIndex;
    use alayasiki_core::model::Edge;
    use std::time::Duration;

    fn snapshot_with(nodes: &[(u64, Vec<f32>)], edges: &[(u64, u64, &str, f32)]) -> SnapshotView {
        let mut hyper_index = HyperIndex::new();
        let mut node_map = HashMap::new();
        for (id, embedding) in nodes {
            hyper_index.insert_node(*id, embedding.clone());
            node_map.insert(*id, Node::new(*id, embedding.clone(), String::new()));
        }
        for (source, target, relation, weight) in edges {
            hyper_index.insert_edge(*source, *target, *relation, *weight);
        }
        SnapshotView {
            snapshot_id: "test".to_string(),
            nodes: node_map,
            hyper_index,
            edge_metadata: HashMap::new(),
        }
    }

    fn session_with(nodes: &[(u64, Vec<f32>)], edges: &[(u64, u64, &str, f32)]) -> SessionGraph {
        let mut session = SessionGraph::new("session".to_string(), Duration::from_secs(60));
        for (id, embedding) in nodes {
            session.insert_node(Node::new(*id, embedding.clone(), String::new()));
        }
        for (source, target, relation, weight) in edges {
            session.insert_edge(Edge::new(*source, *target, *relation, *weight));
        }
        session
    }

    #[test]
    fn neighbors_with_session_dedups_shared_edge_preferring_session_weight() {
        let snapshot = snapshot_with(&[], &[(1, 2, "links", 1.0)]);
        let session = session_with(&[], &[(1, 2, "links", 0.5)]);

        let results = snapshot.neighbors_with_session(1, Some(&session));

        assert_eq!(results, vec![(2, "links".to_string(), 0.5)]);
    }

    #[test]
    fn neighbors_with_session_keeps_distinct_edges() {
        let snapshot = snapshot_with(&[], &[(1, 2, "links", 1.0)]);
        let session = session_with(&[], &[(1, 3, "links", 0.5)]);

        let mut results = snapshot.neighbors_with_session(1, Some(&session));
        results.sort_by_key(|(target, _, _)| *target);

        assert_eq!(
            results,
            vec![(2, "links".to_string(), 1.0), (3, "links".to_string(), 0.5)]
        );
    }

    #[test]
    fn search_vector_with_session_prefers_session_result_over_higher_similarity_snapshot() {
        let snapshot = snapshot_with(&[(1, vec![1.0, 0.0])], &[]);
        let session = session_with(&[(1, vec![0.0, 1.0])], &[]);

        let results = snapshot.search_vector_with_session(&[1.0, 0.0], 5, Some(&session));

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);
        assert_eq!(results[0].1, 0.0);
    }

    #[test]
    fn search_vector_with_session_merges_non_overlapping_results() {
        let snapshot = snapshot_with(&[(1, vec![1.0, 0.0])], &[]);
        let session = session_with(&[(2, vec![0.0, 1.0])], &[]);

        let mut results = snapshot.search_vector_with_session(&[1.0, 0.0], 5, Some(&session));
        results.sort_by_key(|(id, _)| *id);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 1);
        assert_eq!(results[1].0, 2);
    }
}
