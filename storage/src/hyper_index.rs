use crate::index::{LinearAnnIndex, AdjacencyGraph};
use std::collections::HashMap;

/// HyperIndex combines Vector and Graph indexes with ID mapping
pub struct HyperIndex {
    pub vector_index: LinearAnnIndex,
    pub graph_index: AdjacencyGraph,
    // ID mapping for cross-referencing (e.g., entity resolution)
    id_aliases: HashMap<String, u64>,
}

impl HyperIndex {
    pub fn new() -> Self {
        Self {
            vector_index: LinearAnnIndex::new(),
            graph_index: AdjacencyGraph::new(),
            id_aliases: HashMap::new(),
        }
    }

    pub fn insert_node(&mut self, id: u64, embedding: Vec<f32>) {
        self.vector_index.insert(id, embedding);
    }

    pub fn insert_edge(&mut self, source: u64, target: u64, relation: impl Into<String>, weight: f32) {
        self.graph_index.add_edge(source, target, relation, weight);
    }

    pub fn remove_node(&mut self, id: u64) {
        self.vector_index.delete(id);
        self.graph_index.remove_node(id);
        // Remove any aliases pointing to this ID
        self.id_aliases.retain(|_, v| *v != id);
    }

    /// Register an alias (e.g., entity name) for an ID
    pub fn register_alias(&mut self, alias: impl Into<String>, id: u64) {
        self.id_aliases.insert(alias.into(), id);
    }

    /// Resolve an alias to an ID
    pub fn resolve_alias(&self, alias: &str) -> Option<u64> {
        self.id_aliases.get(alias).copied()
    }

    /// Vector search: find top-k similar nodes
    pub fn search_vector(&self, query: &[f32], k: usize) -> Vec<(u64, f32)> {
        self.vector_index.search(query, k)
    }

    /// Graph expansion: get neighbors up to max_hops
    pub fn expand_graph(&self, id: u64, max_hops: u8) -> Vec<(u64, u8)> {
        self.graph_index.expand(id, max_hops)
    }
}

impl Default for HyperIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hyper_index_vector_graph() {
        let mut index = HyperIndex::new();
        
        index.insert_node(1, vec![1.0, 0.0]);
        index.insert_node(2, vec![0.0, 1.0]);
        index.insert_edge(1, 2, "related", 1.0);
        
        // Vector search
        let results = index.search_vector(&[1.0, 0.0], 1);
        assert_eq!(results[0].0, 1);
        
        // Graph expansion
        let neighbors = index.expand_graph(1, 1);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].0, 2);
    }

    #[test]
    fn test_hyper_index_alias() {
        let mut index = HyperIndex::new();
        index.insert_node(1, vec![1.0]);
        index.register_alias("Alice", 1);
        
        assert_eq!(index.resolve_alias("Alice"), Some(1));
        assert_eq!(index.resolve_alias("Bob"), None);
    }
}
