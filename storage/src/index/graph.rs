use std::collections::HashMap;

/// Edge representation: (target_id, relation, weight)
pub type EdgeData = (u64, String, f32);

/// Simple adjacency list graph index
pub struct AdjacencyGraph {
    adjacency: HashMap<u64, Vec<EdgeData>>,
}

impl AdjacencyGraph {
    pub fn new() -> Self {
        Self {
            adjacency: HashMap::new(),
        }
    }

    pub fn add_edge(&mut self, source: u64, target: u64, relation: impl Into<String>, weight: f32) {
        self.adjacency
            .entry(source)
            .or_insert_with(Vec::new)
            .push((target, relation.into(), weight));
    }

    pub fn remove_edge(&mut self, source: u64, target: u64) -> bool {
        if let Some(edges) = self.adjacency.get_mut(&source) {
            let len_before = edges.len();
            edges.retain(|(t, _, _)| *t != target);
            return edges.len() < len_before;
        }
        false
    }

    pub fn remove_node(&mut self, id: u64) {
        // Remove outgoing edges
        self.adjacency.remove(&id);
        // Remove incoming edges
        for edges in self.adjacency.values_mut() {
            edges.retain(|(t, _, _)| *t != id);
        }
    }

    /// Get 1-hop neighbors
    pub fn neighbors(&self, id: u64) -> Vec<&EdgeData> {
        self.adjacency.get(&id)
            .map(|edges| edges.iter().collect())
            .unwrap_or_default()
    }

    /// Get 2-hop neighbors (includes 1-hop)
    pub fn neighbors_2hop(&self, id: u64) -> Vec<(u64, u8)> {
        let mut result: HashMap<u64, u8> = HashMap::new();
        
        // 1-hop
        if let Some(edges) = self.adjacency.get(&id) {
            for (target, _, _) in edges {
                result.insert(*target, 1);
                
                // 2-hop
                if let Some(edges2) = self.adjacency.get(target) {
                    for (target2, _, _) in edges2 {
                        if *target2 != id && !result.contains_key(target2) {
                            result.insert(*target2, 2);
                        }
                    }
                }
            }
        }
        
        result.into_iter().collect()
    }

    pub fn edge_count(&self) -> usize {
        self.adjacency.values().map(|v| v.len()).sum()
    }

    pub fn node_count(&self) -> usize {
        self.adjacency.len()
    }
}

impl Default for AdjacencyGraph {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_graph_1hop() {
        let mut graph = AdjacencyGraph::new();
        graph.add_edge(1, 2, "knows", 1.0);
        graph.add_edge(1, 3, "likes", 0.8);
        
        let neighbors = graph.neighbors(1);
        assert_eq!(neighbors.len(), 2);
    }

    #[test]
    fn test_graph_2hop() {
        let mut graph = AdjacencyGraph::new();
        graph.add_edge(1, 2, "knows", 1.0);
        graph.add_edge(2, 3, "knows", 1.0);
        graph.add_edge(2, 4, "knows", 1.0);
        
        let result = graph.neighbors_2hop(1);
        assert_eq!(result.len(), 3); // 2, 3, 4
        
        let hop1: Vec<_> = result.iter().filter(|(_, h)| *h == 1).collect();
        let hop2: Vec<_> = result.iter().filter(|(_, h)| *h == 2).collect();
        
        assert_eq!(hop1.len(), 1); // Node 2
        assert_eq!(hop2.len(), 2); // Nodes 3, 4
    }

    #[test]
    fn test_graph_remove() {
        let mut graph = AdjacencyGraph::new();
        graph.add_edge(1, 2, "knows", 1.0);
        graph.add_edge(2, 3, "knows", 1.0);
        
        graph.remove_node(2);
        
        assert!(graph.neighbors(1).is_empty());
        assert!(graph.neighbors(2).is_empty());
    }
}
