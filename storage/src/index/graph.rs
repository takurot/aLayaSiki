use std::collections::HashMap;

/// Edge representation: (target_id, relation, weight)
pub type EdgeData = (u64, String, f32);

/// Simple adjacency list graph index
#[derive(Clone, Debug)]
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
            .or_default()
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
        self.adjacency
            .get(&id)
            .map(|edges| edges.iter().collect())
            .unwrap_or_default()
    }

    /// Get neighbors within max_hops (BFS)
    /// Returns a list of (node_id, distance)
    pub fn expand(&self, start_id: u64, max_hops: u8) -> Vec<(u64, u8)> {
        if max_hops == 0 {
            return vec![];
        }

        let mut visited = HashMap::new();
        let mut queue = std::collections::VecDeque::new();

        // (node_id, current_distance)
        // Note: we track visited nodes but don't include start_id in result unless it's a cycle (which BFS wouldn't restart anyway)
        // To strictly match "neighbors", we usually exclude start_id unless it has a self-loop.
        // Here we just want unique neighbors found at dist 1..=max_hops.

        visited.insert(start_id, 0); // Mark start as visited at dist 0
        queue.push_back((start_id, 0));

        let mut result = Vec::new();

        while let Some((curr_id, dist)) = queue.pop_front() {
            if dist >= max_hops {
                continue;
            }

            if let Some(edges) = self.adjacency.get(&curr_id) {
                for (target, _, _) in edges {
                    if !visited.contains_key(target) {
                        visited.insert(*target, dist + 1);
                        result.push((*target, dist + 1));
                        queue.push_back((*target, dist + 1));
                    }
                }
            }
        }

        result
    }

    /// Get 2-hop neighbors (includes 1-hop) - Legacy wrapper around expand
    pub fn neighbors_2hop(&self, id: u64) -> Vec<(u64, u8)> {
        self.expand(id, 2)
    }

    pub fn edge_count(&self) -> usize {
        self.adjacency.values().map(|v| v.len()).sum()
    }

    pub fn edges(&self) -> Vec<(u64, u64, f32)> {
        let mut out = Vec::new();
        for (source, edges) in &self.adjacency {
            for (target, _relation, weight) in edges {
                out.push((*source, *target, *weight));
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        out
    }

    pub fn node_ids(&self) -> Vec<u64> {
        let mut nodes = std::collections::BTreeSet::new();
        for (source, edges) in &self.adjacency {
            nodes.insert(*source);
            for (target, _, _) in edges {
                nodes.insert(*target);
            }
        }
        nodes.into_iter().collect()
    }

    pub fn contains_node(&self, id: u64) -> bool {
        if self.adjacency.contains_key(&id) {
            return true;
        }
        self.adjacency
            .values()
            .any(|edges| edges.iter().any(|(target, _, _)| *target == id))
    }

    pub fn node_count(&self) -> usize {
        self.node_ids().len()
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
