use crate::index::AdjacencyGraph;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Community {
    pub id: usize,
    pub node_ids: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommunityLevel {
    pub level: usize,
    pub communities: Vec<Community>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommunitySummary {
    pub level: usize,
    pub community_id: usize,
    pub top_nodes: Vec<u64>,
    pub summary: String,
}

pub trait CommunitySummarizer: Send + Sync {
    fn summarize(
        &self,
        level: usize,
        community_id: usize,
        node_ids: &[u64],
        top_nodes: &[u64],
    ) -> String;
}

pub struct DeterministicSummarizer;

impl CommunitySummarizer for DeterministicSummarizer {
    fn summarize(
        &self,
        level: usize,
        community_id: usize,
        node_ids: &[u64],
        top_nodes: &[u64],
    ) -> String {
        format!(
            "L{}-C{}: {} nodes, key {:?}",
            level,
            community_id,
            node_ids.len(),
            top_nodes
        )
    }
}

pub struct CommunityEngine {
    graph: AdjacencyGraph,
    hierarchy: Vec<CommunityLevel>,
    summaries: Vec<CommunitySummary>,
    pagerank: HashMap<u64, f64>,
    dirty_nodes: HashSet<u64>,
    max_levels: usize,
}

impl CommunityEngine {
    pub fn new(graph: AdjacencyGraph) -> Self {
        Self {
            graph,
            hierarchy: Vec::new(),
            summaries: Vec::new(),
            pagerank: HashMap::new(),
            dirty_nodes: HashSet::new(),
            max_levels: 3,
        }
    }

    pub fn rebuild_hierarchy(&mut self, max_levels: usize, summarizer: &dyn CommunitySummarizer) {
        self.max_levels = max_levels.max(1);

        let mut level0 = detect_leiden_level(&self.graph);
        if level0.is_empty() {
            level0 = self
                .graph
                .node_ids()
                .into_iter()
                .enumerate()
                .map(|(idx, node_id)| Community {
                    id: idx,
                    node_ids: vec![node_id],
                })
                .collect();
        }

        let mut levels = vec![CommunityLevel {
            level: 0,
            communities: level0.clone(),
        }];

        let mut current = level0;
        for level_idx in 1..self.max_levels {
            if current.len() <= 1 {
                break;
            }

            let super_graph = build_super_graph(&self.graph, &current);
            let super_communities = detect_leiden_level(&super_graph);

            if super_communities.is_empty() || super_communities.len() >= current.len() {
                break;
            }

            let mut next_level = Vec::new();
            for (community_id, super_comm) in super_communities.iter().enumerate() {
                let mut nodes = BTreeSet::new();
                for prev_comm_id in &super_comm.node_ids {
                    if let Some(prev_comm) = current.get(*prev_comm_id as usize) {
                        for node_id in &prev_comm.node_ids {
                            nodes.insert(*node_id);
                        }
                    }
                }

                if !nodes.is_empty() {
                    next_level.push(Community {
                        id: community_id,
                        node_ids: nodes.into_iter().collect(),
                    });
                }
            }

            if next_level.is_empty() || next_level.len() == current.len() {
                break;
            }

            levels.push(CommunityLevel {
                level: level_idx,
                communities: next_level.clone(),
            });
            current = next_level;
        }

        self.hierarchy = levels;
        self.pagerank = compute_pagerank(&self.graph, 30, 0.85);

        let top_nodes = self.fastgraphrag_top_nodes();
        self.summaries = build_summaries(&self.hierarchy, &top_nodes, summarizer);
        self.dirty_nodes.clear();
    }

    pub fn add_edge_incremental(
        &mut self,
        source: u64,
        target: u64,
        relation: impl Into<String>,
        weight: f32,
    ) {
        self.graph.add_edge(source, target, relation, weight);
        self.dirty_nodes.insert(source);
        self.dirty_nodes.insert(target);
    }

    pub fn refresh_incremental(&mut self, summarizer: &dyn CommunitySummarizer) {
        if self.dirty_nodes.is_empty() {
            return;
        }

        // Incremental entry point: current version recomputes from updated graph deterministically.
        self.rebuild_hierarchy(self.max_levels, summarizer);
    }

    pub fn fastgraphrag_top_nodes(&self) -> Vec<u64> {
        if self.pagerank.is_empty() {
            return Vec::new();
        }

        let mut ranked: Vec<(u64, f64)> = self
            .pagerank
            .iter()
            .map(|(id, score)| (*id, *score))
            .collect();
        ranked.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });

        let count = ranked.len();
        let mut top_k = ((count as f64) * 0.10).ceil() as usize;
        if top_k == 0 {
            top_k = 1;
        }
        ranked.truncate(top_k);
        ranked.into_iter().map(|(id, _)| id).collect()
    }

    pub fn hierarchy(&self) -> &[CommunityLevel] {
        &self.hierarchy
    }

    pub fn summaries(&self) -> &[CommunitySummary] {
        &self.summaries
    }
}

fn build_summaries(
    levels: &[CommunityLevel],
    top_nodes: &[u64],
    summarizer: &dyn CommunitySummarizer,
) -> Vec<CommunitySummary> {
    let top_set: HashSet<u64> = top_nodes.iter().copied().collect();
    let mut out = Vec::new();

    for level in levels {
        for community in &level.communities {
            let mut community_top: Vec<u64> = community
                .node_ids
                .iter()
                .copied()
                .filter(|id| top_set.contains(id))
                .collect();
            if community_top.is_empty() && !community.node_ids.is_empty() {
                community_top.push(community.node_ids[0]);
            }

            let summary = summarizer.summarize(
                level.level,
                community.id,
                &community.node_ids,
                &community_top,
            );

            out.push(CommunitySummary {
                level: level.level,
                community_id: community.id,
                top_nodes: community_top,
                summary,
            });
        }
    }

    out
}

fn build_super_graph(graph: &AdjacencyGraph, communities: &[Community]) -> AdjacencyGraph {
    let mut node_to_community = HashMap::new();
    for (community_idx, community) in communities.iter().enumerate() {
        for node_id in &community.node_ids {
            node_to_community.insert(*node_id, community_idx as u64);
        }
    }

    let mut super_graph = AdjacencyGraph::new();

    // Ensure each super-node exists, even without inter-community edges.
    for community_idx in 0..communities.len() {
        super_graph.add_edge(community_idx as u64, community_idx as u64, "self", 0.0);
    }

    for (source, target, weight) in graph.edges() {
        let Some(source_comm) = node_to_community.get(&source).copied() else {
            continue;
        };
        let Some(target_comm) = node_to_community.get(&target).copied() else {
            continue;
        };

        if source_comm != target_comm {
            super_graph.add_edge(source_comm, target_comm, "community_link", weight);
        }
    }

    super_graph
}

fn detect_leiden_level(graph: &AdjacencyGraph) -> Vec<Community> {
    let nodes = graph.node_ids();
    if nodes.is_empty() {
        return Vec::new();
    }

    let undirected = build_undirected_adj(graph);
    let total_weight = total_undirected_weight(&undirected);

    if total_weight <= f64::EPSILON {
        return nodes
            .into_iter()
            .enumerate()
            .map(|(id, node_id)| Community {
                id,
                node_ids: vec![node_id],
            })
            .collect();
    }

    let mut assignment = HashMap::new();
    for (community_id, node_id) in nodes.iter().enumerate() {
        assignment.insert(*node_id, community_id);
    }

    for _ in 0..20 {
        let mut moved = false;

        for node_id in &nodes {
            let current_comm = assignment[node_id];

            let mut candidate_communities = HashSet::new();
            candidate_communities.insert(current_comm);
            if let Some(neighbors) = undirected.get(node_id) {
                for neighbor_id in neighbors.keys() {
                    if let Some(comm) = assignment.get(neighbor_id) {
                        candidate_communities.insert(*comm);
                    }
                }
            }

            let mut best_comm = current_comm;
            let mut best_score = community_affinity(
                *node_id,
                current_comm,
                &undirected,
                &assignment,
                total_weight,
            );

            let mut ordered_candidates: Vec<usize> = candidate_communities.into_iter().collect();
            ordered_candidates.sort_unstable();

            for candidate in ordered_candidates {
                let score =
                    community_affinity(*node_id, candidate, &undirected, &assignment, total_weight);
                if score > best_score + 1e-12 {
                    best_score = score;
                    best_comm = candidate;
                }
            }

            if best_comm != current_comm {
                assignment.insert(*node_id, best_comm);
                moved = true;
            }
        }

        if !moved {
            break;
        }
    }

    let refined = refine_connected_communities(&undirected, &assignment);

    let mut grouped = BTreeMap::<usize, Vec<u64>>::new();
    for node_id in nodes {
        let comm_id = refined[&node_id];
        grouped.entry(comm_id).or_default().push(node_id);
    }

    let mut ordered: Vec<Vec<u64>> = grouped
        .into_values()
        .map(|mut node_ids| {
            node_ids.sort_unstable();
            node_ids
        })
        .collect();

    ordered.sort_by(|a, b| a[0].cmp(&b[0]));

    ordered
        .into_iter()
        .enumerate()
        .map(|(community_id, node_ids)| Community {
            id: community_id,
            node_ids,
        })
        .collect()
}

fn build_undirected_adj(graph: &AdjacencyGraph) -> HashMap<u64, HashMap<u64, f64>> {
    let mut adj: HashMap<u64, HashMap<u64, f64>> = HashMap::new();

    for node_id in graph.node_ids() {
        adj.entry(node_id).or_default();
    }

    for (source, target, weight) in graph.edges() {
        let w = weight as f64;
        *adj.entry(source).or_default().entry(target).or_insert(0.0) += w;
        *adj.entry(target).or_default().entry(source).or_insert(0.0) += w;
    }

    adj
}

fn total_undirected_weight(adj: &HashMap<u64, HashMap<u64, f64>>) -> f64 {
    let total: f64 = adj
        .values()
        .map(|neighbors| neighbors.values().sum::<f64>())
        .sum();
    total / 2.0
}

fn node_degree(node_id: u64, adj: &HashMap<u64, HashMap<u64, f64>>) -> f64 {
    adj.get(&node_id)
        .map(|neighbors| neighbors.values().sum())
        .unwrap_or(0.0)
}

fn community_affinity(
    node_id: u64,
    candidate_comm: usize,
    adj: &HashMap<u64, HashMap<u64, f64>>,
    assignment: &HashMap<u64, usize>,
    total_weight: f64,
) -> f64 {
    let Some(neighbors) = adj.get(&node_id) else {
        return 0.0;
    };

    let k_i: f64 = neighbors.values().sum();
    if k_i <= f64::EPSILON {
        return 0.0;
    }

    let mut k_i_in = 0.0;
    for (neighbor_id, weight) in neighbors {
        if assignment.get(neighbor_id) == Some(&candidate_comm) {
            k_i_in += *weight;
        }
    }

    let mut sum_tot = 0.0;
    for (other_node, comm_id) in assignment {
        if *comm_id == candidate_comm {
            sum_tot += node_degree(*other_node, adj);
        }
    }

    // Leiden-like local move objective (modularity-oriented score).
    k_i_in - (k_i * sum_tot) / (2.0 * total_weight)
}

fn refine_connected_communities(
    adj: &HashMap<u64, HashMap<u64, f64>>,
    assignment: &HashMap<u64, usize>,
) -> HashMap<u64, usize> {
    let mut by_community: BTreeMap<usize, Vec<u64>> = BTreeMap::new();
    for (node_id, comm_id) in assignment {
        by_community.entry(*comm_id).or_default().push(*node_id);
    }

    let mut refined = HashMap::new();
    let mut next_comm_id = 0usize;

    for nodes in by_community.into_values() {
        let components = connected_components(&nodes, adj);
        for component in components {
            for node_id in component {
                refined.insert(node_id, next_comm_id);
            }
            next_comm_id += 1;
        }
    }

    refined
}

fn connected_components(nodes: &[u64], adj: &HashMap<u64, HashMap<u64, f64>>) -> Vec<Vec<u64>> {
    let target_set: HashSet<u64> = nodes.iter().copied().collect();
    let mut visited = HashSet::new();
    let mut components = Vec::new();

    let mut ordered_nodes: Vec<u64> = nodes.to_vec();
    ordered_nodes.sort_unstable();

    for start in ordered_nodes {
        if visited.contains(&start) {
            continue;
        }

        let mut queue = std::collections::VecDeque::new();
        let mut component = Vec::new();

        visited.insert(start);
        queue.push_back(start);

        while let Some(node_id) = queue.pop_front() {
            component.push(node_id);
            if let Some(neighbors) = adj.get(&node_id) {
                let mut neighbor_ids: Vec<u64> = neighbors
                    .keys()
                    .copied()
                    .filter(|neighbor| target_set.contains(neighbor))
                    .collect();
                neighbor_ids.sort_unstable();

                for neighbor_id in neighbor_ids {
                    if visited.insert(neighbor_id) {
                        queue.push_back(neighbor_id);
                    }
                }
            }
        }

        component.sort_unstable();
        components.push(component);
    }

    components
}

fn compute_pagerank(graph: &AdjacencyGraph, iterations: usize, damping: f64) -> HashMap<u64, f64> {
    let nodes = graph.node_ids();
    let n = nodes.len();
    if n == 0 {
        return HashMap::new();
    }

    let n_f64 = n as f64;
    let base = (1.0 - damping) / n_f64;

    let mut out_neighbors: HashMap<u64, Vec<(u64, f64)>> = HashMap::new();
    for node_id in &nodes {
        out_neighbors.entry(*node_id).or_default();
    }
    for (source, target, weight) in graph.edges() {
        out_neighbors
            .entry(source)
            .or_default()
            .push((target, weight as f64));
        out_neighbors.entry(target).or_default();
    }

    for edges in out_neighbors.values_mut() {
        edges.sort_by(|a, b| a.0.cmp(&b.0));
    }

    let mut rank: HashMap<u64, f64> = nodes.iter().copied().map(|id| (id, 1.0 / n_f64)).collect();

    let mut ordered_nodes = nodes;
    ordered_nodes.sort_unstable();

    for _ in 0..iterations {
        let mut next: HashMap<u64, f64> =
            ordered_nodes.iter().copied().map(|id| (id, base)).collect();
        let mut dangling_mass = 0.0;

        for node_id in &ordered_nodes {
            let current_rank = *rank.get(node_id).unwrap_or(&0.0);
            let edges = out_neighbors.get(node_id).cloned().unwrap_or_default();
            let out_sum: f64 = edges.iter().map(|(_, w)| *w).sum();

            if out_sum <= f64::EPSILON {
                dangling_mass += current_rank;
                continue;
            }

            for (target_id, weight) in edges {
                let contribution = damping * current_rank * (weight / out_sum);
                if let Some(value) = next.get_mut(&target_id) {
                    *value += contribution;
                }
            }
        }

        if dangling_mass > 0.0 {
            let distribute = damping * dangling_mass / n_f64;
            for value in next.values_mut() {
                *value += distribute;
            }
        }

        rank = next;
    }

    rank
}

#[cfg(test)]
mod tests {
    use super::*;

    fn graph_for_test() -> AdjacencyGraph {
        let mut graph = AdjacencyGraph::new();
        graph.add_edge(1, 2, "links", 1.0);
        graph.add_edge(2, 3, "links", 1.0);
        graph.add_edge(10, 11, "links", 1.0);
        graph.add_edge(11, 12, "links", 1.0);
        graph
    }

    #[test]
    fn test_detect_leiden_level_returns_communities() {
        let graph = graph_for_test();
        let communities = detect_leiden_level(&graph);
        assert!(!communities.is_empty());
    }

    #[test]
    fn test_pagerank_returns_scores() {
        let graph = graph_for_test();
        let scores = compute_pagerank(&graph, 10, 0.85);
        assert!(!scores.is_empty());
    }
}
