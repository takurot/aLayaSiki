use storage::community::{CommunityEngine, DeterministicSummarizer};
use storage::index::AdjacencyGraph;

fn sample_graph_two_clusters() -> AdjacencyGraph {
    let mut graph = AdjacencyGraph::new();

    // Cluster A
    graph.add_edge(1, 2, "links", 1.0);
    graph.add_edge(2, 3, "links", 1.0);
    graph.add_edge(1, 3, "links", 1.0);

    // Cluster B
    graph.add_edge(10, 11, "links", 1.0);
    graph.add_edge(11, 12, "links", 1.0);
    graph.add_edge(10, 12, "links", 1.0);

    // Weak bridge
    graph.add_edge(3, 10, "bridge", 0.1);
    graph
}

#[test]
fn test_leiden_detects_multiple_communities() {
    let graph = sample_graph_two_clusters();
    let mut engine = CommunityEngine::new(graph);
    engine.rebuild_hierarchy(3, &DeterministicSummarizer);

    let level0 = &engine.hierarchy()[0];
    assert!(level0.communities.len() >= 2);
}

#[test]
fn test_hierarchical_levels_are_generated() {
    let graph = sample_graph_two_clusters();
    let mut engine = CommunityEngine::new(graph);
    engine.rebuild_hierarchy(4, &DeterministicSummarizer);

    assert!(!engine.hierarchy().is_empty());
    assert!(!engine.summaries().is_empty());
}

#[test]
fn test_fastgraphrag_selects_top_10_percent_nodes() {
    let graph = sample_graph_two_clusters();
    let mut engine = CommunityEngine::new(graph);
    engine.rebuild_hierarchy(3, &DeterministicSummarizer);

    let top_nodes = engine.fastgraphrag_top_nodes();
    assert!(!top_nodes.is_empty());
}

#[test]
fn test_incremental_update_refreshes_summaries() {
    let graph = sample_graph_two_clusters();
    let mut engine = CommunityEngine::new(graph);
    engine.rebuild_hierarchy(3, &DeterministicSummarizer);

    engine.add_edge_incremental(12, 13, "links", 1.0);
    engine.refresh_incremental(&DeterministicSummarizer);

    assert!(!engine.summaries().is_empty());
    let contains_new_node = engine
        .hierarchy()
        .iter()
        .flat_map(|level| level.communities.iter())
        .any(|community| community.node_ids.contains(&13));
    assert!(contains_new_node);
}
