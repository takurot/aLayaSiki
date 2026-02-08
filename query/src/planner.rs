use crate::dsl::{QueryRequest, SearchMode};

const GLOBAL_KEYWORDS: [&str; 10] = [
    "全体",
    "主要テーマ",
    "総括",
    "包括",
    "俯瞰",
    "global",
    "overall",
    "theme",
    "themes",
    "summary",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPlan {
    pub effective_search_mode: SearchMode,
    pub vector_top_k: usize,
    pub expansion_depth: u8,
    pub steps: Vec<&'static str>,
}

pub struct QueryPlanner;

impl QueryPlanner {
    pub fn plan(request: &QueryRequest) -> QueryPlan {
        let effective_search_mode = match request.search_mode {
            SearchMode::Auto => infer_auto_mode(&request.query),
            mode => mode,
        };
        let expansion_depth = match effective_search_mode {
            SearchMode::Global => request.traversal.depth.max(2),
            SearchMode::Drift => request.traversal.depth.max(2).saturating_add(1).min(8),
            SearchMode::Local => request.traversal.depth.max(1),
            SearchMode::Auto => request.traversal.depth.max(1),
        };
        let vector_top_k = match effective_search_mode {
            SearchMode::Global => request.top_k.max(10),
            SearchMode::Drift => request.top_k.max(5),
            _ => request.top_k.max(1),
        };

        QueryPlan {
            effective_search_mode,
            vector_top_k,
            expansion_depth,
            steps: vec!["vector_search", "graph_expansion", "context_pruning"],
        }
    }
}

fn infer_auto_mode(query: &str) -> SearchMode {
    let normalized = query.to_lowercase();
    if GLOBAL_KEYWORDS
        .iter()
        .any(|keyword| normalized.contains(&keyword.to_lowercase()))
    {
        SearchMode::Global
    } else {
        SearchMode::Local
    }
}
