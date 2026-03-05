pub mod langchain;
pub mod llama_index;

use query::dsl::{QueryValidationError, Traversal};
use query::QueryRequest;
use std::sync::OnceLock;

const FALLBACK_MAX_TOP_K: usize = 1_000;
const FALLBACK_MAX_DEPTH: u8 = 8;

static QUERY_MAX_TOP_K: OnceLock<usize> = OnceLock::new();
static QUERY_MAX_DEPTH: OnceLock<u8> = OnceLock::new();

pub(crate) fn normalize_top_k(value: usize) -> usize {
    let max_top_k = *QUERY_MAX_TOP_K.get_or_init(detect_query_max_top_k);
    if value == 0 {
        1
    } else {
        value.min(max_top_k)
    }
}

pub(crate) fn normalize_depth(value: u8) -> u8 {
    let max_depth = *QUERY_MAX_DEPTH.get_or_init(detect_query_max_depth);
    if value == 0 {
        1
    } else {
        value.min(max_depth)
    }
}

fn detect_query_max_top_k() -> usize {
    let probe = QueryRequest {
        query: "probe".to_string(),
        top_k: usize::MAX,
        ..QueryRequest::default()
    };
    match probe.validate() {
        Err(QueryValidationError::InvalidTopK(max)) => max,
        _ => FALLBACK_MAX_TOP_K,
    }
}

fn detect_query_max_depth() -> u8 {
    let probe = QueryRequest {
        query: "probe".to_string(),
        top_k: 1,
        traversal: Traversal {
            depth: u8::MAX,
            relation_types: Vec::new(),
        },
        ..QueryRequest::default()
    };
    match probe.validate() {
        Err(QueryValidationError::InvalidDepth(max)) => max,
        _ => FALLBACK_MAX_DEPTH,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_top_k_uses_query_limit() {
        let expected_max = detect_query_max_top_k();
        assert_eq!(normalize_top_k(0), 1);
        assert_eq!(normalize_top_k(usize::MAX), expected_max);
    }

    #[test]
    fn normalize_depth_uses_query_limit() {
        let expected_max = detect_query_max_depth();
        assert_eq!(normalize_depth(0), 1);
        assert_eq!(normalize_depth(u8::MAX), expected_max);
    }
}
