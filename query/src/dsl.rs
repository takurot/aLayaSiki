use serde::{Deserialize, Serialize};
use thiserror::Error;

const DEFAULT_DEPTH: u8 = 1;
const DEFAULT_TOP_K: usize = 20;
const MAX_TOP_K: usize = 1_000;
const MAX_DEPTH: u8 = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum QueryMode {
    #[default]
    Answer,
    Evidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SearchMode {
    Local,
    Global,
    Drift,
    #[default]
    Auto,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct TimeRange {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Default)]
pub struct QueryFilters {
    #[serde(default)]
    pub entity_type: Vec<String>,
    #[serde(default)]
    pub relation_type: Vec<String>,
    #[serde(default)]
    pub time_range: Option<TimeRange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Traversal {
    #[serde(default = "default_depth")]
    pub depth: u8,
    #[serde(default)]
    pub relation_types: Vec<String>,
}

impl Default for Traversal {
    fn default() -> Self {
        Self {
            depth: default_depth(),
            relation_types: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct QueryRequest {
    pub query: String,
    #[serde(default)]
    pub filters: QueryFilters,
    #[serde(default)]
    pub traversal: Traversal,
    #[serde(default = "default_top_k")]
    pub top_k: usize,
    #[serde(default)]
    pub mode: QueryMode,
    #[serde(default)]
    pub search_mode: SearchMode,
    #[serde(default)]
    pub model_id: Option<String>,
    #[serde(default)]
    pub snapshot_id: Option<String>,
}

const fn default_depth() -> u8 {
    DEFAULT_DEPTH
}

const fn default_top_k() -> usize {
    DEFAULT_TOP_K
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum QueryValidationError {
    #[error("query must not be empty")]
    EmptyQuery,
    #[error("top_k must be between 1 and {0}")]
    InvalidTopK(usize),
    #[error("traversal.depth must be between 1 and {0}")]
    InvalidDepth(u8),
    #[error("filters.entity_type must not contain empty values")]
    InvalidEntityTypeFilter,
    #[error("filters.relation_type must not contain empty values")]
    InvalidRelationTypeFilter,
    #[error("traversal.relation_types must not contain empty values")]
    InvalidTraversalRelationTypes,
    #[error("filters.time_range.from/to must be YYYY-MM-DD")]
    InvalidTimeRangeFormat,
    #[error("filters.time_range.from must be <= filters.time_range.to")]
    InvalidTimeRangeOrder,
    #[error("model_id must not be empty when provided")]
    InvalidModelId,
    #[error("snapshot_id must not be empty when provided")]
    InvalidSnapshotId,
}

impl QueryRequest {
    pub fn parse_json(raw: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(raw)
    }

    pub fn validate(&self) -> Result<(), QueryValidationError> {
        if self.query.trim().is_empty() {
            return Err(QueryValidationError::EmptyQuery);
        }
        if self.top_k == 0 || self.top_k > MAX_TOP_K {
            return Err(QueryValidationError::InvalidTopK(MAX_TOP_K));
        }
        if self.traversal.depth == 0 || self.traversal.depth > MAX_DEPTH {
            return Err(QueryValidationError::InvalidDepth(MAX_DEPTH));
        }
        if has_empty_values(&self.filters.entity_type) {
            return Err(QueryValidationError::InvalidEntityTypeFilter);
        }
        if has_empty_values(&self.filters.relation_type) {
            return Err(QueryValidationError::InvalidRelationTypeFilter);
        }
        if has_empty_values(&self.traversal.relation_types) {
            return Err(QueryValidationError::InvalidTraversalRelationTypes);
        }
        if let Some(model_id) = &self.model_id {
            if model_id.trim().is_empty() {
                return Err(QueryValidationError::InvalidModelId);
            }
        }
        if let Some(snapshot_id) = &self.snapshot_id {
            if snapshot_id.trim().is_empty() {
                return Err(QueryValidationError::InvalidSnapshotId);
            }
        }
        if let Some(range) = &self.filters.time_range {
            let from = parse_date(&range.from)?;
            let to = parse_date(&range.to)?;
            if from > to {
                return Err(QueryValidationError::InvalidTimeRangeOrder);
            }
        }
        Ok(())
    }
}

fn has_empty_values(values: &[String]) -> bool {
    values.iter().any(|value| value.trim().is_empty())
}

fn parse_date(input: &str) -> Result<chrono::NaiveDate, QueryValidationError> {
    chrono::NaiveDate::parse_from_str(input, "%Y-%m-%d")
        .map_err(|_| QueryValidationError::InvalidTimeRangeFormat)
}
