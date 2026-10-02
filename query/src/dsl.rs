use serde::{Deserialize, Serialize};
use thiserror::Error;

const DEFAULT_DEPTH: u8 = 1;
const DEFAULT_TOP_K: usize = 20;
const MAX_TOP_K: usize = 1_000;
const MAX_DEPTH: u8 = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum QueryMode {
    #[default]
    Answer,
    Evidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize, Default)]
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
    /// Session ID for agentic workflow (session-scoped subgraph).
    #[serde(default)]
    pub session_id: Option<String>,
    /// Time-travel target: YYYY-MM-DD or RFC3339.
    /// When both snapshot_id and time_travel are provided, snapshot_id takes priority.
    #[serde(default)]
    pub time_travel: Option<String>,
}

/// `QueryRequest::default()` always fails `validate()` because `query` is
/// empty. This is intentional: there is no sensible non-empty default query
/// text, so the default is only useful as a builder starting point (e.g.
/// `QueryRequest { query: "...".into(), ..Default::default() }`), never as a
/// directly-submittable request.
impl Default for QueryRequest {
    fn default() -> Self {
        Self {
            query: String::new(),
            filters: QueryFilters::default(),
            traversal: Traversal::default(),
            top_k: default_top_k(),
            mode: QueryMode::default(),
            search_mode: SearchMode::default(),
            model_id: None,
            snapshot_id: None,
            session_id: None,
            time_travel: None,
        }
    }
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
    #[error("session_id must not be empty when provided")]
    InvalidSessionId,
    #[error("time_travel must be YYYY-MM-DD or RFC3339 format")]
    InvalidTimeTravelFormat,
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
        if let Some(session_id) = &self.session_id {
            if session_id.trim().is_empty() {
                return Err(QueryValidationError::InvalidSessionId);
            }
        }
        if let Some(range) = &self.filters.time_range {
            let from = parse_date(&range.from)?;
            let to = parse_date(&range.to)?;
            if from > to {
                return Err(QueryValidationError::InvalidTimeRangeOrder);
            }
        }
        if let Some(time_travel) = &self.time_travel {
            if time_travel.trim().is_empty() || !is_valid_time_travel(time_travel) {
                return Err(QueryValidationError::InvalidTimeTravelFormat);
            }
        }
        Ok(())
    }
}

fn has_empty_values(values: &[String]) -> bool {
    values.iter().any(|value| value.trim().is_empty())
}

/// Strict YYYY-MM-DD check: `chrono::NaiveDate::parse_from_str` accepts
/// non-zero-padded values (e.g. "2024-6-1") which the error message does
/// not advertise, so reject anything that isn't exactly 10 characters.
fn is_strict_ymd(input: &str) -> bool {
    input.len() == 10
}

fn parse_date(input: &str) -> Result<chrono::NaiveDate, QueryValidationError> {
    if !is_strict_ymd(input) {
        return Err(QueryValidationError::InvalidTimeRangeFormat);
    }
    chrono::NaiveDate::parse_from_str(input, "%Y-%m-%d")
        .map_err(|_| QueryValidationError::InvalidTimeRangeFormat)
}

/// Validate time_travel format: accepts YYYY-MM-DD or RFC3339.
fn is_valid_time_travel(input: &str) -> bool {
    // Try YYYY-MM-DD first
    if is_strict_ymd(input) && chrono::NaiveDate::parse_from_str(input, "%Y-%m-%d").is_ok() {
        return true;
    }
    // Try RFC3339 (e.g. "2024-06-01T10:00:00Z")
    if chrono::DateTime::parse_from_rfc3339(input).is_ok() {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_request() -> QueryRequest {
        QueryRequest {
            query: "x".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn default_request_fails_validation_due_to_empty_query() {
        assert_eq!(
            QueryRequest::default().validate(),
            Err(QueryValidationError::EmptyQuery)
        );
    }

    #[test]
    fn empty_session_id_is_rejected() {
        let request = QueryRequest {
            session_id: Some("  ".to_string()),
            ..base_request()
        };
        assert_eq!(
            request.validate(),
            Err(QueryValidationError::InvalidSessionId)
        );
    }

    #[test]
    fn non_empty_session_id_is_accepted() {
        let request = QueryRequest {
            session_id: Some("session-1".to_string()),
            ..base_request()
        };
        assert!(request.validate().is_ok());
    }

    #[test]
    fn non_zero_padded_time_range_date_is_rejected() {
        let request = QueryRequest {
            filters: QueryFilters {
                time_range: Some(TimeRange {
                    from: "2024-6-1".to_string(),
                    to: "2024-12-31".to_string(),
                }),
                ..Default::default()
            },
            ..base_request()
        };
        assert_eq!(
            request.validate(),
            Err(QueryValidationError::InvalidTimeRangeFormat)
        );
    }

    #[test]
    fn zero_padded_time_range_date_is_accepted() {
        let request = QueryRequest {
            filters: QueryFilters {
                time_range: Some(TimeRange {
                    from: "2024-06-01".to_string(),
                    to: "2024-12-31".to_string(),
                }),
                ..Default::default()
            },
            ..base_request()
        };
        assert!(request.validate().is_ok());
    }

    #[test]
    fn non_zero_padded_time_travel_date_is_rejected() {
        let request = QueryRequest {
            time_travel: Some("2024-6-1".to_string()),
            ..base_request()
        };
        assert_eq!(
            request.validate(),
            Err(QueryValidationError::InvalidTimeTravelFormat)
        );
    }

    #[test]
    fn rfc3339_time_travel_is_accepted() {
        let request = QueryRequest {
            time_travel: Some("2024-06-01T10:00:00Z".to_string()),
            ..base_request()
        };
        assert!(request.validate().is_ok());
    }
}
