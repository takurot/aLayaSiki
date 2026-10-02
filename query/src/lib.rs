pub mod dsl;
pub mod engine;
pub mod graphrag;
pub mod planner;
pub mod semantic_cache;

pub use dsl::{QueryMode, QueryRequest, SearchMode};
pub use engine::{QueryEngine, QueryError, QueryResponse};
pub use graphrag::{compute_groundedness, GroundednessInput};
pub use planner::{QueryPlan, QueryPlanner};

/// Convenience alias for results produced by [`QueryEngine`].
pub type QueryResult<T> = Result<T, QueryError>;

pub const SEMANTIC_CACHE_HIT_STEP: &str = "semantic_cache_hit";
