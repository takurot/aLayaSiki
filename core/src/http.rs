//! Versioned HTTP response contracts shared by server and clients.
use crate::error::ErrorCode;
use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ApiError>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct ApiError {
    pub code: ErrorCode,
    pub message: String,
    pub retryable: bool,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct IngestResponse {
    pub node_ids: Vec<u64>,
    pub snapshot_id: String,
}
