use alayasiki_core::ingest::IngestionRequest;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonIngestionPayload {
    pub content: String,
    pub content_type: String,
    pub metadata: HashMap<String, String>,
    pub idempotency_key: Option<String>,
    pub model_id: Option<String>,
}

impl JsonIngestionPayload {
    pub fn into_request(self) -> IngestionRequest {
        let content_type = self.content_type.to_lowercase();
        if content_type == "application/json" {
            IngestionRequest::File {
                filename: "payload.json".to_string(),
                content: self.content.into_bytes(),
                mime_type: self.content_type,
                metadata: self.metadata,
                idempotency_key: self.idempotency_key,
                model_id: self.model_id,
            }
        } else {
            IngestionRequest::Text {
                content: self.content,
                metadata: self.metadata,
                idempotency_key: self.idempotency_key,
                model_id: self.model_id,
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct MultipartIngestionPayload {
    pub filename: String,
    pub content: Vec<u8>,
    pub mime_type: String,
    pub metadata: HashMap<String, String>,
    pub idempotency_key: Option<String>,
    pub model_id: Option<String>,
}

impl MultipartIngestionPayload {
    pub fn into_request(self) -> IngestionRequest {
        IngestionRequest::File {
            filename: self.filename,
            content: self.content,
            mime_type: self.mime_type,
            metadata: self.metadata,
            idempotency_key: self.idempotency_key,
            model_id: self.model_id,
        }
    }
}
