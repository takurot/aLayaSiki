use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use sha2::{Sha256, Digest};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IngestionRequest {
    Text {
        content: String,
        metadata: HashMap<String, String>,
        idempotency_key: Option<String>,
        model_id: Option<String>,
    },
    File {
        filename: String,
        content: Vec<u8>,
        mime_type: String,
        metadata: HashMap<String, String>,
        idempotency_key: Option<String>,
        model_id: Option<String>,
    },
}

impl IngestionRequest {
    pub fn text(content: String, metadata: HashMap<String, String>) -> Self {
        Self::Text {
            content,
            metadata,
            idempotency_key: None,
            model_id: None,
        }
    }

    pub fn file(
        filename: String,
        content: Vec<u8>,
        mime_type: String,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self::File {
            filename,
            content,
            mime_type,
            metadata,
            idempotency_key: None,
            model_id: None,
        }
    }

    pub fn idempotency_key(&self) -> Option<&str> {
        match self {
            IngestionRequest::Text { idempotency_key, .. } => idempotency_key.as_deref(),
            IngestionRequest::File { idempotency_key, .. } => idempotency_key.as_deref(),
        }
    }

    pub fn model_id(&self) -> Option<&str> {
        match self {
            IngestionRequest::Text { model_id, .. } => model_id.as_deref(),
            IngestionRequest::File { model_id, .. } => model_id.as_deref(),
        }
    }

    pub fn metadata(&self) -> &HashMap<String, String> {
        match self {
            IngestionRequest::Text { metadata, .. } => metadata,
            IngestionRequest::File { metadata, .. } => metadata,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chunk {
    pub content: String,
    pub metadata: HashMap<String, String>,
    pub embedding: Option<Vec<f32>>, // Placeholder for now
}

pub trait ContentHash {
    fn content_hash(&self) -> String;
}

impl ContentHash for IngestionRequest {
    fn content_hash(&self) -> String {
        let mut hasher = Sha256::new();
        match self {
            IngestionRequest::Text { content, .. } => {
                hasher.update(b"text");
                hasher.update(content.as_bytes());
            }
            IngestionRequest::File { content, mime_type, filename, .. } => {
                hasher.update(b"file");
                hasher.update(mime_type.as_bytes());
                hasher.update(filename.as_bytes());
                hasher.update(content);
            }
        }
        format!("{:x}", hasher.finalize())
    }
}

impl ContentHash for Chunk {
    fn content_hash(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.content.as_bytes());
        format!("{:x}", hasher.finalize())
    }
}
