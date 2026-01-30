use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use sha2::{Sha256, Digest};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IngestionRequest {
    Text {
        content: String,
        metadata: HashMap<String, String>,
    },
    File {
        filename: String,
        content: Vec<u8>,
        mime_type: String,
        metadata: HashMap<String, String>,
    },
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
            IngestionRequest::File { content, .. } => {
                hasher.update(b"file");
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
