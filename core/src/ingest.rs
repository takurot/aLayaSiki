use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IngestionRequest {
    Text {
        content: String,
        metadata: HashMap<String, String>,
        idempotency_key: Option<String>,
        /// Deprecated: ambiguously applied to both the embedding and extraction
        /// model roles. Kept only for backward compatibility with callers that
        /// have not migrated to `embedding_model_id`/`extraction_model_id`.
        /// When set, it is treated as an alias for `embedding_model_id` and is
        /// ignored for extraction routing.
        #[serde(default)]
        model_id: Option<String>,
        #[serde(default)]
        embedding_model_id: Option<String>,
        #[serde(default)]
        extraction_model_id: Option<String>,
    },
    File {
        filename: String,
        content: Vec<u8>,
        mime_type: String,
        metadata: HashMap<String, String>,
        idempotency_key: Option<String>,
        /// Deprecated: ambiguously applied to both the embedding and extraction
        /// model roles. Kept only for backward compatibility with callers that
        /// have not migrated to `embedding_model_id`/`extraction_model_id`.
        /// When set, it is treated as an alias for `embedding_model_id` and is
        /// ignored for extraction routing.
        #[serde(default)]
        model_id: Option<String>,
        #[serde(default)]
        embedding_model_id: Option<String>,
        #[serde(default)]
        extraction_model_id: Option<String>,
    },
}

impl IngestionRequest {
    pub fn text(content: String, metadata: HashMap<String, String>) -> Self {
        Self::Text {
            content,
            metadata,
            idempotency_key: None,
            model_id: None,
            embedding_model_id: None,
            extraction_model_id: None,
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
            embedding_model_id: None,
            extraction_model_id: None,
        }
    }

    pub fn idempotency_key(&self) -> Option<&str> {
        match self {
            IngestionRequest::Text {
                idempotency_key, ..
            } => idempotency_key.as_deref(),
            IngestionRequest::File {
                idempotency_key, ..
            } => idempotency_key.as_deref(),
        }
    }

    /// Deprecated legacy accessor. Prefer `embedding_model_id()`/`extraction_model_id()`.
    pub fn model_id(&self) -> Option<&str> {
        match self {
            IngestionRequest::Text { model_id, .. } => model_id.as_deref(),
            IngestionRequest::File { model_id, .. } => model_id.as_deref(),
        }
    }

    /// Effective embedding model id: the explicit `embedding_model_id`, falling
    /// back to the deprecated `model_id` alias for backward compatibility.
    pub fn embedding_model_id(&self) -> Option<&str> {
        match self {
            IngestionRequest::Text {
                embedding_model_id,
                model_id,
                ..
            }
            | IngestionRequest::File {
                embedding_model_id,
                model_id,
                ..
            } => embedding_model_id.as_deref().or(model_id.as_deref()),
        }
    }

    /// Effective extraction model id: only the explicit `extraction_model_id`.
    /// The deprecated `model_id` alias never routes to extraction, so it can
    /// no longer be forwarded to the extraction worker unintentionally.
    pub fn extraction_model_id(&self) -> Option<&str> {
        match self {
            IngestionRequest::Text {
                extraction_model_id,
                ..
            } => extraction_model_id.as_deref(),
            IngestionRequest::File {
                extraction_model_id,
                ..
            } => extraction_model_id.as_deref(),
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
            IngestionRequest::File {
                content,
                mime_type,
                filename,
                ..
            } => {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedding_and_extraction_model_ids_are_independent_when_both_set() {
        let request = IngestionRequest::Text {
            content: "hello".to_string(),
            metadata: HashMap::new(),
            idempotency_key: None,
            model_id: None,
            embedding_model_id: Some("embed-A".to_string()),
            extraction_model_id: Some("extract-B".to_string()),
        };

        assert_eq!(request.embedding_model_id(), Some("embed-A"));
        assert_eq!(request.extraction_model_id(), Some("extract-B"));
    }

    #[test]
    fn legacy_model_id_aliases_embedding_role_only() {
        let request = IngestionRequest::Text {
            content: "hello".to_string(),
            metadata: HashMap::new(),
            idempotency_key: None,
            model_id: Some("legacy".to_string()),
            embedding_model_id: None,
            extraction_model_id: None,
        };

        assert_eq!(request.embedding_model_id(), Some("legacy"));
        assert_eq!(request.extraction_model_id(), None);
    }

    #[test]
    fn explicit_embedding_model_id_takes_precedence_over_legacy_model_id() {
        let request = IngestionRequest::Text {
            content: "hello".to_string(),
            metadata: HashMap::new(),
            idempotency_key: None,
            model_id: Some("legacy".to_string()),
            embedding_model_id: Some("embed-A".to_string()),
            extraction_model_id: None,
        };

        assert_eq!(request.embedding_model_id(), Some("embed-A"));
    }

    #[test]
    fn no_model_ids_set_returns_none_for_both_roles() {
        let request = IngestionRequest::text("hello".to_string(), HashMap::new());

        assert_eq!(request.embedding_model_id(), None);
        assert_eq!(request.extraction_model_id(), None);
    }
}
