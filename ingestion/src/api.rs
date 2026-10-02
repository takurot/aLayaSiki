use alayasiki_core::ingest::IngestionRequest;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ApiPayloadError {
    #[error("invalid mime type for {expected_modality}: {actual_mime_type}")]
    InvalidMediaMimeType {
        expected_modality: &'static str,
        actual_mime_type: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonIngestionPayload {
    pub content: String,
    pub content_type: String,
    pub metadata: HashMap<String, String>,
    pub idempotency_key: Option<String>,
    /// Deprecated: use `embedding_model_id`/`extraction_model_id` instead.
    /// Retained for backward compatibility and aliased to the embedding role only.
    #[serde(default)]
    pub model_id: Option<String>,
    #[serde(default)]
    pub embedding_model_id: Option<String>,
    #[serde(default)]
    pub extraction_model_id: Option<String>,
}

impl JsonIngestionPayload {
    pub fn into_request(self) -> IngestionRequest {
        let normalized_content_type = normalize_mime_type(&self.content_type);
        if normalized_content_type == "application/json" {
            IngestionRequest::File {
                filename: "payload.json".to_string(),
                content: self.content.into_bytes(),
                mime_type: normalized_content_type,
                metadata: self.metadata,
                idempotency_key: self.idempotency_key,
                model_id: self.model_id,
                embedding_model_id: self.embedding_model_id,
                extraction_model_id: self.extraction_model_id,
            }
        } else {
            IngestionRequest::Text {
                content: self.content,
                metadata: self.metadata,
                idempotency_key: self.idempotency_key,
                model_id: self.model_id,
                embedding_model_id: self.embedding_model_id,
                extraction_model_id: self.extraction_model_id,
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
    pub embedding_model_id: Option<String>,
    pub extraction_model_id: Option<String>,
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
            embedding_model_id: self.embedding_model_id,
            extraction_model_id: self.extraction_model_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ImageIngestionPayload {
    pub filename: String,
    pub content: Vec<u8>,
    pub mime_type: String,
    pub metadata: HashMap<String, String>,
    pub idempotency_key: Option<String>,
    pub model_id: Option<String>,
    pub embedding_model_id: Option<String>,
    pub extraction_model_id: Option<String>,
}

impl ImageIngestionPayload {
    pub fn try_into_request(self) -> Result<IngestionRequest, ApiPayloadError> {
        validate_media_mime_type(&self.mime_type, "image")?;

        Ok(IngestionRequest::File {
            filename: self.filename,
            content: self.content,
            mime_type: self.mime_type,
            metadata: with_modality(self.metadata, "image"),
            idempotency_key: self.idempotency_key,
            model_id: self.model_id,
            embedding_model_id: self.embedding_model_id,
            extraction_model_id: self.extraction_model_id,
        })
    }
}

impl From<MultipartIngestionPayload> for ImageIngestionPayload {
    fn from(payload: MultipartIngestionPayload) -> Self {
        Self {
            filename: payload.filename,
            content: payload.content,
            mime_type: payload.mime_type,
            metadata: payload.metadata,
            idempotency_key: payload.idempotency_key,
            model_id: payload.model_id,
            embedding_model_id: payload.embedding_model_id,
            extraction_model_id: payload.extraction_model_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AudioIngestionPayload {
    pub filename: String,
    pub content: Vec<u8>,
    pub mime_type: String,
    pub metadata: HashMap<String, String>,
    pub idempotency_key: Option<String>,
    pub model_id: Option<String>,
    pub embedding_model_id: Option<String>,
    pub extraction_model_id: Option<String>,
}

impl AudioIngestionPayload {
    pub fn try_into_request(self) -> Result<IngestionRequest, ApiPayloadError> {
        validate_media_mime_type(&self.mime_type, "audio")?;

        Ok(IngestionRequest::File {
            filename: self.filename,
            content: self.content,
            mime_type: self.mime_type,
            metadata: with_modality(self.metadata, "audio"),
            idempotency_key: self.idempotency_key,
            model_id: self.model_id,
            embedding_model_id: self.embedding_model_id,
            extraction_model_id: self.extraction_model_id,
        })
    }
}

impl From<MultipartIngestionPayload> for AudioIngestionPayload {
    fn from(payload: MultipartIngestionPayload) -> Self {
        Self {
            filename: payload.filename,
            content: payload.content,
            mime_type: payload.mime_type,
            metadata: payload.metadata,
            idempotency_key: payload.idempotency_key,
            model_id: payload.model_id,
            embedding_model_id: payload.embedding_model_id,
            extraction_model_id: payload.extraction_model_id,
        }
    }
}

fn with_modality(mut metadata: HashMap<String, String>, modality: &str) -> HashMap<String, String> {
    metadata.insert("modality".to_string(), modality.to_string());
    metadata
}

fn normalize_mime_type(mime_type: &str) -> String {
    mime_type
        .split(';')
        .next()
        .expect("split always yields at least one item")
        .trim()
        .to_lowercase()
}

fn validate_media_mime_type(
    mime_type: &str,
    expected_modality: &'static str,
) -> Result<(), ApiPayloadError> {
    let normalized_mime = normalize_mime_type(mime_type);

    let is_match = normalized_mime
        .strip_prefix(expected_modality)
        .is_some_and(|rest| rest.starts_with('/'));

    if is_match {
        Ok(())
    } else {
        Err(ApiPayloadError::InvalidMediaMimeType {
            expected_modality,
            actual_mime_type: mime_type.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json_payload(content_type: &str) -> JsonIngestionPayload {
        JsonIngestionPayload {
            content: "{}".to_string(),
            content_type: content_type.to_string(),
            metadata: HashMap::new(),
            idempotency_key: None,
            model_id: None,
            embedding_model_id: None,
            extraction_model_id: None,
        }
    }

    #[test]
    fn into_request_treats_json_with_charset_param_as_file() {
        let request = json_payload("application/json; charset=utf-8").into_request();
        match request {
            IngestionRequest::File { mime_type, .. } => {
                assert_eq!(mime_type, "application/json");
            }
            IngestionRequest::Text { .. } => panic!("expected File request for JSON content type"),
        }
    }

    #[test]
    fn into_request_normalizes_mixed_case_json_content_type() {
        let request = json_payload("Application/JSON").into_request();
        match request {
            IngestionRequest::File { mime_type, .. } => {
                assert_eq!(mime_type, "application/json");
            }
            IngestionRequest::Text { .. } => panic!("expected File request for JSON content type"),
        }
    }

    #[test]
    fn into_request_treats_plain_text_as_text() {
        let request = json_payload("text/plain").into_request();
        assert!(matches!(request, IngestionRequest::Text { .. }));
    }

    #[test]
    fn validate_media_mime_type_accepts_params_and_mixed_case() {
        assert!(validate_media_mime_type("Image/PNG; charset=binary", "image").is_ok());
    }

    #[test]
    fn validate_media_mime_type_rejects_mismatched_modality() {
        assert_eq!(
            validate_media_mime_type("video/mp4", "image"),
            Err(ApiPayloadError::InvalidMediaMimeType {
                expected_modality: "image",
                actual_mime_type: "video/mp4".to_string(),
            })
        );
    }

    #[test]
    fn validate_media_mime_type_rejects_prefix_without_slash() {
        assert!(validate_media_mime_type("imageography/png", "image").is_err());
    }
}
