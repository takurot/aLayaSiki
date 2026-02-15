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

#[derive(Debug, Clone)]
pub struct ImageIngestionPayload {
    pub filename: String,
    pub content: Vec<u8>,
    pub mime_type: String,
    pub metadata: HashMap<String, String>,
    pub idempotency_key: Option<String>,
    pub model_id: Option<String>,
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
        }
    }
}

fn with_modality(mut metadata: HashMap<String, String>, modality: &str) -> HashMap<String, String> {
    metadata
        .entry("modality".to_string())
        .or_insert_with(|| modality.to_string());
    metadata
}

fn validate_media_mime_type(
    mime_type: &str,
    expected_modality: &'static str,
) -> Result<(), ApiPayloadError> {
    let normalized_mime = mime_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_lowercase();
    let expected_prefix = format!("{expected_modality}/");

    if normalized_mime.starts_with(&expected_prefix) {
        Ok(())
    } else {
        Err(ApiPayloadError::InvalidMediaMimeType {
            expected_modality,
            actual_mime_type: mime_type.to_string(),
        })
    }
}
