use alayasiki_core::ingest::IngestionRequest;
use ingestion::api::{
    AudioIngestionPayload, ImageIngestionPayload, JsonIngestionPayload, MultipartIngestionPayload,
};
use std::collections::HashMap;

#[test]
fn test_json_payload_for_json_content_maps_to_file_request() {
    let payload = JsonIngestionPayload {
        content: "{\"title\":\"doc\"}".to_string(),
        content_type: "application/json".to_string(),
        metadata: HashMap::new(),
        idempotency_key: Some("json-key".to_string()),
        model_id: Some("embedding-default-v1".to_string()),
    };

    match payload.into_request() {
        IngestionRequest::File {
            filename,
            mime_type,
            idempotency_key,
            model_id,
            ..
        } => {
            assert_eq!(filename, "payload.json");
            assert_eq!(mime_type, "application/json");
            assert_eq!(idempotency_key.as_deref(), Some("json-key"));
            assert_eq!(model_id.as_deref(), Some("embedding-default-v1"));
        }
        other => panic!("expected file request, got {:?}", other),
    }
}

#[test]
fn test_image_payload_into_request_sets_image_modality() {
    let payload = ImageIngestionPayload {
        filename: "graph.png".to_string(),
        content: vec![0x89, 0x50],
        mime_type: "image/png".to_string(),
        metadata: HashMap::new(),
        idempotency_key: Some("image-key".to_string()),
        model_id: None,
    };

    match payload.into_request() {
        IngestionRequest::File {
            metadata,
            mime_type,
            idempotency_key,
            ..
        } => {
            assert_eq!(mime_type, "image/png");
            assert_eq!(metadata.get("modality").map(String::as_str), Some("image"));
            assert_eq!(idempotency_key.as_deref(), Some("image-key"));
        }
        other => panic!("expected file request, got {:?}", other),
    }
}

#[test]
fn test_audio_payload_into_request_sets_audio_modality() {
    let payload = AudioIngestionPayload {
        filename: "voice.wav".to_string(),
        content: vec![0x52, 0x49, 0x46, 0x46],
        mime_type: "audio/wav".to_string(),
        metadata: HashMap::new(),
        idempotency_key: None,
        model_id: Some("embedding-default-v1".to_string()),
    };

    match payload.into_request() {
        IngestionRequest::File {
            metadata,
            model_id,
            mime_type,
            ..
        } => {
            assert_eq!(mime_type, "audio/wav");
            assert_eq!(metadata.get("modality").map(String::as_str), Some("audio"));
            assert_eq!(model_id.as_deref(), Some("embedding-default-v1"));
        }
        other => panic!("expected file request, got {:?}", other),
    }
}

#[test]
fn test_media_payload_from_multipart_preserves_existing_modality() {
    let mut metadata = HashMap::new();
    metadata.insert("modality".to_string(), "custom-modality".to_string());

    let multipart = MultipartIngestionPayload {
        filename: "call.mp3".to_string(),
        content: vec![1, 2, 3],
        mime_type: "audio/mpeg".to_string(),
        metadata,
        idempotency_key: Some("audio-1".to_string()),
        model_id: None,
    };

    let audio_payload: AudioIngestionPayload = multipart.into();
    match audio_payload.into_request() {
        IngestionRequest::File { metadata, .. } => {
            assert_eq!(
                metadata.get("modality").map(String::as_str),
                Some("custom-modality")
            );
        }
        other => panic!("expected file request, got {:?}", other),
    }
}
