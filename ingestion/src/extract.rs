use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentKind {
    Text,
    Markdown,
    Json,
    Pdf,
    Image,
    Audio,
    Unsupported,
}

pub fn detect_content_kind(mime_type: &str, filename: Option<&str>) -> ContentKind {
    let mime = mime_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_lowercase();
    if mime.starts_with("image/") {
        return ContentKind::Image;
    }
    if mime.starts_with("audio/") {
        return ContentKind::Audio;
    }
    match mime.as_str() {
        "text/plain" => ContentKind::Text,
        "text/markdown" => ContentKind::Markdown,
        "application/json" => ContentKind::Json,
        "application/pdf" => ContentKind::Pdf,
        _ => {
            if let Some(name) = filename {
                let ext = Path::new(name)
                    .extension()
                    .and_then(|e| e.to_str())
                    .unwrap_or("")
                    .to_lowercase();
                match ext.as_str() {
                    "txt" => ContentKind::Text,
                    "md" | "markdown" => ContentKind::Markdown,
                    "json" => ContentKind::Json,
                    "pdf" => ContentKind::Pdf,
                    "png" | "jpg" | "jpeg" | "gif" | "bmp" | "webp" | "svg" => ContentKind::Image,
                    "wav" | "mp3" | "mpeg" | "m4a" | "ogg" | "flac" => ContentKind::Audio,
                    _ => ContentKind::Unsupported,
                }
            } else {
                ContentKind::Unsupported
            }
        }
    }
}

pub fn extract_utf8(bytes: &[u8]) -> Result<String, std::string::FromUtf8Error> {
    String::from_utf8(bytes.to_vec())
}

pub fn extract_pdf_text(bytes: &[u8]) -> Option<String> {
    // pdf-extract panics on some errors/signals, and handles bytes via Cursor?
    // pdf_extract::extract_text_from_mem (if available) or generic read

    // pdf-extract 0.7 API: extract_text(path) or extract_text_from_mem(bytes)
    match pdf_extract::extract_text_from_mem(bytes) {
        Ok(text) => {
            if text.trim().is_empty() {
                None
            } else {
                Some(text)
            }
        }
        Err(_) => None,
    }
}

pub fn extract_image_text(metadata: &HashMap<String, String>) -> Option<String> {
    extract_metadata_text(
        metadata,
        &["ocr_text", "caption", "alt_text", "description"],
    )
}

pub fn extract_audio_text(metadata: &HashMap<String, String>) -> Option<String> {
    extract_metadata_text(metadata, &["transcript", "caption", "description"])
}

fn extract_metadata_text(metadata: &HashMap<String, String>, fields: &[&str]) -> Option<String> {
    fields.iter().find_map(|field| {
        metadata
            .get(*field)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    })
}
