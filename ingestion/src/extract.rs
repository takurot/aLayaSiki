use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
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
    // `str::split` always yields at least one item (even for an empty string),
    // so `next()` on a non-empty pattern is infallible here; no `unwrap_or` needed.
    let mime = mime_type.split(';').next().unwrap().trim().to_lowercase();
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
    // pdf_extract::extract_text_from_mem panics on some malformed PDFs instead of
    // returning an Err, so it must be run behind catch_unwind to avoid aborting
    // the whole ingestion process on a single corrupt document.
    let result = catch_unwind(AssertUnwindSafe(|| {
        pdf_extract::extract_text_from_mem(bytes)
    }));

    match result {
        Ok(Ok(text)) => {
            if text.trim().is_empty() {
                None
            } else {
                Some(text)
            }
        }
        Ok(Err(err)) => {
            tracing::warn!(error = %err, "pdf text extraction failed");
            None
        }
        Err(panic) => {
            let message = panic
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic".to_string());
            tracing::warn!(panic = %message, "pdf text extraction panicked");
            None
        }
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
    let mut values = Vec::new();

    for field in fields {
        let Some(value) = metadata
            .get(*field)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        else {
            continue;
        };

        if !values.iter().any(|existing| existing == value) {
            values.push(value.to_string());
        }
    }

    if values.is_empty() {
        None
    } else {
        Some(values.join("\n"))
    }
}

#[cfg(test)]
mod tests {
    use super::{extract_audio_text, extract_image_text, extract_pdf_text};
    use std::collections::HashMap;

    #[test]
    fn image_text_combines_all_non_empty_metadata_fields_in_priority_order() {
        let metadata = HashMap::from([
            ("caption".to_string(), "Architecture diagram".to_string()),
            (
                "ocr_text".to_string(),
                "OCR mentions WAL replay".to_string(),
            ),
            (
                "description".to_string(),
                "Storage recovery flow".to_string(),
            ),
        ]);

        let text = extract_image_text(&metadata).unwrap();

        assert_eq!(
            text,
            "OCR mentions WAL replay\nArchitecture diagram\nStorage recovery flow"
        );
    }

    #[test]
    fn audio_text_skips_blank_fields_and_deduplicates_repeated_values() {
        let metadata = HashMap::from([
            ("transcript".to_string(), "Tokyo pilot launch".to_string()),
            ("caption".to_string(), "  ".to_string()),
            ("description".to_string(), "Tokyo pilot launch".to_string()),
        ]);

        let text = extract_audio_text(&metadata).unwrap();

        assert_eq!(text, "Tokyo pilot launch");
    }

    #[test]
    fn pdf_extraction_returns_none_instead_of_panicking_on_malformed_input() {
        // Well-formed PDF syntax but missing the trailer's Root entry, which
        // makes pdf_extract panic internally (see get_catalog in pdf-extract's
        // lib.rs). This must be caught rather than aborting the process.
        let bytes =
            b"%PDF-1.1\n1 0 obj\n<< /Type /Catalog >>\nendobj\ntrailer\n<< /Size 1 >>\n%%EOF";

        let result = extract_pdf_text(bytes);

        assert_eq!(result, None);
    }

    #[test]
    fn pdf_extraction_returns_none_on_garbage_bytes() {
        let bytes = b"not a pdf at all";

        let result = extract_pdf_text(bytes);

        assert_eq!(result, None);
    }
}
