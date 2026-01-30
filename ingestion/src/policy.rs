use thiserror::Error;

#[derive(Error, Debug)]
pub enum PolicyError {
    #[error("Forbidden content detected: {0}")]
    ForbiddenContent(String),
}

pub trait ContentPolicy: Send + Sync {
    fn apply(&self, text: &str) -> Result<String, PolicyError>;
}

pub struct NoOpPolicy;

impl ContentPolicy for NoOpPolicy {
    fn apply(&self, text: &str) -> Result<String, PolicyError> {
        Ok(text.to_string())
    }
}

pub struct BasicPolicy {
    forbidden_words: Vec<String>,
    mask_pii: bool,
}

impl BasicPolicy {
    pub fn new(forbidden_words: Vec<String>, mask_pii: bool) -> Self {
        Self {
            forbidden_words,
            mask_pii,
        }
    }
}

impl ContentPolicy for BasicPolicy {
    fn apply(&self, text: &str) -> Result<String, PolicyError> {
        let lowered = text.to_lowercase();
        for word in &self.forbidden_words {
            if lowered.contains(&word.to_lowercase()) {
                return Err(PolicyError::ForbiddenContent(word.clone()));
            }
        }

        if self.mask_pii {
            Ok(mask_pii(text))
        } else {
            Ok(text.to_string())
        }
    }
}

fn mask_pii(text: &str) -> String {
    let mut out = Vec::new();
    for token in text.split_whitespace() {
        if looks_like_email(token) {
            out.push("[EMAIL]".to_string());
        } else if looks_like_phone(token) {
            out.push("[PHONE]".to_string());
        } else {
            out.push(token.to_string());
        }
    }
    out.join(" ")
}

fn looks_like_email(token: &str) -> bool {
    let has_at = token.contains('@');
    let has_dot = token.contains('.');
    has_at && has_dot
}

fn looks_like_phone(token: &str) -> bool {
    let digit_count = token.chars().filter(|c| c.is_ascii_digit()).count();
    digit_count >= 7
}
