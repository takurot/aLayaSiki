use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Entity {
    pub text: String,
    pub label: String,
    pub confidence: f32,
}

#[async_trait]
pub trait EntityExtractor: Send + Sync {
    async fn extract(&self, text: &str) -> anyhow::Result<Vec<Entity>>;
}

pub struct MockEntityExtractor {
    keywords: Vec<(String, String)>, // (keyword, label)
}

impl MockEntityExtractor {
    pub fn new() -> Self {
        Self {
            keywords: vec![
                ("Rust".to_string(), "Language".to_string()),
                ("Python".to_string(), "Language".to_string()),
                ("AI".to_string(), "Topic".to_string()),
                ("Database".to_string(), "Topic".to_string()),
                ("Graph".to_string(), "Concept".to_string()),
                ("Vector".to_string(), "Concept".to_string()),
            ],
        }
    }
}

impl Default for MockEntityExtractor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EntityExtractor for MockEntityExtractor {
    async fn extract(&self, text: &str) -> anyhow::Result<Vec<Entity>> {
        let mut entities = Vec::new();
        // Simple case-insensitive match
        let lower_text = text.to_lowercase();

        for (keyword, label) in &self.keywords {
            if lower_text.contains(&keyword.to_lowercase()) {
                entities.push(Entity {
                    text: keyword.clone(),
                    label: label.clone(),
                    confidence: 0.9,
                });
            }
        }

        Ok(entities)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_extraction() {
        let extractor = MockEntityExtractor::new();
        let text = "I love programming in Rust and building AI systems.";
        let entities = extractor.extract(text).await.unwrap();

        assert!(entities
            .iter()
            .any(|e| e.text == "Rust" && e.label == "Language"));
        assert!(entities
            .iter()
            .any(|e| e.text == "AI" && e.label == "Topic"));
    }
}
