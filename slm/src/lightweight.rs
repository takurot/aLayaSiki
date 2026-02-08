use crate::ner::{Entity, EntityExtractor};
use crate::registry::{ModelRegistry, RegistryError};
use std::sync::Arc;

pub const TRIPLEX_LITE_MODEL: &str = "triplex-lite";
pub const GLM_FLASH_LITE_MODEL: &str = "glm-4-flash-lite";

pub struct TriplexLiteExtractor;

#[async_trait::async_trait]
impl EntityExtractor for TriplexLiteExtractor {
    async fn extract(&self, text: &str) -> anyhow::Result<Vec<Entity>> {
        let mut entities = Vec::new();
        let lower = text.to_lowercase();

        for keyword in ["acquired", "merged", "invested", "partnership"] {
            if lower.contains(keyword) {
                entities.push(Entity {
                    text: keyword.to_string(),
                    label: "RelationSignal".to_string(),
                    confidence: 0.86,
                });
            }
        }

        for keyword in ["company", "organization", "startup"] {
            if lower.contains(keyword) {
                entities.push(Entity {
                    text: keyword.to_string(),
                    label: "EntityHint".to_string(),
                    confidence: 0.82,
                });
            }
        }

        if lower.contains("rust") {
            entities.push(Entity {
                text: "rust".to_string(),
                label: "TechnicalTopic".to_string(),
                confidence: 0.8,
            });
        }

        Ok(entities)
    }
}

pub struct GlmFlashLiteExtractor;

#[async_trait::async_trait]
impl EntityExtractor for GlmFlashLiteExtractor {
    async fn extract(&self, text: &str) -> anyhow::Result<Vec<Entity>> {
        let mut entities = Vec::new();
        let lower = text.to_lowercase();

        for keyword in ["graph", "vector", "database", "index", "query"] {
            if lower.contains(keyword) {
                entities.push(Entity {
                    text: keyword.to_string(),
                    label: "TechnicalTopic".to_string(),
                    confidence: 0.84,
                });
            }
        }

        Ok(entities)
    }
}

pub fn register_default_lightweight_models(
    registry: &mut ModelRegistry,
) -> Result<(), RegistryError> {
    registry.register(TRIPLEX_LITE_MODEL, "1.0.0", Arc::new(TriplexLiteExtractor))?;
    registry.register(
        GLM_FLASH_LITE_MODEL,
        "1.0.0",
        Arc::new(GlmFlashLiteExtractor),
    )?;

    // Keep Triplex as default extraction model for cost-focused extraction first.
    registry.activate(TRIPLEX_LITE_MODEL, "1.0.0")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_triplex_extractor_returns_relation_signals() {
        let extractor = TriplexLiteExtractor;
        let out = extractor
            .extract("The startup acquired another company in a partnership")
            .await
            .unwrap();

        assert!(!out.is_empty());
        assert!(out.iter().any(|e| e.label == "RelationSignal"));
    }

    #[tokio::test]
    async fn test_glm_flash_extractor_returns_technical_topics() {
        let extractor = GlmFlashLiteExtractor;
        let out = extractor
            .extract("Graph database query over vector index")
            .await
            .unwrap();

        assert!(out.iter().any(|e| e.label == "TechnicalTopic"));
    }
}
