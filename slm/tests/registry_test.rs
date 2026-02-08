use slm::ner::{Entity, EntityExtractor};
use slm::registry::{ModelRegistry, RegistryError};
use std::sync::Arc;

struct StaticExtractor {
    entities: Vec<Entity>,
}

#[async_trait::async_trait]
impl EntityExtractor for StaticExtractor {
    async fn extract(&self, _text: &str) -> anyhow::Result<Vec<Entity>> {
        Ok(self.entities.clone())
    }
}

#[test]
fn test_registry_register_activate_and_resolve() {
    let mut registry = ModelRegistry::new();
    registry
        .register(
            "triplex-lite",
            "1.0.0",
            Arc::new(StaticExtractor {
                entities: vec![Entity {
                    text: "Rust".to_string(),
                    label: "Language".to_string(),
                    confidence: 0.9,
                }],
            }),
        )
        .unwrap();

    registry.activate("triplex-lite", "1.0.0").unwrap();

    let resolved = registry.resolve("triplex-lite").unwrap();
    assert_eq!(resolved.model_id, "triplex-lite");
    assert_eq!(resolved.version, "1.0.0");
}

#[test]
fn test_registry_rollback_restores_previous_active_version() {
    let mut registry = ModelRegistry::new();
    registry
        .register(
            "glm-flash-lite",
            "1.0.0",
            Arc::new(StaticExtractor { entities: vec![] }),
        )
        .unwrap();
    registry
        .register(
            "glm-flash-lite",
            "1.1.0",
            Arc::new(StaticExtractor { entities: vec![] }),
        )
        .unwrap();

    registry.activate("glm-flash-lite", "1.0.0").unwrap();
    registry.activate("glm-flash-lite", "1.1.0").unwrap();
    let rolled_back = registry.rollback("glm-flash-lite").unwrap();

    assert_eq!(rolled_back.version, "1.0.0");
}

#[test]
fn test_registry_resolve_missing_model_returns_error() {
    let registry = ModelRegistry::new();
    let result = registry.resolve("missing-model");
    assert!(matches!(result, Err(RegistryError::ModelNotFound(_))));
}
