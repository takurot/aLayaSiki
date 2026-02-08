use crate::ner::EntityExtractor;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RegistryError {
    #[error("model not found: {0}")]
    ModelNotFound(String),
    #[error("version not found for model {model_id}: {version}")]
    VersionNotFound { model_id: String, version: String },
    #[error("model version already exists for model {model_id}: {version}")]
    VersionAlreadyExists { model_id: String, version: String },
    #[error("rollback target is not available for model: {0}")]
    NoRollbackTarget(String),
}

#[derive(Clone)]
pub struct ResolvedModel {
    pub model_id: String,
    pub version: String,
    pub extractor: Arc<dyn EntityExtractor>,
}

#[derive(Default)]
struct ModelFamily {
    versions: BTreeMap<String, Arc<dyn EntityExtractor>>,
    active_version: Option<String>,
    activation_history: Vec<String>,
}

#[derive(Default)]
pub struct ModelRegistry {
    families: HashMap<String, ModelFamily>,
}

impl ModelRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &mut self,
        model_id: impl Into<String>,
        version: impl Into<String>,
        extractor: Arc<dyn EntityExtractor>,
    ) -> Result<(), RegistryError> {
        let model_id = model_id.into();
        let version = version.into();
        let family = self.families.entry(model_id.clone()).or_default();

        if family.versions.contains_key(&version) {
            return Err(RegistryError::VersionAlreadyExists { model_id, version });
        }

        family.versions.insert(version.clone(), extractor);

        // First registered version becomes active by default.
        if family.active_version.is_none() {
            family.active_version = Some(version.clone());
            family.activation_history.push(version);
        }

        Ok(())
    }

    pub fn activate(
        &mut self,
        model_id: &str,
        version: &str,
    ) -> Result<ResolvedModel, RegistryError> {
        let family = self
            .families
            .get_mut(model_id)
            .ok_or_else(|| RegistryError::ModelNotFound(model_id.to_string()))?;

        let extractor = family.versions.get(version).cloned().ok_or_else(|| {
            RegistryError::VersionNotFound {
                model_id: model_id.to_string(),
                version: version.to_string(),
            }
        })?;

        let current = family.active_version.as_deref();
        if current != Some(version) {
            family.active_version = Some(version.to_string());
            family.activation_history.push(version.to_string());
        }

        Ok(ResolvedModel {
            model_id: model_id.to_string(),
            version: version.to_string(),
            extractor,
        })
    }

    pub fn resolve(&self, model_ref: &str) -> Result<ResolvedModel, RegistryError> {
        let (model_id, pinned_version) = parse_model_ref(model_ref);
        let family = self
            .families
            .get(model_id)
            .ok_or_else(|| RegistryError::ModelNotFound(model_id.to_string()))?;

        let version = match pinned_version {
            Some(v) => v.to_string(),
            None => family
                .active_version
                .clone()
                .ok_or_else(|| RegistryError::ModelNotFound(model_id.to_string()))?,
        };

        let extractor = family.versions.get(&version).cloned().ok_or_else(|| {
            RegistryError::VersionNotFound {
                model_id: model_id.to_string(),
                version: version.clone(),
            }
        })?;

        Ok(ResolvedModel {
            model_id: model_id.to_string(),
            version,
            extractor,
        })
    }

    pub fn rollback(&mut self, model_id: &str) -> Result<ResolvedModel, RegistryError> {
        let family = self
            .families
            .get_mut(model_id)
            .ok_or_else(|| RegistryError::ModelNotFound(model_id.to_string()))?;

        if family.activation_history.len() < 2 {
            return Err(RegistryError::NoRollbackTarget(model_id.to_string()));
        }

        // Pop current, restore previous.
        family.activation_history.pop();
        let previous = family
            .activation_history
            .last()
            .cloned()
            .ok_or_else(|| RegistryError::NoRollbackTarget(model_id.to_string()))?;

        family.active_version = Some(previous.clone());

        let extractor = family.versions.get(&previous).cloned().ok_or_else(|| {
            RegistryError::VersionNotFound {
                model_id: model_id.to_string(),
                version: previous.clone(),
            }
        })?;

        Ok(ResolvedModel {
            model_id: model_id.to_string(),
            version: previous,
            extractor,
        })
    }
}

fn parse_model_ref(model_ref: &str) -> (&str, Option<&str>) {
    match model_ref.split_once('@') {
        Some((model_id, version)) if !model_id.is_empty() && !version.is_empty() => {
            (model_id, Some(version))
        }
        _ => (model_ref, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ner::{Entity, EntityExtractor};

    struct StaticExtractor;

    #[async_trait::async_trait]
    impl EntityExtractor for StaticExtractor {
        async fn extract(&self, _text: &str) -> anyhow::Result<Vec<Entity>> {
            Ok(vec![])
        }
    }

    #[test]
    fn test_parse_model_ref() {
        let (model_id, version) = parse_model_ref("triplex-lite@1.0.0");
        assert_eq!(model_id, "triplex-lite");
        assert_eq!(version, Some("1.0.0"));

        let (model_id, version) = parse_model_ref("triplex-lite");
        assert_eq!(model_id, "triplex-lite");
        assert_eq!(version, None);
    }

    #[test]
    fn test_register_duplicate_fails() {
        let mut registry = ModelRegistry::new();
        registry
            .register("triplex-lite", "1.0.0", Arc::new(StaticExtractor))
            .unwrap();

        let err = registry
            .register("triplex-lite", "1.0.0", Arc::new(StaticExtractor))
            .unwrap_err();

        assert_eq!(
            err,
            RegistryError::VersionAlreadyExists {
                model_id: "triplex-lite".to_string(),
                version: "1.0.0".to_string(),
            }
        );
    }
}
