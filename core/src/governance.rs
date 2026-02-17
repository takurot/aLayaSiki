use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EncryptionPolicy {
    pub at_rest_encryption: bool,
    pub kms_key_id: Option<String>,
}

impl EncryptionPolicy {
    pub fn disabled() -> Self {
        Self::default()
    }

    pub fn kms(kms_key_id: impl Into<String>) -> Self {
        Self {
            at_rest_encryption: true,
            kms_key_id: Some(kms_key_id.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantGovernancePolicy {
    pub tenant: String,
    pub residency_region: String,
    pub retention_days: u32,
    pub encryption: EncryptionPolicy,
}

impl TenantGovernancePolicy {
    pub fn new(
        tenant: impl Into<String>,
        residency_region: impl Into<String>,
        retention_days: u32,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            residency_region: residency_region.into(),
            retention_days,
            encryption: EncryptionPolicy::disabled(),
        }
    }

    pub fn with_encryption(
        mut self,
        encryption: EncryptionPolicy,
    ) -> Result<Self, GovernanceError> {
        self.encryption = encryption;
        self.validate()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<(), GovernanceError> {
        let tenant = self.tenant.trim();
        if tenant.is_empty() {
            return Err(GovernanceError::MissingTenant);
        }

        if self.residency_region.trim().is_empty() {
            return Err(GovernanceError::MissingResidencyRegion {
                tenant: tenant.to_string(),
            });
        }

        if self.encryption.at_rest_encryption {
            let kms_key_id = self
                .encryption
                .kms_key_id
                .as_deref()
                .map(str::trim)
                .unwrap_or_default();
            if kms_key_id.is_empty() {
                return Err(GovernanceError::MissingKmsKeyId);
            }
        }

        Ok(())
    }

    pub fn ensure_residency(&self, region: Option<&str>) -> Result<(), GovernanceError> {
        let tenant = self.tenant.clone();
        let Some(actual_region) = region.map(str::trim).filter(|region| !region.is_empty()) else {
            return Err(GovernanceError::MissingRegionMetadata { tenant });
        };

        if actual_region != self.residency_region {
            return Err(GovernanceError::ResidencyViolation {
                tenant,
                expected_region: self.residency_region.clone(),
                actual_region: actual_region.to_string(),
            });
        }

        Ok(())
    }

    pub fn retention_deadline_unix(&self, now_unix: u64) -> u64 {
        const DAY_SECONDS: u64 = 24 * 60 * 60;
        now_unix.saturating_add(self.retention_days as u64 * DAY_SECONDS)
    }

    pub fn kms_key_id(&self) -> Option<&str> {
        self.encryption.kms_key_id.as_deref()
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum GovernanceError {
    #[error("tenant is required")]
    MissingTenant,
    #[error("residency region is required for tenant {tenant}")]
    MissingResidencyRegion { tenant: String },
    #[error("region metadata is required for tenant {tenant}")]
    MissingRegionMetadata { tenant: String },
    #[error(
        "data residency violation for tenant {tenant}: expected {expected_region}, got {actual_region}"
    )]
    ResidencyViolation {
        tenant: String,
        expected_region: String,
        actual_region: String,
    },
    #[error("kms key id is required when at-rest encryption is enabled")]
    MissingKmsKeyId,
    #[error("governance policy store lock poisoned")]
    PolicyStorePoisoned,
}

pub trait GovernancePolicyStore: Send + Sync {
    fn upsert_policy(&self, policy: TenantGovernancePolicy) -> Result<(), GovernanceError>;

    fn get_policy(&self, tenant: &str) -> Result<Option<TenantGovernancePolicy>, GovernanceError>;
}

#[derive(Default)]
pub struct InMemoryGovernancePolicyStore {
    policies: RwLock<HashMap<String, TenantGovernancePolicy>>,
}

impl InMemoryGovernancePolicyStore {
    pub fn upsert_policy(&self, policy: TenantGovernancePolicy) -> Result<(), GovernanceError> {
        GovernancePolicyStore::upsert_policy(self, policy)
    }

    pub fn get_policy(
        &self,
        tenant: &str,
    ) -> Result<Option<TenantGovernancePolicy>, GovernanceError> {
        GovernancePolicyStore::get_policy(self, tenant)
    }
}

impl GovernancePolicyStore for InMemoryGovernancePolicyStore {
    fn upsert_policy(&self, policy: TenantGovernancePolicy) -> Result<(), GovernanceError> {
        policy.validate()?;

        let mut map = self
            .policies
            .write()
            .map_err(|_| GovernanceError::PolicyStorePoisoned)?;
        map.insert(policy.tenant.clone(), policy);
        Ok(())
    }

    fn get_policy(&self, tenant: &str) -> Result<Option<TenantGovernancePolicy>, GovernanceError> {
        let normalized_tenant = tenant.trim();
        if normalized_tenant.is_empty() {
            return Err(GovernanceError::MissingTenant);
        }

        let map = self
            .policies
            .read()
            .map_err(|_| GovernanceError::PolicyStorePoisoned)?;
        Ok(map.get(normalized_tenant).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_kms_policy_without_key_id() {
        let policy = TenantGovernancePolicy::new("acme", "ap-northeast-1", 30).with_encryption(
            EncryptionPolicy {
                at_rest_encryption: true,
                kms_key_id: None,
            },
        );
        assert!(matches!(policy, Err(GovernanceError::MissingKmsKeyId)));
    }

    #[test]
    fn validates_residency_region() {
        let policy = TenantGovernancePolicy::new("acme", "ap-northeast-1", 30);
        assert!(policy.ensure_residency(Some("ap-northeast-1")).is_ok());
        assert!(matches!(
            policy.ensure_residency(Some("us-east-1")),
            Err(GovernanceError::ResidencyViolation { .. })
        ));
    }

    #[test]
    fn store_round_trips_policy() {
        let store = InMemoryGovernancePolicyStore::default();
        let policy = TenantGovernancePolicy::new("acme", "ap-northeast-1", 7)
            .with_encryption(EncryptionPolicy::kms("kms-key-acme"))
            .unwrap();

        store.upsert_policy(policy.clone()).unwrap();
        let loaded = store.get_policy("acme").unwrap().unwrap();

        assert_eq!(loaded, policy);
    }
}
