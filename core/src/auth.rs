use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Ingest,
    Query,
    Admin,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub subject: String,
    pub tenant: String,
    pub roles: HashSet<String>,
    pub scopes: HashSet<String>,
    pub attributes: HashMap<String, String>,
}

impl Principal {
    pub fn new(subject: impl Into<String>, tenant: impl Into<String>) -> Self {
        Self {
            subject: subject.into(),
            tenant: tenant.into(),
            roles: HashSet::new(),
            scopes: HashSet::new(),
            attributes: HashMap::new(),
        }
    }

    pub fn with_roles<I, S>(mut self, roles: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.roles = roles.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_scopes<I, S>(mut self, scopes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.scopes = scopes.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JwtClaims {
    pub sub: String,
    pub tenant: String,
    #[serde(default)]
    pub roles: Vec<String>,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub attributes: HashMap<String, String>,
    #[serde(default)]
    pub iss: Option<String>,
    #[serde(default)]
    pub aud: Option<String>,
    pub exp: usize,
    #[serde(default)]
    pub nbf: Option<usize>,
    #[serde(default)]
    pub iat: Option<usize>,
}

impl TryFrom<JwtClaims> for Principal {
    type Error = AuthError;

    fn try_from(claims: JwtClaims) -> Result<Self, Self::Error> {
        if claims.sub.trim().is_empty() {
            return Err(AuthError::MissingSubject);
        }
        if claims.tenant.trim().is_empty() {
            return Err(AuthError::MissingTenant);
        }

        let roles = claims
            .roles
            .into_iter()
            .filter_map(|role| {
                let trimmed = role.trim();
                (!trimmed.is_empty()).then(|| trimmed.to_string())
            })
            .collect();

        let scopes = claims
            .scope
            .unwrap_or_default()
            .split_whitespace()
            .filter_map(|scope| {
                let trimmed = scope.trim();
                (!trimmed.is_empty()).then(|| trimmed.to_string())
            })
            .collect();

        Ok(Principal {
            subject: claims.sub,
            tenant: claims.tenant,
            roles,
            scopes,
            attributes: claims.attributes,
        })
    }
}

pub struct JwtAuthenticator {
    decoding_key: DecodingKey,
    validation: Validation,
}

impl JwtAuthenticator {
    pub fn new_hs256(
        secret: impl AsRef<[u8]>,
        issuer: Option<&str>,
        audience: Option<&str>,
    ) -> Self {
        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.leeway = 0;
        if let Some(issuer) = issuer {
            validation.set_issuer(&[issuer]);
        }
        if let Some(audience) = audience {
            validation.set_audience(&[audience]);
        }

        Self {
            decoding_key: DecodingKey::from_secret(secret.as_ref()),
            validation,
        }
    }

    pub fn authenticate(&self, token: &str) -> Result<Principal, AuthError> {
        let normalized = token
            .trim()
            .strip_prefix("Bearer ")
            .or_else(|| token.trim().strip_prefix("bearer "))
            .unwrap_or(token)
            .trim();
        if normalized.is_empty() {
            return Err(AuthError::MissingToken);
        }

        let token_data = decode::<JwtClaims>(normalized, &self.decoding_key, &self.validation)
            .map_err(|err| AuthError::InvalidToken(err.to_string()))?;
        Principal::try_from(token_data.claims)
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AuthError {
    #[error("missing bearer token")]
    MissingToken,
    #[error("invalid jwt: {0}")]
    InvalidToken(String),
    #[error("jwt claim sub must not be empty")]
    MissingSubject,
    #[error("jwt claim tenant must not be empty")]
    MissingTenant,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceContext {
    pub tenant: String,
    pub required_attributes: HashMap<String, String>,
    pub min_clearance_level: Option<u8>,
}

impl ResourceContext {
    pub fn new(tenant: impl Into<String>) -> Self {
        Self {
            tenant: tenant.into(),
            required_attributes: HashMap::new(),
            min_clearance_level: None,
        }
    }

    pub fn require_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.required_attributes.insert(key.into(), value.into());
        self
    }

    pub fn require_min_clearance(mut self, level: u8) -> Self {
        self.min_clearance_level = Some(level);
        self
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AuthzError {
    #[error("permission denied for action {action:?}")]
    PermissionDenied { action: Action },
    #[error("resource tenant is required when tenant boundary is enabled")]
    MissingResourceTenant,
    #[error("tenant boundary violation: principal tenant {principal_tenant} cannot access {resource_tenant}")]
    TenantMismatch {
        principal_tenant: String,
        resource_tenant: String,
    },
    #[error("missing required attribute: {key}")]
    MissingAttribute { key: String },
    #[error("attribute mismatch for {key}: expected {expected}, got {actual}")]
    AttributeMismatch {
        key: String,
        expected: String,
        actual: String,
    },
    #[error("invalid numeric attribute {key}: {value}")]
    InvalidAttributeValue { key: String, value: String },
    #[error("insufficient clearance level: required {required}, got {actual}")]
    InsufficientClearance { required: u8, actual: u8 },
}

#[derive(Debug, Clone)]
pub struct Authorizer {
    role_permissions: HashMap<String, HashSet<Action>>,
    action_scopes: HashMap<Action, HashSet<String>>,
    enforce_tenant_boundary: bool,
}

impl Default for Authorizer {
    fn default() -> Self {
        let mut role_permissions = HashMap::new();
        role_permissions.insert("reader".to_string(), HashSet::from([Action::Query]));
        role_permissions.insert("ingestor".to_string(), HashSet::from([Action::Ingest]));
        role_permissions.insert(
            "admin".to_string(),
            HashSet::from([Action::Ingest, Action::Query, Action::Admin]),
        );

        let mut action_scopes = HashMap::new();
        action_scopes.insert(
            Action::Ingest,
            HashSet::from(["ingest:write".to_string(), "admin:*".to_string()]),
        );
        action_scopes.insert(
            Action::Query,
            HashSet::from(["query:execute".to_string(), "admin:*".to_string()]),
        );
        action_scopes.insert(Action::Admin, HashSet::from(["admin:*".to_string()]));

        Self {
            role_permissions,
            action_scopes,
            enforce_tenant_boundary: true,
        }
    }
}

impl Authorizer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_tenant_boundary(mut self, enforce: bool) -> Self {
        self.enforce_tenant_boundary = enforce;
        self
    }

    pub fn with_role_permissions<I>(mut self, role: impl Into<String>, actions: I) -> Self
    where
        I: IntoIterator<Item = Action>,
    {
        self.role_permissions
            .insert(role.into(), actions.into_iter().collect());
        self
    }

    pub fn with_action_scopes<I, S>(mut self, action: Action, scopes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.action_scopes
            .insert(action, scopes.into_iter().map(Into::into).collect());
        self
    }

    pub fn authorize(
        &self,
        principal: &Principal,
        action: Action,
        resource: &ResourceContext,
    ) -> Result<(), AuthzError> {
        if self.enforce_tenant_boundary {
            let resource_tenant = resource.tenant.trim();
            if resource_tenant.is_empty() {
                return Err(AuthzError::MissingResourceTenant);
            }
            if principal.tenant != resource_tenant {
                return Err(AuthzError::TenantMismatch {
                    principal_tenant: principal.tenant.clone(),
                    resource_tenant: resource_tenant.to_string(),
                });
            }
        }

        if !self.is_action_permitted(principal, action) {
            return Err(AuthzError::PermissionDenied { action });
        }

        self.validate_resource_attributes(principal, resource)?;
        self.validate_clearance(principal, resource)?;
        Ok(())
    }

    fn is_action_permitted(&self, principal: &Principal, action: Action) -> bool {
        let role_allows = principal.roles.iter().any(|role| {
            self.role_permissions
                .get(role)
                .map(|actions| actions.contains(&Action::Admin) || actions.contains(&action))
                .unwrap_or(false)
        });

        let scope_allows = self
            .action_scopes
            .get(&action)
            .map(|required| {
                required
                    .iter()
                    .any(|scope| principal.scopes.contains(scope))
            })
            .unwrap_or(false);

        role_allows || scope_allows
    }

    fn validate_resource_attributes(
        &self,
        principal: &Principal,
        resource: &ResourceContext,
    ) -> Result<(), AuthzError> {
        for (key, expected_value) in &resource.required_attributes {
            let actual = principal
                .attributes
                .get(key)
                .ok_or_else(|| AuthzError::MissingAttribute { key: key.clone() })?;
            if actual != expected_value {
                return Err(AuthzError::AttributeMismatch {
                    key: key.clone(),
                    expected: expected_value.clone(),
                    actual: actual.clone(),
                });
            }
        }
        Ok(())
    }

    fn validate_clearance(
        &self,
        principal: &Principal,
        resource: &ResourceContext,
    ) -> Result<(), AuthzError> {
        let Some(required_level) = resource.min_clearance_level else {
            return Ok(());
        };

        let key = "clearance_level".to_string();
        let raw_level = principal
            .attributes
            .get(&key)
            .ok_or_else(|| AuthzError::MissingAttribute { key: key.clone() })?;
        let actual_level =
            raw_level
                .parse::<u8>()
                .map_err(|_| AuthzError::InvalidAttributeValue {
                    key: key.clone(),
                    value: raw_level.clone(),
                })?;

        if actual_level < required_level {
            return Err(AuthzError::InsufficientClearance {
                required: required_level,
                actual: actual_level,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn now() -> usize {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as usize
    }

    fn build_claims(exp_offset_secs: i64) -> JwtClaims {
        let n = now() as i64;
        JwtClaims {
            sub: "user-1".to_string(),
            tenant: "acme".to_string(),
            roles: vec!["reader".to_string()],
            scope: Some("query:execute".to_string()),
            attributes: HashMap::from([("department".to_string(), "finance".to_string())]),
            iss: Some("alayasiki-auth".to_string()),
            aud: Some("alayasiki-api".to_string()),
            exp: (n + exp_offset_secs).max(0) as usize,
            nbf: Some((n - 1).max(0) as usize),
            iat: Some(n.max(0) as usize),
        }
    }

    fn encode_claims(secret: &str, claims: &JwtClaims) -> String {
        encode(
            &Header::new(Algorithm::HS256),
            claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap()
    }

    #[test]
    fn authenticates_valid_token() {
        let secret = "test-secret";
        let claims = build_claims(300);
        let token = encode_claims(secret, &claims);
        let auth =
            JwtAuthenticator::new_hs256(secret, Some("alayasiki-auth"), Some("alayasiki-api"));

        let principal = auth.authenticate(&token).unwrap();
        assert_eq!(principal.subject, "user-1");
        assert_eq!(principal.tenant, "acme");
        assert!(principal.roles.contains("reader"));
        assert!(principal.scopes.contains("query:execute"));
    }

    #[test]
    fn rejects_expired_token() {
        let secret = "test-secret";
        let claims = build_claims(-10);
        let token = encode_claims(secret, &claims);
        let auth =
            JwtAuthenticator::new_hs256(secret, Some("alayasiki-auth"), Some("alayasiki-api"));

        let result = auth.authenticate(&token);
        assert!(matches!(result, Err(AuthError::InvalidToken(_))));
    }

    #[test]
    fn rejects_invalid_signature() {
        let claims = build_claims(300);
        let token = encode_claims("wrong-secret", &claims);
        let auth = JwtAuthenticator::new_hs256(
            "expected-secret",
            Some("alayasiki-auth"),
            Some("alayasiki-api"),
        );

        let result = auth.authenticate(&token);
        assert!(matches!(result, Err(AuthError::InvalidToken(_))));
    }

    #[test]
    fn authorizes_with_rbac() {
        let principal = Principal::new("u1", "acme").with_roles(["ingestor"]);
        let resource = ResourceContext::new("acme");
        let authorizer = Authorizer::default();

        let result = authorizer.authorize(&principal, Action::Ingest, &resource);
        assert!(result.is_ok());
    }

    #[test]
    fn authorizes_with_scope() {
        let principal = Principal::new("u1", "acme").with_scopes(["query:execute"]);
        let resource = ResourceContext::new("acme");
        let authorizer = Authorizer::default();

        let result = authorizer.authorize(&principal, Action::Query, &resource);
        assert!(result.is_ok());
    }

    #[test]
    fn denies_missing_permission() {
        let principal = Principal::new("u1", "acme").with_roles(["reader"]);
        let resource = ResourceContext::new("acme");
        let authorizer = Authorizer::default();

        let result = authorizer.authorize(&principal, Action::Ingest, &resource);
        assert!(matches!(result, Err(AuthzError::PermissionDenied { .. })));
    }

    #[test]
    fn denies_tenant_mismatch() {
        let principal = Principal::new("u1", "acme").with_roles(["reader"]);
        let resource = ResourceContext::new("other-tenant");
        let authorizer = Authorizer::default();

        let result = authorizer.authorize(&principal, Action::Query, &resource);
        assert!(matches!(result, Err(AuthzError::TenantMismatch { .. })));
    }

    #[test]
    fn denies_missing_resource_tenant_by_default() {
        let principal = Principal::new("u1", "acme").with_roles(["reader"]);
        let resource = ResourceContext::default();
        let authorizer = Authorizer::default();

        let result = authorizer.authorize(&principal, Action::Query, &resource);
        assert!(matches!(result, Err(AuthzError::MissingResourceTenant)));
    }

    #[test]
    fn denies_attribute_mismatch() {
        let principal = Principal::new("u1", "acme")
            .with_roles(["reader"])
            .with_attribute("department", "sales");
        let resource = ResourceContext::new("acme").require_attribute("department", "finance");
        let authorizer = Authorizer::default();

        let result = authorizer.authorize(&principal, Action::Query, &resource);
        assert!(matches!(result, Err(AuthzError::AttributeMismatch { .. })));
    }

    #[test]
    fn denies_insufficient_clearance() {
        let principal = Principal::new("u1", "acme")
            .with_roles(["reader"])
            .with_attribute("clearance_level", "2");
        let resource = ResourceContext::new("acme").require_min_clearance(3);
        let authorizer = Authorizer::default();

        let result = authorizer.authorize(&principal, Action::Query, &resource);
        assert!(matches!(
            result,
            Err(AuthzError::InsufficientClearance { .. })
        ));
    }
}
