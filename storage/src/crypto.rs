use std::collections::HashMap;
use std::sync::Arc;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CryptoError {
    #[error("kms key not found: {0}")]
    MissingKey(String),
    #[error("kms key must not be empty")]
    EmptyKey,
}

pub trait AtRestCipher: Send + Sync {
    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, CryptoError>;

    fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>, CryptoError>;

    fn key_id(&self) -> Option<&str> {
        None
    }
}

#[derive(Default)]
pub struct NoOpCipher;

impl AtRestCipher for NoOpCipher {
    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        Ok(plaintext.to_vec())
    }

    fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        Ok(ciphertext.to_vec())
    }
}

pub trait KmsKeyProvider: Send + Sync {
    fn resolve_data_key(&self, key_id: &str) -> Result<Vec<u8>, CryptoError>;
}

#[derive(Default)]
pub struct InMemoryKmsKeyProvider {
    keys: HashMap<String, Vec<u8>>,
}

impl InMemoryKmsKeyProvider {
    pub fn from_keys<I, K>(entries: I) -> Self
    where
        I: IntoIterator<Item = (K, Vec<u8>)>,
        K: Into<String>,
    {
        Self {
            keys: entries
                .into_iter()
                .map(|(key_id, key)| (key_id.into(), key))
                .collect(),
        }
    }
}

impl KmsKeyProvider for InMemoryKmsKeyProvider {
    fn resolve_data_key(&self, key_id: &str) -> Result<Vec<u8>, CryptoError> {
        self.keys
            .get(key_id)
            .cloned()
            .ok_or_else(|| CryptoError::MissingKey(key_id.to_string()))
    }
}

/// KMS hook cipher for at-rest encryption extension points.
///
/// The current implementation uses XOR to keep the storage path deterministic
/// and testable; production deployments should replace this with authenticated
/// encryption backed by a real KMS envelope workflow.
pub struct KmsHookCipher {
    key_id: String,
    key_provider: Arc<dyn KmsKeyProvider>,
}

impl KmsHookCipher {
    pub fn new(key_id: impl Into<String>, key_provider: Arc<dyn KmsKeyProvider>) -> Self {
        Self {
            key_id: key_id.into(),
            key_provider,
        }
    }

    fn transform(&self, input: &[u8]) -> Result<Vec<u8>, CryptoError> {
        let key = self.key_provider.resolve_data_key(&self.key_id)?;
        xor_payload(input, &key)
    }
}

impl AtRestCipher for KmsHookCipher {
    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        self.transform(plaintext)
    }

    fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        self.transform(ciphertext)
    }

    fn key_id(&self) -> Option<&str> {
        Some(self.key_id.as_str())
    }
}

fn xor_payload(data: &[u8], key: &[u8]) -> Result<Vec<u8>, CryptoError> {
    if key.is_empty() {
        return Err(CryptoError::EmptyKey);
    }

    Ok(data
        .iter()
        .enumerate()
        .map(|(index, byte)| byte ^ key[index % key.len()])
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kms_hook_cipher_round_trip() {
        let kms = Arc::new(InMemoryKmsKeyProvider::from_keys([(
            "kms-1",
            vec![0xAA, 0xBB],
        )])) as Arc<dyn KmsKeyProvider>;
        let cipher = KmsHookCipher::new("kms-1", kms);

        let encrypted = cipher.encrypt(b"payload").unwrap();
        assert_ne!(encrypted, b"payload");

        let decrypted = cipher.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, b"payload");
    }
}
