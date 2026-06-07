use std::collections::HashMap;
use std::sync::Arc;

use aes_gcm::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    Aes256Gcm, Nonce,
};
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CryptoError {
    #[error("kms key not found: {0}")]
    MissingKey(String),
    #[error("kms key must not be empty")]
    EmptyKey,
    #[error("encryption failure: {0}")]
    Encryption(String),
    #[error("decryption failure: {0}")]
    Decryption(String),
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
/// Uses authenticated encryption (AES-256-GCM) with a unique cryptographically
/// secure random nonce per operation. Data keys of arbitrary size are derived
/// into a 256-bit key using SHA-256.
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
}

impl AtRestCipher for KmsHookCipher {
    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        let key = self.key_provider.resolve_data_key(&self.key_id)?;
        if key.is_empty() {
            return Err(CryptoError::EmptyKey);
        }

        // Derive 256-bit key from input key of arbitrary length
        let mut hasher = Sha256::new();
        hasher.update(&key);
        let hashed_key = hasher.finalize();

        let cipher = Aes256Gcm::new(&hashed_key);
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);

        let ciphertext = cipher
            .encrypt(&nonce, plaintext)
            .map_err(|e| CryptoError::Encryption(format!("{:?}", e)))?;

        // Prepend 12-byte nonce to the ciphertext
        let mut result = Vec::with_capacity(nonce.len() + ciphertext.len());
        result.extend_from_slice(&nonce);
        result.extend_from_slice(&ciphertext);

        Ok(result)
    }

    fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        let key = self.key_provider.resolve_data_key(&self.key_id)?;
        if key.is_empty() {
            return Err(CryptoError::EmptyKey);
        }

        if ciphertext.len() < 12 {
            return Err(CryptoError::Decryption(
                "ciphertext too short (must be at least 12 bytes to contain nonce)".to_string(),
            ));
        }

        // Derive 256-bit key from input key of arbitrary length
        let mut hasher = Sha256::new();
        hasher.update(&key);
        let hashed_key = hasher.finalize();

        let cipher = Aes256Gcm::new(&hashed_key);

        let (nonce_bytes, encrypted_data) = ciphertext.split_at(12);
        let nonce = Nonce::from_slice(nonce_bytes);

        let plaintext = cipher
            .decrypt(nonce, encrypted_data)
            .map_err(|e| CryptoError::Decryption(format!("{:?}", e)))?;

        Ok(plaintext)
    }

    fn key_id(&self) -> Option<&str> {
        Some(self.key_id.as_str())
    }
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

        let payload = b"payload";
        let encrypted = cipher.encrypt(payload).unwrap();
        assert_ne!(encrypted, payload);

        // AEAD prepends a 12-byte nonce, so ciphertext length must be at least 12 + plaintext.len()
        assert!(encrypted.len() >= 12 + payload.len());

        let decrypted = cipher.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, payload);
    }

    #[test]
    fn kms_hook_cipher_nonce_uniqueness() {
        let kms = Arc::new(InMemoryKmsKeyProvider::from_keys([(
            "kms-1",
            vec![0xAA, 0xBB],
        )])) as Arc<dyn KmsKeyProvider>;
        let cipher = KmsHookCipher::new("kms-1", kms);

        let payload = b"same-payload";
        let enc1 = cipher.encrypt(payload).unwrap();
        let enc2 = cipher.encrypt(payload).unwrap();

        // Nonces must be unique, meaning ciphertexts must differ
        assert_ne!(enc1, enc2);
    }

    #[test]
    fn kms_hook_cipher_decryption_failure() {
        let kms = Arc::new(InMemoryKmsKeyProvider::from_keys([(
            "kms-1",
            vec![0xAA, 0xBB],
        )])) as Arc<dyn KmsKeyProvider>;
        let cipher = KmsHookCipher::new("kms-1", kms);

        let mut encrypted = cipher.encrypt(b"my-secret").unwrap();
        // Tamper with the ciphertext (e.g., flip the last byte)
        if let Some(last) = encrypted.last_mut() {
            *last ^= 0xFF;
        }

        let decrypt_result = cipher.decrypt(&encrypted);
        assert!(decrypt_result.is_err());
        assert!(matches!(
            decrypt_result.unwrap_err(),
            CryptoError::Decryption(_)
        ));
    }
}
