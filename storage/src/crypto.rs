use std::collections::HashMap;
use std::sync::Arc;

use aes_gcm::{
    aead::{AeadCore, AeadInPlace, KeyInit, OsRng},
    Aes256Gcm, Nonce,
};
use hkdf::Hkdf;
use sha2::Sha256;
use thiserror::Error;

const NONCE_SIZE: usize = 12; // 96-bit nonce for AES-GCM

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

// Private helper to derive a cryptographically strong 256-bit symmetric key from an arbitrary length KMS key
fn derive_key(key: &[u8]) -> Result<aes_gcm::Key<Aes256Gcm>, CryptoError> {
    if key.is_empty() {
        return Err(CryptoError::EmptyKey);
    }
    let hk = Hkdf::<Sha256>::new(None, key);
    let mut derived_key = aes_gcm::Key::<Aes256Gcm>::default();
    hk.expand(&[], &mut derived_key)
        .map_err(|_| CryptoError::Encryption("KDF expansion failed".to_string()))?;
    Ok(derived_key)
}

/// KMS hook cipher for at-rest encryption extension points.
///
/// Uses authenticated encryption (AES-256-GCM) with a unique cryptographically
/// secure random nonce per operation. Data keys of arbitrary size are derived
/// into a 256-bit key using HKDF-SHA256.
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
        let derived_key = derive_key(&key)?;

        let cipher = Aes256Gcm::new(&derived_key);
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);

        // Pre-allocate a single buffer for: Nonce + Plaintext + 16-byte Tag
        let mut result = Vec::with_capacity(NONCE_SIZE + plaintext.len() + 16);
        result.extend_from_slice(&nonce);
        result.extend_from_slice(plaintext);

        // Encrypt the plaintext payload part in-place
        let payload_mut = &mut result[NONCE_SIZE..];
        let tag = cipher
            .encrypt_in_place_detached(&nonce, &[], payload_mut)
            .map_err(|_| CryptoError::Encryption("AEAD encryption failed".to_string()))?;

        result.extend_from_slice(&tag);
        Ok(result)
    }

    fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        let key = self.key_provider.resolve_data_key(&self.key_id)?;
        let derived_key = derive_key(&key).map_err(|_| {
            CryptoError::Decryption("Failed to derive key for decryption".to_string())
        })?;

        if ciphertext.len() < NONCE_SIZE {
            return Err(CryptoError::Decryption(
                "ciphertext too short (must contain nonce)".to_string(),
            ));
        }

        let cipher = Aes256Gcm::new(&derived_key);

        let (nonce_bytes, encrypted_payload) = ciphertext.split_at(NONCE_SIZE);
        let nonce = Nonce::from_slice(nonce_bytes);

        // Copy encrypted payload (ciphertext + tag) to a mutable vector and decrypt in-place
        let mut buffer = encrypted_payload.to_vec();
        cipher
            .decrypt_in_place(nonce, &[], &mut buffer)
            .map_err(|_| CryptoError::Decryption("AEAD decryption failed".to_string()))?;

        Ok(buffer)
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
        assert!(encrypted.len() >= NONCE_SIZE + payload.len());

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
