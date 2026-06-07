use std::sync::Arc;

use alayasiki_core::model::Node;
use storage::crypto::{InMemoryKmsKeyProvider, KmsHookCipher};
use storage::repo::Repository;
use storage::wal::Wal;
use tempfile::tempdir;

#[tokio::test]
async fn wal_round_trips_payload_with_kms_hook_cipher() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("encrypted.wal");

    let kms = Arc::new(InMemoryKmsKeyProvider::from_keys([(
        "kms-key-acme",
        vec![0x42, 0x99, 0x11, 0xA7],
    )]));
    let cipher = Arc::new(KmsHookCipher::new("kms-key-acme", kms));

    {
        let mut wal = Wal::open_with_cipher(&path, cipher.clone()).await.unwrap();
        wal.append(b"sensitive-record").await.unwrap();
        wal.flush().await.unwrap();
    }

    let on_disk = tokio::fs::read(&path).await.unwrap();
    assert!(
        !on_disk
            .windows(b"sensitive-record".len())
            .any(|w| w == b"sensitive-record"),
        "plaintext must not appear in WAL payload"
    );

    let mut recovered = Vec::new();
    let mut wal = Wal::open_with_cipher(&path, cipher).await.unwrap();
    wal.replay(|_lsn, payload| {
        recovered.push(payload);
        Ok(())
    })
    .await
    .unwrap();

    assert_eq!(recovered, vec![b"sensitive-record".to_vec()]);
}

#[tokio::test]
async fn repo_encryption_integration_test() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("repo.wal");

    let kms_correct = Arc::new(InMemoryKmsKeyProvider::from_keys([(
        "kms-key-correct",
        vec![0x01, 0x02, 0x03, 0x04],
    )]));
    let cipher_correct = Arc::new(KmsHookCipher::new("kms-key-correct", kms_correct));

    // 1. Write node with correct cipher
    {
        let repo = Repository::open_with_cipher(&wal_path, cipher_correct.clone())
            .await
            .unwrap();
        let node = Node::new(
            42,
            vec![0.1, 0.2, 0.3],
            "super-secret-metadata-pii-content".to_string(),
        );
        repo.put_node(node).await.unwrap();

        let retrieved = repo.get_node(42).await.unwrap();
        assert_eq!(retrieved.data, "super-secret-metadata-pii-content");
    }

    // 2. Assert ciphertext is encrypted on disk and plaintext does not appear
    let on_disk = tokio::fs::read(&wal_path).await.unwrap();
    assert!(
        !on_disk
            .windows(b"super-secret-metadata-pii-content".len())
            .any(|w| w == b"super-secret-metadata-pii-content"),
        "plaintext secret must not appear on disk"
    );

    // 3. Re-open with correct cipher and verify decryption and replay
    {
        let repo = Repository::open_with_cipher(&wal_path, cipher_correct)
            .await
            .unwrap();
        let retrieved = repo.get_node(42).await.unwrap();
        assert_eq!(retrieved.data, "super-secret-metadata-pii-content");
    }

    // 4. Try opening with a different key and verify it fails decryption
    let kms_wrong = Arc::new(InMemoryKmsKeyProvider::from_keys([(
        "kms-key-wrong",
        vec![0xFF, 0xFF, 0xFF, 0xFF],
    )]));
    let cipher_wrong = Arc::new(KmsHookCipher::new("kms-key-wrong", kms_wrong));

    let reopen_wrong_result = Repository::open_with_cipher(&wal_path, cipher_wrong).await;
    assert!(reopen_wrong_result.is_err());
}
