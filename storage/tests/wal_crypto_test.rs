use std::sync::Arc;

use storage::crypto::{InMemoryKmsKeyProvider, KmsHookCipher};
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
