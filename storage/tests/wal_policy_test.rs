use std::thread;
use std::time::Duration;

use storage::wal::{Wal, WalFlushPolicy, WalOptions, WalRecoveryMode};
use tempfile::tempdir;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

#[tokio::test]
async fn wal_recover_to_last_good_offset_truncates_crc_mismatch_and_preserves_lsn() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("crc_recover.wal");

    let stable_len = {
        let mut wal = Wal::open(&path).await.unwrap();
        assert_eq!(wal.append(b"Entry 1").await.unwrap(), 1);
        assert_eq!(wal.append(b"Entry 2").await.unwrap(), 2);
        wal.flush().await.unwrap();
        tokio::fs::metadata(&path).await.unwrap().len()
    };

    {
        let mut wal = Wal::open(&path).await.unwrap();
        assert_eq!(wal.append(b"Entry 3").await.unwrap(), 3);
        wal.flush().await.unwrap();
    }

    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .await
            .unwrap();
        file.seek(std::io::SeekFrom::Start(stable_len + 8))
            .await
            .unwrap();
        let mut crc_bytes = [0u8; 4];
        file.read_exact(&mut crc_bytes).await.unwrap();
        file.seek(std::io::SeekFrom::Start(stable_len + 8))
            .await
            .unwrap();
        crc_bytes[0] ^= 0xFF;
        file.write_all(&crc_bytes).await.unwrap();
        file.flush().await.unwrap();
    }

    let mut wal = Wal::open_with_options(
        &path,
        WalOptions {
            recovery_mode: WalRecoveryMode::RecoverToLastGoodOffset,
            ..WalOptions::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(wal.current_lsn(), 2);
    assert_eq!(tokio::fs::metadata(&path).await.unwrap().len(), stable_len);
    assert_eq!(wal.append(b"Entry 3 recovered").await.unwrap(), 3);
}

#[tokio::test]
async fn wal_fail_fast_is_default_for_crc_mismatch() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("crc_fail_fast.wal");

    let stable_len = {
        let mut wal = Wal::open(&path).await.unwrap();
        assert_eq!(wal.append(b"Entry 1").await.unwrap(), 1);
        wal.flush().await.unwrap();
        tokio::fs::metadata(&path).await.unwrap().len()
    };

    {
        let mut wal = Wal::open(&path).await.unwrap();
        assert_eq!(wal.append(b"Entry 2").await.unwrap(), 2);
        wal.flush().await.unwrap();
    }

    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .await
            .unwrap();
        file.seek(std::io::SeekFrom::Start(stable_len + 8))
            .await
            .unwrap();
        let mut crc_bytes = [0u8; 4];
        file.read_exact(&mut crc_bytes).await.unwrap();
        file.seek(std::io::SeekFrom::Start(stable_len + 8))
            .await
            .unwrap();
        crc_bytes[0] ^= 0xFF;
        file.write_all(&crc_bytes).await.unwrap();
        file.flush().await.unwrap();
    }

    let err = match Wal::open(&path).await {
        Ok(_) => panic!("default recovery mode must fail fast"),
        Err(err) => err,
    };
    assert!(matches!(err, storage::wal::WalError::CrcMismatch));
    assert!(tokio::fs::metadata(&path).await.unwrap().len() > stable_len);
}

#[tokio::test]
async fn wal_batch_flush_policy_flushes_after_threshold() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("batch_flush.wal");

    let mut wal = Wal::open_with_options(
        &path,
        WalOptions {
            flush_policy: WalFlushPolicy::Batch { max_entries: 2 },
            ..WalOptions::default()
        },
    )
    .await
    .unwrap();

    wal.append(b"Entry 1").await.unwrap();
    assert_eq!(tokio::fs::metadata(&path).await.unwrap().len(), 0);

    wal.append(b"Entry 2").await.unwrap();
    assert!(tokio::fs::metadata(&path).await.unwrap().len() > 0);
}

#[tokio::test]
async fn wal_interval_flush_policy_flushes_on_next_append_after_interval() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("interval_flush.wal");

    let mut wal = Wal::open_with_options(
        &path,
        WalOptions {
            flush_policy: WalFlushPolicy::Interval(Duration::from_millis(20)),
            ..WalOptions::default()
        },
    )
    .await
    .unwrap();

    wal.append(b"Entry 1").await.unwrap();
    assert_eq!(tokio::fs::metadata(&path).await.unwrap().len(), 0);

    thread::sleep(Duration::from_millis(25));

    wal.append(b"Entry 2").await.unwrap();
    assert!(tokio::fs::metadata(&path).await.unwrap().len() > 0);
}
