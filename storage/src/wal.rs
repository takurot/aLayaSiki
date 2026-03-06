use crate::crypto::{AtRestCipher, CryptoError, NoOpCipher};
use alayasiki_core::error::{AlayasikiError, ErrorCode};
use crc32fast::Hasher;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter};

#[derive(Error, Debug)]
pub enum WalError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Data integrity error (CRC mismatch)")]
    CrcMismatch,
    #[error("Corrupt entry")]
    CorruptEntry,
    #[error("At-rest encryption error: {0}")]
    Encryption(String),
}

impl AlayasikiError for WalError {
    fn error_code(&self) -> ErrorCode {
        match self {
            WalError::Io(_) => ErrorCode::Internal,
            WalError::CrcMismatch => ErrorCode::Internal,
            WalError::CorruptEntry => ErrorCode::Internal,
            WalError::Encryption(_) => ErrorCode::Internal,
        }
    }
}

impl From<CryptoError> for WalError {
    fn from(value: CryptoError) -> Self {
        Self::Encryption(value.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WalRecoveryMode {
    #[default]
    FailFast,
    RecoverToLastGoodOffset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WalFlushPolicy {
    #[default]
    Always,
    Interval(Duration),
    Batch {
        max_entries: usize,
    },
}

impl WalFlushPolicy {
    fn normalized(self) -> Self {
        match self {
            Self::Always => Self::Always,
            Self::Interval(interval) if interval.is_zero() => Self::Always,
            Self::Interval(interval) => Self::Interval(interval),
            Self::Batch { max_entries } => Self::Batch {
                max_entries: max_entries.max(1),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WalOptions {
    pub recovery_mode: WalRecoveryMode,
    pub flush_policy: WalFlushPolicy,
}

impl WalOptions {
    fn normalized(self) -> Self {
        Self {
            recovery_mode: self.recovery_mode,
            flush_policy: self.flush_policy.normalized(),
        }
    }
}

pub struct Wal {
    file: BufWriter<File>,
    current_lsn: AtomicU64,
    cipher: Arc<dyn AtRestCipher>,
    recovery_mode: WalRecoveryMode,
    flush_policy: WalFlushPolicy,
    pending_appends: usize,
    last_flush_at: Instant,
}

impl Wal {
    /// Open a WAL file. If it doesn't exist, it will be created.
    /// If it exists, it will be read to determine the next LSN.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, WalError> {
        Self::open_with_options(path, WalOptions::default()).await
    }

    /// Open a WAL file with custom recovery and flush options.
    pub async fn open_with_options(
        path: impl AsRef<Path>,
        options: WalOptions,
    ) -> Result<Self, WalError> {
        Self::open_with_cipher_and_options(path, Arc::new(NoOpCipher), options).await
    }

    /// Open a WAL file with a custom at-rest cipher (KMS hook point).
    pub async fn open_with_cipher(
        path: impl AsRef<Path>,
        cipher: Arc<dyn AtRestCipher>,
    ) -> Result<Self, WalError> {
        Self::open_with_cipher_and_options(path, cipher, WalOptions::default()).await
    }

    /// Open a WAL file with custom cipher, recovery mode, and flush policy.
    pub async fn open_with_cipher_and_options(
        path: impl AsRef<Path>,
        cipher: Arc<dyn AtRestCipher>,
        options: WalOptions,
    ) -> Result<Self, WalError> {
        let path = path.as_ref().to_path_buf();
        let options = options.normalized();

        // Ensure directory exists
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .await?;

        let mut wal = Self {
            file: BufWriter::new(file),
            current_lsn: AtomicU64::new(0),
            cipher,
            recovery_mode: options.recovery_mode,
            flush_policy: options.flush_policy,
            pending_appends: 0,
            last_flush_at: Instant::now(),
        };

        // Recover the latest committed LSN at startup so new appends remain monotonic.
        wal.scan_entries(|_lsn, _payload| Ok(())).await?;

        Ok(wal)
    }

    /// Append an entry to the WAL. Returns the assigned LSN.
    /// Format: [LSN: 8 bytes][CRC: 4 bytes][Len: 4 bytes][Payload: Len bytes]
    pub async fn append(&mut self, payload: &[u8]) -> Result<u64, WalError> {
        let encrypted_payload = self.cipher.encrypt(payload)?;
        let lsn = self.current_lsn.fetch_add(1, Ordering::SeqCst) + 1;
        let len = encrypted_payload.len() as u32;

        let mut hasher = Hasher::new();
        hasher.update(&encrypted_payload);
        let crc = hasher.finalize();

        // Write Header
        self.file.write_u64(lsn).await?;
        self.file.write_u32(crc).await?;
        self.file.write_u32(len).await?;

        // Write Payload
        self.file.write_all(&encrypted_payload).await?;

        self.pending_appends += 1;
        self.flush_if_needed().await?;

        Ok(lsn)
    }

    /// Flush the internal buffer to disk, ensuring durability.
    pub async fn flush(&mut self) -> Result<(), WalError> {
        self.durable_flush().await
    }

    pub fn flush_policy(&self) -> WalFlushPolicy {
        self.flush_policy
    }

    pub fn recovery_mode(&self) -> WalRecoveryMode {
        self.recovery_mode
    }

    async fn durable_flush(&mut self) -> Result<(), WalError> {
        self.file.flush().await?;
        self.file.get_ref().sync_all().await?; // fsync
        self.pending_appends = 0;
        self.last_flush_at = Instant::now();
        Ok(())
    }

    pub fn current_lsn(&self) -> u64 {
        self.current_lsn.load(Ordering::SeqCst)
    }

    /// Replays the WAL from the beginning.
    /// Returns the last valid LSN found.
    /// If an incomplete entry is found at the end, it is truncated.
    pub async fn replay<F>(&mut self, mut callback: F) -> Result<u64, WalError>
    where
        F: FnMut(u64, Vec<u8>) -> Result<(), WalError>,
    {
        self.scan_entries(&mut callback).await
    }

    async fn flush_if_needed(&mut self) -> Result<(), WalError> {
        let should_flush = match self.flush_policy {
            WalFlushPolicy::Always => true,
            WalFlushPolicy::Interval(interval) => self.last_flush_at.elapsed() >= interval,
            WalFlushPolicy::Batch { max_entries } => self.pending_appends >= max_entries,
        };

        if should_flush {
            self.durable_flush().await?;
        }

        Ok(())
    }

    async fn scan_entries<F>(&mut self, mut callback: F) -> Result<u64, WalError>
    where
        F: FnMut(u64, Vec<u8>) -> Result<(), WalError>,
    {
        self.file.flush().await?;
        let file = self.file.get_mut();
        file.seek(std::io::SeekFrom::Start(0)).await?;

        let mut last_lsn = 0;
        let mut last_good_offset = 0;
        let total_len = file.metadata().await?.len();

        loop {
            let entry_start = file.stream_position().await?;
            let lsn = match file.read_u64().await {
                Ok(v) => v,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    if entry_start < total_len {
                        truncate_tail(file, last_good_offset).await?;
                    }
                    break;
                }
                Err(e) => return Err(WalError::Io(e)),
            };

            let crc = match file.read_u32().await {
                Ok(v) => v,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    truncate_tail(file, last_good_offset).await?;
                    break;
                }
                Err(e) => return Err(WalError::Io(e)),
            };

            let len = match file.read_u32().await {
                Ok(v) => v as usize,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    truncate_tail(file, last_good_offset).await?;
                    break;
                }
                Err(e) => return Err(WalError::Io(e)),
            };

            let payload_start = file.stream_position().await?;
            if (len as u64) > total_len.saturating_sub(payload_start) {
                truncate_tail(file, last_good_offset).await?;
                break;
            }

            let mut payload = vec![0u8; len];
            match file.read_exact(&mut payload).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    truncate_tail(file, last_good_offset).await?;
                    break;
                }
                Err(e) => return Err(WalError::Io(e)),
            }

            let mut hasher = Hasher::new();
            hasher.update(&payload);
            if hasher.finalize() != crc {
                if matches!(self.recovery_mode, WalRecoveryMode::RecoverToLastGoodOffset) {
                    truncate_tail(file, last_good_offset).await?;
                    break;
                }
                return Err(WalError::CrcMismatch);
            }

            let decrypted_payload = self.cipher.decrypt(&payload)?;
            callback(lsn, decrypted_payload)?;
            last_lsn = lsn;
            last_good_offset = file.stream_position().await?;
        }

        file.seek(std::io::SeekFrom::End(0)).await?;
        self.current_lsn.store(last_lsn, Ordering::SeqCst);
        self.pending_appends = 0;
        self.last_flush_at = Instant::now();

        Ok(last_lsn)
    }
}

async fn truncate_tail(file: &mut File, last_good_offset: u64) -> Result<(), WalError> {
    if last_good_offset < file.metadata().await?.len() {
        file.set_len(last_good_offset).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_wal_append_and_recover() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let mut wal = Wal::open(&path).await.expect("failed to open wal");

        let entry1 = b"Hello WAL";
        let lsn1 = wal.append(entry1).await.expect("append failed");

        let entry2 = b"Second Entry";
        let lsn2 = wal.append(entry2).await.expect("append failed");

        assert!(lsn1 > 0);
        assert!(lsn2 > lsn1);

        wal.flush().await.expect("flush failed");

        // Simple file size check
        let metadata = tokio::fs::metadata(&path).await.unwrap();
        // Header (8+4+4=16) * 2 + Payload (9 + 12) = 32 + 21 = 53 bytes
        assert_eq!(
            metadata.len(),
            (16 * 2) + entry1.len() as u64 + entry2.len() as u64
        );
    }

    #[tokio::test]
    async fn test_wal_replay() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("replay.wal");

        // 1. Write entries
        {
            let mut wal = Wal::open(&path).await.unwrap();
            wal.append(b"Entry 1").await.unwrap();
            wal.append(b"Entry 2").await.unwrap();
            wal.flush().await.unwrap();
        }

        // 2. Reopen and Replay
        {
            let mut wal = Wal::open(&path).await.unwrap();
            let mut recovered = Vec::new();

            let last_lsn = wal
                .replay(|lsn, payload| {
                    recovered.push((lsn, payload));
                    Ok(())
                })
                .await
                .unwrap();

            assert_eq!(last_lsn, 2);
            assert_eq!(recovered.len(), 2);
            assert_eq!(recovered[0].1, b"Entry 1");
            assert_eq!(recovered[1].1, b"Entry 2");
            assert_eq!(wal.current_lsn(), 2);
        }
    }

    #[tokio::test]
    async fn test_wal_open_restores_current_lsn_without_replay() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("restore_lsn.wal");

        {
            let mut wal = Wal::open(&path).await.unwrap();
            assert_eq!(wal.append(b"Entry 1").await.unwrap(), 1);
            assert_eq!(wal.append(b"Entry 2").await.unwrap(), 2);
            wal.flush().await.unwrap();
        }

        {
            let mut wal = Wal::open(&path).await.unwrap();
            assert_eq!(wal.current_lsn(), 2);
            assert_eq!(wal.append(b"Entry 3").await.unwrap(), 3);
        }
    }

    #[tokio::test]
    async fn test_wal_open_truncates_partial_tail_and_restores_lsn() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("partial_tail_recovery.wal");

        let stable_len = {
            let mut wal = Wal::open(&path).await.unwrap();
            assert_eq!(wal.append(b"Entry 1").await.unwrap(), 1);
            assert_eq!(wal.append(b"Entry 2").await.unwrap(), 2);
            wal.flush().await.unwrap();
            tokio::fs::metadata(&path).await.unwrap().len()
        };

        {
            let payload = b"Entry 3";
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(payload);
            let crc = hasher.finalize();

            let mut file = tokio::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .await
                .unwrap();
            file.write_u64(3).await.unwrap();
            file.write_u32(crc).await.unwrap();
            file.write_u32(payload.len() as u32).await.unwrap();
            file.write_all(&payload[..3]).await.unwrap(); // Intentionally partial payload
            file.flush().await.unwrap();
        }

        let corrupted_len = tokio::fs::metadata(&path).await.unwrap().len();
        assert!(corrupted_len > stable_len);

        {
            let wal = Wal::open(&path).await.unwrap();
            assert_eq!(wal.current_lsn(), 2);
        }

        let recovered_len = tokio::fs::metadata(&path).await.unwrap().len();
        assert_eq!(recovered_len, stable_len);

        {
            let mut wal = Wal::open(&path).await.unwrap();
            assert_eq!(wal.append(b"Entry 3").await.unwrap(), 3);
        }
    }
}
