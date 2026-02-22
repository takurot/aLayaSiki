use alayasiki_core::error::{AlayasikiError, ErrorCode};
use crate::crypto::{AtRestCipher, CryptoError, NoOpCipher};
use crc32fast::Hasher;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
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

pub struct Wal {
    file: BufWriter<File>,
    current_lsn: AtomicU64,
    cipher: Arc<dyn AtRestCipher>,
}

impl Wal {
    /// Open a WAL file. If it doesn't exist, it will be created.
    /// If it exists, it will be read to determine the next LSN.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, WalError> {
        Self::open_with_cipher(path, Arc::new(NoOpCipher)).await
    }

    /// Open a WAL file with a custom at-rest cipher (KMS hook point).
    pub async fn open_with_cipher(
        path: impl AsRef<Path>,
        cipher: Arc<dyn AtRestCipher>,
    ) -> Result<Self, WalError> {
        let path = path.as_ref().to_path_buf();

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

        // TODO: Scan existing file to recovery last LSN (simplified for now to 0)
        let current_lsn = AtomicU64::new(0);

        Ok(Self {
            file: BufWriter::new(file),
            current_lsn,
            cipher,
        })
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

        // Note: We don't flush here by default for batch performance,
        // explicit flush() or periodic flush is expected.

        Ok(lsn)
    }

    /// Flush the internal buffer to disk, ensuring durability.
    pub async fn flush(&mut self) -> Result<(), WalError> {
        self.file.flush().await?;
        self.file.get_ref().sync_all().await?; // fsync
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
        // Seek to start
        self.file.flush().await?; // Ensure everything is written before seeking
        let file = self.file.get_mut();
        file.seek(std::io::SeekFrom::Start(0)).await?;

        let mut last_lsn = 0;
        let mut valid_end_pos = 0;

        loop {
            // Read Header
            let lsn = match file.read_u64().await {
                Ok(v) => v,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break, // End of file
                Err(e) => return Err(WalError::Io(e)),
            };

            let crc = file.read_u32().await?;
            let len = file.read_u32().await? as usize;

            // Read Payload
            let mut payload = vec![0u8; len];
            match file.read_exact(&mut payload).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break, // Partial write
                Err(e) => return Err(WalError::Io(e)),
            }

            // Verify CRC
            let mut hasher = Hasher::new();
            hasher.update(&payload);
            if hasher.finalize() != crc {
                return Err(WalError::CrcMismatch);
            }

            let decrypted_payload = self.cipher.decrypt(&payload)?;
            callback(lsn, decrypted_payload)?;
            last_lsn = lsn;
            valid_end_pos = file.stream_position().await?;
        }

        // Truncate partial writes at the end
        if valid_end_pos < file.metadata().await?.len() {
            file.set_len(valid_end_pos).await?;
        }

        // Restore cursor to end
        file.seek(std::io::SeekFrom::End(0)).await?;
        self.current_lsn.store(last_lsn, Ordering::SeqCst);

        Ok(last_lsn)
    }
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
}
