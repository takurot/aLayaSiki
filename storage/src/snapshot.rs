use alayasiki_core::error::{AlayasikiError, ErrorCode};
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::fs;

#[derive(Error, Debug)]
pub enum SnapshotError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

impl AlayasikiError for SnapshotError {
    fn error_code(&self) -> ErrorCode {
        match self {
            SnapshotError::Io(_) => ErrorCode::Internal,
        }
    }
}

pub struct SnapshotManager {
    dir: PathBuf,
}

impl SnapshotManager {
    pub fn new(dir: impl AsRef<Path>) -> Self {
        Self {
            dir: dir.as_ref().to_path_buf(),
        }
    }

    /// Create a new snapshot with the given LSN and data.
    /// Atomically writes to a temp file then renames.
    pub async fn create_snapshot(&self, lsn: u64, data: &[u8]) -> Result<PathBuf, SnapshotError> {
        if !self.dir.exists() {
            fs::create_dir_all(&self.dir).await?;
        }

        let path = self.dir.join(format!("snapshot_{:020}.rkyv", lsn));
        let tmp_path = path.with_extension("tmp");

        fs::write(&tmp_path, data).await?;
        fs::rename(&tmp_path, &path).await?;

        Ok(path)
    }

    /// Find the latest snapshot file (highest LSN).
    pub async fn latest_snapshot(&self) -> Result<Option<(u64, PathBuf)>, SnapshotError> {
        self.latest_snapshot_at_or_before(u64::MAX).await
    }

    /// Find the latest snapshot file whose LSN is <= the requested LSN.
    pub async fn latest_snapshot_at_or_before(
        &self,
        upper_lsn: u64,
    ) -> Result<Option<(u64, PathBuf)>, SnapshotError> {
        if !self.dir.exists() {
            return Ok(None);
        }

        let mut entries = fs::read_dir(&self.dir).await?;
        let mut max_seen_lsn = None;
        let mut max_path = None;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                if let Some(lsn) = parse_snapshot_lsn(file_name) {
                    if lsn <= upper_lsn && max_seen_lsn.is_none_or(|max| lsn > max) {
                        max_seen_lsn = Some(lsn);
                        max_path = Some(path);
                    }
                }
            }
        }

        if let (Some(lsn), Some(path)) = (max_seen_lsn, max_path) {
            Ok(Some((lsn, path)))
        } else {
            Ok(None)
        }
    }
}

fn parse_snapshot_lsn(file_name: &str) -> Option<u64> {
    let lsn = file_name.strip_prefix("snapshot_")?.strip_suffix(".rkyv")?;
    lsn.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn latest_snapshot_at_or_before_filters_by_lsn() {
        let dir = tempdir().unwrap();
        let manager = SnapshotManager::new(dir.path());

        manager.create_snapshot(1, b"s1").await.unwrap();
        manager.create_snapshot(5, b"s5").await.unwrap();
        manager.create_snapshot(9, b"s9").await.unwrap();

        let at_or_before_five = manager.latest_snapshot_at_or_before(5).await.unwrap();
        assert_eq!(at_or_before_five.unwrap().0, 5);

        let at_or_before_seven = manager.latest_snapshot_at_or_before(7).await.unwrap();
        assert_eq!(at_or_before_seven.unwrap().0, 5);

        let no_match = manager.latest_snapshot_at_or_before(0).await.unwrap();
        assert!(no_match.is_none());
    }
}
