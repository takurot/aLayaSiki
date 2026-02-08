use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::fs;

#[derive(Error, Debug)]
pub enum SnapshotError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
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
        if !self.dir.exists() {
            return Ok(None);
        }

        let mut entries = fs::read_dir(&self.dir).await?;
        let mut max_lsn = None;
        let mut max_path = None;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                if file_name.starts_with("snapshot_") && file_name.ends_with(".rkyv") {
                    let lsn_str = &file_name[9..29]; // "snapshot_" len is 9, 20 digits
                    if let Ok(lsn) = lsn_str.parse::<u64>() {
                        if max_lsn.is_none_or(|max| lsn > max) {
                            max_lsn = Some(lsn);
                            max_path = Some(path);
                        }
                    }
                }
            }
        }

        if let (Some(lsn), Some(path)) = (max_lsn, max_path) {
            Ok(Some((lsn, path)))
        } else {
            Ok(None)
        }
    }
}
