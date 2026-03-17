use alayasiki_core::error::{AlayasikiError, ErrorCode};
use rkyv::ser::{serializers::AllocSerializer, Serializer};
use rkyv::{Archive, Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::fs;

#[derive(Error, Debug)]
pub enum SnapshotError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error")]
    Serialization,
    #[error("Deserialization error")]
    Deserialization,
}

impl AlayasikiError for SnapshotError {
    fn error_code(&self) -> ErrorCode {
        match self {
            SnapshotError::Io(_) => ErrorCode::Internal,
            SnapshotError::Serialization => ErrorCode::Internal,
            SnapshotError::Deserialization => ErrorCode::Internal,
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

#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[archive(check_bytes)]
pub struct SnapshotCatalogEntry {
    pub snapshot_id: String,
    pub lsn: u64,
    pub created_at_unix_ms: i64,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
#[archive(check_bytes)]
struct SnapshotCatalogFile {
    entries: Vec<SnapshotCatalogEntry>,
}

pub struct SnapshotCatalog {
    path: Option<PathBuf>,
    entries: Vec<SnapshotCatalogEntry>,
}

impl SnapshotCatalog {
    pub fn new_in_memory() -> Self {
        Self {
            path: None,
            entries: Vec::new(),
        }
    }

    pub async fn open(path: impl AsRef<Path>) -> Result<Self, SnapshotError> {
        let path = path.as_ref().to_path_buf();
        if !path.exists() {
            return Ok(Self {
                path: Some(path),
                entries: Vec::new(),
            });
        }

        let bytes = fs::read(&path).await?;
        let archived = rkyv::check_archived_root::<SnapshotCatalogFile>(&bytes[..])
            .map_err(|_| SnapshotError::Deserialization)?;
        let file: SnapshotCatalogFile = archived
            .deserialize(&mut rkyv::Infallible)
            .map_err(|_| SnapshotError::Deserialization)?;

        Ok(Self {
            path: Some(path),
            entries: file.entries,
        })
    }

    pub fn entries(&self) -> &[SnapshotCatalogEntry] {
        &self.entries
    }

    pub async fn truncate_after_lsn(&mut self, max_lsn: u64) -> Result<bool, SnapshotError> {
        let original_len = self.entries.len();
        self.entries.retain(|entry| entry.lsn <= max_lsn);
        if self.entries.len() == original_len {
            return Ok(false);
        }

        self.persist().await?;
        Ok(true)
    }

    pub async fn record_snapshot(
        &mut self,
        lsn: u64,
        created_at_unix_ms: i64,
    ) -> Result<bool, SnapshotError> {
        if self.entries.last().is_some_and(|entry| entry.lsn >= lsn) {
            return Ok(false);
        }

        self.entries.push(SnapshotCatalogEntry {
            snapshot_id: format!("wal-lsn-{lsn}"),
            lsn,
            created_at_unix_ms,
        });
        self.persist().await?;
        Ok(true)
    }

    pub fn resolve_as_of(&self, as_of_unix_ms: i64) -> Option<&SnapshotCatalogEntry> {
        let idx = self
            .entries
            .partition_point(|entry| entry.created_at_unix_ms <= as_of_unix_ms);
        if idx == 0 {
            None
        } else {
            self.entries.get(idx - 1)
        }
    }

    async fn persist(&self) -> Result<(), SnapshotError> {
        let Some(path) = &self.path else {
            return Ok(());
        };

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let file = SnapshotCatalogFile {
            entries: self.entries.clone(),
        };
        let mut serializer = AllocSerializer::<1024>::default();
        serializer
            .serialize_value(&file)
            .map_err(|_| SnapshotError::Serialization)?;
        let bytes = serializer.into_serializer().into_inner();

        let tmp_path = path.with_extension("tmp");
        fs::write(&tmp_path, bytes).await?;
        fs::rename(&tmp_path, path).await?;
        Ok(())
    }
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

    #[tokio::test]
    async fn snapshot_catalog_persists_and_resolves_as_of() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("catalog.rkyv");
        let mut catalog = SnapshotCatalog::open(&path).await.unwrap();

        catalog.record_snapshot(0, 100).await.unwrap();
        catalog.record_snapshot(2, 200).await.unwrap();
        catalog.record_snapshot(5, 350).await.unwrap();

        assert_eq!(
            catalog.resolve_as_of(99),
            None,
            "timestamps before the first snapshot should not resolve"
        );
        assert_eq!(
            catalog
                .resolve_as_of(200)
                .map(|entry| entry.snapshot_id.as_str()),
            Some("wal-lsn-2")
        );
        assert_eq!(
            catalog
                .resolve_as_of(999)
                .map(|entry| entry.snapshot_id.as_str()),
            Some("wal-lsn-5")
        );

        let reopened = SnapshotCatalog::open(&path).await.unwrap();
        assert_eq!(reopened.entries().len(), 3);
        assert_eq!(
            reopened
                .resolve_as_of(349)
                .map(|entry| entry.snapshot_id.as_str()),
            Some("wal-lsn-2")
        );
    }

    #[tokio::test]
    async fn snapshot_catalog_truncates_stale_entries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("catalog-truncate.rkyv");
        let mut catalog = SnapshotCatalog::open(&path).await.unwrap();

        catalog.record_snapshot(0, 100).await.unwrap();
        catalog.record_snapshot(3, 200).await.unwrap();
        catalog.record_snapshot(7, 300).await.unwrap();
        catalog.truncate_after_lsn(3).await.unwrap();

        assert_eq!(catalog.entries().len(), 2);
        assert_eq!(
            catalog
                .resolve_as_of(999)
                .map(|entry| entry.snapshot_id.as_str()),
            Some("wal-lsn-3")
        );
    }
}
