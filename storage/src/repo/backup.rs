use super::replay::{apply_replayed_entry, load_materialized_state_from_backup};
use super::{
    collect_backup_edges, current_unix_timestamp_ms, parse_wal_snapshot_lsn, RepoError, Repository,
    RepositoryBackupSnapshot, SnapshotView,
};
use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer;
use rkyv::Deserialize;

impl Repository {
    pub(super) async fn record_durable_snapshot(&self, durable_lsn: u64) -> Result<(), RepoError> {
        let mut catalog = self.snapshot_catalog.lock().await;
        catalog
            .record_snapshot(durable_lsn, current_unix_timestamp_ms())
            .await?;
        Ok(())
    }

    /// Create a durable backup snapshot file at the current WAL LSN.
    pub async fn create_backup_snapshot(&self) -> Result<String, RepoError> {
        let snapshot_manager = self
            .snapshot_manager
            .as_ref()
            .ok_or(RepoError::SnapshotNotConfigured)?;

        let snapshot = {
            let _tx_guard = self.tx_lock.lock().await;

            let lsn = {
                let mut wal = self.wal.lock().await;
                wal.flush().await?;
                wal.durable_lsn()
            };
            self.record_durable_snapshot(lsn).await?;

            let mut nodes: Vec<alayasiki_core::model::Node> =
                self.nodes.read().await.values().cloned().collect();
            nodes.sort_by_key(|node| node.id);

            let edges = {
                let index = self.hyper_index.read().await;
                collect_backup_edges(&index)
            };

            let mut idempotency: Vec<super::BackupIdempotencyRecord> = self
                .idempotency_index
                .read()
                .await
                .iter()
                .map(|(key, node_ids)| super::BackupIdempotencyRecord {
                    key: key.clone(),
                    node_ids: node_ids.clone(),
                })
                .collect();
            idempotency.sort_by(|a, b| a.key.cmp(&b.key));

            let mut edge_metadata: Vec<super::BackupEdgeMetadataRecord> = self
                .edge_metadata
                .read()
                .await
                .iter()
                .map(
                    |((source, target, relation), metadata)| super::BackupEdgeMetadataRecord {
                        source: *source,
                        target: *target,
                        relation: relation.clone(),
                        metadata: metadata.clone(),
                    },
                )
                .collect();
            edge_metadata.sort_by(|a, b| {
                a.source
                    .cmp(&b.source)
                    .then(a.target.cmp(&b.target))
                    .then(a.relation.cmp(&b.relation))
            });

            RepositoryBackupSnapshot {
                lsn,
                nodes,
                edges,
                idempotency,
                edge_metadata,
            }
        };

        let encoded = serialize_backup_snapshot(&snapshot)?;
        snapshot_manager
            .create_snapshot(snapshot.lsn, &encoded)
            .await?;

        Ok(format!("wal-lsn-{}", snapshot.lsn))
    }

    /// Rebuild in-memory state from the latest backup snapshot plus WAL delta replay.
    pub async fn restore_from_latest_backup(&self) -> Result<String, RepoError> {
        if self.snapshot_manager.is_none() {
            return Err(RepoError::SnapshotNotConfigured);
        }

        let _tx_guard = self.tx_lock.lock().await;
        let target_lsn = {
            let wal = self.wal.lock().await;
            wal.current_lsn()
        };

        let (mut materialized, base_lsn) = load_materialized_state_from_backup(
            self.snapshot_manager.as_ref(),
            Some(target_lsn),
            self.storage_profile.clone(),
        )
        .await?;

        {
            let mut wal = self.wal.lock().await;
            wal.replay(|lsn, data| {
                if lsn <= base_lsn || lsn > target_lsn {
                    return Ok(());
                }

                let archived = rkyv::check_archived_root::<super::WalEntry>(&data[..])
                    .map_err(|_| crate::wal::WalError::CorruptEntry)?;
                let entry: super::WalEntry = archived
                    .deserialize(&mut rkyv::Infallible)
                    .expect("infallible deserializer");
                apply_replayed_entry(
                    &entry,
                    &mut materialized.nodes,
                    &mut materialized.hyper_index,
                    &mut materialized.idempotency_index,
                    &mut materialized.edge_metadata,
                );
                Ok(())
            })
            .await?;
        }

        *self.nodes.write().await = materialized.nodes;
        *self.hyper_index.write().await = materialized.hyper_index;
        *self.idempotency_index.write().await = materialized.idempotency_index;
        *self.edge_metadata.write().await = materialized.edge_metadata;

        Ok(format!("wal-lsn-{target_lsn}"))
    }

    /// Materialize an immutable read view at the specified snapshot.
    /// Supported format: `wal-lsn-<number>`.
    pub async fn load_snapshot_view(&self, snapshot_id: &str) -> Result<SnapshotView, RepoError> {
        let target_lsn = parse_wal_snapshot_lsn(snapshot_id)
            .ok_or_else(|| RepoError::InvalidSnapshotId(snapshot_id.to_string()))?;

        let current_lsn = {
            let wal = self.wal.lock().await;
            wal.current_lsn()
        };
        if target_lsn > current_lsn {
            return Err(RepoError::SnapshotNotFound(snapshot_id.to_string()));
        }

        let (mut materialized, base_lsn) = load_materialized_state_from_backup(
            self.snapshot_manager.as_ref(),
            Some(target_lsn),
            self.storage_profile.clone(),
        )
        .await?;

        let mut wal = self.wal.lock().await;
        wal.replay(|lsn, data| {
            if lsn <= base_lsn || lsn > target_lsn {
                return Ok(());
            }

            let archived = rkyv::check_archived_root::<super::WalEntry>(&data[..])
                .map_err(|_| crate::wal::WalError::CorruptEntry)?;
            let entry: super::WalEntry = archived
                .deserialize(&mut rkyv::Infallible)
                .expect("infallible deserializer");
            apply_replayed_entry(
                &entry,
                &mut materialized.nodes,
                &mut materialized.hyper_index,
                &mut materialized.idempotency_index,
                &mut materialized.edge_metadata,
            );
            Ok(())
        })
        .await?;

        Ok(SnapshotView {
            snapshot_id: snapshot_id.to_string(),
            nodes: materialized.nodes,
            hyper_index: materialized.hyper_index,
            edge_metadata: materialized.edge_metadata,
        })
    }
}

fn serialize_backup_snapshot(snapshot: &RepositoryBackupSnapshot) -> Result<Vec<u8>, RepoError> {
    let mut serializer = AllocSerializer::<4096>::default();
    serializer
        .serialize_value(snapshot)
        .map_err(|_| RepoError::Serialization)?;
    Ok(serializer.into_serializer().into_inner().to_vec())
}
