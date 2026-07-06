use super::{
    EdgeMetaKey, MaterializedState, RepoError, RepositoryBackupSnapshot, TxOperation, WalEntry,
};
use crate::hyper_index::HyperIndex;
use crate::snapshot::{SnapshotError, SnapshotManager};
use crate::tiering::StorageProfile;
use alayasiki_core::model::Node;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer;
use rkyv::Deserialize;
use std::collections::HashMap;
use std::path::Path;

pub(super) fn apply_replayed_entry(
    entry: &WalEntry,
    node_map: &mut HashMap<u64, Node>,
    h_index: &mut HyperIndex,
    idem_map: &mut HashMap<String, Vec<u64>>,
    edge_meta: &mut HashMap<EdgeMetaKey, HashMap<String, String>>,
) {
    match entry {
        WalEntry::Put(node) => {
            let id = node.id;
            let embedding = node.embedding.clone();
            node_map.insert(id, node.clone());
            h_index.insert_node(id, embedding);
        }
        WalEntry::PutEdge(edge) => {
            let key = (edge.source, edge.target, edge.relation.clone());
            if edge.metadata.is_empty() {
                edge_meta.remove(&key);
            } else {
                edge_meta.insert(key, edge.metadata.clone());
            }
            h_index.upsert_edge(edge.source, edge.target, &edge.relation, edge.weight);
        }
        WalEntry::Delete(id) => {
            node_map.remove(id);
            h_index.remove_node(*id);
            edge_meta.retain(|(src, tgt, _), _| *src != *id && *tgt != *id);
        }
        WalEntry::IdempotencyKey { key, node_ids } => {
            record_idempotency_if_absent(idem_map, key, node_ids);
        }
        WalEntry::Transaction(operations) => {
            for operation in operations {
                apply_tx_operation(operation, node_map, h_index, idem_map, edge_meta);
            }
        }
    }
}

pub(super) fn apply_tx_operation(
    operation: &TxOperation,
    node_map: &mut HashMap<u64, Node>,
    h_index: &mut HyperIndex,
    idem_map: &mut HashMap<String, Vec<u64>>,
    edge_meta: &mut HashMap<EdgeMetaKey, HashMap<String, String>>,
) {
    match operation {
        TxOperation::Put(node) => {
            let id = node.id;
            let embedding = node.embedding.clone();
            node_map.insert(id, node.clone());
            h_index.insert_node(id, embedding);
        }
        TxOperation::PutEdge(edge) => {
            let key = (edge.source, edge.target, edge.relation.clone());
            if edge.metadata.is_empty() {
                edge_meta.remove(&key);
            } else {
                edge_meta.insert(key, edge.metadata.clone());
            }
            h_index.upsert_edge(edge.source, edge.target, &edge.relation, edge.weight);
        }
        TxOperation::Delete(id) => {
            node_map.remove(id);
            h_index.remove_node(*id);
            edge_meta.retain(|(src, tgt, _), _| *src != *id && *tgt != *id);
        }
        TxOperation::RecordIdempotency { key, node_ids } => {
            record_idempotency_if_absent(idem_map, key, node_ids);
        }
    }
}

pub(super) fn mutations_to_tx_operations(mutations: &[super::IndexMutation]) -> Vec<TxOperation> {
    mutations
        .iter()
        .map(|mutation| match mutation {
            super::IndexMutation::PutNode(node) => TxOperation::Put(node.clone()),
            super::IndexMutation::PutEdge(edge) => TxOperation::PutEdge(edge.clone()),
            super::IndexMutation::DeleteNode(id) => TxOperation::Delete(*id),
        })
        .collect()
}

pub(super) fn serialize_wal_entry(entry: &WalEntry) -> Result<Vec<u8>, RepoError> {
    let mut serializer = AllocSerializer::<4096>::default();
    serializer
        .serialize_value(entry)
        .map_err(|_| RepoError::Serialization)?;
    Ok(serializer.into_serializer().into_inner().to_vec())
}

pub(super) async fn load_materialized_state_from_backup(
    snapshot_manager: Option<&SnapshotManager>,
    target_lsn: Option<u64>,
    storage_profile: StorageProfile,
) -> Result<(MaterializedState, u64), RepoError> {
    let empty_state = || MaterializedState {
        nodes: HashMap::new(),
        hyper_index: HyperIndex::with_storage_profile(storage_profile.clone()),
        idempotency_index: HashMap::new(),
        edge_metadata: HashMap::new(),
    };

    let Some(manager) = snapshot_manager else {
        return Ok((empty_state(), 0));
    };

    let selected = match target_lsn {
        Some(lsn) => manager.latest_snapshot_at_or_before(lsn).await?,
        None => manager.latest_snapshot().await?,
    };

    let Some((snapshot_lsn, path)) = selected else {
        return Ok((empty_state(), 0));
    };

    let snapshot = deserialize_backup_snapshot(&path).await?;
    if snapshot.lsn != snapshot_lsn {
        return Err(RepoError::Deserialization);
    }

    let mut nodes = HashMap::new();
    let mut hyper_index = HyperIndex::with_storage_profile(storage_profile);
    for node in snapshot.nodes {
        let id = node.id;
        hyper_index.insert_node(id, node.embedding.clone());
        nodes.insert(id, node);
    }

    for edge in snapshot.edges {
        hyper_index.upsert_edge(edge.source, edge.target, &edge.relation, edge.weight);
    }

    let mut idempotency_index = HashMap::new();
    for record in snapshot.idempotency {
        idempotency_index.insert(record.key, record.node_ids);
    }

    let mut edge_metadata = HashMap::new();
    for record in snapshot.edge_metadata {
        edge_metadata.insert(
            (record.source, record.target, record.relation),
            record.metadata,
        );
    }

    Ok((
        MaterializedState {
            nodes,
            hyper_index,
            idempotency_index,
            edge_metadata,
        },
        snapshot_lsn,
    ))
}

async fn deserialize_backup_snapshot(path: &Path) -> Result<RepositoryBackupSnapshot, RepoError> {
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|err| RepoError::Snapshot(SnapshotError::Io(err)))?;
    let archived = rkyv::check_archived_root::<RepositoryBackupSnapshot>(&bytes[..])
        .map_err(|_| RepoError::Deserialization)?;
    archived
        .deserialize(&mut rkyv::Infallible)
        .map_err(|_| RepoError::Deserialization)
}

fn record_idempotency_if_absent(
    idem_map: &mut HashMap<String, Vec<u64>>,
    key: &str,
    node_ids: &[u64],
) {
    idem_map
        .entry(key.to_string())
        .or_insert_with(|| node_ids.to_vec());
}
