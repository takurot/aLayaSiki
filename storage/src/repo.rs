use crate::crypto::{AtRestCipher, NoOpCipher};
use crate::hyper_index::HyperIndex;
use crate::snapshot::{SnapshotError, SnapshotManager};
use crate::wal::{Wal, WalError};
use alayasiki_core::model::{Edge, Node};
use rkyv::ser::{serializers::AllocSerializer, Serializer};
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use tokio::fs;
use tokio::sync::{Mutex, RwLock};

#[derive(Error, Debug)]
pub enum RepoError {
    #[error("WAL error: {0}")]
    Wal(#[from] WalError),
    #[error("Serialization error")]
    Serialization,
    #[error("Deserialization error")]
    Deserialization,
    #[error("Not found")]
    NotFound,
    #[error("Invalid transaction: {0}")]
    InvalidTransaction(String),
    #[error("Invalid snapshot id: {0}")]
    InvalidSnapshotId(String),
    #[error("Snapshot not found: {0}")]
    SnapshotNotFound(String),
    #[error("Snapshot manager is not configured")]
    SnapshotNotConfigured,
    #[error("Snapshot error: {0}")]
    Snapshot(#[from] SnapshotError),
}

/// WAL Entry types for durability
#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
#[archive(check_bytes)]
pub enum WalEntry {
    Put(Node),
    PutEdge(Edge),
    Delete(u64),
    IdempotencyKey { key: String, node_ids: Vec<u64> },
    Transaction(Vec<TxOperation>),
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
#[archive(check_bytes)]
pub enum TxOperation {
    Put(Node),
    PutEdge(Edge),
    Delete(u64),
}

#[derive(Debug, Clone)]
pub enum IndexMutation {
    PutNode(Node),
    PutEdge(Edge),
    DeleteNode(u64),
}

/// Key for edge metadata lookup: (source, target, relation)
pub type EdgeMetaKey = (u64, u64, String);

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
#[archive(check_bytes)]
struct BackupEdgeRecord {
    source: u64,
    target: u64,
    relation: String,
    weight: f32,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
#[archive(check_bytes)]
struct BackupIdempotencyRecord {
    key: String,
    node_ids: Vec<u64>,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
#[archive(check_bytes)]
struct BackupEdgeMetadataRecord {
    source: u64,
    target: u64,
    relation: String,
    metadata: HashMap<String, String>,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
#[archive(check_bytes)]
struct RepositoryBackupSnapshot {
    lsn: u64,
    nodes: Vec<Node>,
    edges: Vec<BackupEdgeRecord>,
    idempotency: Vec<BackupIdempotencyRecord>,
    edge_metadata: Vec<BackupEdgeMetadataRecord>,
}

struct MaterializedState {
    nodes: HashMap<u64, Node>,
    hyper_index: HyperIndex,
    idempotency_index: HashMap<String, Vec<u64>>,
    edge_metadata: HashMap<EdgeMetaKey, HashMap<String, String>>,
}

impl MaterializedState {
    fn empty() -> Self {
        Self {
            nodes: HashMap::new(),
            hyper_index: HyperIndex::new(),
            idempotency_index: HashMap::new(),
            edge_metadata: HashMap::new(),
        }
    }
}

pub struct SnapshotView {
    snapshot_id: String,
    nodes: HashMap<u64, Node>,
    hyper_index: HyperIndex,
    edge_metadata: HashMap<EdgeMetaKey, HashMap<String, String>>,
}

impl SnapshotView {
    pub fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    pub fn list_node_ids(&self) -> Vec<u64> {
        let mut out: Vec<u64> = self.nodes.keys().copied().collect();
        out.sort_unstable();
        out
    }

    pub fn get_nodes_by_ids(&self, ids: &[u64]) -> Vec<Node> {
        let mut out: Vec<Node> = ids
            .iter()
            .filter_map(|id| self.nodes.get(id).cloned())
            .collect();
        out.sort_by_key(|node| node.id);
        out
    }

    pub fn embedding_dimension(&self) -> Option<usize> {
        self.nodes
            .values()
            .find_map(|node| (!node.embedding.is_empty()).then_some(node.embedding.len()))
    }

    pub fn search_vector(&self, query: &[f32], k: usize) -> Vec<(u64, f32)> {
        self.hyper_index.search_vector(query, k)
    }

    pub fn neighbors(&self, node_id: u64) -> Vec<(u64, String, f32)> {
        self.hyper_index
            .graph_index
            .neighbors(node_id)
            .into_iter()
            .cloned()
            .collect()
    }

    pub fn get_edge_metadata_bulk(
        &self,
        keys: &[(u64, u64, String)],
    ) -> HashMap<EdgeMetaKey, HashMap<String, String>> {
        keys.iter()
            .filter_map(|key| {
                self.edge_metadata
                    .get(key)
                    .map(|meta| (key.clone(), meta.clone()))
            })
            .collect()
    }
}

pub struct Repository {
    wal: Arc<Mutex<Wal>>,
    tx_lock: Arc<Mutex<()>>,
    nodes: Arc<RwLock<HashMap<u64, Node>>>,
    pub hyper_index: Arc<RwLock<HyperIndex>>,
    idempotency_index: Arc<RwLock<HashMap<String, Vec<u64>>>>,
    edge_metadata: Arc<RwLock<HashMap<EdgeMetaKey, HashMap<String, String>>>>,
    snapshot_manager: Option<SnapshotManager>,
}

impl Repository {
    /// Create a new empty Repository (no replay)
    pub fn new(wal: Arc<Mutex<Wal>>) -> Self {
        Self {
            wal,
            tx_lock: Arc::new(Mutex::new(())),
            nodes: Arc::new(RwLock::new(HashMap::new())),
            hyper_index: Arc::new(RwLock::new(HyperIndex::new())),
            idempotency_index: Arc::new(RwLock::new(HashMap::new())),
            edge_metadata: Arc::new(RwLock::new(HashMap::new())),
            snapshot_manager: None,
        }
    }

    /// Open a Repository with WAL replay to restore previous state
    pub async fn open(wal_path: impl AsRef<Path>) -> Result<Self, RepoError> {
        Self::open_with_cipher(wal_path, Arc::new(NoOpCipher)).await
    }

    /// Open a repository with a custom at-rest cipher for WAL replay and writes.
    pub async fn open_with_cipher(
        wal_path: impl AsRef<Path>,
        cipher: Arc<dyn AtRestCipher>,
    ) -> Result<Self, RepoError> {
        Self::open_internal(wal_path.as_ref().to_path_buf(), cipher, None).await
    }

    /// Open a repository and restore state from backup snapshots first, then WAL deltas.
    pub async fn open_with_snapshots(
        wal_path: impl AsRef<Path>,
        snapshot_dir: impl AsRef<Path>,
    ) -> Result<Self, RepoError> {
        Self::open_with_cipher_and_snapshots(wal_path, Arc::new(NoOpCipher), snapshot_dir).await
    }

    /// Open a repository with custom cipher and snapshot-backed recovery.
    pub async fn open_with_cipher_and_snapshots(
        wal_path: impl AsRef<Path>,
        cipher: Arc<dyn AtRestCipher>,
        snapshot_dir: impl AsRef<Path>,
    ) -> Result<Self, RepoError> {
        let snapshot_manager = SnapshotManager::new(snapshot_dir.as_ref());
        Self::open_internal(
            wal_path.as_ref().to_path_buf(),
            cipher,
            Some(snapshot_manager),
        )
        .await
    }

    async fn open_internal(
        wal_path: PathBuf,
        cipher: Arc<dyn AtRestCipher>,
        snapshot_manager: Option<SnapshotManager>,
    ) -> Result<Self, RepoError> {
        let wal_instance = Wal::open_with_cipher(&wal_path, cipher).await?;
        let wal = Arc::new(Mutex::new(wal_instance));
        let tx_lock = Arc::new(Mutex::new(()));
        let (mut materialized, base_lsn) =
            load_materialized_state_from_backup(snapshot_manager.as_ref(), None).await?;

        // Replay WAL entries newer than the snapshot baseline.
        {
            let mut wal_lock = wal.lock().await;
            let last_replayed_lsn = wal_lock
                .replay(|lsn, data| {
                    if lsn <= base_lsn {
                        return Ok(());
                    }
                    // Deserialize (zero-copy check)
                    let archived = rkyv::check_archived_root::<WalEntry>(&data[..])
                        .map_err(|_| WalError::CorruptEntry)?;
                    let entry: WalEntry = archived.deserialize(&mut rkyv::Infallible).unwrap();
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

            if base_lsn > last_replayed_lsn {
                return Err(RepoError::SnapshotNotFound(format!("wal-lsn-{base_lsn}")));
            }
        }

        Ok(Self {
            wal,
            tx_lock,
            nodes: Arc::new(RwLock::new(materialized.nodes)),
            hyper_index: Arc::new(RwLock::new(materialized.hyper_index)),
            idempotency_index: Arc::new(RwLock::new(materialized.idempotency_index)),
            edge_metadata: Arc::new(RwLock::new(materialized.edge_metadata)),
            snapshot_manager,
        })
    }

    pub async fn put_node(&self, node: Node) -> Result<(), RepoError> {
        self.apply_index_transaction(vec![IndexMutation::PutNode(node)])
            .await
    }

    pub async fn put_edge(&self, edge: Edge) -> Result<(), RepoError> {
        self.apply_index_transaction(vec![IndexMutation::PutEdge(edge)])
            .await
    }

    pub async fn get_node(&self, id: u64) -> Result<Node, RepoError> {
        let nodes = self.nodes.read().await;
        nodes.get(&id).cloned().ok_or(RepoError::NotFound)
    }

    pub async fn list_node_ids(&self) -> Vec<u64> {
        let nodes = self.nodes.read().await;
        let mut out: Vec<u64> = nodes.keys().copied().collect();
        out.sort_unstable();
        out
    }

    pub async fn get_nodes_by_ids(&self, ids: &[u64]) -> Vec<Node> {
        let nodes = self.nodes.read().await;
        let mut out: Vec<Node> = ids.iter().filter_map(|id| nodes.get(id).cloned()).collect();
        out.sort_by_key(|node| node.id);
        out
    }

    pub async fn embedding_dimension(&self) -> Option<usize> {
        let nodes = self.nodes.read().await;
        nodes
            .values()
            .find_map(|node| (!node.embedding.is_empty()).then_some(node.embedding.len()))
    }

    pub async fn delete_node(&self, id: u64) -> Result<(), RepoError> {
        self.apply_index_transaction(vec![IndexMutation::DeleteNode(id)])
            .await
    }

    /// Apply index updates atomically within one transaction boundary.
    /// If validation fails, nothing is written to WAL or in-memory indexes.
    pub async fn apply_index_transaction(
        &self,
        mutations: Vec<IndexMutation>,
    ) -> Result<(), RepoError> {
        if mutations.is_empty() {
            return Ok(());
        }

        // Serialize transaction validation and apply to avoid TOCTOU between concurrent writers.
        let _tx_guard = self.tx_lock.lock().await;

        self.validate_index_transaction(&mutations).await?;

        let tx_operations = mutations_to_tx_operations(&mutations);
        let tx_entry = WalEntry::Transaction(tx_operations);
        let tx_bytes = serialize_wal_entry(&tx_entry)?;

        // Durability first for the full transaction boundary.
        {
            let mut wal = self.wal.lock().await;
            wal.append(&tx_bytes).await?;
            wal.flush().await?;
        }

        // Apply in-memory updates under write locks so readers don't observe partial state.
        let mut nodes = self.nodes.write().await;
        let mut index = self.hyper_index.write().await;
        let mut edge_meta = self.edge_metadata.write().await;

        for mutation in mutations {
            match mutation {
                IndexMutation::PutNode(node) => {
                    let id = node.id;
                    let embedding = node.embedding.clone();
                    nodes.insert(id, node);
                    index.insert_node(id, embedding);
                }
                IndexMutation::PutEdge(edge) => {
                    let key = (edge.source, edge.target, edge.relation.clone());
                    // Always update: replace with new metadata or clear stale provenance
                    if edge.metadata.is_empty() {
                        edge_meta.remove(&key);
                    } else {
                        edge_meta.insert(key, edge.metadata.clone());
                    }
                    index.upsert_edge(edge.source, edge.target, &edge.relation, edge.weight);
                }
                IndexMutation::DeleteNode(id) => {
                    nodes.remove(&id);
                    index.remove_node(id);
                    // Remove edge metadata for edges involving the deleted node
                    edge_meta.retain(|(src, tgt, _), _| *src != id && *tgt != id);
                }
            }
        }

        Ok(())
    }
    pub async fn check_idempotency(&self, key: &str) -> Option<Vec<u64>> {
        let index = self.idempotency_index.read().await;
        index.get(key).cloned()
    }

    /// Get metadata for an edge identified by (source, target, relation).
    pub async fn get_edge_metadata(
        &self,
        source: u64,
        target: u64,
        relation: &str,
    ) -> HashMap<String, String> {
        let edge_meta = self.edge_metadata.read().await;
        edge_meta
            .get(&(source, target, relation.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    /// Bulk-get metadata for multiple edges in a single lock acquisition.
    /// Returns a map from (source, target, relation) to metadata.
    pub async fn get_edge_metadata_bulk(
        &self,
        keys: &[(u64, u64, String)],
    ) -> HashMap<EdgeMetaKey, HashMap<String, String>> {
        let edge_meta = self.edge_metadata.read().await;
        keys.iter()
            .filter_map(|key| edge_meta.get(key).map(|meta| (key.clone(), meta.clone())))
            .collect()
    }

    pub async fn current_snapshot_id(&self) -> String {
        let wal = self.wal.lock().await;
        format!("wal-lsn-{}", wal.current_lsn())
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
                let wal = self.wal.lock().await;
                wal.current_lsn()
            };

            let mut nodes: Vec<Node> = self.nodes.read().await.values().cloned().collect();
            nodes.sort_by_key(|node| node.id);

            let edges = {
                let index = self.hyper_index.read().await;
                collect_backup_edges(&index)
            };

            let mut idempotency: Vec<BackupIdempotencyRecord> = self
                .idempotency_index
                .read()
                .await
                .iter()
                .map(|(key, node_ids)| BackupIdempotencyRecord {
                    key: key.clone(),
                    node_ids: node_ids.clone(),
                })
                .collect();
            idempotency.sort_by(|a, b| a.key.cmp(&b.key));

            let mut edge_metadata: Vec<BackupEdgeMetadataRecord> = self
                .edge_metadata
                .read()
                .await
                .iter()
                .map(
                    |((source, target, relation), metadata)| BackupEdgeMetadataRecord {
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

        let (mut materialized, base_lsn) =
            load_materialized_state_from_backup(self.snapshot_manager.as_ref(), Some(target_lsn))
                .await?;

        {
            let mut wal = self.wal.lock().await;
            wal.replay(|lsn, data| {
                if lsn <= base_lsn || lsn > target_lsn {
                    return Ok(());
                }

                let archived = rkyv::check_archived_root::<WalEntry>(&data[..])
                    .map_err(|_| WalError::CorruptEntry)?;
                let entry: WalEntry = archived.deserialize(&mut rkyv::Infallible).unwrap();
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

        let (mut materialized, base_lsn) =
            load_materialized_state_from_backup(self.snapshot_manager.as_ref(), Some(target_lsn))
                .await?;

        let mut wal = self.wal.lock().await;
        wal.replay(|lsn, data| {
            if lsn <= base_lsn || lsn > target_lsn {
                return Ok(());
            }

            let archived = rkyv::check_archived_root::<WalEntry>(&data[..])
                .map_err(|_| WalError::CorruptEntry)?;
            let entry: WalEntry = archived.deserialize(&mut rkyv::Infallible).unwrap();
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

    pub async fn record_idempotency(&self, key: &str, node_ids: Vec<u64>) -> Result<(), RepoError> {
        // Optimization: Lock once and check.
        // Review feedback suggests removing double-check pattern if racy or checking under write lock.
        // We will just acquire write lock immediately to be atomic and safe.
        // Read check optimization is good for read-heavy, but here we expect write if we got this far.

        {
            let mut index = self.idempotency_index.write().await;
            if index.contains_key(key) {
                return Ok(());
            }

            // Create WalEntry inside write lock to ensure consistency (though WAL append is async).
            // Actually, we should serialize first to avoid holding lock during serialization?
            // But we can't check-then-serialize-then-lock-then-write without race.
            // So we hold lock.

            let entry = WalEntry::IdempotencyKey {
                key: key.to_string(),
                node_ids: node_ids.clone(),
            };
            // Increase buffer size for large ID lists
            let mut serializer = AllocSerializer::<4096>::default();
            serializer
                .serialize_value(&entry)
                .map_err(|_| RepoError::Serialization)?;
            let bytes = serializer.into_serializer().into_inner();

            {
                let mut wal = self.wal.lock().await;
                wal.append(&bytes).await?;
                wal.flush().await?;
            }

            index.insert(key.to_string(), node_ids);
        }

        Ok(())
    }

    async fn validate_index_transaction(
        &self,
        mutations: &[IndexMutation],
    ) -> Result<(), RepoError> {
        let nodes = self.nodes.read().await;
        let mut visible_nodes: HashSet<u64> = nodes.keys().copied().collect();

        for mutation in mutations {
            match mutation {
                IndexMutation::PutNode(node) => {
                    visible_nodes.insert(node.id);
                }
                IndexMutation::PutEdge(edge) => {
                    if !visible_nodes.contains(&edge.source) {
                        return Err(RepoError::InvalidTransaction(format!(
                            "edge source {} does not exist",
                            edge.source
                        )));
                    }
                    if !visible_nodes.contains(&edge.target) {
                        return Err(RepoError::InvalidTransaction(format!(
                            "edge target {} does not exist",
                            edge.target
                        )));
                    }
                }
                IndexMutation::DeleteNode(id) => {
                    if !visible_nodes.remove(id) {
                        return Err(RepoError::NotFound);
                    }
                }
            }
        }

        Ok(())
    }
}

fn mutations_to_tx_operations(mutations: &[IndexMutation]) -> Vec<TxOperation> {
    mutations
        .iter()
        .map(|mutation| match mutation {
            IndexMutation::PutNode(node) => TxOperation::Put(node.clone()),
            IndexMutation::PutEdge(edge) => TxOperation::PutEdge(edge.clone()),
            IndexMutation::DeleteNode(id) => TxOperation::Delete(*id),
        })
        .collect()
}

fn serialize_wal_entry(entry: &WalEntry) -> Result<Vec<u8>, RepoError> {
    let mut serializer = AllocSerializer::<4096>::default();
    serializer
        .serialize_value(entry)
        .map_err(|_| RepoError::Serialization)?;
    Ok(serializer.into_serializer().into_inner().to_vec())
}

fn serialize_backup_snapshot(snapshot: &RepositoryBackupSnapshot) -> Result<Vec<u8>, RepoError> {
    let mut serializer = AllocSerializer::<4096>::default();
    serializer
        .serialize_value(snapshot)
        .map_err(|_| RepoError::Serialization)?;
    Ok(serializer.into_serializer().into_inner().to_vec())
}

async fn deserialize_backup_snapshot(path: &Path) -> Result<RepositoryBackupSnapshot, RepoError> {
    let bytes = fs::read(path)
        .await
        .map_err(|err| RepoError::Snapshot(SnapshotError::Io(err)))?;
    let archived = rkyv::check_archived_root::<RepositoryBackupSnapshot>(&bytes[..])
        .map_err(|_| RepoError::Deserialization)?;
    archived
        .deserialize(&mut rkyv::Infallible)
        .map_err(|_| RepoError::Deserialization)
}

async fn load_materialized_state_from_backup(
    snapshot_manager: Option<&SnapshotManager>,
    target_lsn: Option<u64>,
) -> Result<(MaterializedState, u64), RepoError> {
    let Some(manager) = snapshot_manager else {
        return Ok((MaterializedState::empty(), 0));
    };

    let selected = match target_lsn {
        Some(lsn) => manager.latest_snapshot_at_or_before(lsn).await?,
        None => manager.latest_snapshot().await?,
    };

    let Some((snapshot_lsn, path)) = selected else {
        return Ok((MaterializedState::empty(), 0));
    };

    let snapshot = deserialize_backup_snapshot(&path).await?;
    if snapshot.lsn != snapshot_lsn {
        return Err(RepoError::Deserialization);
    }

    let mut nodes = HashMap::new();
    let mut hyper_index = HyperIndex::new();
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

fn collect_backup_edges(index: &HyperIndex) -> Vec<BackupEdgeRecord> {
    let mut edges = Vec::new();
    for source in index.graph_index.node_ids() {
        for (target, relation, weight) in index.graph_index.neighbors(source) {
            edges.push(BackupEdgeRecord {
                source,
                target: *target,
                relation: relation.clone(),
                weight: *weight,
            });
        }
    }

    edges.sort_by(|a, b| {
        a.source
            .cmp(&b.source)
            .then(a.target.cmp(&b.target))
            .then(a.relation.cmp(&b.relation))
    });
    edges
}

fn apply_replayed_entry(
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
            idem_map.insert(key.clone(), node_ids.clone());
        }
        WalEntry::Transaction(operations) => {
            for operation in operations {
                apply_replayed_tx_operation(operation, node_map, h_index, edge_meta);
            }
        }
    }
}

fn apply_replayed_tx_operation(
    operation: &TxOperation,
    node_map: &mut HashMap<u64, Node>,
    h_index: &mut HyperIndex,
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
    }
}

fn parse_wal_snapshot_lsn(snapshot_id: &str) -> Option<u64> {
    snapshot_id.strip_prefix("wal-lsn-")?.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_repo_put_get() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        let repo = Repository::open(&wal_path).await.unwrap();

        let node = Node::new(1, vec![0.1, 0.2, 0.3], "Test Node".to_string());
        repo.put_node(node.clone()).await.unwrap();

        let retrieved = repo.get_node(1).await.unwrap();
        assert_eq!(retrieved, node);
    }

    #[tokio::test]
    async fn test_repo_replay_on_restart() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("replay.wal");

        // 1. Create and populate
        {
            let repo = Repository::open(&wal_path).await.unwrap();
            repo.put_node(Node::new(1, vec![1.0], "Node 1".to_string()))
                .await
                .unwrap();
            repo.put_node(Node::new(2, vec![2.0], "Node 2".to_string()))
                .await
                .unwrap();
        }

        // 2. Reopen and verify replay
        {
            let repo = Repository::open(&wal_path).await.unwrap();
            assert_eq!(repo.get_node(1).await.unwrap().data, "Node 1");
            assert_eq!(repo.get_node(2).await.unwrap().data, "Node 2");
        }
    }

    #[tokio::test]
    async fn test_repo_delete_tombstone() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("delete.wal");

        // 1. Create, put, delete
        {
            let repo = Repository::open(&wal_path).await.unwrap();
            repo.put_node(Node::new(1, vec![1.0], "Node 1".to_string()))
                .await
                .unwrap();
            repo.delete_node(1).await.unwrap();
        }

        // 2. Reopen and verify deleted node is gone
        {
            let repo = Repository::open(&wal_path).await.unwrap();
            assert!(repo.get_node(1).await.is_err());
        }
    }

    #[tokio::test]
    async fn test_repo_edge_and_index_restore() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("edge_index.wal");

        // 1. Create Data (Node + Edge)
        {
            let repo = Repository::open(&wal_path).await.unwrap();
            repo.put_node(Node::new(1, vec![1.0, 0.0], "N1".to_string()))
                .await
                .unwrap();
            repo.put_node(Node::new(2, vec![0.0, 1.0], "N2".to_string()))
                .await
                .unwrap();
            repo.put_edge(Edge::new(1, 2, "links", 1.0)).await.unwrap();

            // Verify in-memory index
            let index = repo.hyper_index.read().await;
            let neighbors = index.expand_graph(1, 1);
            assert_eq!(neighbors.len(), 1);
            assert_eq!(neighbors[0].0, 2);
        }

        // 2. Restart and Verify Restoration
        {
            let repo = Repository::open(&wal_path).await.unwrap();

            // Check HyperIndex restored
            let index = repo.hyper_index.read().await;

            // Validation 1: Vector Index
            let search_res = index.search_vector(&[1.0, 0.0], 1);
            assert_eq!(search_res.len(), 1);
            assert_eq!(search_res[0].0, 1);

            // Validation 2: Graph Index
            let neighbors = index.expand_graph(1, 1);
            assert_eq!(neighbors.len(), 1);
            assert_eq!(neighbors[0].0, 2);
        }
    }

    #[tokio::test]
    async fn test_index_transaction_commits_all_mutations() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("txn_commit.wal");
        let repo = Repository::open(&wal_path).await.unwrap();

        let n1 = Node::new(1, vec![1.0, 0.0], "N1".to_string());
        let n2 = Node::new(2, vec![0.0, 1.0], "N2".to_string());
        let e = Edge::new(1, 2, "links", 1.0);

        repo.apply_index_transaction(vec![
            IndexMutation::PutNode(n1),
            IndexMutation::PutNode(n2),
            IndexMutation::PutEdge(e),
        ])
        .await
        .unwrap();

        assert_eq!(repo.get_node(1).await.unwrap().data, "N1");
        assert_eq!(repo.get_node(2).await.unwrap().data, "N2");

        let index = repo.hyper_index.read().await;
        let neighbors = index.expand_graph(1, 1);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].0, 2);
    }

    #[tokio::test]
    async fn test_index_transaction_rollback_on_validation_error() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("txn_rollback.wal");
        let repo = Repository::open(&wal_path).await.unwrap();

        let n1 = Node::new(1, vec![1.0], "N1".to_string());
        let invalid_edge = Edge::new(1, 999, "links", 1.0);

        let result = repo
            .apply_index_transaction(vec![
                IndexMutation::PutNode(n1),
                IndexMutation::PutEdge(invalid_edge),
            ])
            .await;

        assert!(result.is_err());
        assert!(
            repo.get_node(1).await.is_err(),
            "node should not be partially committed"
        );

        let reopened = Repository::open(&wal_path).await.unwrap();
        assert!(
            reopened.get_node(1).await.is_err(),
            "node should not be recoverable after failed transaction"
        );
    }

    #[tokio::test]
    async fn test_index_transaction_persists_single_wal_record() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("txn_single_record.wal");
        let repo = Repository::open(&wal_path).await.unwrap();

        repo.apply_index_transaction(vec![
            IndexMutation::PutNode(Node::new(1, vec![1.0], "N1".to_string())),
            IndexMutation::PutNode(Node::new(2, vec![2.0], "N2".to_string())),
            IndexMutation::PutEdge(Edge::new(1, 2, "links", 1.0)),
        ])
        .await
        .unwrap();

        drop(repo);

        let mut wal = Wal::open(&wal_path).await.unwrap();
        let mut record_count = 0usize;
        let mut tx_mutation_count = 0usize;

        wal.replay(|_lsn, payload| {
            record_count += 1;
            let archived = rkyv::check_archived_root::<WalEntry>(&payload[..])
                .map_err(|_| WalError::CorruptEntry)?;
            let entry: WalEntry = archived.deserialize(&mut rkyv::Infallible).unwrap();

            match entry {
                WalEntry::Transaction(entries) => {
                    tx_mutation_count = entries.len();
                }
                _ => return Err(WalError::CorruptEntry),
            }

            Ok(())
        })
        .await
        .unwrap();

        assert_eq!(
            record_count, 1,
            "transaction should be written as one WAL record"
        );
        assert_eq!(tx_mutation_count, 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_concurrent_delete_and_put_edge_do_not_leave_dangling_edge() {
        use tokio::sync::Barrier;

        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("txn_concurrency.wal");
        let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

        repo.put_node(Node::new(1, vec![1.0], "N1".to_string()))
            .await
            .unwrap();

        for _ in 0..64 {
            if repo.get_node(2).await.is_err() {
                repo.put_node(Node::new(2, vec![2.0], "N2".to_string()))
                    .await
                    .unwrap();
            }

            let barrier = Arc::new(Barrier::new(3));

            let repo_put = repo.clone();
            let barrier_put = barrier.clone();
            let put_task = tokio::spawn(async move {
                barrier_put.wait().await;
                let _ = repo_put.put_edge(Edge::new(1, 2, "links", 1.0)).await;
            });

            let repo_delete = repo.clone();
            let barrier_delete = barrier.clone();
            let delete_task = tokio::spawn(async move {
                barrier_delete.wait().await;
                let _ = repo_delete.delete_node(2).await;
            });

            barrier.wait().await;
            put_task.await.unwrap();
            delete_task.await.unwrap();

            assert!(
                repo.get_node(2).await.is_err(),
                "node 2 should be deleted after concurrent operations"
            );

            let index = repo.hyper_index.read().await;
            let neighbors = index.expand_graph(1, 1);
            assert!(
                !neighbors.iter().any(|(id, _)| *id == 2),
                "dangling edge to deleted node must not remain"
            );
        }
    }

    #[tokio::test]
    async fn test_load_snapshot_view_reconstructs_historical_state() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("snapshot_view.wal");
        let repo = Repository::open(&wal_path).await.unwrap();

        repo.put_node(Node::new(1, vec![1.0], "N1".to_string()))
            .await
            .unwrap();
        let snapshot_lsn_1 = repo.current_snapshot_id().await;

        repo.put_node(Node::new(2, vec![2.0], "N2".to_string()))
            .await
            .unwrap();
        repo.delete_node(1).await.unwrap();

        let view_at_lsn_1 = repo.load_snapshot_view(&snapshot_lsn_1).await.unwrap();
        assert_eq!(view_at_lsn_1.snapshot_id(), snapshot_lsn_1);
        assert_eq!(view_at_lsn_1.list_node_ids(), vec![1]);
        assert_eq!(view_at_lsn_1.get_nodes_by_ids(&[1])[0].data, "N1");

        let view_at_lsn_3 = repo.load_snapshot_view("wal-lsn-3").await.unwrap();
        assert_eq!(view_at_lsn_3.list_node_ids(), vec![2]);
    }

    #[tokio::test]
    async fn test_load_snapshot_view_rejects_missing_or_invalid_snapshot_id() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("snapshot_view_errors.wal");
        let repo = Repository::open(&wal_path).await.unwrap();

        repo.put_node(Node::new(1, vec![1.0], "N1".to_string()))
            .await
            .unwrap();

        let missing = repo.load_snapshot_view("wal-lsn-99").await;
        assert!(matches!(missing, Err(RepoError::SnapshotNotFound(_))));

        let invalid = repo.load_snapshot_view("snap-custom").await;
        assert!(matches!(invalid, Err(RepoError::InvalidSnapshotId(_))));
    }

    #[tokio::test]
    async fn test_open_with_snapshots_restores_snapshot_and_wal_delta() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("snapshot_restore.wal");
        let snapshot_dir = dir.path().join("snapshots");

        {
            let repo = Repository::open_with_snapshots(&wal_path, &snapshot_dir)
                .await
                .unwrap();
            repo.put_node(Node::new(1, vec![1.0], "N1".to_string()))
                .await
                .unwrap();
            repo.put_node(Node::new(2, vec![2.0], "N2".to_string()))
                .await
                .unwrap();

            let backup_id = repo.create_backup_snapshot().await.unwrap();
            assert_eq!(backup_id, "wal-lsn-2");

            repo.put_node(Node::new(3, vec![3.0], "N3".to_string()))
                .await
                .unwrap();
        }

        let reopened = Repository::open_with_snapshots(&wal_path, &snapshot_dir)
            .await
            .unwrap();
        assert_eq!(reopened.list_node_ids().await, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_restore_from_latest_backup_rebuilds_in_memory_state() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("restore_latest_from_backup.wal");
        let snapshot_dir = dir.path().join("snapshots");

        let repo = Repository::open_with_snapshots(&wal_path, &snapshot_dir)
            .await
            .unwrap();
        repo.put_node(Node::new(1, vec![1.0], "N1".to_string()))
            .await
            .unwrap();
        repo.put_node(Node::new(2, vec![2.0], "N2".to_string()))
            .await
            .unwrap();
        repo.create_backup_snapshot().await.unwrap();
        repo.delete_node(2).await.unwrap();

        // Simulate transient in-memory corruption and verify restore recovers from durable state.
        repo.nodes.write().await.clear();
        *repo.hyper_index.write().await = HyperIndex::new();
        repo.idempotency_index.write().await.clear();
        repo.edge_metadata.write().await.clear();

        assert!(repo.list_node_ids().await.is_empty());

        let restored_snapshot = repo.restore_from_latest_backup().await.unwrap();
        assert_eq!(restored_snapshot, "wal-lsn-3");
        assert_eq!(repo.list_node_ids().await, vec![1]);
    }

    #[tokio::test]
    async fn test_backup_requires_snapshot_manager_configuration() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("no_snapshot_manager.wal");
        let repo = Repository::open(&wal_path).await.unwrap();

        let create_backup = repo.create_backup_snapshot().await;
        assert!(matches!(
            create_backup,
            Err(RepoError::SnapshotNotConfigured)
        ));

        let restore = repo.restore_from_latest_backup().await;
        assert!(matches!(restore, Err(RepoError::SnapshotNotConfigured)));
    }

    #[tokio::test]
    async fn test_open_with_snapshots_rejects_snapshot_newer_than_wal() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("snapshot_newer_than_wal.wal");
        let snapshot_dir = dir.path().join("snapshots");

        {
            let repo = Repository::open_with_snapshots(&wal_path, &snapshot_dir)
                .await
                .unwrap();
            repo.put_node(Node::new(1, vec![1.0], "N1".to_string()))
                .await
                .unwrap();
            repo.put_node(Node::new(2, vec![2.0], "N2".to_string()))
                .await
                .unwrap();
            let snapshot_id = repo.create_backup_snapshot().await.unwrap();
            assert_eq!(snapshot_id, "wal-lsn-2");
        }

        tokio::fs::write(&wal_path, &[]).await.unwrap();

        let reopened = Repository::open_with_snapshots(&wal_path, &snapshot_dir).await;
        assert!(matches!(
            reopened,
            Err(RepoError::SnapshotNotFound(ref snapshot_id)) if snapshot_id == "wal-lsn-2"
        ));
    }
}
