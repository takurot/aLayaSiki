mod backup;
mod replay;
mod search;
mod transaction;

use crate::crypto::{AtRestCipher, NoOpCipher};
use crate::hyper_index::HyperIndex;
use crate::index::AdjacencyGraph;
use crate::session::{SessionGraph, SessionManager, SessionOwner};
use crate::snapshot::{SnapshotCatalog, SnapshotCatalogEntry, SnapshotError, SnapshotManager};
use crate::tiering::{StorageCapabilities, StorageProfile};
use crate::wal::{Wal, WalError, WalOptions};
use alayasiki_core::error::{AlayasikiError, ErrorCode};
use alayasiki_core::model::{Edge, Node};
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
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
    #[error("Session access denied: {0}")]
    SessionAccessDenied(String),
}

impl AlayasikiError for RepoError {
    fn error_code(&self) -> ErrorCode {
        match self {
            RepoError::Wal(err) => err.error_code(),
            RepoError::Serialization => ErrorCode::Internal,
            RepoError::Deserialization => ErrorCode::Internal,
            RepoError::NotFound => ErrorCode::NotFound,
            RepoError::InvalidTransaction(_) => ErrorCode::InvalidArgument,
            RepoError::InvalidSnapshotId(_) => ErrorCode::InvalidArgument,
            RepoError::SnapshotNotFound(_) => ErrorCode::NotFound,
            RepoError::SnapshotNotConfigured => ErrorCode::Internal,
            RepoError::Snapshot(err) => err.error_code(),
            RepoError::SessionAccessDenied(_) => ErrorCode::PermissionDenied,
        }
    }
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
    RecordIdempotency { key: String, node_ids: Vec<u64> },
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

pub struct SnapshotView {
    snapshot_id: String,
    nodes: HashMap<u64, Node>,
    hyper_index: HyperIndex,
    edge_metadata: HashMap<EdgeMetaKey, HashMap<String, String>>,
}

pub struct Repository {
    wal: Arc<Mutex<Wal>>,
    tx_lock: Arc<Mutex<()>>,
    nodes: Arc<RwLock<HashMap<u64, Node>>>,
    pub hyper_index: Arc<RwLock<HyperIndex>>,
    idempotency_index: Arc<RwLock<HashMap<String, Vec<u64>>>>,
    edge_metadata: Arc<RwLock<HashMap<EdgeMetaKey, HashMap<String, String>>>>,
    snapshot_manager: Option<SnapshotManager>,
    snapshot_catalog: Arc<Mutex<SnapshotCatalog>>,
    pub session_manager: Arc<SessionManager>,
    storage_profile: StorageProfile,
    storage_capabilities: StorageCapabilities,
}

const DEFAULT_SESSION_TTL: Duration = Duration::from_secs(30 * 60);

impl Repository {
    /// Create a new empty Repository (no replay)
    pub fn new(wal: Arc<Mutex<Wal>>) -> Self {
        Self::new_with_profile(wal, StorageProfile::default())
    }

    pub fn new_with_profile(wal: Arc<Mutex<Wal>>, storage_profile: StorageProfile) -> Self {
        let storage_capabilities = storage_profile.resolve_capabilities();

        Self {
            wal,
            tx_lock: Arc::new(Mutex::new(())),
            nodes: Arc::new(RwLock::new(HashMap::new())),
            hyper_index: Arc::new(RwLock::new(HyperIndex::with_storage_profile(
                storage_profile.clone(),
            ))),
            idempotency_index: Arc::new(RwLock::new(HashMap::new())),
            edge_metadata: Arc::new(RwLock::new(HashMap::new())),
            snapshot_manager: None,
            snapshot_catalog: Arc::new(Mutex::new(SnapshotCatalog::new_in_memory())),
            session_manager: Arc::new(SessionManager::new(DEFAULT_SESSION_TTL)),
            storage_profile,
            storage_capabilities,
        }
    }

    /// Open a Repository with WAL replay to restore previous state
    pub async fn open(wal_path: impl AsRef<Path>) -> Result<Self, RepoError> {
        Self::open_with_profile_and_options(
            wal_path,
            StorageProfile::default(),
            WalOptions::default(),
        )
        .await
    }

    /// Open a repository with custom WAL recovery and flush options.
    pub async fn open_with_options(
        wal_path: impl AsRef<Path>,
        wal_options: WalOptions,
    ) -> Result<Self, RepoError> {
        Self::open_with_profile_and_options(wal_path, StorageProfile::default(), wal_options).await
    }

    pub async fn open_with_profile(
        wal_path: impl AsRef<Path>,
        storage_profile: StorageProfile,
    ) -> Result<Self, RepoError> {
        Self::open_with_profile_and_options(wal_path, storage_profile, WalOptions::default()).await
    }

    pub async fn open_with_profile_and_options(
        wal_path: impl AsRef<Path>,
        storage_profile: StorageProfile,
        wal_options: WalOptions,
    ) -> Result<Self, RepoError> {
        Self::open_internal(
            wal_path.as_ref().to_path_buf(),
            Arc::new(NoOpCipher),
            None,
            wal_options,
            storage_profile,
        )
        .await
    }

    /// Open a repository with a custom at-rest cipher for WAL replay and writes.
    pub async fn open_with_cipher(
        wal_path: impl AsRef<Path>,
        cipher: Arc<dyn AtRestCipher>,
    ) -> Result<Self, RepoError> {
        Self::open_with_cipher_and_options(wal_path, cipher, WalOptions::default()).await
    }

    /// Open a repository with custom at-rest cipher and WAL options.
    pub async fn open_with_cipher_and_options(
        wal_path: impl AsRef<Path>,
        cipher: Arc<dyn AtRestCipher>,
        wal_options: WalOptions,
    ) -> Result<Self, RepoError> {
        Self::open_internal(
            wal_path.as_ref().to_path_buf(),
            cipher,
            None,
            wal_options,
            StorageProfile::default(),
        )
        .await
    }

    /// Open a repository and restore state from backup snapshots first, then WAL deltas.
    pub async fn open_with_snapshots(
        wal_path: impl AsRef<Path>,
        snapshot_dir: impl AsRef<Path>,
    ) -> Result<Self, RepoError> {
        Self::open_with_cipher_and_snapshots_and_options(
            wal_path,
            Arc::new(NoOpCipher),
            snapshot_dir,
            WalOptions::default(),
        )
        .await
    }

    /// Open a repository with custom cipher and snapshot-backed recovery.
    pub async fn open_with_cipher_and_snapshots(
        wal_path: impl AsRef<Path>,
        cipher: Arc<dyn AtRestCipher>,
        snapshot_dir: impl AsRef<Path>,
    ) -> Result<Self, RepoError> {
        Self::open_with_cipher_and_snapshots_and_options(
            wal_path,
            cipher,
            snapshot_dir,
            WalOptions::default(),
        )
        .await
    }

    /// Open a repository with custom cipher, snapshot-backed recovery, and WAL options.
    pub async fn open_with_cipher_and_snapshots_and_options(
        wal_path: impl AsRef<Path>,
        cipher: Arc<dyn AtRestCipher>,
        snapshot_dir: impl AsRef<Path>,
        wal_options: WalOptions,
    ) -> Result<Self, RepoError> {
        let snapshot_manager = SnapshotManager::new(snapshot_dir.as_ref());
        Self::open_internal(
            wal_path.as_ref().to_path_buf(),
            cipher,
            Some(snapshot_manager),
            wal_options,
            StorageProfile::default(),
        )
        .await
    }

    async fn open_internal(
        wal_path: PathBuf,
        cipher: Arc<dyn AtRestCipher>,
        snapshot_manager: Option<SnapshotManager>,
        wal_options: WalOptions,
        storage_profile: StorageProfile,
    ) -> Result<Self, RepoError> {
        let wal_instance =
            Wal::open_with_cipher_and_options(&wal_path, cipher, wal_options).await?;
        let wal = Arc::new(Mutex::new(wal_instance));
        let tx_lock = Arc::new(Mutex::new(()));
        let (mut materialized, base_lsn) = replay::load_materialized_state_from_backup(
            snapshot_manager.as_ref(),
            None,
            storage_profile.clone(),
        )
        .await?;

        // Replay WAL entries newer than the snapshot baseline.
        {
            let mut wal_lock = wal.lock().await;
            let last_replayed_lsn = wal_lock
                .replay(|lsn, data| {
                    if lsn <= base_lsn {
                        return Ok(());
                    }
                    let archived = rkyv::check_archived_root::<WalEntry>(&data[..])
                        .map_err(|_| WalError::CorruptEntry)?;
                    let entry: WalEntry = archived
                        .deserialize(&mut rkyv::Infallible)
                        .expect("infallible deserializer");
                    replay::apply_replayed_entry(
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

        let mut snapshot_catalog = SnapshotCatalog::open(snapshot_catalog_path(&wal_path)).await?;
        let durable_lsn = {
            let wal_lock = wal.lock().await;
            wal_lock.durable_lsn()
        };
        snapshot_catalog.truncate_after_lsn(durable_lsn).await?;
        snapshot_catalog
            .record_snapshot(durable_lsn, current_unix_timestamp_ms())
            .await?;

        let storage_capabilities = storage_profile.resolve_capabilities();

        Ok(Self {
            wal,
            tx_lock,
            nodes: Arc::new(RwLock::new(materialized.nodes)),
            hyper_index: Arc::new(RwLock::new(materialized.hyper_index)),
            idempotency_index: Arc::new(RwLock::new(materialized.idempotency_index)),
            edge_metadata: Arc::new(RwLock::new(materialized.edge_metadata)),
            snapshot_manager,
            snapshot_catalog: Arc::new(Mutex::new(snapshot_catalog)),
            session_manager: Arc::new(SessionManager::new(DEFAULT_SESSION_TTL)),
            storage_profile,
            storage_capabilities,
        })
    }

    pub fn storage_profile(&self) -> &StorageProfile {
        &self.storage_profile
    }

    pub fn storage_capabilities(&self) -> &StorageCapabilities {
        &self.storage_capabilities
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

    pub async fn graph_index(&self) -> AdjacencyGraph {
        let index = self.hyper_index.read().await;
        index.graph_index.clone()
    }

    pub async fn delete_node(&self, id: u64) -> Result<(), RepoError> {
        self.apply_index_transaction(vec![IndexMutation::DeleteNode(id)])
            .await
    }

    pub async fn get_node_with_session(
        &self,
        id: u64,
        session_id: Option<&str>,
    ) -> Result<Node, RepoError> {
        if let Some(sid) = session_id {
            if let Some(session) = self.session_manager.get(sid) {
                if let Some(node) = session.nodes.get(&id) {
                    return Ok(node.clone());
                }
            }
        }
        self.get_node(id).await
    }

    pub fn get_session_with_owner(
        &self,
        session_id: &str,
        owner: Option<&SessionOwner>,
    ) -> Result<Option<SessionGraph>, RepoError> {
        let Some(session) = self.session_manager.get(session_id) else {
            return Ok(None);
        };

        if let Some(owner) = owner {
            match session.owner.as_ref() {
                Some(existing) if existing == owner => {}
                _ => {
                    return Err(RepoError::SessionAccessDenied(session_id.to_string()));
                }
            }
        }

        Ok(Some(session.clone()))
    }

    pub async fn search_vector_with_session_graph(
        &self,
        query: &[f32],
        k: usize,
        session: Option<&SessionGraph>,
    ) -> Vec<(u64, f32)> {
        let mut results = {
            let index = self.hyper_index.read().await;
            index.search_vector(query, k)
        };

        if let Some(session) = session {
            use alayasiki_core::embedding::cosine_similarity;
            let mut session_results: Vec<(u64, f32)> = session
                .nodes
                .values()
                .filter_map(|node| {
                    cosine_similarity(query, &node.embedding).map(|sim| (node.id, sim))
                })
                .collect();

            results.append(&mut session_results);
            results.sort_by(|a, b| {
                a.0.cmp(&b.0)
                    .then_with(|| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal))
            });
            results.dedup_by_key(|(id, _)| *id);
            results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            results.truncate(k);
        }
        results
    }

    pub async fn search_vector_with_session(
        &self,
        query: &[f32],
        k: usize,
        session_id: Option<&str>,
    ) -> Vec<(u64, f32)> {
        let session = session_id.and_then(|sid| self.session_manager.get(sid).map(|s| s.clone()));
        self.search_vector_with_session_graph(query, k, session.as_ref())
            .await
    }

    pub async fn neighbors_with_session_graph(
        &self,
        node_id: u64,
        session: Option<&SessionGraph>,
    ) -> Vec<(u64, String, f32)> {
        let mut results: Vec<(u64, String, f32)> = {
            let index = self.hyper_index.read().await;
            index
                .graph_index
                .neighbors(node_id)
                .into_iter()
                .cloned()
                .collect()
        };
        if let Some(session) = session {
            for edge in &session.edges {
                if edge.source == node_id {
                    results.push((edge.target, edge.relation.clone(), edge.weight));
                }
            }
        }
        results
    }

    pub async fn neighbors_with_session(
        &self,
        node_id: u64,
        session_id: Option<&str>,
    ) -> Vec<(u64, String, f32)> {
        let session = session_id.and_then(|sid| self.session_manager.get(sid).map(|s| s.clone()));
        self.neighbors_with_session_graph(node_id, session.as_ref())
            .await
    }

    pub fn ingest_to_session(&self, session_id: &str, node: Node) {
        let mut session = self.session_manager.get_or_create(session_id);
        session.insert_node(node);
    }

    pub fn ingest_to_session_with_owner(
        &self,
        session_id: &str,
        owner: &SessionOwner,
        node: Node,
    ) -> Result<(), RepoError> {
        let mut session = self.session_manager.get_or_create(session_id);
        match session.owner.as_ref() {
            Some(existing) if existing == owner => {}
            Some(_) => {
                return Err(RepoError::SessionAccessDenied(session_id.to_string()));
            }
            None => {
                session.owner = Some(owner.clone());
            }
        }
        session.insert_node(node);
        Ok(())
    }

    pub fn insert_edge_to_session(&self, session_id: &str, edge: Edge) {
        let mut session = self.session_manager.get_or_create(session_id);
        session.insert_edge(edge);
    }

    pub fn insert_edge_to_session_with_owner(
        &self,
        session_id: &str,
        owner: &SessionOwner,
        edge: Edge,
    ) -> Result<(), RepoError> {
        let mut session = self.session_manager.get_or_create(session_id);
        match session.owner.as_ref() {
            Some(existing) if existing == owner => {}
            Some(_) => {
                return Err(RepoError::SessionAccessDenied(session_id.to_string()));
            }
            None => {
                session.owner = Some(owner.clone());
            }
        }
        session.insert_edge(edge);
        Ok(())
    }

    /// Promote all nodes and edges in a session to persistent storage.
    /// This is an atomic operation within a single WAL transaction.
    pub async fn promote_session_to_persistent(&self, session_id: &str) -> Result<(), RepoError> {
        let session = self
            .session_manager
            .take(session_id)
            .ok_or(RepoError::NotFound)?;
        let mut session_to_restore = Some(session.clone());

        let nodes = session.nodes.values().cloned().collect::<Vec<_>>();
        let edges = session.edges.clone();

        let mut mutations = Vec::with_capacity(nodes.len() + edges.len());
        for node in nodes {
            mutations.push(IndexMutation::PutNode(node));
        }
        for edge in edges {
            mutations.push(IndexMutation::PutEdge(edge));
        }

        if let Err(err) = self.apply_index_transaction(mutations).await {
            if let Some(session) = session_to_restore.take() {
                self.session_manager.restore(session);
            }
            return Err(err);
        }

        Ok(())
    }

    /// Apply index updates atomically within one transaction boundary.
    /// If validation fails, nothing is written to WAL or in-memory indexes.
    pub async fn check_idempotency(&self, key: &str) -> Option<Vec<u64>> {
        let index = self.idempotency_index.read().await;
        index.get(key).cloned()
    }

    /// Force pending WAL entries to durable storage.
    ///
    /// Call this before graceful shutdown when using buffered flush policies.
    pub async fn flush(&self) -> Result<(), RepoError> {
        let _tx_guard = self.tx_lock.lock().await;
        let durable_lsn = {
            let mut wal = self.wal.lock().await;
            wal.flush().await?;
            wal.durable_lsn()
        };
        self.record_durable_snapshot(durable_lsn).await?;
        Ok(())
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

    /// Return the latest durable WAL snapshot id.
    pub async fn current_snapshot_id(&self) -> String {
        let wal = self.wal.lock().await;
        format!("wal-lsn-{}", wal.durable_lsn())
    }

    pub async fn resolve_snapshot_id_at_or_before(
        &self,
        as_of_unix_ms: i64,
    ) -> Result<String, RepoError> {
        let catalog = self.snapshot_catalog.lock().await;
        catalog
            .resolve_as_of(as_of_unix_ms)
            .map(|entry| entry.snapshot_id.clone())
            .ok_or_else(|| RepoError::SnapshotNotFound(format!("as-of-{as_of_unix_ms}")))
    }

    pub async fn snapshot_catalog_entries(&self) -> Vec<SnapshotCatalogEntry> {
        let catalog = self.snapshot_catalog.lock().await;
        catalog.entries().to_vec()
    }
}

pub fn parse_wal_snapshot_lsn(snapshot_id: &str) -> Option<u64> {
    snapshot_id.strip_prefix("wal-lsn-")?.parse::<u64>().ok()
}

fn snapshot_catalog_path(wal_path: &Path) -> PathBuf {
    wal_path.with_extension("snapshot_catalog.rkyv")
}

pub(crate) fn current_unix_timestamp_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
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

#[cfg(test)]
mod tests;
