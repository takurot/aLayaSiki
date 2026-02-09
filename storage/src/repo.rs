use crate::hyper_index::HyperIndex;
use crate::wal::{Wal, WalError};
use alayasiki_core::model::{Edge, Node};
use rkyv::ser::{serializers::AllocSerializer, Serializer};
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
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

pub struct Repository {
    wal: Arc<Mutex<Wal>>,
    tx_lock: Arc<Mutex<()>>,
    nodes: Arc<RwLock<HashMap<u64, Node>>>,
    pub hyper_index: Arc<RwLock<HyperIndex>>,
    idempotency_index: Arc<RwLock<HashMap<String, Vec<u64>>>>,
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
        }
    }

    /// Open a Repository with WAL replay to restore previous state
    pub async fn open(wal_path: impl AsRef<Path>) -> Result<Self, RepoError> {
        let wal_instance = Wal::open(&wal_path).await?;
        let wal = Arc::new(Mutex::new(wal_instance));
        let tx_lock = Arc::new(Mutex::new(()));
        let nodes = Arc::new(RwLock::new(HashMap::new()));
        let hyper_index = Arc::new(RwLock::new(HyperIndex::new()));
        let idempotency_index = Arc::new(RwLock::new(HashMap::new()));

        // 1. Replay WAL
        {
            let mut wal_lock = wal.lock().await;
            let mut node_map = nodes.write().await;
            let mut h_index = hyper_index.write().await;
            let mut idem_map = idempotency_index.write().await;

            wal_lock
                .replay(|_lsn, data| {
                    // Deserialize (zero-copy check)
                    let archived = rkyv::check_archived_root::<WalEntry>(&data[..])
                        .map_err(|_| WalError::CorruptEntry)?;
                    let entry: WalEntry = archived.deserialize(&mut rkyv::Infallible).unwrap();
                    apply_replayed_entry(&entry, &mut node_map, &mut h_index, &mut idem_map);
                    Ok(())
                })
                .await?;
        }

        Ok(Self {
            wal,
            tx_lock,
            nodes,
            hyper_index,
            idempotency_index,
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

        for mutation in mutations {
            match mutation {
                IndexMutation::PutNode(node) => {
                    let id = node.id;
                    let embedding = node.embedding.clone();
                    nodes.insert(id, node);
                    index.insert_node(id, embedding);
                }
                IndexMutation::PutEdge(edge) => {
                    index.insert_edge(edge.source, edge.target, edge.relation, edge.weight);
                }
                IndexMutation::DeleteNode(id) => {
                    nodes.remove(&id);
                    index.remove_node(id);
                }
            }
        }

        Ok(())
    }
    pub async fn check_idempotency(&self, key: &str) -> Option<Vec<u64>> {
        let index = self.idempotency_index.read().await;
        index.get(key).cloned()
    }

    pub async fn current_snapshot_id(&self) -> String {
        let wal = self.wal.lock().await;
        format!("wal-lsn-{}", wal.current_lsn())
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

fn apply_replayed_entry(
    entry: &WalEntry,
    node_map: &mut HashMap<u64, Node>,
    h_index: &mut HyperIndex,
    idem_map: &mut HashMap<String, Vec<u64>>,
) {
    match entry {
        WalEntry::Put(node) => {
            let id = node.id;
            let embedding = node.embedding.clone();
            node_map.insert(id, node.clone());
            h_index.insert_node(id, embedding);
        }
        WalEntry::PutEdge(edge) => {
            h_index.insert_edge(edge.source, edge.target, edge.relation.clone(), edge.weight);
        }
        WalEntry::Delete(id) => {
            node_map.remove(id);
            h_index.remove_node(*id);
        }
        WalEntry::IdempotencyKey { key, node_ids } => {
            idem_map.insert(key.clone(), node_ids.clone());
        }
        WalEntry::Transaction(operations) => {
            for operation in operations {
                apply_replayed_tx_operation(operation, node_map, h_index);
            }
        }
    }
}

fn apply_replayed_tx_operation(
    operation: &TxOperation,
    node_map: &mut HashMap<u64, Node>,
    h_index: &mut HyperIndex,
) {
    match operation {
        TxOperation::Put(node) => {
            let id = node.id;
            let embedding = node.embedding.clone();
            node_map.insert(id, node.clone());
            h_index.insert_node(id, embedding);
        }
        TxOperation::PutEdge(edge) => {
            h_index.insert_edge(edge.source, edge.target, edge.relation.clone(), edge.weight);
        }
        TxOperation::Delete(id) => {
            node_map.remove(id);
            h_index.remove_node(*id);
        }
    }
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
}
