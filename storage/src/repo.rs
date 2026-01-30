use std::collections::HashMap;
use std::path::Path;
use alayasiki_core::model::{Node, Edge};
use crate::wal::{Wal, WalError};
use crate::hyper_index::HyperIndex;
use thiserror::Error;
use rkyv::{Archive, Deserialize, Serialize};
use rkyv::ser::{serializers::AllocSerializer, Serializer};
use std::sync::Arc;
use tokio::sync::RwLock;

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
}

/// WAL Entry types for durability
#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
#[archive(check_bytes)]
pub enum WalEntry {
    Put(Node),
    PutEdge(Edge),
    Delete(u64),
    IdempotencyKey { key: String, node_ids: Vec<u64> },
}

pub struct Repository {
    wal: Arc<tokio::sync::Mutex<Wal>>,
    nodes: Arc<RwLock<HashMap<u64, Node>>>,
    pub hyper_index: Arc<RwLock<HyperIndex>>,
    idempotency_index: Arc<RwLock<HashMap<String, Vec<u64>>>>,
}

impl Repository {
    /// Create a new empty Repository (no replay)
    pub fn new(wal: Arc<tokio::sync::Mutex<Wal>>) -> Self {
        Self {
            wal,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            hyper_index: Arc::new(RwLock::new(HyperIndex::new())),
            idempotency_index: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Open a Repository with WAL replay to restore previous state
    pub async fn open(wal_path: impl AsRef<Path>) -> Result<Self, RepoError> {
        let wal_instance = Wal::open(&wal_path).await?;
        let wal = Arc::new(tokio::sync::Mutex::new(wal_instance));
        let nodes = Arc::new(RwLock::new(HashMap::new()));
        let hyper_index = Arc::new(RwLock::new(HyperIndex::new()));
        let idempotency_index = Arc::new(RwLock::new(HashMap::new()));
        
        // 1. Replay WAL
        {
            let mut wal_lock = wal.lock().await;
            let mut node_map = nodes.write().await;
            let mut h_index = hyper_index.write().await;
            let mut idem_map = idempotency_index.write().await;

            wal_lock.replay(|_lsn, data| {
                // Deserialize (zero-copy check)
                let entry = rkyv::check_archived_root::<WalEntry>(&data[..])
                    .map_err(|_| WalError::CorruptEntry)?;

                match entry {
                    ArchivedWalEntry::Put(node) => {
                        let deserialized: Node = node.deserialize(&mut rkyv::Infallible).unwrap();
                        let id = deserialized.id;
                        let embedding = deserialized.embedding.clone();
                        
                        node_map.insert(id, deserialized);
                        h_index.insert_node(id, embedding);
                    }
                    ArchivedWalEntry::Delete(id_archived) => {
                        let id: u64 = id_archived.deserialize(&mut rkyv::Infallible).unwrap();
                        node_map.remove(&id);
                        h_index.remove_node(id);
                    }
                    ArchivedWalEntry::PutEdge(edge) => {
                        let deserialized: Edge = edge.deserialize(&mut rkyv::Infallible).unwrap();
                        h_index.insert_edge(
                            deserialized.source,
                            deserialized.target,
                            deserialized.relation,
                            deserialized.weight
                        );
                    }
                    ArchivedWalEntry::IdempotencyKey { key, node_ids } => {
                        let key: String = key.deserialize(&mut rkyv::Infallible).unwrap();
                        let node_ids: Vec<u64> = node_ids.deserialize(&mut rkyv::Infallible).unwrap();
                        idem_map.insert(key, node_ids);
                    }
                }
                Ok(())
            }).await?;
        }

        Ok(Self {
            wal,
            nodes,
            hyper_index,
            idempotency_index,
        })
    }

    pub async fn put_node(&self, node: Node) -> Result<(), RepoError> {
        // 1. Create WalEntry
        let entry = WalEntry::Put(node.clone());
        let mut serializer = AllocSerializer::<256>::default();
        serializer.serialize_value(&entry).map_err(|_| RepoError::Serialization)?;
        let bytes = serializer.into_serializer().into_inner();
        
        // 2. Append to WAL (Durability First)
        {
            let mut wal = self.wal.lock().await;
            wal.append(&bytes).await?;
            wal.flush().await?;
        }

        // 3. Update In-Memory State
        {
            let mut nodes = self.nodes.write().await;
            nodes.insert(node.id, node.clone());
        }
        {
            let mut index = self.hyper_index.write().await;
            index.insert_node(node.id, node.embedding);
        }

        Ok(())
    }

    pub async fn put_edge(&self, edge: Edge) -> Result<(), RepoError> {
        // 1. Create WalEntry
        let entry = WalEntry::PutEdge(edge.clone());
        let mut serializer = AllocSerializer::<128>::default();
        serializer.serialize_value(&entry).map_err(|_| RepoError::Serialization)?;
        let bytes = serializer.into_serializer().into_inner();

        // 2. Append to WAL
        {
            let mut wal = self.wal.lock().await;
            wal.append(&bytes).await?;
            wal.flush().await?;
        }

        // 3. Update Index (Edges are currently only in HyperIndex)
        {
            let mut index = self.hyper_index.write().await;
            index.insert_edge(edge.source, edge.target, edge.relation, edge.weight);
        }

        Ok(())
    }

    pub async fn get_node(&self, id: u64) -> Result<Node, RepoError> {
        let nodes = self.nodes.read().await;
        nodes.get(&id).cloned().ok_or(RepoError::NotFound)
    }

    pub async fn delete_node(&self, id: u64) -> Result<(), RepoError> {
        // 1. Check existence first
        {
            let nodes = self.nodes.read().await;
            if !nodes.contains_key(&id) {
                return Err(RepoError::NotFound);
            }
        }

        // 2. Create WalEntry for tombstone
        let entry = WalEntry::Delete(id);
        let mut serializer = AllocSerializer::<64>::default();
        serializer.serialize_value(&entry).map_err(|_| RepoError::Serialization)?;
        let bytes = serializer.into_serializer().into_inner();

        // 3. Append to WAL
        {
            let mut wal = self.wal.lock().await;
            wal.append(&bytes).await?;
            wal.flush().await?;
        }

        // 4. Remove from memory
        {
            let mut nodes = self.nodes.write().await;
            nodes.remove(&id);
        }
        {
            let mut index = self.hyper_index.write().await;
            index.remove_node(id);
        }

        Ok(())
    }
    pub async fn check_idempotency(&self, key: &str) -> Option<Vec<u64>> {
        let index = self.idempotency_index.read().await;
        index.get(key).cloned()
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
                node_ids: node_ids.clone() 
            };
            // Increase buffer size for large ID lists
            let mut serializer = AllocSerializer::<4096>::default();
            serializer.serialize_value(&entry).map_err(|_| RepoError::Serialization)?;
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
            repo.put_node(Node::new(1, vec![1.0], "Node 1".to_string())).await.unwrap();
            repo.put_node(Node::new(2, vec![2.0], "Node 2".to_string())).await.unwrap();
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
            repo.put_node(Node::new(1, vec![1.0], "Node 1".to_string())).await.unwrap();
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
            repo.put_node(Node::new(1, vec![1.0, 0.0], "N1".to_string())).await.unwrap();
            repo.put_node(Node::new(2, vec![0.0, 1.0], "N2".to_string())).await.unwrap();
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
}
