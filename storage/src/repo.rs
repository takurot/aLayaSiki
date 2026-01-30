use std::collections::HashMap;
use std::path::Path;
use alayasiki_core::model::Node;
use crate::wal::{Wal, WalError};
use thiserror::Error;
use rkyv::{Archive, Deserialize, Serialize, to_bytes, from_bytes};
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
    Delete(u64),
}

pub struct Repository {
    wal: Arc<tokio::sync::Mutex<Wal>>,
    nodes: Arc<RwLock<HashMap<u64, Node>>>,
}

impl Repository {
    /// Create a new empty Repository (no replay)
    pub fn new(wal: Arc<tokio::sync::Mutex<Wal>>) -> Self {
        Self {
            wal,
            nodes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Open a Repository with WAL replay to restore previous state
    pub async fn open(wal_path: impl AsRef<Path>) -> Result<Self, RepoError> {
        let mut wal = Wal::open(&wal_path).await?;
        let mut nodes_map: HashMap<u64, Node> = HashMap::new();

        // Collect WAL entries first (sync callback)
        let mut entries: Vec<Vec<u8>> = Vec::new();
        wal.replay(|_lsn, payload| {
            entries.push(payload);
            Ok(())
        }).await?;

        // Apply entries to in-memory state
        for payload in entries {
            let entry: WalEntry = rkyv::from_bytes(&payload)
                .map_err(|_| RepoError::Deserialization)?;

            match entry {
                WalEntry::Put(node) => {
                    nodes_map.insert(node.id, node);
                }
                WalEntry::Delete(id) => {
                    nodes_map.remove(&id);
                }
            }
        }

        Ok(Self {
            wal: Arc::new(tokio::sync::Mutex::new(wal)),
            nodes: Arc::new(RwLock::new(nodes_map)),
        })
    }

    pub async fn put_node(&self, node: Node) -> Result<(), RepoError> {
        // 1. Create WalEntry
        let entry = WalEntry::Put(node.clone());
        let bytes = to_bytes::<_, 1024>(&entry).map_err(|_| RepoError::Serialization)?;
        
        // 2. Append to WAL (Durability First)
        {
            let mut wal = self.wal.lock().await;
            wal.append(&bytes).await?;
            wal.flush().await?;
        }

        // 3. Update In-Memory State
        {
            let mut nodes = self.nodes.write().await;
            nodes.insert(node.id, node);
        }

        Ok(())
    }

    pub async fn get_node(&self, id: u64) -> Result<Node, RepoError> {
        let nodes = self.nodes.read().await;
        nodes.get(&id).cloned().ok_or(RepoError::NotFound)
    }

    pub async fn delete_node(&self, id: u64) -> Result<(), RepoError> {
        // 1. Create WalEntry for tombstone
        let entry = WalEntry::Delete(id);
        let bytes = to_bytes::<_, 64>(&entry).map_err(|_| RepoError::Serialization)?;

        // 2. Append to WAL
        {
            let mut wal = self.wal.lock().await;
            wal.append(&bytes).await?;
            wal.flush().await?;
        }

        // 3. Remove from memory
        {
            let mut nodes = self.nodes.write().await;
            if nodes.remove(&id).is_none() {
                return Err(RepoError::NotFound);
            }
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
}
