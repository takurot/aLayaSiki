use std::collections::HashMap;
use alayasiki_core::model::{Node, Edge};
use crate::wal::{Wal, WalError};
use thiserror::Error;
use rkyv::{Archive, Deserialize, Serialize, to_bytes};
use std::sync::{Arc, Mutex};

#[derive(Error, Debug)]
pub enum RepoError {
    #[error("WAL error: {0}")]
    Wal(#[from] WalError),
    #[error("Serialization error")]
    Serialization,
    #[error("Not found")]
    NotFound,
}

pub struct Repository {
    wal: Arc<tokio::sync::Mutex<Wal>>,
    // Simple in-memory index for now (ID -> check existence)
    // In PR-04 this will be replaced by the Hyper-Index
    nodes: Arc<Mutex<HashMap<u64, Node>>>, 
}

impl Repository {
    pub fn new(wal: Arc<tokio::sync::Mutex<Wal>>) -> Self {
        Self {
            wal,
            nodes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn put_node(&self, node: Node) -> Result<(), RepoError> {
        // 1. Serialize
        let bytes = to_bytes::<_, 256>(&node).map_err(|_| RepoError::Serialization)?;
        
        // 2. Append to WAL (Durability First)
        {
            let mut wal = self.wal.lock().await;
            wal.append(&bytes).await?;
            // Note: In a real system, we might delay flush or flush here depending on consistency (Ack)
             wal.flush().await?; // Strong consistency for PR-03
        }

        // 3. Update In-Memory State
        {
            let mut nodes = self.nodes.lock().unwrap();
            nodes.insert(node.id, node);
        }

        Ok(())
    }

    pub async fn get_node(&self, id: u64) -> Result<Node, RepoError> {
        let nodes = self.nodes.lock().unwrap();
        nodes.get(&id).cloned().ok_or(RepoError::NotFound)
    }

    pub async fn delete_node(&self, id: u64) -> Result<(), RepoError> {
         // 1. Create Tombstone (simplified as special byte sequence or just ignoring for now in PR-03 simple CRUD)
         // For strictly implementation plan, we will just remove from memory and log emptiness or special validation
         // Ideally we should have a WalEntry enum. For PR-03, we focus on Put correctness.
         
         let mut nodes = self.nodes.lock().unwrap();
         if nodes.remove(&id).is_none() {
             return Err(RepoError::NotFound);
         }
         
         // In a real WAL, we append a Delete command. 
         // For this MVP, we just ensure in-memory consistency.
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
        
        // 1. Setup Repo
        let wal = Wal::open(&wal_path).await.unwrap();
        let repo = Repository::new(Arc::new(tokio::sync::Mutex::new(wal)));

        // 2. Create Node
        let node = Node::new(
            1, 
            vec![0.1, 0.2, 0.3], 
            "Test Node".to_string()
        );

        // 3. Put Node
        repo.put_node(node.clone()).await.unwrap();

        // 4. Get Node
        let retrieved = repo.get_node(1).await.unwrap();
        assert_eq!(retrieved, node);
    }
}
