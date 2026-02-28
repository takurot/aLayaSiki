use alayasiki_core::model::{Edge, Node};
use dashmap::DashMap;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

/// A session-scoped subgraph that exists only in memory.
#[derive(Debug, Clone)]
pub struct SessionGraph {
    pub session_id: String,
    pub nodes: HashMap<u64, Node>,
    pub edges: Vec<Edge>,
    pub expires_at: SystemTime,
}

impl SessionGraph {
    pub fn new(session_id: String, ttl: Duration) -> Self {
        Self {
            session_id,
            nodes: HashMap::new(),
            edges: Vec::new(),
            expires_at: SystemTime::now() + ttl,
        }
    }

    pub fn is_expired(&self) -> bool {
        SystemTime::now() > self.expires_at
    }

    pub fn insert_node(&mut self, node: Node) {
        self.nodes.insert(node.id, node);
    }

    pub fn insert_edge(&mut self, edge: Edge) {
        self.edges.push(edge);
    }

    pub fn clear(&mut self) {
        self.nodes.clear();
        self.edges.clear();
    }
}

/// Manager for multiple session graphs with TTL support.
pub struct SessionManager {
    sessions: DashMap<String, SessionGraph>,
    default_ttl: Duration,
}

impl SessionManager {
    pub fn new(default_ttl: Duration) -> Self {
        Self {
            sessions: DashMap::new(),
            default_ttl,
        }
    }

    /// Get or create a session.
    pub fn get_or_create(
        &self,
        session_id: &str,
    ) -> dashmap::mapref::one::RefMut<'_, String, SessionGraph> {
        self.sessions
            .entry(session_id.to_string())
            .or_insert_with(|| SessionGraph::new(session_id.to_string(), self.default_ttl))
    }

    /// Get a session if it exists and is not expired.
    pub fn get(
        &self,
        session_id: &str,
    ) -> Option<dashmap::mapref::one::Ref<'_, String, SessionGraph>> {
        let entry = self.sessions.get(session_id)?;
        if entry.is_expired() {
            drop(entry);
            self.sessions.remove(session_id);
            None
        } else {
            Some(entry)
        }
    }

    /// Remove a session manually.
    pub fn remove(&self, session_id: &str) {
        self.sessions.remove(session_id);
    }

    /// Cleanup all expired sessions.
    pub fn cleanup_expired(&self) {
        self.sessions.retain(|_, v| !v.is_expired());
    }

    /// Total count of active sessions.
    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_session_lifecycle() {
        let manager = SessionManager::new(Duration::from_millis(100));
        let session_id = "test-session";

        {
            let mut session = manager.get_or_create(session_id);
            session.insert_node(Node::new(1, vec![0.1], "test node".into()));
            assert_eq!(session.nodes.len(), 1);
        }

        assert!(manager.get(session_id).is_some());

        thread::sleep(Duration::from_millis(150));

        // Should be expired
        assert!(manager.get(session_id).is_none());
        assert_eq!(manager.len(), 0);
    }

    #[test]
    fn test_session_cleanup() {
        let manager = SessionManager::new(Duration::from_millis(100));
        manager.get_or_create("s1");
        manager.get_or_create("s2");
        assert_eq!(manager.len(), 2);

        thread::sleep(Duration::from_millis(150));
        manager.cleanup_expired();
        assert_eq!(manager.len(), 0);
    }
}
