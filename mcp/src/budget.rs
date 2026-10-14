//! Conservative shared quotas for TTL-backed working memory.
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};
type Key = (String, String, String);
struct Usage {
    bytes: usize,
    writes: usize,
    expires: SystemTime,
    pending: usize,
}
pub struct SessionBudget {
    usage: Mutex<BTreeMap<Key, Usage>>,
    max_sessions: usize,
    max_writes: usize,
    max_bytes: usize,
}
impl SessionBudget {
    pub fn new(max_sessions: usize, max_writes: usize, max_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            usage: Mutex::new(BTreeMap::new()),
            max_sessions,
            max_writes,
            max_bytes,
        })
    }
    pub(crate) fn reserve(
        self: &Arc<Self>,
        repo: &storage::repo::Repository,
        key: Key,
        bytes: usize,
    ) -> Result<Reservation, &'static str> {
        let now = SystemTime::now();
        repo.cleanup_expired_sessions();
        let mut usage = self
            .usage
            .lock()
            .map_err(|_| "Working memory unavailable")?;
        usage.retain(|_, entry| entry.pending > 0 || entry.expires > now);
        let total_bytes: usize = usage.values().map(|entry| entry.bytes).sum();
        let total_writes: usize = usage.values().map(|entry| entry.writes).sum();
        if (!usage.contains_key(&key) && usage.len() >= self.max_sessions)
            || total_bytes.saturating_add(bytes) > self.max_bytes
            || total_writes >= self.max_writes
        {
            return Err("Working memory quota exceeded");
        }
        let entry = usage.entry(key.clone()).or_insert(Usage {
            bytes: 0,
            writes: 0,
            expires: now + Duration::from_secs(1800),
            pending: 0,
        });
        entry.bytes += bytes;
        entry.writes += 1;
        entry.pending += 1;
        Ok(Reservation {
            budget: self.clone(),
            key,
            bytes,
            committed: false,
        })
    }
}
impl Default for SessionBudget {
    fn default() -> Self {
        Self {
            usage: Mutex::new(BTreeMap::new()),
            max_sessions: 128,
            max_writes: 4096,
            max_bytes: 16 * 1024 * 1024,
        }
    }
}
pub(crate) struct Reservation {
    budget: Arc<SessionBudget>,
    key: Key,
    bytes: usize,
    committed: bool,
}
impl Reservation {
    pub(crate) fn commit(mut self, expires: SystemTime) -> Result<(), &'static str> {
        let mut usage = self
            .budget
            .usage
            .lock()
            .map_err(|_| "Working memory unavailable")?;
        if let Some(entry) = usage.get_mut(&self.key) {
            entry.expires = expires;
            entry.pending -= 1;
        }
        self.committed = true;
        Ok(())
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.committed {
            if let Ok(mut usage) = self.budget.usage.lock() {
                if let Some(entry) = usage.get_mut(&self.key) {
                    entry.bytes -= self.bytes;
                    entry.writes -= 1;
                    entry.pending -= 1;
                    if entry.writes == 0 {
                        usage.remove(&self.key);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn failed_reservations_release_and_expired_graphs_are_reclaimed() {
        let dir = tempfile::tempdir().unwrap();
        let repo = storage::repo::Repository::open(dir.path().join("quota.wal"))
            .await
            .unwrap();
        let budget = SessionBudget::new(1, 1, 64);
        let key = ("local".into(), "agent".into(), "old".into());
        {
            let _reservation = budget.reserve(&repo, key.clone(), 32).unwrap();
            assert!(budget.reserve(&repo, key.clone(), 1).is_err());
        }
        assert!(budget.usage.lock().unwrap().is_empty());
        let reservation = budget.reserve(&repo, key, 32).unwrap();
        {
            let mut graph = repo.session_manager.get_or_create("old");
            graph.expires_at = SystemTime::UNIX_EPOCH;
        }
        reservation.commit(SystemTime::UNIX_EPOCH).unwrap();
        assert_eq!(repo.session_manager.len(), 1);
        let reservation = budget
            .reserve(&repo, ("local".into(), "agent".into(), "new".into()), 32)
            .unwrap();
        assert_eq!(repo.session_manager.len(), 0);
        assert_eq!(budget.usage.lock().unwrap().len(), 1);
        drop(reservation);
        assert!(budget.usage.lock().unwrap().is_empty());
        assert!(budget
            .reserve(&repo, ("local".into(), "agent".into(), "big".into()), 65)
            .is_err());
    }
}
