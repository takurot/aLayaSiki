//! Durable, WAL-backed job queue with bounded retry and dead-letter handling.
//!
//! Reuses `storage::wal::Wal` for a dedicated jobs WAL (`<dir>/jobs.wal`) that is
//! logically separate from the graph mutation WAL. Jobs survive process crashes;
//! delivery is at-least-once and consumers must tolerate redelivery (the extraction
//! worker is idempotent: entity node ids are derived from `sha256(text)` and edges
//! are keyed by `(source, target, relation)`, so reprocessing overwrites safely).

use crate::queue::{Job, JobQueue};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use storage::wal::{Wal, WalFlushPolicy, WalOptions, WalRecoveryMode};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};

const JOB_WAL_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum JobQueueError {
    #[error("WAL error: {0}")]
    Wal(#[from] storage::wal::WalError),
    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Unsupported job WAL schema version: expected {expected}, found {found}")]
    SchemaVersion { expected: u32, found: u32 },
}

/// A job envelope carrying the durable metadata required for retry, audit, and
/// recovery alongside the [`Job`] payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobEnvelope {
    pub id: u64,
    pub attempt: u32,
    pub enqueued_at_ms: i64,
    pub job: Job,
}

/// A job that exceeded the configured retry budget and was moved to the dead-letter
/// table for operator inspection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterEntry {
    pub id: u64,
    pub reason: String,
    pub attempts: u32,
    pub envelope: JobEnvelope,
    pub dead_lettered_at_ms: i64,
}

/// Snapshot of queue counters used for observability and SLO reporting.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JobQueueStats {
    pub enqueued: u64,
    pub completed: u64,
    pub retried: u64,
    pub dead_lettered: u64,
    pub pending_depth: usize,
}

/// Configuration for [`DurableJobQueue`].
#[derive(Debug, Clone)]
pub struct DurableQueueConfig {
    /// Maximum delivery attempts before a job is dead-lettered.
    pub max_attempts: u32,
    /// Base backoff for the n-th retry (`base * 2^(attempt-1)`). Zero disables delay.
    pub base_backoff: Duration,
    /// Bounded capacity of the in-memory announcement channel.
    pub channel_capacity: usize,
    /// Maximum number of dead-letter entries retained in memory (FIFO eviction).
    pub max_dead_letters: usize,
    /// WAL recovery mode on reopen. Defaults to fail-safe truncation.
    pub recovery_mode: WalRecoveryMode,
}

impl Default for DurableQueueConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_backoff: Duration::from_millis(100),
            channel_capacity: 256,
            max_dead_letters: 1024,
            recovery_mode: WalRecoveryMode::RecoverToLastGoodOffset,
        }
    }
}

/// Versioned record appended to the jobs WAL.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct JobWalRecord {
    v: u32,
    op: JobWalOp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum JobWalOp {
    Enqueue(JobEnvelope),
    Complete {
        id: u64,
    },
    DeadLetter {
        id: u64,
        reason: String,
        envelope: JobEnvelope,
    },
}

struct QueueState {
    pending: BTreeMap<u64, JobEnvelope>,
    dead_letters: VecDeque<DeadLetterEntry>,
    stats: JobQueueStats,
}

/// A job queue that persists every enqueue/complete/dead-letter operation to a
/// dedicated WAL before acknowledging, so pending work survives crashes and is
/// re-announced on reopen.
pub struct DurableJobQueue {
    wal: Arc<Mutex<Wal>>,
    state: Arc<Mutex<QueueState>>,
    sender: mpsc::Sender<JobEnvelope>,
    next_id: AtomicU64,
    config: DurableQueueConfig,
}

impl DurableJobQueue {
    /// Open (or create) a durable queue at `path` with default configuration.
    pub async fn open(
        path: impl AsRef<Path>,
    ) -> Result<(Self, mpsc::Receiver<JobEnvelope>), JobQueueError> {
        Self::open_with_config(path, DurableQueueConfig::default()).await
    }

    /// Open (or create) a durable queue at `path` with the supplied configuration.
    ///
    /// On reopen the WAL is replayed to rebuild the in-memory pending set and
    /// dead-letter table, counters are restored, and every still-pending job is
    /// re-announced on the returned receiver (in ascending id order).
    pub async fn open_with_config(
        path: impl AsRef<Path>,
        config: DurableQueueConfig,
    ) -> Result<(Self, mpsc::Receiver<JobEnvelope>), JobQueueError> {
        let wal_options = WalOptions {
            recovery_mode: config.recovery_mode,
            flush_policy: WalFlushPolicy::Always,
        };
        let mut wal = Wal::open_with_options(path, wal_options).await?;

        let mut records: Vec<JobWalRecord> = Vec::new();
        wal.replay(
            |_lsn, payload| match serde_json::from_slice::<JobWalRecord>(&payload) {
                Ok(record) => {
                    records.push(record);
                    Ok(())
                }
                Err(_) => Err(storage::wal::WalError::CorruptEntry),
            },
        )
        .await?;

        let mut state = QueueState {
            pending: BTreeMap::new(),
            dead_letters: VecDeque::new(),
            stats: JobQueueStats::default(),
        };

        let mut max_id: u64 = 0;
        let mut total_enqueue_ops: u64 = 0;
        let mut unique_enqueued: HashSet<u64> = HashSet::new();
        let mut completed: HashSet<u64> = HashSet::new();

        for record in &records {
            if record.v != JOB_WAL_SCHEMA_VERSION {
                return Err(JobQueueError::SchemaVersion {
                    expected: JOB_WAL_SCHEMA_VERSION,
                    found: record.v,
                });
            }
            match &record.op {
                JobWalOp::Enqueue(envelope) => {
                    total_enqueue_ops += 1;
                    unique_enqueued.insert(envelope.id);
                    max_id = max_id.max(envelope.id);
                    // Latest envelope wins so the restored attempt count reflects retries.
                    state.pending.insert(envelope.id, envelope.clone());
                }
                JobWalOp::Complete { id } => {
                    state.pending.remove(id);
                    completed.insert(*id);
                }
                JobWalOp::DeadLetter {
                    id,
                    reason,
                    envelope,
                } => {
                    state.pending.remove(id);
                    push_dead_letter(
                        &mut state,
                        DeadLetterEntry {
                            id: *id,
                            reason: reason.clone(),
                            attempts: envelope.attempt,
                            envelope: envelope.clone(),
                            dead_lettered_at_ms: now_unix_ms(),
                        },
                        config.max_dead_letters,
                    );
                }
            }
        }

        // Reconstruct counters from the replayed op stream. Retries are emitted as
        // additional `Enqueue` records for an already-seen id, so `retried` is the
        // surplus of enqueue ops over unique ids.
        state.stats.enqueued = unique_enqueued.len() as u64;
        state.stats.retried = total_enqueue_ops.saturating_sub(unique_enqueued.len() as u64);
        state.stats.completed = completed.len() as u64;
        state.stats.dead_lettered = state.dead_letters.len() as u64;
        state.stats.pending_depth = state.pending.len();

        let next_id = max_id + 1;

        let (sender, receiver) = mpsc::channel(config.channel_capacity.max(1));
        // Re-announce pending jobs. The receiver is not being drained yet (the worker
        // is spawned by the caller after this returns), so messages buffer in the
        // channel up to its capacity; overflow is logged and the job remains pending
        // for the next recovery cycle.
        for envelope in state.pending.values() {
            if let Err(err) = sender.try_send(envelope.clone()) {
                tracing::warn!(
                    job_id = envelope.id,
                    "pending job could not be buffered for re-announce on recovery: {err}"
                );
            }
        }

        let queue = Self {
            wal: Arc::new(Mutex::new(wal)),
            state: Arc::new(Mutex::new(state)),
            sender,
            next_id: AtomicU64::new(next_id),
            config,
        };

        Ok((queue, receiver))
    }

    /// Enqueue a job, returning the assigned id.
    pub async fn enqueue_tracked(&self, job: Job) -> Result<u64, JobQueueError> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let envelope = JobEnvelope {
            id,
            attempt: 0,
            enqueued_at_ms: now_unix_ms(),
            job,
        };
        self.apply_enqueue(&envelope).await?;
        self.announce(envelope);
        Ok(id)
    }

    /// Acknowledge successful processing of `id`. Idempotent: a no-op (with a trace
    /// log) if the job is no longer pending, which tolerates at-least-once redelivery.
    pub async fn complete(&self, id: u64) -> Result<(), JobQueueError> {
        let bytes = serde_json::to_vec(&JobWalRecord {
            v: JOB_WAL_SCHEMA_VERSION,
            op: JobWalOp::Complete { id },
        })?;
        {
            let mut wal = self.wal.lock().await;
            wal.append(&bytes).await?;
        }
        {
            let mut state = self.state.lock().await;
            if state.pending.remove(&id).is_some() {
                state.stats.completed += 1;
                state.stats.pending_depth = state.pending.len();
            } else {
                tracing::trace!(job_id = id, "complete: job not pending, no-op");
            }
        }
        Ok(())
    }

    /// Report processing failure for `id`. Retries (with exponential backoff) until
    /// `max_attempts` is reached, after which the job is dead-lettered. Idempotent
    /// when the job is no longer pending.
    pub async fn fail(&self, id: u64, reason: String) -> Result<(), JobQueueError> {
        let mut resend: Option<JobEnvelope> = None;
        {
            let mut state = self.state.lock().await;
            let Some(envelope) = state.pending.get(&id).cloned() else {
                tracing::trace!(job_id = id, "fail: job not pending, no-op");
                return Ok(());
            };
            let new_attempt = envelope.attempt + 1;

            if new_attempt >= self.config.max_attempts {
                let record = JobWalRecord {
                    v: JOB_WAL_SCHEMA_VERSION,
                    op: JobWalOp::DeadLetter {
                        id,
                        reason: reason.clone(),
                        envelope: envelope.clone(),
                    },
                };
                self.append_locked(&mut state, &record).await?;
                state.pending.remove(&id);
                push_dead_letter(
                    &mut state,
                    DeadLetterEntry {
                        id,
                        reason,
                        attempts: new_attempt,
                        envelope,
                        dead_lettered_at_ms: now_unix_ms(),
                    },
                    self.config.max_dead_letters,
                );
                state.stats.dead_lettered += 1;
                state.stats.pending_depth = state.pending.len();
            } else {
                let mut retried = envelope.clone();
                retried.attempt = new_attempt;
                let record = JobWalRecord {
                    v: JOB_WAL_SCHEMA_VERSION,
                    op: JobWalOp::Enqueue(retried.clone()),
                };
                self.append_locked(&mut state, &record).await?;
                state.pending.insert(id, retried.clone());
                state.stats.retried += 1;
                resend = Some(retried);
            }
        }

        if let Some(envelope) = resend {
            self.schedule_resend(envelope);
        }
        Ok(())
    }

    /// Current counters snapshot.
    pub async fn stats(&self) -> JobQueueStats {
        self.state.lock().await.stats.clone()
    }

    /// Snapshot of the in-memory dead-letter table (oldest first).
    pub async fn dead_letters(&self) -> Vec<DeadLetterEntry> {
        self.state
            .lock()
            .await
            .dead_letters
            .iter()
            .cloned()
            .collect()
    }

    /// Snapshot of currently pending envelopes (ordered by id).
    pub async fn pending(&self) -> Vec<JobEnvelope> {
        self.state.lock().await.pending.values().cloned().collect()
    }

    async fn apply_enqueue(&self, envelope: &JobEnvelope) -> Result<(), JobQueueError> {
        let record = JobWalRecord {
            v: JOB_WAL_SCHEMA_VERSION,
            op: JobWalOp::Enqueue(envelope.clone()),
        };
        let bytes = serde_json::to_vec(&record)?;
        {
            let mut wal = self.wal.lock().await;
            wal.append(&bytes).await?;
        }
        {
            let mut state = self.state.lock().await;
            state.pending.insert(envelope.id, envelope.clone());
            state.stats.enqueued += 1;
            state.stats.pending_depth = state.pending.len();
        }
        Ok(())
    }

    /// Append a WAL record while already holding the state lock. Lock order is
    /// `state -> wal`; no other path acquires `wal` first, so this cannot deadlock.
    async fn append_locked(
        &self,
        _state: &mut QueueState,
        record: &JobWalRecord,
    ) -> Result<(), JobQueueError> {
        let bytes = serde_json::to_vec(record)?;
        let mut wal = self.wal.lock().await;
        wal.append(&bytes).await?;
        Ok(())
    }

    fn announce(&self, envelope: JobEnvelope) {
        if let Err(err) = self.sender.try_send(envelope.clone()) {
            tracing::warn!(
                job_id = envelope.id,
                "job persisted but not delivered immediately (buffered or recovery will redeliver): {err}"
            );
        }
    }

    fn schedule_resend(&self, envelope: JobEnvelope) {
        let sender = self.sender.clone();
        let backoff = self
            .config
            .base_backoff
            .saturating_mul(2u32.saturating_pow(envelope.attempt.saturating_sub(1)));
        tokio::spawn(async move {
            if !backoff.is_zero() {
                tokio::time::sleep(backoff).await;
            }
            if let Err(err) = sender.try_send(envelope.clone()) {
                tracing::warn!(
                    job_id = envelope.id,
                    "retry resend could not be delivered: {err}"
                );
            }
        });
    }
}

#[async_trait::async_trait]
impl JobQueue for DurableJobQueue {
    async fn enqueue(&self, job: Job) -> anyhow::Result<()> {
        self.enqueue_tracked(job).await?;
        Ok(())
    }
}

fn push_dead_letter(state: &mut QueueState, entry: DeadLetterEntry, max: usize) {
    state.dead_letters.push_back(entry);
    while state.dead_letters.len() > max {
        state.dead_letters.pop_front();
    }
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn enqueue_then_complete_updates_stats() {
        let dir = tempfile::tempdir().unwrap();
        let (queue, mut rx) = DurableJobQueue::open(dir.path().join("jobs.wal"))
            .await
            .unwrap();

        let id = queue.enqueue_tracked(sample_job(1)).await.unwrap();
        let envelope = rx.recv().await.unwrap();
        assert_eq!(envelope.id, id);
        assert_eq!(envelope.attempt, 0);

        queue.complete(id).await.unwrap();

        let stats = queue.stats().await;
        assert_eq!(stats.enqueued, 1);
        assert_eq!(stats.completed, 1);
        assert_eq!(stats.pending_depth, 0);
        assert_eq!(stats.retried, 0);
        assert_eq!(stats.dead_lettered, 0);
        assert!(queue.dead_letters().await.is_empty());
    }

    #[tokio::test]
    async fn fail_retries_and_re_announces_with_bumped_attempt() {
        let config = DurableQueueConfig {
            max_attempts: 3,
            base_backoff: Duration::ZERO,
            ..DurableQueueConfig::default()
        };
        let dir = tempfile::tempdir().unwrap();
        let (queue, mut rx) =
            DurableJobQueue::open_with_config(dir.path().join("jobs.wal"), config)
                .await
                .unwrap();

        let id = queue.enqueue_tracked(sample_job(2)).await.unwrap();
        let first = rx.recv().await.unwrap();
        assert_eq!(first.attempt, 0);

        queue.fail(id, "boom".to_string()).await.unwrap();

        let retried = rx.recv().await.unwrap();
        assert_eq!(retried.id, id);
        assert_eq!(retried.attempt, 1);

        let stats = queue.stats().await;
        assert_eq!(stats.retried, 1);
        assert_eq!(stats.pending_depth, 1);
        assert_eq!(stats.dead_lettered, 0);
    }

    fn sample_job(node_id: u64) -> Job {
        Job::ExtractEntities {
            node_id,
            content: format!("content-{node_id}"),
            model_id: "legacy-default".to_string(),
            snapshot_id: "wal-lsn-0".to_string(),
        }
    }
}
