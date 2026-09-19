use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditOperation {
    Ingest,
    Query,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditOutcome {
    Succeeded,
    Denied,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEvent {
    pub sequence: u64,
    pub operation: AuditOperation,
    pub outcome: AuditOutcome,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<String>,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl AuditEvent {
    pub fn new(operation: AuditOperation, outcome: AuditOutcome) -> Self {
        Self {
            sequence: 0,
            operation,
            outcome,
            actor: None,
            tenant: None,
            model_id: None,
            snapshot_id: None,
            metadata: HashMap::new(),
        }
    }
}

#[derive(Debug, Error)]
pub enum AuditError {
    #[error("audit sink lock poisoned")]
    LockPoisoned,
    #[error("audit io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("audit serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub trait AuditSink: Send + Sync {
    fn record(&self, event: AuditEvent) -> Result<(), AuditError>;
}

/// Governs what happens when a configured `AuditSink` fails to durably
/// record an event. This only applies when a sink is actually configured;
/// callers that intentionally run without a sink (e.g. anonymous/dev mode)
/// are unaffected and never consult this policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AuditFailurePolicy {
    /// Fail the operation rather than report success when the audit event
    /// could not be durably recorded. This is the default: configuring a
    /// sink is a signal that audit evidence is mandatory, so silent loss
    /// must not be allowed to masquerade as a successful operation.
    #[default]
    Strict,
    /// Allow the operation to continue despite an audit write failure. The
    /// failure is still surfaced via `AuditHealth` (logging + a degraded
    /// counter) so operators have a reliable signal that audit evidence is
    /// being lost.
    BestEffort,
}

/// Tracks audit sink health so that operators have a metric/health signal
/// when audit persistence degrades, independent of whether individual
/// operations are configured to fail closed or continue best-effort.
#[derive(Default)]
pub struct AuditHealth {
    failure_count: AtomicU64,
    last_error: Mutex<Option<String>>,
}

impl AuditHealth {
    /// Record an observed audit sink failure. Intentionally does not
    /// attempt to write to the audit sink itself, since a sink that just
    /// failed should not be retried recursively for its own failure.
    pub fn record_failure(&self, error: &AuditError) {
        self.failure_count.fetch_add(1, Ordering::SeqCst);
        if let Ok(mut last_error) = self.last_error.lock() {
            *last_error = Some(error.to_string());
        }
        tracing::error!(error = %error, "audit sink record failed");
    }

    pub fn failure_count(&self) -> u64 {
        self.failure_count.load(Ordering::SeqCst)
    }

    pub fn is_degraded(&self) -> bool {
        self.failure_count() > 0
    }

    pub fn last_error(&self) -> Option<String> {
        self.last_error.lock().ok().and_then(|guard| guard.clone())
    }
}

#[derive(Default)]
pub struct InMemoryAuditSink {
    events: Mutex<Vec<AuditEvent>>,
    sequence: AtomicU64,
}

impl InMemoryAuditSink {
    pub fn events(&self) -> Result<Vec<AuditEvent>, AuditError> {
        let events = self.events.lock().map_err(|_| AuditError::LockPoisoned)?;
        Ok(events.clone())
    }
}

impl AuditSink for InMemoryAuditSink {
    fn record(&self, mut event: AuditEvent) -> Result<(), AuditError> {
        // Sequence assignment happens while holding the events lock so that
        // the physical push order always matches assigned sequence order.
        let mut events = self.events.lock().map_err(|_| AuditError::LockPoisoned)?;
        let next = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
        event.sequence = next;
        events.push(event);
        Ok(())
    }
}

struct JsonlSinkState {
    file: std::fs::File,
    sequence: u64,
}

pub struct JsonlAuditSink {
    state: Mutex<JsonlSinkState>,
}

impl JsonlAuditSink {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, AuditError> {
        let path = path.as_ref();

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Open once in read+append mode: the same handle is used to derive
        // the starting sequence and for subsequent writes, so no other
        // process/thread can append between the read and the reopen (TOCTOU).
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let content = String::from_utf8_lossy(&bytes);

        // Recover the starting sequence from the max sequence observed among
        // parseable lines, rather than counting lines. This tolerates
        // corrupt/partial lines, non-UTF-8 content, and gaps left by prior
        // write failures without under- or over-counting.
        let starting_sequence = content
            .lines()
            .filter_map(|line| serde_json::from_str::<AuditEvent>(line).ok())
            .map(|event| event.sequence)
            .max()
            .unwrap_or(0);

        Ok(Self {
            state: Mutex::new(JsonlSinkState {
                file,
                sequence: starting_sequence,
            }),
        })
    }
}

impl AuditSink for JsonlAuditSink {
    fn record(&self, mut event: AuditEvent) -> Result<(), AuditError> {
        let mut state = self.state.lock().map_err(|_| AuditError::LockPoisoned)?;
        let next = state.sequence + 1;
        event.sequence = next;

        let line = serde_json::to_string(&event)?;
        state.file.write_all(line.as_bytes())?;
        state.file.write_all(b"\n")?;
        state.file.flush()?;

        // Only commit the sequence advance after a successful flush, so a
        // failed write does not irreversibly consume a sequence number.
        state.sequence = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn in_memory_sink_records_monotonic_sequence() {
        let sink = InMemoryAuditSink::default();

        sink.record(AuditEvent::new(
            AuditOperation::Ingest,
            AuditOutcome::Succeeded,
        ))
        .unwrap();
        sink.record(AuditEvent::new(
            AuditOperation::Query,
            AuditOutcome::Succeeded,
        ))
        .unwrap();

        let events = sink.events().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].sequence, 1);
        assert_eq!(events[1].sequence, 2);
    }

    #[test]
    fn jsonl_sink_writes_operation_and_model_id() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("audit.log");
        let sink = JsonlAuditSink::open(&path).unwrap();

        let mut event = AuditEvent::new(AuditOperation::Query, AuditOutcome::Succeeded);
        event.model_id = Some("embedding-default-v1".to_string());
        sink.record(event).unwrap();

        let content = std::fs::read_to_string(path).unwrap();
        assert!(content.contains("\"operation\":\"query\""));
        assert!(content.contains("\"model_id\":\"embedding-default-v1\""));
    }

    #[test]
    fn jsonl_sink_sequence_continues_after_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("audit-seq.log");

        let sink = JsonlAuditSink::open(&path).unwrap();
        sink.record(AuditEvent::new(
            AuditOperation::Query,
            AuditOutcome::Succeeded,
        ))
        .unwrap();
        sink.record(AuditEvent::new(
            AuditOperation::Query,
            AuditOutcome::Succeeded,
        ))
        .unwrap();
        drop(sink);

        let sink = JsonlAuditSink::open(&path).unwrap();
        sink.record(AuditEvent::new(
            AuditOperation::Query,
            AuditOutcome::Succeeded,
        ))
        .unwrap();

        let content = std::fs::read_to_string(path).unwrap();
        let last_line = content.lines().last().unwrap();
        let event: AuditEvent = serde_json::from_str(last_line).unwrap();
        assert_eq!(event.sequence, 3);
    }

    #[test]
    fn in_memory_sink_concurrent_records_are_physically_ordered_by_sequence() {
        use std::sync::Arc;
        use std::thread;

        let sink = Arc::new(InMemoryAuditSink::default());
        let mut handles = Vec::new();
        for _ in 0..8 {
            let sink = Arc::clone(&sink);
            handles.push(thread::spawn(move || {
                sink.record(AuditEvent::new(
                    AuditOperation::Ingest,
                    AuditOutcome::Succeeded,
                ))
                .unwrap();
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }

        let events = sink.events().unwrap();
        assert_eq!(events.len(), 8);
        let sequences: Vec<u64> = events.iter().map(|e| e.sequence).collect();
        let mut sorted = sequences.clone();
        sorted.sort_unstable();
        assert_eq!(
            sequences, sorted,
            "physical storage order must match sequence order"
        );
    }

    #[test]
    fn jsonl_sink_recovers_from_corrupt_and_non_utf8_lines() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("audit-corrupt.log");

        let mut event1 = AuditEvent::new(AuditOperation::Ingest, AuditOutcome::Succeeded);
        event1.sequence = 1;
        let mut event2 = AuditEvent::new(AuditOperation::Query, AuditOutcome::Succeeded);
        event2.sequence = 2;

        let mut bytes = Vec::new();
        bytes.extend_from_slice(serde_json::to_string(&event1).unwrap().as_bytes());
        bytes.push(b'\n');
        // Corrupt/partial line.
        bytes.extend_from_slice(b"{not valid json");
        bytes.push(b'\n');
        // Non-UTF-8 line.
        bytes.extend_from_slice(&[0xff, 0xfe, 0xfd]);
        bytes.push(b'\n');
        bytes.extend_from_slice(serde_json::to_string(&event2).unwrap().as_bytes());
        bytes.push(b'\n');
        std::fs::write(&path, &bytes).unwrap();

        let sink = JsonlAuditSink::open(&path).unwrap();
        sink.record(AuditEvent::new(
            AuditOperation::Query,
            AuditOutcome::Succeeded,
        ))
        .unwrap();

        let raw = std::fs::read(&path).unwrap();
        let content = String::from_utf8_lossy(&raw);
        let last_line = content.lines().last().unwrap();
        let event: AuditEvent = serde_json::from_str(last_line).unwrap();
        assert_eq!(event.sequence, 3);
    }

    #[test]
    fn jsonl_sink_concurrent_records_have_unique_sequential_sequences() {
        use std::sync::Arc;
        use std::thread;

        let dir = tempdir().unwrap();
        let path = dir.path().join("audit-concurrent.log");
        let sink = Arc::new(JsonlAuditSink::open(&path).unwrap());

        let mut handles = Vec::new();
        for _ in 0..8 {
            let sink = Arc::clone(&sink);
            handles.push(thread::spawn(move || {
                sink.record(AuditEvent::new(
                    AuditOperation::Ingest,
                    AuditOutcome::Succeeded,
                ))
                .unwrap();
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }

        let content = std::fs::read_to_string(&path).unwrap();
        let mut sequences: Vec<u64> = content
            .lines()
            .map(|line| serde_json::from_str::<AuditEvent>(line).unwrap().sequence)
            .collect();
        sequences.sort_unstable();
        assert_eq!(sequences, (1..=8).collect::<Vec<u64>>());
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn jsonl_sink_surfaces_write_failure_on_disk_full() {
        // `/dev/full` always reports ENOSPC on write, letting us exercise the
        // real write/flush failure path without relying on filesystem quotas
        // or permission tricks that don't reliably fail an already-open fd.
        let sink = JsonlAuditSink::open("/dev/full").unwrap();

        let result = sink.record(AuditEvent::new(
            AuditOperation::Ingest,
            AuditOutcome::Succeeded,
        ));

        assert!(matches!(result, Err(AuditError::Io(_))));
    }

    #[test]
    fn audit_health_tracks_failures_without_recursing_into_the_sink() {
        let health = AuditHealth::default();
        assert!(!health.is_degraded());
        assert_eq!(health.failure_count(), 0);

        health.record_failure(&AuditError::LockPoisoned);
        health.record_failure(&AuditError::LockPoisoned);

        assert!(health.is_degraded());
        assert_eq!(health.failure_count(), 2);
        assert_eq!(
            health.last_error().as_deref(),
            Some("audit sink lock poisoned")
        );
    }
}
