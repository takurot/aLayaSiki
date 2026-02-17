use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
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
        let next = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
        event.sequence = next;
        let mut events = self.events.lock().map_err(|_| AuditError::LockPoisoned)?;
        events.push(event);
        Ok(())
    }
}

pub struct JsonlAuditSink {
    writer: Mutex<std::fs::File>,
    sequence: AtomicU64,
}

impl JsonlAuditSink {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, AuditError> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }

        let writer = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path.as_ref())?;

        Ok(Self {
            writer: Mutex::new(writer),
            sequence: AtomicU64::new(0),
        })
    }
}

impl AuditSink for JsonlAuditSink {
    fn record(&self, mut event: AuditEvent) -> Result<(), AuditError> {
        let next = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
        event.sequence = next;

        let line = serde_json::to_string(&event)?;
        let mut writer = self.writer.lock().map_err(|_| AuditError::LockPoisoned)?;
        writer.write_all(line.as_bytes())?;
        writer.write_all(b"\n")?;
        writer.flush()?;
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
}
