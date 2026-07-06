use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use jobs::durable::{DurableJobQueue, DurableQueueConfig};
use jobs::queue::{Job, JobQueue};
use jobs::worker::Worker;
use slm::ner::{Entity, EntityExtractor, MockEntityExtractor};
use storage::repo::Repository;
use storage::wal::WalRecoveryMode;
use tempfile::tempdir;
use tokio::time::sleep;

/// Wait up to `timeout` for `check` to return true, polling every 10 ms.
async fn wait_until<Fut>(timeout: Duration, mut check: impl FnMut() -> Fut) -> bool
where
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if check().await {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        sleep(Duration::from_millis(10)).await;
    }
}

fn zero_backoff() -> DurableQueueConfig {
    DurableQueueConfig {
        max_attempts: 2,
        base_backoff: Duration::ZERO,
        ..DurableQueueConfig::default()
    }
}

fn sample_job(node_id: u64) -> Job {
    Job::ExtractEntities {
        node_id,
        content: format!("content-{node_id}"),
        model_id: "legacy-default".to_string(),
        snapshot_id: "wal-lsn-0".to_string(),
    }
}

#[tokio::test]
async fn poison_message_lands_in_dead_letter_after_configured_attempts() {
    let dir = tempdir().unwrap();
    let (queue, mut rx) =
        DurableJobQueue::open_with_config(dir.path().join("jobs.wal"), zero_backoff())
            .await
            .unwrap();

    let id = queue.enqueue_tracked(sample_job(1)).await.unwrap();
    // First failure -> retry (attempt becomes 1).
    rx.recv().await.unwrap();
    queue.fail(id, "transient".to_string()).await.unwrap();
    // Second failure -> dead-letter (max_attempts == 2).
    rx.recv().await.unwrap();
    queue.fail(id, "permanent".to_string()).await.unwrap();

    let stats = queue.stats().await;
    assert_eq!(stats.dead_lettered, 1);
    assert_eq!(stats.pending_depth, 0);
    let dead_letters = queue.dead_letters().await;
    assert_eq!(dead_letters.len(), 1);
    assert_eq!(dead_letters[0].id, id);
    assert!(dead_letters[0].reason.contains("permanent"));
    assert_eq!(dead_letters[0].attempts, 2);

    // Pipeline continues: a fresh job still completes.
    let id2 = queue.enqueue_tracked(sample_job(2)).await.unwrap();
    rx.recv().await.unwrap();
    queue.complete(id2).await.unwrap();
    let stats = queue.stats().await;
    assert_eq!(stats.completed, 1);
    assert_eq!(stats.dead_lettered, 1);
}

#[tokio::test]
async fn enqueued_jobs_survive_reopen_and_complete() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("jobs.wal");

    // Enqueue without ever consuming or completing, then drop the queue (crash).
    let pending_id = {
        let (queue, _rx) = DurableJobQueue::open_with_config(&path, zero_backoff())
            .await
            .unwrap();
        queue.enqueue_tracked(sample_job(1)).await.unwrap()
    };

    // Reopen: the pending job must be recovered and re-announced.
    let (queue, mut rx) = DurableJobQueue::open_with_config(&path, zero_backoff())
        .await
        .unwrap();
    let recovered = rx.recv().await.unwrap();
    assert_eq!(recovered.id, pending_id);
    assert_eq!(recovered.attempt, 0);
    assert_eq!(queue.stats().await.enqueued, 1);
    assert_eq!(queue.stats().await.pending_depth, 1);

    queue.complete(pending_id).await.unwrap();
    let stats = queue.stats().await;
    assert_eq!(stats.completed, 1);
    assert_eq!(stats.pending_depth, 0);
}

#[tokio::test]
async fn retry_attempt_is_preserved_across_restart() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("jobs.wal");

    // Enqueue, fail once (attempt -> 1), then crash before the retry is processed.
    let id = {
        let (queue, mut rx) = DurableJobQueue::open_with_config(&path, zero_backoff())
            .await
            .unwrap();
        let id = queue.enqueue_tracked(sample_job(1)).await.unwrap();
        rx.recv().await.unwrap();
        queue.fail(id, "first".to_string()).await.unwrap();
        // Drain the retry announcement so the in-memory state reflects attempt 1.
        let retried = rx.recv().await.unwrap();
        assert_eq!(retried.attempt, 1);
        id
    };

    // Reopen with the same budget: the recovered attempt must be 1, so a single
    // further failure dead-letters instead of resetting the retry counter.
    let (queue, mut rx) = DurableJobQueue::open_with_config(&path, zero_backoff())
        .await
        .unwrap();
    let recovered = rx.recv().await.unwrap();
    assert_eq!(recovered.id, id);
    assert_eq!(recovered.attempt, 1);
    queue.fail(id, "second".to_string()).await.unwrap();

    assert!(rx.try_recv().is_err(), "no further retry expected");
    assert_eq!(queue.stats().await.dead_lettered, 1);
    assert_eq!(queue.dead_letters().await.len(), 1);
}

#[tokio::test]
async fn complete_is_idempotent_under_redelivery() {
    let dir = tempdir().unwrap();
    let (queue, mut rx) =
        DurableJobQueue::open_with_config(dir.path().join("jobs.wal"), zero_backoff())
            .await
            .unwrap();
    let id = queue.enqueue_tracked(sample_job(1)).await.unwrap();
    rx.recv().await.unwrap();

    queue.complete(id).await.unwrap();
    // A duplicate ack (e.g. redelivery) must not inflate counters or panic.
    queue.complete(id).await.unwrap();

    let stats = queue.stats().await;
    assert_eq!(stats.completed, 1);
    assert_eq!(stats.pending_depth, 0);
}

#[tokio::test]
async fn fail_after_complete_is_noop() {
    let dir = tempdir().unwrap();
    let (queue, mut rx) =
        DurableJobQueue::open_with_config(dir.path().join("jobs.wal"), zero_backoff())
            .await
            .unwrap();
    let id = queue.enqueue_tracked(sample_job(1)).await.unwrap();
    rx.recv().await.unwrap();

    queue.complete(id).await.unwrap();
    // A late failure for an already-terminal job must not dead-letter it.
    queue.fail(id, "late".to_string()).await.unwrap();

    assert_eq!(queue.stats().await.completed, 1);
    assert!(queue.dead_letters().await.is_empty());
}

#[tokio::test]
async fn dead_letter_attempts_persist_across_restart() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("jobs.wal");
    let max_attempts = 2;

    {
        let (queue, mut rx) = DurableJobQueue::open_with_config(&path, zero_backoff())
            .await
            .unwrap();
        let id = queue.enqueue_tracked(sample_job(1)).await.unwrap();
        rx.recv().await.unwrap();
        queue.fail(id, "first".to_string()).await.unwrap();
        rx.recv().await.unwrap();
        queue.fail(id, "second".to_string()).await.unwrap(); // -> dead-letter
        let dead_letters = queue.dead_letters().await;
        assert_eq!(dead_letters[0].attempts, max_attempts);
    }

    // The replayed DLQ entry must retain the bumped attempt count.
    let (queue, _rx) = DurableJobQueue::open_with_config(&path, zero_backoff())
        .await
        .unwrap();
    let dead_letters = queue.dead_letters().await;
    assert_eq!(dead_letters.len(), 1);
    assert_eq!(
        dead_letters[0].attempts, max_attempts,
        "persisted DLQ attempt count must survive restart"
    );
}

#[tokio::test]
async fn dead_letter_table_evicts_oldest_when_capped() {
    let config = DurableQueueConfig {
        max_dead_letters: 1,
        base_backoff: Duration::ZERO,
        ..zero_backoff()
    };
    let dir = tempdir().unwrap();
    let (queue, mut rx) = DurableJobQueue::open_with_config(dir.path().join("jobs.wal"), config)
        .await
        .unwrap();

    let a = queue.enqueue_tracked(sample_job(1)).await.unwrap();
    rx.recv().await.unwrap();
    queue.fail(a, "e".to_string()).await.unwrap();
    rx.recv().await.unwrap();
    queue.fail(a, "e".to_string()).await.unwrap(); // dead-letter a

    let b = queue.enqueue_tracked(sample_job(2)).await.unwrap();
    rx.recv().await.unwrap();
    queue.fail(b, "e".to_string()).await.unwrap();
    rx.recv().await.unwrap();
    queue.fail(b, "e".to_string()).await.unwrap(); // dead-letter b, evicts a

    let dead_letters = queue.dead_letters().await;
    assert_eq!(
        dead_letters.len(),
        1,
        "max_dead_letters=1 should evict the oldest"
    );
    assert_eq!(dead_letters[0].id, b);
}

#[tokio::test]
async fn full_channel_keeps_jobs_durable_and_eventually_delivers() {
    let config = DurableQueueConfig {
        channel_capacity: 1,
        base_backoff: Duration::ZERO,
        ..DurableQueueConfig::default()
    };
    let dir = tempdir().unwrap();
    let (queue, mut rx) = DurableJobQueue::open_with_config(dir.path().join("jobs.wal"), config)
        .await
        .unwrap();

    let _id1 = queue.enqueue_tracked(sample_job(1)).await.unwrap();
    let _id2 = queue.enqueue_tracked(sample_job(2)).await.unwrap();

    // Both jobs are durable in the WAL + pending even though the channel holds one.
    assert_eq!(queue.stats().await.pending_depth, 2);

    // Draining frees capacity; the deferred announcement of the second job then
    // completes and is received.
    let e1 = rx.recv().await.unwrap();
    let e2 = rx.recv().await.unwrap();
    assert_ne!(e1.id, e2.id);
}

#[tokio::test]
async fn unsupported_schema_version_aborts_open() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("jobs.wal");

    // Append a record with an incompatible schema version directly to the WAL.
    let payload = serde_json::to_vec(&serde_json::json!({"v": 2u32, "op": {"Enqueue": {"id": 1u64, "attempt": 0u32, "enqueued_at_ms": 0i64, "job": {"ExtractEntities": {"node_id": 1u64, "content": "x", "model_id": "m", "snapshot_id": "s"}}}}})).unwrap();
    {
        let mut wal = storage::wal::Wal::open(&path).await.unwrap();
        wal.append(&payload).await.unwrap();
        wal.flush().await.unwrap();
    }

    let result = DurableJobQueue::open_with_config(&path, zero_backoff()).await;
    let Err(err) = result else {
        panic!("expected schema-version error on reopen");
    };
    assert!(
        format!("{err}").contains("schema version"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn stats_counters_rebuild_after_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("jobs.wal");

    {
        let (queue, mut rx) = DurableJobQueue::open_with_config(&path, zero_backoff())
            .await
            .unwrap();
        let a = queue.enqueue_tracked(sample_job(1)).await.unwrap();
        rx.recv().await.unwrap();
        queue.complete(a).await.unwrap();
        let b = queue.enqueue_tracked(sample_job(2)).await.unwrap();
        rx.recv().await.unwrap();
        queue.fail(b, "err".to_string()).await.unwrap();
        rx.recv().await.unwrap();
        queue.fail(b, "err".to_string()).await.unwrap(); // -> dead-letter
    }

    let (queue, _rx) = DurableJobQueue::open_with_config(&path, zero_backoff())
        .await
        .unwrap();
    let stats = queue.stats().await;
    assert_eq!(stats.enqueued, 2);
    assert_eq!(stats.completed, 1);
    assert_eq!(stats.retried, 1);
    assert_eq!(stats.dead_lettered, 1);
    assert_eq!(stats.pending_depth, 0);
}

#[tokio::test]
async fn fail_fast_recovery_aborts_on_corrupt_wal() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("jobs.wal");

    {
        let (queue, _rx) = DurableJobQueue::open_with_config(&path, zero_backoff())
            .await
            .unwrap();
        queue.enqueue_tracked(sample_job(1)).await.unwrap();
        queue.enqueue_tracked(sample_job(2)).await.unwrap();
    }

    flip_last_byte(&path).await;

    let config = DurableQueueConfig {
        recovery_mode: WalRecoveryMode::FailFast,
        ..zero_backoff()
    };
    let result = DurableJobQueue::open_with_config(&path, config).await;
    assert!(result.is_err(), "FailFast must reject a corrupt jobs WAL");
}

#[tokio::test]
async fn recover_to_last_good_keeps_earlier_jobs() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("jobs.wal");

    {
        let (queue, _rx) = DurableJobQueue::open_with_config(&path, zero_backoff())
            .await
            .unwrap();
        queue.enqueue_tracked(sample_job(1)).await.unwrap();
        queue.enqueue_tracked(sample_job(2)).await.unwrap();
    }

    flip_last_byte(&path).await;

    // Default config already uses RecoverToLastGoodOffset.
    let (queue, mut rx) = DurableJobQueue::open_with_config(&path, zero_backoff())
        .await
        .unwrap();
    // The first job survives; the corrupt tail (second job) is truncated.
    let recovered = rx.recv().await.unwrap();
    assert_eq!(recovered.id, 1);
    assert_eq!(queue.stats().await.enqueued, 1);
}

#[tokio::test]
async fn durable_worker_processes_job_and_completes() {
    let dir = tempdir().unwrap();
    let repo = Arc::new(Repository::open(dir.path().join("repo.wal")).await.unwrap());

    let config = DurableQueueConfig {
        max_attempts: 3,
        base_backoff: Duration::ZERO,
        ..DurableQueueConfig::default()
    };
    let (queue, rx) = DurableJobQueue::open_with_config(dir.path().join("jobs.wal"), config)
        .await
        .unwrap();
    let queue = Arc::new(queue);

    let extractor = Arc::new(MockEntityExtractor::new());
    let worker = Worker::new_durable(repo.clone(), extractor);
    let worker_queue = queue.clone();
    tokio::spawn(async move {
        worker.run_durable(worker_queue, rx).await;
    });

    queue
        .enqueue(Job::ExtractEntities {
            node_id: 9_999,
            content: "Rust and AI".to_string(),
            model_id: "legacy-default".to_string(),
            snapshot_id: "wal-lsn-0".to_string(),
        })
        .await
        .unwrap();

    assert!(
        wait_until(Duration::from_secs(2), || async {
            queue.stats().await.completed >= 1
        })
        .await,
        "worker should complete the enqueued job"
    );
    assert_eq!(queue.stats().await.pending_depth, 0);

    let nodes = repo.list_node_ids().await;
    let has_entity = repo
        .get_nodes_by_ids(&nodes)
        .await
        .iter()
        .any(|node| node.data == "Rust");
    assert!(has_entity, "entity node should be materialized in the repo");
}

struct ConditionalFailingExtractor;

#[async_trait]
impl EntityExtractor for ConditionalFailingExtractor {
    async fn extract(&self, text: &str) -> anyhow::Result<Vec<Entity>> {
        if text.contains("poison") {
            return Err(anyhow::anyhow!("simulated SLM outage"));
        }
        MockEntityExtractor::new().extract(text).await
    }
}

#[tokio::test]
async fn durable_worker_dead_letters_failing_job_and_continues() {
    let dir = tempdir().unwrap();
    let repo = Arc::new(Repository::open(dir.path().join("repo.wal")).await.unwrap());

    let config = DurableQueueConfig {
        max_attempts: 2,
        base_backoff: Duration::ZERO,
        ..DurableQueueConfig::default()
    };
    let (queue, rx) = DurableJobQueue::open_with_config(dir.path().join("jobs.wal"), config)
        .await
        .unwrap();
    let queue = Arc::new(queue);

    let worker = Worker::new_durable(repo.clone(), Arc::new(ConditionalFailingExtractor));
    let worker_queue = queue.clone();
    tokio::spawn(async move {
        worker.run_durable(worker_queue, rx).await;
    });

    queue
        .enqueue(Job::ExtractEntities {
            node_id: 1,
            content: "poison".to_string(),
            model_id: "legacy-default".to_string(),
            snapshot_id: "wal-lsn-0".to_string(),
        })
        .await
        .unwrap();

    assert!(
        wait_until(Duration::from_secs(2), || async {
            !queue.dead_letters().await.is_empty()
        })
        .await,
        "failing job should be dead-lettered after max attempts"
    );

    // The worker loop must still be alive: a subsequent healthy job is processed
    // and completed by the same worker.
    queue
        .enqueue(Job::ExtractEntities {
            node_id: 2,
            content: "Rust and AI".to_string(),
            model_id: "legacy-default".to_string(),
            snapshot_id: "wal-lsn-0".to_string(),
        })
        .await
        .unwrap();

    assert!(
        wait_until(Duration::from_secs(2), || async {
            queue.stats().await.completed >= 1
        })
        .await,
        "worker should continue processing after a dead-letter"
    );
    assert_eq!(queue.stats().await.dead_lettered, 1);
    assert_eq!(queue.stats().await.pending_depth, 0);
}

async fn flip_last_byte(path: &std::path::Path) {
    use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

    let mut file = tokio::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .await
        .unwrap();
    let len = file.metadata().await.unwrap().len();
    assert!(len > 0, "WAL must have content to corrupt");
    let flip_offset = len - 1;
    file.seek(std::io::SeekFrom::Start(flip_offset))
        .await
        .unwrap();
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte).await.unwrap();
    byte[0] = byte[0].wrapping_add(1);
    file.seek(std::io::SeekFrom::Start(flip_offset))
        .await
        .unwrap();
    file.write_all(&byte).await.unwrap();
    file.flush().await.unwrap();
}
