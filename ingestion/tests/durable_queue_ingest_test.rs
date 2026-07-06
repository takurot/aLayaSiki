use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alayasiki_core::ingest::IngestionRequest;
use ingestion::processor::IngestionPipeline;
use jobs::durable::{DurableJobQueue, DurableQueueConfig, JobQueueStats};
use jobs::queue::JobQueue;
use jobs::worker::Worker;
use slm::ner::MockEntityExtractor;
use storage::repo::Repository;
use tempfile::tempdir;
use tokio::time::sleep;

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

/// The durable queue is a drop-in `JobQueue` for the ingestion pipeline: ingest
/// durably enqueues extraction jobs, and a durable worker drains and completes them.
#[tokio::test]
async fn ingestion_pipeline_enqueues_into_durable_queue() {
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
    let queue: Arc<DurableJobQueue> = Arc::new(queue);

    let worker = Worker::new_durable(repo.clone(), Arc::new(MockEntityExtractor::new()));
    let worker_queue = queue.clone();
    tokio::spawn(async move {
        worker.run_durable(worker_queue, rx).await;
    });

    let mut pipeline = IngestionPipeline::new(repo.clone());
    // The durable queue satisfies the same `JobQueue` contract as `ChannelJobQueue`.
    pipeline.set_job_queue(queue.clone() as Arc<dyn JobQueue>);

    pipeline
        .ingest(IngestionRequest::Text {
            content: "Tesla and BYD lead the EV market.".to_string(),
            metadata: HashMap::from([("source".to_string(), "ev_report.txt".to_string())]),
            idempotency_key: Some("doc-durable-1".to_string()),
            model_id: Some("embedding-default-v1".to_string()),
        })
        .await
        .unwrap();

    // Ingest must have durably persisted at least one extraction job.
    assert!(
        wait_until(Duration::from_secs(2), || async {
            queue.stats().await.enqueued >= 1
        })
        .await,
        "ingest should durably enqueue extraction jobs"
    );

    // The durable worker drains the queue and completes the extraction.
    assert!(
        wait_until(Duration::from_secs(3), || async {
            let stats: JobQueueStats = queue.stats().await;
            stats.completed >= 1 && stats.pending_depth == 0
        })
        .await,
        "durable worker should complete the ingested extraction job"
    );
}
