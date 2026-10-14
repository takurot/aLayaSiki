use alayasiki_core::ingest::IngestionRequest;
use alayasiki_runtime::RuntimeBuilder;
use std::collections::HashMap;
use std::time::Duration;

fn request() -> IngestionRequest {
    IngestionRequest::text("Rust startup acquired a company".into(), HashMap::new())
}

#[tokio::test]
async fn standard_runtime_extracts_without_manual_queue_wiring() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = RuntimeBuilder::new(dir.path()).start().await.unwrap();
    let ids = runtime.ingestion().ingest(request()).await.unwrap();
    runtime.wait_for_jobs(Duration::from_secs(5)).await.unwrap();
    assert!(!runtime
        .repository()
        .neighbors_with_session(ids[0], None)
        .await
        .is_empty());
    assert_eq!(runtime.job_stats().await.unwrap().completed, 1);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn explicit_vector_only_mode_has_no_worker_and_preserves_data_on_restart() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = RuntimeBuilder::new(dir.path())
        .extraction_enabled(false)
        .start()
        .await
        .unwrap();
    let ids = runtime.ingestion().ingest(request()).await.unwrap();
    assert!(runtime.job_stats().await.is_none());
    assert!(runtime
        .repository()
        .neighbors_with_session(ids[0], None)
        .await
        .is_empty());
    runtime.shutdown().await.unwrap();
    let runtime = RuntimeBuilder::new(dir.path()).start().await.unwrap();
    assert!(runtime.repository().get_node(ids[0]).await.is_ok());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn pending_durable_jobs_recover_in_standard_runtime() {
    use jobs::{durable::DurableJobQueue, queue::Job};
    let dir = tempfile::tempdir().unwrap();
    let runtime = RuntimeBuilder::new(dir.path())
        .extraction_enabled(false)
        .start()
        .await
        .unwrap();
    let ids = runtime.ingestion().ingest(request()).await.unwrap();
    let snapshot = runtime.repository().current_snapshot_id().await;
    runtime.shutdown().await.unwrap();
    let (queue, rx) = DurableJobQueue::open(dir.path().join("jobs.wal"))
        .await
        .unwrap();
    queue
        .enqueue_tracked(Job::ExtractEntities {
            node_id: ids[0],
            content: "Rust startup acquired a company".into(),
            model_id: "triplex-lite@1.0.0".into(),
            snapshot_id: snapshot,
        })
        .await
        .unwrap();
    drop(rx);
    drop(queue);
    let runtime = RuntimeBuilder::new(dir.path()).start().await.unwrap();
    runtime.wait_for_jobs(Duration::from_secs(5)).await.unwrap();
    assert!(!runtime
        .repository()
        .neighbors_with_session(ids[0], None)
        .await
        .is_empty());
    assert_eq!(runtime.job_stats().await.unwrap().completed, 1);
    runtime.shutdown().await.unwrap();
}

struct BrokenExtractor;
#[async_trait::async_trait]
impl slm::ner::EntityExtractor for BrokenExtractor {
    async fn extract(&self, _: &str) -> anyhow::Result<Vec<slm::ner::Entity>> {
        anyhow::bail!("extractor unavailable")
    }
}

#[tokio::test]
async fn extraction_failure_settles_in_dlq_without_losing_vector_ingest() {
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let mut registry = slm::registry::ModelRegistry::new();
    registry
        .register("broken", "1", Arc::new(BrokenExtractor))
        .unwrap();
    let runtime = RuntimeBuilder::new(dir.path())
        .with_registry(Arc::new(registry), "broken@1")
        .with_queue_config(jobs::durable::DurableQueueConfig {
            max_attempts: 1,
            ..Default::default()
        })
        .start()
        .await
        .unwrap();
    let ids = runtime.ingestion().ingest(request()).await.unwrap();
    runtime.wait_for_jobs(Duration::from_secs(5)).await.unwrap();
    let stats = runtime.job_stats().await.unwrap();
    assert_eq!(stats.dead_lettered, 1);
    assert_eq!(stats.completed, 0);
    assert!(!runtime
        .repository()
        .get_node(ids[0])
        .await
        .unwrap()
        .embedding
        .is_empty());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn unknown_default_model_fails_before_worker_start() {
    let dir = tempfile::tempdir().unwrap();
    let result = RuntimeBuilder::new(dir.path())
        .with_registry(
            std::sync::Arc::new(slm::registry::ModelRegistry::new()),
            "missing@1",
        )
        .start()
        .await;
    assert!(matches!(
        result,
        Err(alayasiki_runtime::RuntimeError::Registry(_))
    ));
}

#[tokio::test]
async fn configured_policy_masks_before_graph_and_job_persistence() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = RuntimeBuilder::new(dir.path())
        .with_ingestion_config(|pipeline| {
            pipeline.with_policy(Box::new(ingestion::policy::BasicPolicy::new(vec![], true)))
        })
        .start()
        .await
        .unwrap();
    let ids = runtime
        .ingestion()
        .ingest(IngestionRequest::text(
            "Rust contact user@example.com".into(),
            HashMap::new(),
        ))
        .await
        .unwrap();
    runtime.wait_for_jobs(Duration::from_secs(5)).await.unwrap();
    assert!(runtime
        .repository()
        .get_node(ids[0])
        .await
        .unwrap()
        .data
        .contains("[EMAIL]"));
    runtime.shutdown().await.unwrap();
    for name in ["graph.wal", "jobs.wal"] {
        let bytes = std::fs::read(dir.path().join(name)).unwrap();
        assert!(!bytes
            .windows(b"user@example.com".len())
            .any(|slice| slice == b"user@example.com"));
    }
}

struct GatedExtractor {
    started: std::sync::Arc<tokio::sync::Notify>,
    release: std::sync::Arc<tokio::sync::Notify>,
}
#[async_trait::async_trait]
impl slm::ner::EntityExtractor for GatedExtractor {
    async fn extract(&self, _: &str) -> anyhow::Result<Vec<slm::ner::Entity>> {
        self.started.notify_one();
        self.release.notified().await;
        Ok(vec![slm::ner::Entity {
            text: "gated".into(),
            label: "test".into(),
            confidence: 1.0,
        }])
    }
}

#[tokio::test]
async fn shutdown_finishes_active_job_before_reopening_repository() {
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let mut registry = slm::registry::ModelRegistry::new();
    registry
        .register(
            "gated",
            "1",
            Arc::new(GatedExtractor {
                started: started.clone(),
                release: release.clone(),
            }),
        )
        .unwrap();
    let runtime = RuntimeBuilder::new(dir.path())
        .with_registry(Arc::new(registry), "gated@1")
        .start()
        .await
        .unwrap();
    let ids = runtime.ingestion().ingest(request()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .unwrap();
    let shutdown = tokio::spawn(runtime.shutdown());
    tokio::task::yield_now().await;
    assert!(!shutdown.is_finished());
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let runtime = RuntimeBuilder::new(dir.path()).start().await.unwrap();
    assert_eq!(runtime.job_stats().await.unwrap().completed, 1);
    let edges = runtime
        .repository()
        .neighbors_with_session(ids[0], None)
        .await;
    assert_eq!(edges.len(), 1);
    let entity = runtime.repository().get_node(edges[0].0).await.unwrap();
    assert_eq!(entity.metadata["extraction_model_id"], "gated@1");
    runtime.shutdown().await.unwrap();
}
