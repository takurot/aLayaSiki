use alayasiki_core::ingest::{Chunk, IngestionRequest};
use ingestion::chunker::{BoxFuture, Chunker, SemanticChunker};
use ingestion::embedding::DeterministicEmbedder;
use ingestion::policy::BasicPolicy;
use ingestion::processor::IngestionPipeline;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use storage::repo::Repository;
use storage::wal::{Wal, WalFlushPolicy, WalOptions};
use tempfile::tempdir;
use tokio::sync::Mutex;

struct FixedChunker {
    chunks: Vec<String>,
}

impl Chunker for FixedChunker {
    fn chunk<'a>(
        &'a self,
        _content: &'a str,
        base_metadata: HashMap<String, String>,
    ) -> BoxFuture<'a, Vec<Chunk>> {
        Box::pin(async move {
            self.chunks
                .iter()
                .enumerate()
                .map(|(index, content)| {
                    let mut metadata = base_metadata.clone();
                    metadata.insert("chunk_index".to_string(), index.to_string());
                    Chunk {
                        content: content.clone(),
                        metadata,
                        embedding: None,
                    }
                })
                .collect()
        })
    }
}

#[tokio::test]
async fn test_ingestion_flow() {
    // 1. Setup
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("ingest.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    let pipeline = IngestionPipeline::new(repo.clone());

    // 2. Create Request
    let content = "Hello world. This is a test of the ingestion pipeline.";
    let mut metadata = HashMap::new();
    metadata.insert("source".to_string(), "test".to_string());

    let request = IngestionRequest::Text {
        content: content.to_string(),
        metadata: metadata.clone(),
        idempotency_key: None,
        model_id: None,
    };

    // 3. Ingest
    let node_ids = pipeline.ingest(request).await.unwrap();
    assert!(!node_ids.is_empty());

    // 4. Verify Storage
    let node_id = node_ids[0];
    let retrieved_node = repo.get_node(node_id).await.unwrap();

    assert!(retrieved_node.data.contains("Hello world")); // Should contain part of the text
    assert_eq!(retrieved_node.metadata.get("source").unwrap(), "test");
    assert!(!retrieved_node.embedding.is_empty());
}

#[tokio::test]
async fn test_ingestion_idempotency_key() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("idempotent.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    let pipeline = IngestionPipeline::new(repo.clone());

    let mut metadata = HashMap::new();
    metadata.insert("source".to_string(), "test".to_string());

    let request = IngestionRequest::Text {
        content: "Idempotent content".to_string(),
        metadata,
        idempotency_key: Some("fixed-key".to_string()),
        model_id: None,
    };

    let first_ids = pipeline.ingest(request.clone()).await.unwrap();
    let second_ids = pipeline.ingest(request).await.unwrap();

    assert_eq!(first_ids, second_ids);
}

#[tokio::test]
async fn test_ingestion_batches_chunks_and_idempotency_into_single_wal_record() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("batched_ingest.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    let pipeline = IngestionPipeline::with_components(
        repo.clone(),
        Box::new(FixedChunker {
            chunks: vec!["chunk-a".to_string(), "chunk-b".to_string()],
        }),
        Box::new(DeterministicEmbedder::default()),
        Box::new(BasicPolicy::new(Vec::new(), false)),
        "embedding-default-v1",
    );

    let request = IngestionRequest::Text {
        content: "ignored by fixed chunker".to_string(),
        metadata: HashMap::new(),
        idempotency_key: Some("batched-key".to_string()),
        model_id: None,
    };

    let node_ids = pipeline.ingest(request.clone()).await.unwrap();
    assert_eq!(node_ids.len(), 2);
    assert_eq!(repo.current_snapshot_id().await, "wal-lsn-1");
    assert_eq!(
        repo.check_idempotency("batched-key").await,
        Some(node_ids.clone())
    );
    assert_eq!(
        pipeline.ingest(request).await.unwrap(),
        node_ids,
        "idempotent retry should reuse previously committed node ids"
    );

    drop(pipeline);
    drop(repo);

    let reopened = Arc::new(Repository::open(&wal_path).await.unwrap());
    assert_eq!(reopened.current_snapshot_id().await, "wal-lsn-1");
    assert_eq!(
        reopened.check_idempotency("batched-key").await,
        Some(node_ids.clone())
    );

    let mut wal = Wal::open(&wal_path).await.unwrap();
    let mut record_count = 0usize;

    wal.replay(|_lsn, _payload| {
        record_count += 1;
        Ok(())
    })
    .await
    .unwrap();

    assert_eq!(record_count, 1);
}

#[tokio::test]
async fn test_ingestion_policy_forbidden_word() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("policy.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    let policy = BasicPolicy::new(vec!["forbidden".to_string()], true);
    let pipeline = IngestionPipeline::with_components(
        repo.clone(),
        Box::new(SemanticChunker::default()),
        Box::new(DeterministicEmbedder::default()),
        Box::new(policy),
        "embedding-default-v1",
    );

    let request = IngestionRequest::Text {
        content: "This contains a forbidden token.".to_string(),
        metadata: HashMap::new(),
        idempotency_key: None,
        model_id: None,
    };

    let result = pipeline.ingest(request).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_ingestion_pdf_extract() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("pdf.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());
    let pipeline = IngestionPipeline::new(repo.clone());

    let pdf_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/assets/dummy.pdf");
    let pdf_bytes = std::fs::read(pdf_path).unwrap();

    let request = IngestionRequest::File {
        filename: "dummy.pdf".to_string(),
        content: pdf_bytes,
        mime_type: "application/pdf".to_string(),
        metadata: HashMap::from([("source".to_string(), "tests/assets/dummy.pdf".to_string())]),
        idempotency_key: None,
        model_id: None,
    };

    let node_ids = pipeline.ingest(request).await.unwrap();
    let node = repo.get_node(node_ids[0]).await.unwrap();
    assert!(node.data.contains("Dummy PDF file"));
}

#[tokio::test]
async fn test_ingestion_with_job_queue() {
    use jobs::queue::ChannelJobQueue;
    use jobs::worker::Worker;
    use slm::lightweight::register_default_lightweight_models;
    use slm::registry::ModelRegistry;
    use tokio::sync::mpsc;

    // 1. Setup Repo and Pipeline
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("full_flow.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    // 2. Setup Worker and Queue
    let (tx, rx) = mpsc::channel(100);
    let queue = Arc::new(ChannelJobQueue::new(tx));
    let mut registry = ModelRegistry::new();
    register_default_lightweight_models(&mut registry).unwrap();
    let worker = Worker::with_registry(rx, repo.clone(), Arc::new(registry), "triplex-lite");

    // Spawn worker in background
    tokio::spawn(async move {
        worker.run().await;
    });

    // 3. Setup Pipeline with Queue
    let mut pipeline = IngestionPipeline::new(repo.clone());
    pipeline.set_job_queue(queue);

    // 4. Ingest Content with standard keywords ("Rust")
    let request = IngestionRequest::Text {
        content: "I love coding in Rust.".to_string(),
        metadata: HashMap::new(),
        idempotency_key: None,
        model_id: None,
    };

    let node_ids = pipeline.ingest(request).await.unwrap();
    let source_id = node_ids[0];

    // 5. Wait for async processing (Polling)
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(5);
    let mut found = false;

    while start.elapsed() < timeout {
        let index = repo.hyper_index.read().await;
        // Expand graph to see if edge created
        let neighbors = index.expand_graph(source_id, 1);
        if !neighbors.is_empty() {
            found = true;
            break;
        }
        drop(index); // Release lock
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    // 6. Verify Edge Creation
    assert!(
        found,
        "Should have created an edge to 'Rust' entity within timeout"
    );
}

struct CapturingQueue {
    jobs: Arc<Mutex<Vec<jobs::queue::Job>>>,
}

#[async_trait::async_trait]
impl jobs::queue::JobQueue for CapturingQueue {
    async fn enqueue(&self, job: jobs::queue::Job) -> anyhow::Result<()> {
        self.jobs.lock().await.push(job);
        Ok(())
    }
}

#[tokio::test]
async fn test_ingestion_enqueues_fixed_model_and_snapshot_for_reproducibility() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("repro.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    let captured = Arc::new(Mutex::new(Vec::new()));
    let queue = Arc::new(CapturingQueue {
        jobs: captured.clone(),
    });

    let mut pipeline = IngestionPipeline::new(repo);
    pipeline.set_job_queue(queue);

    let request = IngestionRequest::Text {
        content: "Graph database query".to_string(),
        metadata: HashMap::new(),
        idempotency_key: None,
        model_id: Some("triplex-lite@1.0.0".to_string()),
    };

    pipeline.ingest(request).await.unwrap();

    let jobs = captured.lock().await;
    assert!(!jobs.is_empty());
    match &jobs[0] {
        jobs::queue::Job::ExtractEntities {
            model_id,
            snapshot_id,
            ..
        } => {
            assert_eq!(model_id, "triplex-lite@1.0.0");
            assert!(snapshot_id.starts_with("wal-lsn-"));
        }
    }
}

#[tokio::test]
async fn test_ingestion_flushes_buffered_wal_before_enqueuing_snapshot() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("repro_buffered.wal");
    let repo = Arc::new(
        Repository::open_with_options(
            &wal_path,
            WalOptions {
                flush_policy: WalFlushPolicy::Batch { max_entries: 16 },
                ..WalOptions::default()
            },
        )
        .await
        .unwrap(),
    );

    let captured = Arc::new(Mutex::new(Vec::new()));
    let queue = Arc::new(CapturingQueue {
        jobs: captured.clone(),
    });

    let mut pipeline = IngestionPipeline::new(repo.clone());
    pipeline.set_job_queue(queue);

    let request = IngestionRequest::Text {
        content: "Graph database query".to_string(),
        metadata: HashMap::new(),
        idempotency_key: None,
        model_id: Some("triplex-lite@1.0.0".to_string()),
    };

    pipeline.ingest(request).await.unwrap();

    let jobs = captured.lock().await;
    assert!(!jobs.is_empty());
    match &jobs[0] {
        jobs::queue::Job::ExtractEntities { snapshot_id, .. } => {
            assert_eq!(snapshot_id, "wal-lsn-1");
        }
    }
    drop(jobs);

    assert_eq!(repo.current_snapshot_id().await, "wal-lsn-1");
}

struct FailingExtractor;

#[async_trait::async_trait]
impl slm::ner::EntityExtractor for FailingExtractor {
    async fn extract(&self, _text: &str) -> anyhow::Result<Vec<slm::ner::Entity>> {
        anyhow::bail!("simulated extractor failure")
    }
}

#[tokio::test]
async fn test_ingestion_is_failsafe_when_extraction_model_fails() {
    use jobs::queue::ChannelJobQueue;
    use jobs::worker::Worker;
    use slm::registry::ModelRegistry;
    use tokio::sync::mpsc;

    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("failsafe.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    let mut registry = ModelRegistry::new();
    registry
        .register("broken-model", "1.0.0", Arc::new(FailingExtractor))
        .unwrap();
    registry.activate("broken-model", "1.0.0").unwrap();

    let (tx, rx) = mpsc::channel(16);
    let queue = Arc::new(ChannelJobQueue::new(tx));
    let worker = Worker::with_registry(rx, repo.clone(), Arc::new(registry), "broken-model");
    tokio::spawn(async move { worker.run().await });

    let mut pipeline = IngestionPipeline::new(repo.clone());
    pipeline.set_job_queue(queue);

    let request = IngestionRequest::Text {
        content: "This ingestion should succeed even if extraction fails.".to_string(),
        metadata: HashMap::new(),
        idempotency_key: None,
        model_id: Some("broken-model".to_string()),
    };

    let node_ids = pipeline.ingest(request).await.unwrap();
    assert!(!node_ids.is_empty());
    let source_id = node_ids[0];

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let node = repo.get_node(source_id).await.unwrap();
    assert!(!node.data.is_empty());

    let index = repo.hyper_index.read().await;
    let neighbors = index.expand_graph(source_id, 1);
    assert!(
        neighbors.is_empty(),
        "failed extraction must not break ingestion and should produce no graph edges"
    );
}
