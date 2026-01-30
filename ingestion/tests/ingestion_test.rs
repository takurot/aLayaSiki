use ingestion::processor::IngestionPipeline;
use ingestion::chunker::SemanticChunker;
use ingestion::embedding::DeterministicEmbedder;
use ingestion::policy::BasicPolicy;
use alayasiki_core::ingest::IngestionRequest;
use storage::repo::Repository;
use std::sync::Arc;
use tempfile::tempdir;
use std::collections::HashMap;

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
#[ignore] // TODO: Requires valid PDF binary for pdf-extract (mock is too simple)
async fn test_ingestion_pdf_extract() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("pdf.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());
    let pipeline = IngestionPipeline::new(repo.clone());

    let pdf_bytes = b"%PDF-1.4\n1 0 obj\n<<>>\nendobj\n2 0 obj\n<< /Length 44 >>\nstream\nBT\n/F1 12 Tf\n(Hello PDF) Tj\nET\nendstream\nendobj\nxref\n0 3\n0000000000 65535 f \ntrailer\n<<>>\nstartxref\n0\n%%EOF".to_vec();

    let request = IngestionRequest::File {
        filename: "sample.pdf".to_string(),
        content: pdf_bytes,
        mime_type: "application/pdf".to_string(),
        metadata: HashMap::new(),
        idempotency_key: None,
        model_id: None,
    };

    let node_ids = pipeline.ingest(request).await.unwrap();
    let node = repo.get_node(node_ids[0]).await.unwrap();
    assert!(node.data.contains("Hello PDF"));
}

#[tokio::test]
async fn test_ingestion_with_job_queue() {
    use jobs::queue::ChannelJobQueue;
    use jobs::worker::Worker;
    use slm::ner::MockEntityExtractor;
    use tokio::sync::mpsc;

    // 1. Setup Repo and Pipeline
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("full_flow.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    // 2. Setup Worker and Queue
    let (tx, rx) = mpsc::channel(100);
    let queue = Arc::new(ChannelJobQueue::new(tx));
    let extractor = Arc::new(MockEntityExtractor::new());
    
    let worker = Worker::new(rx, repo.clone(), extractor);
    
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
    assert!(found, "Should have created an edge to 'Rust' entity within timeout");
}
