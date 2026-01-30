use ingestion::processor::IngestionPipeline;
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
    };

    // 3. Ingest
    let node_ids = pipeline.ingest(request).await.unwrap();
    assert!(!node_ids.is_empty());

    // 4. Verify Storage
    let node_id = node_ids[0];
    let retrieved_node = repo.get_node(node_id).await.unwrap();
    
    assert!(retrieved_node.data.contains("Hello world")); // Should contain part of the text
    assert_eq!(retrieved_node.metadata.get("source").unwrap(), "test");
    assert!(retrieved_node.embedding.iter().any(|&x| x == 0.0)); // Placeholder embedding
}
