use std::collections::HashMap;
use std::sync::Arc;

use alayasiki_core::ingest::IngestionRequest;
use alayasiki_sdk::integrations::langchain::{
    GraphVectorStore, LangChainAdapter, LangChainGraphQuery, LangChainSimilarityQuery,
};
use alayasiki_sdk::integrations::llama_index::{
    GraphStore, LlamaGraphQuery, LlamaIndexAdapter, LlamaVectorQuery, VectorStore,
};
use alayasiki_sdk::ClientBuilder;
use query::SearchMode;
use tempfile::tempdir;

#[tokio::test]
async fn test_llama_vector_store_add_and_similarity_search() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("llama_vector_store.wal");

    let client = Arc::new(
        ClientBuilder::new()
            .connect_in_process(&wal_path)
            .await
            .unwrap(),
    );
    let adapter = LlamaIndexAdapter::new(client);

    let ingest = VectorStore::add(
        &adapter,
        text_request("Acme expands battery plants in Osaka.", "llama/doc-1"),
    )
    .await
    .unwrap();
    assert!(!ingest.node_ids.is_empty());

    let response = VectorStore::similarity_search(
        &adapter,
        LlamaVectorQuery {
            query: "battery plants osaka".to_string(),
            top_k: 5,
            model_id: Some("embedding-default-v1".to_string()),
            snapshot_id: None,
        },
    )
    .await
    .unwrap();

    assert_eq!(response.explain.effective_search_mode, SearchMode::Local);
    assert!(response.answer.is_none());
    assert!(response
        .evidence
        .nodes
        .iter()
        .any(|node| node.data.contains("battery plants")));
}

#[tokio::test]
async fn test_llama_graph_store_normalizes_invalid_depth_and_top_k() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("llama_graph_store.wal");

    let client = Arc::new(
        ClientBuilder::new()
            .connect_in_process(&wal_path)
            .await
            .unwrap(),
    );
    let adapter = LlamaIndexAdapter::new(client);

    VectorStore::add(
        &adapter,
        text_request(
            "Graph search evidence for electric vehicle strategy.",
            "llama/doc-2",
        ),
    )
    .await
    .unwrap();

    let response = GraphStore::query_subgraph(
        &adapter,
        LlamaGraphQuery {
            query: "electric vehicle strategy".to_string(),
            top_k: 0,
            depth: 0,
            model_id: None,
            snapshot_id: None,
        },
    )
    .await
    .unwrap();

    assert_eq!(response.explain.effective_search_mode, SearchMode::Local);
    assert!(response.answer.is_none());
}

#[tokio::test]
async fn test_langchain_graph_vector_store_batch_add_and_similarity_search() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("langchain_graph_vector_store.wal");

    let client = Arc::new(
        ClientBuilder::new()
            .connect_in_process(&wal_path)
            .await
            .unwrap(),
    );
    let adapter = LangChainAdapter::new(client);

    let ingest_results = GraphVectorStore::add_documents(
        &adapter,
        vec![
            text_request("Tesla scales EV battery production.", "langchain/doc-1"),
            text_request("BYD expands charging network in APAC.", "langchain/doc-2"),
        ],
    )
    .await
    .unwrap();

    assert_eq!(ingest_results.len(), 2);
    assert!(ingest_results
        .iter()
        .all(|result| !result.node_ids.is_empty()));

    let response = GraphVectorStore::similarity_search(
        &adapter,
        LangChainSimilarityQuery {
            query: "EV battery production".to_string(),
            top_k: 5,
            model_id: Some("embedding-default-v1".to_string()),
            snapshot_id: None,
        },
    )
    .await
    .unwrap();

    assert!(response.answer.is_none());
    assert!(response
        .evidence
        .nodes
        .iter()
        .any(|node| node.data.contains("battery production")));
}

#[tokio::test]
async fn test_langchain_graph_vector_store_graph_search_normalizes_inputs() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("langchain_graph_search.wal");

    let client = Arc::new(
        ClientBuilder::new()
            .connect_in_process(&wal_path)
            .await
            .unwrap(),
    );
    let adapter = LangChainAdapter::new(client);

    GraphVectorStore::add_documents(
        &adapter,
        vec![text_request(
            "Supply chain graph for battery components.",
            "langchain/doc-3",
        )],
    )
    .await
    .unwrap();

    let response = GraphVectorStore::graph_search(
        &adapter,
        LangChainGraphQuery {
            query: "battery components".to_string(),
            top_k: 0,
            depth: 0,
            model_id: None,
            snapshot_id: None,
        },
    )
    .await
    .unwrap();

    assert_eq!(response.explain.effective_search_mode, SearchMode::Local);
    assert!(response.answer.is_none());
}

fn text_request(content: &str, source: &str) -> IngestionRequest {
    let mut metadata = HashMap::new();
    metadata.insert("source".to_string(), source.to_string());
    metadata.insert("entity_type".to_string(), "Document".to_string());
    metadata.insert("timestamp".to_string(), "2025-01-01".to_string());

    IngestionRequest::Text {
        content: content.to_string(),
        metadata,
        idempotency_key: None,
        model_id: Some("embedding-default-v1".to_string()),
    }
}
