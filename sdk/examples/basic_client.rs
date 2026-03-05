use std::collections::HashMap;

use alayasiki_core::ingest::IngestionRequest;
use alayasiki_sdk::ClientBuilder;
use query::QueryRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let wal_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "./tmp/sdk-example.wal".to_string());

    let client = ClientBuilder::new().connect_in_process(&wal_path).await?;

    let mut metadata = HashMap::new();
    metadata.insert("source".to_string(), "example/sdk-basic.md".to_string());
    metadata.insert("entity_type".to_string(), "Company".to_string());
    metadata.insert("timestamp".to_string(), "2025-01-01".to_string());

    let ingest = client
        .ingest(IngestionRequest::Text {
            content: "Toyota and Tesla compete on EV battery strategy".to_string(),
            metadata,
            idempotency_key: Some("sdk-example-doc-1".to_string()),
            model_id: Some("embedding-default-v1".to_string()),
        })
        .await?;

    let request = QueryRequest::parse_json(
        r#"{
            "query": "EV battery strategy",
            "mode": "evidence",
            "search_mode": "local",
            "top_k": 5,
            "model_id": "embedding-default-v1"
        }"#,
    )?;

    let response = client.query(request).await?;

    println!(
        "ingested {} node(s), snapshot={} ",
        ingest.node_ids.len(),
        ingest.snapshot_id
    );
    println!("evidence_nodes={}", response.evidence.nodes.len());

    Ok(())
}
