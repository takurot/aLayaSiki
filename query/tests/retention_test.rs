use std::sync::Arc;

use alayasiki_core::embedding::deterministic_embedding;
use alayasiki_core::model::Node;
use query::{QueryEngine, QueryRequest};
use storage::repo::Repository;
use tempfile::tempdir;

#[tokio::test]
async fn query_excludes_retention_expired_nodes() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("retention_filter.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    let mut expired = Node::new(
        1,
        deterministic_embedding("EV strategy", "embedding-default-v1", 8),
        "expired evidence".to_string(),
    );
    expired
        .metadata
        .insert("retention_until_unix".to_string(), "1".to_string());
    repo.put_node(expired).await.unwrap();

    let engine = QueryEngine::new(repo);
    let request = QueryRequest::parse_json(
        r#"{
            "query":"EV strategy",
            "mode":"evidence",
            "search_mode":"local",
            "top_k":5
        }"#,
    )
    .unwrap();

    let response = engine.execute(request).await.unwrap();

    assert!(response.evidence.nodes.is_empty());
    assert!(response
        .explain
        .exclusions
        .iter()
        .any(|reason| reason.reason == "retention_expired"));
}
