use ingestion::chunker::{CharacterChunker, Chunker, ChunkingConfig};
use std::collections::HashMap;

#[tokio::test]
async fn unicode_sizes_distinguish_characters_and_bytes() {
    let metadata = HashMap::from([
        ("source".into(), "fixture".into()),
        ("chunk_chars".into(), "incorrect".into()),
    ]);
    let chunks = CharacterChunker::default()
        .chunk("日本語🙂e\u{301}", metadata.clone())
        .await;
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].content, "日本語🙂e\u{301}");
    assert_eq!(chunks[0].metadata["chunk_chars"], "6");
    assert_eq!(chunks[0].metadata["chunk_bytes"], "16");
    assert_eq!(chunks[0].metadata["chunk_strategy"], "character");
    assert!(!chunks[0].metadata.contains_key("chunk_tokens"));
    assert_eq!(chunks[0].metadata["source"], "fixture");
    assert_eq!(metadata["chunk_chars"], "incorrect");
}

#[tokio::test]
async fn fixed_windows_and_overlap_remain_deterministic() {
    let chunker = CharacterChunker::new(ChunkingConfig {
        max_chars: 3,
        overlap_chars: 2,
    });
    let chunks = chunker.chunk("あいうえおかきくけ", HashMap::new()).await;
    let texts: Vec<_> = chunks.iter().map(|c| c.content.as_str()).collect();
    assert_eq!(texts, ["あいう", "いうえおか", "おかきくけ"]);
    for (index, chunk) in chunks.iter().enumerate() {
        assert_eq!(chunk.metadata["chunk_index"], index.to_string());
        assert_eq!(
            chunk.metadata["chunk_chars"],
            chunk.content.chars().count().to_string()
        );
        assert_eq!(
            chunk.metadata["chunk_bytes"],
            chunk.content.len().to_string()
        );
        assert!(chunk.content.chars().count() <= 5);
    }
    assert_eq!(
        texts,
        chunker
            .chunk("あいうえおかきくけ", HashMap::new())
            .await
            .iter()
            .map(|c| c.content.as_str())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn empty_short_and_clamped_configuration() {
    assert!(CharacterChunker::default()
        .chunk(" \n\t", HashMap::new())
        .await
        .is_empty());
    let chunker = CharacterChunker::new(ChunkingConfig {
        max_chars: 0,
        overlap_chars: usize::MAX,
    });
    let chunks = chunker.chunk("あい", HashMap::new()).await;
    assert_eq!(
        chunks
            .iter()
            .map(|c| c.content.as_str())
            .collect::<Vec<_>>(),
        ["あ", "あい"]
    );
    assert!(chunks.iter().all(|c| c.metadata["chunk_overlap"] == "1"));
    let chunks = CharacterChunker::new(ChunkingConfig {
        max_chars: 1,
        overlap_chars: 0,
    })
    .chunk("ab", HashMap::new())
    .await;
    assert_eq!(
        chunks
            .iter()
            .map(|c| c.content.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
}

#[tokio::test]
async fn runtime_config_reaches_persisted_chunks_and_stable_ids() {
    use alayasiki_core::config::AppConfig;
    use alayasiki_core::ingest::IngestionRequest;
    use ingestion::processor::IngestionPipeline;
    use std::sync::Arc;
    use storage::repo::Repository;
    let config: AppConfig = serde_json::from_value(serde_json::json!({
        "server": {"host": "127.0.0.1", "port": 8080},
        "storage": {"data_dir": "./data", "wal_flush_interval_ms": 100},
        "chunking": {"strategy": "character", "max_chars": 3, "overlap_chars": 0}
    }))
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let repo = Arc::new(
        Repository::open(dir.path().join("chunks.wal"))
            .await
            .unwrap(),
    );
    let pipeline = IngestionPipeline::with_config(repo.clone(), &config);
    let request = IngestionRequest::Text {
        content: "あいうえおか".into(),
        metadata: HashMap::new(),
        idempotency_key: Some("unicode".into()),
        model_id: None,
        embedding_model_id: None,
        extraction_model_id: None,
    };
    let ids = pipeline.ingest(request.clone()).await.unwrap();
    assert_eq!(ids.len(), 2);
    assert_eq!(ids, pipeline.ingest(request.clone()).await.unwrap());
    let node = repo.get_node(ids[0]).await.unwrap();
    assert_eq!(node.data, "あいう");
    assert_eq!(node.metadata["chunk_chars"], "3");
    assert_eq!(node.metadata["chunk_bytes"], "9");
    let other_dir = tempfile::tempdir().unwrap();
    let other_repo = Arc::new(
        Repository::open(other_dir.path().join("chunks.wal"))
            .await
            .unwrap(),
    );
    let explicit = IngestionPipeline::with_chunker(
        other_repo,
        Box::new(CharacterChunker::new(config.chunking.into())),
    );
    assert_eq!(ids, explicit.ingest(request).await.unwrap());
}
