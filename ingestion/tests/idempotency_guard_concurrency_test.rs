use alayasiki_core::ingest::{Chunk, IngestionRequest};
use ingestion::chunker::{BoxFuture, Chunker};
use ingestion::embedding::DeterministicEmbedder;
use ingestion::policy::{BasicPolicy, NoOpPolicy};
use ingestion::processor::{IngestionError, IngestionPipeline};
use std::collections::HashMap;
use std::sync::Arc;
use storage::repo::Repository;
use tempfile::tempdir;
use tokio::sync::Notify;

/// A `Chunker` that signals `started` as soon as it is invoked, then blocks
/// until `release` is notified. Used to pin an in-flight `ingest()` call
/// inside the idempotency-guarded critical section so a second, concurrent
/// call can be deterministically observed racing against the guard.
struct BlockingChunker {
    started: Arc<Notify>,
    release: Arc<Notify>,
}

impl Chunker for BlockingChunker {
    fn chunk<'a>(
        &'a self,
        content: &'a str,
        base_metadata: HashMap<String, String>,
    ) -> BoxFuture<'a, Vec<Chunk>> {
        Box::pin(async move {
            self.started.notify_one();
            self.release.notified().await;
            vec![Chunk {
                content: content.to_string(),
                metadata: base_metadata,
                embedding: None,
            }]
        })
    }
}

async fn new_pipeline(
    dir: &tempfile::TempDir,
    name: &str,
    chunker: Box<dyn Chunker>,
) -> (Arc<Repository>, Arc<IngestionPipeline>) {
    let wal_path = dir.path().join(name);
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());
    let pipeline = Arc::new(IngestionPipeline::with_components(
        repo.clone(),
        chunker,
        Box::new(DeterministicEmbedder::default()),
        Box::new(NoOpPolicy),
        "embedding-default-v1",
    ));
    (repo, pipeline)
}

fn text_request(content: &str, idempotency_key: Option<&str>) -> IngestionRequest {
    IngestionRequest::Text {
        content: content.to_string(),
        metadata: HashMap::new(),
        idempotency_key: idempotency_key.map(str::to_string),
        model_id: None,
    }
}

/// Same explicit idempotency key + same content: the second concurrent
/// caller must be rejected with `IdempotencyConflict` while the first is
/// still in flight, and a follow-up call after release must observe the
/// committed result rather than another conflict.
#[tokio::test]
async fn concurrent_ingest_same_key_same_content_rejects_second_caller() {
    let dir = tempdir().unwrap();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (_repo, pipeline) = new_pipeline(
        &dir,
        "same_key_same_content.wal",
        Box::new(BlockingChunker {
            started: started.clone(),
            release: release.clone(),
        }),
    )
    .await;

    let request = text_request("shared content", Some("key-a"));

    let pipeline_bg = pipeline.clone();
    let request_bg = request.clone();
    let handle = tokio::spawn(async move { pipeline_bg.ingest(request_bg).await });

    started.notified().await;

    let conflict = pipeline.ingest(request.clone()).await;
    assert!(
        matches!(&conflict, Err(IngestionError::IdempotencyConflict(k)) if k == "key-a"),
        "expected IdempotencyConflict, got {:?}",
        conflict
    );

    release.notify_one();
    let first_ids = handle.await.unwrap().unwrap();

    // Guard must be released: a follow-up call reuses the persisted result
    // instead of hitting the in-flight guard again.
    let followup_ids = pipeline.ingest(request).await.unwrap();
    assert_eq!(first_ids, followup_ids);
}

/// Same explicit idempotency key + different content: the guard is keyed on
/// the idempotency key alone, so a concurrent caller reusing the key with
/// different content is still rejected while the first call is in flight.
#[tokio::test]
async fn concurrent_ingest_same_key_different_content_rejects_second_caller() {
    let dir = tempdir().unwrap();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (_repo, pipeline) = new_pipeline(
        &dir,
        "same_key_diff_content.wal",
        Box::new(BlockingChunker {
            started: started.clone(),
            release: release.clone(),
        }),
    )
    .await;

    let first_request = text_request("first content", Some("key-b"));
    let second_request = text_request("completely different content", Some("key-b"));

    let pipeline_bg = pipeline.clone();
    let first_bg = first_request.clone();
    let handle = tokio::spawn(async move { pipeline_bg.ingest(first_bg).await });

    started.notified().await;

    let conflict = pipeline.ingest(second_request).await;
    assert!(
        matches!(&conflict, Err(IngestionError::IdempotencyConflict(k)) if k == "key-b"),
        "expected IdempotencyConflict, got {:?}",
        conflict
    );

    release.notify_one();
    handle.await.unwrap().unwrap();
}

/// No explicit idempotency key: the guard falls back to the content hash, so
/// two concurrent callers submitting identical content race on that hash and
/// exactly one is admitted.
#[tokio::test]
async fn concurrent_ingest_no_key_identical_content_hash_rejects_second_caller() {
    let dir = tempdir().unwrap();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (_repo, pipeline) = new_pipeline(
        &dir,
        "no_key_same_hash.wal",
        Box::new(BlockingChunker {
            started: started.clone(),
            release: release.clone(),
        }),
    )
    .await;

    let request = text_request("identical content, no explicit key", None);

    let pipeline_bg = pipeline.clone();
    let request_bg = request.clone();
    let handle = tokio::spawn(async move { pipeline_bg.ingest(request_bg).await });

    started.notified().await;

    let content_hash = {
        use alayasiki_core::ingest::ContentHash;
        request.content_hash()
    };

    let conflict = pipeline.ingest(request.clone()).await;
    assert!(
        matches!(&conflict, Err(IngestionError::IdempotencyConflict(k)) if *k == content_hash),
        "expected IdempotencyConflict, got {:?}",
        conflict
    );

    release.notify_one();
    let first_ids = handle.await.unwrap().unwrap();

    let followup_ids = pipeline.ingest(request).await.unwrap();
    assert_eq!(first_ids, followup_ids);
}

/// The in-flight guard must be released on the success path so a later,
/// non-concurrent call with the same key is free to acquire it again.
#[tokio::test]
async fn guard_is_released_after_successful_ingest() {
    let dir = tempdir().unwrap();
    let (_repo, pipeline) = new_pipeline(
        &dir,
        "guard_release_success.wal",
        Box::new(ingestion::chunker::SemanticChunker::default()),
    )
    .await;

    let request = text_request("some content to ingest", Some("release-success-key"));

    pipeline.ingest(request.clone()).await.unwrap();
    // If the guard were not released, this second call would fail with
    // IdempotencyConflict instead of returning the persisted result.
    let result = pipeline.ingest(request).await;
    assert!(result.is_ok(), "expected Ok, got {:?}", result);
}

/// The in-flight guard must also be released when `ingest()` errors out
/// (e.g. a content-policy rejection), so a retry isn't wrongly stuck behind
/// a stale `IdempotencyConflict`.
#[tokio::test]
async fn guard_is_released_after_failed_ingest() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("guard_release_error.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());
    let pipeline = IngestionPipeline::with_components(
        repo.clone(),
        Box::new(ingestion::chunker::SemanticChunker::default()),
        Box::new(DeterministicEmbedder::default()),
        Box::new(BasicPolicy::new(vec!["forbidden".to_string()], false)),
        "embedding-default-v1",
    );

    let request = text_request("this contains a forbidden word", Some("release-error-key"));

    let first = pipeline.ingest(request.clone()).await;
    assert!(matches!(first, Err(IngestionError::Policy(_))));

    // If the guard leaked on the error path, this would return
    // IdempotencyConflict instead of the same policy error.
    let second = pipeline.ingest(request).await;
    assert!(matches!(second, Err(IngestionError::Policy(_))));
}
