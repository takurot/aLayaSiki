use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::Instant;

use alayasiki_core::embedding::deterministic_embedding;
use alayasiki_core::ingest::IngestionRequest;
use alayasiki_core::model::{Edge, Node};
use ingestion::processor::IngestionPipeline;
use query::{QueryEngine, QueryRequest};
use storage::repo::Repository;

const DIMS: usize = 32;
const MODEL_ID: &str = "embedding-default-v1";

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn percentile_ns(samples: &[u128], p: f64) -> u128 {
    if samples.is_empty() {
        return 0;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let rank = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[rank]
}

fn fmt_ns(ns: u128) -> String {
    if ns >= 1_000_000 {
        format!("{:.3} ms", ns as f64 / 1_000_000.0)
    } else if ns >= 1_000 {
        format!("{:.3} us", ns as f64 / 1_000.0)
    } else {
        format!("{ns} ns")
    }
}

async fn seed_repo(repo: &Arc<Repository>, node_count: u64) {
    for id in 1..=node_count {
        let mut metadata = HashMap::new();
        metadata.insert(
            "entity_type".to_string(),
            if id % 2 == 0 {
                "Company".to_string()
            } else {
                "Policy".to_string()
            },
        );
        metadata.insert(
            "timestamp".to_string(),
            format!("2024-02-{:02}", (id % 28) + 1),
        );
        metadata.insert("source".to_string(), format!("seed/doc-{id}.md"));

        let text = format!("EV benchmark node {id} with battery and market context");
        let mut node = Node::new(id, deterministic_embedding(&text, MODEL_ID, DIMS), text);
        node.metadata = metadata;
        repo.put_node(node).await.unwrap();
    }

    for id in 1..node_count {
        repo.put_edge(Edge::new(id, id + 1, "related_to", 1.0))
            .await
            .unwrap();
        if id + 5 <= node_count {
            repo.put_edge(Edge::new(id, id + 5, "influences", 0.7))
                .await
                .unwrap();
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let node_count = env_u64("ALAYASIKI_BENCH_NODES", 5_000);
    let workers = env_usize("ALAYASIKI_BENCH_WORKERS", 8);
    let ops_per_worker = env_usize("ALAYASIKI_BENCH_OPS_PER_WORKER", 120);
    let write_every = env_usize("ALAYASIKI_BENCH_WRITE_EVERY", 10).max(1);

    let temp_dir = tempfile::tempdir().unwrap();
    let wal_path = temp_dir.path().join("operational_latency_bench.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    seed_repo(&repo, node_count).await;

    let read_latencies = Arc::new(tokio::sync::Mutex::new(Vec::<u128>::new()));
    let write_latencies = Arc::new(tokio::sync::Mutex::new(Vec::<u128>::new()));

    let scenario_start = Instant::now();
    let mut handles = Vec::with_capacity(workers);
    for worker_id in 0..workers {
        let repo = repo.clone();
        let read_latencies = read_latencies.clone();
        let write_latencies = write_latencies.clone();

        let handle = tokio::spawn(async move {
            let engine = QueryEngine::new(repo.clone());
            let pipeline = IngestionPipeline::new(repo.clone());

            for op in 0..ops_per_worker {
                if op % write_every == 0 {
                    let mut metadata = HashMap::new();
                    metadata.insert("source".to_string(), format!("runtime/worker-{worker_id}"));
                    metadata.insert("entity_type".to_string(), "Company".to_string());
                    metadata.insert("timestamp".to_string(), "2024-03-10".to_string());

                    let request = IngestionRequest::Text {
                        content: format!(
                            "Runtime ingest worker={worker_id} op={op} EV battery expansion."
                        ),
                        metadata,
                        idempotency_key: Some(format!("runtime-{worker_id}-{op}")),
                        model_id: Some(MODEL_ID.to_string()),
                    };

                    let begin = Instant::now();
                    pipeline.ingest(request).await.unwrap();
                    let elapsed = begin.elapsed().as_nanos();
                    write_latencies.lock().await.push(elapsed);
                } else {
                    let query_json = match (worker_id + op) % 4 {
                        0 => {
                            r#"{"query":"EV battery market","mode":"evidence","search_mode":"local","top_k":20,"traversal":{"depth":2}}"#
                        }
                        1 => {
                            r#"{"query":"overall EV themes","mode":"answer","search_mode":"global","top_k":30}"#
                        }
                        2 => {
                            r#"{"query":"insufficient context expansion","mode":"evidence","search_mode":"drift","top_k":20,"traversal":{"depth":1}}"#
                        }
                        _ => {
                            r#"{"query":"EV policy and company relations","mode":"answer","search_mode":"auto","top_k":20,"traversal":{"depth":2}}"#
                        }
                    };
                    let request = QueryRequest::parse_json(query_json).unwrap();

                    let begin = Instant::now();
                    let _ = engine.execute(request).await.unwrap();
                    let elapsed = begin.elapsed().as_nanos();
                    read_latencies.lock().await.push(elapsed);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let total_elapsed = scenario_start.elapsed();
    let read_samples = read_latencies.lock().await.clone();
    let write_samples = write_latencies.lock().await.clone();
    let total_ops = read_samples.len() + write_samples.len();
    let throughput = if total_elapsed.as_secs_f64() > 0.0 {
        total_ops as f64 / total_elapsed.as_secs_f64()
    } else {
        0.0
    };

    println!("=== Operational Latency Benchmark (Query + Ingestion) ===");
    println!(
        "config: nodes={}, workers={}, ops_per_worker={}, write_every={} (read:write ~= {}:{})",
        node_count,
        workers,
        ops_per_worker,
        write_every,
        write_every - 1,
        1
    );
    println!(
        "workload: total_ops={}, read_ops={}, write_ops={}, elapsed={:.3}s, throughput={:.2} ops/s",
        total_ops,
        read_samples.len(),
        write_samples.len(),
        total_elapsed.as_secs_f64(),
        throughput
    );

    println!(
        "read latency: p50={}, p95={}, p99={}",
        fmt_ns(percentile_ns(&read_samples, 0.50)),
        fmt_ns(percentile_ns(&read_samples, 0.95)),
        fmt_ns(percentile_ns(&read_samples, 0.99))
    );
    println!(
        "write latency: p50={}, p95={}, p99={}",
        fmt_ns(percentile_ns(&write_samples, 0.50)),
        fmt_ns(percentile_ns(&write_samples, 0.95)),
        fmt_ns(percentile_ns(&write_samples, 0.99))
    );
}
