use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use alayasiki_core::embedding::deterministic_embedding;
use alayasiki_core::ingest::IngestionRequest;
use alayasiki_core::model::{Edge, Node};
use ingestion::processor::IngestionPipeline;
use prototypes::bench_eval::{
    build_latency_summary, format_ns, now_unix, write_json_report, LatencySummary,
};
use query::{QueryEngine, QueryRequest};
use serde::Serialize;
use storage::repo::Repository;
use storage::wal::{WalFlushPolicy, WalOptions};

const DIMS: usize = 32;
const MODEL_ID: &str = "embedding-default-v1";

#[derive(Debug, Serialize)]
struct OperationalBenchmarkReport {
    benchmark: String,
    generated_at_unix: u64,
    config: ReportConfig,
    totals: Totals,
    read_latency_ns: LatencySummary,
    write_latency_ns: LatencySummary,
    durability_barrier: DurabilityBarrier,
}

#[derive(Debug, Serialize)]
struct ReportConfig {
    nodes: u64,
    workers: usize,
    ops_per_worker: usize,
    write_every: usize,
    read_to_write_ratio: String,
    wal_flush_policy: String,
    seed_wal_flush_policy: String,
    write_latency_scope: String,
}

#[derive(Debug, Serialize)]
struct Totals {
    elapsed_sec: f64,
    total_ops: usize,
    read_ops: usize,
    write_ops: usize,
    throughput_ops_per_sec: f64,
}

#[derive(Debug, Serialize)]
struct DurabilityBarrier {
    final_flush_ns: u128,
    final_flush_ms: f64,
}

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

fn env_f64(key: &str) -> Option<f64> {
    env::var(key).ok().and_then(|v| v.parse::<f64>().ok())
}

fn env_u64_optional(key: &str) -> Option<u64> {
    env::var(key).ok().and_then(|v| v.parse::<u64>().ok())
}

fn parse_wal_flush_policy() -> WalFlushPolicy {
    match env::var("ALAYASIKI_BENCH_WAL_FLUSH_POLICY")
        .unwrap_or_else(|_| "always".to_string())
        .to_ascii_lowercase()
        .as_str()
    {
        "always" => WalFlushPolicy::Always,
        "interval" => WalFlushPolicy::Interval(Duration::from_millis(
            env_u64_optional("ALAYASIKI_BENCH_WAL_FLUSH_INTERVAL_MS").unwrap_or(10),
        )),
        "batch" => WalFlushPolicy::Batch {
            max_entries: env_usize("ALAYASIKI_BENCH_WAL_FLUSH_BATCH_MAX_ENTRIES", 16),
        },
        other => panic!(
            "unsupported ALAYASIKI_BENCH_WAL_FLUSH_POLICY value: {other} (expected always|interval|batch)"
        ),
    }
}

fn normalize_wal_flush_policy(policy: WalFlushPolicy) -> WalFlushPolicy {
    match policy {
        WalFlushPolicy::Always => WalFlushPolicy::Always,
        WalFlushPolicy::Interval(interval) if interval.is_zero() => WalFlushPolicy::Always,
        WalFlushPolicy::Interval(interval) => WalFlushPolicy::Interval(interval),
        WalFlushPolicy::Batch { max_entries } => WalFlushPolicy::Batch {
            max_entries: max_entries.max(1),
        },
    }
}

fn format_wal_flush_policy(policy: WalFlushPolicy) -> String {
    match policy {
        WalFlushPolicy::Always => "always".to_string(),
        WalFlushPolicy::Interval(interval) => format!("interval:{}ms", interval.as_millis()),
        WalFlushPolicy::Batch { max_entries } => format!("batch:{max_entries}"),
    }
}

fn write_latency_scope(policy: WalFlushPolicy) -> &'static str {
    match policy {
        WalFlushPolicy::Always => "durable",
        WalFlushPolicy::Interval(_) | WalFlushPolicy::Batch { .. } => "submit_only",
    }
}

fn default_results_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("benchmarks")
        .join("results")
        .join("operational_latency_latest.json")
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
    let wal_flush_policy = normalize_wal_flush_policy(parse_wal_flush_policy());
    let seed_batch_entries = env_usize("ALAYASIKI_BENCH_SEED_WAL_BATCH_MAX_ENTRIES", 1_024);
    let seed_wal_flush_policy = normalize_wal_flush_policy(WalFlushPolicy::Batch {
        max_entries: seed_batch_entries.max(1),
    });
    let results_path = env::var("ALAYASIKI_BENCH_RESULTS_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| default_results_path());

    let temp_dir = tempfile::tempdir().unwrap();
    let wal_path = temp_dir.path().join("operational_latency_bench.wal");
    let seed_repo_handle = Arc::new(
        Repository::open_with_options(
            &wal_path,
            WalOptions {
                flush_policy: seed_wal_flush_policy,
                ..WalOptions::default()
            },
        )
        .await
        .unwrap(),
    );

    seed_repo(&seed_repo_handle, node_count).await;
    seed_repo_handle.flush().await.unwrap();
    drop(seed_repo_handle);

    let repo = Arc::new(
        Repository::open_with_options(
            &wal_path,
            WalOptions {
                flush_policy: wal_flush_policy,
                ..WalOptions::default()
            },
        )
        .await
        .unwrap(),
    );

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

    // Buffered WAL policies defer some fsync cost to a final durability barrier.
    let final_flush_start = Instant::now();
    repo.flush().await.unwrap();
    let final_flush_ns = final_flush_start.elapsed().as_nanos();
    let total_elapsed = scenario_start.elapsed();
    let read_samples = read_latencies.lock().await.clone();
    let write_samples = write_latencies.lock().await.clone();
    let total_ops = read_samples.len() + write_samples.len();
    let throughput = if total_elapsed.as_secs_f64() > 0.0 {
        total_ops as f64 / total_elapsed.as_secs_f64()
    } else {
        0.0
    };
    let read_summary = build_latency_summary(&read_samples);
    let write_summary = build_latency_summary(&write_samples);

    let report = OperationalBenchmarkReport {
        benchmark: "operational_latency_bench".to_string(),
        generated_at_unix: now_unix(),
        config: ReportConfig {
            nodes: node_count,
            workers,
            ops_per_worker,
            write_every,
            read_to_write_ratio: format!("{}:1", write_every.saturating_sub(1)),
            wal_flush_policy: format_wal_flush_policy(wal_flush_policy),
            seed_wal_flush_policy: format_wal_flush_policy(seed_wal_flush_policy),
            write_latency_scope: write_latency_scope(wal_flush_policy).to_string(),
        },
        totals: Totals {
            elapsed_sec: total_elapsed.as_secs_f64(),
            total_ops,
            read_ops: read_samples.len(),
            write_ops: write_samples.len(),
            throughput_ops_per_sec: throughput,
        },
        read_latency_ns: read_summary,
        write_latency_ns: write_summary,
        durability_barrier: DurabilityBarrier {
            final_flush_ns,
            final_flush_ms: final_flush_ns as f64 / 1_000_000.0,
        },
    };

    write_json_report(&results_path, &report);

    println!("=== Operational Latency Benchmark (Query + Ingestion) ===");
    println!(
        "config: nodes={}, workers={}, ops_per_worker={}, write_every={} (read:write ~= {}:{}), wal_flush_policy={}, seed_wal_flush_policy={}, write_latency_scope={}",
        node_count,
        workers,
        ops_per_worker,
        write_every,
        write_every - 1,
        1,
        report.config.wal_flush_policy,
        report.config.seed_wal_flush_policy,
        report.config.write_latency_scope,
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
        format_ns(report.read_latency_ns.p50_ns),
        format_ns(report.read_latency_ns.p95_ns),
        format_ns(report.read_latency_ns.p99_ns)
    );
    println!(
        "write latency: p50={}, p95={}, p99={}",
        format_ns(report.write_latency_ns.p50_ns),
        format_ns(report.write_latency_ns.p95_ns),
        format_ns(report.write_latency_ns.p99_ns)
    );
    println!(
        "durability barrier: final_flush={}",
        format_ns(report.durability_barrier.final_flush_ns)
    );

    let read_p95_ms = report.read_latency_ns.p95_ms;
    let write_p95_ms = report.write_latency_ns.p95_ms;
    let min_throughput = env_f64("ALAYASIKI_BENCH_MIN_THROUGHPUT");
    let max_read_p95_ms = env_f64("ALAYASIKI_BENCH_MAX_READ_P95_MS");
    let max_write_p95_ms = env_f64("ALAYASIKI_BENCH_MAX_WRITE_P95_MS");

    if let Some(limit) = min_throughput {
        assert!(
            throughput >= limit,
            "throughput regression: {:.2} ops/s < {:.2} ops/s",
            throughput,
            limit
        );
    }
    if let Some(limit) = max_read_p95_ms {
        assert!(
            read_p95_ms <= limit,
            "read p95 regression: {:.3} ms > {:.3} ms",
            read_p95_ms,
            limit
        );
    }
    if let Some(limit) = max_write_p95_ms {
        assert!(
            write_p95_ms <= limit,
            "write p95 regression: {:.3} ms > {:.3} ms",
            write_p95_ms,
            limit
        );
    }

    println!("result_json: {}", results_path.display());
}
