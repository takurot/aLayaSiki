use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use alayasiki_core::embedding::deterministic_embedding;
use alayasiki_core::ingest::IngestionRequest;
use alayasiki_core::metrics::MetricsSnapshot;
use alayasiki_core::model::{Edge, Node};
use ingestion::chunker::SemanticChunker;
use ingestion::embedding::DeterministicEmbedder;
use ingestion::policy::NoOpPolicy;
use ingestion::processor::IngestionPipeline;
use prototypes::bench_eval::{
    build_latency_summary, format_ns, now_unix, write_json_report, LatencySummary, ReadObservation,
    ReadQualityAccumulator, ReadQualitySummary,
};
use query::{QueryEngine, QueryRequest};
use serde::Serialize;
use storage::community::{CommunityEngine, DeterministicSummarizer};
use storage::repo::Repository;

const DIMS: usize = 64;
const MODEL_ID: &str = "embedding-default-v1";
const THEMES: [&str; 8] = [
    "battery",
    "policy",
    "supply_chain",
    "manufacturing",
    "charging",
    "autonomy",
    "recycling",
    "raw_materials",
];

#[derive(Debug, Clone)]
struct BenchConfig {
    seed_nodes: u64,
    workers: usize,
    warmup_ops_per_worker: usize,
    measured_ops_per_worker: usize,
    write_every: usize,
    top_k: usize,
    traversal_depth: u8,
    results_path: String,
    min_throughput_ops: Option<f64>,
    max_read_p95_ms: Option<f64>,
    max_write_p95_ms: Option<f64>,
}

#[derive(Debug, Default)]
struct WorkerStats {
    read_latencies_ns: Vec<u128>,
    write_latencies_ns: Vec<u128>,
    read_ops: usize,
    write_ops: usize,
    quality: ReadQualityAccumulator,
    local_reads: usize,
    global_reads: usize,
    drift_reads: usize,
    auto_reads: usize,
}

#[derive(Debug, Clone, Copy)]
struct WorkerRunConfig {
    warmup_ops: usize,
    measured_ops: usize,
    write_every: usize,
    top_k: usize,
    depth: u8,
}

impl WorkerStats {
    fn merge(&mut self, other: Self) {
        self.read_latencies_ns.extend(other.read_latencies_ns);
        self.write_latencies_ns.extend(other.write_latencies_ns);
        self.read_ops += other.read_ops;
        self.write_ops += other.write_ops;
        self.quality.merge(other.quality);
        self.local_reads += other.local_reads;
        self.global_reads += other.global_reads;
        self.drift_reads += other.drift_reads;
        self.auto_reads += other.auto_reads;
    }
}

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    benchmark: String,
    generated_at_unix: u64,
    config: ReportConfig,
    totals: Totals,
    read_latency_ns: LatencySummary,
    write_latency_ns: LatencySummary,
    read_quality: ReadQualitySummary,
    mode_mix: ModeMix,
    query_engine_metrics: MetricsSnapshot,
}

#[derive(Debug, Serialize)]
struct ReportConfig {
    seed_nodes: u64,
    workers: usize,
    warmup_ops_per_worker: usize,
    measured_ops_per_worker: usize,
    write_every: usize,
    read_to_write_ratio: String,
    top_k: usize,
    traversal_depth: u8,
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
struct ModeMix {
    local: usize,
    global: usize,
    drift: usize,
    auto: usize,
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_u8(key: &str, default: u8) -> u8 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<u8>().ok())
        .unwrap_or(default)
}

fn env_f64(key: &str) -> Option<f64> {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
}

fn load_config() -> BenchConfig {
    let write_every = env_usize("ALAYASIKI_GRAPHRAG_WRITE_EVERY", 10).max(1);
    let default_results_path = default_results_path();
    BenchConfig {
        seed_nodes: env_u64("ALAYASIKI_GRAPHRAG_SEED_NODES", 8_000),
        workers: env_usize("ALAYASIKI_GRAPHRAG_WORKERS", 8),
        warmup_ops_per_worker: env_usize("ALAYASIKI_GRAPHRAG_WARMUP_OPS", 20),
        measured_ops_per_worker: env_usize("ALAYASIKI_GRAPHRAG_MEASURED_OPS", 120),
        write_every,
        top_k: env_usize("ALAYASIKI_GRAPHRAG_TOP_K", 24),
        traversal_depth: env_u8("ALAYASIKI_GRAPHRAG_DEPTH", 2),
        results_path: env::var("ALAYASIKI_GRAPHRAG_RESULTS_PATH").unwrap_or(default_results_path),
        min_throughput_ops: env_f64("ALAYASIKI_GRAPHRAG_MIN_THROUGHPUT"),
        max_read_p95_ms: env_f64("ALAYASIKI_GRAPHRAG_MAX_READ_P95_MS"),
        max_write_p95_ms: env_f64("ALAYASIKI_GRAPHRAG_MAX_WRITE_P95_MS"),
    }
}

fn default_results_path() -> String {
    let path: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("benchmarks")
        .join("results")
        .join("graphrag_production_latest.json");
    path.to_string_lossy().into_owned()
}

async fn seed_repository(repo: &Arc<Repository>, node_count: u64) {
    for id in 1..=node_count {
        let theme = THEMES[id as usize % THEMES.len()];
        let text =
            format!("{theme} signal node {id}: EV market analysis on cost, policy, and production");
        let mut node = Node::new(id, deterministic_embedding(&text, MODEL_ID, DIMS), text);
        node.metadata
            .insert("source".to_string(), format!("seed/{theme}-{id}.md"));
        node.metadata.insert(
            "entity_type".to_string(),
            if id % 2 == 0 { "Company" } else { "Policy" }.to_string(),
        );
        node.metadata.insert(
            "timestamp".to_string(),
            format!("2025-{:02}-{:02}", (id % 12) + 1, (id % 28) + 1),
        );
        node.metadata
            .insert("tenant".to_string(), "benchmark".to_string());
        repo.put_node(node).await.unwrap();
    }

    for id in 1..node_count {
        repo.put_edge(Edge::new(id, id + 1, "related_to", 1.0))
            .await
            .unwrap();

        if id + 8 <= node_count {
            repo.put_edge(Edge::new(id, id + 8, "same_theme", 0.8))
                .await
                .unwrap();
        }

        if id + 97 <= node_count {
            repo.put_edge(Edge::new(id, id + 97, "cross_signal", 0.45))
                .await
                .unwrap();
        }
    }
}

fn build_query_json(
    worker_id: usize,
    op: usize,
    mode_index: usize,
    top_k: usize,
    depth: u8,
) -> String {
    let theme = THEMES[(worker_id + op) % THEMES.len()];

    match mode_index {
        0 => format!(
            r#"{{"query":"{theme} battery supply chain risk","mode":"evidence","search_mode":"local","top_k":{top_k},"traversal":{{"depth":{depth}}}}}"#
        ),
        1 => format!(
            r#"{{"query":"global synthesis for {theme} market themes","mode":"answer","search_mode":"global","top_k":{top_k}}}"#
        ),
        2 => format!(
            r#"{{"query":"iterative drift for {theme} production constraints","mode":"evidence","search_mode":"drift","top_k":{top_k},"traversal":{{"depth":1}}}}"#
        ),
        _ => format!(
            r#"{{"query":"{theme} policy and company influence","mode":"answer","search_mode":"auto","top_k":{top_k},"traversal":{{"depth":{depth}}}}}"#
        ),
    }
}

fn select_mode_index(worker_id: usize, op: usize) -> usize {
    // Production-like mix: local 50%, global 15%, drift 10%, auto 25%.
    let bucket = (worker_id + op) % 20;
    if bucket < 10 {
        0
    } else if bucket < 13 {
        1
    } else if bucket < 15 {
        2
    } else {
        3
    }
}

async fn run_worker(
    engine: Arc<QueryEngine>,
    pipeline: Arc<IngestionPipeline>,
    worker_id: usize,
    config: WorkerRunConfig,
) -> WorkerStats {
    let mut stats = WorkerStats::default();

    for op in 0..config.warmup_ops {
        if op % config.write_every == 0 {
            let request = IngestionRequest::Text {
                content: format!("warmup worker={worker_id} op={op} EV benchmark text"),
                metadata: HashMap::from([
                    ("source".to_string(), format!("warmup/worker-{worker_id}")),
                    ("entity_type".to_string(), "Company".to_string()),
                    ("timestamp".to_string(), "2025-10-01".to_string()),
                ]),
                idempotency_key: Some(format!("warmup-{worker_id}-{op}")),
                model_id: Some(MODEL_ID.to_string()),
            };
            pipeline.ingest(request).await.unwrap();
        } else {
            let mode_index = select_mode_index(worker_id, op);
            let query_json =
                build_query_json(worker_id, op, mode_index, config.top_k, config.depth);
            let request = QueryRequest::parse_json(&query_json).unwrap();
            let _ = engine.execute(request).await.unwrap();
        }
    }

    for op in 0..config.measured_ops {
        if op % config.write_every == 0 {
            let request = IngestionRequest::Text {
                content: format!(
                    "measured worker={worker_id} op={op} GraphRAG runtime ingest event"
                ),
                metadata: HashMap::from([
                    (
                        "source".to_string(),
                        format!("measured/worker-{worker_id}-op-{op}.md"),
                    ),
                    ("entity_type".to_string(), "Company".to_string()),
                    (
                        "timestamp".to_string(),
                        format!("2025-11-{:02}", (op % 28) + 1),
                    ),
                ]),
                idempotency_key: Some(format!("measured-{worker_id}-{op}")),
                model_id: Some(MODEL_ID.to_string()),
            };

            let begin = Instant::now();
            pipeline.ingest(request).await.unwrap();
            stats.write_latencies_ns.push(begin.elapsed().as_nanos());
            stats.write_ops += 1;
        } else {
            let mode_index = select_mode_index(worker_id, op);
            let query_json =
                build_query_json(worker_id, op, mode_index, config.top_k, config.depth);
            let request = QueryRequest::parse_json(&query_json).unwrap();

            let begin = Instant::now();
            let response = engine.execute(request).await.unwrap();
            stats.read_latencies_ns.push(begin.elapsed().as_nanos());
            stats.read_ops += 1;
            let semantic_cache_hit = response
                .explain
                .steps
                .iter()
                .any(|step| step == "semantic_cache_hit");
            stats.quality.record(ReadObservation::new(
                response.answer.is_some(),
                response.evidence.nodes.len(),
                response.citations.len(),
                response.groundedness,
                semantic_cache_hit,
            ));

            match mode_index {
                0 => stats.local_reads += 1,
                1 => stats.global_reads += 1,
                2 => stats.drift_reads += 1,
                _ => stats.auto_reads += 1,
            }
        }
    }

    stats
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let config = load_config();

    let temp_dir = tempfile::tempdir().unwrap();
    let wal_path = temp_dir.path().join("graphrag_production_bench.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    seed_repository(&repo, config.seed_nodes).await;

    let summarizer = DeterministicSummarizer;
    let graph = repo.graph_index().await;
    let summaries = {
        let mut community_engine = CommunityEngine::new(graph);
        community_engine.rebuild_hierarchy(3, &summarizer);
        community_engine.summaries().to_vec()
    };

    let engine = Arc::new(QueryEngine::new(repo.clone()).with_community_summaries(summaries));
    let pipeline = Arc::new(IngestionPipeline::with_components(
        repo,
        Box::new(SemanticChunker::default()),
        Box::new(DeterministicEmbedder::new(DIMS)),
        Box::new(NoOpPolicy),
        MODEL_ID,
    ));

    let benchmark_start = Instant::now();
    let mut handles = Vec::with_capacity(config.workers);
    for worker_id in 0..config.workers {
        let engine = engine.clone();
        let pipeline = pipeline.clone();
        let worker_config = WorkerRunConfig {
            warmup_ops: config.warmup_ops_per_worker,
            measured_ops: config.measured_ops_per_worker,
            write_every: config.write_every,
            top_k: config.top_k,
            depth: config.traversal_depth,
        };

        handles.push(tokio::spawn(async move {
            run_worker(engine, pipeline, worker_id, worker_config).await
        }));
    }

    let mut combined = WorkerStats::default();
    for handle in handles {
        let stats = handle.await.unwrap();
        combined.merge(stats);
    }

    let elapsed = benchmark_start.elapsed();
    let total_ops = combined.read_ops + combined.write_ops;
    let throughput = if elapsed.as_secs_f64() > 0.0 {
        total_ops as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    let read_summary = build_latency_summary(&combined.read_latencies_ns);
    let write_summary = build_latency_summary(&combined.write_latencies_ns);

    let read_quality = combined.quality.summary();

    if let Some(limit) = config.min_throughput_ops {
        assert!(
            throughput >= limit,
            "throughput regression: {:.2} < {:.2} ops/s",
            throughput,
            limit
        );
    }
    if let Some(limit) = config.max_read_p95_ms {
        assert!(
            read_summary.p95_ms <= limit,
            "read p95 regression: {:.3} > {:.3} ms",
            read_summary.p95_ms,
            limit
        );
    }
    if let Some(limit) = config.max_write_p95_ms {
        assert!(
            write_summary.p95_ms <= limit,
            "write p95 regression: {:.3} > {:.3} ms",
            write_summary.p95_ms,
            limit
        );
    }

    let report = BenchmarkReport {
        benchmark: "graphrag_production_bench".to_string(),
        generated_at_unix: now_unix(),
        config: ReportConfig {
            seed_nodes: config.seed_nodes,
            workers: config.workers,
            warmup_ops_per_worker: config.warmup_ops_per_worker,
            measured_ops_per_worker: config.measured_ops_per_worker,
            write_every: config.write_every,
            read_to_write_ratio: format!("{}:1", config.write_every.saturating_sub(1)),
            top_k: config.top_k,
            traversal_depth: config.traversal_depth,
        },
        totals: Totals {
            elapsed_sec: elapsed.as_secs_f64(),
            total_ops,
            read_ops: combined.read_ops,
            write_ops: combined.write_ops,
            throughput_ops_per_sec: throughput,
        },
        read_latency_ns: read_summary,
        write_latency_ns: write_summary,
        read_quality,
        mode_mix: ModeMix {
            local: combined.local_reads,
            global: combined.global_reads,
            drift: combined.drift_reads,
            auto: combined.auto_reads,
        },
        query_engine_metrics: engine.metrics(),
    };

    write_json_report(Path::new(&config.results_path), &report);

    println!("=== GraphRAG Production-like Benchmark ===");
    println!(
        "config: nodes={}, workers={}, warmup_ops/worker={}, measured_ops/worker={}, write_every={}, top_k={}, depth={}",
        config.seed_nodes,
        config.workers,
        config.warmup_ops_per_worker,
        config.measured_ops_per_worker,
        config.write_every,
        config.top_k,
        config.traversal_depth
    );
    println!(
        "workload: total_ops={}, read_ops={}, write_ops={}, elapsed={:.3}s, throughput={:.2} ops/s",
        report.totals.total_ops,
        report.totals.read_ops,
        report.totals.write_ops,
        report.totals.elapsed_sec,
        report.totals.throughput_ops_per_sec
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
        "quality: answer_reads={}, avg_groundedness={:.4}, avg_answer_groundedness={:.4}, avg_evidence_nodes={:.2}, semantic_cache_hit_rate={:.4}, evidence_attachment_rate={:.4}, answer_with_evidence_rate={:.4}, answer_with_citations_rate={:.4}",
        report.read_quality.answer_reads,
        report.read_quality.avg_groundedness,
        report.read_quality.avg_answer_groundedness,
        report.read_quality.avg_evidence_nodes,
        report.read_quality.semantic_cache_hit_rate,
        report.read_quality.evidence_attachment_rate,
        report.read_quality.answer_with_evidence_rate,
        report.read_quality.answer_with_citations_rate
    );
    println!(
        "mode_mix: local={}, global={}, drift={}, auto={}",
        report.mode_mix.local, report.mode_mix.global, report.mode_mix.drift, report.mode_mix.auto
    );
    println!("result_json: {}", config.results_path);
}
