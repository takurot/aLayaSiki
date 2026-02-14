use std::sync::Arc;

use alayasiki_core::embedding::deterministic_embedding;
use alayasiki_core::model::{Edge, Node};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use storage::repo::Repository;
use tempfile::TempDir;
use tokio::runtime::Runtime;

const DIMS: usize = 32;
const MODEL_ID: &str = "embedding-default-v1";

fn build_repo(runtime: &Runtime, node_count: u64) -> (TempDir, Arc<Repository>) {
    runtime.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("storage_bench.wal");
        let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

        for id in 1..=node_count {
            let text = format!("node-{id} ev battery benchmark");
            let node = Node::new(id, deterministic_embedding(&text, MODEL_ID, DIMS), text);
            repo.put_node(node).await.unwrap();
        }

        for id in 1..node_count {
            repo.put_edge(Edge::new(id, id + 1, "connected_to", 1.0))
                .await
                .unwrap();
        }

        (dir, repo)
    })
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let (_tmpdir, repo) = build_repo(&runtime, 1_000);
    let query = deterministic_embedding("ev battery search", MODEL_ID, DIMS);

    let mut group = c.benchmark_group("storage_repo");
    group.sample_size(20);

    group.bench_function("vector_search_top20", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let index = repo.hyper_index.read().await;
                black_box(index.search_vector(black_box(&query), 20));
            });
        });
    });

    group.bench_function("graph_expand_2hop", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let index = repo.hyper_index.read().await;
                black_box(index.expand_graph(500, 2));
            });
        });
    });

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
