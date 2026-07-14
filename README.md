# aLayaSiki

**Autonomous GraphRAG Database** — "Insert Raw, Retrieve Reasoned"

aLayaSiki is a next-generation AI-native database designed to eliminate the need
for complex ETL pipelines and custom RAG implementations. It autonomously
structures unstructured data (PDFs, text, etc.) into a knowledge graph while
generating vector embeddings, enabling high-precision, reasoned retrieval with
minimal latency.

> **Status:** Pre-Alpha / CPU-first foundation with a GPU-first roadmap.
> Current runtime paths are CPU-based. The repository exposes a GPU-first
> storage profile abstraction, but GPUDirect Storage and VRAM-resident
> persistence remain staged follow-up work (tracked from Issue #51).

## Core Concept

**Insert Raw, Retrieve Reasoned.**
Developers simply ingest raw files. The database handles:

1. **Auto-Chunking & Embedding** — Dynamic segmentation and vectorization.
2. **Auto-Graph Construction** — Real-time extraction of entities and relations
   using embedded SLMs.
3. **GraphRAG Inference** — Multi-hop reasoning
   (Vector Search → Graph Expansion → Context Pruning) inside the engine.

## Key Features

- **Neural-Storage Engine** — Compute and storage integration with a GPU-first
  storage profile and an explicit CPU fallback path.
- **Vector-Graph Hybrid Model** — Co-located ANN index and graph adjacency for
  O(1) cross-reference.
- **Embedded SLM** — Lightweight models resident on shards for autonomous data
  processing.
- **Durable Job Queue** — WAL-backed ingestion/retry pipeline with a dead-letter
  store for resilient async work.
- **Feasibility & Scalability** — Designed for 100M+ nodes with sub-second
  retrieval latency.

## Repository Layout

This is a Rust workspace (edition 2021) plus a Python benchmark suite and a web
dashboard.

| Path | Language | Responsibility |
| --- | --- | --- |
| `core/` | Rust | Shared domain types, config, embedding helpers, ingest primitives. |
| `storage/` | Rust | Persistence & indexing: `wal`, `snapshot`, `repo`, `index/ann`, `index/graph`. |
| `ingestion/` | Rust | Ingest pipeline: `extract`, `chunker`, `embedding`, `processor`, `api` + E2E tests. |
| `query/` | Rust | Query DSL, planner, execution engine, GraphRAG pipeline. |
| `slm/` | Rust | Lightweight model registry/inference. |
| `jobs/` | Rust | Background job orchestration (durable WAL-backed queue). |
| `sdk/` | Rust | Client SDK with examples and integration tests. |
| `prototypes/` | Rust | Criterion-based benches (`operational_latency_bench`, `graphrag_production_bench`, `storage_bench`). |
| `benchmarks/` | Python | ANN benchmark scripts, baselines, and result artifacts. |
| `ui/` | TypeScript | React + Vite dashboard. |
| `docs/` | Markdown | Product spec (`SPEC.md`), plan (`PLAN.md`), research, evaluation, and ADRs. |

## Build, Test & Benchmarks

Rust formatting and linting are enforced in CI:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test -p ingestion --test e2e_pipeline_test -- --nocapture   # ingest -> query E2E
```

### Benchmark suite

`benchmarks/benchmark_suite.py` runs the PR-14 benchmark set (Rust criterion
benches + a Python ANN benchmark) and writes normalized JSON/Markdown artifacts
into `benchmarks/results/`. Past runs are preserved under timestamped
`benchmarks/results_archive_*/` directories.

```sh
python3 -m venv .venv-benchmarks
.venv-benchmarks/bin/pip install -r benchmarks/requirements.txt

# PR-14 baseline suite (operational latency + GraphRAG + ANN)
.venv-benchmarks/bin/python benchmarks/benchmark_suite.py --profile baseline

# Larger manual profile
.venv-benchmarks/bin/python benchmarks/benchmark_suite.py --profile scale

# PR-14.6 operational matrix (flush policy / scale / worker sweeps)
.venv-benchmarks/bin/python benchmarks/benchmark_suite.py --mode pr14-6-operational
```

## Latest Benchmark Results

Captured on the `baseline` profile (10k samples / 128 dims / 100 queries,
4,000 graph nodes, 6 workers). Design target: 100M nodes / 300M edges. Full
artifacts live in [`benchmarks/results/`](benchmarks/results).

### PR-14 Baseline Suite

| Workload | Throughput (ops/s) | Read p95 (ms) | Write p95 (ms) |
| --- | ---: | ---: | ---: |
| Operational latency | **1114.22** | 10.18 | 40.69 |
| GraphRAG production | **680.01** | 23.25 | 33.58 |

- GraphRAG quality: average groundedness `0.6032`, evidence attachment rate
  `1.0000`, answer-with-evidence rate `1.0000`.
- ANN search (top-10 over 10k×128): `usearch` `0.0038` s, `faiss_flat` `0.0157` s.

### PR-14.6 Operational Matrix (WAL flush, scale, worker sweeps)

WAL **batch(32)** flush delivers a **+645%** throughput gain over `always`
flush while cutting read p95 by ~152 ms — the dominant lever for write-heavy
mixed workloads.

| Scenario | Throughput (ops/s) | Read p95 (ms) | Write p95 (ms) |
| --- | ---: | ---: | ---: |
| flush: always (100k, 8w) | 91.92 | 176.66 | 469.56 (durable) |
| flush: batch(32) (100k, 8w) | **684.92** | 24.41 | 137.20 |
| scale: 1M nodes (batch:32) | 53.94 | 255.78 | 1560.09 |
| workers: 32 (batch:32) | 433.18 | 350.21 | 475.68 |
| workers: 128 (batch:32) | 243.24 | 230.63 | 5786.22 |

> Write-latency `submit_only` figures are not directly comparable to the
> `durable` scope of the `always` baseline (see matrix notes). The matrix also
> shows that contention rises sharply past 8 workers on the current scheduler.

## Continuous Integration

CI (`.github/workflows/ci.yml`) runs on every PR across Ubuntu and macOS:

- **Rust quality** — `cargo fmt --check` + `cargo clippy -D warnings`.
- **Cargo audit** — security advisory scanning.
- **Build & tests** — `cargo build --workspace` / `cargo test --workspace`.
- **Coverage** — per-PR workspace coverage via `cargo-llvm-cov` (report-only;
  enforcement threshold to follow — Issue #65).
- **E2E** — ingest → query pipeline test.
- **ANN recall gate** — HNSW recall@k vs. linear ground-truth.
- **Benchmark gates** — operational latency + GraphRAG p95 thresholds.
- **Python ANN** — ANN benchmark with baseline regression check.
- **UI quality** — dashboard build + lint.

## Test Coverage

CI reports per-PR workspace coverage using
[`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov) (report-only at
this stage; an enforcement threshold will follow once the baseline is triaged —
see Issue #65). HTML and LCOV artifacts are attached to every CI run.

To generate the same coverage report locally:

```sh
cargo install cargo-llvm-cov   # one-time install
# Run tests once, then emit both report formats from the same profile data:
cargo llvm-cov --workspace --no-report
cargo llvm-cov report --lcov --output-path lcov.info
cargo llvm-cov report --html --output-dir coverage
open coverage/html/index.html   # macOS; use xdg-open on Linux
```

The summary table can be printed without writing files:

```sh
cargo llvm-cov --workspace --no-report
cargo llvm-cov report          # prints the per-file coverage table
```

## Documentation

- [Product Specification](docs/SPEC.md)
- [Implementation Plan](docs/PLAN.md)
- [Research Notes](docs/RESEARCH.md)
- [Evaluation 2026-06](docs/EVALUATION_2026-06.md)
- [Architecture Decision Records](docs/adr/)

## License

[TBD]
