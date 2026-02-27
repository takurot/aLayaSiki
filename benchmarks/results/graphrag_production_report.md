# GraphRAG Production-like Benchmark Report

## Overview
- Benchmark: `prototypes/benches/graphrag_production_bench.rs`
- Generated at (unix): `1772180838`
- Result JSON: `benchmarks/results/graphrag_production_latest.json`

## Execution Command
```bash
ALAYASIKI_GRAPHRAG_SEED_NODES=4000 \
ALAYASIKI_GRAPHRAG_WORKERS=6 \
ALAYASIKI_GRAPHRAG_WARMUP_OPS=20 \
ALAYASIKI_GRAPHRAG_MEASURED_OPS=100 \
cargo bench -p prototypes --bench graphrag_production_bench
```

## Workload Profile
- Seed nodes: `4000`
- Workers: `6`
- Warmup ops/worker: `20`
- Measured ops/worker: `100`
- Read:Write: `9:1` (`write_every=10`)
- Query mode mix target: Local `50%`, Global `15%`, Drift `10%`, Auto `25%`
- Traversal depth: `2`
- `top_k`: `24`

## Results
- Total ops: `600` (read `540`, write `60`)
- Elapsed: `2.5409 s`
- Throughput: `236.14 ops/s`

### Read latency
- p50: `10.137 ms`
- p95: `26.820 ms`
- p99: `31.768 ms`

### Write latency
- p50: `69.338 ms`
- p95: `137.278 ms`
- p99: `147.155 ms`

### Read quality / behavior
- Average groundedness: `0.5933`
- Average evidence nodes: `24.00`
- Semantic cache hit rate: `0.3759`
- Mode mix (actual): local `270`, global `75`, drift `50`, auto `145`

### Engine metrics snapshot
- total_queries: `648`
- hit_rate: `0.3657`
- p50/p95/p99 (engine internal, microseconds): `10915 / 26798 / 31705`

## Notes
- This run uses a medium-scale profile intended to be closer to practical operation while remaining executable in local CI-like environments.
- The benchmark is designed to scale via environment variables to approach production loads (e.g., `seed_nodes`, `workers`, `measured_ops_per_worker`).
- For regression gating, set thresholds using:
  - `ALAYASIKI_GRAPHRAG_MIN_THROUGHPUT`
  - `ALAYASIKI_GRAPHRAG_MAX_READ_P95_MS`
  - `ALAYASIKI_GRAPHRAG_MAX_WRITE_P95_MS`
