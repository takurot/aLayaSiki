# aLayaSiki - QWEN.md

## Project Overview

**aLayaSiki** (アラヤシキ) is an **Autonomous GraphRAG Database** with the core concept: *"Insert Raw, Retrieve Reasoned"*. It eliminates the need for complex ETL pipelines and custom RAG implementations by autonomously structuring unstructured data (PDFs, text, etc.) into a knowledge graph while generating vector embeddings.

### Core Architecture

| Component | Description |
|-----------|-------------|
| **Neural-Storage Engine** | Compute/storage integration with GPU-first persistence for zero-copy access |
| **Vector-Graph Hybrid Model** | Co-located ANN index and graph adjacency for O(1) cross-reference |
| **Embedded SLM** | Lightweight models resident on shards for autonomous data processing |
| **GraphRAG Inference** | Multi-hop reasoning (Vector Search → Graph Expansion → Context Pruning) |

### Technology Stack

- **Language:** Rust 2021 edition
- **Serialization:** `rkyv` (zero-copy deserialization)
- **Async Runtime:** `tokio`
- **ANN Index:** HNSW (usearch/hnswlib) - validated in feasibility spikes
- **Graph Algorithm:** Leiden (community detection)

## Project Structure

```
aLayaSiki/
├── core/           # Shared domain types, config, embedding helpers, ingest primitives
├── storage/        # Persistence & indexing (wal, snapshot, repo, index/ann, index/graph)
├── ingestion/      # Ingest pipeline (extract, chunker, embedding, processor, api)
├── query/          # Query DSL, planner, execution engine, GraphRAG pipeline
├── slm/            # Lightweight model registry/inference
├── jobs/           # Background job orchestration
├── prototypes/     # Criterion-based Rust benchmarks
├── benchmarks/     # Python ANN benchmark scripts and baselines
└── docs/           # Product spec, plan, and ADRs
```

### Workspace Members (Cargo.toml)

```toml
[workspace]
members = ["core", "storage", "ingestion", "slm", "jobs", "query", "prototypes"]
resolver = "2"
```

## Building and Running

### Prerequisites

- Rust toolchain (rustc, cargo)
- Python 3.x (for benchmarks)

### Build Commands

```bash
# Check formatting
cargo fmt --all -- --check

# Lint (fail on warnings)
cargo clippy --workspace --all-targets -- -D warnings

# Run all tests
cargo test --workspace

# Run specific integration tests
cargo test -p ingestion --test e2e_pipeline_test -- --nocapture

# Run benchmarks
cargo bench -p prototypes --bench operational_latency_bench
```

### Python Benchmarks

```bash
pip install -r benchmarks/requirements.txt
python benchmarks/ann_benchmark.py
```

## Development Conventions

### Naming Conventions

| Type | Convention | Example |
|------|------------|---------|
| Files/Modules | `snake_case` | `hyper_index.rs` |
| Functions | `snake_case` | `append_wal_entry` |
| Types/Traits | `PascalCase` | `HyperIndex`, `NodeResolver` |
| Constants | `UPPER_SNAKE_CASE` | `MAX_NODE_COUNT` |

### Coding Style

- Use `thiserror` for typed errors with actionable messages
- Keep modules small and crate-local with explicit boundaries
- Put shared contracts in `core` crate
- Prefer validated zero-copy access with `rkyv::check_archived_root`
- Never deep-clone large data structures unless necessary

### Testing Practices

- Unit tests: `mod tests` close to source
- Integration tests: `<crate>/tests/*_test.rs`
- Use `#[tokio::test]` for async tests
- Add regression tests with every bug fix
- Ensure deterministic assertions (no HashMap iteration order dependency)

### Commit Guidelines

Follow Conventional Commits format:

```
<type>(<scope>): <description>
```

**Types:** `feat`, `fix`, `test`, `docs`, `refactor`, `chore`, `ci`

**Examples:**
```
feat(storage): implement WAL append and recovery
fix(query): correct graph expansion logic in multi-hop
docs(plan): update PR-04 task checklist
```

### Branch Naming

```
feature/<pr-id>-<short-description>
# Example: feature/pr-02-storage-wal
```

## Key Design Principles

### 1. Zero-Copy & Performance
- Use `rkyv` with `check_archived_root` for validated zero-copy deserialization
- Avoid deep-cloning Node/Edge structures
- Prefer in-place updates to indices over rebuild

### 2. Reproducibility & Determinism
- Query results MUST be reproducible given identical `model_id` and `snapshot_id`
- Log all stochastic components (SLM inference) with `model_id`
- Never rely on `HashMap` iteration order for event ordering

### 3. Graph + Vector Consistency
- Hyper-Index (ANN + Graph Adjacency) MUST remain consistent after mutations
- Index updates are atomic; partial updates must not be observable

### 4. WAL-First Durability
- All writes MUST be persisted to WAL before ACK
- Crash recovery must restore state from WAL + last snapshot

### 5. Safety & Security
- Do not delete untracked files without explicit permission
- PII masking and policy hooks run before data storage

## Documentation References

| Document | Description |
|----------|-------------|
| `docs/SPEC.md` | Product Requirements Document (PRD) - functional specifications |
| `docs/PLAN.md` | Implementation plan broken down by PR tasks |
| `docs/PROMPT.md` | Implementation playbook for AI agents |
| `docs/adr/` | Architecture Decision Records |
| `AGENTS.md` | Repository guidelines and development workflow |

## Current Status

**Pre-Alpha / Feasibility Spike Phase (PR-00)**

Completed feasibility validations:
- [x] ANN index evaluation (usearch/hnswlib)
- [x] Graph adjacency storage prototype
- [x] GPU memory residency concept validation
- [x] SLM inference pipeline prototype (async job model)
- [x] ADR documentation for technology stack

## Core Data Structures

### Node (Entity)
```rust
pub struct Node {
    pub id: u64,
    pub embedding: Vec<f32>,
    pub metadata: String, // JSON
    // + provenance, confidence, model_id
}
```

### Edge (Relation)
```rust
pub struct Edge {
    pub source: u64,
    pub target: u64,
    pub relation_type: u8,
    pub weight: f32,
    // + direction, provenance, confidence
}
```

All structures use `rkyv` derives with `#[archive(check_bytes)]` for safe zero-copy access.

## Query Flow (GraphRAG)

```
1. Vector Search (ANN) → Initial candidate nodes
2. Graph Expansion → Multi-hop neighbor traversal (2-3 hops)
3. Context Pruning → Relevance filtering
4. Response Generation → Evidence-backed output
```

Target latency: Sub-second for retrieval-only, few seconds for 2-3 hop GraphRAG.
