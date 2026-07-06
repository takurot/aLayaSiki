# aLayaSiki

**Autonomous GraphRAG Database** - "Insert Raw, Retrieve Reasoned"

aLayaSiki is a next-generation AI-native database designed to eliminate the need for complex ETL pipelines and custom RAG implementations. It autonomously structures unstructured data (PDFs, text, etc.) into a knowledge graph while generating vector embeddings, enabling high-precision, reasoned retrieval with minimal latency.

## Core Concept

**Insert Raw, Retrieve Reasoned.**
Developers simply ingest raw files. The database handles:
1.  **Auto-Chunking & Embedding**: Dynamic segmentation and vectorization.
2.  **Auto-Graph Construction**: Real-time extraction of entities and relations using embedded SLMs.
3.  **GraphRAG Inference**: Multi-hop reasoning (Vector Search -> Graph Expansion -> Context Pruning) within the database engine.

## Key Features

*   **Neural-Storage Engine**: Compute and storage integration with a GPU-first storage profile and explicit CPU fallback path.
*   **Vector-Graph Hybrid Model**: Co-located ANN index and graph adjacency for O(1) cross-reference.
*   **Embedded SLM**: Lightweight models resident on shards for autonomous data processing.
*   **Feasibility & Scalability**: Designed for 100M+ nodes with sub-second retrieval latency.

## Documentation

*   [Product Specification](docs/SPEC.md)
*   [Implementation Plan](docs/PLAN.md)

## Status

**Pre-Alpha / CPU-first foundation with GPU-first roadmap**
Current runtime paths are CPU-based. The repository now exposes a GPU-first storage profile abstraction, but GPUDirect Storage and VRAM-resident persistence remain staged follow-up work tracked from Issue #51.

## Test Coverage

CI reports per-PR workspace coverage using [`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov) (report-only at this stage; an enforcement threshold will follow once the baseline is triaged — see Issue #65). HTML and LCOV artifacts are attached to every CI run.

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

## License

[TBD]
