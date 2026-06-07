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

## License

[TBD]
