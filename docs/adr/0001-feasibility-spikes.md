# ADR 0001: Initial Technology Stack & Feasibility

## Status
Accepted

## Context
We need to validate the feasibility of aLayaSiki's core claims: "Zero-Copy Access" and embedded high-performance indexing.
We evaluated potential languages and libraries for the core engine.

## Decisions

### 1. Core Language: Rust
**Decision:** Use Rust for the storage engine and core logic.
**Reasoning:** 
- `rkyv` library enables true zero-copy deserialization (validated in prototypes).
- Memory safety without GC pauses is critical for consistent sub-second latency.
- Excellent FFI support for Python (future SDK).

### 2. ANN Indexing: Hybrid Approach
**Decision:** 
- Use **HNSW (via usearch/hnswlib)** for in-memory hot index.
- Use **DiskANN-like** structure for larger-than-memory tiers (future).
- Why `usearch`? Simple API, header-only C++ core, easy Rust/Python bindings.
- `faiss` is powerful but heavy (OpenMP dependency, complex build).

### 3. Serialization: rkyv
**Decision:** Use `rkyv` for node/edge serialization.
**Reasoning:**
- Benchmarks/Tests confirm zero parsing cost for accessing fields.
- Mapping a file into memory and casting it to a struct is safe and instant.

## Consequences
- Team needs Rust expertise.
- Building the Graph structure on top of `rkyv` requires careful layout design (e.g., relative pointers for adjacency lists in a single buffer).
