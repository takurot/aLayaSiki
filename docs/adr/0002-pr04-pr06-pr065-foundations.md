# ADR 0002: PR-04 / PR-06 / PR-06.5 Foundation Decisions

## Status
Accepted

## Context
To complete remaining PR-04 and implement PR-06/PR-06.5 tasks, we needed clear boundaries for:
- Atomic index updates and rollback behavior.
- Reproducible SLM extraction (`model_id` / `snapshot_id`) and model lifecycle.
- Community-centric GraphRAG foundations (community detection, hierarchy, summary, incremental refresh).

## Decisions

### 1. Repository Index Transaction Boundary
- Added `Repository::apply_index_transaction(Vec<IndexMutation>)`.
- Transaction flow: validate -> WAL append/flush -> in-memory apply.
- Validation rejects edge insertion when source/target nodes are not visible in the same transaction projection.
- `put_node`, `put_edge`, `delete_node` now run through this boundary.

### 2. SLM Registry and Versioned Resolution
- Added `slm::registry::ModelRegistry` with:
  - model/version registration
  - active version switching
  - rollback to previous activation
  - model ref resolution with optional `@version`
- Worker resolves extraction model via registry and can fall back to a configured default model.

### 3. Reproducibility and Fail-Safe in Lazy Graph Construction
- Job payload now carries `model_id` and `snapshot_id`.
- Ingestion fixes these values at enqueue time; `snapshot_id` is derived from WAL LSN (`wal-lsn-*`).
- Worker annotates extracted artifacts with `extraction_model_id` and `snapshot_id`.
- Extraction failures remain non-fatal to ingestion; vectorized ingestion path continues.

### 4. Lightweight Extraction Model Integration
- Added lightweight extractors:
  - `triplex-lite`
  - `glm-4-flash-lite`
- Registered as default lightweight model set in `slm::lightweight`.

### 5. Community Engine for PR-06.5
- Added `storage::community::CommunityEngine` with:
  - Leiden-like local move + connectivity refinement
  - hierarchical community rebuild
  - deterministic summarizer interface (`CommunitySummarizer`)
  - FastGraphRAG top-10% node selection via PageRank
  - incremental refresh entrypoint (`add_edge_incremental` + `refresh_incremental`)

## Consequences
- Index mutation behavior is now explicitly atomic at repository API level.
- Reproducibility metadata is carried end-to-end through lazy extraction jobs.
- Community/summary layer is available for upcoming global search and GraphRAG planning work.
