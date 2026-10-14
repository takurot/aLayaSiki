# ADR 0003: Runtime-owned snapshot-scoped community summaries

## Status
Accepted

## Decision
The standard runtime owns a trusted CommunitySummaryProvider. QueryEngine retains
its static summary injection API for low-level callers and tests. Production global
queries materialize an immutable SnapshotView before asking the provider, ensuring
summary inputs and evidence share the resolved durable LSN.

Startup rebuilds the latest version deterministically. Mutations invalidate by WAL
LSN, including direct repository writes and background extraction. CommunityEngine
rebuilds on demand; a bounded 32-entry cache is keyed by LSN and optional tenant.
Summaries carry an exact LSN range. Historical cache misses reconstruct from WAL
and snapshots, so process restarts do not require separate summary persistence.

Tenant ownership is applied to nodes and both edge endpoints before clustering.
Entity/time filters and live retention metadata use filtered evidence fallback until
request-scoped summary generation is implemented. Live provider global results
bypass semantic response caching to respect expiry at unchanged LSN.

## Consequences
This provides one reproducible lifecycle without per-query Leiden recomputation.
Summary text is a bounded deterministic excerpt, not neural summarization. First
queries for a version still incur snapshot reconstruction and full community rebuild;
incremental refresh and snapshot-view caching are future optimizations. Retention
and filter constrained requests remain safe but do not use community synthesis.
