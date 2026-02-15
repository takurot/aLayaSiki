# Repository Guidelines

## Project Structure & Module Organization
This repository is a Rust workspace for an autonomous GraphRAG database prototype.

- `core/`: shared domain types, config, embedding helpers, ingest primitives.
- `storage/`: persistence and indexing (`wal`, `snapshot`, `repo`, `index/ann`, `index/graph`).
- `ingestion/`: ingest pipeline (`extract`, `chunker`, `embedding`, `processor`, `api`) plus integration/E2E tests.
- `query/`: query DSL, planner, execution engine, GraphRAG pipeline, and output-spec tests.
- `slm/`, `jobs/`: lightweight model registry/inference and background job orchestration.
- `prototypes/benches/`: Criterion-based Rust benchmarks.
- `benchmarks/`: Python ANN benchmark scripts and baselines.
- `docs/`: product spec, plan, and ADRs.

## Build, Test, and Development Commands
- `cargo fmt --all -- --check`: enforce Rust formatting.
- `cargo clippy --workspace --all-targets -- -D warnings`: fail on lint warnings.
- `cargo test --workspace`: run all unit and integration tests.
- `cargo test -p ingestion --test e2e_pipeline_test -- --nocapture`: run ingest->query E2E tests.
- `cargo bench -p prototypes --bench operational_latency_bench`: run operational latency benchmark gate.
- `pip install -r benchmarks/requirements.txt && python benchmarks/ann_benchmark.py ...`: run Python ANN benchmark.

## Coding Style & Naming Conventions
- Rust 2021 edition across crates; keep code `rustfmt`-clean.
- Naming: files/modules/functions `snake_case`, types/traits `PascalCase`, constants `UPPER_SNAKE_CASE`.
- Prefer small, crate-local modules with explicit boundaries; put shared contracts in `core`.
- Use `thiserror` for typed errors and keep error messages actionable.

## Testing Guidelines
- Keep unit tests close to source (`mod tests`), integration tests in `<crate>/tests/*_test.rs`.
- Use `#[tokio::test]` for async paths.
- Add regression tests with every bug fix, especially for query correctness and storage atomicity.
- Before opening a PR, run: fmt, clippy, `cargo test --workspace`, and relevant benchmark/test targets for touched areas.

## Commit & Pull Request Guidelines
- Follow Conventional Commit style seen in history: `feat(query): ...`, `fix(storage): ...`, `docs(plan): ...`, `chore: ...`.
- Use scoped, imperative commit subjects; keep each commit focused on one change.
- PRs should include: purpose, key design decisions, commands executed, and observed results.
- Link related issue/plan items; include benchmark artifacts or output samples when query/benchmark behavior changes.
