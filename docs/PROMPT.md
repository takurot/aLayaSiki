# Implementation Playbook (AI Agent)

Use this file as the default execution rules for implementing tasks/PRs in this repository.

## Inputs (What you are given)
- A PR identifier (e.g., **PR-01**) or a request to implement a subset of tasks.

## Primary References (Always read these first)
- **Specification**: `docs/SPEC.md`
- **Implementation tasks**: `docs/PLAN.md`
- **Architecture Decision Records**: `docs/adr/`
- **This playbook (process & rules)**: `docs/PROMPT.md`

---

## Non‑Negotiable Project Principles

### 1) Zero‑Copy & Performance
- Use **`rkyv`** with `check_archived_root` for validated zero-copy deserialization.
- Never deep-clone large data structures (Node/Edge) unless absolutely necessary.
- Prefer **in-place updates** to indices over rebuild.

### 2) Reproducibility & Determinism
- Query results **MUST** be reproducible given identical `model_id` and `snapshot_id`.
- All stochastic components (SLM inference) **MUST** be logged with `model_id` for reproducibility.
- Event ordering **MUST** be deterministic; never rely on `HashMap` iteration order.

### 3) Graph + Vector Consistency
- The **Hyper-Index** (ANN + Graph Adjacency) **MUST** remain consistent after every mutation.
- Index updates are atomic; partial updates must not be observable by queries.

### 4) WAL‑First Durability
- All writes **MUST** be persisted to WAL before ACK.
- Crash recovery must restore state from WAL + last snapshot.

### 5) Safety & Security
- Do **not** delete untracked files without explicit permission (exception: macOS `._*` files).
- PII masking and policy hooks run **before** data is stored.
- Keep diffs focused; avoid unrelated refactors during feature work.

---

## Standard Implementation Workflow

### 0) Pre‑flight
- Read `docs/SPEC.md` and the relevant section(s) in `docs/PLAN.md`.
- Check `docs/adr/` for relevant architectural decisions.
- Clarify scope: what exactly is in/out for the requested PR/tasks.
- Create a short checklist of tasks you will complete (map to `docs/PLAN.md`).
- Define explicit acceptance points from `docs/SPEC.md` for the scoped task, and verify implementation/test coverage against them.

### 1) Branching
- Branch name: `feature/<pr-id>-<short-description>`
  - Example: `feature/pr-02-storage-wal`
- Branch off `main`.

### 2) Environment Setup (macOS/Linux)

#### Rust (Core Engine)
```bash
cargo --version
rustc --version
cargo build
```

#### Python (Benchmarks / SDK, if present)
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r benchmarks/requirements.txt
```

### 3) TDD (Test‑Driven Development)
- **Red**: Write a failing test first (Rust unit/integration test).
- **Green**: Implement the minimum code to pass.
- **Refactor**: Clean up; maintain zero-copy and consistency rules.

Use the test naming/layout conventions in `docs/PLAN.md`.

### 4) Run Tests (Frequently)
```bash
cargo test
```

For Python benchmarks (if present):
```bash
source benchmarks/.venv/bin/activate
python benchmarks/ann_benchmark.py
```

### 5) Benchmarks (When relevant)
- Rust Criterion benchmarks:
```bash
cargo bench
```

- Python ANN/Graph benchmarks:
```bash
python benchmarks/ann_benchmark.py
```

Compare against baseline and document results in `benchmarks/` or ADRs.

### 6) Code Quality
```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
```

Python (if present):
```bash
ruff check benchmarks/
ruff format benchmarks/
```

### 7) Update Documentation
- Update `docs/PLAN.md` checkboxes and add notes for follow‑ups/risks.
- If there is a gap between `docs/SPEC.md` and implementation: add a note under the relevant future task in `docs/PLAN.md`; if no such task exists, add a new task in `docs/PLAN.md`.
- Create/update `docs/adr/` if making architectural decisions.
- Only update `docs/SPEC.md` if changing the intended behavior/contract.

### 8) Commits & PR
- Commit message format: `<type>(<scope>): <description>`
  - Types: `feat`, `fix`, `test`, `docs`, `refactor`, `chore`, `ci`
  - Example: `feat(storage): implement WAL append and recovery`
- Keep commits small and logically separated.

GitHub CLI:
```bash
gh pr create --title "<PR-ID>: <Title>" --body "<Description>"
gh pr checks
```

---

## Key Data Structures

### Node (Entity)
```rust
pub struct Node {
    pub id: u64,
    pub embedding: Vec<f32>,
    pub metadata: String, // JSON
}
```

### Edge (Relation)
```rust
pub struct Edge {
    pub source: u64,
    pub target: u64,
    pub relation_type: u8,
    pub weight: f32,
}
```

Use `rkyv` derives with `#[archive(check_bytes)]` for safe zero-copy access.

---

## Checklist (Before you call a PR "done")
- [ ] Implemented the requested tasks with minimal diffs.
- [ ] Added/updated Rust tests with deterministic assertions.
- [ ] Verified zero-copy/validation paths are used (no raw `unsafe` without justification).
- [ ] All tests pass (`cargo test`).
- [ ] Benchmarks checked when performance-sensitive code changed.
- [ ] `cargo fmt` / `cargo clippy` are clean.
- [ ] Checked for gaps between `docs/SPEC.md` and the implementation in scope; if a future task exists, added a note in `docs/PLAN.md`; if not, added a new task in `docs/PLAN.md`.
- [ ] `docs/PLAN.md` updated with progress and notes.
- [ ] ADR created if architectural decision was made.

---

## Agentic Workflow

1. **Consultation**: Refer to `CODEX_CLI.md` and discuss implementation details/strategy with `codex`.
2. **Implementation**: Create a feature branch and develop using TDD (Test-Driven Development).
3. **Benchmarking**: After implementation, refer to `README.md` to run benchmarks and save results in `benchmarks/`.
4. **Pull Request**: Commit and push changes, then create a PR.
5. **Code Review**: After creating the PR, refer to `CODEX_CLI.md` to request a code review from `codex`.
