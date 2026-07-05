# Implementation Playbook (AI Agent)

Use this file as the default execution rules for implementing tasks/PRs in this repository.

## Inputs (What you are given)
- A **PR identifier** (e.g., **PR-01**), an **Issue number** (e.g., `#157`), or a request to implement a subset of tasks.
- The agent MUST autonomously complete the full loop (investigate → implement → test → review → CI → merge) for whatever input it receives.

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

### 6) Storage Tiering & CPU Fallback
- When GPU-First (`StorageTier::GpuVram`) is requested but the GPU runtime is unavailable or disabled, the engine **MUST** automatically fall back to CPU memory (`StorageTier::CpuMemory`) and disable zero-copy access.
- Always query and respect effective capabilities resolved from `StorageProfile` (i.e. `StorageCapabilities`) rather than assuming requested configurations are active.

---

## Standard Implementation Workflow

### 0) Pre‑flight
- **If an Issue number is given**, inspect it first and capture requirements, background, and acceptance criteria:
  ```bash
  gh issue view <number>
  ```
  - Identify related existing code, tests, and documentation by searching the repository.
  - Cross‑map each Issue acceptance criterion to a concrete test you will write.
- Read `docs/SPEC.md` and the relevant section(s) in `docs/PLAN.md`.
- Check `docs/adr/` for relevant architectural decisions.
- Clarify scope: what exactly is in/out for the requested PR/Issue/tasks.
- Create a short checklist of tasks you will complete (map to `docs/PLAN.md` / the Issue).
- Define explicit acceptance points from `docs/SPEC.md` (or the Issue body) for the scoped task, and verify implementation/test coverage against them.

### 1) Branching
- Branch off `main`.
- **PR‑driven**: `feature/<pr-id>-<short-description>`
  - Example: `feature/pr-02-storage-wal`
- **Issue‑driven**: `feature/issue-<number>-<short-description>`
  - Example: `feature/issue-157-benchmark-hermetic`

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
- **Select the appropriate skill(s) for the task** and load them via the skill tool before coding:
  - `tdd-workflow` — new features, bug fixes, refactors (test‑first).
  - `rust-testing` — Rust TDD with coverage (`cargo-llvm-cov`).
  - `rust-patterns` — idiomatic ownership/error/trait patterns.
  - `ai-regression-testing` — sandbox/API regression without DB deps.
  - `plan` — decompose complex implementations before writing code.
- **Red**: Write a failing test first (Rust unit/integration test).
- **Green**: Implement the minimum code to pass.
- **Refactor**: Clean up; maintain zero-copy and consistency rules.
- Cover unit, integration, and E2E tests for the change.

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
- For Issue‑driven work, include the trailer `Closes #<number>` in the PR body and reference the Issue in the title (e.g., `feat(storage): ... (Issue #157)`).
- Keep commits small and logically separated.

GitHub CLI:
```bash
git push -u origin <branch>
gh pr create --title "<PR-ID or Issue #N>: <Title>" \
  --body "Closes #<number>

## Summary
...
## Test plan
- [ ] ..."
gh pr checks
```

### 9) Sub‑agent Code Review
- Perform an automated code review using a sub‑agent (`general`/`explore`, or a dedicated review skill such as `rust-patterns`/`ai-regression-testing`).
- Post the review summary to the PR:
  ```bash
  gh pr comment <PR-number> --body "<review findings>"
  ```
- Address findings by severity:
  - **CRITICAL / HIGH**: must fix.
  - **MEDIUM**: fix where feasible.
  - **LOW**: note and move on.
- Commit and push fixes; re‑run the review if the diff changed materially.

### 10) CI Verification & Merge
- Poll CI until every check is green:
  ```bash
  gh pr checks <PR-number>     # watch until all pass
  gh run view                  # inspect failures
  ```
- On failure: root‑cause, fix locally, re‑run tests, and push again. Repeat until green.
- **Final verification (Issue‑driven)**: walk through each Issue acceptance criterion, attach evidence (test names, command output, benchmark numbers), and record the result in the PR body.
- When green and verified, merge:
  ```bash
  gh pr merge <PR-number> --squash --delete-branch
  ```
- If verification fails, loop back to step 3 (implement → test → review) until correct.

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
- [ ] **Issue requirements & acceptance criteria captured** (if Issue‑driven) via `gh issue view`.
- [ ] Implemented the requested tasks with minimal diffs.
- [ ] Added/updated Rust tests with deterministic assertions (unit + integration + E2E).
- [ ] Selected and used the appropriate skill(s) for the task.
- [ ] Verified zero-copy/validation paths are used (no raw `unsafe` without justification).
- [ ] All tests pass (`cargo test`).
- [ ] Benchmarks checked when performance-sensitive code changed.
- [ ] `cargo fmt` / `cargo clippy` are clean.
- [ ] Checked for gaps between `docs/SPEC.md` and the implementation in scope; if a future task exists, added a note in `docs/PLAN.md`; if not, added a new task in `docs/PLAN.md`.
- [ ] `docs/PLAN.md` updated with progress and notes.
- [ ] ADR created if architectural decision was made.
- [ ] Sub‑agent code review performed and findings addressed.
- [ ] CI is fully green (`gh pr checks`).
- [ ] **Issue acceptance criteria verified** with evidence in the PR body.
- [ ] PR merged.

---

## Agentic Workflow

1. **Consultation**: Refer to `CODEX_CLI.md` and discuss implementation details/strategy with `codex`.
2. **Implementation**: Create a feature branch and develop using TDD (Test-Driven Development).
3. **Benchmarking**: After implementation, refer to `README.md` to run benchmarks and save results in `benchmarks/`.
4. **Pull Request**: Commit and push changes, then create a PR.
5. **Code Review**: After creating the PR, refer to `CODEX_CLI.md` to request a code review from `codex`.

---

## Issue‑Driven Autonomous Execution

When invoked with an **Issue number** (e.g., "implement Issue #157"), the agent MUST autonomously run the entire loop below without further prompting, branching off `main` and looping on failures until the Issue can be closed.

### Invocation
```
Implement Issue #<number> following docs/PROMPT.md.
```

### Autonomous loop
1. **Investigate** — `gh issue view <number>`; read `docs/SPEC.md`, `docs/PLAN.md`, `docs/adr/`; search related code/tests.
2. **Branch** — `feature/issue-<number>-<short-description>` from `main`.
3. **Plan & skill‑up** — pick skills (`tdd-workflow`, `rust-testing`, `rust-patterns`, `ai-regression-testing`, `plan`) as needed; write a checklist mapped to Issue acceptance criteria.
4. **TDD** — Red → Green → Refactor (unit + integration + E2E).
5. **Quality gate** — `cargo fmt --all -- --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test`; benchmarks if perf‑sensitive.
6. **Commit & PR** — `Closes #<number>` in the PR body; conventional commits.
7. **Sub‑agent review** — post findings to the PR via `gh pr comment`; fix CRITICAL/HIGH.
8. **CI loop** — `gh pr checks <PR>` until green; fix and re‑push on failure.
9. **Verify** — walk each Issue acceptance criterion with evidence.
10. **Merge** — `gh pr merge <PR> --squash --delete-branch` once green and verified; otherwise loop back to step 4.

### Autonomy rules
- Do not stop on a red test or a failed CI check: root‑cause and fix until green.
- Do not request human input for routine decisions (naming, test placement, commit granularity) — follow this playbook.
- Only escalate when blocked by an ambiguous requirement in the Issue, a spec conflict, or a destructive operation not covered by the Safety & Security principle.
