---
name: start-task
description: Guides the agent on how to start and execute a development task in the aLayaSiki (Autonomous GraphRAG Database) project following strict repository rules. Use this when the user asks you to start a task, pick up the next task, or proceed with development.
---

# Start Task Skill

This skill defines the standard operating procedure for starting and completing a development task in the **aLayaSiki** (自律型グラフRAGデータベース) repository. It ensures that the implementation playbook (`docs/PROMPT.md`), coding conventions (`AGENTS.md`), product specification (`docs/SPEC.md`), and PR rules are strictly followed from start to finish.

## When to use this skill

- When the user asks you to "start a task", "pick up the next task", "implement the next phase", or "continue with development".
- When you are tasked with selecting the next valid task/PR from `docs/PLAN.md` to work on.

## Strategy: Explore -> Plan -> Implement -> Commit

Follow these steps sequentially to execute a task:

### 1. Explore & Plan

Do not jump straight to coding. Build the necessary context first:

1. **Read the Rules**: Review `docs/PROMPT.md` (Implementation Playbook) carefully. This document contains absolute rules for branching, TDD, testing, code quality, and the "done" checklist. You MUST adhere to them.
2. **Read Coding Conventions**: Review `AGENTS.md` for project structure, build/test commands, coding style, naming conventions, and commit/PR guidelines.
3. **Review Specifications**: Check both `docs/PLAN.md` and `docs/SPEC.md`. Understand the current project phase and PR dependencies from `PLAN.md`.
4. **Check ADRs**: Review `docs/adr/` for relevant Architecture Decision Records that affect the task.
5. **Select the Task**: Identify the next logical task/PR to implement that has not been completed yet (e.g., `PR-13.5`), based on `docs/PLAN.md`. Verify its dependencies (marked with "Depends on") are already completed (all items `[x]`).
6. **Acquire Code Context**: Explore the existing codebase to understand the areas your selected task will modify. The workspace crates are:
   - `core/` — shared domain types, config, embedding helpers, ingest primitives
   - `storage/` — persistence and indexing (WAL, snapshot, repo, ANN index, graph index)
   - `ingestion/` — ingest pipeline (extract, chunker, embedding, processor, API)
   - `query/` — query DSL, planner, execution engine, GraphRAG pipeline
   - `slm/` — lightweight model registry and inference
   - `jobs/` — background job orchestration
   - `sdk/` — client SDK and framework integrations (LlamaIndex, LangChain)
   - `prototypes/` — Criterion-based Rust benchmarks
   - `benchmarks/` — Python ANN benchmark scripts and baselines
7. **Create Implementation Plan**: If the task scope is non-trivial or modifies multiple crates, create an `implementation_plan.md` artifact and ask the user for verification before proceeding.

### 2. Implement & Verify

Ensure you have a reliable way to verify your work:

1. **Create Branch**: Check out a new branch from the latest `main`. The branch name must follow the convention:
   ```
   feature/<pr-id>-<short-description>
   ```
   Example: `feature/pr-13.5-graph-explorer-ui`

2. **Test-Driven Development (TDD)**: Perform strict TDD (Red -> Green -> Refactor) as defined in `docs/PROMPT.md`:
   - **Red**: Write a failing test first (Rust unit test in `mod tests` or integration test in `<crate>/tests/*_test.rs`).
   - **Green**: Implement the minimum code to pass.
   - **Refactor**: Clean up while maintaining zero-copy, consistency, and WAL-first rules.

3. **Write and Execute Code**: Write your implementation in small, incremental steps. Follow these non-negotiable principles from `docs/PROMPT.md`:
   - Use `rkyv` with `check_archived_root` for validated zero-copy deserialization.
   - All writes MUST be persisted to WAL before ACK.
   - Hyper-Index (ANN + Graph Adjacency) MUST remain consistent after every mutation.
   - Query results MUST be reproducible given identical `model_id` and `snapshot_id`.
   - Use `thiserror` for typed errors and keep error messages actionable.

4. **Local Quality Gates**: Ensure your code passes all mandatory quality checks:
   ```bash
   # Formatting
   cargo fmt --all -- --check

   # Linting
   cargo clippy --workspace --all-targets -- -D warnings

   # All tests
   cargo test --workspace

   # E2E tests (if touching ingestion/query)
   cargo test -p ingestion --test e2e_pipeline_test -- --nocapture

   # Benchmarks (if touching performance-sensitive code)
   cargo bench -p prototypes --bench operational_latency_bench
   ```

### 3. Commit, PR & Document Update

1. **Update Docs**:
   - Mark the completed task with `[x]` in `docs/PLAN.md`. Add any follow-up notes.
   - If there is a gap between `docs/SPEC.md` and implementation: add a note under the relevant future task in `docs/PLAN.md`; if no such task exists, add a new one.
   - Create/update `docs/adr/` if making architectural decisions.
   - Only update `docs/SPEC.md` if changing the intended behavior/contract.

2. **Commit**: Ensure any new files are added with `git add`. Use Conventional Commits for your commit messages:
   ```
   <type>(<scope>): <description>
   ```
   - Types: `feat`, `fix`, `test`, `docs`, `refactor`, `chore`, `ci`
   - Scopes: `core`, `storage`, `ingestion`, `query`, `slm`, `jobs`, `sdk`, `prototypes`, `benchmarks`, `plan`
   - Example: `feat(storage): implement WAL append and recovery`
   - Keep commits small and logically separated.

3. **Push**: Push your created branch to the remote repository:
   ```bash
   git push origin feature/<pr-id>-<short-description>
   ```

4. **Create PR**: Open a Pull Request using the GitHub CLI:
   ```bash
   gh pr create --title "<PR-ID>: <Title>" --body "<Description>"
   ```
   PRs should include: purpose, key design decisions, commands executed, and observed results.

5. **Code Review**: After creating the PR, refer to the `codex` skill (`CODEX_CLI.md`) to request a code review.

6. **Monitor CI**: Ensure CI passes (`gh pr checks`) and fix any issues automatically.
