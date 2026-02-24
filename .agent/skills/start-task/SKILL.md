---
name: start-task
description: Guides the agent on how to start and execute a development task in the aLayaSiki Rust workspace following repository rules.
metadata:
  owner: user
  version: "1.0.0"
---

# Start Task Skill

This skill defines the standard operating procedure for starting and completing a task in the `aLayaSiki` repository.
The workflow must follow `docs/PROMPT.md`, `docs/PLAN.md`, and `docs/SPEC.md`.

## When to use this skill

- When the user asks you to "start a task", "pick up the next task", "implement the next phase", or "continue with development".
- When you are tasked with selecting and implementing the next valid unchecked task from `docs/PLAN.md`.

## Strategy: Explore -> Plan -> Implement -> Verify -> Commit -> CI Green

Follow these steps sequentially to execute a task:

### 1. Explore & Plan
Do not jump straight to coding.
Build context first:
1. Read `docs/PROMPT.md` (non-negotiable rules).
2. Read relevant sections in `docs/PLAN.md` and `docs/SPEC.md`.
3. Select a valid task whose dependencies are satisfied.
4. Explore impacted crates/modules/tests before editing.
5. If scope is ambiguous, ask the user before implementation.

### 2. Implement & Verify
Ensure work is testable and deterministic:
1. Create branch from latest `main` using:
   - `feature/pr-xx-short-description`
2. Use TDD:
   - Red: add failing test first
   - Green: implement minimal fix/feature
   - Refactor: keep behavior and determinism
3. Keep diffs focused; avoid unrelated refactors.
4. Mandatory local checks:
   - `cargo fmt --all -- --check`
   - `cargo clippy --workspace --all-targets -- -D warnings`
   - `cargo test --workspace`
5. If touching ingest/query flows, run E2E explicitly:
   - `cargo test -p ingestion --test e2e_pipeline_test -- --nocapture`

### 3. Docs / Commit / PR / CI
1. Update `docs/PLAN.md` checkboxes and notes for completed scope.
2. Commit with Conventional Commit style:
   - `<type>(<scope>): <summary>`
3. Push branch:
   - `git push -u origin <branch>`
4. Create PR with `gh pr create` including:
   - Purpose
   - Changes
   - Exit Criteria
   - Test Results
5. Monitor checks until all green:
   - `gh pr checks --watch`
   - Fix failures and push follow-up commits until CI is fully green.

## Project-specific reminders

- Workspace crates include: `core`, `storage`, `ingestion`, `query`, `slm`, `jobs`, `prototypes`.
- Keep WAL-first durability and graph/vector consistency constraints from `docs/PROMPT.md`.
- Do not delete unrelated untracked files without explicit user instruction.
- Do not use destructive git operations unless explicitly requested.
