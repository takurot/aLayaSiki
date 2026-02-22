---
name: start-task
description: Guides the agent on how to start and execute a development task in the Moltgames project following strict repository rules. Use this when the user asks you to start a task, pick up the next task, or proceed with development.
---

# Start Task Skill

This skill defines the standard operating procedure for starting and completing a development task in the `moltgames` repository. It ensures that planning documents, coding conventions, architectural specifications, and PR rules are strictly followed from start to finish.

## When to use this skill

- When the user asks you to "start a task", "pick up the next task", "implement the next phase", or "continue with development".
- When you are tasked with selecting the next valid task from the `PLAN.md` to work on.

## Strategy: Explore -> Plan -> Implement -> Commit

Follow these steps sequentially to execute a task:

### 1. Explore & Plan
Do not jump straight to coding. Build the necessary context first:
1. **Read the Rules**: Review `@docs/PROMPT.md` carefully. This document contains absolute rules for branching, committing, testing, error handling, and coding standards. You MUST adhere to them. 
2. **Review Specifications**: Check both `@docs/PLAN.md` and `@docs/SPEC.md`. Understand the current project phase and PR dependencies from `PLAN.md`.
3. **Select the Task**: Identify the next logical task/PR to implement that has not been completed yet, based on the dependency map in `PLAN.md`.
4. **Acquire Code Context**: Explore the existing codebase (using file viewing, directory listing, or search tools) to fully understand the areas your selected task will modify.
5. **Ask/Formulate**: If the task scope is ambiguous or modifies multiple core files, create a brief implementation plan and ask the user for verification.

### 2. Implement & Verify
Ensure you have a reliable way to verify your work:
1. **Create Branch**: Check out a new branch from the latest `main`. The branch name must follow the `codex/<type>/<pr-id>-<slug>` convention as defined in `PROMPT.md`.
2. **Test-Driven Development (TDD)**: Perform TDD (Red -> Green -> Refactor) for any business logic or game rule changes. Always start by writing a failing test.
3. **Write and Execute Code**: Write your implementation code in small, incremental steps. Keep changes focused and under 500 lines if possible (1 Task = 1 Branch = 1 PR).
4. **Local Quality Gates**: Ensure your code passes all mandatory quality checks before concluding. For example, run tests (`pnpm test:unit` / `pnpm test:integration`), type checks (`pnpm typecheck`), and linting (`pnpm lint`) to verify the root cause of any issues is fixed.

### 3. Commit & Pull Request
1. **Commit**: Use Conventional Commits (`<type>(<scope>): <summary>`) for your commit messages as rigidly detailed in `PROMPT.md`.
2. **Push**: Push your created branch to the remote repository.
3. **Create PR**: Open a Pull Request for your branch using the GitHub CLI (`gh pr create`). Ensure the PR description includes all mandatory sections required by `PROMPT.md` (Purpose, Changes, Test Results, Risk/Rollback, and References to SPEC/PLAN).
