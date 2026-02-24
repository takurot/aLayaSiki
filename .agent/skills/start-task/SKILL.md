---
name: start-task
description: Guides the agent to start and complete development tasks in the aLayaSiki Rust workspace using repository-specific rules.
metadata:
  owner: user
  version: "1.0.0"
---

# Start Task Skill (aLayaSiki)

このスキルは、このリポジトリで新しいタスクを開始して完了するまでの標準手順です。
対象は Rust workspace (`core`, `storage`, `ingestion`, `query`, `slm`, `jobs`, `prototypes`) です。

## When To Use

- 「新しいタスクを開始して」
- 「次のPRタスクを実装して」
- 「PLAN.md の未完了項目を進めて」

## Mandatory Order

1. Explore
2. Plan
3. Implement (TDD)
4. Verify
5. Commit / Push / PR
6. CI Green まで対応

## 1) Explore

着手前に必ず以下を読む:

1. `docs/PROMPT.md`
2. `docs/PLAN.md`
3. `docs/SPEC.md`
4. 必要に応じて `docs/adr/*`

その上で:

- 依存関係を満たす次タスクを `docs/PLAN.md` から選ぶ
- 変更対象モジュールの既存実装と既存テストを確認する
- スコープ外の変更は入れない（最小差分）

## 2) Plan

- 実装前に短い実行計画を作る
- 受け入れ条件を `docs/PLAN.md` / `docs/SPEC.md` から明文化する
- 曖昧な要件がある場合は、実装前にユーザーへ確認する

## 3) Implement (TDD)

### Branch

- `main` 最新から分岐
- 命名: `feature/pr-xx-short-description`

### TDD

1. 先に failing test を追加（Red）
2. 最小実装で通す（Green）
3. 必要最小限の整理（Refactor）

### Implementation Rules

- 再現性・決定性を維持（順序非決定な実装を避ける）
- WAL-first / index整合性など `docs/PROMPT.md` の非交渉ルールを守る
- 無関係なリファクタを混ぜない

## 4) Verify

変更完了時は最低限これを実行:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

追加で、影響範囲に応じて実行:

```bash
# E2E (ingestion/query に触れた場合)
cargo test -p ingestion --test e2e_pipeline_test -- --nocapture

# ベンチ/性能系に触れた場合
cargo bench -p prototypes --bench operational_latency_bench
```

## 5) Commit / Push / PR

### Docs Update

- 完了項目を `docs/PLAN.md` で `[x]` に更新
- 必要に応じて Notes 追記（仕様との差分、次タスクへの引き継ぎ）

### Commit

- Conventional Commits: `<type>(<scope>): <summary>`
- 1コミット1論理変更を優先

### Push / PR

```bash
git push -u origin <branch>
gh pr create --base main --head <branch>
```

PR本文には以下を含める:

- Purpose
- Changes
- Exit Criteria
- Test Results

## 6) CI Green

- `gh pr checks` で状態確認
- 失敗時はログを確認して修正→再push
- **全チェック green になるまで対応**して完了報告する

## Guardrails

- ユーザー指示がない限り破壊的操作（`reset --hard` など）をしない
- ユーザーが意図していない untracked ファイルを勝手に削除しない
- 既存の unrelated changes は巻き戻さない
