# aLayaSiki 実装計画 (PR 分割)

本計画は `docs/SPEC.md` をベースに、PR単位の実装タスクへ分解したものです。
各PRのタスクは「何をするか」を具体化し、依存関係を明示しています。

---

## 実現性レビューと段階的実装方針

本PRDは野心的なため、初期フェーズでは**段階的に実現性を検証しながら進める**前提とする。

- GPU-First / GPUDirect は高難度のため、**CPU経路を先に実装**し、GPUはティアリングの拡張として段階導入する。
- Embedded SLM は**同期処理ではなく非同期ジョブ**として開始し、失敗時はベクトル化のみで継続するフェイルセーフを先に確立する。
- Hyper-Index の O(1) 相互参照は最適化項目として扱い、**初期版はIDマップ経由**での参照を許容する。
- マルチモーダル取り込みは**テキスト/PDF優先**で開始し、画像/音声は後続PRで拡張する。
- 1億ノード規模は**ベンチマーク段階で段階的にスケール検証**し、初期は10^5〜10^6規模で性能特性を把握する。
- 再現性/Time-Travel/監査は後追い実装が困難なため、**スキーマ/メタデータ設計を先行**させる。

## 主要な技術選定（要決定）

- 実装言語 / ランタイム（Rust/Go/C++ など）
- ANNインデックス（HNSW/IVF/FAISS など）とGPU対応方針
- グラフストレージ方式（隣接リスト/CSR/インメモリ + 永続化）
- コミュニティ検出アルゴリズム（**Leiden推奨** - Louvainより高速で連結性保証）
- 取り込み時のテキスト解析器（PDF/Markdown/JSON）
- SLM推論実行基盤（内蔵推論/外部推論サービス）
- 抽出特化SLM（**Triplex/GLM-4-Flash推奨** - コスト最大98%削減可能）

---

## PR-00: Feasibility Spikes（検証プロトタイプ）

* Depends on: なし

- [x] ANNインデックスの候補評価（検索精度/速度/更新性能）
- [x] グラフ隣接ストレージの最小プロトタイプ（1-hop/2-hop性能）
- [x] GPUメモリ常駐とティアリングの概念検証（GPUなしでも動作可能な設計）
- [x] SLM推論の最小パイプライン試作（非同期ジョブ前提）
- [x] ADR（Architecture Decision Record）として選定結果を記録

---

## PR-01: リポジトリ基盤・CI・モジュール骨格

* Depends on: なし

- [x] プロジェクト構成の確定（core / storage / ingestion / query / api / sdk など）
- [x] ビルド設定・CIワークフロー追加（lint/test/build）
- [x] 設定管理の骨格（設定ファイル、環境変数、起動オプション）
- [x] ログ基盤（構造化ログ、ログレベル、出力先）

---

## PR-02: ストレージ基盤 (WAL/スナップショット)

* Depends on: PR-01

- [x] WALのインターフェース定義と永続化実装（書き込みACK条件を明文化）
- [x] スナップショット作成/復元の最小実装（メタ情報保存含む）
- [x] 単一ノード前提の耐障害性（クラッシュリカバリの検証）
- [x] ストレージ障害時のエラーハンドリング定義

---

## PR-03: データモデル (Vector-Graph Hybrid) 実装

* Depends on: PR-01, PR-02

- [x] Node/Edge スキーマ定義（raw_data/embedding/metadata/provenance/confidence/model_id 等）
- [x] Entity Resolution の最小実装（同一性スコア・正規化ルール）
- [x] Hyper-Index のメタデータ構造定義（ANN + adjacency 共存）
- [x] CRUD API の最小セット（Node/Edge の作成・取得・削除）
- [x] ID設計（安定ID/バージョン/スナップショット参照）

---

## PR-04: インデックス基盤 (ANN + グラフ隣接)

* Depends on: PR-03

- [x] **[High] WAL Replay による再起動復元**: Repository 起動時に WAL を replay してノードを復元する (PR-03 レビュー指摘)
- [x] **[High] 削除トゥームストーン対応**: delete_node を WAL に記録し、クラッシュ後も削除を維持する (PR-03 レビュー指摘)
- [x] ANNインデックスの初期実装（挿入・検索・削除）
- [x] グラフ隣接リストの初期実装（1-hop/2-hop探索）
- [x] Hyper-Index の相互参照を実装（IDマップ経由の参照で開始）
- [x] インデックス更新の原子性保証（トランザクション境界の定義）
- [x] **[Medium] AllocSerializer への切り替え**: to_bytes の固定サイズを動的シリアライザに変更 (PR-04 レビュー指摘)
- [x] **[Medium] Repository と HyperIndex の連携**: 再起動後にインデックスも復元する (PR-04 レビュー指摘)
- [x] **[Low] delete_node 順序修正**: 存在確認後に WAL 書き込み (PR-04 レビュー指摘)
- [x] **[Low] expand_graph 仕様明確化**: max_hops の挙動を修正 (PR-04 レビュー指摘)
- [x] **[Low] ANN 次元チェック**: 次元不一致を除外/エラー化 (PR-04 レビュー指摘)

---

## PR-05: Ingestion パイプライン (マルチモーダル + Chunking)

* Depends on: PR-01, PR-03

- [x] Ingestion API のエンドポイント定義（PDF/JSON/Markdown）
- [x] Ingestion API のエンドポイント定義（画像/音声）
- [x] Auto-Chunking のルール実装（意味区切り/最大長/オーバーラップ）
- [x] Embedding 生成フローの実装（model_id を付与）
- [x] Idempotency/Dedup（content_hash + idempotency_key）
- [x] PIIマスキング/禁止語フィルタのポリシーフック
- [x] 初期版はテキスト/PDFを優先し、画像/音声は後続PRで拡張

---

## PR-06: SLM 組み込みと Lazy Graph Construction

* Depends on: PR-05

- [x] SLMレジストリ（model_id、バージョン管理、ロールバック）
- [x] NER / Relation Extraction の最小実装 (Mock)
- [x] Lazy Graph Construction のジョブキュー（バックグラウンド/オンデマンド）
- [x] 再現性担保のための model_id / snapshot_id 固定ロジック
- [x] 失敗時のフェイルセーフ（ベクトル化のみで取り込み継続）
- [x] **[New] Triplex/GLM-4-Flash のような軽量抽出モデル統合** (コスト削減)

---

## PR-06.5: コミュニティ検出と階層的要約 (NEW)

* Depends on: PR-04, PR-06

- [x] **Leidenアルゴリズム実装** (コミュニティ検出)
- [x] 階層的コミュニティ構造の維持
- [x] 各レベルでの自然言語要約生成 (LLM使用)
- [x] **FastGraphRAGアプローチ**: PageRankで上位10%ノードを特定し要約
- [x] コミュニティ要約の増分更新サポート

---

## PR-07: Query 実行基盤 (DSL/自然言語/Explain)

* Depends on: PR-03, PR-04

- [x] JSON DSL のパーサーとバリデーション
- [x] **search_mode** (local/global/drift/auto) のパラメータ追加とバリデーション
- [x] Query Planner（Vector Search → Graph Expansion → Context Pruning）
- [x] Explain Plan 出力（アンカー/経路/除外理由）
- [x] Query Mode（answer/evidence）の切替実装

---

## PR-08: GraphRAG 推論パイプライン

* Depends on: PR-07, PR-06, PR-06.5

- [x] Vector Search のアンカー特定
- [x] Graph Expansion の hop 探索
- [x] Context Pruning の初期実装（ノイズ除外）
- [x] **ローカル検索**: エンティティ中心の探索 (1-2 hop)
- [x] **グローバル検索**: コミュニティ要約を活用したMap-Reduceスタイル回答
- [x] **DRIFT検索**: フィードバックループによる動的グラフ拡張
- [x] 生成回答の groundedness スコア付与
- [x] GraphRAG失敗時のフォールバック（Vector-only回答）

---

## PR-09: 出力仕様 (Evidence/Provenance/Citation)

* Depends on: PR-08

- [x] Evidence サブグラフ返却形式の実装
- [x] Provenance/Confidence を返却に含める
- [x] Citation 形式（source + span）を定義・返却
- [x] time_travel / snapshot_id の優先順序を反映

**Notes:**
- `EvidenceNode`/`EvidenceEdge` に `Provenance` 構造体と `confidence` フィールドを追加
- `Citation` に `node_id`/`confidence` を追加、`span` をデータ全長ベースに改善
- `QueryRequest` に `time_travel` フィールド追加（YYYY-MM-DD / RFC3339）、`snapshot_id` が優先
- `QueryResponse` に `latency_ms`/`time_travel` を追加（SPEC 4.1 準拠）
- Repository にエッジメタデータストアを追加し、エッジ provenance の WAL 永続化・復元を実装
- `Provenance` は `source`, `extraction_model_id`, `snapshot_id`, `ingested_at` を保持
- `confidence` はノードメタデータの明示値がある場合はそれを使用、なければスコアにフォールバック

---

## PR-10: セキュリティ・ガバナンス

* Depends on: PR-01

- [x] 認証/認可（OAuth/JWT + RBAC/ABAC）
- [x] 暗号化（TLS/保存時暗号化/KMS連携のフック）
- [x] 監査ログ（操作・クエリ・model_id 追跡）
- [x] データレジデンシ/保持期間ポリシーの設定機構

**Notes:**
- `core::auth` に JWT 認証 (`JwtAuthenticator`) と `Principal` を追加
- `Authorizer` で RBAC (role-permission) と ABAC (tenant 境界 / 属性一致 / clearance_level) を評価
- `IngestionPipeline::ingest_authorized` / `QueryEngine::execute_authorized` を追加し、実行前に認可を強制
- `core`/`ingestion`/`query` に認可回帰テストを追加（許可/拒否ケース）
- `core::audit` に監査イベント (`AuditEvent`) と `AuditSink` を追加
- `InMemoryAuditSink` と JSONL 永続化の `JsonlAuditSink` を実装
- `IngestionPipeline` / `QueryEngine` から操作成功・拒否・失敗を監査ログへ出力し、`model_id` を追跡
- `storage::crypto` に保存時暗号化フック（`AtRestCipher`）と KMS 連携フック（`KmsHookCipher` / `KmsKeyProvider`）を追加
- `Wal::open_with_cipher` と `Repository::open_with_cipher` を追加し、WAL 書き込み/リプレイで暗号化フックを適用
- `core::governance` にテナント単位のデータレジデンシ/保持期間ポリシー（`TenantGovernancePolicy`）を追加
- `IngestionPipeline` にガバナンスポリシーストア連携を追加し、取り込み時にリージョン検証・保持期限/KMS メタデータ付与を実装
- `QueryEngine` で `retention_until_unix` を尊重し、期限切れデータを回答対象から除外

---

## PR-11: 運用機能 (Cache/Time-Travel/Backup)

* Depends on: PR-02, PR-07

- [x] Semantic Cache（意味的同一クエリの再利用）
- [x] Time-Travel のスナップショット参照
- [x] Backup/Restore の実装（PITR含む）
- [x] 退避/キャッシュポリシーの設定値反映

**Notes:**
- `query::semantic_cache` を追加し、`QueryEngine` 実行前に `QueryRequest` + `model_id` + `snapshot_id` をキーとして意味類似照合するキャッシュを実装
- 類似度は正規化クエリのトークンJaccardで判定し、閾値以上の場合は `semantic_cache_hit` を `explain.steps` に付与して即時返却
- スナップショット境界（`snapshot_id`）をキーに含め、異なる時点のデータ間でキャッシュが混線しないことを `query/tests/semantic_cache_test.rs` で検証
- `snapshot_id=wal-lsn-<N>` を指定したクエリは、WAL を `N` まで再生した読み取りビューを使って実行される（`storage::repo::Repository::load_snapshot_view`）
- 存在しない `snapshot_id` は `QueryError::NotFound`（NOT_FOUND 相当）で返却する
- `time_travel` 単独指定時の「日時→スナップショット解決」は未実装で、現状は最新スナップショットにフォールバックする（PR-11 のフォローアップタスクで対応）
- `search_mode=global` かつ `snapshot_id` 指定時は、非バージョン化のコミュニティ要約による時点混線を避けるため要約合成を無効化して evidence ベースにフォールバックする。スナップショット整合のある global map-reduce は将来タスク（PR-11 Backup/Restore 連携）で対応。
- `storage::snapshot::SnapshotManager` を Repository 復元経路へ統合し、WAL 差分再生と組み合わせた PITR を実装
- `Repository::open_with_snapshots` を追加し、起動時に最新バックアップスナップショットを復元してから WAL 差分を再生する経路を実装
- `Repository::create_backup_snapshot` / `restore_from_latest_backup` を追加し、運用時のバックアップ作成と復元をサポート
- `Repository::load_snapshot_view` は snapshot ディレクトリが設定されている場合、`target_lsn` 以前の最新バックアップを起点に PITR を構築するよう拡張

---

## PR-11.5: エージェンティックワークフロー対応 (NEW)

* Depends on: PR-04, PR-07

- [x] セッショングラフ（TTL付きの一時サブグラフ）
- [x] ワーキングメモリ用の低レイテンシ読み書きAPI
- [x] セッション境界の分離（テナント/ユーザー単位）
- [x] セッショングラフのスナップショット化/クリーンアップ

---

## PR-12: Observability & SLO

* Depends on: PR-01, PR-07

- [x] レイテンシ/ヒット率/GPU使用率/抽出精度のメトリクス
- [x] SLO計測（P95/P99）とダッシュボード出力
- [x] エラーカテゴリ（INVALID_ARGUMENT など）を統一

**Notes:**
- `core::error::ErrorCode` and `AlayasikiError` trait added for unified error mapping.
- `QueryError`, `RepoError`, `WalError`, `SnapshotError`, and `AuthzError` now implement `AlayasikiError`.
- `QueryResponse` includes `error_code` field.
- `core::metrics::MetricsCollector` implemented for high-precision (microsecond) latency tracking and SLM/GPU metrics.
- P50, P95, P99 percentiles calculated in `MetricsSnapshot`.
- `QueryEngine` integrated with `MetricsCollector`.

---

## PR-13: SDK / クライアントAPI

* Depends on: PR-07, PR-09

- [x] SDK のクライアント実装（ingest/query/response）
- [x] サンプルコード（SPEC の擬似コード準拠）
- [x] リトライ/バックオフの方針実装
- [x] **LlamaIndex 統合**: `GraphStore` / `VectorStore` インターフェース実装
- [x] **LangChain 統合**: `GraphVectorStore` 対応

**Notes:**
- `sdk` crate (`alayasiki-sdk`) を追加し、`Client` / `InProcessTransport` / `ClientBuilder` を実装。
- `Client::ingest` は `IngestionRequest` を受けて `IngestResult { node_ids, snapshot_id }` を返却。
- `Client::query` は `QueryRequest` を受けて `QueryResponse` を返却し、既存 Query Engine と整合。
- `RetryConfig`（max_attempts / initial_backoff / max_backoff / multiplier）を実装し、retryable エラー時に指数バックオフ再試行。
- `sdk/tests/client_test.rs` で ingest->query の一気通貫、retryable/non-retryable の再試行挙動、repo 注入ビルドを検証。
- `sdk/examples/basic_client.rs` に SPEC の擬似コードに沿った最小サンプルを追加。
- `sdk::integrations::llama_index` を追加し、`VectorStore`/`GraphStore` trait と `LlamaIndexAdapter` を実装。
- `sdk::integrations::langchain` を追加し、`GraphVectorStore` trait と `LangChainAdapter` を実装。
- アダプタは `Client` を利用し、`top_k`/`depth` の不正値を正規化して `QueryRequest` バリデーションエラーを回避。
- `sdk/tests/framework_adapters_test.rs` で LlamaIndex/LangChain の追加・検索・正規化挙動を検証。

---

## PR-13.5: 可視化UI (NEW)

* Depends on: PR-13, PR-06.5

- [x] グラフエクスプローラーUI (フォースダイレクテッドレイアウト)
- [x] コミュニティクラスタリング表示
- [x] ノード/エッジ詳細パネル
- [x] AIチャットとのインタラクティブ連携
- [x] フロントエンド: React + D3.js

---

## PR-14: ベンチマーク & 評価

* Depends on: PR-04, PR-08, PR-12

- [x] ベンチマークスイート（1億ノード/3億エッジ想定）
- [x] read:write=9:1 プロファイルでの負荷試験
- [x] GraphRAG 精度（根拠付き回答率）測定

**Notes:**
- `benchmarks/benchmark_suite.py` を追加し、Operational Latency / GraphRAG Production / Python ANN ベンチを `baseline` / `scale` プロファイルで一括実行・集計できるようにした。
- `prototypes/src/bench_eval.rs` にレイテンシ要約と read-quality 集計を抽出し、GraphRAG ベンチで `evidence_attachment_rate` / `answer_with_evidence_rate` / `answer_with_citations_rate` を明示的に測定するようにした。
- `prototypes/benches/operational_latency_bench.rs` は JSON レポート出力を追加し、スイート集計で再利用可能にした。
- ベースライン実行結果を `benchmarks/results/pr14_suite_baseline.json` / `benchmarks/results/pr14_suite_baseline.md` に保存した。
- ベースライン結果: operational throughput `389.38 ops/s`, read p95 `16.96 ms`, write p95 `188.67 ms`; GraphRAG throughput `221.73 ops/s`, read p95 `32.89 ms`, write p95 `141.74 ms`, `evidence_attachment_rate=1.0`, `answer_with_evidence_rate=1.0`; ANN search `usearch=0.0035 s`, `faiss_flat=0.0306 s`.

---

## PR-14.5: E2E/CI/ベンチ基盤拡充 (NEW)

* Depends on: PR-05, PR-07, PR-14

- [x] **E2E統合テスト追加**: ingest -> query の一気通貫テスト（フィルタ/引用/再現性）を追加
- [x] **CIワークフロー拡充**: `fmt` / `clippy` / `cargo test --workspace` を必須化
- [x] **E2Eジョブ追加**: `cargo test -p ingestion --test e2e_pipeline_test` をCIで常時実行
- [x] **ベンチスモーク追加**: Criterionベースのストレージ検索ベンチをCIで定期実行
- [x] **実運用レイテンシ評価ベンチ追加**: read:write=9:1 / 並列ワーカーで p50/p95/p99 を算出
- [x] **ベンチ回帰ゲート追加**: 実運用レイテンシベンチで p95/throughput 閾値超過時にCI fail
- [x] Python ANNベンチ（faiss/usearch）のCIジョブ化と成果物保存
- [x] Python ANNベンチの baseline 比較（回帰率チェック）をCIに追加

---

## PR-15: ドキュメント整備

* Depends on: PR-13, PR-14, PR-17.1, PR-17.2, PR-17.3, PR-17.4

- [ ] 運用ガイド（バックアップ/復元/監査ログ）
- [ ] APIリファレンス（JSON DSL スキーマ/レスポンス）
- [ ] 実運用の制約（GPUメモリ逼迫時の挙動）を明文化

---

## PR-14.6: 実運用レイテンシ改善と評価拡張 (NEW)

* Depends on: PR-14.5

- [ ] **Writeレイテンシ改善**: ingest の WAL flush 周りを計測し、group commit / バッチ書き込みの改善案を検証
- [ ] **スケール検証拡張**: `10^5 -> 10^6` ノードで read/write p50/p95/p99 と throughput を比較
- [ ] **並列度検証**: worker 数（8/32/128）別に read:write=9:1 の劣化カーブを取得
- [x] **結果保存の標準化**: ベンチ結果を `benchmarks/results/*.json` に出力し、比較可能な履歴を残す
- [x] **回帰ガード**: CI に p95 閾値チェック（read/write）を導入し、悪化時に失敗させる
- [x] **CIベンチ基準の実運用化**: Operational/ANN ベンチの入力条件を baseline と一致させ、閾値（read/write p95・throughput・ANN回帰率）を現実的な SLO ベースに再定義し、過剰に緩い基準を解消する

**Notes:**
- `prototypes/benches/graphrag_production_bench.rs` を追加し、実運用近似の GraphRAG 負荷（read:write=9:1、並列ワーカー、`local/global/drift/auto` 混在）を計測可能にした
- ベンチ結果は JSON で `benchmarks/results/graphrag_production_latest.json` に保存（`ALAYASIKI_GRAPHRAG_RESULTS_PATH` で履歴ファイル名を切替可能）
- レポート作成用の主要指標（read/write p50/p95/p99、throughput、groundedness、evidence件数、semantic cache hit率、mode mix）を出力対象に追加
- ベースライン実行（`seed_nodes=4000, workers=6, warmup=20, measured=100`）の結果を `benchmarks/results/graphrag_production_report.md` に記録
- CI に `Benchmark Gate (graphrag p95 thresholds)` ジョブを追加し、`ALAYASIKI_GRAPHRAG_MAX_READ_P95_MS` / `ALAYASIKI_GRAPHRAG_MAX_WRITE_P95_MS` / `ALAYASIKI_GRAPHRAG_MIN_THROUGHPUT` による回帰判定を常時実行
- Operational ベンチ条件を baseline プロファイル（`nodes=4000, workers=6, ops_per_worker=100, read:write=9:1`）へ統一し、閾値を `read_p95<=30ms`, `write_p95<=200ms`, `throughput>=250ops/s` に更新
- ANN ベンチ入力を baseline（`n_samples=10000, n_dims=128, n_queries=100, top_k=10, seed=42`）に合わせ、回帰率閾値を `10.0` から `2.0` に引き締め
- `prototypes/benches/operational_latency_bench.rs` は `ALAYASIKI_BENCH_WAL_FLUSH_POLICY`（`always` / `interval` / `batch`）と seed 用 batch flush を受け付けるよう拡張し、WAL flush 方針比較と大規模 seed を同じベンチで扱えるようにした
- `benchmarks/benchmark_suite.py --mode pr14-6-operational` を追加し、WAL flush 比較・`10^5 -> 10^6` ノード scale sweep・`8/32/128` worker sweep を `benchmarks/results/pr14_6_operational_*.json` と `pr14_6_operational_matrix.{json,md}` に保存できるようにした
- 長時間の実ベンチ結果はこの変更では未同梱。上記 runner を実行して成果物を生成し、閾値/採用 flush policy を確定した時点でチェックボックスを更新する

---

## PR-14.7: E2Eテスト網羅性の向上 (NEW)

* Depends on: PR-10, PR-14.5

- [ ] **マルチモーダルE2Eの完結**:
    - [ ] テスト用バイナリPDFアセットの導入と `pdf-extract` 正常系テストの有効化
    - [ ] 画像/音声データのメタデータ抽出・インデックス・検索の一気通貫テスト
- [ ] **セキュリティ・マルチテナンシーE2E**:
    - [x] 認証・認可の統合テスト: JWT発行 -> 認可済みIngest -> 認可済みQuery のフロー検証
    - [x] テナント分離の厳格な検証: 他テナントのデータが検索結果に混入しないことの確認
    - [x] RBAC/ABAC 動的権限変更時の挙動検証
- [ ] **ガバナンス・ポリシーE2E**:
    - [ ] PIIマスキングの実効性検証: 個人情報を含むデータの投入 -> 検索結果でのマスキング確認
    - [x] データレジデンシの強制検証: 指定リージョン外からの要求拒否フロー
    - [x] 保持期限（Retention）の動的検証: 期限切れデータが検索対象から自動除外されることの確認

**Notes:**
- JWTトークンを直接受け取り認証・認可を一体で実行するAPIを追加 (`IngestionPipeline::ingest_jwt_authorized`, `QueryEngine::execute_json_jwt_authorized`)
- `ingestion/tests/e2e_pipeline_test.rs` に JWT発行 -> 認可済みIngest -> 認可済みQuery のE2Eテストを追加し、実行経路を固定化
- 認可済み ingest でノードメタデータに `tenant` を強制付与し、認可済み query では同一 `tenant` のノードのみ探索・返却するよう制限
- テナント分離E2E (`test_e2e_tenant_isolation_prevents_cross_tenant_leakage`) を追加し、クロステナント混入が発生しないことを検証
- 動的権限変更E2E (`test_e2e_dynamic_rbac_abac_permission_transition`) を追加し、RBAC更新前後の拒否理由遷移（`PermissionDenied` -> `InsufficientClearance`）とABAC更新後の許可を検証
- 保持期限E2E (`test_e2e_retention_dynamic_excludes_expired_nodes`) を追加し、ガバナンスポリシー更新後に期限切れデータが `retention_expired` として検索結果から除外されることを検証
- データレジデンシE2E (`test_e2e_data_residency_enforces_region_boundary`) を追加し、越境リージョン取り込みの `ResidencyViolation` 拒否と同一リージョン取り込み後の検索成功を検証

---

## PR-16: レプリケーション / HA（商用化向け）

* Depends on: PR-02, PR-03, PR-07, PR-17.1

- [ ] シャード単位のレプリケーション（準同期/遅延許容）
- [ ] フェイルオーバー設計と整合性ポリシー
- [ ] リードルーティング / リード・ユア・ライト検証
- [ ] 災害復旧手順とRPO/RTOの明文化

---

## PR-17: コア信頼性ハードニング（Epic）(NEW)

* Depends on: PR-02, PR-04, PR-06, PR-11

- [ ] PR-17.1 WAL整合性ハードニング
- [ ] PR-17.2 time_travel解決とスナップショット整合
- [ ] PR-17.3 ANN置換（HNSW化）
- [ ] PR-17.4 Mock脱却（抽出器本実装化）

**Notes:**
- PR-17 は単一PRではなく、段階リリースと切り戻し容易性を優先したエピック（複数PR）として運用する。

---

## PR-17.1: WAL整合性ハードニング (NEW)

* Depends on: PR-02

- [x] `Wal::open_with_cipher` で既存WALを走査し `current_lsn` を復元（TODOの `0` 初期化を廃止）
- [x] 起動時LSN復元と `replay` 読み取りロジックを共通化し、末尾部分書き込み切り詰め/CRC検証を一元化
- [x] CRC不整合時は fail-fast を既定化し、運用向けに `last_good_offset` まで復旧するリカバリモードを追加
- [x] `flush_policy`（always / interval / batch）を設定化し、耐久性と遅延トレードオフを測定可能にする

**Done Criteria:**
- [x] 再起動後の LSN が単調増加し、クラッシュリカバリ後も欠番/巻き戻りが発生しない
- [x] WAL破損系の回帰テスト（CRC mismatch / partial write）が追加される

**Notes:**
- `storage::wal` に `WalOptions` / `WalRecoveryMode` / `WalFlushPolicy` を追加し、起動時復元と通常 `replay` の読み取り経路を `scan_entries` へ統合
- CRC mismatch は既定で fail-fast とし、`RecoverToLastGoodOffset` 指定時のみ破損エントリ手前まで切り詰めて継続
- `Wal::append` が `flush_policy` に従って自動 flush し、`Repository::open_with_options` / `open_with_cipher_and_options` から上位層へ設定を伝搬
- `storage/tests/wal_policy_test.rs` で CRC fail-fast / recovery truncation / batch flush / interval flush を回帰検証

---

## PR-17.2: time_travel解決とスナップショット整合 (NEW)

* Depends on: PR-11, PR-17.1

- [ ] `snapshot_id` 優先を維持しつつ、`time_travel(YYYY-MM-DD/RFC3339)` を UTC as-of として `snapshot_id` に解決
- [ ] スナップショット台帳（`snapshot_id`, `lsn`, `created_at_unix`）を保存し、日時→LSN解決を O(log N) で実行
- [ ] コミュニティ要約のスナップショット版管理（最低限 `snapshot_lsn_range`）を導入し、`search_mode=global` で時点混線を防止
- [ ] エラー規約を統一（該当時点なし: `NOT_FOUND`、形式不正: `INVALID_ARGUMENT`）

**Done Criteria:**
- [ ] 同一データに対する `snapshot_id` と `time_travel` 解決結果が再現可能
- [ ] `search_mode=global` + `time_travel` の回帰テストで要約混線が発生しない

---

## PR-17.3: ANN置換（HNSW化）(NEW)

* Depends on: PR-04, PR-14.6

- [ ] `LinearAnnIndex` 依存を `VectorIndex` 抽象へ分離し、`HyperIndex` から実装差し替え可能にする
- [ ] `usearch` ベース HNSW を第一実装として導入（挿入・検索・削除・次元整合チェック・top-k 安定ソート）
- [ ] 再起動時は WAL replay + ノード再走査で再構築し、将来の高速起動向けに ANN サイドカースナップショット形式を定義
- [ ] 線形探索を真値として `recall@k` と `p95 latency` をCI計測し、PR-14.6 ベンチゲートへ統合

**Done Criteria:**
- [ ] ANN回帰ゲート（recall/latency）を満たした状態で関連ベンチが通過
- [ ] feature flag で線形探索フォールバックへ安全に切り戻せる

---

## PR-17.4: Mock脱却（抽出器本実装化）(NEW)

* Depends on: PR-06

- [ ] `MockEntityExtractor` を本番デフォルトから外し、テスト専用実装へ隔離
- [ ] ルールベース抽出 + 軽量SLM抽出（Triplex/GLM-4-Flash等）のハイブリッド抽出器を `ModelRegistry` で切替可能にする
- [ ] entityノードの `embedding` 方針（生成する/しない）を明文化し、`confidence`/`provenance` を抽出経路ごとに記録
- [ ] 固定fixture抽出回帰、失敗時フェイルセーフ（ベクトル化継続）E2E、マルチモーダル抽出E2E（PR-14.7）を接続

**Done Criteria:**
- [ ] デフォルト経路に Mock 実装が残らない（テスト用途のみ）
- [ ] 抽出失敗時の ingest 継続性が E2E で保証される

---

## 依存関係サマリ

- PR-00 → 技術選定と方針確定
- PR-01 → すべての基盤
- PR-02 → PR-03/11/16/17/17.1
- PR-03 → PR-04/07
- PR-04 → PR-06.5/08/14/17/17.3
- PR-05 → PR-06
- PR-06 → PR-06.5/08/17/17.4
- PR-06.5 → PR-08/13.5 (NEW: コミュニティ検出)
- PR-07 → PR-08/09/11/12/13
- PR-08 → PR-09/14
- PR-09 → PR-13
- PR-10 → 独立 (PR-01依存)
- PR-11 → PR-11.5/17/17.2
- PR-12 → PR-14
- PR-13 → PR-13.5/15
- PR-13.5 → PR-15 (NEW: 可視化UI)
- PR-14 → PR-15
- PR-14.5 → PR-15 (NEW: 品質ゲートと性能計測の継続運用)
- PR-14.6 → PR-15/17.3 (NEW: 実運用レイテンシ改善と回帰防止)
- PR-14.7 → PR-15 (NEW: E2Eテスト網羅性の向上)
- PR-16 → 商用化フェーズのHA/冗長性
- PR-17 (Epic) → PR-17.1/17.2/17.3/17.4
- PR-17.1 → PR-16/17.2/15
- PR-17.2 → PR-15
- PR-17.3 → PR-15
- PR-17.4 → PR-15

---

## リサーチに基づく追加検討項目

- **エンティティ解決**: ベクトル類似度 + LLM検証のハイブリッドアプローチ
- **エージェンティックワークフロー**: セッショングラフ、ワーキングメモリ (FalkorDB参考)
- **コスト最適化**: LazyGraphRAG, FastGraphRAGの実装検討
- **特化SLM**: Triplex (Phi-3ベース) や GLM-4-Flash の統合
