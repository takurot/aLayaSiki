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
- 取り込み時のテキスト解析器（PDF/Markdown/JSON）
- SLM推論実行基盤（内蔵推論/外部推論サービス）

---

## PR-00: Feasibility Spikes（検証プロトタイプ）

* Depends on: なし

- [ ] ANNインデックスの候補評価（検索精度/速度/更新性能）
- [ ] グラフ隣接ストレージの最小プロトタイプ（1-hop/2-hop性能）
- [ ] GPUメモリ常駐とティアリングの概念検証（GPUなしでも動作可能な設計）
- [ ] SLM推論の最小パイプライン試作（非同期ジョブ前提）
- [ ] ADR（Architecture Decision Record）として選定結果を記録

---

## PR-01: リポジトリ基盤・CI・モジュール骨格

* Depends on: なし

- [ ] プロジェクト構成の確定（core / storage / ingestion / query / api / sdk など）
- [ ] ビルド設定・CIワークフロー追加（lint/test/build）
- [ ] 設定管理の骨格（設定ファイル、環境変数、起動オプション）
- [ ] ログ基盤（構造化ログ、ログレベル、出力先）

---

## PR-02: ストレージ基盤 (WAL/スナップショット)

* Depends on: PR-01

- [ ] WALのインターフェース定義と永続化実装（書き込みACK条件を明文化）
- [ ] スナップショット作成/復元の最小実装（メタ情報保存含む）
- [ ] 単一ノード前提の耐障害性（クラッシュリカバリの検証）
- [ ] ストレージ障害時のエラーハンドリング定義

---

## PR-03: データモデル (Vector-Graph Hybrid) 実装

* Depends on: PR-01, PR-02

- [ ] Node/Edge スキーマ定義（raw_data/embedding/metadata/provenance/confidence/model_id 等）
- [ ] Entity Resolution の最小実装（同一性スコア・正規化ルール）
- [ ] Hyper-Index のメタデータ構造定義（ANN + adjacency 共存）
- [ ] CRUD API の最小セット（Node/Edge の作成・取得・削除）
- [ ] ID設計（安定ID/バージョン/スナップショット参照）

---

## PR-04: インデックス基盤 (ANN + グラフ隣接)

* Depends on: PR-03

- [x] **[High] WAL Replay による再起動復元**: Repository 起動時に WAL を replay してノードを復元する (PR-03 レビュー指摘)
- [x] **[High] 削除トゥームストーン対応**: delete_node を WAL に記録し、クラッシュ後も削除を維持する (PR-03 レビュー指摘)
- [x] ANNインデックスの初期実装（挿入・検索・削除）
- [x] グラフ隣接リストの初期実装（1-hop/2-hop探索）
- [x] Hyper-Index の相互参照を実装（IDマップ経由の参照で開始）
- [ ] インデックス更新の原子性保証（トランザクション境界の定義）
- [ ] **[Medium] AllocSerializer への切り替え**: to_bytes の固定サイズを動的シリアライザに変更 (PR-04 レビュー指摘)
- [ ] **[Medium] Repository と HyperIndex の連携**: 再起動後にインデックスも復元する (PR-04 レビュー指摘)
- [ ] **[Low] delete_node 順序修正**: 存在確認後に WAL 書き込み (PR-04 レビュー指摘)
- [ ] **[Low] expand_graph 仕様明確化**: max_hops の挙動を修正 (PR-04 レビュー指摘)
- [ ] **[Low] ANN 次元チェック**: 次元不一致を除外/エラー化 (PR-04 レビュー指摘)

---

## PR-05: Ingestion パイプライン (マルチモーダル + Chunking)

* Depends on: PR-01, PR-03

- [ ] Ingestion API のエンドポイント定義（PDF/JSON/Markdown/画像/音声）
- [ ] Auto-Chunking のルール実装（意味区切り/最大長/オーバーラップ）
- [ ] Embedding 生成フローの実装（model_id を付与）
- [ ] Idempotency/Dedup（content_hash + idempotency_key）
- [ ] PIIマスキング/禁止語フィルタのポリシーフック
- [ ] 初期版はテキスト/PDFを優先し、画像/音声は後続PRで拡張

---

## PR-06: SLM 組み込みと Lazy Graph Construction

* Depends on: PR-05

- [ ] SLMレジストリ（model_id、バージョン管理、ロールバック）
- [ ] NER / Relation Extraction の最小実装
- [ ] Lazy Graph Construction のジョブキュー（バックグラウンド/オンデマンド）
- [ ] 再現性担保のための model_id / snapshot_id 固定ロジック
- [ ] 失敗時のフェイルセーフ（ベクトル化のみで取り込み継続）

---

## PR-07: Query 実行基盤 (DSL/自然言語/Explain)

* Depends on: PR-03, PR-04

- [ ] JSON DSL のパーサーとバリデーション
- [ ] Query Planner（Vector Search → Graph Expansion → Context Pruning）
- [ ] Explain Plan 出力（アンカー/経路/除外理由）
- [ ] Query Mode（answer/evidence）の切替実装

---

## PR-08: GraphRAG 推論パイプライン

* Depends on: PR-07, PR-06

- [ ] Vector Search のアンカー特定
- [ ] Graph Expansion の hop 探索
- [ ] Context Pruning の初期実装（ノイズ除外）
- [ ] 生成回答の groundedness スコア付与
- [ ] GraphRAG失敗時のフォールバック（Vector-only回答）

---

## PR-09: 出力仕様 (Evidence/Provenance/Citation)

* Depends on: PR-08

- [ ] Evidence サブグラフ返却形式の実装
- [ ] Provenance/Confidence を返却に含める
- [ ] Citation 形式（source + span）を定義・返却
- [ ] time_travel / snapshot_id の優先順序を反映

---

## PR-10: セキュリティ・ガバナンス

* Depends on: PR-01

- [ ] 認証/認可（OAuth/JWT + RBAC/ABAC）
- [ ] 暗号化（TLS/保存時暗号化/KMS連携のフック）
- [ ] 監査ログ（操作・クエリ・model_id 追跡）
- [ ] データレジデンシ/保持期間ポリシーの設定機構

---

## PR-11: 運用機能 (Cache/Time-Travel/Backup)

* Depends on: PR-02, PR-07

- [ ] Semantic Cache（意味的同一クエリの再利用）
- [ ] Time-Travel のスナップショット参照
- [ ] Backup/Restore の実装（PITR含む）
- [ ] 退避/キャッシュポリシーの設定値反映

---

## PR-12: Observability & SLO

* Depends on: PR-01, PR-07

- [ ] レイテンシ/ヒット率/GPU使用率/抽出精度のメトリクス
- [ ] SLO計測（P95/P99）とダッシュボード出力
- [ ] エラーカテゴリ（INVALID_ARGUMENT など）を統一

---

## PR-13: SDK / クライアントAPI

* Depends on: PR-07, PR-09

- [ ] SDK のクライアント実装（ingest/query/response）
- [ ] サンプルコード（SPEC の擬似コード準拠）
- [ ] リトライ/バックオフの方針実装

---

## PR-14: ベンチマーク & 評価

* Depends on: PR-04, PR-08, PR-12

- [ ] ベンチマークスイート（1億ノード/3億エッジ想定）
- [ ] read:write=9:1 プロファイルでの負荷試験
- [ ] GraphRAG 精度（根拠付き回答率）測定

---

## PR-15: ドキュメント整備

* Depends on: PR-13, PR-14

- [ ] 運用ガイド（バックアップ/復元/監査ログ）
- [ ] APIリファレンス（JSON DSL スキーマ/レスポンス）
- [ ] 実運用の制約（GPUメモリ逼迫時の挙動）を明文化

---

## PR-16: レプリケーション / HA（商用化向け）

* Depends on: PR-02, PR-03, PR-07

- [ ] シャード単位のレプリケーション（準同期/遅延許容）
- [ ] フェイルオーバー設計と整合性ポリシー
- [ ] リードルーティング / リード・ユア・ライト検証
- [ ] 災害復旧手順とRPO/RTOの明文化

---

## 依存関係サマリ

- PR-00 → 技術選定と方針確定
- PR-01 → すべての基盤
- PR-02 → PR-03/11/16
- PR-03 → PR-04/07
- PR-04 → PR-08/14
- PR-05 → PR-06
- PR-06 → PR-08
- PR-07 → PR-08/09/11/12/13
- PR-08 → PR-09/14
- PR-09 → PR-13
- PR-10 → 独立 (PR-01依存)
- PR-12 → PR-14
- PR-13 → PR-15
- PR-14 → PR-15
- PR-16 → 商用化フェーズのHA/冗長性
