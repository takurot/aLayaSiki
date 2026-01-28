商用化を前提とした、次世代AIネイティブDB **「aLayaSiki (仮称)」** の機能仕様書を作成しました。

このDBのコアコンセプトは、**「Insert Raw, Retrieve Reasoned（生のデータを入れれば、論理付けされた答えが返る）」**です。開発者がETLパイプラインやRAGの複雑な仕組みを構築する必要をなくし、データベース自体が情報の構造化（グラフ化）と意味理解（ベクトル化）を自律的に行うシステムです。

---

# Product Requirements Document (PRD): aLayaSiki

## 1. 製品概要

* **製品名:** aLayaSiki (アラヤシキ)
* **カテゴリ:** Autonomous GraphRAG Database (自律型グラフRAGデータベース)
* **ターゲット:** LLMアプリケーション開発者、エンタープライズAI基盤
* **核心的価値:** データの「ベクトル化」「グラフ構築」「検索」「推論」をワンストップで提供し、推論レイテンシを最小化する。

### 1.1. 目的

* 非構造データからの検索・推論を、ETLや個別のRAG構築なしで実現する。
* GPU近接計算により、推論に必要な文脈取得のレイテンシを最小化する。
* Graph + Vectorの一体運用で、関連性の探索と論理的なつながりの追跡を容易にする。

### 1.2. 非目的

* 既存OLTP/OLAPデータベースの完全置換。
* 大規模LLMのトレーニング基盤。
* すべての推論を100%自動化すること（人間レビューが必要な領域を想定）。

### 1.3. 想定ユースケース

* 社内ナレッジの自動構造化と質問応答（規程・契約・議事録）。
* マルチモーダル監査・調査（PDF + 画像 + 音声の横断探索）。
* 企業向けRAGアプリの基盤DB（説明可能性・根拠提示が必須）。

### 1.4. 成功指標 (暫定)

* Retrieval-only のP95レイテンシはサブセカンドを目標。
* 2〜3 hopのGraphRAGでも数秒以内での応答を目標。
* 生成回答の根拠付き出力率（evidence添付率）> 95%。
* 運用コストは同等の外部RAG構成比で総コスト削減を目標。
* **評価条件 (暫定):**
  * データ規模: 1億ノード / 3億エッジ、ベクトル次元 1024。
  * 負荷プロファイル: read:write = 9:1、同時接続 500、P95測定。
  * キャッシュヒット率: 30% を想定。

### 1.5. 非機能要件 (SLO 方針)

* **可用性:** 99.9%を目標。
* **耐久性:** 重要データは複数レプリカで保持し、RPO/RTOを明示する。
* **セキュリティ:** 企業導入を前提に監査ログと暗号化を必須。

---

## 2. コア・アーキテクチャ仕様

### 2.1. ストレージ & コンピュート統合 (Neural-Storage Engine)

従来の「Compute」と「Storage」の分離を廃止し、データが存在する場所で計算を行います。

* **GPU-First Persistence:**
  * プライマリデータストアとしてNVMe SSDを使用するが、**ホットデータとインデックスは常時VRAM (GPUメモリ) に常駐**させるティアリング構造。
* **Zero-Copy Access:**
  * CPUを経由せず、ストレージからGPUへDMA転送 (NVIDIA GPUDirect Storage準拠技術)。
* **Embedded SLM (Small Language Model):**
  * 各シャード（データ区画）ごとに、軽量な言語モデル（例: 1B〜3Bパラメータクラス）が常駐し、データの取り込み時の処理（ETL）を担当。

### 2.2. データモデル: "Vector-Graph Hybrid"

ノード（点）とエッジ（線）の双方に、高次元ベクトルとメタデータを保持します。

* **Node (Entity):** テキストチャンク、画像、固有表現（人名、地名など）。
  * 属性: `raw_data`, `embedding`, `metadata (JSON)`, `provenance`, `confidence`, `model_id`
* **Edge (Relation):** ノード間の関係性。
  * 属性: `relation_type` (例: is_part_of, contradicts), `weight`, `direction`, `provenance`, `confidence`
* **Entity Resolution:**
  * 同一エンティティ統合のための正規化ルールと同一性スコアを保持。
* **Hyper-Index:**
  * ベクトル検索用のANNインデックスと、グラフ探索用の隣接リストを**同一メモリ空間でマッピング**し、O(1)で相互参照可能にする。
  * インデックス更新は原子的に行い、検索結果の整合性を担保する。

### 2.3. 一貫性・耐久性・レプリケーション

* **Write Path:**
  * すべての書き込みはNVMe上のWALに永続化後にACKする。
* **レプリケーション:**
  * シャード単位での複製を基本とし、可用性と耐久性を担保。
* **Read Semantics:**
  * 同一セッション内での read-your-writes を保証し、シャード間は準同期を想定。
* **バックアップ/スナップショット:**
  * 時点復元を可能にするスナップショット運用を標準化。

### 2.4. Embedded SLMの運用

* **モデルレジストリ:** `model_id` によるバージョン管理とロールバックを提供。
* **A/Bテスト:** 同一データに対する抽出品質の比較を実施。
* **コスト制御:** SLM実行の予算と頻度をポリシーで制御。
* **フェイルセーフ:** SLM停止時はベクトル化のみで取り込み継続可能。
* **再現性:** クエリは `model_id` とデータスナップショットを固定可能にし、結果の再現性を担保。

---

## 3. 機能詳細仕様

### 3.1. 自律的データ取り込み (Autonomous Ingestion)

開発者はファイルを投げるだけです。外部のPythonスクリプトによる前処理は不要です。

* **Multi-Modal Ingestion API:**
  * PDF, JSON, Markdown, 画像, 音声データをそのまま受け入れるエンドポイント。
* **Auto-Chunking & Embedding:**
  * 入力データの内容（意味の区切り）をSLMが解析し、動的にチャンク分割。自動でベクトル化を実行。
* **Auto-Graph Construction (自動グラフ構築):** 【最大の差別化機能】
  * テキスト読み込み時に、SLMが**固有表現抽出 (NER)** と **関係抽出 (Relation Extraction)** をリアルタイムで実行。
  * 例: 「A社はB社を買収した」というテキストから、`Node(A社)` --`[acquired]`--> `Node(B社)` というグラフ構造を自動生成し、データベースに格納する。
* **Idempotency & Dedup:**
  * `content_hash` と `idempotency_key` により重複投入を防止。
* **ポリシー実行:**
  * PIIマスキング、禁止語フィルタ、リージョン制約を取り込み時に適用。
* **バックプレッシャ:** GPU/VRAM逼迫時は取り込みをキューイングし、再試行可能にする。

### 3.2. クエリ & 検索インターフェース

SQLは必須ではなく、自然言語と構造化JSONによるハイブリッドクエリを採用します。

* **Natural Language Query:**
  * `query("トヨタのEV戦略における競合との違いは？")` のような自然言語を受け付け、内部で検索プランに変換。
* **Structured JSON DSL:**
  * 時間範囲、エンティティタイプ、関係タイプ、深さなどを指定可能。
  * 例:
    ```json
    {
      "query": "トヨタのEV戦略の差分",
      "filters": {
        "entity_type": ["Company"],
        "time_range": {"from": "2023-01-01", "to": "2024-12-31"}
      },
      "traversal": {"depth": 2, "relation_types": ["competitor_of"]},
      "top_k": 50
    }
    ```
* **GraphRAG Traversal (多段階推論検索):**
  * 単なる類似度検索ではなく、以下の3ステップをDB内部で高速実行する。
    1. **Vector Search:** クエリに近いノード（アンカー）を特定。
    2. **Graph Expansion:** アンカーからエッジを辿り、関連するコンテキスト（2-hop, 3-hop先）を収集。
    3. **Context Pruning:** 収集した情報から、クエリと矛盾するものやノイズをLLMがフィルタリング。
* **Explain Plan:**
  * 実行された検索プラン（アンカー、拡張経路、除外理由）を取得可能。
* **Query Mode:**
  * `answer` は生成込み、`evidence` は根拠サブグラフのみ返却。
* **Reproducibility:**
  * `model_id` と `snapshot_id` を指定した場合、検索/生成の再現性を保証。

#### 3.2.1. JSON DSL スキーマ (暫定)

* **query** (string, required): 自然言語クエリ。
* **filters** (object, optional):
  * **entity_type** (string[], optional)
  * **relation_type** (string[], optional)
  * **time_range** (object, optional): `{ "from": "YYYY-MM-DD", "to": "YYYY-MM-DD" }`
* **traversal** (object, optional):
  * **depth** (number, optional, default=1)
  * **relation_types** (string[], optional)
* **top_k** (number, optional, default=20)
* **mode** (string, optional): `answer` | `evidence`
* **model_id** (string, optional)
* **snapshot_id** (string, optional)

### 3.3. 出力仕様

* **Output Format:**
  * 単なるドキュメントリストではなく、**「回答生成に必要なサブグラフ（関係図）」**または**「生成された回答そのもの」**を返すモードを選択可能。
* **Evidence & Provenance:**
  * 返却するノード/エッジには出典、抽出モデル、信頼度スコアを付与。
* **Groundedness:**
  * 生成回答には根拠一致率（スコア）と引用リストを付随する。

### 3.4. 運用・管理機能 (Ops)

* **Learned Index Optimization:**
  * クエリの傾向を学習し、頻繁にアクセスされる「推論パス（思考回路）」をショートカットとしてインデックス化。
* **Semantic Cache:**
  * 完全に一致するクエリだけでなく、「意味的に同じ質問」が来た場合、過去の生成結果を即座に返すキャッシュ機能。
* **Time-Travel & Versioning:**
  * Gitのようにデータのバージョン管理を行い、「2023年時点のデータでの推論結果」を再現可能にする。
* **Observability:**
  * レイテンシ、GPU使用率、クエリヒット率、抽出精度をメトリクスとして提供。
* **Backup & Restore:**
  * スナップショットとポイントインタイムリカバリを提供。

### 3.5. セキュリティ & ガバナンス

* **認証・認可:** OAuth/JWT + RBAC/ABACをサポート。
* **暗号化:** 転送時TLS、保存時暗号化、KMS連携。
* **監査ログ:** 操作・クエリ・モデルバージョンの監査証跡を保持。
* **データ削除:** 削除要求 (Right to be forgotten) に準拠した完全削除と追跡可能な削除ログ。
* **データレジデンシ:** リージョン固定と越境制御をポリシーで保証。
* **保持期間:** データ保持期間と削除ポリシーをテナント単位で設定可能。
* **テナント分離:** 共有クラスタでも論理分離とGPUリソース分離を保証。

---

## 4. API設計イメージ (擬似コード)

開発者が操作する際のシンプルさを最優先します。

```python
import aLayaSiki

db = aLayaSiki.Client(mode="gpu_direct", tenant="acme")

# 1. データの投入
db.ingest(
    source="./apple_news_2024.pdf",
    auto_graph=True,
    idempotency_key="sha256:...",
    policy={"pii_redact": True, "region": "ap-northeast-1"}
)

# 2. 検索・推論
response = db.query(
    prompt="Vision Proの発売による、競合他社（Metaなど）の株価への影響と考えられる要因は？",
    strategy="multi_hop",
    depth=2,
    mode="answer",  # answer | evidence
    time_travel="2024-12-31",
    model_id="slm-2.1.0",
    snapshot_id="snap-2024-12-31"
)

# 3. 結果の取得
print(response.answer)
print(response.evidence)
print(response.citations)
print(response.groundedness)
```

`time_travel` は `YYYY-MM-DD` もしくは `RFC3339` 形式を受け付ける。`snapshot_id` を指定した場合は `time_travel` より優先される。

### 4.1. 代表的なレスポンス構造

```json
{
  "answer": "...",
  "evidence": {"nodes": [], "edges": []},
  "citations": [{"source": "s3://...", "span": [12, 34]}],
  "groundedness": 0.87,
  "model_id": "slm-2.1.0",
  "snapshot_id": "snap-2024-12-31",
  "latency_ms": 920
}
```

### 4.2. エラーハンドリング (概要)

* **INVALID_ARGUMENT:** 入力形式不正、未知のrelation_type等。
* **RESOURCE_EXHAUSTED:** GPU/VRAM逼迫、クォータ超過。
* **NOT_FOUND:** 指定したスナップショットやエンティティが存在しない。
* **UNAUTHENTICATED / PERMISSION_DENIED:** 認証・認可エラー。

---

## 5. ロードマップと商用化フェーズ

### Phase 1: コアエンジンの確立 (MVP)

* **機能:** GPUネイティブなベクトル検索 + 基本的なグラフ構造の保持。
* **制限:** グラフ構築は手動（API経由）。
* **ターゲット:** ベクトルDBの速度に不満を持つハイエンドユーザー。
* **成功基準:** Retrieval-only でサブセカンドP95、安定稼働。

### Phase 2: 自動化とGraphRAG (Alpha)

* **機能:** SLM搭載による「自動グラフ構築」の実装。GraphRAGアルゴリズムの内蔵。
* **ターゲット:** RAG精度の向上（ハルシネーション低減）を目指す企業。
* **成功基準:** GraphRAG の根拠付き回答率 > 90%。

### Phase 3: エコシステムと学習型インデックス (GA - 一般提供)

* **機能:** クエリログからのインデックス自己最適化。主要LLMフレームワーク（LangChain, LlamaIndex）とのネイティブ統合。
* **ビジネスモデル:**
  * **Managed Cloud:** 使用したVRAM容量とCompute時間による課金。
  * **On-Premise:** 金融・医療機関向けのプライベートインスタンス（ライセンス販売）。
* **成功基準:** 企業利用での監査・ガバナンス要件を満たす。

---

## 6. 技術的な懸念点と検証計画

### 6.1. 懸念点

* **自動グラフ構築の精度とコスト**
* **GPUメモリ逼迫時の性能劣化**
* **モデルドリフトによる関係抽出精度の低下**
* **クエリの説明可能性不足による運用リスク**

### 6.2. 解決策と検証方針

* **遅延グラフ構築（Lazy Graph Construction）**
  * データ投入時はまずベクトル化だけ行い（高速）、バックグラウンドプロセスで、またはそのデータが初めてアクセスされた時に、詳細なグラフ関係性を抽出・構築する仕様とする。
* **キャッシュ/退避ポリシー**
  * VRAM常駐データの優先度と退避ルールを明文化。
* **評価指標の設定**
  * 関係抽出のPrecision/Recall、根拠付き回答率、P95レイテンシを継続測定。

---

この仕様書に基づいた開発であれば、既存のPineconeやNeo4jとは全く異なる、**「AIのためのOS」**のような立ち位置のデータベース製品が実現可能です。
