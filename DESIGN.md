# LLMテーマ生成パイプライン 設計ドキュメント

## 概要

BigQueryに登録された3万件のデータをVertex AIのLLMで処理し、各データから6つのテーマを生成してSpannerに登録するパイプラインシステム。

## アーキテクチャ

```
BigQuery (30,000件)
    ↓
[バッチ分割] (300バッチ × 100件)
    ↓
[並列処理] (最大50並列)
    ↓ (各バッチ内で非同期処理)
LLM呼び出し (Vertex AI / Gemini)
    ↓
Spanner (結果保存)
```

## 処理フロー

### 1. バッチ分割コンポーネント (`create_batches`)
- BigQueryからデータ総数を取得
- 指定サイズ（デフォルト100件）でバッチに分割
- 各バッチの`offset`と`limit`を生成

### 2. バッチ処理コンポーネント (`process_batch`)
各バッチで以下を実行：
1. BigQueryからデータ取得（`LIMIT`と`OFFSET`使用）
2. LLMを非同期で呼び出し（`asyncio.gather`）
3. 結果をSpannerにバッチ挿入

### 3. 並列実行
- Vertex AI Pipelinesの`ParallelFor`で並列実行
- 最大並列数：50（調整可能）

## 処理時間短縮のための最適化手法

### 1. 並列処理戦略

#### パイプラインレベル
- **ParallelFor**: 最大50バッチを同時実行
- 各バッチは独立したKubernetes Podとして実行

#### バッチ内レベル
- **非同期処理**: `asyncio`で複数のLLM呼び出しを並行実行
- バッチサイズ100件 → 100件を非同期で同時処理

#### 並列度の計算例
```
バッチ並列数: 50
バッチサイズ: 100件
バッチ内非同期並列: 100件

理論上の最大並列処理数 = 50 × 100 = 5,000リクエスト
（実際はAPI制限により調整される）
```

### 2. LLM呼び出しの最適化

#### APIレート制限対策
- **Gemini Flash**: RPM/TPM制限を考慮
- エラーハンドリングとリトライ機能実装
- レート制限エラー時の自動バックオフ

#### プロンプト最適化
- 簡潔なプロンプト設計でトークン数削減
- JSON形式での出力指定で解析を簡素化
- `temperature`や`max_output_tokens`の適切な設定

#### モデル選択
- **Gemini 1.5 Flash**: 高速・低コスト（推奨）
- **Gemini 1.5 Pro**: より高品質が必要な場合

### 3. データベースアクセス最適化

#### BigQuery
- `LIMIT`と`OFFSET`で効率的なページネーション
- 必要なカラムのみ取得（`SELECT *`は実際には調整推奨）
- パーティショニングテーブルの活用

#### Spanner
- **バッチ挿入**: `database.batch()`で複数行を一括挿入
- 書き込みスループット向上
- トランザクションオーバーヘッド削減

### 4. リソース設定

#### コンピュートリソース
```python
@dsl.component(
    base_image="python:3.10",
    cpu_request="2",      # CPU要求
    memory_request="4G",  # メモリ要求
)
```

#### パイプライン設定
- `batch_size`: 100件（調整可能）
- `max_parallelism`: 50（調整可能）
- リージョン: `asia-northeast1`（レイテンシ削減）

### 5. エラーハンドリング

#### リトライ戦略
- LLM APIの一時的なエラーに対応
- 失敗した行はログに記録
- 成功した結果のみSpannerに登録

#### 部分的な失敗の許容
- 1バッチ内の一部失敗でもパイプライン継続
- 失敗データは別途再処理可能

## パラメータチューニングガイド

### バッチサイズの決定
| バッチサイズ | バッチ数 | 並列度 | 推奨ケース |
|------------|---------|--------|-----------|
| 50件 | 600 | 高 | API制限が厳しい場合 |
| 100件 | 300 | 中 | **推奨デフォルト** |
| 200件 | 150 | 低 | API制限が緩い場合 |

### 最大並列数の決定
| 並列数 | 特徴 | リスク |
|-------|------|--------|
| 10-20 | 安全 | 処理時間が長い |
| 30-50 | **推奨** | バランスが良い |
| 50-100 | 高速 | API制限・リソース競合 |

### 処理時間の見積もり
```
前提条件:
- データ数: 30,000件
- バッチサイズ: 100件
- 並列数: 50
- 1件あたりLLM処理時間: 2秒

計算:
総バッチ数 = 30,000 / 100 = 300バッチ
並列実行時のラウンド数 = 300 / 50 = 6ラウンド
1ラウンドの処理時間 = 100件 × 2秒 / 100並列 = 2秒
総処理時間 ≈ 6 × 2秒 = 12秒

※実際はオーバーヘッド、API制限、ネットワーク遅延により数分〜数十分
```

## 実装時の注意点

### 1. API制限
- Vertex AI APIのクォータを事前確認
- プロジェクトのRPM/TPM制限を確認
- 必要に応じてクォータ引き上げを申請

### 2. コスト管理
- Gemini Flashで約$0.00001875/1Kトークン
- 30,000件 × 6テーマ × 平均トークン数で見積もり
- Vertex AI Pipelinesの実行コスト（Kubernetes）

### 3. データスキーマ
- BigQueryのテーブル構造に合わせて調整
- Spannerのテーブル定義を事前作成
- ID列名、カラム名を実際の環境に合わせる

### 4. 監視とロギング
- Cloud Loggingでパイプライン実行状況を確認
- 失敗したバッチの特定と再処理
- 処理時間とコストのモニタリング

## 代替アプローチ

### オプション1: Batch Prediction API
```python
# より大規模なバッチ処理に適している
from google.cloud import aiplatform

batch_prediction_job = aiplatform.BatchPredictionJob.create(
    job_display_name="theme-generation",
    model_name="gemini-1.5-flash",
    input_source="bq://project.dataset.table",
    output_destination="bq://project.dataset.output_table"
)
```
- **メリット**: 非同期、低コスト、大規模処理向け
- **デメリット**: 結果までに時間がかかる、リアルタイム性が低い

### オプション2: Cloud Run Jobs + Cloud Tasks
```
Cloud Tasks (キュー)
    ↓
Cloud Run Jobs (並列実行)
    ↓
LLM呼び出し + Spanner書き込み
```
- **メリット**: より細かいスケーリング制御、長時間実行可能
- **デメリット**: 実装が複雑、パイプライン管理の利点が減る

### オプション3: Dataflow
```python
# Apache Beamでストリーミング/バッチ処理
import apache_beam as beam

with beam.Pipeline() as p:
    (p
     | beam.io.ReadFromBigQuery(query=query)
     | beam.ParDo(CallLLM())
     | beam.io.WriteToSpanner(instance, database, table))
```
- **メリット**: 大規模データ処理に最適、自動スケーリング
- **デメリット**: 学習コスト、セットアップが複雑

## 次のステップ

1. **環境準備**
   - BigQueryテーブルの確認
   - Spannerテーブルの作成
   - GCSバケット作成（Pipeline Root用）

2. **実装調整**
   - `pipeline.py`のスキーマ情報を実際の環境に合わせる
   - プロンプトを業務要件に合わせて調整

3. **テスト実行**
   - 小規模データ（100件）でテスト
   - パラメータチューニング
   - エラーハンドリングの確認

4. **本番実行**
   - 監視体制の準備
   - クォータ制限の確認
   - スケジュール実行の設定（必要な場合）

## 参考リンク

- [Vertex AI Pipelines](https://cloud.google.com/vertex-ai/docs/pipelines)
- [Kubeflow Pipelines SDK](https://www.kubeflow.org/docs/components/pipelines/)
- [Vertex AI Gemini API](https://cloud.google.com/vertex-ai/docs/generative-ai/model-reference/gemini)
- [Cloud Spanner Python Client](https://cloud.google.com/spanner/docs/reference/libraries)
