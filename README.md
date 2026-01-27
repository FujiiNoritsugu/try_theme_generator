# LLMテーマ生成パイプライン

BigQueryのデータからVertex AI LLMでテーマを生成し、Spannerに保存するパイプライン。

## ファイル構成

```
.
├── pipeline.py          # Vertex AI Pipelinesの実装
├── DESIGN.md           # 設計ドキュメント（アーキテクチャ・最適化手法）
├── run_pipeline.py     # 実行スクリプト
└── requirements.txt    # 依存パッケージ
```

## セットアップ

### 1. 依存パッケージのインストール

```bash
pip install -r requirements.txt
```

### 2. GCP認証

```bash
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
```

### 3. 環境準備

#### BigQueryテーブル
既存のテーブルを使用（例: `your-project.your_dataset.your_table`）

#### Spannerテーブル作成

```sql
CREATE TABLE themes_table (
    id STRING(MAX) NOT NULL,
    themes STRING(MAX),
    created_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (id);
```

#### GCSバケット作成

```bash
gsutil mb -l asia-northeast1 gs://your-bucket-name
```

## 実行方法

### パイプラインのコンパイルのみ

```bash
python pipeline.py
```

これにより `llm_pipeline.json` が生成されます。

### パイプラインの実行

```bash
python run_pipeline.py
```

または直接Pythonコードで：

```python
from pipeline import run_pipeline

run_pipeline(
    project_id="your-project-id",
    dataset_id="your_dataset",
    table_id="your_table",
    spanner_instance="your-instance",
    spanner_database="your-database",
    spanner_table="themes_table",
    pipeline_root="gs://your-bucket/pipeline-root",
    batch_size=100,
    max_parallelism=50
)
```

## パラメータ説明

| パラメータ | 説明 | デフォルト値 |
|-----------|------|------------|
| `project_id` | GCPプロジェクトID | 必須 |
| `dataset_id` | BigQueryデータセットID | 必須 |
| `table_id` | BigQueryテーブルID | 必須 |
| `spanner_instance` | Spannerインスタンス名 | 必須 |
| `spanner_database` | Spannerデータベース名 | 必須 |
| `spanner_table` | Spannerテーブル名 | 必須 |
| `pipeline_root` | パイプライン実行のルートGCSパス | 必須 |
| `batch_size` | 1バッチあたりの件数 | 100 |
| `max_parallelism` | 最大並列実行数 | 50 |
| `location` | リージョン | asia-northeast1 |
| `model_name` | 使用するLLMモデル | gemini-1.5-flash |

## トラブルシューティング

### API制限エラー
- Vertex AI APIのクォータを確認
- `max_parallelism`を減らす（例: 20-30）
- バッチサイズを減らす（例: 50）

### メモリ不足
- コンポーネントのメモリ設定を増やす
- バッチサイズを減らす

### Spanner書き込みエラー
- テーブルスキーマを確認
- `pipeline.py`のカラム名を実際のテーブルに合わせる

## 監視

### Cloud Console
https://console.cloud.google.com/vertex-ai/pipelines

### ログ確認
```bash
gcloud logging read "resource.type=ml_job" --limit 50
```

## コスト見積もり

### Gemini API（Flash）
- 入力: $0.00001875/1Kトークン
- 出力: $0.000075/1Kトークン
- 30,000件 × 平均500トークン ≈ $0.30-$1.00

### Vertex AI Pipelines
- 実行時間に応じた課金（Kubernetes）
- 1時間あたり約$0.01-$0.10（リソースによる）

### Spanner
- ノード時間あたりの課金
- 書き込み操作のコスト

## 次のステップ

1. `DESIGN.md`で設計詳細を確認
2. `run_pipeline.py`を実際の環境に合わせて編集
3. 小規模データでテスト実行
4. パラメータチューニング
5. 本番環境での実行
