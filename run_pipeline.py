"""
パイプライン実行スクリプト

実際の環境に合わせて以下の値を変更してください。
"""

from pipeline import run_pipeline

# ========================================
# 設定値（実際の環境に合わせて変更）
# ========================================

# GCP設定
PROJECT_ID = "gen-lang-client-0471694923"
LOCATION = "asia-northeast1"
PIPELINE_ROOT = "gs://try_theme_generator/pipeline-root"

# BigQuery設定
DATASET_ID = "theme_generator"
TABLE_ID = "test_data"

# Spanner設定
SPANNER_INSTANCE = "try-theme-generator-instance"
SPANNER_DATABASE = "try-theme-generator-database"
SPANNER_TABLE = "themes_table"

# パイプライン設定
BATCH_SIZE = 100  # 1バッチあたりの処理件数
MAX_PARALLELISM = 50  # 最大並列実行数
MODEL_NAME = "gemini-1.5-flash"  # 使用するLLMモデル

# ========================================
# 実行
# ========================================

if __name__ == "__main__":
    print("=" * 60)
    print("LLMテーマ生成パイプライン実行")
    print("=" * 60)
    print(f"プロジェクト: {PROJECT_ID}")
    print(f"BigQuery: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    print(f"Spanner: {SPANNER_INSTANCE}/{SPANNER_DATABASE}/{SPANNER_TABLE}")
    print(f"バッチサイズ: {BATCH_SIZE}")
    print(f"最大並列数: {MAX_PARALLELISM}")
    print(f"モデル: {MODEL_NAME}")
    print("=" * 60)

    # 確認
    response = input("\n上記の設定で実行しますか？ (yes/no): ")
    if response.lower() not in ["yes", "y"]:
        print("実行をキャンセルしました。")
        exit(0)

    # パイプライン実行
    print("\nパイプラインを開始します...")

    try:
        job = run_pipeline(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            spanner_instance=SPANNER_INSTANCE,
            spanner_database=SPANNER_DATABASE,
            spanner_table=SPANNER_TABLE,
            pipeline_root=PIPELINE_ROOT,
            location=LOCATION,
            batch_size=BATCH_SIZE,
            max_parallelism=MAX_PARALLELISM,
            model_name=MODEL_NAME,
        )

        print("\n" + "=" * 60)
        print("パイプラインが正常に開始されました！")
        print("=" * 60)
        print(f"ジョブ名: {job.display_name}")
        print(f"リソース: {job.resource_name}")
        print(f"\nステータス確認: https://console.cloud.google.com/vertex-ai/pipelines")
        print("=" * 60)

    except Exception as e:
        print("\n" + "=" * 60)
        print("エラーが発生しました")
        print("=" * 60)
        print(f"エラー内容: {e}")
        print("\n対処方法:")
        print("1. GCP認証を確認: gcloud auth application-default login")
        print("2. プロジェクトIDを確認: gcloud config get-value project")
        print("3. 必要なAPIが有効化されているか確認")
        print("   - Vertex AI API")
        print("   - BigQuery API")
        print("   - Spanner API")
        print("=" * 60)
        exit(1)
