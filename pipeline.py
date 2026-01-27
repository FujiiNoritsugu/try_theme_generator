"""
Vertex AI Pipeline for LLM Theme Generation
BigQuery -> LLM (Vertex AI) -> Spanner
"""

from kfp import dsl
from kfp.dsl import Output, Dataset, Input
from kfp import compiler
from google.cloud.aiplatform import PipelineJob


# BigQueryからデータを分割取得
@dsl.component(base_image="python:3.10", packages_to_install=["google-cloud-bigquery"])
def create_batches(
    project_id: str,
    dataset_id: str,
    table_id: str,
    batch_size: int,
    batches: Output[Dataset]
):
    """BigQueryのデータをバッチに分割"""
    from google.cloud import bigquery
    import json

    client = bigquery.Client(project=project_id)
    query = f"SELECT COUNT(*) as cnt FROM `{project_id}.{dataset_id}.{table_id}`"
    total_count = list(client.query(query))[0].cnt

    batch_list = []
    for offset in range(0, total_count, batch_size):
        batch_list.append({"offset": offset, "limit": batch_size})

    with open(batches.path, 'w') as f:
        json.dump(batch_list, f)


# 各バッチを処理
@dsl.component(
    base_image="python:3.10",
    packages_to_install=[
        "google-cloud-bigquery",
        "google-cloud-spanner",
        "google-cloud-aiplatform",
    ]
)
def process_batch(
    project_id: str,
    dataset_id: str,
    table_id: str,
    spanner_instance: str,
    spanner_database: str,
    spanner_table: str,
    offset: int,
    limit: int,
    location: str = "asia-northeast1",
    model_name: str = "gemini-1.5-flash"
):
    """1バッチのデータを処理：BigQuery取得 -> LLM呼び出し -> Spanner登録"""
    from google.cloud import bigquery, spanner
    import vertexai
    from vertexai.generative_models import GenerativeModel
    import asyncio
    import json

    # BigQueryからデータ取得
    bq_client = bigquery.Client(project=project_id)
    query = f"""
        SELECT * FROM `{project_id}.{dataset_id}.{table_id}`
        LIMIT {limit} OFFSET {offset}
    """
    rows = list(bq_client.query(query))
    print(f"Processing {len(rows)} rows (offset: {offset})")

    # Vertex AI初期化
    vertexai.init(project=project_id, location=location)
    model = GenerativeModel(model_name)

    # 非同期でLLM呼び出し
    async def generate_themes(row):
        """1行のデータから6つのテーマを生成"""
        try:
            # プロンプト作成（実際のデータ構造に合わせて調整してください）
            prompt = f"""以下のデータから6つのテーマ名を生成してください。
JSON形式で出力してください: {{"themes": ["テーマ1", "テーマ2", "テーマ3", "テーマ4", "テーマ5", "テーマ6"]}}

データ: {dict(row)}
"""
            response = await model.generate_content_async(
                prompt,
                generation_config={
                    "temperature": 0.7,
                    "max_output_tokens": 1024,
                }
            )

            # レスポンスからテーマを抽出
            themes_text = response.text.strip()
            # JSON部分を抽出（```json``` で囲まれている場合に対応）
            if "```json" in themes_text:
                themes_text = themes_text.split("```json")[1].split("```")[0].strip()
            elif "```" in themes_text:
                themes_text = themes_text.split("```")[1].split("```")[0].strip()

            themes_data = json.loads(themes_text)
            themes = themes_data.get("themes", [])

            return {
                "id": row.get("id"),  # 実際のID列名に合わせて調整
                "themes": json.dumps(themes, ensure_ascii=False),
                "success": True
            }
        except Exception as e:
            print(f"Error processing row {row.get('id')}: {e}")
            return {
                "id": row.get("id"),
                "themes": json.dumps([]),
                "success": False,
                "error": str(e)
            }

    async def process_all():
        """全ての行を非同期で処理"""
        tasks = [generate_themes(row) for row in rows]
        return await asyncio.gather(*tasks)

    # 並行処理実行
    results = asyncio.run(process_all())

    # 成功した結果のみをSpannerに書き込み
    successful_results = [r for r in results if r.get("success", False)]
    print(f"Successfully processed {len(successful_results)}/{len(results)} rows")

    # Spannerに書き込み
    if successful_results:
        spanner_client = spanner.Client(project=project_id)
        instance = spanner_client.instance(spanner_instance)
        database = instance.database(spanner_database)

        with database.batch() as batch:
            for result in successful_results:
                batch.insert(
                    table=spanner_table,
                    columns=["id", "themes"],  # 実際のカラム名に合わせて調整
                    values=[[result["id"], result["themes"]]]
                )
        print(f"Inserted {len(successful_results)} rows into Spanner")


# パイプライン定義
@dsl.pipeline(
    name="bigquery-llm-spanner-pipeline",
    description="BigQueryのデータからLLMでテーマを生成してSpannerに保存"
)
def llm_theme_generation_pipeline(
    project_id: str,
    dataset_id: str,
    table_id: str,
    spanner_instance: str,
    spanner_database: str,
    spanner_table: str,
    batch_size: int = 100,
    max_parallelism: int = 50,
    location: str = "asia-northeast1",
    model_name: str = "gemini-1.5-flash"
):
    """
    パイプラインのメイン定義

    Args:
        project_id: GCPプロジェクトID
        dataset_id: BigQueryデータセットID
        table_id: BigQueryテーブルID
        spanner_instance: Spannerインスタンス名
        spanner_database: Spannerデータベース名
        spanner_table: Spannerテーブル名
        batch_size: 1バッチあたりの処理件数
        max_parallelism: 最大並列実行数
        location: リージョン
        model_name: 使用するLLMモデル名
    """
    # バッチ作成
    batches_task = create_batches(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        batch_size=batch_size
    )

    # 並列処理
    with dsl.ParallelFor(
        items=batches_task.outputs["batches"],
        parallelism=max_parallelism  # 最大並列数
    ) as batch:
        process_batch(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            spanner_instance=spanner_instance,
            spanner_database=spanner_database,
            spanner_table=spanner_table,
            offset=batch.offset,
            limit=batch.limit,
            location=location,
            model_name=model_name
        )


def compile_pipeline(output_path: str = "llm_pipeline.json"):
    """パイプラインをコンパイル"""
    compiler.Compiler().compile(
        pipeline_func=llm_theme_generation_pipeline,
        package_path=output_path
    )
    print(f"Pipeline compiled to {output_path}")


def run_pipeline(
    project_id: str,
    dataset_id: str,
    table_id: str,
    spanner_instance: str,
    spanner_database: str,
    spanner_table: str,
    pipeline_root: str,
    location: str = "asia-northeast1",
    batch_size: int = 100,
    max_parallelism: int = 50,
    model_name: str = "gemini-1.5-flash"
):
    """パイプラインを実行"""

    # コンパイル
    compile_pipeline()

    # 実行
    job = PipelineJob(
        display_name="llm-theme-generation",
        template_path="llm_pipeline.json",
        pipeline_root=pipeline_root,
        parameter_values={
            "project_id": project_id,
            "dataset_id": dataset_id,
            "table_id": table_id,
            "spanner_instance": spanner_instance,
            "spanner_database": spanner_database,
            "spanner_table": spanner_table,
            "batch_size": batch_size,
            "max_parallelism": max_parallelism,
            "location": location,
            "model_name": model_name
        },
        location=location,
        project=project_id
    )

    job.run()
    print(f"Pipeline job started: {job.resource_name}")
    return job


if __name__ == "__main__":
    # 使用例
    # パイプラインをコンパイルのみ
    compile_pipeline()

    # または実行（実際の値に置き換えてください）
    # run_pipeline(
    #     project_id="your-project-id",
    #     dataset_id="your_dataset",
    #     table_id="your_table",
    #     spanner_instance="your-instance",
    #     spanner_database="your-database",
    #     spanner_table="themes_table",
    #     pipeline_root="gs://your-bucket/pipeline-root",
    #     batch_size=100,
    #     max_parallelism=50
    # )
