"""
BigQueryのtest_dataテーブルに30000件のテストデータを挿入するスクリプト
"""

import pandas as pd
from google.cloud import bigquery
import random

# 設定
PROJECT_ID = "gen-lang-client-0471694923"
DATASET_ID = "theme_generator"
TABLE_ID = "test_data"
NUM_RECORDS = 3000

# 旅行関連のサンプルデータ
DESTINATIONS = [
    "東京",
    "京都",
    "大阪",
    "北海道",
    "沖縄",
    "福岡",
    "広島",
    "金沢",
    "鎌倉",
    "箱根",
    "パリ",
    "ロンドン",
    "ニューヨーク",
    "ローマ",
    "バルセロナ",
    "シンガポール",
    "バンコク",
    "ソウル",
    "台北",
    "香港",
    "バリ島",
    "ハワイ",
    "グアム",
    "プーケット",
    "ドバイ",
]

ACTIVITIES = [
    "温泉巡り",
    "グルメツアー",
    "街歩き",
    "美術館巡り",
    "ショッピング",
    "寺社仏閣巡り",
    "自然散策",
    "登山",
    "ビーチリゾート",
    "ナイトマーケット探索",
    "絶景スポット巡り",
    "歴史的建造物見学",
    "カフェ巡り",
    "フォトジェニックスポット巡り",
    "クルーズ",
    "アクティビティ体験",
    "現地料理体験",
    "文化体験",
    "リラクゼーション",
    "絶景ドライブ",
]

THEMES = [
    "家族旅行",
    "一人旅",
    "カップル旅行",
    "女子旅",
    "卒業旅行",
    "新婚旅行",
    "週末旅行",
    "長期滞在",
    "弾丸旅行",
    "ワーケーション",
    "癒しの旅",
    "冒険の旅",
    "グルメ旅",
    "歴史探訪",
    "アート巡り",
    "自然満喫",
    "リゾート旅行",
    "ビジネス＋観光",
]

SEASONS = [
    "春の",
    "夏の",
    "秋の",
    "冬の",
    "桜の季節の",
    "紅葉シーズンの",
    "ゴールデンウィークの",
    "年末年始の",
    "夏休みの",
    "シルバーウィークの",
]


def generate_travel_summary():
    """旅行関連のサマリを生成"""
    templates = [
        f"{random.choice(SEASONS)}{random.choice(DESTINATIONS)}で{random.choice(THEMES)}を楽しむ。{random.choice(ACTIVITIES)}を中心に満喫。",
        f"{random.choice(DESTINATIONS)}への{random.choice(THEMES)}。{random.choice(ACTIVITIES)}と{random.choice(ACTIVITIES)}を体験。",
        f"{random.choice(THEMES)}で{random.choice(DESTINATIONS)}を訪問。{random.choice(ACTIVITIES)}で充実した時間を過ごす。",
        f"{random.choice(DESTINATIONS)}の{random.choice(ACTIVITIES)}を楽しむ{random.choice(THEMES)}。思い出に残る体験。",
        f"{random.choice(SEASONS)}{random.choice(DESTINATIONS)}旅行。{random.choice(ACTIVITIES)}、{random.choice(ACTIVITIES)}など盛りだくさん。",
        f"{random.choice(DESTINATIONS)}で過ごす{random.choice(THEMES)}。{random.choice(ACTIVITIES)}でリフレッシュ。",
        f"{random.choice(THEMES)}プラン：{random.choice(DESTINATIONS)}の{random.choice(ACTIVITIES)}と{random.choice(ACTIVITIES)}。",
        f"{random.choice(DESTINATIONS)}を訪れる{random.choice(THEMES)}。{random.choice(SEASONS)}{random.choice(ACTIVITIES)}を満喫。",
    ]
    return random.choice(templates)


def insert_test_data():
    """BigQueryにテストデータを挿入"""
    print(f"テストデータを生成中... ({NUM_RECORDS}件)")

    # 旅行関連のサマリを生成
    data = {"summary": [generate_travel_summary() for _ in range(NUM_RECORDS)]}

    df = pd.DataFrame(data)
    print(f"データ生成完了: {len(df)}件")

    # BigQueryクライアントを初期化
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    print(f"\nBigQueryにデータを挿入中...")
    print(f"テーブル: {table_ref}")

    # データを挿入
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)

    job.result()  # ジョブの完了を待つ

    print(f"\n完了: {NUM_RECORDS}件のレコードを挿入しました")

    # 確認
    table = client.get_table(table_ref)
    print(f"テーブルの総レコード数: {table.num_rows}")


if __name__ == "__main__":
    try:
        insert_test_data()
    except Exception as e:
        print(f"\nエラーが発生しました: {e}")
        exit(1)
