import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Import your business logic from the include folder
from include.scrapper import NewsScraper
from include.settings import SEARCH_KEYWORDS, S3_BUCKET


def run_news_extraction_and_upload():
    """Extracts news from API and uploads raw JSON to S3"""
    scraper = NewsScraper()
    s3_hook = S3Hook(aws_conn_id="aws_default")
    now = datetime.now()

    for word in SEARCH_KEYWORDS:
        print(f"--- Starting Extraction: {word} ---")
        articles = scraper.fetch_news(word, page_size=50)

        if articles:
            # Partitioned S3 Key: bronze/news/year/month/day/keyword/file.json
            s3_key = (
                f"bronze/news/"
                f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
                f"keyword={word}/{word}_{now.strftime('%H%M')}.json"
            )

            # Upload directly from memory
            s3_hook.load_string(
                string_data=json.dumps(articles, indent=4),
                key=s3_key,
                bucket_name=S3_BUCKET,
                replace=True,
            )
            print(f"✅ S3 Upload Success: {s3_key}")
        else:
            print(f"⚠️ No data found for {word}")


# Define the DAG
with DAG(
    dag_id="news_intelligence_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    # 1. PYTHON TASK: Scrape API -> S3
    extract_to_s3 = PythonOperator(
        task_id="extract_news_to_s3", python_callable=run_news_extraction_and_upload
    )

    # 2. SNOWFLAKE TASK: S3 -> Bronze (Raw Table)
    bronze_load = SQLExecuteQueryOperator(
        task_id="bronze_s3_to_raw",
        conn_id="snowflake_conn",  # Changed parameter from snowflake_conn_id to conn_id
        sql="""
            COPY INTO raw_news_data (raw_json, filename)
            FROM (
              SELECT $1, METADATA$FILENAME 
              FROM @news_bronze_stage
            )
            FILE_FORMAT = (FORMAT_NAME = 'news_json_ff')
            ON_ERROR = 'CONTINUE';
        """,
    )

    # 3. SNOWFLAKE TASK: Bronze -> Silver (Deduplicated & Structured)
    silver_transform = SQLExecuteQueryOperator(
        task_id="silver_raw_to_structured",
        conn_id="snowflake_conn",
        sql="""
            MERGE INTO news_silver AS target
            USING (
                SELECT 
                    raw_json:id::STRING AS id,
                    raw_json:author::STRING AS author,
                    raw_json:title::STRING AS title,
                    raw_json:description::STRING AS description,
                    raw_json:url::STRING AS sourceurl,
                    raw_json:published_at::STRING AS published_at,
                    raw_json:content::STRING AS content,
                    raw_json:keyword::STRING AS keyword,
                    raw_json:source::STRING as source,
                    filename,
                    inserted_at AS loaded_at
                FROM raw_news_data
                WHERE title IS NOT NULL AND title != '[Removed]'
                QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY inserted_at DESC) = 1
            ) AS source
            ON target.id = source.id
            WHEN NOT MATCHED THEN
                INSERT (id, author, title, description, sourceurl, published_at, content, keyword, source, filename, loaded_at)
                VALUES (source.id, source.author, source.title, source.description, source.sourceurl, source.published_at, source.content, source.keyword, source.source, source.filename, source.loaded_at);
        """,
    )

    # 4. SNOWFLAKE TASK: Silver -> Gold (Aggregated Trends)
    # 4. GOLD: Incremental Sentiment (Using the reliable ds_add macro)
    gold_sentiment = SQLExecuteQueryOperator(
        task_id="gold_sentiment_incremental",
        conn_id="snowflake_conn",
        sql="""
                -- 1. Remove existing data for 'yesterday' to allow for clean reruns
                DELETE FROM news_gold_sentiment_metrics 
                WHERE news_date = '{{ macros.ds_add(ds, -1) }}';

                -- 2. Insert fresh calculations for 'yesterday'
                INSERT INTO news_gold_sentiment_metrics (keyword, news_date, avg_sentiment_score, article_count, overall_tone)
                SELECT 
                    keyword,
                    DATE(TO_TIMESTAMP_NTZ(published_at)) AS news_date,
                    AVG(SNOWFLAKE.CORTEX.SENTIMENT(title)) AS avg_sentiment_score,
                    COUNT(*) AS article_count,
                    CASE 
                        WHEN AVG(SNOWFLAKE.CORTEX.SENTIMENT(title)) > 0.15 THEN 'Positive'
                        WHEN AVG(SNOWFLAKE.CORTEX.SENTIMENT(title)) < -0.15 THEN 'Negative'
                        ELSE 'Neutral'
                    END AS overall_tone
                FROM news_silver
                -- Filter logic: only looking at the date relative to the DAG run
                WHERE DATE(TO_TIMESTAMP_NTZ(published_at)) = '{{ macros.ds_add(ds, -1) }}'
                GROUP BY 1, 2;
            """,
    )

    # 5. GOLD: Running Totals (Often better as Full Refresh unless data is massive)
    # These tables don't have a specific date, so we refresh them to keep them accurate
    gold_source = SQLExecuteQueryOperator(
        task_id="gold_source_authority",
        conn_id="snowflake_conn",
        sql="""
                CREATE OR REPLACE TABLE news_gold_source_metrics AS
                SELECT source, keyword, COUNT(*) as article_count
                FROM news_silver
                GROUP BY 1, 2;
            """,
    )

    # ... (Define other Gold tasks)

    # DEPENDENCY CHAIN
    # Task 4 is now incremental and efficient!
    extract_to_s3 >> bronze_load >> silver_transform >> [gold_sentiment, gold_source]
