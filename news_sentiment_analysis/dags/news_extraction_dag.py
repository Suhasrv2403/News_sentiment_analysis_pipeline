"""News Intelligence medallion-architecture Airflow DAG.

Orchestrates the end-to-end pipeline: fetch articles from NewsAPI for each
configured keyword, land the raw JSON in S3 (Bronze), copy it into a raw
Snowflake table, deduplicate/structure it into a Silver table, then
aggregate Snowflake Cortex sentiment scores and source-authority counts
into Gold tables.

Inputs: ``configs/config.yaml`` (search keywords, S3 bucket, sentiment
threshold, DAG schedule — see include/settings.py for the full list) via
``include/settings.py``; Airflow connections ``aws_default`` and
``snowflake_conn``.
Outputs: JSON objects under ``s3://<S3_BUCKET>/<bronze_prefix>/...`` and
the Snowflake tables ``raw_news_data``, ``news_silver``,
``news_gold_sentiment_metrics``, ``news_gold_source_metrics``.

Reproducibility note: this pipeline has no RNG-driven behavior to seed —
its only nondeterminism is external (NewsAPI result ordering/content and
Snowflake Cortex's sentiment model), which no seed controls. Every value
that previously varied across environments or was hardcoded in this file
is now sourced from config.yaml instead (see include/settings.py).
"""

import json
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Import your business logic from the include folder
from include.scrapper import NewsScraper
from include.settings import (
    SEARCH_KEYWORDS,
    S3_BUCKET,
    S3_BRONZE_PREFIX,
    NEWS_PAGESIZE,
    SENTIMENT_NEUTRAL_BAND,
    DAG_ID,
    DAG_START_DATE,
    DAG_SCHEDULE,
    DAG_CATCHUP,
)


def run_news_extraction_and_upload() -> None:
    """Fetch news for every configured keyword and upload raw JSON to S3.

    For each keyword in ``SEARCH_KEYWORDS``, calls ``NewsScraper.fetch_news``
    and, if any articles are returned, writes them as a single JSON file to
    a date- and keyword-partitioned S3 key under the Bronze layer
    (``bronze/news/year=.../month=.../day=.../keyword=.../*.json``).
    Keywords with no results are skipped and logged, not treated as errors.

    Returns:
        None. Side effect: uploads objects to ``S3_BUCKET`` via
        ``S3Hook``.
    """
    scraper = NewsScraper()
    s3_hook = S3Hook(aws_conn_id="aws_default")
    now = datetime.now()

    for word in SEARCH_KEYWORDS:
        print(f"--- Starting Extraction: {word} ---")
        articles = scraper.fetch_news(word, page_size=NEWS_PAGESIZE)

        if articles:
            # Partitioned S3 Key: <bronze_prefix>/year/month/day/keyword/file.json
            s3_key = (
                f"{S3_BRONZE_PREFIX}/"
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
    dag_id=DAG_ID,
    start_date=datetime.fromisoformat(DAG_START_DATE),
    schedule=DAG_SCHEDULE,
    catchup=DAG_CATCHUP,
) as dag:

    # 1. PYTHON TASK: Scrape API -> S3
    extract_to_s3 = PythonOperator(
        task_id="extract_news_to_s3", python_callable=run_news_extraction_and_upload
    )

    # 2. SNOWFLAKE TASK: S3 -> Bronze (Raw Table)
    # Column lists below (raw_news_data, news_silver, news_gold_sentiment_metrics)
    # must match snowflake_setup.sql, which is the canonical schema definition —
    # SQL has no cross-file import, so this is a manual contract; update both
    # files together if you add/rename a column.
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
                -- NewsAPI returns a placeholder title of '[Removed]' for
                -- articles that were taken down after indexing; these carry
                -- no usable content so they're dropped rather than merged.
                WHERE title IS NOT NULL AND title != '[Removed]'
                -- raw_news_data can contain the same article id multiple
                -- times (re-fetched across DAG runs); keep only the most
                -- recently loaded copy so the MERGE below sees one row per id.
                QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY inserted_at DESC) = 1
            ) AS source
            ON target.id = source.id
            WHEN NOT MATCHED THEN
                INSERT (id, author, title, description, sourceurl, published_at, content, keyword, source, filename, loaded_at)
                VALUES (source.id, source.author, source.title, source.description, source.sourceurl, source.published_at, source.content, source.keyword, source.source, source.filename, source.loaded_at);
        """,
    )

    # 4. GOLD: Incremental Sentiment (Using the reliable ds_add macro)
    # SENTIMENT_NEUTRAL_BAND (from config.yaml, default 0.15) is injected via
    # a %%TOKEN%% + str.replace() rather than an f-string or .format(),
    # because the SQL below also contains Jinja templating ('{{ ... }}')
    # that Airflow renders at task-execution time — an f-string would try
    # to evaluate those braces as Python, and .format() would swallow them
    # as escaped literal braces. Plain substring replace avoids both.
    _gold_sentiment_sql = """
                -- Processes ds - 1 (yesterday relative to the DAG's logical
                -- date), not ds itself: the DAG runs on @daily schedule at
                -- the start of the day, so "today's" articles are still
                -- arriving when this task executes. Delete+insert (rather
                -- than MERGE) keeps reruns for the same date idempotent.
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
                    -- SNOWFLAKE.CORTEX.SENTIMENT returns a score in [-1, 1].
                    -- The neutral band below is a "dead zone" around 0 so
                    -- that mildly-worded headlines land in 'Neutral' rather
                    -- than flipping to Positive/Negative on small score noise.
                    CASE
                        WHEN AVG(SNOWFLAKE.CORTEX.SENTIMENT(title)) > %%NEUTRAL_BAND%% THEN 'Positive'
                        WHEN AVG(SNOWFLAKE.CORTEX.SENTIMENT(title)) < -%%NEUTRAL_BAND%% THEN 'Negative'
                        ELSE 'Neutral'
                    END AS overall_tone
                FROM news_silver
                -- Filter logic: only looking at the date relative to the DAG run
                WHERE DATE(TO_TIMESTAMP_NTZ(published_at)) = '{{ macros.ds_add(ds, -1) }}'
                GROUP BY 1, 2;
            """.replace("%%NEUTRAL_BAND%%", str(SENTIMENT_NEUTRAL_BAND))

    gold_sentiment = SQLExecuteQueryOperator(
        task_id="gold_sentiment_incremental",
        conn_id="snowflake_conn",
        sql=_gold_sentiment_sql,
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
