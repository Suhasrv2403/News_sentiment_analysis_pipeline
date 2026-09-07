-- Snowflake setup for the News Intelligence medallion pipeline.
--
-- CANONICAL SCHEMA: the table/column definitions below are the single
-- source of truth for raw_news_data, news_silver, and
-- news_gold_sentiment_metrics. dags/news_extraction_dag.py re-declares
-- these same column lists inline in its COPY INTO / MERGE INTO / INSERT
-- SQL (Snowflake SQL has no cross-file import) — if you add, rename, or
-- retype a column here, update the matching SQL in the DAG to match.
--
-- Review database/schema names, warehouse sizing, and the stage's auth
-- method against your own Snowflake account before running — this was
-- not tested against a live account as part of this review.
--
-- Run once per environment, before the DAG's first execution.

-- ---------------------------------------------------------------------
-- 1. Database / schema
-- ---------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS NEWS_INTELLIGENCE;
CREATE SCHEMA IF NOT EXISTS NEWS_INTELLIGENCE.PUBLIC;
USE DATABASE NEWS_INTELLIGENCE;
USE SCHEMA PUBLIC;

-- ---------------------------------------------------------------------
-- 2. Bronze: external stage + file format the S3 -> raw COPY INTO reads from
--    (see task "bronze_s3_to_raw" in news_extraction_dag.py)
-- ---------------------------------------------------------------------
CREATE FILE FORMAT IF NOT EXISTS news_json_ff
    TYPE = JSON
    STRIP_OUTER_ARRAY = TRUE;

-- Prefer a STORAGE INTEGRATION over embedding AWS credentials directly in
-- the stage DDL. Replace <YOUR_STORAGE_INTEGRATION> and <YOUR_BUCKET_NAME>
-- (must match configs/config.yaml's s3.bucket_name).
CREATE STAGE IF NOT EXISTS news_bronze_stage
    URL = 's3://<YOUR_BUCKET_NAME>/bronze/news/'
    STORAGE_INTEGRATION = <YOUR_STORAGE_INTEGRATION>
    FILE_FORMAT = news_json_ff;

CREATE TABLE IF NOT EXISTS raw_news_data (
    raw_json    VARIANT,
    filename    STRING,
    inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------
-- 3. Silver: deduplicated, structured articles
--    (see task "silver_raw_to_structured" in news_extraction_dag.py)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS news_silver (
    id            STRING,
    author        STRING,
    title         STRING,
    description   STRING,
    sourceurl     STRING,
    published_at  STRING,
    content       STRING,
    keyword       STRING,
    source        STRING,
    filename      STRING,
    loaded_at     TIMESTAMP_NTZ
);

-- ---------------------------------------------------------------------
-- 4. Gold: sentiment + source-authority metrics
--    (see tasks "gold_sentiment_incremental" and "gold_source_authority")
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS news_gold_sentiment_metrics (
    keyword             STRING,
    news_date           DATE,
    avg_sentiment_score FLOAT,
    article_count       NUMBER,
    overall_tone        STRING
);

-- news_gold_source_metrics is fully recreated by the DAG each run
-- (CREATE OR REPLACE TABLE AS SELECT ...), so no predefinition is required.
