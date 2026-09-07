"""Structural tests for the news_intelligence_pipeline DAG.

Skipped when apache-airflow isn't installed (e.g. running tests locally
without the Astro runtime); runs in CI where airflow + providers are
installed (see .github/workflows/ci.yml).
"""

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("airflow")

from dags.news_extraction_dag import dag, run_news_extraction_and_upload  # noqa: E402
from include import settings  # noqa: E402


def test_dag_loads_with_configured_id() -> None:
    # Asserts against include/settings.py's exports (sourced from
    # config.yaml), not hardcoded literals, so this test tracks config
    # changes instead of masking them.
    assert dag.dag_id == settings.DAG_ID
    assert dag.schedule == settings.DAG_SCHEDULE
    assert dag.catchup == settings.DAG_CATCHUP


def test_dag_has_expected_tasks() -> None:
    assert set(dag.task_dict) == {
        "extract_news_to_s3",
        "bronze_s3_to_raw",
        "silver_raw_to_structured",
        "gold_sentiment_incremental",
        "gold_source_authority",
    }


def test_dag_dependency_chain() -> None:
    extract = dag.get_task("extract_news_to_s3")
    bronze = dag.get_task("bronze_s3_to_raw")
    silver = dag.get_task("silver_raw_to_structured")

    assert [t.task_id for t in extract.downstream_list] == ["bronze_s3_to_raw"]
    assert [t.task_id for t in bronze.downstream_list] == ["silver_raw_to_structured"]
    assert {t.task_id for t in silver.downstream_list} == {
        "gold_sentiment_incremental",
        "gold_source_authority",
    }


def test_silver_transform_drops_removed_placeholder_titles() -> None:
    # NewsAPI's '[Removed]' title placeholder should never make it into
    # news_silver — see the comment on this WHERE clause in the DAG.
    silver_sql = dag.get_task("silver_raw_to_structured").sql
    assert "[Removed]" in silver_sql


def test_gold_sentiment_sql_uses_configured_neutral_band_not_a_stale_literal() -> None:
    # Regression guard for the %%NEUTRAL_BAND%% substitution: the rendered
    # SQL must contain the value settings.py actually loaded from config,
    # not a hardcoded 0.15 left over from before this was config-driven.
    sentiment_sql = dag.get_task("gold_sentiment_incremental").sql
    assert f"> {settings.SENTIMENT_NEUTRAL_BAND}" in sentiment_sql
    assert f"< -{settings.SENTIMENT_NEUTRAL_BAND}" in sentiment_sql
    assert "%%NEUTRAL_BAND%%" not in sentiment_sql


@patch("dags.news_extraction_dag.S3Hook")
@patch("dags.news_extraction_dag.NewsScraper")
def test_extraction_uploads_once_per_keyword_when_articles_found(
    mock_scraper_cls: MagicMock, mock_s3hook_cls: MagicMock
) -> None:
    mock_scraper = MagicMock()
    mock_scraper.fetch_news.return_value = [{"id": "a1", "title": "headline"}]
    mock_scraper_cls.return_value = mock_scraper
    mock_s3hook = MagicMock()
    mock_s3hook_cls.return_value = mock_s3hook

    run_news_extraction_and_upload()

    assert mock_s3hook.load_string.call_count == len(settings.SEARCH_KEYWORDS)


@patch("dags.news_extraction_dag.S3Hook")
@patch("dags.news_extraction_dag.NewsScraper")
def test_extraction_skips_upload_when_no_articles_found(
    mock_scraper_cls: MagicMock, mock_s3hook_cls: MagicMock
) -> None:
    mock_scraper = MagicMock()
    mock_scraper.fetch_news.return_value = []
    mock_scraper_cls.return_value = mock_scraper
    mock_s3hook = MagicMock()
    mock_s3hook_cls.return_value = mock_s3hook

    run_news_extraction_and_upload()

    mock_s3hook.load_string.assert_not_called()
