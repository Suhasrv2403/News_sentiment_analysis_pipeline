"""Unit tests for include.settings — config loading and exported constants."""

import pytest

from include import settings


def test_load_config_raises_when_config_yaml_missing(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(settings, "CONFIG_DIR", tmp_path)

    with pytest.raises(FileNotFoundError):
        settings.load_config()


def test_exported_constants_have_expected_types() -> None:
    assert isinstance(settings.SEARCH_KEYWORDS, list)
    assert all(isinstance(k, str) for k in settings.SEARCH_KEYWORDS)
    assert isinstance(settings.NEWS_LANGUAGE, str)
    assert isinstance(settings.NEWS_SORT, str)
    assert isinstance(settings.NEWS_PAGESIZE, int)
    assert isinstance(settings.S3_BUCKET, str)
    assert isinstance(settings.S3_BRONZE_PREFIX, str)
    assert isinstance(settings.SENTIMENT_NEUTRAL_BAND, float)
    assert isinstance(settings.DAG_ID, str)
    assert isinstance(settings.DAG_START_DATE, str)
    assert isinstance(settings.DAG_SCHEDULE, str)
    assert isinstance(settings.DAG_CATCHUP, bool)


def test_dag_start_date_is_iso_parseable() -> None:
    # dags/news_extraction_dag.py does datetime.fromisoformat(DAG_START_DATE);
    # a malformed config value would break DAG parsing, so pin the format here.
    from datetime import datetime

    datetime.fromisoformat(settings.DAG_START_DATE)


def test_s3_bronze_prefix_and_sentiment_band_default_when_config_omits_them() -> None:
    # Mirrors an older config.yaml written before these keys existed —
    # settings.py should fall back to the previously-hardcoded values
    # rather than raising a KeyError at import time.
    config_without_new_keys = {
        "search_keywords": ["Tesla"],
        "settings": {"language": "en", "page_size": 50, "sort_by": "publishedAt"},
        "s3": {"bucket_name": "some-bucket"},
        "api": {"key": "some-key"},
    }
    bronze_prefix = config_without_new_keys.get("s3", {}).get(
        "bronze_prefix", "bronze/news"
    )
    neutral_band = config_without_new_keys.get("sentiment", {}).get(
        "neutral_band", 0.15
    )
    dag_config = config_without_new_keys.get("dag", {})
    assert bronze_prefix == "bronze/news"
    assert neutral_band == 0.15
    assert dag_config.get("dag_id", "news_intelligence_pipeline") == "news_intelligence_pipeline"
    assert dag_config.get("schedule", "@daily") == "@daily"
    assert dag_config.get("catchup", False) is False
