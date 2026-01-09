"""Unit tests for include.scrapper.NewsScraper.

Mocks include.scrapper.NewsApiClient so tests don't hit the real NewsAPI.
"""

from unittest.mock import MagicMock, patch

from include.scrapper import NewsScraper

SAMPLE_RESPONSE = {
    "status": "ok",
    "articles": [
        {
            "source": {"name": "Reuters"},
            "author": "Jane Doe",
            "title": "Sample headline",
            "description": "Sample description",
            "url": "https://example.com/a",
            "publishedAt": "2026-01-09T00:00:00Z",
            "content": "Sample content",
        }
    ],
}


def _mock_scraper(mock_client_cls: MagicMock, response: dict) -> NewsScraper:
    mock_client = MagicMock()
    mock_client.get_everything.return_value = response
    mock_client_cls.return_value = mock_client
    return NewsScraper()


@patch("include.scrapper.NewsApiClient")
def test_fetch_news_returns_normalized_articles(mock_client_cls: MagicMock) -> None:
    scraper = _mock_scraper(mock_client_cls, SAMPLE_RESPONSE)

    results = scraper.fetch_news("Tesla")

    assert len(results) == 1
    article = results[0]
    assert article["keyword"] == "Tesla"
    assert article["source"] == "Reuters"
    assert article["title"] == "Sample headline"
    assert article["url"] == "https://example.com/a"
    assert article["published_at"] == "2026-01-09T00:00:00Z"


@patch("include.scrapper.NewsApiClient")
def test_fetch_news_id_is_a_stable_hash(mock_client_cls: MagicMock) -> None:
    scraper = _mock_scraper(mock_client_cls, SAMPLE_RESPONSE)

    id1 = scraper.fetch_news("Tesla")[0]["id"]
    id2 = scraper.fetch_news("Tesla")[0]["id"]

    assert id1 == id2
    assert isinstance(id1, str)
    assert id1.startswith("Tesla_")


@patch("include.scrapper.NewsApiClient")
def test_fetch_news_empty_response(mock_client_cls: MagicMock) -> None:
    scraper = _mock_scraper(mock_client_cls, {"status": "ok", "articles": []})

    results = scraper.fetch_news("Tesla")

    assert results == []


@patch("include.scrapper.NewsApiClient")
def test_fetch_news_non_ok_status_returns_empty_list(mock_client_cls: MagicMock) -> None:
    scraper = _mock_scraper(mock_client_cls, {"status": "error", "articles": []})

    results = scraper.fetch_news("Tesla")

    assert results == []


@patch("include.scrapper.NewsApiClient")
def test_fetch_news_handles_api_error_gracefully(mock_client_cls: MagicMock) -> None:
    mock_client = MagicMock()
    mock_client.get_everything.side_effect = Exception("boom")
    mock_client_cls.return_value = mock_client

    scraper = NewsScraper()
    results = scraper.fetch_news("Tesla")

    assert results == []


# The Snowflake Silver MERGE (dags/news_extraction_dag.py, silver_transform
# task) extracts exactly these fields via `raw_json:<field>::STRING` from
# the JSON this scraper writes to S3. If fetch_news's output keys drift
# from this set, the Silver transform silently loses or nulls a column
# instead of failing loudly — this test is the producer/consumer contract
# check for that pipeline boundary.
EXPECTED_SILVER_SOURCE_FIELDS = {
    "id",
    "author",
    "title",
    "description",
    "url",  # aliased to `sourceurl` on the Silver side
    "published_at",
    "content",
    "keyword",
    "source",
}


@patch("include.scrapper.NewsApiClient")
def test_fetch_news_article_keys_match_silver_transform_contract(
    mock_client_cls: MagicMock,
) -> None:
    scraper = _mock_scraper(mock_client_cls, SAMPLE_RESPONSE)

    article = scraper.fetch_news("Tesla")[0]

    assert set(article.keys()) == EXPECTED_SILVER_SOURCE_FIELDS
