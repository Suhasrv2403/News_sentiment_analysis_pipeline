import pytest
from unittest.mock import patch
from include.scrapper import NewsScraper


def test_generate_id_is_consistent():
    scraper = NewsScraper()
    data = {"title": "Test News", "publishedAt": "2026-01-09"}

    id1 = scraper.generate_id(data)
    id2 = scraper.generate_id(data)

    assert id1 == id2
    assert isinstance(id1, str)


@patch("requests.get")
def test_fetch_news_empty_response(mock_get):
    # Simulate an empty API response
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"articles": []}

    scraper = NewsScraper()
    results = scraper.fetch_news("Tesla")

    assert results == []
