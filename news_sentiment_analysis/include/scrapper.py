"""News extraction client.

Wraps the ``newsapi-python`` client to fetch articles for a search keyword
and normalize each article into a flat dict ready for JSON serialization
and upload to the Bronze S3 layer (see ``dags/news_extraction_dag.py``).

Inputs: NewsAPI credentials and query settings, sourced from
``include/settings.py`` (which in turn loads ``configs/config.yaml``).
Outputs: list of article dicts with a stable, hash-derived ``id`` field
used downstream for de-duplication in the Snowflake Silver MERGE.
"""

from typing import Any

from newsapi import NewsApiClient
from include.settings import NEWS_SORT, NEWS_LANGUAGE, NEWS_API_KEY
import hashlib


class NewsScraper:
    """Thin client around NewsAPI's "everything" search endpoint.

    Language and sort order are fixed at construction time from
    ``configs/config.yaml`` (via ``include/settings.py``); only the search
    keyword and page size vary per call.
    """

    def __init__(self) -> None:
        # NEWS_API_KEY is now pulled directly from your YAML via settings.py
        self.api_key = NEWS_API_KEY
        self.client = NewsApiClient(api_key=self.api_key)

    def fetch_news(self, keyword: str, page_size: int = 50) -> list[dict[str, Any]]:
        """Fetch articles matching ``keyword`` from NewsAPI.

        Args:
            keyword: Search term passed to NewsAPI's ``q`` parameter.
            page_size: Max number of articles to request (NewsAPI caps this
                at 100 per page). Defaults to 50.

        Returns:
            A list of article dicts with keys ``id``, ``keyword``,
            ``source``, ``author``, ``title``, ``description``, ``url``,
            ``published_at``, and ``content``. Returns an empty list if the
            API call fails or the response status is not ``"ok"``.
        """
        print(f"🔍 Fetching news for: {keyword}")
        articles_data = []
        try:
            response = self.client.get_everything(
                q=keyword,
                language=NEWS_LANGUAGE,
                sort_by=NEWS_SORT,
                page_size=page_size,
            )

            if response["status"] == "ok":
                for art in response["articles"]:
                    url = art.get("url", "")
                    # Hash the URL (not the article ID, which NewsAPI doesn't
                    # provide) to create a stable, deterministic ID. This lets
                    # the Snowflake Silver MERGE dedupe re-fetched articles
                    # across DAG runs instead of inserting duplicates.
                    article_id = (
                        f"{keyword}_{hashlib.md5(url.encode('utf-8')).hexdigest()}"
                    )

                    articles_data.append(
                        {
                            "id": article_id,
                            "keyword": keyword,
                            "source": art["source"]["name"],
                            "author": art.get("author"),
                            "title": art.get("title"),
                            "description": art.get("description"),
                            "url": url,
                            "published_at": art.get("publishedAt"),
                            "content": art.get("content"),
                        }
                    )
            return articles_data
        except Exception as e:
            # Broad except is intentional here: a failed fetch for one
            # keyword should not crash the whole extraction task loop in
            # the DAG (see run_news_extraction_and_upload), so we log and
            # return an empty list rather than raising.
            print(f"❌ Error during API call: {e}")
            return []
