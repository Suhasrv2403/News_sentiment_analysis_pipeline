from newsapi import NewsApiClient
from include.settings import NEWS_SORT, NEWS_LANGUAGE, NEWS_API_KEY
import hashlib


class NewsScraper:
    def __init__(self):
        # NEWS_API_KEY is now pulled directly from your YAML via settings.py
        self.api_key = NEWS_API_KEY
        self.client = NewsApiClient(api_key=self.api_key)

    def fetch_news(self, keyword: str, page_size: int = 50):
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
                    # Hash the URL to create a unique ID for Snowflake MERGE later
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
            print(f"❌ Error during API call: {e}")
            return []
