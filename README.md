# News Intelligence Medallion Pipeline 🚀

An end-to-end Data Engineering pipeline that extracts global news data, stores it in a Scalable Data Lake (AWS S3), and transforms it within a Snowflake Data Warehouse using a Medallion Architecture and AI-driven sentiment analysis.

## 🏗 Architecture
The project follows the **Medallion Architecture** to ensure data quality and reliability:

1.  **Bronze (Raw):** Landing zone for raw JSON news data from NewsAPI, stored in S3 and loaded into Snowflake with file metadata.
2.  **Silver (Cleaned):** De-duplicated and structured relational data. Idempotency is handled via unique article hashing.
3.  **Gold (Insights):** Aggregated business metrics, including **Snowflake Cortex AI Sentiment Analysis** and publication trends.



---

## 🛠 Tech Stack
* **Orchestration:** Apache Airflow
* **Data Lake:** AWS S3
* **Data Warehouse:** Snowflake
* **AI/ML:** Snowflake Cortex (LLM-based Sentiment Analysis)
* **Language:** Python 3.12
* **API:** NewsAPI

---

## 🌟 Key Features
* **Idempotent Design:** Uses a custom hashing logic in the Python scraper to ensure no duplicate articles enter the Silver layer.
* **Incremental Loading:** The Gold layer uses Airflow Jinja templates (`ds_add`) to process only the previous day's data, significantly reducing Snowflake compute costs.
* **AI Enrichment:** Leveraging Snowflake Cortex LLMs to perform on-the-fly sentiment analysis on news headlines.
* **Automated Schema Setup:** The DAG includes `CREATE TABLE IF NOT EXISTS` logic for a "plug-and-play" deployment experience.

---

## 📁 Project Structure
```text
.
├── dags/
│   └── news_extraction_pipeline.py    # Main Airflow DAG
├── include/
│   ├── scrapper.py                    # News extraction logic
│   └── settings.py                    # Project configurations
├── tests/
│   └── test_scrapper.py               # Pytest unit tests
├── snowflake_setup.sql                # Initial SQL setup scripts
├── requirements.txt                   # Project dependencies
└── README.md