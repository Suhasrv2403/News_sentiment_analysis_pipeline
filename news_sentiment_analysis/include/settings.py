"""Project configuration loader.

Reads ``configs/config.yaml`` once at import time and exposes its values
as module-level constants (search keywords, NewsAPI settings, S3 bucket
name, API key) for consumption by ``include/scrapper.py`` and
``dags/news_extraction_dag.py``.

Inputs: ``configs/config.yaml`` (path resolved relative to this file, not
the current working directory, so it works regardless of where Airflow
invokes the DAG from).
Outputs: module-level constants listed in the "Export variables" section
below.
"""

from typing import Any

import yaml
from pathlib import Path

# 1. Locate configs/ as a sibling of the include/ folder this file lives in
# (news_sentiment_analysis/configs/), not relative to the current working
# directory — this ensures the script finds config.yaml regardless of
# where Airflow triggers it from.
CONFIG_DIR = Path(__file__).resolve().parent.parent / "configs"


def load_config() -> dict[str, Any]:
    """Load and parse ``config.yaml`` from the configs directory.

    Returns:
        The parsed YAML content as a nested dict.

    Raises:
        FileNotFoundError: If ``config.yaml`` is missing from ``configs/``.
    """
    config_path = CONFIG_DIR / "config.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"Missing config.yaml at {config_path}")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


# 2. Load the YAML data
_config = load_config()

# 3. Export variables from YAML for the rest of the app
S3_BUCKET = _config["s3"]["bucket_name"]
NEWS_LANGUAGE = _config["settings"]["language"]
NEWS_PAGESIZE = _config["settings"]["page_size"]
NEWS_SORT = _config["settings"]["sort_by"]
SEARCH_KEYWORDS = _config["search_keywords"]
NEWS_API_KEY = _config["api"]["key"]

# Optional keys with defaults matching the values these replaced when they
# were hardcoded in dags/news_extraction_dag.py, so an older config.yaml
# missing these sections doesn't break the DAG at parse time.
S3_BRONZE_PREFIX = _config.get("s3", {}).get("bronze_prefix", "bronze/news")
SENTIMENT_NEUTRAL_BAND = _config.get("sentiment", {}).get("neutral_band", 0.15)

# DAG scheduling — same defensive-default pattern as above; these match
# what was previously hardcoded as DAG(...) kwargs in news_extraction_dag.py.
_dag_config = _config.get("dag", {})
DAG_ID = _dag_config.get("dag_id", "news_intelligence_pipeline")
DAG_START_DATE = _dag_config.get("start_date", "2026-01-01")
DAG_SCHEDULE = _dag_config.get("schedule", "@daily")
DAG_CATCHUP = _dag_config.get("catchup", False)

# We no longer need os.getenv or load_dotenv since secrets are in the YAML.
