import yaml
from pathlib import Path

# 1. Locate the directory where this file lives (the include folder)
# This ensures the script finds config.yaml regardless of where Airflow triggers it.
INCLUDE_DIR = Path(__file__).resolve().parent


def load_config():
    """Helper to load the YAML file"""
    config_path = INCLUDE_DIR / "config.yaml"
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

# We no longer need os.getenv or load_dotenv since secrets are in the YAML.
