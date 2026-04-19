from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "earthquakes.db"

USGS_BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
OUTPUT_FORMAT = "geojson"

LOOKBACK_DAYS = 30
PAGE_LIMIT = 200
REQUEST_TIMEOUT = 30

LOG_LEVEL = "INFO"