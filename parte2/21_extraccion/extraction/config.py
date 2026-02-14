import os

BASE_URL = "https://api.spaceflightnewsapi.net/v4"
ENDPOINTS = ("articles", "blogs", "reports", "info")
PAGE_SIZE = 100
MAX_RETRIES = 3
RETRY_BACKOFF_SEC = 2
RATE_LIMIT_DELAY_SEC = 0.5

# Límite opcional por tipo (articles, blogs, reports). Ej. EXTRACCION_MAX_ITEMS=1 → 1 de cada. None = sin límite.
_max = os.environ.get("EXTRACCION_MAX_ITEMS")
MAX_ITEMS_PER_TYPE = int(_max) if (_max is not None and str(_max).strip().isdigit()) else None

CONTENT_TYPE_BY_ENDPOINT = {
    "articles": "article",
    "blogs": "blog",
    "reports": "report",
    "info": "info",
}
