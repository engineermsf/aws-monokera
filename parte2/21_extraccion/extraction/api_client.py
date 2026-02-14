import logging
import time
import requests

from extraction.config import (
    BASE_URL,
    CONTENT_TYPE_BY_ENDPOINT,
    MAX_RETRIES,
    PAGE_SIZE,
    RATE_LIMIT_DELAY_SEC,
    RETRY_BACKOFF_SEC,
)

logger = logging.getLogger(__name__)


def _request(url, session):
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 429:
                wait = RETRY_BACKOFF_SEC * (2 ** attempt)
                logger.warning("Rate limit (429), reintento en %s s", wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                raise
            wait = RETRY_BACKOFF_SEC * (2 ** attempt)
            logger.warning("Error %s, reintento en %s s", e, wait)
            time.sleep(wait)
    raise RuntimeError("Max retries exceeded")


def fetch_page(url, session):
    data = _request(url, session)
    time.sleep(RATE_LIMIT_DELAY_SEC)
    return data.get("results", []), data.get("next"), data.get("count", 0)


def fetch_all_for_endpoint(endpoint, session, base_url=BASE_URL, max_items=None):
    content_type = CONTENT_TYPE_BY_ENDPOINT[endpoint]
    url = f"{base_url.rstrip('/')}/{endpoint}/?limit={PAGE_SIZE}&offset=0"
    total_count = 0
    items = []
    while url:
        page_items, next_url, count = fetch_page(url, session)
        if total_count == 0:
            total_count = count
        for item in page_items:
            item["content_type"] = content_type
            items.append(item)
            if max_items is not None and len(items) >= max_items:
                return items[:max_items], total_count
        url = next_url
    return items, total_count


def fetch_info(session, base_url=BASE_URL):
    url = f"{base_url.rstrip('/')}/info/"
    return _request(url, session)


def fetch_articles(session, base_url=BASE_URL, max_items=None):
    return fetch_all_for_endpoint("articles", session, base_url, max_items)


def fetch_blogs(session, base_url=BASE_URL, max_items=None):
    return fetch_all_for_endpoint("blogs", session, base_url, max_items)


def fetch_reports(session, base_url=BASE_URL, max_items=None):
    return fetch_all_for_endpoint("reports", session, base_url, max_items)
