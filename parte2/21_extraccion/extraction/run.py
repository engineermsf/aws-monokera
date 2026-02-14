import logging
import requests

from extraction.api_client import fetch_articles, fetch_blogs, fetch_reports
from extraction.parquet_writer import write_bronze_parquet

logger = logging.getLogger(__name__)


def _dedup_by_content_type_id(records):
    """Deja un único registro por (content_type, id), manteniendo el primero."""
    seen = set()
    out = []
    for r in records:
        key = (r.get("content_type"), r.get("id"))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def run(output_path, session=None, base_url=None):
    """
    Extrae articles, blogs y reports de la API, deduplica por (content_type, id)
    y escribe Parquet en output_path (directorio local o s3://bucket/prefix).
    Si EXTRACCION_MAX_ITEMS está definido (ej. 1), se extrae hasta N ítems de cada tipo (articles, blogs, reports).
    """
    if session is None:
        session = requests.Session()
    if base_url is None:
        from extraction.config import BASE_URL
        base_url = BASE_URL
    from extraction.config import MAX_ITEMS_PER_TYPE

    all_items = []
    for name, fetch_fn in (
        ("articles", fetch_articles),
        ("blogs", fetch_blogs),
        ("reports", fetch_reports),
    ):
        logger.info("Extrayendo %s...", name)
        items, total = fetch_fn(session, base_url, max_items=MAX_ITEMS_PER_TYPE)
        all_items.extend(items)
        logger.info("%s: %s ítems (total en API: %s)", name, len(items), total)

    logger.info("Total antes de dedup: %s", len(all_items))
    unique = _dedup_by_content_type_id(all_items)
    logger.info("Total después de dedup: %s", len(unique))

    written = write_bronze_parquet(unique, output_path)
    return {"fetched": len(all_items), "deduped": len(unique), "written": written}
