import logging
import os

from extraction.run import run

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _parse_max_items(value):
    """Convierte evento/entorno a int o None (sin límite). Acepta 'all'/'todo' como sin límite."""
    if value is None or value == "":
        return None
    if isinstance(value, int) and value >= 0:
        return value
    s = str(value).strip().lower()
    if s in ("all", "todo", "full"):
        return None
    return int(s) if s.isdigit() else None


def lambda_handler(event, context):
    output_path = event.get("output_path") or os.environ.get("BRONZE_OUTPUT_PATH", "/tmp/bronze")
    ingestion_date = event.get("ingestion_date")
    # Límite por tipo: si el evento trae valor (all, 1, etc.) se usa ese; si no, la variable de entorno
    event_val = event.get("extraction_max_items")
    env_val = os.environ.get("EXTRACCION_MAX_ITEMS")
    logger.info(
        "extraction_max_items: evento=%r, EXTRACCION_MAX_ITEMS(env)=%r",
        event_val,
        env_val,
    )
    if event_val is None or (isinstance(event_val, str) and event_val.strip() == ""):
        max_items = _parse_max_items(env_val)
    else:
        max_items = _parse_max_items(event_val)
    logger.info("extraction_max_items resuelto: max_items_per_type=%s", max_items)
    result = run(output_path, ingestion_date=ingestion_date, max_items_per_type=max_items)
    return {"statusCode": 200, "body": result}
