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
    # Límite por tipo: primero evento (p. ej. desde Airflow), luego variable de entorno
    max_items = _parse_max_items(event.get("extraction_max_items"))
    if max_items is None:
        max_items = _parse_max_items(os.environ.get("EXTRACCION_MAX_ITEMS"))
    result = run(output_path, ingestion_date=ingestion_date, max_items_per_type=max_items)
    return {"statusCode": 200, "body": result}
