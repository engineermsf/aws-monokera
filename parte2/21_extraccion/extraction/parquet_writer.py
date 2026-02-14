import logging
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


def _prepare_columns(records):
    """Convierte listas/dicts anidados a tipos serializables para Parquet."""
    out = []
    for r in records:
        row = dict(r)
        if "authors" in row and isinstance(row["authors"], list):
            row["authors"] = str(row["authors"])
        if "launches" in row and isinstance(row["launches"], list):
            row["launches"] = str(row["launches"])
        if "events" in row and isinstance(row["events"], list):
            row["events"] = str(row["events"])
        out.append(row)
    return out


def write_bronze_parquet(records, base_path, ingestion_date=None):
    """
    Escribe registros en Parquet particionado por content_type, year, month, day.
    base_path: directorio local o prefijo S3 (s3://bucket/prefix).
    """
    if not records:
        logger.warning("Sin registros para escribir")
        return 0

    if ingestion_date is None:
        ingestion_date = datetime.utcnow()
    year = ingestion_date.strftime("%Y")
    month = ingestion_date.strftime("%m")
    day = ingestion_date.strftime("%d")

    rows = _prepare_columns(records)
    table = pa.Table.from_pylist(rows)
    table = table.append_column("year", pa.array([year] * len(records)))
    table = table.append_column("month", pa.array([month] * len(records)))
    table = table.append_column("day", pa.array([day] * len(records)))

    if base_path.startswith("s3://"):
        import boto3
        from pyarrow import fs

        path_clean = base_path.rstrip("/")
        bucket = path_clean.replace("s3://", "").split("/")[0]
        prefix = "/".join(path_clean.replace("s3://", "").split("/")[1:])
        s3 = boto3.client("s3")
        region = s3.meta.region_name or "us-east-1"
        fs_s3 = fs.S3FileSystem(region=region)
        full_base = f"s3://{bucket}/{prefix}" if prefix else f"s3://{bucket}"
        pq.write_to_dataset(
            table,
            root_path=full_base,
            partition_cols=["content_type", "year", "month", "day"],
            existing_data_behavior="overwrite_or_ignore",
            filesystem=fs_s3,
        )
    else:
        base = Path(base_path)
        base.mkdir(parents=True, exist_ok=True)
        pq.write_to_dataset(
            table,
            root_path=base,
            partition_cols=["content_type", "year", "month", "day"],
            existing_data_behavior="overwrite_or_ignore",
        )

    logger.info("Escritos %s registros en %s", len(records), base_path)
    return len(records)
