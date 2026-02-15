import logging
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# Esquema explícito para info: evita que al mezclar con articles/blogs/reports
# el Parquet de la partición info pierda version/news_sites por inferencia de esquema.
INFO_SCHEMA = pa.schema([
    ("id", pa.int64()),
    ("content_type", pa.string()),
    ("version", pa.string()),
    ("news_sites", pa.string()),
    ("year", pa.string()),
    ("month", pa.string()),
    ("day", pa.string()),
])


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
        if "news_sites" in row and isinstance(row["news_sites"], list):
            row["news_sites"] = str(row["news_sites"])
        out.append(row)
    return out


def _write_dataset(table, base_path, root_path, partition_cols, is_s3, fs_s3=None):
    """Escribe tabla Parquet particionada (local o S3)."""
    if is_s3:
        pq.write_to_dataset(
            table,
            root_path=root_path,
            partition_cols=partition_cols,
            existing_data_behavior="overwrite_or_ignore",
            filesystem=fs_s3,
        )
    else:
        base = Path(base_path)
        base.mkdir(parents=True, exist_ok=True)
        pq.write_to_dataset(
            table,
            root_path=base,
            partition_cols=partition_cols,
            existing_data_behavior="overwrite_or_ignore",
        )


def write_bronze_parquet(records, base_path, ingestion_date=None):
    """
    Escribe registros en Parquet particionado por content_type, year, month, day.
    Los registros info se escriben con esquema explícito para que version y news_sites
    queden siempre en el Parquet (evita problemas al mezclar con articles/blogs/reports).
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

    content_records = [r for r in records if (r.get("content_type") or "").lower() != "info"]
    info_records = [r for r in records if (r.get("content_type") or "").lower() == "info"]

    is_s3 = base_path.startswith("s3://")
    fs_s3 = None
    root_path = base_path.rstrip("/")
    if is_s3:
        from pyarrow import fs
        path_clean = base_path.rstrip("/").replace("s3://", "")
        parts = path_clean.split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        root_path = f"{bucket}/{prefix}" if prefix else bucket
        import os
        region = os.environ.get("AWS_REGION", "us-east-1")
        fs_s3 = fs.S3FileSystem(region=region)

    written = 0

    if content_records:
        rows = _prepare_columns(content_records)
        table = pa.Table.from_pylist(rows)
        table = table.append_column("year", pa.array([year] * len(content_records)))
        table = table.append_column("month", pa.array([month] * len(content_records)))
        table = table.append_column("day", pa.array([day] * len(content_records)))
        _write_dataset(table, base_path, root_path, ["content_type", "year", "month", "day"], is_s3, fs_s3)
        written += len(content_records)

    if info_records:
        rows = _prepare_columns(info_records)
        # Esquema explícito: garantiza version y news_sites en el Parquet
        arrays = [
            pa.array([r.get("id", 0) for r in rows], type=pa.int64()),
            pa.array([r.get("content_type") or "info" for r in rows], type=pa.string()),
            pa.array([r.get("version") if r.get("version") is not None else "" for r in rows], type=pa.string()),
            pa.array([r.get("news_sites") if r.get("news_sites") is not None else "" for r in rows], type=pa.string()),
            pa.array([year] * len(rows), type=pa.string()),
            pa.array([month] * len(rows), type=pa.string()),
            pa.array([day] * len(rows), type=pa.string()),
        ]
        info_table = pa.Table.from_arrays(arrays, schema=INFO_SCHEMA)
        _write_dataset(info_table, base_path, root_path, ["content_type", "year", "month", "day"], is_s3, fs_s3)
        written += len(info_records)

    logger.info("Escritos %s registros en %s", written, base_path)
    return written
