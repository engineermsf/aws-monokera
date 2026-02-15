#!/usr/bin/env python3
"""
Job clean_and_deduplicate: Bronze (Parquet) → Silver (Iceberg).
Lee la partición de Bronze por fecha de ingesta, deduplica por (content_type, id)
manteniendo la versión más reciente por updated_at, escribe en tablas Silver
(articles, blogs, reports, info) con partición por fecha de negocio (published_at);
info se particiona por fecha de ingesta.
Uso: spark-submit clean_and_deduplicate.py --bronze_path <path> --silver_warehouse <path> --partition_date YYYY-MM-DD
"""
import argparse
import logging
import os
import sys
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Añadir el directorio jobs al path para imports (spark-submit puede ejecutarse desde cualquier sitio)
_JOBS_DIR = os.path.dirname(os.path.abspath(__file__))
if _JOBS_DIR not in sys.path:
    sys.path.insert(0, _JOBS_DIR)
from common.spark_session import get_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BRONZE_PARTITION_COLS = ["content_type", "year", "month", "day"]
SILVER_DB = "silver"
CONTENT_TABLES = ["article", "blog", "report"]
INFO_TABLE = "info"


def parse_args():
    p = argparse.ArgumentParser(description="Clean and deduplicate Bronze → Silver")
    p.add_argument("--bronze_path", required=True, help="Ruta base Bronze (Parquet), ej. s3://bucket/bronze/spaceflight o file:///tmp/bronze")
    p.add_argument("--silver_warehouse", required=True, help="Warehouse Iceberg para Silver, ej. s3://bucket/silver o file:///tmp/silver_warehouse")
    p.add_argument("--partition_date", required=True, help="Fecha de partición de ingesta YYYY-MM-DD")
    return p.parse_args()


def parse_partition_date(s):
    """Devuelve (year, month, day) a partir de YYYY-MM-DD."""
    dt = datetime.strptime(s, "%Y-%m-%d")
    return dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")


def read_bronze(spark, bronze_path, year, month, day):
    """Lee Bronze filtrando por partición de ingesta."""
    path = bronze_path.rstrip("/")
    df = spark.read.parquet(path)
    if "year" in df.columns and "month" in df.columns and "day" in df.columns:
        df = df.filter(
            (F.col("year") == year) & (F.col("month") == month) & (F.col("day") == day)
        )
    return df


def read_bronze_info_only(spark, bronze_path, year, month, day):
    """Lee solo la partición content_type=info para evitar que el esquema unificado pierda version/news_sites."""
    path = bronze_path.rstrip("/")
    info_path = f"{path}/content_type=info"
    df = spark.read.parquet(info_path)
    if "year" in df.columns and "month" in df.columns and "day" in df.columns:
        df = df.filter(
            (F.col("year") == year) & (F.col("month") == month) & (F.col("day") == day)
        )
    return df


def deduplicate_by_content_type_id(df):
    """Mantiene un registro por (content_type, id) con updated_at más reciente."""
    if "updated_at" not in df.columns:
        return df.dropDuplicates(["content_type", "id"])
    window = Window.partitionBy("content_type", "id").orderBy(F.col("updated_at").desc())
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


def add_silver_partition_columns(df, use_published_at=True, run_year=None, run_month=None, run_day=None):
    """
    Añade year, month, day para partición Silver.
    Si use_published_at: derivados de published_at (fecha de negocio).
    Si no (info): usa run_year, run_month, run_day (fecha de ingesta).
    """
    if use_published_at and "published_at" in df.columns:
        df = df.withColumn("year", F.substring(F.col("published_at"), 1, 4))
        df = df.withColumn("month", F.substring(F.col("published_at"), 6, 2))
        df = df.withColumn("day", F.substring(F.col("published_at"), 9, 2))
    else:
        df = df.withColumn("year", F.lit(run_year))
        df = df.withColumn("month", F.lit(run_month))
        df = df.withColumn("day", F.lit(run_day))
    return df


def ensure_silver_table_exists(spark, table_name, df_sample, partition_cols):
    """Crea la tabla Iceberg si no existe (usando esquema del DF)."""
    full_name = f"iceberg.{SILVER_DB}.{table_name}"
    try:
        spark.sql(f"DESCRIBE TABLE {full_name}")
        logger.info("Tabla %s ya existe", full_name)
    except Exception:
        logger.info("Creando tabla %s", full_name)
        df_sample.limit(0).writeTo(full_name).using("iceberg").partitionedBy(*partition_cols).create()


def write_silver_merge(spark, table_name, df, partition_cols, merge_on=None):
    """Escribe en Silver: si la tabla existe hace MERGE; si no, crea. merge_on: columnas para ON (default: content_type, id)."""
    full_name = f"iceberg.{SILVER_DB}.{table_name}"
    temp_view = f"_batch_{table_name}"
    df.createOrReplaceTempView(temp_view)
    try:
        spark.sql(f"DESCRIBE TABLE {full_name}")
        table_exists = True
    except Exception:
        table_exists = False
    if not table_exists:
        df.writeTo(full_name).using("iceberg").partitionedBy(*partition_cols).create()
        logger.info("Tabla %s creada y datos escritos", full_name)
        return
    on_cols = merge_on if merge_on is not None else ["content_type", "id"]
    on_clause = " AND ".join(f"t.{c} = u.{c}" for c in on_cols)
    merge_sql = f"""
    MERGE INTO {full_name} AS t
    USING {temp_view} AS u
    ON {on_clause}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
    spark.sql(merge_sql)
    logger.info("Merge realizado en %s", full_name)


def main():
    args = parse_args()
    year, month, day = parse_partition_date(args.partition_date)

    spark = get_spark_session(args.silver_warehouse)
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Leyendo Bronze desde %s partición %s-%s-%s", args.bronze_path, year, month, day)
    bronze = read_bronze(spark, args.bronze_path, year, month, day)
    if bronze.isEmpty():
        logger.warning("No hay datos en Bronze para la partición indicada")
        return 0

    logger.info("Deduplicando por (content_type, id) manteniendo updated_at más reciente")
    deduped = deduplicate_by_content_type_id(bronze)

    partition_cols = ["year", "month", "day"]

    for content_type in CONTENT_TABLES:
        subset = deduped.filter(F.col("content_type") == content_type)
        if subset.isEmpty():
            continue
        subset = add_silver_partition_columns(subset, use_published_at=True)
        # Columnas de ingesta para poder filtrar por "día que se cargó" (ej. datos al 15)
        subset = subset.withColumn("ingestion_year", F.lit(year)).withColumn("ingestion_month", F.lit(month)).withColumn("ingestion_day", F.lit(day))
        table_name = f"{content_type}s"  # articles, blogs, reports
        write_silver_merge(spark, table_name, subset, partition_cols)

    # Info: leer solo content_type=info; en Silver explotar news_sites a un registro por sitio (version/demás se repiten)
    info_df = read_bronze_info_only(spark, args.bronze_path, year, month, day)
    if not info_df.isEmpty():
        if "content_type" not in info_df.columns:
            info_df = info_df.withColumn("content_type", F.lit("info"))
        info_df = add_silver_partition_columns(
            info_df, use_published_at=False,
            run_year=year, run_month=month, run_day=day
        )
        base_info_cols = ["id", "content_type", "version", "news_sites", "year", "month", "day"]
        for c in base_info_cols:
            if c not in info_df.columns:
                info_df = info_df.withColumn(c, F.lit(None).cast("int") if c == "id" else F.lit(None).cast("string"))
        info_df = info_df.select(base_info_cols)

        # Explotar lista news_sites (formato "['A', 'B', ...]") a un registro por news_site
        # substring(3, len-4) quita "['" y "']"; split por "', '" da el array (expr para compatibilidad Spark)
        sites_expr = F.when(
            F.length(F.col("news_sites")) <= 2,
            F.array(F.lit(None).cast("string")),
        ).otherwise(
            F.split(F.expr("substring(news_sites, 3, length(news_sites) - 5)"), "', '")
        )
        info_df = info_df.withColumn("news_site", F.explode(sites_expr)).drop("news_sites")
        info_cols = ["id", "content_type", "version", "news_site", "year", "month", "day"]
        info_df = info_df.select(info_cols)
        write_silver_merge(
            spark, INFO_TABLE, info_df, partition_cols,
            merge_on=["news_site", "year", "month", "day"],
        )

    logger.info("Job clean_and_deduplicate finalizado")
    return 0


if __name__ == "__main__":
    sys.exit(main())
