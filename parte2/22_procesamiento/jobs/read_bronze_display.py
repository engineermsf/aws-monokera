#!/usr/bin/env python3
"""
Lee solo el Parquet de Bronze correspondiente a INFO y hace display.
Uso (dentro del contenedor Glue, con credenciales AWS ya exportadas):
  spark-submit /home/hadoop/workspace/parte2/22_procesamiento/jobs/read_bronze_display.py
O con partición opcional:
  spark-submit read_bronze_display.py [--partition_date 2026-02-15]
"""
import argparse
import sys
from datetime import datetime

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lee Bronze Parquet solo INFO y muestra contenido")
    parser.add_argument(
        "--bronze_path",
        default="s3://bnc-lkh-dev-bronze/spaceflight",
        help="Ruta base Bronze (Parquet)",
    )
    parser.add_argument(
        "--partition_date",
        default=None,
        help="Opcional: YYYY-MM-DD para filtrar por partición (year/month/day)",
    )
    args = parser.parse_args()

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("read_bronze_info")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Leer solo la partición content_type=info (menos datos y más rápido)
    path = args.bronze_path.rstrip("/")
    info_path = f"{path}/content_type=info"
    df = spark.read.parquet(info_path)

    if args.partition_date:
        try:
            dt = datetime.strptime(args.partition_date, "%Y-%m-%d")
            y, m, d = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")
            if "year" in df.columns and "month" in df.columns and "day" in df.columns:
                df = df.filter((df.year == y) & (df.month == m) & (df.day == d))
        except ValueError:
            pass

    # Solo columnas de info
    info_cols = ["id", "content_type", "version", "news_sites", "year", "month", "day"]
    existing = [c for c in info_cols if c in df.columns]
    df = df.select(existing) if existing else df

    print("\n=== INFO (Bronze) – Esquema ===\n")
    df.printSchema()

    n = df.count()
    print("\n=== Conteo ===\n")
    print(n, "filas\n")

    print("\n=== Contenido INFO (version, news_sites) ===\n")
    df.show(50, truncate=200, vertical=True)

    spark.stop()
    sys.exit(0)
