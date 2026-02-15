"""
Sesión Spark con soporte Iceberg.
Para local: catálogo hadoop con warehouse en ruta local o S3.
Para Glue (AWS): se puede inyectar GlueContext o usar catálogo Glue vía job parameters.
"""
import os


def get_spark_session(warehouse_path=None):
    """
    Obtiene o crea SparkSession con Iceberg.
    warehouse_path: ruta del warehouse (ej. file:///tmp/iceberg_warehouse o s3://bucket/warehouse).
    Si no se pasa, usa ICEBERG_WAREHOUSE o /tmp/iceberg_warehouse.
    """
    from pyspark.sql import SparkSession

    warehouse = warehouse_path or os.environ.get("ICEBERG_WAREHOUSE", "/tmp/iceberg_warehouse")
    # Asegurar que sea URI para S3
    if warehouse.startswith("s3://") or warehouse.startswith("file://"):
        pass
    elif not warehouse.startswith("/"):
        warehouse = f"file://{os.path.abspath(warehouse)}"
    else:
        warehouse = f"file://{warehouse}"

    builder = (
        SparkSession.builder.appName("clean_and_deduplicate")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", warehouse)
    )
    return builder.getOrCreate()
