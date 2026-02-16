#!/usr/bin/env python3
"""
Job content_and_gold: Silver (Iceberg) → Gold (Iceberg).
Fases: dims (dim_news_source, dim_topic), facts (fact_article). Todas las tablas Gold se escriben por MERGE.
- dim_news_source: MERGE por news_site (preserva news_source_id existentes).
- dim_topic: MERGE por topic_id.
- fact_article: MERGE por (article_id, content_type). Sin --partition_date lee todo Silver y mergea.
Uso: spark-submit content_and_gold.py --silver_warehouse <path> --gold_warehouse <path> [--phases dims,facts] [--partition_date YYYY-MM-DD] [--catalog_gold hadoop|glue]. Con --catalog_gold glue las tablas Gold se registran en Glue Data Catalog y Athena las ve sin crawler.
"""
import argparse
import logging
import os
import sys
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window

_JOBS_DIR = __file__.rsplit("/", 1)[0] if "/" in __file__ else "."
if _JOBS_DIR not in sys.path:
    sys.path.insert(0, _JOBS_DIR)
from common.spark_session import get_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SILVER_DB = "silver"
GOLD_DB = "gold"
DEFAULT_PHASES = "dims,facts"


def parse_args():
    p = argparse.ArgumentParser(description="Silver → Gold (dims, facts); opcional --partition_date para incremental")
    p.add_argument("--silver_warehouse", required=True, help="Warehouse Iceberg Silver (ej. s3://bucket/silver_warehouse)")
    p.add_argument("--gold_warehouse", required=True, help="Warehouse Iceberg Gold (ej. s3://bucket/gold_warehouse)")
    p.add_argument(
        "--phases",
        default=DEFAULT_PHASES,
        help="Fases a ejecutar: dims,facts (por defecto: %s)" % DEFAULT_PHASES,
    )
    p.add_argument(
        "--partition_date",
        default=None,
        help="Opcional. Para uso futuro (ingesta incremental por último día desde la fuente). Sin él, merge completo: lee todo Silver y mergea en Gold.",
    )
    p.add_argument(
        "--catalog_silver",
        default="hadoop",
        choices=("hadoop", "glue"),
        help="Catálogo Silver (lectura): hadoop o glue. Usar glue cuando Silver está en Glue Data Catalog.",
    )
    p.add_argument(
        "--catalog_gold",
        default="hadoop",
        choices=("hadoop", "glue"),
        help="Catálogo para Gold: hadoop (metadata en S3, por defecto) o glue (Glue Data Catalog; Athena ve las tablas sin crawler). Usar glue cuando el job corre en AWS.",
    )
    # parse_known_args para ignorar argumentos que Glue inyecta (--JOB_ID, --JOB_RUN_ID, --JOB_NAME)
    return p.parse_known_args()[0]


def parse_partition_date(s):
    """Devuelve (year, month, day) a partir de YYYY-MM-DD."""
    dt = datetime.strptime(s, "%Y-%m-%d")
    return dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")


def register_gold_catalogs(spark, silver_warehouse, gold_warehouse, catalog_silver="hadoop", catalog_gold="hadoop"):
    """Registra catálogos iceberg_silver (hadoop o glue) e iceberg_gold (hadoop o glue).
    En AWS Glue 4.0+ con Glue Data Catalog se usa catalog-impl (no type=glue)."""
    spark.conf.set("spark.sql.catalog.iceberg_silver", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.iceberg_silver.warehouse", silver_warehouse)
    if catalog_silver == "glue":
        spark.conf.set("spark.sql.catalog.iceberg_silver.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        spark.conf.set("spark.sql.catalog.iceberg_silver.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        logger.info("Catálogo Silver (lectura): Glue Data Catalog")
    else:
        spark.conf.set("spark.sql.catalog.iceberg_silver.type", "hadoop")
    spark.conf.set("spark.sql.catalog.iceberg_gold", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.iceberg_gold.warehouse", gold_warehouse)
    if catalog_gold == "glue":
        spark.conf.set("spark.sql.catalog.iceberg_gold.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        spark.conf.set("spark.sql.catalog.iceberg_gold.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        logger.info("Catálogo Gold: Glue Data Catalog (catalog-impl=GlueCatalog; Athena verá las tablas)")
    else:
        spark.conf.set("spark.sql.catalog.iceberg_gold.type", "hadoop")
        logger.info("Catálogo Gold: Hadoop (metadata en S3)")


def run_phase_dims(spark):
    """Construye dim_news_source (desde silver.info) y dim_topic (placeholder). Ambas con MERGE."""
    logger.info("Fase dims: dim_news_source, dim_topic (MERGE)")
    info = spark.table(f"iceberg_silver.{SILVER_DB}.info")
    # dim_news_source: un registro por news_site (version = max si hay varias); ids existentes se preservan
    new_sites = (
        info.select("news_site", "version")
        .filter(F.col("news_site").isNotNull() & (F.col("news_site") != ""))
        .groupBy("news_site")
        .agg(F.max("version").alias("version"))
    )
    if not new_sites.isEmpty():
        dim_source_batch = _build_dim_news_source_batch(spark, new_sites)
        _ensure_dim_news_source_exists(spark, dim_source_batch)
        _write_dim_news_source_merge(spark, dim_source_batch)
    else:
        # Sin sitios: asegurar que la tabla exista para que facts pueda hacer join
        empty = new_sites.withColumn("news_source_id", F.lit(None).cast("long")).select(
            "news_source_id", "news_site", "version"
        )
        _ensure_dim_news_source_exists(spark, empty)

    # dim_topic: placeholder (topic por content_type), MERGE por topic_id
    dim_topic = spark.createDataFrame(
        [("article", "Article"), ("blog", "Blog"), ("report", "Report")],
        ["topic_id", "topic_name"],
    )
    _ensure_dim_topic_exists(spark, dim_topic)
    _write_dim_topic_merge(spark, dim_topic)


def _read_silver_content(spark, year, month, day):
    """Lee articles, blogs, reports de Silver; si year/month/day se pasan, filtra por esa partición."""
    articles = spark.table(f"iceberg_silver.{SILVER_DB}.articles").withColumn("content_type", F.lit("article"))
    blogs = spark.table(f"iceberg_silver.{SILVER_DB}.blogs").withColumn("content_type", F.lit("blog"))
    reports = spark.table(f"iceberg_silver.{SILVER_DB}.reports").withColumn("content_type", F.lit("report"))
    if year is not None and month is not None and day is not None:
        articles = articles.filter(
            (F.col("year") == year) & (F.col("month") == month) & (F.col("day") == day)
        )
        blogs = blogs.filter(
            (F.col("year") == year) & (F.col("month") == month) & (F.col("day") == day)
        )
        reports = reports.filter(
            (F.col("year") == year) & (F.col("month") == month) & (F.col("day") == day)
        )
    if "news_site" not in articles.columns:
        articles = articles.withColumn("news_site", F.lit(None).cast("string"))
    if "news_site" not in blogs.columns:
        blogs = blogs.withColumn("news_site", F.lit(None).cast("string"))
    if "news_site" not in reports.columns:
        reports = reports.withColumn("news_site", F.lit(None).cast("string"))
    return articles.unionByName(blogs, allowMissingColumns=True).unionByName(reports, allowMissingColumns=True)


def _build_facts_from_union(spark, union_df):
    """A partir del union articles+blogs+reports, hace join a dim_news_source y devuelve fact_article (con article_id)."""
    dim_source = spark.table(f"iceberg_gold.{GOLD_DB}.dim_news_source")
    if "news_site" in union_df.columns:
        facts = union_df.join(dim_source, union_df.news_site == dim_source.news_site, "left").select(
            union_df["id"],
            union_df["content_type"],
            dim_source["news_source_id"],
            union_df["title"],
            union_df["published_at"],
            union_df["updated_at"],
            union_df["year"],
            union_df["month"],
            union_df["day"],
        )
    else:
        facts = union_df.withColumn("news_source_id", F.lit(None).cast("long")).select(
            "id", "content_type", "news_source_id", "title", "published_at", "updated_at", "year", "month", "day"
        )
    return facts.withColumnRenamed("id", "article_id")


def _empty_fact_article_schema():
    """Esquema de fact_article para crear la tabla vacía si no hay datos en Silver para la partición."""
    return StructType([
        StructField("article_id", LongType(), True),
        StructField("content_type", StringType(), True),
        StructField("news_source_id", LongType(), True),
        StructField("title", StringType(), True),
        StructField("published_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("year", StringType(), True),
        StructField("month", StringType(), True),
        StructField("day", StringType(), True),
    ])


def run_phase_facts(spark, partition_date=None):
    """Construye fact_article desde silver.articles, blogs, reports; join a dim_news_source.
    Siempre escribe por MERGE por (article_id, content_type). Con partition_date lee solo esa partición de Silver; sin él, lee todo.
    Si no hay datos para esa partición, igual se asegura que la tabla fact_article exista en Gold (para que aparezca en Glue/Athena)."""
    if partition_date:
        year, month, day = parse_partition_date(partition_date)
        logger.info("Fase facts: fact_article MERGE incremental (partición %s-%s-%s)", year, month, day)
        union_df = _read_silver_content(spark, year, month, day)
    else:
        logger.info("Fase facts: fact_article MERGE (lectura completa de Silver)")
        union_df = _read_silver_content(spark, None, None, None)
    if union_df.isEmpty():
        logger.warning(
            "No hay datos en Silver para esta partición; se crea/asegura la tabla fact_article vacía para que exista en Glue/Athena"
        )
        empty_schema = _empty_fact_article_schema()
        empty_facts = spark.createDataFrame([], empty_schema)
        _ensure_fact_article_exists(spark, empty_facts)
        return
    facts = _build_facts_from_union(spark, union_df)
    facts = facts.repartition(4)
    _ensure_fact_article_exists(spark, facts)
    _write_fact_article_merge(spark, facts)


def _ensure_glue_database_gold(gold_warehouse):
    """Crea la base de datos 'gold' en Glue con LocationUri = warehouse/gold (sin .db).
    Así en S3 aparece la carpeta gold/ en lugar de gold.db/. Solo se usa cuando catalog_gold=glue."""
    try:
        import boto3
        location = f"{gold_warehouse.rstrip('/')}/{GOLD_DB}"
        client = boto3.client("glue", region_name=os.environ.get("AWS_REGION", "us-east-1"))
        try:
            client.get_database(Name=GOLD_DB)
            logger.info("Base de datos Glue '%s' ya existe (location: %s)", GOLD_DB, location)
        except client.exceptions.EntityNotFoundException:
            client.create_database(
                DatabaseInput={
                    "Name": GOLD_DB,
                    "Description": "Gold layer (Iceberg) - Spaceflight",
                    "LocationUri": location,
                }
            )
            logger.info("Base de datos Glue '%s' creada con LocationUri=%s", GOLD_DB, location)
    except Exception as e:
        logger.warning("No se pudo crear/verificar la base Glue '%s' (boto3): %s", GOLD_DB, e)


def _ensure_gold_database(spark, gold_warehouse=None, catalog_gold="hadoop"):
    """Crea la base de datos/namespace Gold en el catálogo (Glue o Hadoop) si no existe.
    Con catalog_gold=glue, creamos la base en Glue con LocationUri = warehouse/gold (carpeta gold/, no gold.db/)."""
    if catalog_gold == "glue" and gold_warehouse:
        _ensure_glue_database_gold(gold_warehouse)
        return
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg_gold.{GOLD_DB}")
        logger.info("Namespace iceberg_gold.%s verificado/creado", GOLD_DB)
    except Exception as e:
        logger.warning("No se pudo crear el namespace iceberg_gold.%s: %s", GOLD_DB, e)


def _build_dim_news_source_batch(spark, new_sites):
    """Arma el batch para dim_news_source: preserva news_source_id existentes, asigna id a sitios nuevos."""
    full_name = f"iceberg_gold.{GOLD_DB}.dim_news_source"
    try:
        current = spark.table(full_name)
    except Exception:
        return new_sites.withColumn(
            "news_source_id", F.monotonically_increasing_id()
        ).select("news_source_id", "news_site", "version")
    max_id_row = current.agg(F.max("news_source_id").alias("m")).first()
    max_id = int(max_id_row["m"]) if max_id_row and max_id_row["m"] is not None else 0
    w = Window.orderBy("news_site")
    batch = new_sites.join(
        current.select(F.col("news_site").alias("_ns"), "news_source_id"),
        F.col("news_site") == F.col("_ns"),
        "left",
    ).drop("_ns")
    batch = batch.withColumn(
        "news_source_id",
        F.coalesce(F.col("news_source_id"), max_id + F.row_number().over(w)),
    )
    return batch.select("news_source_id", "news_site", "version")


def _ensure_dim_news_source_exists(spark, df_sample):
    """Crea dim_news_source en Gold si no existe."""
    full_name = f"iceberg_gold.{GOLD_DB}.dim_news_source"
    try:
        spark.sql(f"DESCRIBE TABLE {full_name}")
        logger.info("Tabla %s ya existe", full_name)
    except Exception:
        logger.info("Creando tabla %s", full_name)
        df_sample.limit(0).writeTo(full_name).using("iceberg").create()


def _write_dim_news_source_merge(spark, df):
    """MERGE en dim_news_source por news_site."""
    full_name = f"iceberg_gold.{GOLD_DB}.dim_news_source"
    df.createOrReplaceTempView("_batch_dim_news_source")
    merge_sql = f"""
    MERGE INTO {full_name} AS t
    USING _batch_dim_news_source AS u
    ON t.news_site = u.news_site
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
    spark.sql(merge_sql)
    logger.info("Merge realizado en %s", full_name)


def _ensure_dim_topic_exists(spark, df_sample):
    """Crea dim_topic en Gold si no existe."""
    full_name = f"iceberg_gold.{GOLD_DB}.dim_topic"
    try:
        spark.sql(f"DESCRIBE TABLE {full_name}")
        logger.info("Tabla %s ya existe", full_name)
    except Exception:
        logger.info("Creando tabla %s", full_name)
        df_sample.limit(0).writeTo(full_name).using("iceberg").create()


def _write_dim_topic_merge(spark, df):
    """MERGE en dim_topic por topic_id."""
    full_name = f"iceberg_gold.{GOLD_DB}.dim_topic"
    df.createOrReplaceTempView("_batch_dim_topic")
    merge_sql = f"""
    MERGE INTO {full_name} AS t
    USING _batch_dim_topic AS u
    ON t.topic_id = u.topic_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
    spark.sql(merge_sql)
    logger.info("Merge realizado en %s", full_name)


def _ensure_fact_article_exists(spark, df_sample):
    """Crea fact_article en Gold si no existe (esquema del DF)."""
    full_name = f"iceberg_gold.{GOLD_DB}.fact_article"
    try:
        spark.sql(f"DESCRIBE TABLE {full_name}")
        logger.info("Tabla %s ya existe", full_name)
    except Exception:
        logger.info("Creando tabla %s", full_name)
        df_sample.limit(0).writeTo(full_name).using("iceberg").partitionedBy("year", "month", "day").create()


def _write_fact_article_merge(spark, df):
    """MERGE en fact_article por (article_id, content_type)."""
    full_name = f"iceberg_gold.{GOLD_DB}.fact_article"
    temp_view = "_batch_fact_article"
    df.createOrReplaceTempView(temp_view)
    merge_sql = f"""
    MERGE INTO {full_name} AS t
    USING {temp_view} AS u
    ON t.article_id = u.article_id AND t.content_type = u.content_type
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
    spark.sql(merge_sql)
    logger.info("Merge realizado en %s", full_name)


def main():
    args = parse_args()
    phases = [p.strip() for p in args.phases.split(",") if p.strip()]

    # Usar gold_warehouse como warehouse por defecto para crear sesión; luego registramos ambos catálogos
    spark = get_spark_session(args.gold_warehouse)
    spark.sparkContext.setLogLevel("WARN")
    register_gold_catalogs(
        spark,
        args.silver_warehouse,
        args.gold_warehouse,
        catalog_silver=getattr(args, "catalog_silver", "hadoop"),
        catalog_gold=getattr(args, "catalog_gold", "hadoop"),
    )

    catalog_gold = getattr(args, "catalog_gold", "hadoop")
    _ensure_gold_database(spark, gold_warehouse=args.gold_warehouse, catalog_gold=catalog_gold)
    # Con Glue Data Catalog siempre hacemos full merge (leer todo Silver); si el job tiene --partition_date por defecto, lo ignoramos.
    partition_for_facts = None if catalog_gold == "glue" else args.partition_date
    if "dims" in phases:
        run_phase_dims(spark)
    if "facts" in phases:
        run_phase_facts(spark, partition_date=partition_for_facts)

    logger.info("Job content_and_gold finalizado (fases: %s)%s", phases, " incremental" if partition_for_facts else " (full merge)")
    return 0


if __name__ == "__main__":
    code = main()
    if code != 0:
        sys.exit(code)
    # Éxito: no usar sys.exit(0), Glue/Spark a veces lo interpreta como fallo
