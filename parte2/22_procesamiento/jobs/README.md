# Jobs de procesamiento (Glue / Spark)

Scripts para ejecutar en AWS Glue o en local con la imagen Docker de Glue.

## clean_and_deduplicate.py

**Bronze (Parquet) → Silver (Iceberg).**

- Lee Bronze por partición de ingesta (`year`, `month`, `day`).
- Deduplica por `(content_type, id)` manteniendo la versión más reciente por `updated_at`.
- Escribe en tablas Iceberg `iceberg.silver.articles`, `iceberg.silver.blogs`, `iceberg.silver.reports`, `iceberg.silver.info`.
- Partición Silver por fecha de negocio (`published_at`), salvo `info` (partición por fecha de ingesta).
- En **articles**, **blogs** y **reports** se añaden columnas `ingestion_year`, `ingestion_month`, `ingestion_day` (fecha de la corrida) para poder filtrar por “datos cargados el día X”.
- Merge (upsert) por `content_type`, `id`.

**Por qué no ves “datos al 15” en articles/blogs/reports:** En Silver, esas tablas se particionan por `published_at` (fecha de publicación), no por la fecha de ingesta. Los datos que subiste el 15 están en Bronze en `year=2026, month=02, day=15`, pero en Silver aparecen en la partición de la fecha de publicación de cada registro. Para ver “todo lo cargado el 15”, filtra por `ingestion_year='2026' AND ingestion_month='02' AND ingestion_day='15'`.

### Parámetros

| Parámetro | Obligatorio | Descripción |
|-----------|-------------|-------------|
| `--bronze_path` | Sí | Ruta base Bronze (Parquet). Ej. `s3://bucket/bronze/spaceflight` o `file:///tmp/bronze` |
| `--silver_warehouse` | Sí | Warehouse Iceberg para Silver. Ej. `s3://bucket/silver_warehouse` o `file:///tmp/silver_warehouse` |
| `--partition_date` | Sí | Fecha de partición de ingesta en formato `YYYY-MM-DD` |

### Ejecución en local (contenedor Glue)

**1. Renovar sesión AWS** (en el host, si usas SSO):

```bash
aws sso login --profile bnc-lkh-dev
# opcional: comprobar
aws sts get-caller-identity --profile bnc-lkh-dev
```

**2. Entrar al contenedor** (desde la raíz del repo, con el perfil de la cuenta dev):

```bash
cd /home/engin/git/aws-monokera
AWS_PROFILE=bnc-lkh-dev docker compose -f localconfig/glue/docker-compose.yml run --rm glue
```

(Si aparece *"cannot execute binary file"*, ver `localconfig/glue/README.md` — primera vez puede hacer falta `docker compose ... pull`.)

**3a. Bronze y Silver en S3 (recomendado)** — Los resultados persisten y puedes verlos en S3 o consultarlos con Athena/Spark. Dentro del contenedor:

```bash
spark-submit \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg.type=hadoop \
  --conf spark.sql.catalog.iceberg.warehouse=s3://bnc-lkh-dev-silver/warehouse \
  /home/hadoop/workspace/parte2/22_procesamiento/jobs/clean_and_deduplicate.py \
  --bronze_path s3://bnc-lkh-dev-bronze/spaceflight \
  --silver_warehouse s3://bnc-lkh-dev-silver/warehouse \
  --partition_date 2026-02-14
```

Crea el bucket `bnc-lkh-dev-silver` en la cuenta dev si no existe (misma cuenta que `bnc-lkh-dev-bronze`). Ajusta `--partition_date` a una fecha que exista en tus particiones Bronze.

**3b. Solo Bronze en S3, Silver en disco del contenedor** — Para pruebas rápidas; los datos Silver se pierden al salir del contenedor:

```bash
spark-submit \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg.type=hadoop \
  --conf spark.sql.catalog.iceberg.warehouse=file:///tmp/silver_warehouse \
  /home/hadoop/workspace/parte2/22_procesamiento/jobs/clean_and_deduplicate.py \
  --bronze_path s3://bnc-lkh-dev-bronze/spaceflight \
  --silver_warehouse file:///tmp/silver_warehouse \
  --partition_date 2026-02-14
```

**3c. Todo en disco local** (si tienes Parquet en `/tmp/bronze`):

```bash
spark-submit \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg.type=hadoop \
  --conf spark.sql.catalog.iceberg.warehouse=file:///tmp/silver_warehouse \
  /home/hadoop/workspace/parte2/22_procesamiento/jobs/clean_and_deduplicate.py \
  --bronze_path file:///tmp/bronze \
  --silver_warehouse file:///tmp/silver_warehouse \
  --partition_date 2026-02-15
```

### Ver los resultados (Silver)

**Si escribiste Silver en S3** (`s3://bnc-lkh-dev-silver/warehouse`):

- **Listar datos en S3:** desde el host: `aws s3 ls s3://bnc-lkh-dev-silver/warehouse/ --profile bnc-lkh-dev` (verás las bases `silver.db`, etc.).
- **Consultar con Spark:** dentro del mismo contenedor (o otra sesión), arranca PySpark con el mismo catálogo y consulta:
  ```bash
  pyspark --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.iceberg.type=hadoop \
    --conf spark.sql.catalog.iceberg.warehouse=s3://bnc-lkh-dev-silver/warehouse
  ```
  Luego en la shell de PySpark:
  ```python
  spark.sql("USE iceberg.silver")
  spark.sql("SHOW TABLES").show()
  spark.sql("SELECT * FROM articles LIMIT 10").show()
  spark.sql("SELECT * FROM blogs LIMIT 10").show()
  ```
- **Consultar con Athena:** puedes registrar el warehouse como ubicación de tablas Iceberg o usar tablas externas apuntando a la ruta S3; en muchos entornos se usa el conector Iceberg para Athena.

**Si escribiste Silver en disco del contenedor** (`file:///tmp/silver_warehouse`):

- Entra al contenedor y arranca PySpark con ese warehouse, luego:
  ```bash
  pyspark --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.iceberg.type=hadoop \
    --conf spark.sql.catalog.iceberg.warehouse=file:///tmp/silver_warehouse
  ```
  ```python
  spark.sql("USE iceberg.silver")
  spark.sql("SELECT * FROM articles LIMIT 10").show()
  ```

### content_and_gold.py (Silver → Gold)

**Silver (Iceberg) → Gold (Iceberg).** Modelo dimensional y agregados.

- **Fases:** `dims` (dim_news_source desde silver.info, dim_topic placeholder), `facts` (fact_article desde articles/blogs/reports). Los agregados (por mes, por fuente y mes) son **vistas en Athena** (`gold.agg_articles_by_month`, `gold.agg_articles_by_source_month`); ver `localconfig/athena/scripts/sql/05_views_gold.sql`.
- **Parámetros:** `--silver_warehouse`, `--gold_warehouse`, `--phases` (por defecto `dims,facts`), **`--partition_date YYYY-MM-DD`** (opcional, para **incremental**).
- **fact_article:** Siempre se escribe por **MERGE** (nunca DROP+CREATE). Con `--partition_date` se procesa solo esa partición de Silver; sin él, se lee todo Silver y se mergea.

**Ejecución en local (dentro del contenedor Glue, después de clean_and_deduplicate; misma sesión con credenciales exportadas):**

```bash
spark-submit \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg.type=hadoop \
  --conf spark.sql.catalog.iceberg.warehouse=s3://bnc-lkh-dev-gold/warehouse \
  /home/hadoop/workspace/parte2/22_procesamiento/jobs/content_and_gold.py \
  --silver_warehouse s3://bnc-lkh-dev-silver/warehouse \
  --gold_warehouse s3://bnc-lkh-dev-gold/warehouse
```

**Solo facts incremental (una partición; la tabla debe existir o haberse corrido antes dims+facts sin fecha):**

```bash
spark-submit ... content_and_gold.py \
  --silver_warehouse s3://bnc-lkh-dev-silver/warehouse \
  --gold_warehouse s3://bnc-lkh-dev-gold/warehouse \
  --phases facts \
  --partition_date 2025-02-12
```

**De dónde sale la fecha (`--partition_date`):** es **fecha de negocio** (la misma que particiona Silver: year/month/day derivados de `published_at`), no fecha de ejecución. Debe ser la **fecha de corte** que use el pipeline (el campo o regla que define hasta qué fecha de negocio se procesa). Quien invoca el job debe pasarla: orquestador (con la fecha de corte del intervalo), paso previo que la escribe en una tabla de control, o parámetro calculado desde la misma fuente que alimenta la fecha de corte.

El script registra los catálogos `iceberg_silver` e `iceberg_gold` en tiempo de ejecución. Crea el bucket `bnc-lkh-dev-gold` si no existe.

### Estructura esperada de Bronze

Parquet particionado por `content_type`, `year`, `month`, `day` (como escribe la Lambda de extracción). Columnas incluyen: `id`, `content_type`, `title`, `url`, `news_site`, `summary`, `published_at`, `updated_at`, `featured`, `authors`, `launches`, `events`, etc.
