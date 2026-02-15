# Explicación del funcionamiento de los jobs de procesamiento

Este documento describe el flujo de los dos jobs de Glue (Spark) que llevan Bronze → Silver y Silver → Gold, y la parametrización por fases del job Gold.

---

## 1. Contexto (arquitectura parte1)

- **Bronze:** Parquet en S3, escrito por la Lambda. Particiones: `content_type`, year, month, day (**fecha de ingesta**, del run).
- **Silver:** Iceberg en S3, tablas separadas por entidad (articles, blogs, reports, info) más tabla de contenido enriquecido. Partición por **fecha de negocio** (year/month/day derivados de `published_at`). Escrito por el job **clean_and_deduplicate** (y la fase content para la tabla enriquecida).
- **Gold:** Iceberg en S3, modelo dimensional (dim_news_source, dim_topic, fact_article, tablas de agregados). Hechos particionados por **fecha de negocio** (year/month/day de `published_at`); dimensiones sin partición. Escrito por el job **content_and_gold**.

El DAG (MWAA) ejecuta: Lambda → clean_and_deduplicate → content_and_gold. Para **lectura desde Bronze** se usa la fecha de ejecución (partición de ingesta). Para **escritura en Silver y Gold** se particiona por **fecha de negocio** (`published_at`): cada registro se escribe en la partición que corresponde a su fecha de publicación, de modo que las consultas por periodo de publicación sean eficientes.

---

## 2. Job clean_and_deduplicate (Bronze → Silver)

### 2.1 Entrada

- **Origen:** Tablas Bronze en Glue Data Catalog (o rutas S3 de Bronze). Formato Parquet, particionado por `content_type` y year/month/day (fecha de ingesta).
- **Parámetros típicos:** `--partition_year`, `--partition_month`, `--partition_day` (o `--partition_date`) para leer solo la partición de Bronze de esa ejecución.

### 2.2 Flujo

1. Leer desde Bronze las particiones que correspondan a la fecha de ejecución (y opcionalmente rangos si se reprocesa).
2. Validar esquema (columnas esperadas, tipos).
3. Limpiar: nulos, normalización de strings, fechas en formato consistente.
4. Deduplicar por `(content_type, id)` manteniendo la versión más reciente según `updated_at`.
5. Escribir en las tablas Silver (Iceberg): una tabla por entidad (articles, blogs, reports, info). **Partición por fecha de negocio:** year/month/day derivados de `published_at` de cada registro; cada fila va a la partición de su fecha de publicación. **Merge** por partición (upsert por `content_type`, `id`) para no perder registros de ejecuciones anteriores en la misma partición.

### 2.3 Salida

- Tablas Silver actualizadas en el catálogo. Particiones por fecha de negocio (year/month/day de `published_at`); una misma ejecución puede escribir en varias particiones.

---

## 3. Job content_and_gold (Silver → Gold)

### 3.1 Entrada

- **Origen:** Tablas Silver (articles, blogs, reports) en Glue Data Catalog. Formato Iceberg.
- **Parámetros:** Fecha de partición (year, month, day) y **fases a ejecutar** (p. ej. `--phases content,dims,facts,aggregates`).

### 3.2 Fases parametrizables

| Fase | Descripción | Entrada | Salida |
|------|-------------|---------|--------|
| **content** | Análisis de contenido: extracción de keywords y temas, entidades, clasificación por tema. | Silver (articles, blogs, reports) | DataFrame enriquecido (en memoria o tabla intermedia). |
| **dims** | Construcción de dimensiones: dim_news_source, dim_topic. | Tabla de contenido enriquecido en Silver y tabla Silver **info** (lista oficial `news_sites` para validación y catálogo de fuentes). | Tablas Gold dim_* (Iceberg). |
| **facts** | Construcción de fact_article (y hechos que apliquen). | DataFrame enriquecido + dims. | Tabla(s) Gold fact_* (Iceberg). |
| **aggregates** | Agregados: tendencias de temas por tiempo, fuentes más activas. | fact_article + dims (o lectura desde Gold ya escrito). | Tablas Gold de agregados (Iceberg). |

Si se invoca con `--phases aggregates`, el job puede **leer desde Gold** (fact_article, dim_*) y solo recalcular y reescribir las tablas de agregados, sin repetir content/dims/facts.

### 3.3 Orden de ejecución

Dentro del script se ejecutan solo las fases indicadas en `--phases`, en orden: content → dims → facts → aggregates. Las fases omitidas no se ejecutan; si se pide solo `aggregates`, se asume que Gold ya tiene hechos y dimensiones y se leen desde el catálogo.

### 3.4 Optimizaciones

- **Particionamiento:** Lectura desde Bronze por partición de ingesta (fecha del run); escritura en Silver y Gold por **fecha de negocio** (published_at) para consultas eficientes por periodo de publicación.
- **Caching:** DataFrames que se reutilizan (p. ej. Silver leído una vez, dimensiones construidas y usadas en varios joins) con `.cache()` o `.persist()` donde aporte valor.

### 3.5 Salida

- Tablas Gold: dim_news_source, dim_topic, fact_article, y tablas de agregados (tendencias, fuentes activas). Todas en Glue Data Catalog, formato Iceberg en S3.

---

## 4. Resumen del flujo

1. **MWAA** dispara Lambda (escribe Bronze) → **clean_and_deduplicate** (lee Bronze, escribe Silver) → **content_and_gold** (lee Silver, escribe Gold).
2. Ambos jobs reciben la fecha de partición para procesamiento incremental/idempotente.
3. **content_and_gold** admite parametrización por fases para reprocesos selectivos (p. ej. solo agregados) sin repetir todo el pipeline.
