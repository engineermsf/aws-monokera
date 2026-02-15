# Estructura y deploy – 2.2 Procesamiento con Spark

## Jobs de Glue (resumen)

| Job | Origen | Destino | Descripción |
|-----|--------|---------|-------------|
| **clean_and_deduplicate** | Bronze (Parquet) | Silver (Iceberg) | Limpieza, validación de esquema, deduplicación por `(content_type, id)`. Tablas Silver: articles, blogs, reports, info. Partición por **fecha de negocio** (year/month/day de `published_at`). |
| **content_and_gold** | Silver (Iceberg) | Gold (Iceberg) | Análisis de contenido (keywords, temas, entidades, clasificación), modelo dimensional (dim_news_source, dim_topic, fact_article) y agregados (tendencias, fuentes activas). **Parametrizable por fases** (content, dims, facts, aggregates). |

El job **content_and_gold** puede recibir un parámetro (p. ej. `--phases content,dims,facts,aggregates` o `--phases aggregates`) para ejecutar solo las fases indicadas; si solo se pide `aggregates`, puede leer desde Gold ya escrito y regenerar solo las tablas de agregados.

## Fases del job content_and_gold (`--phases content,dims,facts,aggregates`)

Orden lógico de ejecución (cada fase depende de la anterior):

1. **content** → enriquece Silver (keywords, temas, entidades, clasificación); sin esto no hay temas ni atributos para dims/facts.
2. **dims** → construye dimensiones a partir del resultado de content (y Silver); facts necesita las claves de dims.
3. **facts** → construye fact_article con FKs a dims; aggregates necesita los hechos.
4. **aggregates** → agrega sobre fact_article (y dims); puede ejecutarse sola leyendo Gold ya escrito.

| Orden | Fase | Descripción |
|-------|------|-------------|
| 1 | **content** | Análisis de contenido sobre Silver: extracción de palabras clave y temas principales (p. ej. de summary/title), identificación de entidades (compañías, personas, lugares) y clasificación de artículos por tema. El resultado se **persiste en Iceberg en Silver** (tabla de contenido enriquecido) particionada por **fecha de negocio** (year/month/day de `published_at`); las fases dims/facts/aggregates leen desde esa tabla. |
| 2 | **dims** | Construcción de las dimensiones **dim_news_source** (fuentes de noticias) y **dim_topic** (temas). Origen: tabla de contenido enriquecido en Silver. Se escriben en Gold (Iceberg) y se reutilizan en facts y aggregates. |
| 3 | **facts** | Construcción de la tabla de hechos **fact_article**: una fila por artículo/blog/report con claves foráneas a dim_news_source y dim_topic, fechas (published_at, updated_at), métricas y atributos necesarios para análisis. Partición por **fecha de negocio** (year/month/day de `published_at`). |
| 4 | **aggregates** | Cálculo de agregados para tendencias y fuentes activas: tendencias de temas por tiempo (p. ej. conteos por tema y mes), análisis de fuentes más activas (conteos por news_site/periodo). Se escriben en tablas Gold (Iceberg). Si se ejecuta solo esta fase, el job lee fact_article y dims ya escritos en Gold y solo regenera estas tablas. |

La tabla Silver **info** (versión de la API y lista `news_sites`) se escribe por clean_and_deduplicate; no tiene `published_at`, por lo que se particiona por **fecha de ingesta** (year/month/day del run). Se **usa** en content_and_gold para validar que los `news_site` de articles/blogs/reports estén en la lista oficial y como catálogo para construir **dim_news_source**.

Al invocar con `--phases content,dims,facts,aggregates` se ejecutan las cuatro fases en ese orden. En un subconjunto (p. ej. `--phases dims,facts` o `--phases aggregates`) se ejecutan solo las fases indicadas, respetando el orden entre ellas (p. ej. dims antes que facts).

**Tabla de content (Iceberg) – historia o sobrescritura:** Se mantiene historial: cada registro se escribe en la partición que corresponde a su **fecha de negocio** (`published_at`). En una misma ejecución pueden escribirse varias particiones (una por cada día de publicación de los ítems procesados). No se sobrescribe la tabla entera; se usa merge por partición (upsert por clave) para re-ejecuciones idempotentes.

## Catálogo y rutas

- **Glue Data Catalog:** Todas las tablas (Bronze, Silver, Gold) se registran en el catálogo. Los jobs resuelven nombres de base de datos y tabla por variable de entorno o parámetro, sin hardcodear rutas S3.
- **Particionamiento:** Bronze por `content_type` y **fecha de ingesta** (year/month/day del run). Silver y Gold (Iceberg): partición por **fecha de negocio** (year/month/day de `published_at`), salvo la tabla Silver **info**, que se particiona por fecha de ingesta (no tiene published_at). Dimensiones Gold sin partición temporal.

## Deploy / ejecución

- **AWS Glue:** Los jobs se crean en la consola Glue o con IaC (Terraform, CDK, SAM). El código (script PySpark o JAR) puede subirse desde S3 o desde un repositorio; un workflow de CI/CD (p. ej. GitHub Actions) puede empaquetar `jobs/` y subir el artefacto a S3 para que Glue lo use.
- **MWAA:** El DAG invoca primero la Lambda (extracción), luego `clean_and_deduplicate` (pasando la fecha de partición), luego `content_and_gold` (con parámetros de fases y fecha). Retries y alertas según documento técnico (parte1).

## Referencias

- Diagrama y flujo: [parte1/1diagrama/diagrama_arquitectura.md](../../parte1/1diagrama/diagrama_arquitectura.md).
- Documento técnico: [parte1/2documento/documento_tecnico.md](../../parte1/2documento/documento_tecnico.md).
- Fuentes y diccionario: [docs/01_entendimiento_fuentes.md](../../docs/01_entendimiento_fuentes.md).
