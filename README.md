# Spaceflight News – Lakehouse en AWS (Iceberg)

Solución de ingesta y procesamiento de datos de la Spaceflight News API en un lakehouse Bronze–Silver–Gold sobre AWS, con orquestación por Airflow, extracción mediante Lambda, y jobs Glue que escriben tablas Iceberg en Silver y Gold registradas en Glue Data Catalog.

---

## Estructura del repositorio

El contenido versionado del proyecto se organiza en las siguientes carpetas y archivos. La configuración local de despliegue (Airflow, Glue, Athena, etc.) está en `.gitignore` y no forma parte del repositorio.

### Raíz

| Elemento | Descripción |
|---------|-------------|
| `documento_entrega_final.md` | Documento de entrega: arquitectura, flujo end-to-end, evidencias (imágenes) y mapeo de requisitos. |
| `imagenes_entrega/` | Carpeta de imágenes del documento de entrega (diagramas, capturas de ejecución, logs, conteos, resultados de consultas). Ver `imagenes_entrega/README.md` para el listado de archivos. |
| `docs/` | Documentación transversal (p. ej. entendimiento de fuentes de datos). |
| `.github/workflows/` | Definición de workflows de CI/CD (p. ej. despliegue de la Lambda de extracción). |

---

### parte1 – Diseño de arquitectura

| Ruta | Contenido |
|------|-----------|
| `parte1/1diagrama/` | Diagramas de arquitectura (AWS y local) y documento que describe componentes y flujo de datos. |
| `parte1/2documento/` | Documento técnico del sistema. |

---

### parte2 – Extracción y procesamiento

**parte2/21_extraccion/** – Extracción desde la API y escritura en Bronze (Parquet).

| Ruta | Contenido |
|------|-----------|
| `extraction/` | Cliente de API, escritor Parquet y orquestación del run de extracción (paginação, deduplicación, partición por content_type y fecha). |
| `lambda/` | Handler de la Lambda que invoca la lógica de extracción y escribe en S3 Bronze. |
| `docs/` | Estructura, despliegue, pruebas y funcionamiento de la extracción y la Lambda. |
| `tests/` | Pruebas unitarias del cliente de API, escritor Parquet y deduplicación. |

**parte2/22_procesamiento/** – Jobs Spark (Glue) Bronze → Silver y Silver → Gold.

| Ruta | Contenido |
|------|-----------|
| `jobs/` | Scripts principales: `clean_and_deduplicate.py` (Bronze → Silver, tablas articles, blogs, reports, info) y `content_and_gold.py` (Silver → Gold: dim_news_source, dim_topic, fact_article). Módulo `common/` con sesión Spark. `read_bronze_display.py` para inspección de Bronze. |
| `docs/` | Estructura, despliegue, pruebas locales y explicación del funcionamiento de los jobs. |

---

### parte3 – Modelo de datos y análisis

| Ruta | Contenido |
|------|-----------|
| `parte3/31_disenho_dwh/` | Modelo multidimensional (dimensiones, fact_article), diagrama y estrategia de partición y optimización. |
| `parte3/32_analisis_sql/` | Consultas y vistas de análisis (tendencias por tema/mes, fuentes más influyentes); documentación y script SQL de referencia. |

---

### parte4 – Pipeline Airflow y documentación de la Parte 4

| Ruta | Contenido |
|------|-----------|
| `parte4/41_mapeo_requisitos/` | Mapeo de requisitos del enunciado frente a la implementación (extracción, procesamiento, DAG). |
| `parte4/42_codigo_dags/` | Código del DAG: `spaceflight_pipeline_aws.py` (Lambda + Glue en AWS) y `spaceflight_pipeline_local.py` (Docker para Glue local). |
| `parte4/43_documentacion/` | Entregables de la Parte 4 y explicación detallada de los DAG. |

---

## Documento de entrega

El archivo `documento_entrega_final.md` en la raíz resume la solución, incluye la imagen de arquitecturas, el flujo end-to-end, las evidencias de ejecución (imágenes 1 a 8) y la sección de requisitos cubiertos y decisiones técnicas. Las imágenes se referencian desde la carpeta `imagenes_entrega/`.

---

## Convenciones

- **Bronze:** Parquet en S3, particionado por `content_type` y fecha de ingesta. Escrito por la Lambda.
- **Silver:** Iceberg en S3 (warehouse), tablas articles, blogs, reports, info. Escrito por el job Glue `clean_and_deduplicate`. Catálogo Glue con base `silver`.
- **Gold:** Iceberg en S3 (warehouse), dim_news_source, dim_topic, fact_article. Escrito por el job Glue `content_and_gold`. Catálogo Glue con base `gold`. Consultas de tendencias sobre vistas/agregados en Gold.

