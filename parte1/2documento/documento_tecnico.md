# Parte 1: Documento técnico – Diseño de arquitectura

Este documento cumple el entregable **"Documento técnico"** de la Parte 1 (Diseño de arquitectura), e incluye:

- Estimación de volumen de datos.
- Estrategia de almacenamiento y búsqueda.
- Plan de contingencia.
- Sistema de monitoreo.

---

## 1. Estimación de volumen de datos

### 1.1 Fuentes y conteos de referencia

Los volúmenes se obtienen de la **Spaceflight News API (v4)** y del documento [01_entendimiento_fuentes.md](../../docs/01_entendimiento_fuentes.md). Los conteos de registros provienen del **campo `count`** de la respuesta paginada de cada endpoint (p. ej. `GET /v4/articles/?limit=1`, `GET /v4/blogs/?limit=1`, `GET /v4/reports/?limit=1`). El parámetro `limit` solo reduce cuántos ítems vienen en esa página; **`count`** devuelve siempre el **total** de documentos disponibles para ese endpoint (sin filtros de búsqueda ni fecha). Ese total puede variar con el tiempo.

| Fuente    | Orden de magnitud (registros) | Comentario |
|-----------|--------------------------------|------------|
| Articles  | ~29 000 – 32 000               | Crece con la ingesta diaria. |
| Blogs     | ~1 900                         | Menor volumen. |
| Reports   | ~1 400                         | Menor volumen. |
| Info      | 1 respuesta (lista + versión)  | Sin paginación. |

### 1.2 Tamaño aproximado por registro

Cada ítem (article/blog/report) en JSON tiene del orden de:

- **Campos fijos:** id, title, url, image_url, news_site, summary, published_at, updated_at, featured, authors (array), launches (array), events (array).
- **Tamaño medio por documento:** ~1,5 – 3 KB en JSON (título + summary + URLs + metadatos). Se asume **~2 KB por documento** para la estimación.

### 1.3 Volumen total y crecimiento

| Concepto | Cálculo | Resultado aproximado |
|----------|---------|----------------------|
| Registros totales (una vez) | 29 000 + 1 900 + 1 400 | **~32 300** documentos |
| Volumen Bronze (Parquet, una carga completa) | 32 300 × ~2 KB comprimido | **~65 MB** (orden de magnitud) |
| Crecimiento diario (nuevos + actualizados) | Estimado 50–200 docs/día × 2 KB | **~0,1 – 0,4 MB/día** |
| Volumen Bronze en 1 año (ingesta diaria incremental) | 65 MB + 365 × 0,2 MB | **~140 MB** (orden de magnitud) |
| Silver/Gold (Iceberg, particionado, comprimido) | ~30–50 % del Bronze | **~45–70 MB** al año para capas procesadas |

En un horizonte de **2–3 años** y con posibles picos (eventos espaciales), un orden de magnitud razonable para el data lake es **varios cientos de MB hasta ~1–2 GB** en total. Para la prueba técnica, el volumen sigue siendo bajo y manejable con S3 + Glue/Athena.

---

## 2. Estrategia de almacenamiento y búsqueda

### 2.1 Capas del lakehouse (Bronze, Silver, Gold)

- **Bronze (S3 + Parquet):** Datos crudos de la API. **Lambda** escribe directamente en formato **Parquet** en S3 (particionado por `content_type` y fecha de ingesta en jerarquía **año/mes/día**, ej. `year=2025/month=02/day=13`). Sin capa Iceberg; más simple y óptimo para ingesta.
- **Silver (S3 + Iceberg):** Datos limpios, deduplicados por `(content_type, id)`; los tres tipos (articles, blogs, reports) se unifican en esta capa. Escritos por Glue. Partición por **año/mes/día** (ej. `year=2025/month=02/day=13`), igual que Bronze, para consultas por tiempo y reprocesos incrementales.
- **Gold (S3 + Iceberg):** Modelo dimensional (dim_news_source, dim_topic, fact_article, agregados). Escrito por Glue. **Tablas de hechos** (fact_article): partición por **año/mes/día**. **Dimensiones** (dim_news_source, dim_topic): sin partición temporal (volumen bajo). Listo para Athena y QuickSight.

### 2.2 Formato y catálogo

- **Formato por capa:** **Bronze:** Parquet en S3 (escrito por Lambda). **Silver y Gold:** Apache Iceberg sobre S3 (escrito por Glue); ventajas: ACID, time travel, evolución de esquema y compatibilidad con Athena.
- **Catálogo:** AWS Glue Data Catalog. Registro de tablas: Bronze (tabla externa sobre Parquet) y Silver/Gold (tablas Iceberg) para que Glue jobs y Athena resuelvan ubicación y esquema.
- **Búsqueda y consultas:** Amazon Athena sobre las tablas Gold (y Silver si se requieren análisis detallados). QuickSight se conecta a Athena, no directamente al catálogo.

### 2.3 Retención y ciclo de vida

- **Bronze:** Retener según política (ej. 90 días en estándar, luego mover a otra clase o eliminar si no se requiere reproceso largo).
- **Silver/Gold:** Retención larga (ej. 2–3 años) para análisis de tendencias. Opcional: políticas S3 (lifecycle) para ahorro en datos antiguos.

---

## 3. Plan de contingencia

### 3.1 Fallos de la API (Spaceflight News API no disponible)

- **Prevención:** Reintentos con backoff exponencial en Lambda (extracción). Límite de reintentos (ej. 3–5) por ejecución.
- **Mitigación:** El DAG (MWAA) puede tener retries a nivel de tarea (ej. 2 reintentos con delay). Si la API sigue caída, la ejecución falla y se notifica por CloudWatch/alertas; la siguiente ejecución diaria reintentará.
- **Datos:** No se pierden datos históricos ya ingeridos; solo se retrasa la ingesta del día.

### 3.2 Fallos en procesamiento (Glue, Spark)

- **Idempotencia:** Los jobs de Silver y Gold están diseñados para poder re-ejecutarse. Se usa **merge** (upsert por clave) para no reescribir toda la historia en cada ejecución: solo se leen las particiones nuevas o incrementales de Bronze, se hace merge en Silver por clave `(content_type, id)` (insertar nuevos, actualizar existentes); igual criterio en Gold para las tablas de hechos. Así se mantiene la historia y el diseño escala bien cuando hay mucho volumen.

  **Cómo se identifican las particiones nuevas:** La fecha de ejecución del DAG (MWAA) define la partición a procesar. Lambda escribe en Bronze con `year/month/day` = fecha de esa ejecución. El job de Glue recibe esa fecha como parámetro (pasada desde el DAG) y lee **solo** las rutas de Bronze para esa partición (p. ej. `content_type=*/year=2025/month=02/day=13`). Con esos datos hace el merge en Silver; no lee el resto de Bronze. Para Gold, mismo criterio: se procesa lo que aportó la ejecución (o las particiones de Silver afectadas).
- **Re-ejecución:** Silver y Gold están desacoplados (jobs de Glue independientes). Desde MWAA se puede re-ejecutar solo la tarea fallida (por ejemplo solo `build_gold` si falló, sin volver a lanzar `clean_and_deduplicate`; o solo este si falló, sin tocar Gold). También se puede re-ejecutar el DAG desde un punto intermedio, sin re-ingestar desde la API si Bronze ya tiene los datos.
- **Datos:** Bronze (Parquet escrito por Lambda) actúa como capa de respaldo cruda; se puede reprocesar Silver y Gold desde Bronze.

### 3.3 Pérdida o corrupción de datos en S3

- **S3:** Versionado habilitado en los buckets del lakehouse para recuperación ante borrados accidentales o sobrescrituras.
- **Iceberg (Silver/Gold):** Los metadatos y snapshots de Iceberg permiten time travel; en caso de escritura incorrecta se puede volver a un snapshot anterior si se detecta a tiempo.
- **Backup adicional (opcional):** Copias cruzadas a otra cuenta o región para entornos productivos críticos.

### 3.4 Contingencia operativa

- **Secretos y configuración:** AWS Secrets Manager (o Parameter Store) para credenciales; evita hardcodear valores sensibles.
- **Documentación:** Este documento y el diagrama en `parte1/1diagrama` sirven como referencia para recuperación y entendimiento del flujo en caso de rotación de personas o incidentes.

---

## 4. Sistema de monitoreo

### 4.1 Amazon CloudWatch (transversal)

Todos los componentes que participan en el pipeline reportan a CloudWatch:

- **MWAA (Airflow):** Logs de DAGs y tareas; métricas de éxito/fallo y duración. Alertas cuando un DAG o una tarea crítica falla.
- **Lambda (extracción):** Logs de invocación (stdout, stderr, excepciones); métricas de invocaciones, errores y duración.
- **Glue (Spark):** Logs del job (driver y workers); métricas de ejecución. Alertas ante fallos de job.
- **Athena:** Métricas de consultas para uso y coste.

### 4.2 Alertas a implementar

- **DAG o tarea fallida en MWAA:** Alarma que notifique (email, SNS) cuando la ejecución diaria del pipeline falle.
- **Lambda:** Errores o timeouts por encima de un umbral.
- **Glue:** Fallo de los jobs `clean_and_deduplicate` o `build_gold`.

### 4.3 Dashboards

- Un dashboard en CloudWatch con métricas clave: ejecuciones del DAG (éxito/fallo), duración de Lambda y de los jobs Glue, y volumen de datos ingeridos (métrica expuesta desde Lambda).

---

## 5. Referencias

- **Diagrama de arquitectura:** [parte1/1diagrama/diagrama_arquitectura.md](../1diagrama/diagrama_arquitectura.md) (imagen, componentes, flujo y backup).
- Entendimiento de fuentes y diccionario de datos: [docs/01_entendimiento_fuentes.md](../../docs/01_entendimiento_fuentes.md).
- Spaceflight News API: <https://api.spaceflightnewsapi.net/v4/docs/>.
