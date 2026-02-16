# Diseño del Data Warehouse – Optimizaciones y estrategia (Parte 3.1)

Complemento al modelo multidimensional: particionamiento temporal, estrategia de actualización y decisiones de rendimiento para la capa Gold.

---

## Particionamiento temporal

| Tabla           | Partición              | Justificación |
|-----------------|------------------------|----------------|
| **fact_article** | `year`, `month`, `day` | Valores derivados de `published_at`. Permite **partition pruning** en consultas por rango de fechas (p. ej. "últimos 3 meses", "febrero 2025"). Iceberg y Athena leen solo las particiones necesarias. |
| **dim_news_source** | Sin partición          | Tabla de catálogo pequeña; no tiene dimensión temporal relevante. |
| **dim_topic**       | Sin partición          | Catálogo fijo de tres filas. |

Las vistas de agregados (por mes, por fuente y mes) se apoyan en estas particiones al consultar `fact_article`.

---

## Estrategia de actualización

Todas las tablas Gold se actualizan por **MERGE** (upsert), nunca por DROP + CREATE:

| Tabla           | Clave de MERGE              | Comportamiento |
|-----------------|-----------------------------|----------------|
| **dim_news_source** | `news_site`                 | Coincidencia por sitio: actualiza `version`; sitios nuevos se insertan con `news_source_id` asignado. Se preservan los ids existentes para no romper claves foráneas en `fact_article`. |
| **dim_topic**       | `topic_id`                  | Coincidencia por tema; actualización de nombre si cambia. |
| **fact_article**    | `(article_id, content_type)`| Coincidencia por artículo y tipo: actualiza el resto de columnas; registros nuevos se insertan. |

**Ventajas:**

- **Idempotencia:** re-ejecutar el job con los mismos datos no duplica filas ni borra historia.
- **Incremental:** con `--partition_date` el job puede procesar solo una partición de Silver y hacer MERGE en Gold.
- **Seguridad:** se evita borrar toda la tabla por error.

---

## Índices y rendimiento

En Athena sobre Iceberg no se definen índices secundarios como en un RDBMS. Las optimizaciones se basan en:

1. **Particionamiento:** `fact_article` particionado por `year`, `month`, `day` reduce el escaneo en consultas por tiempo.
2. **Vistas pre-agregadas:** `agg_articles_by_month`, `agg_articles_by_source_month` y `v_agg_articles_by_content_type_month` permiten análisis de tendencias y fuentes sin recalcular agregados en cada query.
3. **Joins por clave surrogada:** `fact_article.news_source_id` → `dim_news_source.news_source_id` y `content_type` → `dim_topic.topic_id` son joins eficientes sobre columnas que el motor puede optimizar.

Para cargas muy grandes, Iceberg permite compactación y gestión de archivos pequeños; el catálogo de Glue/Athena usa los metadatos de Iceberg para planificar lecturas.
