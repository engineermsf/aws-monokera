# Optimizaciones futuras: extracción incremental

Objetivo: no reconsumir todo el catálogo en cada ejecución. En Silver habrá **tablas separadas por entidad** (articles, blogs, reports, info). El plan pendiente (sección 2) es obtener la máxima fecha por tabla desde Silver vía Athena y usar ese valor como filtro `published_at_gte` (y opcionalmente `updated_at_gte`) en la API.

---

## 1. Soporte de la Spaceflight News API v4

La API **permite filtrar por fecha**. Se han probado en vivo los siguientes parámetros (todos devuelven HTTP 200 y la API incluye el filtro en el campo `next` de la respuesta):

| Parámetro            | Endpoints probados     | Uso en incremental                          |
|----------------------|------------------------|---------------------------------------------|
| `published_at_gte`   | articles, blogs, reports | Pedir solo ítems publicados desde una fecha |
| `published_at_lte`   | articles (documentado en [FAQ SNAPI](https://github.com/TheSpaceDevs/Tutorials/blob/main/faqs/faq_SNAPI.md)) | Acotar por fecha máxima                     |
| `updated_at_gte`    | articles               | Pedir solo ítems modificados desde una fecha (capturar actualizaciones) |

Ejemplos comprobados:

- `GET /v4/articles/?limit=1&published_at_gte=2025-01-01` → 200, `count` acotado a ítems desde esa fecha.
- `GET /v4/articles/?limit=1&published_at_lte=2022-01-01` → 200, ítems hasta esa fecha.
- `GET /v4/articles/?limit=1&updated_at_gte=2026-02-01` → 200, ítems con `updated_at` ≥ esa fecha.
- `GET /v4/blogs/?limit=1&published_at_gte=2025-01-01` → 200.
- `GET /v4/reports/?limit=2&published_at_gte=2024-09-01` → 200, `count` acotado, resultados con `published_at` ≥ 2024-09-01 (p. ej. 1662, 1661).

Los ítems incluyen `published_at` y `updated_at` en ISO 8601 UTC. Para la extracción incremental se puede usar **`published_at_gte`** (y, en articles, **`updated_at_gte`** si se quieren reingestar ítems modificados). La API suele tardar unos segundos por petición; el paginado (`limit`/`offset`) se mantiene y el filtro se propaga en la URL de `next`.

---

## 2. Plan pendiente: estado desde Silver vía Athena

**Estado: pendiente.**

En Silver hay **tablas separadas por entidad**: articles, blogs, reports, info. La “última fecha” se obtiene desde esas tablas dentro de la misma Lambda de extracción:

1. **Nuevo módulo** (p. ej. `extraction/silver_state.py`): función que usa **Athena** para consultar cada tabla Silver (articles, blogs, reports; info si aplica) y devolver el **max** por entidad. Una query por tabla, p. ej. `SELECT MAX(published_at), MAX(updated_at) FROM silver_db.articles` (y análogo para blogs, reports). Esperar `GetQueryExecution` hasta `SUCCEEDED`, leer `GetQueryResults` y devolver un dict p. ej. `{"article": "2026-02-14T15:59:47.123456Z", "blog": "...", "report": "..."}`.
2. **Orquestación en `run.py`**: al inicio llamar a esa función; si hay fechas, pasarlas al cliente de API para armar `published_at_gte` (y opcionalmente `updated_at_gte`) por endpoint; si una tabla está vacía, fallback a full refresh o fecha por defecto antigua para ese tipo.
3. **Formato de fecha**: ISO 8601 UTC con la misma precisión que la API (p. ej. milisegundos) para que el filtro sea correcto.

**Requisitos:** permisos Lambda para Athena (StartQueryExecution, GetQueryExecution, GetQueryResults), workgroup y bucket de resultados de Athena, acceso al catálogo/Glue y S3 de Silver. La query añade unos segundos por ejecución.
