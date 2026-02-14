# Checklist de implementación – 2.1 Extracción de datos

---

| # | Tarea |
|---|--------|
| 1 | Cliente HTTP y conexión a la API (base URL, endpoints) |
| 2 | Funciones por endpoint: articles, blogs, reports, info |
| 3 | Paginación eficiente (limit/offset o campo `next`) |
| 4 | Añadir campo `content_type` a cada ítem según endpoint |
| 5 | Deduplicación por `(content_type, id)` antes de escribir |
| 6 | Manejo de rate limits (reintentos con backoff) |
| 7 | Sistema de logs detallado (etapas, conteos, errores) |
| 8 | Escritura a Bronze en Parquet (partición content_type + year/month/day) |
| 9 | Tests unitarios (paginación, deduplicación, errores/reintentos) |
| 10 | Documentación en 21_extraccion |
