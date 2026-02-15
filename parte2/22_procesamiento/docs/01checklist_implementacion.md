# Checklist de implementación – 2.2 Procesamiento con Spark

Requisitos de la prueba técnica (2.2) y del documento técnico parte1.

---

## Análisis de contenido (Content Analysis)

| # | Tarea |
|---|--------|
| 1 | Extracción de palabras clave y temas principales a partir de summary/title |
| 2 | Identificación de entidades (compañías, personas, lugares) |
| 3 | Clasificación de artículos por tema (reglas o lógica ligera; sin modelos ML pesados) |

## Análisis de tendencias (Trend Analysis)

| # | Tarea |
|---|--------|
| 4 | Tendencias de temas por tiempo (agregados por periodo: día/mes) |
| 5 | Análisis de fuentes de noticias más activas (agregados por news_site / dim_news_source) |

## Optimizaciones requeridas

| # | Tarea |
|---|--------|
| 6 | Particionamiento de datos históricos (usar particiones year/month/day en lectura y escritura) |
| 7 | Caching de resultados frecuentes (persist/cache en Spark donde se reutilicen DataFrames) |

## Requerimientos generales

| # | Tarea |
|---|--------|
| 8 | Manejo de fallos y retries (a nivel MWAA para los jobs Glue; opcionalmente reintentos dentro del script) |
| 9 | Validación de datos (esquema, nulos, rangos de fechas) antes de escribir Silver/Gold |
| 10 | Documentación completa (docs en 22_procesamiento, explicación de jobs y fases) |

## Jobs y fases

| # | Tarea |
|---|--------|
| 11 | Job **clean_and_deduplicate**: lectura Bronze, limpieza, dedup por (content_type, id), escritura Silver (Iceberg) |
| 12 | Job **content_and_gold**: lectura Silver, fases parametrizables (content, dims, facts, aggregates), escritura Gold (Iceberg) |
| 13 | Parametrización del job por fases (p. ej. `--phases content,dims,facts,aggregates` o `--phases aggregates`) |
