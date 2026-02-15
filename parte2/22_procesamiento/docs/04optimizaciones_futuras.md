# Optimizaciones futuras – 2.2 Procesamiento

Documento para mejoras posteriores al entregable 2.2. Sin carácter obligatorio.

---

## 0. Pipeline por published_at (urgente)

**Objetivo:** Reducir volumen de datos y permitir ejecuciones incrementales con una misma fecha (`published_at`) en todo el pipeline, sin que quien lance el proceso tenga que “saber” la fecha.

**Solución de raíz:**

1. **API filtrada por published_at:** Las llamadas al API de extracción se filtran por **published_at** según el día actual (o la ventana que corresponda). Solo se traen ítems publicados en esa fecha.
2. **Misma fecha en todos los pasos:** Esa fecha (`published_at`) se usa de forma **consistente** en todo el pipeline:
   - Extracción (Lambda): pide al API solo datos de esa fecha.
   - Bronze: partición por esa misma fecha (year, month, day).
   - Job Silver (`clean_and_deduplicate`): `--partition_date` = esa fecha (lee solo esa partición de Bronze, escribe en Silver).
   - Job Gold (`content_and_gold`): `--partition_date` = esa misma fecha (lee solo esa partición de Silver, MERGE en fact_article).

**Beneficios:** Menos datos desde el API, menos datos en Bronze/Silver/Gold, jobs incrementales por `published_at` y una sola fecha de corte que fluye de punta a punta. Hoy el job Gold hace full merge (lee todo Silver); con esta optimización podría volver a usar `--partition_date` para procesar solo la partición del día.

---

## 1. Reproceso por fases desde el DAG

Definir en MWAA tareas opcionales que invoquen **content_and_gold** con `--phases aggregates` (o solo `dims,facts`) para reprocesos rápidos sin re-ejecutar análisis de contenido cuando solo cambian agregados o dimensiones.

## 2. Caché y particionamiento

- Revisar qué DataFrames se reutilizan en el job y aplicar `.persist()` con nivel adecuado (MEMORY_AND_DISK si el volumen lo requiere).
- Revisar granularidad de partición en tablas Gold de agregados (p. ej. por mes en lugar de día) si el volumen lo requiere.

## 3. Validación y alertas

- Validación de datos (esquema, conteos mínimos) al final de cada job y escritura de métricas en CloudWatch o S3 para dashboards.
- **Alarma ante fallo de job:** configurar alarma en CloudWatch cuando un job de Glue falle y enviar la notificación a Teams/Slack vía AWS Chatbot, según documento técnico parte1.
