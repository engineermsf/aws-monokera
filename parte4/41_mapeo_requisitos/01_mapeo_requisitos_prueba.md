# Mapeo de requisitos Parte 4

---

## 1. Extracción

| Implementación | Operador |
|----------------|----------|
| **extract** (una tarea; articles, blogs, reports, info en un solo run) | LambdaInvokeFunctionOperator |

**Justificación:** Una sola Lambda con paginación y escritura Parquet en Bronze simplifica el DAG; el resultado (Bronze particionado) es el mismo que tener varias tareas de extracción.

---

## 2. Procesamiento (Spark)

| Implementación | Operador |
|----------------|----------|
| **silver** (clean_and_deduplicate) | DockerOperator (local) / GlueJobOperator (AWS) |
| **gold** (content_and_gold: análisis de contenido, dims, facts, aggregates) | DockerOperator / GlueJobOperator |

**Justificación:** `content_and_gold` agrupa análisis de contenido, identificación de temas, dimensiones/hechos y agregados.

---

## 3. Carga y análisis

| Implementación | Operador |
|----------------|----------|
| **gold** (escritura Iceberg: dims, fact_article, agregados; tendencias y fuentes activas en Gold). update_dashboards no implementado. | Incluido en la tarea **gold**; dashboards como mejora futura (ver `43_documentacion/02_entregables.md`). |

---

## 4. Otras tareas del DAG

| Tarea   | Propósito |
|-----------------|-----------|
| **log_payload** | PythonOperator que se ejecuta antes de **extract**. Lee de conf (Trigger), params o Variable el valor de `extraction_max_items` que se inyectará en el payload a la Lambda y lo escribe en el log de la tarea. Sirve para comprobar que el valor enviado es el esperado (p. ej. `"all"` para volcado completo o `"1"` para pruebas). No exigida por la prueba; mejora operativa. |

---

## 5. Dependencias

**Implementación:**
```text
log_payload >> extract >> silver >> gold
```

- **extract** reemplaza las tres extracciones.
- **silver** = clean_and_deduplicate.
- **gold** = perform_analysis + identify_topics + load_processed_data (+ agregados). generate_daily_insights y update_dashboards no están como tareas separadas (insights = tablas Gold; dashboards = pendiente opcional).
