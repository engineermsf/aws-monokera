# Parte 4: Entregables cerrados

---

## 1. Entregables considerados cerrados

| # | Entregable | Dónde / Cómo |
|---|------------|--------------|
| 1 | **DAG de orquestación** | `parte4/42_codigo_dags/` y `localconfig/airflow/dags/`. Flujo: extracción → Silver (clean + dedup) → Gold (análisis, dims, facts, agregados). |
| 2 | **Tarea de extracción** | `extract`: LambdaInvokeFunctionOperator que invoca la Lambda (articles, blogs, reports, info). Parámetro `extraction_max_items` vía Trigger/conf o Variable. |
| 3 | **Tareas de procesamiento Spark** | `silver`: clean_and_deduplicate (DockerOperator local / GlueJobOperator AWS). `gold`: content_and_gold (análisis de contenido, dims, facts, aggregates). |
| 4 | **Dependencias entre tareas** | Lineales: log_payload → extract → silver → gold. Sin ramas paralelas en el ejemplo de la prueba; equivalente funcional cubierto. |
| 5 | **Configuración y documentación** | Variables (bronze_path, silver_warehouse, gold_warehouse, etc.), conexión `aws_default`, README en `localconfig/airflow/README.md`. Timeout Lambda (config_kwargs) y memoria Spark documentados. |
| 6 | **Dos modos de ejecución** | **Local:** Lambda + Glue en Docker. **AWS:** Lambda + Glue jobs en AWS; misma lógica, mismo flujo. |
| 7 | **Documentación Parte 4** | `parte4/README.md`, `41_mapeo_requisitos/`, `43_documentacion/` (mapeo, entregables, explicación detallada). |

---

## 2. Resumen en una frase

**Se entrega:** Dos DAGs (local y AWS) que orquestan extracción (Lambda), limpieza/dedup (Silver) y análisis/carga (Gold), con documentación que mapea los requisitos de la prueba y lista los entregables cerrados.
