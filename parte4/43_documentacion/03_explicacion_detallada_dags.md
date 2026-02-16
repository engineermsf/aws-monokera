# Explicación detallada de los DAGs y cumplimiento Parte 4

Este documento describe cómo funcionan los dos DAGs (local y AWS) y cómo cubren los requisitos de la **Parte 4: Pipeline con Airflow** de la prueba técnica.

---

## 1. Lo que pide la Parte 4

La prueba pide un DAG que incluya:

- **Tasks de extracción** (ejemplo: extract_articles, extract_blogs, extract_reports).
- **Procesamiento** con Spark (clean_and_deduplicate, perform_analysis, identify_topics).
- **Carga y análisis** (load_processed_data, generate_daily_insights, update_dashboards).
- **Dependencias** entre tareas.

Indica que **no es necesario seguir los mismos nombres ni operadores**. La implementación usa una sola tarea de extracción (Lambda), dos tareas de procesamiento (silver y gold) y dependencias lineales.

---

## 2. Estructura común de ambos DAGs

Los dos DAGs comparten:

- **Mismo flujo:** `log_payload` → `extract` → `silver` → `gold`.
- **Mismo parámetro** en el DAG: `extraction_max_items` (opcional en el Trigger).
- **Mismo payload** hacia la Lambda: `output_path`, `ingestion_date`, `extraction_max_items`.
- **Misma lógica** de silver y gold (scripts `clean_and_deduplicate.py` y `content_and_gold.py`).

La diferencia es **dónde** se ejecutan silver y gold: en **local** con contenedores Docker (DockerOperator) y en **AWS** con Glue jobs (GlueJobOperator).

---

## 3. DAG local: `spaceflight_pipeline_local`

Código en `parte4/42_codigo_dags/spaceflight_pipeline_local.py`.

### 3.1 Configuración y Variables (parse time)

Al cargar el DAG, Airflow lee Variables para no hardcodear rutas ni nombres:

- `repo_host_path`, `bronze_path`, `silver_warehouse`, `gold_warehouse`, `glue_image`, `lambda_function_name`.

Se usa también un **Mount** de Docker que mapea la ruta del repo en el host al path `/home/hadoop/workspace` dentro del contenedor Glue, para que los jobs puedan ejecutar los scripts de `parte2/22_procesamiento/jobs/`.

**Requisito cubierto:** Configuración parametrizada y documentada (Variables, conexión).

### 3.2 Función `_docker_env()`

Obtiene credenciales AWS desde la conexión `aws_default` (login, password, extra con `region_name` y `aws_session_token`) y las devuelve como diccionario de variables de entorno. Ese diccionario se pasa a los `DockerOperator` de silver y gold para que el contenedor Glue pueda leer y escribir en S3.

**Requisito cubierto:** Un solo punto de configuración (conexión AWS) para Lambda y para los contenedores de procesamiento.

### 3.3 Función `_log_extraction_payload(**context)`

Es el **callable** de la tarea `log_payload`. Recibe el contexto de Airflow y:

1. Lee `dag_run.conf` y `params` (y claves alternativas por si el formulario de Trigger envía el valor con otro nombre).
2. Obtiene el valor de `extraction_max_items` que se usará en el payload (conf → params → Variable).
3. Escribe en el log de la tarea ese valor.

No es obligatoria para la prueba; sirve para depuración y para verificar que el payload a la Lambda lleva el valor esperado. **`"all"`** (o vacío) = extracción completa (todos los ítems por tipo); **`"1"`** = limitar a 1 ítem por tipo para pruebas rápidas.

### 3.4 Definición del DAG y params

- `dag_id="spaceflight_pipeline_local"`.
- `schedule=None`: solo ejecución manual (Trigger).
- `params`: un único parámetro, `extraction_max_items`, tipo string o null, descripción para el formulario de Trigger.

**Requisito cubierto:** Parámetros del DAG para controlar el comportamiento (límite de extracción) desde la UI.

### 3.5 Tarea `log_payload` (PythonOperator)

Ejecuta `_log_extraction_payload` con el contexto de la tarea. Siempre es la primera tarea del flujo.

### 3.6 Tarea `extract` (LambdaInvokeFunctionOperator) — **Extracción**

- **Operador:** LambdaInvokeFunctionOperator (equivalente funcional a varias tareas de extracción con PythonOperator en el ejemplo de la prueba).
- **Función:** nombre desde Variable `lambda_function_name` (por defecto `monokera-extraccion`).
- **Payload (Jinja):**
  - `output_path`: Variable `bronze_path`.
  - `ingestion_date`: `{{ ds }}` (fecha lógica del run).
  - `extraction_max_items`: valor resuelto desde conf, params o Variable (varias claves por compatibilidad con el formulario).
- **Conexión:** `aws_default`. Para extracciones largas, la conexión debe incluir en Extra `config_kwargs` con `read_timeout` alto (p. ej. 900).

La Lambda invocada lee los cuatro endpoints (articles, blogs, reports, info), con paginación y límite opcional, y escribe Parquet en Bronze particionado por fecha de ingesta.

**Requisito cubierto:** Tareas de extracción (consolidadas en una sola invocación Lambda que cubre articles, blogs, reports e info).

### 3.7 Tarea `silver` (DockerOperator) — **Procesamiento Spark**

- **Imagen:** Variable `glue_image` (p. ej. imagen oficial de Glue con Spark e Iceberg).
- **Comando:** `spark-submit` con:
  - Memoria: `spark.driver.memory=4g`, `spark.executor.memory=3g`.
  - Catálogo Iceberg tipo hadoop y warehouse = `params.silver_warehouse`.
  - Script: `clean_and_deduplicate.py`.
  - Argumentos: `--bronze_path`, `--silver_warehouse`, `--partition_date {{ ds }}`.

El job lee la partición de Bronze correspondiente a la fecha del run, deduplica por (content_type, id) y escribe tablas Silver en Iceberg (articles, blogs, reports, info).

**Requisito cubierto:** Procesamiento con Spark (clean_and_deduplicate).

### 3.8 Tarea `gold` (DockerOperator) — **Procesamiento y carga**

- **Comando:** `spark-submit` con catálogo Gold y script `content_and_gold.py`.
- **Argumentos:** solo `--silver_warehouse` y `--gold_warehouse` (sin `--partition_date` para merge completo).

El job lee Silver, construye dimensiones (dim_news_source, dim_topic), tabla de hechos (fact_article) y escribe en Gold con MERGE. Equivale a perform_analysis, identify_topics y load_processed_data (y agregados) en una sola tarea.

**Requisito cubierto:** Procesamiento Spark (análisis, temas), carga de datos procesados y generación de resultados para análisis (tablas Gold). generate_daily_insights queda cubierto por el contenido de Gold; update_dashboards no implementado (mejora futura).

### 3.9 Dependencias

```python
log_payload >> extract >> silver >> gold
```

Flujo lineal: primero se registra el payload, luego extracción, luego limpieza/dedup, luego análisis y carga a Gold.

**Requisito cubierto:** Dependencias entre tareas (extracción → procesamiento → carga).

---

## 4. DAG AWS: `spaceflight_pipeline_aws`

Código en `parte4/42_codigo_dags/spaceflight_pipeline_aws.py`.

La lógica es la misma que la del DAG local; solo cambia el **donde** se ejecutan silver y gold:

- **extract:** mismo LambdaInvokeFunctionOperator y mismo payload (rutas con defaults S3).
- **silver:** GlueJobOperator que lanza el job Glue `monokera-spaceflight-silver` en AWS, con `script_args`: `--bronze_path`, `--silver_warehouse`, `--partition_date {{ ds }}`.
- **gold:** GlueJobOperator que lanza el job `monokera-spaceflight-gold` con `--silver_warehouse` y `--gold_warehouse` (sin partition_date para merge completo).

Las rutas y nombres de jobs se leen de Variables (con defaults para el entorno dev). Los `script_args` usan templates Jinja para que las Variables se resuelvan en tiempo de ejecución.

**Requisito cubierto:** Mismo DAG conceptual ejecutable en otro entorno (AWS), manteniendo extracción, procesamiento Spark y carga.

---

## 5. Resumen de cobertura Parte 4

| Requisito prueba        | Implementación |
|-------------------------|----------------|
| Extracción              | Tarea `extract` (Lambda; articles, blogs, reports, info). |
| Procesamiento Spark     | Tareas `silver` (clean_and_deduplicate) y `gold` (content_and_gold). |
| Carga y análisis        | Incluido en `gold` (escritura Iceberg Gold; insights = tablas Gold). |
| Dependencias            | log_payload → extract → silver → gold. |
| Configuración/documentación | Variables, conexión aws_default, README y docs en parte4. |

Los DAGs están documentados en el código (docstrings), el mapeo con la prueba en `41_mapeo_requisitos/01_mapeo_requisitos_prueba.md` y los entregables en `43_documentacion/02_entregables.md`. El código en ejecución está en `localconfig/airflow/dags/`; la copia para el entregable en `parte4/42_codigo_dags/`.
