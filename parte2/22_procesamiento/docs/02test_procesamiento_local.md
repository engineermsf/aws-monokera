# Tests y ejecución local – 2.2 Procesamiento

## Qué se puede probar

- **Lógica de limpieza y dedup:** Con DataFrames pequeños (muestras de Bronze) validar que la deduplicación por `(content_type, id)` y la limpieza de esquema funcionan antes de escribir Silver.
- **Lógica de enriquecimiento:** Extracción de keywords/temas/entidades y clasificación por tema sobre muestras de texto (summary, title) sin necesidad de Glue.
- **Modelo dimensional:** Construcción de dim_news_source, dim_topic y fact_article a partir de un Silver de prueba (pocas filas).
- **Agregados:** Cálculo de tendencias por tema y fuentes más activas sobre un fact_article de prueba.

## Estructura de tests (prevista)

- **tests/** – Tests unitarios.
- **tests/fixtures/** – Muestras en Parquet (esquema Bronze y Silver) para alimentar los tests.
- **tests/conftest.py** – SparkSession compartido y paths de fixtures.

## Ejecución local del job

Se usa la **imagen Docker de AWS Glue** para ejecutar el script del job en local con el mismo runtime que en Glue (Spark, librerías Glue). Se montan el código y los datos de prueba (fixtures o rutas locales), se pasan los parámetros del job (fecha de partición, fases, rutas de entrada/salida) y se lanza el contenedor. Así se valida el flujo sin desplegar en AWS.

Los comandos concretos (docker run, montajes, variables) se documentarán junto con los scripts en `jobs/`.

## Referencias

- Checklist: [01checklist_implementacion.md](01checklist_implementacion.md).
- Funcionamiento de los jobs: [03explicacion_funcionamiento_jobs.md](03explicacion_funcionamiento_jobs.md).
