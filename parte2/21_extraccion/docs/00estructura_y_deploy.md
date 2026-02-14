# Estructura y deploy – 2.1 Extracción

## Estructura

- **extraction/** – Lógica de extracción (cliente API, Parquet, run). Se desarrolla y prueba en local; la Lambda la importa.
- **lambda/** – Lo que se despliega en AWS (handler + requirements).
- **tests/** – Tests unitarios (pytest). Ver [02_test_extraccion_local.md](02_test_extraccion_local.md).

## Descripción de los módulos

**extraction/config.py**  
Constantes: URL base de la API, lista de endpoints (articles, blogs, reports), tamaño de página, número de reintentos y backoff ante 429, delay entre requests. Mapeo endpoint → valor de `content_type` (article, blog, report).

**extraction/api_client.py**  
Cliente HTTP contra la Spaceflight News API. `_request` hace el GET con reintentos ante 429 o error de red (backoff exponencial). `fetch_page` devuelve la lista de ítems de una página, la URL `next` y el `count` total. `fetch_all_for_endpoint` pagina siguiendo `next`, añade `content_type` a cada ítem y devuelve todos los ítems y el total. `fetch_articles`, `fetch_blogs`, `fetch_reports` son atajos por endpoint. `fetch_info` obtiene la respuesta de `/info/` (versión y news_sites). Entre cada request se aplica un delay para no saturar la API.

**extraction/parquet_writer.py**  
`_prepare_columns` convierte listas/dicts anidados (authors, launches, events) a tipos que Parquet acepta (p. ej. string). `write_bronze_parquet` recibe una lista de registros, una ruta base (local o `s3://bucket/prefix`) y opcionalmente una fecha de ingesta; añade columnas year/month/day, construye una tabla PyArrow y escribe un dataset Parquet particionado por `content_type`, year, month, day (local con `Path` o S3 con `pyarrow.fs.S3FileSystem`).

**extraction/run.py**  
Orquesta la extracción: obtiene articles, blogs y reports con una misma sesión HTTP, concatena los ítems, deduplica por `(content_type, id)` manteniendo el primero, escribe Parquet en `output_path` y devuelve conteos (fetched, deduped, written). Acepta `session` y `base_url` para tests o entornos distintos.

**lambda/handler.py**  
Punto de entrada de la Lambda. Obtiene `output_path` del evento o de la variable de entorno `BRONZE_OUTPUT_PATH` (por defecto `/tmp/bronze`), llama a `run(output_path)` y devuelve el resultado en el body.

## Estilo del código: funciones vs P.O.O.

El código está escrito con **funciones** y módulos, sin clases. Para este alcance (un solo flujo de extracción, una API, un tipo de escritura Parquet) es suficiente y fácil de seguir y testear.

**P.O.O.** tendría sentido si aparecieran, por ejemplo: varios clientes de API con la misma interfaz, varios destinos de escritura (S3, local, otro almacén), o mucha configuración y estado compartido. Ahí se podría tener una clase `SpaceflightAPIClient`, otra `BronzeParquetWriter`, y una `ExtractionPipeline` que las use. Añadiendo capas y archivos; para un pipeline pequeño suele ser más claro mantener funciones y parámetros.

## Deploy automático (GitHub Actions)

El workflow `.github/workflows/deploy-extraccion.yml` se dispara en push a `feature/develop` cuando cambia algo bajo `parte2/21_extraccion/`. Empaqueta `extraction/`, `handler.py` y las dependencias en un zip y actualiza la función Lambda de dev.

Configurar en el repo (Settings → Secrets and variables → Actions):

- **AWS_ROLE_ARN** – ARN del rol IAM para OIDC (GitHub como identidad federada), o usar access key/secret en su lugar y ajustar el paso `Configure AWS credentials`.
- **LAMBDA_EXTRACCION_NAME** – Nombre de la función Lambda en la cuenta (ej. `monokera-extraccion`).

La función Lambda debe existir antes del primer deploy (creada a mano o con IaC); el workflow solo actualiza el código.
