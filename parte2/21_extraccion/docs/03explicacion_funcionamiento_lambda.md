# Explicación del funcionamiento de la Lambda de extracción

Este documento describe paso a paso cómo funciona la función Lambda `monokera-extraccion`: desde la invocación hasta la escritura de Parquet en S3.

---

## 1. Punto de entrada: `handler.py`

La Lambda usa el **handler** `handler.lambda_handler`. Al invocar la función, AWS ejecuta:

```python
def lambda_handler(event, context):
    output_path = event.get("output_path") or os.environ.get("BRONZE_OUTPUT_PATH", "/tmp/bronze")
    result = run(output_path)
    return {"statusCode": 200, "body": result}
```

- **`event`**: payload de la invocación (puede ser `{}` si se invoca sin argumentos).
- **`output_path`**: se resuelve en este orden:
  1. Si el `event` trae `"output_path"`, se usa ese valor (útil para pruebas con rutas distintas).
  2. Si no, se usa la variable de entorno **`BRONZE_OUTPUT_PATH`** (en AWS suele ser `s3://bnc-lkh-dev-bronze/spaceflight`).
  3. Si no existe, se usa `/tmp/bronze` (solo válido para pruebas locales o invocación sin config).
- **`run(output_path)`**: hace toda la lógica de extracción (ver siguiente sección).
- **Respuesta**: siempre `statusCode: 200` y en `body` un dict con `fetched`, `deduped`, `written` (y opcionalmente el resultado de errores si se maneja después).

El handler no hace reintentos ni validación fuerte del `event`; delega todo en `run()`.

---

## 2. Configuración: `extraction/config.py`

Centraliza URLs, límites y mapeos:

| Variable | Valor / Origen | Uso |
|----------|----------------|-----|
| `BASE_URL` | `https://api.spaceflightnewsapi.net/v4` | API Spaceflight News. |
| `ENDPOINTS` | `("articles", "blogs", "reports")` | Nombres de endpoints. |
| `PAGE_SIZE` | `100` | Ítems por página en la API. |
| `MAX_RETRIES` | `3` | Reintentos por request. |
| `RETRY_BACKOFF_SEC` | `2` | Base del backoff exponencial (2, 4, 8 s). |
| `RATE_LIMIT_DELAY_SEC` | `0.5` | Pausa entre páginas para no saturar la API. |
| **`MAX_ITEMS_PER_TYPE`** | Env `EXTRACCION_MAX_ITEMS` (int o `None`) | Límite **por tipo**: hasta N ítems de articles, N de blogs, N de reports. Si no está definido o no es número, no hay límite. |
| `CONTENT_TYPE_BY_ENDPOINT` | `articles→article`, `blogs→blog`, `reports→report` | Etiqueta que se añade a cada ítem para las particiones Parquet. |

La Lambda en AWS tiene `BRONZE_OUTPUT_PATH` y opcionalmente `EXTRACCION_MAX_ITEMS` (ej. `1` para pruebas: 1 article + 1 blog + 1 report).

---

## 3. Orquestación: `extraction/run.py`

`run(output_path, session=None, base_url=None)` es el núcleo de la extracción.

### 3.1 Inicialización

- Crea una **sesión HTTP** (`requests.Session()`) si no se pasa una (útil para tests con mocks).
- Usa `BASE_URL` por defecto si no se pasa `base_url`.
- Lee **`MAX_ITEMS_PER_TYPE`** de `config` (que a su vez viene de `EXTRACCION_MAX_ITEMS`).

### 3.2 Extracción por tipo

Para cada tipo en orden: **articles**, **blogs**, **reports**:

1. Se llama a `fetch_articles`, `fetch_blogs` o `fetch_reports` con la misma `session`, `base_url` y **`max_items=MAX_ITEMS_PER_TYPE`**.
2. Cada función devuelve `(items, total)`:
   - `items`: lista de ítems ya con el campo **`content_type`** asignado (article, blog, report).
   - `total`: total de ítems de ese tipo en la API (informativo).
3. Se hace **log** del número de ítems obtenidos y del total en la API.
4. Se añaden todos los ítems a una lista única **`all_items`**.

No se hace “remaining” entre tipos: el límite es **por tipo**. Con `MAX_ITEMS_PER_TYPE=1` se obtiene 1 article + 1 blog + 1 report (3 ítems en total).

### 3.3 Deduplicación

- **`_dedup_by_content_type_id(records)`**: deja un único registro por `(content_type, id)`.
- La clave es **`(content_type, id)`**: el mismo `id` en article y en blog se consideran distintos y se mantienen ambos.
- Se mantiene la primera aparición de cada clave.
- Se hace log de “Total antes de dedup” y “Total después de dedup”.

### 3.4 Escritura Parquet

- Se llama a **`write_bronze_parquet(unique, output_path)`** con la lista deduplicada y la ruta (local o `s3://...`).
- Devuelve el número de registros escritos.
- **`run()`** devuelve: `{"fetched": len(all_items), "deduped": len(unique), "written": written}`.

---

## 4. Cliente de API: `extraction/api_client.py`

### 4.1 Request y reintentos (`_request`)

- Un solo **GET** con timeout 30 s.
- Si la API responde **429** (rate limit): espera `RETRY_BACKOFF_SEC * 2^intento` segundos y reintenta (hasta `MAX_RETRIES`).
- Cualquier otra **RequestException** (red, 4xx/5xx): mismo backoff y reintento.
- Si tras todos los reintentos sigue fallando, se lanza excepción.
- Respuesta esperada: JSON; se devuelve el dict.

### 4.2 Una página (`fetch_page`)

- Llama a `_request(url, session)`.
- Aplica **`RATE_LIMIT_DELAY_SEC`** (0.5 s) después de cada request para no golpear la API.
- Del JSON se extrae:
  - **`results`**: lista de ítems de la página.
  - **`next`**: URL de la siguiente página (o `None`).
  - **`count`**: total de ítems del recurso en la API.
- Devuelve `(results, next, count)`.

### 4.3 Todos los ítems de un endpoint (`fetch_all_for_endpoint`)

- Construye la URL: `{BASE_URL}/{endpoint}/?limit={PAGE_SIZE}&offset=0`.
- En un **bucle**:
  1. Llama a `fetch_page(url, session)`.
  2. Guarda `count` la primera vez (total en API).
  3. A cada ítem de `results` le añade el campo **`content_type`** según `CONTENT_TYPE_BY_ENDPOINT[endpoint]`.
  4. Añade los ítems a una lista.
  5. Si **`max_items`** está definido y ya se tienen `>= max_items`, devuelve `items[:max_items]` y `total_count` (se deja de pedir páginas).
  6. Si hay **`next`**, actualiza `url` y repite; si no, devuelve la lista completa y `total_count`.

Las funciones **`fetch_articles`**, **`fetch_blogs`** y **`fetch_reports`** son wrappers que llaman a `fetch_all_for_endpoint` con el endpoint correspondiente y pasan `max_items` desde `run()`.

---

## 5. Escritura Parquet: `extraction/parquet_writer.py`

### 5.1 Preparación de columnas (`_prepare_columns`)

- Recorre cada registro y hace una **copia** del dict.
- Campos que vienen como **listas** en la API (`authors`, `launches`, `events`) se convierten a **string** para que Parquet pueda serializarlos sin problemas (tipos homogéneos por columna).

### 5.2 Fecha de ingesta

- Si no se pasa **`ingestion_date`**, se usa **`datetime.utcnow()`**.
- Se obtienen **year**, **month**, **day** como strings (ej. `2026`, `02`, `14`) y se usan como particiones.

### 5.3 Tabla y columnas de partición

- **`rows`** = registros preparados con `_prepare_columns`.
- Se crea un **PyArrow Table** con `pa.Table.from_pylist(rows)`.
- Se añaden tres columnas: **year**, **month**, **day** (mismo valor para todos los registros de esa ejecución).

### 5.4 Destino: local vs S3

- **Si `base_path` empieza por `s3://`**:
  - Se quita el prefijo `s3://` y se separa **bucket** y **prefix**.
  - Para PyArrow **S3FileSystem**, `root_path` debe ser **`bucket/prefix`** (sin URI).
  - Se usa **`AWS_REGION`** (env) o `us-east-1` por defecto.
  - **`pq.write_to_dataset`** con `filesystem=fs_s3`, `root_path=bucket/prefix`, `partition_cols=["content_type", "year", "month", "day"]`, `existing_data_behavior="overwrite_or_ignore"`.
- **Si no** (ruta local):
  - Se usa **`Path(base_path)`**, se crean los directorios si no existen.
  - **`pq.write_to_dataset`** con ese `root_path` y las mismas particiones, sin filesystem (local).

En ambos casos las particiones son: **content_type** (article/blog/report), **year**, **month**, **day**. La ruta final tiene la forma:

- Local: `base_path/content_type=article/year=2026/month=02/day=14/archivo.parquet`
- S3: `s3://bucket/prefix/content_type=article/year=2026/month=02/day=14/archivo.parquet`

PyArrow genera nombres de archivo únicos dentro de cada partición.

### 5.5 Retorno

- Devuelve **`len(records)`** (número de registros escritos).
- Si no hay registros, no escribe nada y devuelve **0**.

---

## 6. Flujo completo (resumen)

1. **Invocación** → `lambda_handler(event, context)`.
2. **Ruta de salida** → `event["output_path"]` o `BRONZE_OUTPUT_PATH` o `/tmp/bronze`.
3. **Límite por tipo** → `EXTRACCION_MAX_ITEMS` → `MAX_ITEMS_PER_TYPE` (N por tipo o sin límite).
4. **Extracción** → Para articles, blogs y reports:
   - Request paginado a la API (limit 100 por página, delay 0.5 s, reintentos con backoff).
   - Hasta N ítems por tipo si hay límite.
   - Cada ítem con `content_type` (article/blog/report).
5. **Deduplicación** → Por `(content_type, id)`.
6. **Parquet** → Preparar columnas (listas → string), añadir year/month/day, escribir con particiones `content_type`, year, month, day en local o S3.
7. **Respuesta** → `{ statusCode: 200, body: { fetched, deduped, written } }`.

Con esto se entiende de punta a punta el comportamiento de la Lambda de extracción y cómo se relacionan handler, config, run, api_client y parquet_writer.
