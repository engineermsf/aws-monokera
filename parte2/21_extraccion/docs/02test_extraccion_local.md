# Tests y ejecución local – 2.1 Extracción

## Qué prueba cada test

**test_dedup.py**  
- `test_dedup_elimina_duplicados_mismo_content_type_id`: con dos registros con el mismo (content_type, id), solo queda el primero; con un tercero distinto id, quedan dos.  
- `test_dedup_mantiene_mismo_id_distinto_content_type`: mismo id en article y en blog se mantienen ambos (la clave es (content_type, id)).  
- `test_dedup_lista_vacia`: lista vacía devuelve lista vacía.

**test_api_client.py**  
- `test_fetch_page_anade_content_type_y_sigue_next`: con un session mock que devuelve results, next y count, fetch_page devuelve los ítems, la URL next y el count (no añade content_type; eso lo hace fetch_all_for_endpoint).  
- `test_fetch_all_for_endpoint_anade_content_type`: con mock de una sola página (next None), fetch_all_for_endpoint devuelve un ítem con content_type "article" y total 1.

**test_parquet_writer.py**  
- `test_write_bronze_parquet_crea_particiones`: escribe dos registros (article y blog) en un directorio temporal; comprueba que se escribieron 2 y que existen particiones por content_type o archivos parquet.  
- `test_write_bronze_parquet_lista_vacia_no_escribe`: con lista vacía, written es 0 y no se crea ningún .parquet.

## Tests unitarios

Con el entorno `monokera` activado.

Desde `parte2/21_extraccion`:

```bash
conda activate monokera
cd parte2/21_extraccion
pytest tests/ -v
```

Desde la raíz del repo:

```bash
PYTHONPATH=parte2/21_extraccion pytest parte2/21_extraccion/tests/ -v
```

## Ejecutar extracción en local (API real)

Escribe Parquet en un directorio local; útil para probar sin Lambda.

```bash
conda activate monokera
cd parte2/21_extraccion
PYTHONPATH=. python -c "from extraction.run import run; run('/tmp/bronze_test')"
```

Los archivos quedan bajo `/tmp/bronze_test/` con particiones `content_type`, year, month, day.

## Límite para pruebas (por tipo)

Con la variable **`EXTRACCION_MAX_ITEMS`** (ej. `1` o `2`) se extrae hasta N ítems **de cada tipo** (articles, blogs, reports). Ej.: `1` → 1 article + 1 blog + 1 report.

En local:

```bash
EXTRACCION_MAX_ITEMS=1 PYTHONPATH=. python -c "from extraction.run import run; run('/tmp/bronze_test')"
```

En Lambda está configurado `EXTRACCION_MAX_ITEMS=1` para las pruebas (1 de cada tipo). Para producción, quita la variable o ponla vacía.
