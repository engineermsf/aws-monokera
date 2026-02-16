# Imágenes para el documento de entrega

Coloca aquí las capturas con **exactamente** estos nombres. El `documento_entrega_final.md` las referenciará desde esta carpeta.

| Archivo | Contenido que debe tener |
|---------|---------------------------|
| `0_arquitecturas.png` | Diagrama(s) de arquitectura: diseño AWS y/o flujo del pipeline. |
| `1_ejecucion_pipeline_airflow.png` | DAG en Airflow: tareas log_payload, extract, silver, gold (en verde o ejecutándose). |
| `2_logs_lambda.png` | Logs de la Lambda de extracción en CloudWatch (invocación, duración, resultado). |
| `3_datos_bronze.png` | S3 o catálogo: datos en Bronze (rutas, particiones o vista previa Parquet). |
| `4_logs_glue_silver.png` | Logs del job Glue *clean_and_deduplicate* en CloudWatch o consola Glue. |
| `5_logs_glue_gold.png` | Logs del job Glue *content_and_gold* en CloudWatch o consola Glue. |
| `6_modelo_datos.png` | Diagrama del modelo dimensional: dim_news_source, dim_topic, fact_article y relaciones. |
| `7_cantidades_por_capa.png` | Tabla/captura con conteos por capa (API, Bronze, Silver, Gold). |
| `8_tendencias_1.png` | Resultado: tendencias de temas (content_type) por mes. |
| `8_tendencias_2.png` | Resultado: fuentes más influyentes por volumen total. |
| `8_tendencias_3.png` | Resultado: fuentes por mes reciente. |

Cuando añadas cada archivo, el documento ya mostrará la imagen en la sección correspondiente.
