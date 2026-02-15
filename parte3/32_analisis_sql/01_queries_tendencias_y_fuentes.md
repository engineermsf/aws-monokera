# Análisis SQL – Tendencias y fuentes (Parte 3.2)

---

## 1. Tendencias de temas por mes

Obtiene el volumen de publicaciones por **tipo de contenido** (tema: article, blog, report) y por **mes**, ordenado por tiempo y por cantidad.

**Query:**

```sql
-- Tendencias de temas (content_type) por mes
SELECT
  t.topic_name AS tema,
  v.year,
  v.month,
  v.article_count AS cantidad
FROM gold.v_agg_articles_by_content_type_month v
LEFT JOIN gold.dim_topic t ON v.content_type = t.topic_id
ORDER BY v.year DESC, v.month DESC, v.article_count DESC
LIMIT 50;
```

**Qué hace:** Usa la vista que agrupa por `content_type`, `year`, `month` y une con `dim_topic` para mostrar el nombre del tema. Útil para ver evolución mensual de artículos vs blogs vs reportes.


---

## 2. Fuentes más influyentes

Obtiene las **fuentes de noticias** con más publicaciones (total histórico o por periodo reciente), ordenadas por volumen.

**Query (por volumen total):**

```sql
-- Fuentes más influyentes (por volumen total de publicaciones)
SELECT
  news_site AS fuente,
  SUM(article_count) AS total_publicaciones
FROM gold.agg_articles_by_source_month
WHERE news_site IS NOT NULL
GROUP BY news_site
ORDER BY total_publicaciones DESC
LIMIT 20;
```

**Query alternativa (top por mes reciente, sin agrupar total):**

```sql
SELECT news_site AS fuente, year, month, article_count AS publicaciones
FROM gold.agg_articles_by_source_month
WHERE news_site IS NOT NULL
ORDER BY year DESC, month DESC, article_count DESC
LIMIT 30;
```

La **primera** query de esta sección (volumen total por fuente) es la que responde directamente “fuentes más influyentes” (más publicaciones = más influencia en el dataset).


