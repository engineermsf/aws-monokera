-- Parte 3.2 – Análisis SQL (ejecutar en Athena, base gold)
-- Requiere vistas creadas: 05_views_gold.sql

-- ========== 1. Tendencias de temas por mes ==========
SELECT
  t.topic_name AS tema,
  v.year,
  v.month,
  v.article_count AS cantidad
FROM gold.v_agg_articles_by_content_type_month v
LEFT JOIN gold.dim_topic t ON v.content_type = t.topic_id
ORDER BY v.year DESC, v.month DESC, v.article_count DESC
LIMIT 50;

-- ========== 2. Fuentes más influyentes (por volumen total) ==========
SELECT
  news_site AS fuente,
  SUM(article_count) AS total_publicaciones
FROM gold.agg_articles_by_source_month
WHERE news_site IS NOT NULL
GROUP BY news_site
ORDER BY total_publicaciones DESC
LIMIT 20;
