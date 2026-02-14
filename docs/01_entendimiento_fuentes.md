# Diccionario de datos – Spaceflight News API

Este documento explica **de qué va el negocio**, **qué fuentes de datos tenemos** y **cómo vienen los datos** (con muestras reales en JSON). Sirve como base para el pipeline de ingesta y el modelo de datos.

---

## 1. De qué trata el negocio

### 1.1 La industria espacial en las noticias

La **industria espacial** no es solo cohetes y astronautas: incluye empresas (SpaceX, Blue Origin, Boeing), agencias (NASA, ESA, CNES), misiones (ISS, Luna, Marte), satélites, política espacial y negocios (comunicaciones, turismo, minería). Todo eso genera **noticias, reportes y blogs** en muchos medios.

Entender **qué se publica, cuándo y quién** permite:

- Ver **tendencias**: qué temas suben o bajan (ej. “lanzamientos”, “ISS”, “SpaceX”).
- Saber **qué fuentes son más activas** o influyentes.
- Clasificar contenido por **temas** (lanzamientos, ciencia, negocios, etc.) para dashboards y análisis.

### 1.2 Qué es la Spaceflight News API

La **Spaceflight News API** (SNAPI) es un servicio que **agrega** noticias, blogs y reportes sobre espacio de muchas fuentes en un solo sitio. En lugar de rastrear decenas de webs, usas **una API** para obtener:

- **Artículos**: noticias de medios (NASA, Space.com, Reuters, etc.).
- **Blogs**: entradas de blogs (incluye contenido amateur de calidad).
- **Reportes**: por ejemplo resúmenes diarios de la Estación Espacial Internacional (ISS).

Los datos se actualizan aproximadamente **cada 10 minutos**. La API es gratuita y no requiere API key.

### 1.3 Qué vamos a hacer con estos datos (en este proyecto)

En este proyecto construimos un **pipeline de datos** que:

1. **Ingiere** diariamente artículos, blogs y reportes desde la API.
2. **Procesa** el contenido (palabras clave, temas, entidades).
3. **Almacena** todo de forma histórica para análisis.
4. **Analiza tendencias** (temas por mes, fuentes más activas, etc.).

Para hacer eso bien, primero hay que entender **qué fuentes hay** y **qué trae cada una**. Eso es lo que sigue.

---

## 2. Las fuentes de datos

Tenemos **cuatro fuentes** en la API. Tres son **contenido** (artículos, blogs, reportes) y una es **metadatos** (info de la API).

| Fuente      | Qué es en pocas palabras |
|------------|---------------------------|
| **Articles** | Noticias de medios profesionales (NASA, Space.com, Reuters, etc.). Es el volumen más grande. |
| **Blogs**    | Entradas de blogs; pueden ser de medios o de autores/amateur de calidad. |
| **Reports**  | Reportes formales (ej. resúmenes de la ISS, comunicados de empresas como Boeing). |
| **Info**     | No es contenido: devuelve la **versión de la API** y la **lista oficial de sitios** (`news_sites`). Sirve para validar fuentes y monitoreo. |

Las tres de contenido (**articles**, **blogs**, **reports**) comparten **la misma estructura** de cada ítem (mismos campos). La diferencia es **el tipo**: si es artículo, blog o reporte. En nuestro pipeline vamos a añadir un campo **`content_type`** con valor `article`, `blog` o `report` según el endpoint.

A continuación se detalla cada fuente y se muestra **una muestra real de JSON** de cada una.

---

## 3. Fuente: Articles (artículos)

- **URL base:** `https://api.spaceflightnewsapi.net/v4/articles/`
- **Qué es:** Listado paginado de **noticias** de sitios profesionales sobre espacio.
- **Uso en el pipeline:** Principal fuente de volumen; base para tendencias y clasificación por temas.

### Muestra de respuesta (JSON real)

Respuesta de `GET /v4/articles/?limit=1`:

```json
{
  "count": 29000,
  "next": "https://api.spaceflightnewsapi.net/v4/articles/?limit=1&offset=1",
  "previous": null,
  "results": [
    {
      "id": 32549,
      "title": "Live Coverage: SpaceX Falcon 9 to make another attempt to launch Amazon Project Kuiper mission",
      "authors": [
        {
          "name": "Spaceflight Now",
          "socials": null
        }
      ],
      "url": "https://spaceflightnow.com/2025/08/10/live-coverage-spacex-falcon-9-to-make-another-attempt-to-launch-amazon-project-kuiper-mission/",
      "image_url": "http://spaceflightnow.com/wp-content/uploads/2025/08/20250810-Falcon-9-KF-02.jpg",
      "news_site": "Spaceflight Now",
      "summary": "Liftoff from Space Launch Complex 40 at Cape Canaveral Space Force Station in Florida is scheduled for 8:57 a.m. EDT (1257 UTC), after three earlier attempts were scrubbed.",
      "published_at": "2025-08-10T08:52:02Z",
      "updated_at": "2025-08-10T09:00:23.687897Z",
      "featured": false,
      "launches": [],
      "events": []
    }
  ]
}
```

- **`count`**: total de artículos que cumplen el filtro (aquí sin filtros).
- **`next` / `previous`**: URLs para paginar (offset/limit).
- **`results`**: lista de objetos; cada uno es un “article” con los campos del diccionario más abajo.

---

## 4. Fuente: Blogs

- **URL base:** `https://api.spaceflightnewsapi.net/v4/blogs/`
- **Qué es:** Listado paginado de **entradas de blog** (medios y contenido amateur de calidad).
- **Uso en el pipeline:** Mismo tratamiento que artículos; diferenciados por `content_type = 'blog'`.

### Muestra de respuesta (JSON real)

Respuesta de `GET /v4/blogs/?limit=1`:

```json
{
  "count": 1881,
  "next": "https://api.spaceflightnewsapi.net/v4/blogs/?limit=1&offset=1",
  "previous": null,
  "results": [
    {
      "id": 2095,
      "title": "NASA's SpaceX Crew-12 Reaches Orbit, News Conference at 6:45 a.m. EST",
      "authors": [
        {
          "name": "NASA",
          "socials": null
        }
      ],
      "url": "https://www.nasa.gov/blogs/spacestation/2026/02/13/nasas-spacex-crew-12-reaches-orbit-news-conference-at-645-a-m-est/",
      "image_url": "https://www.nasa.gov/wp-content/uploads/2026/02/nasas-spacex-crew-12-launch.jpg",
      "news_site": "NASA",
      "summary": "NASA astronauts Jessica Meir and Jack Hathaway, ESA (European Space Agency) astronaut Sophie Adenot, and Roscosmos cosmonaut Andrey Fedyaev, and their SpaceX Dragon spacecraft have reached orbit...",
      "published_at": "2026-02-13T10:56:29Z",
      "updated_at": "2026-02-13T11:00:14.121191Z",
      "featured": false,
      "launches": [],
      "events": []
    }
  ]
}
```

Estructura **idéntica** a articles; solo cambia el endpoint y el tipo de contenido.

---

## 5. Fuente: Reports

- **URL base:** `https://api.spaceflightnewsapi.net/v4/reports/`
- **Qué es:** Listado paginado de **reportes** (ej. resúmenes ISS, comunicados de empresas).
- **Uso en el pipeline:** Mismo esquema; `content_type = 'report'`. Algunos reportes pueden tener `authors` vacío o menos campos rellenados.

### Muestra de respuesta (JSON real)

Respuesta de `GET /v4/reports/?limit=1`:

```json
{
  "count": 1415,
  "next": "https://api.spaceflightnewsapi.net/v4/reports/?limit=1&offset=1",
  "previous": null,
  "results": [
    {
      "id": 1662,
      "title": "Starliner arrives safely back on Earth",
      "authors": [],
      "url": "https://starlinerupdates.com/starliner-arrives-safely-back-on-earth/",
      "image_url": "https://boeing-jtti.s3.amazonaws.com/wp-content/uploads/2023/02/13165520/docking_with_earth_in_background_out_window.png",
      "news_site": "Boeing",
      "summary": "Boeing's Starliner landed safely at 12:01 a.m. Eastern time on Saturday, Sept. 7...",
      "published_at": "2024-09-07T04:14:32Z",
      "updated_at": "2024-09-07T04:18:06.656360Z"
    }
  ]
}
```

En esta muestra no aparecen `featured`, `launches` ni `events`; en la API pueden ser opcionales en reportes. En el pipeline es recomendable tratarlos como **opcionales** (presentes o no) para los tres tipos.

---

## 6. Fuente: Info (metadatos de la API)

- **URL base:** `https://api.spaceflightnewsapi.net/v4/info/`
- **Qué es:** **No es contenido**. Devuelve la versión de la API y el listado oficial de nombres de sitios (`news_sites`).
- **Uso en el pipeline:** Validar que los `news_site` que vengan en articles/blogs/reports estén en esta lista; catálogo para la dimensión “fuente”; monitoreo de versión.

### Muestra de respuesta (JSON real)

Respuesta de `GET /v4/info/`:

```json
{
  "version": "4.29.0",
  "news_sites": [
    "ABC News",
    "AmericaSpace",
    "Arstechnica",
    "Blue Origin",
    "Boeing",
    "CNBC",
    "CNES",
    "ElonX",
    "ESA",
    "Euronews",
    "European Spaceflight",
    "Horizon",
    "Jet Propulsion Laboratory",
    "NASA",
    "NASASpaceflight",
    "National Geographic",
    "National Space Society",
    "Phys",
    "Planetary Society",
    "Reuters",
    "Space.com",
    "SpaceDaily",
    "SpaceFlight Insider",
    "Spaceflight Now",
    "SpaceNews",
    "SpacePolicyOnline.com",
    "Space Scout",
    "SpaceX",
    "SyFy",
    "TechCrunch",
    "Teslarati",
    "The Drive",
    "The Japan Times",
    "The Launch Pad",
    "The National",
    "The New York Times",
    "The Space Devs",
    "The Space Review",
    "The Verge",
    "The Wall Street Journal",
    "United Launch Alliance",
    "Virgin Galactic"
  ]
}
```

No hay paginación; es un único objeto.

---

## 7. Diccionario de campos (detalle por fuente)

### 7.1 Respuesta paginada (articles, blogs, reports)

| Campo     | Tipo     | Descripción |
|----------|----------|-------------|
| `count`  | integer  | Total de registros que cumplen el filtro (para saber cuántas páginas hay). |
| `next`   | string o null | URL de la siguiente página (incluye `offset` y `limit`). |
| `previous` | string o null | URL de la página anterior. |
| `results` | array   | Lista de documentos (cada uno con la estructura de la tabla siguiente). |

### 7.2 Documento individual (cada ítem en `results`)

Común a **articles**, **blogs** y **reports**. Algunos campos pueden ser opcionales en reportes.

| Campo          | Tipo     | Descripción |
|----------------|----------|-------------|
| `id`           | integer  | Identificador único del documento. Clave para deduplicación y PK en el data warehouse. |
| `title`        | string   | Título del contenido. |
| `url`          | string   | URL del contenido en el sitio original. |
| `image_url`    | string   | URL de la imagen principal. Puede estar vacía o no existir. |
| `news_site`    | string   | Nombre del sitio (ej. "NASA", "Space.com"). Debe coincidir con un valor de `info.news_sites`. |
| `summary`      | string   | Resumen o descripción. Texto principal para análisis (keywords, temas). |
| `published_at` | string   | Fecha y hora de publicación en formato ISO 8601 UTC (ej. `2025-08-10T08:52:02Z`). |
| `updated_at`   | string   | Fecha y hora de última actualización en ISO 8601 UTC. |
| `featured`     | boolean  | Si el contenido está destacado. Puede no aparecer en algunos reportes. |
| `authors`      | array    | Lista de autores (ver tabla siguiente). Puede ser `[]`. |
| `launches`     | array    | Lanzamientos relacionados (Launch Library 2). Puede ser `[]` o no aparecer. |
| `events`       | array    | Eventos relacionados (Launch Library 2). Puede ser `[]` o no aparecer. |

### 7.3 Objeto dentro de `authors[]`

| Campo   | Tipo   | Descripción |
|---------|--------|-------------|
| `name`  | string | Nombre del autor. |
| `socials` | object o null | Redes sociales. Si es null, no hay enlaces. |
| `socials.x` | string | Twitter/X. |
| `socials.youtube` | string | YouTube. |
| `socials.instagram` | string | Instagram. |
| `socials.linkedin` | string | LinkedIn. |
| `socials.mastodon` | string | Mastodon. |
| `socials.bluesky` | string | Bluesky. |

### 7.4 Objeto dentro de `launches[]`

| Campo        | Tipo   | Descripción |
|-------------|--------|-------------|
| `launch_id` | string | UUID del lanzamiento en Launch Library 2. |
| `provider`  | string | Proveedor del ID (ej. "ll2"). |

### 7.5 Objeto dentro de `events[]`

| Campo      | Tipo    | Descripción |
|------------|---------|-------------|
| `event_id` | integer | ID del evento en Launch Library 2. |
| `provider` | string  | Proveedor del ID. |

### 7.6 Respuesta de Info (sin paginación)

| Campo        | Tipo           | Descripción |
|-------------|----------------|-------------|
| `version`   | string         | Versión de la API (ej. `"4.29.0"`). |
| `news_sites`| array of string| Lista oficial de nombres de sitios. |

---

## 8. Resumen rápido para el pipeline

- **Negocio:** noticias/espacio agregadas en una API; nosotros ingerimos, procesamos y analizamos tendencias.
- **Cuatro fuentes:** articles (noticias), blogs (posts), reports (reportes), info (metadatos).
- **Articles, blogs y reports** comparten el mismo esquema de ítem; distinguirlos con `content_type`.
- **Paginación:** `limit` + `offset`; usar `next` hasta que sea null.
- **Deduplicación:** por `(content_type, id)` al unificar articles, blogs y reports en una sola tabla.
- **Fechas:** siempre ISO 8601 en UTC.
- **Info** sirve para validar `news_site` y para la dimensión de fuentes en el modelo dimensional.

Con esto se puede diseñar la extracción, el modelo de datos y los análisis de tendencias de forma alineada a las fuentes reales.
