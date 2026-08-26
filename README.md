# Accesibilidad a Servicios de Salud Primaria — Región Metropolitana

Plataforma web para calcular isócronas de accesibilidad a CESFAM/SAPU en
comunas de la Región Metropolitana, cruzar esa cobertura con datos
censales e identificar "desiertos de salud" y nuevas ubicaciones óptimas.

Proyecto de Geoinformática USACH, semestre 1 de 2026, Grupo 4 (ruta
comercial). El motor de ruteo es `georoute`, entregado para este curso.

---

## Stack

| Capa | Tecnología |
|---|---|
| Base de datos | PostgreSQL 15 + PostGIS 3.4 |
| Backend | Python 3.11, FastAPI, Uvicorn |
| Motor de rutas | `georoute` (Rust) — foot, car y transit (RAPTOR/GTFS) |
| Análisis espacial | GeoPandas, Shapely, NetworkX, OSMnx 1.6 |
| Frontend | React 18 + Leaflet, servido por Nginx |
| Contenedores | Docker, Docker Compose |

Servicios principales del backend: `IsochroneService`,
`TransitIsochroneService`, `PopulationCoverageService`,
`CarHealthDesertService`, `LocationOptimizationService` y los clientes
Rust (`GeorouteClient`, `GeorouteTransitClient`).

Convención de CRS: EPSG:4326 para almacenamiento/interoperabilidad y
EPSG:32719 (UTM 19S) para cálculos métricos. No mezclar en una misma
operación.

---

## Puesta en marcha

Prerrequisitos: Docker Desktop instalado y en ejecución, y conexión
estable (la primera corrida descarga archivos grandes: PBF OSM ~330 MB y
datos base).

```bash
cp .env.example .env
docker compose --profile web up -d --build
```

- Frontend (React): http://localhost:8501
- Backend (FastAPI): http://localhost:8000

La primera ejecución compila el motor Rust (varios minutos), construye
los grafos de la RM y recién después levanta los servidores. Las
ejecuciones posteriores reutilizan los artefactos ya generados.

> Nota de desarrollo: si la base de datos falla por puerto ocupado,
> cambiar en `docker-compose.yml` el mapeo `"5433:5432"` si el 5433 ya
> está usado. Para cargar datos base por primera vez puede requerirse
> correr el servicio `drive-sync` (perfil `batch`) una vez antes del
> arranque web.

---

## Uso

La aplicación ofrece cuatro vistas:

1. **Isócronas** — caminata y transporte público desde un centro u origen
   libre.
2. **Desiertos de salud** — territorio sin cobertura dentro del umbral.
3. **Cobertura poblacional** — por comuna o consolidado RM, con modos
   caminata, automóvil y transporte público.
4. **Ubicaciones óptimas** — propuesta de nuevos centros que maximiza la
   población (o adultos mayores) adicionalmente cubierta.

---

## Reproducción (tests, benchmark y artefactos)

Con el stack levantado:

```bash
# Pruebas del backend (38 tests)
docker compose --profile web exec -T backend pytest -q

# Benchmark NetworkX (PEP1) vs georoute (PEP2)
docker compose --profile web exec -T backend python -m scripts.benchmark_georoute

# Métricas y figuras para una comuna
docker compose --profile web exec -T backend python -m scripts.generate_metrics --comuna puente_alto

# Métricas regionales completas (52 comunas, ~16-20 min)
docker compose --profile web exec -T backend python -m scripts.generate_metrics
```

La imagen Rust (`backend/Dockerfile.georoute`) ejecuta
`cargo test --workspace --locked` antes de compilar; si las pruebas del
motor fallan no se publica la imagen.

---

## Configuración con variables de entorno

Ver `.env.example`. Las variables sensibles (`POSTGRES_PASSWORD`,
tokens) se mantienen en `.env` local, fuera de Git.

- `GEOROUTE_FOOT_URL`, `GEOROUTE_CAR_URL`, `GEOROUTE_TRANSIT_URL` — URLs
  internas de los motores Rust en la red Docker.
- `GEOROUTE_TRANSIT_DATE` — fecha del GTFS usada por RAPTOR (vacío =
  hoy).
- `GEOROUTE_TIMEOUT_SECONDS` — timeout de consulta al motor.

---

## Endpoints principales (API)

| Endpoint | Motor | Descripción |
|---|---|---|
| `GET /api/v1/isochrone` | `georoute` foot | Isócrona peatonal |
| `GET /api/v1/transit/isochrone` | `georoute-transit` | Isócrona por transporte público |
| `GET /api/v1/health-deserts` | `georoute` foot | Desiertos de salud (caminata) |
| `GET /api/v1/car/health-deserts` y `-rm` | `georoute` car | Desiertos en automóvil |
| `GET /api/v1/population/coverage` y `-rm` | foot/car | Cobertura poblacional |
| `GET /api/v1/population/transit-coverage` y `-rm` | transit | Cobertura por transporte público |
| `GET /api/v1/population/accessibility` | foot/2SFCA | Accesibilidad 2SFCA |
| `GET /api/v1/location/optimize` | foot + greedy | Ubicaciones óptimas |
| `POST /api/v1/export/*` | — | Exportación de mapas a PNG |

Todos devuelven GeoJSON `FeatureCollection` conforme al contrato del
pipeline; la exportación reproyecta a EPSG:32719 con leyenda, norte,
escala y fuente a 300 dpi.

---

## Estructura del repositorio

```
backend/
  app/ controllers/ repository/ services/ utils/   Backend FastAPI
  scripts/           scripts de carga, métricas y benchmark
  tests/             pruebas pytest
  Dockerfile         imagen Python
  Dockerfile.georoute imagen del motor Rust
  data/              datos base, grafos (no versionados)
frontend/
  src/               React + Leaflet
  Dockerfile         build Vite + Nginx
docker-compose.yml   orquestación (perfiles web y batch)
docs/                notas de integración de georoute
figuras/             mapas generados para los informes
resultados_informe/  tablas, gráficos y lab 3
```

---

## Reproducibilidad y entregables

- `docker compose --profile web up -d --build` levanta todo sin pasos
  manuales.
- `requirements.txt` (Python) y `Cargo.toml`/`Cargo.lock` (Rust, dentro
  del contenedor del motor) reflejan las dependencias.
- Sin TODOs críticos sin resolver.
- Informes LaTeX: `informe.tex` (PEP 1) e `informe2.tex` (PEP 2).

Referencias de datos: Censo de Población y Vivienda 2024 (INE), Red
Metropolitana de Movilidad (DTPM, GTFS) y OpenStreetMap.