# AGENTS.md — Proyecto CESFAM (Accesibilidad a Servicios de Salud RM)

Geoinformática USACH — Grupo 4 — Ruta comercial

---

## 1. Contexto del proyecto

Plataforma web que calcula isócronas de accesibilidad a CESFAM/SAPU en comunas de la
Región Metropolitana, cruza esa cobertura con datos censales, e identifica "desiertos de salud".

Etapa actual: motor Rust `georoute` integrado en el pipeline real (foot,
car y transit/RAPTOR), aplicación React desplegada y escalamiento regional
a las 52 comunas verificado.

---

## 2. Stack

| Capa | Tecnología |
|---|---|
| Base de datos | PostgreSQL 15 + PostGIS 3.4 |
| Backend | Python 3.11, FastAPI, Uvicorn |
| Motor de rutas | `georoute` (Rust) — integrado: `georoute-foot`, `georoute-car`, `georoute-transit` (RAPTOR/GTFS) |
| Análisis espacial | GeoPandas, Shapely, NetworkX, OSMnx 1.6 (NetworkX solo en método legacy) |
| Frontend | React 18 + Leaflet, servido por Nginx (Vite en build) |
| Contenedores | Docker, Docker Compose |

Servicios ya definidos, no renombrar sin razón técnica documentada:
`IsochroneService`, `TransitIsochroneService`, `PopulationCoverageService`,
`CarHealthDesertService` (y `LocationOptimizationService`).

---

## 3. Qué no tocar sin revisión humana previa

- El esquema de PostGIS ya cargado (nodos/aristas viales, tablas GTFS, manzanas censales).
- La lógica de `IsochroneService` para Puente Alto — está validada y respalda los números
  ya publicados (≈96,7% cobertura poblacional total, ≈98,0% adultos mayores).
- La convención de CRS: EPSG:4326 para almacenamiento/interoperabilidad, EPSG:32719
  (UTM 19S) para cálculos métricos (áreas, distancias). No mezclar sistemas de referencia
  dentro de una misma operación geométrica.
- La versión de `georoute`: el Dockerfile clona la rama `master` por defecto
  (`GEOROUTE_REF`). No cambiarla sin fijar antes un commit probado y registrar la elección.

Si una tarea requiere modificar algo de esta lista, generar el diff pero no aplicarlo
automáticamente — dejarlo para revisión de un integrante.

---

## 4. Idioma y convenciones de código

- Código, nombres de variables/funciones, mensajes de commit: **inglés**.
- Docstrings y comentarios de lógica de negocio/dominio: **español** (consistente con el
  resto del proyecto).
- Formato de salida de los servicios: GeoJSON (`FeatureCollection`), como ya lo hace el
  resto del pipeline — mantener ese contrato al agregar nuevos servicios o comunas.

---

## 5. Cómo delegar tareas (para que el agente reciba specs útiles)

Toda tarea debe tener:
1. **Alcance acotado** — una función, un endpoint, un módulo. No "mejora el backend" o
   "agrega soporte para toda la RM".
2. **Criterio de éxito verificable** — qué test pasa, qué comuna/dataset se usa para
   comprobar el resultado, qué output se espera.

Ejemplo bien acotado:
> "Adaptar `PopulationCoverageService` para aceptar la RM, usando el
> mismo esquema de manzanas censales ya cargado en PostGIS. Criterio de éxito: genera el
> mismo tipo de output GeoJSON que Puente Alto, validado visualmente contra el mapa de
> cobertura poblacional."

Ejemplo mal acotado:
> "Optimiza el backend."

---

## 6. Integración de `georoute` — COMPLETADA y verificada

La guía del curso está en `integracion_georoute.pdf` y las notas
técnicas del contrato en `docs/georoute-integration.md`. Estado:

- **Punto del pipeline:** reemplaza a OSMnx+NetworkX en el flujo activo
  (isócronas, desiertos, cobertura y optimización de ubicaciones). El
  método legacy con NetworkX permanece solo para el benchmark y como
  comparación, no en el flujo público.
- **Contrato:** los tres motores (`georoute-foot`, `georoute-car`,
  `georoute-transit`) reciben coordenadas WGS84 (`lon,lat`, EPSG:4326) y
  minutos; la respuesta es GeoJSON `FeatureCollection` con
  `engine=georoute`. El cálculo métrico downstream usa EPSG:32719.
- **Empaquetado:** el motor se compila dentro de `backend/Dockerfile.georoute`
  (clona `georoute` con `GEOROUTE_REF`, corre `cargo test --workspace --locked`)
  y los servicios se orquestan en `docker-compose.yml`; el flag `--profile web`
  levanta PostGIS, los tres servidores Rust y el backend.
- **Benchmark:** `cd backend && python -m scripts.benchmark_georoute`
  (NetworkX vs georoute, mismos parámetros); `benchmark_lab3.py` cubre el
  caso real con isócronas/2SFCA/RAPTOR. Resultados en
  `resultados_informe/benchmark_georoute.md`.

---

## 7. Cómo verificar un cambio antes de aceptarlo

1. Correr el pipeline contra Puente Alto (caso ya validado) y confirmar que los
   indicadores no cambian de forma inesperada.
2. Si el cambio toca geometrías o CRS, inspeccionar visualmente el mapa resultante
   (mismos elementos que ya usan: barra de escala, norte, leyenda, fuente).
3. Confirmar que el output GeoJSON sigue siendo consumible por el frontend React (Leaflet)
   sin cambios adicionales, y que el mapa exportado conserva leyenda, norte, escala y fuente.

---

## 8. Reproducibilidad

- `docker compose --profile web up -d --build` debe levantar el proyecto completo sin pasos manuales adicionales.
- `requirements.txt` (Python) y `Cargo.toml`/`Cargo.lock` (Rust, dentro del
  contenedor del motor) deben reflejar las dependencias reales.
- Sin TODOs críticos sin resolver en el código que se entrega.