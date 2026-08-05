# AGENTS.md — Proyecto CESFAM (Accesibilidad a Servicios de Salud RM)

Geoinformática USACH — Grupo 4 — Ruta comercial

---

## 1. Contexto del proyecto

Plataforma web que calcula isócronas de accesibilidad a CESFAM/SAPU en comunas de la
Región Metropolitana, cruza esa cobertura con datos censales, e identifica "desiertos de salud".

Etapa actual: escalar de 1 comuna validada (Puente Alto) a un segundo caso generalizado (Región Metropolitana completa), e
integrar el motor Rust `georoute` en el pipeline real.

---

## 2. Stack

| Capa | Tecnología |
|---|---|
| Base de datos | PostgreSQL 15 + PostGIS 3.4 |
| Backend | Python 3.11, FastAPI, Uvicorn |
| Motor de rutas | `georoute` (Rust) — por integrar |
| Análisis espacial | GeoPandas, Shapely, NetworkX, OSMnx 1.6 |
| Frontend | Streamlit + Folium |
| Contenedores | Docker, Docker Compose |

Servicios ya definidos, no renombrar sin razón técnica documentada:
`IsochroneService`, `TransitIsochroneService`, `PopulationCoverageService`,
`CarHealthDesertService`.

---

## 3. Qué no tocar sin revisión humana previa

- El esquema de PostGIS ya cargado (nodos/aristas viales, tablas GTFS, manzanas censales).
- La lógica de `IsochroneService` para Puente Alto — está validada y respalda los números
  ya publicados (≈96,5% cobertura poblacional total, ≈97,9% adultos mayores).
- La convención de CRS: EPSG:4326 para almacenamiento/interoperabilidad, EPSG:32719
  (UTM 19S) para cálculos métricos (áreas, distancias). No mezclar sistemas de referencia
  dentro de una misma operación geométrica.

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

## 6. Integración de `georoute` — PENDIENTE DE DETALLE

⚠️ Falta la guía `integracion_georoute.tex`. Completar esta sección con:

- [ ] Punto exacto del pipeline donde reemplaza o complementa a OSMnx+NetworkX
- [ ] Contrato de entrada/salida esperado (formato de grafo, unidades de tiempo/distancia)
- [ ] Cómo se compila/empaqueta dentro del `docker-compose.yml` existente
- [ ] Cómo correr un benchmark simple (tiempo de cálculo antes/después del cambio)

---

## 7. Cómo verificar un cambio antes de aceptarlo

1. Correr el pipeline contra Puente Alto (caso ya validado) y confirmar que los
   indicadores no cambian de forma inesperada.
2. Si el cambio toca geometrías o CRS, inspeccionar visualmente el mapa resultante
   (mismos elementos que ya usan: barra de escala, norte, leyenda, fuente).
3. Confirmar que el output GeoJSON sigue siendo consumible por el frontend Streamlit sin
   cambios adicionales.

---

## 8. Reproducibilidad

- `docker compose up` debe levantar el proyecto completo sin pasos manuales adicionales.
- `requirements.txt` (Python) y `Cargo.toml`/`Cargo.lock` (Rust, una vez integrado
  `georoute`) deben reflejar las dependencias reales.
- Sin TODOs críticos sin resolver en el código que se entrega.