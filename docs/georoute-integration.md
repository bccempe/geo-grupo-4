# Integracion de georoute

`georoute` reemplaza el calculo de rutas: PostGIS continua entregando limites
comunales, centros de salud y manzanas censales; FastAPI conserva GeoJSON y el
frontend no requiere cambios.

## Artefactos requeridos

Los archivos existentes en `backend/data/osm` son 52 redes `*.graphml`
generadas por OSMnx, una por comuna. Sirven al flujo legacy de NetworkX y ya
fueron cargados en PostGIS, pero no son una entrada compatible con georoute.
Ademas, fueron descargados con perfil peatonal, por lo que no permiten generar
el perfil `car` de georoute.

No se debe intentar convertir los GraphML: no preservan la informacion OSM
necesaria para perfiles y restricciones. El contenedor descarga automaticamente
el PBF de Chile desde Geofabrik y lo recorta a una caja que cubre las 52
comunas de la RM. Luego construye una sola vez los grafos y las jerarquias de
contraccion. No se requiere ejecutar un comando separado: el generador se
ejecuta automaticamente antes de iniciar los servidores georoute con:

```sh
docker compose --profile web up --build
```

La primera ejecucion descarga aproximadamente 330 MB y puede tardar varios
minutos. Conserva el PBF y cada artefacto generado; en ejecuciones posteriores
omite los archivos que ya existan. Crea los siguientes archivos, que no se deben
versionar:

- `backend/data/georoute/rm-foot.grt`
- `backend/data/georoute/rm-foot.chg`
- `backend/data/georoute/rm-car.grt`
- `backend/data/georoute/rm-car.chg`

Una vez disponibles los cuatro artefactos,
`docker compose --profile web up --build` inicia PostGIS, los dos servidores
georoute y el backend.

## Contrato

- `georoute-foot:8090` procesa isocronas peatonales y 2SFCA.
- `georoute-car:8090` procesa isocronas vehiculares.
- Ambos reciben y devuelven coordenadas WGS84 (`lon,lat`, EPSG:4326).
- Los calculos de area/interseccion existentes deben seguir usando EPSG:32719;
  la capa de datos debe entregar esas geometrias reproyectadas antes de medir.

Los endpoints peatonales `GET /api/v1/isochrone`,
`GET /api/v1/health-deserts`, `GET /api/v1/population/accessibility`,
`GET /api/v1/population/coverage` y `GET /api/v1/population/coverage-rm`
usan `georoute-foot` y mantienen el contrato `FeatureCollection`.

Los endpoints vehiculares `GET /api/v1/car/health-deserts` y
`GET /api/v1/car/health-deserts-rm` usan `georoute-car`.

Los endpoints de transporte publico siguen usando el grafo GTFS multimodal en
Python/NetworkX porque el compose actual no define un servidor georoute con
perfil de transporte publico ni un contrato HTTP para GTFS.

Ejemplo:

```text
/api/v1/population/accessibility?comuna=puente_alto&minutes=30&decay=step
```

## Validacion y benchmark

1. Comparar visualmente Puente Alto contra el resultado publicado antes de
   aceptar cambios en isocronas peatonales.
2. Ejecutar una peticion de isocrona repetida contra cada servidor y registrar
   el tiempo total del backend. El servidor carga los grafos una sola vez.
3. Verificar que los endpoints de isocronas, desiertos, cobertura poblacional y
   accesibilidad sean `FeatureCollection` consumibles por el mapa.
