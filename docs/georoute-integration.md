# Integración de georoute

`georoute` reemplaza el cálculo de rutas: PostGIS continúa entregando límites
comunales, centros de salud y manzanas censales; FastAPI conserva GeoJSON y el
frontend no requiere cambios.

## Artefactos requeridos

Los archivos existentes en `backend/data/osm` son 52 redes `*.graphml`
generadas por OSMnx, una por comuna. Sirven al flujo actual de NetworkX y ya
fueron cargados en PostGIS, pero **no son una entrada compatible con
georoute**. Además, fueron descargados con perfil peatonal, por lo que no
permiten generar el perfil `car` de georoute.

No se debe intentar convertir los GraphML: no preservan la información OSM
necesaria para perfiles y restricciones. El contenedor descarga automáticamente
el PBF de Chile desde Geofabrik y lo recorta a una caja que cubre las 52
comunas de la RM. Luego construye una sola vez los grafos y las jerarquías de
contracción. No se requiere ejecutar un comando separado: el generador se
ejecuta automáticamente antes de iniciar los servidores georoute con:

```sh
docker compose --profile web up --build
```

La primera ejecución descarga aproximadamente 330 MB y puede tardar varios
minutos. Conserva el PBF y cada artefacto generado; en ejecuciones posteriores
omite los archivos que ya existan. Crea los
siguientes archivos, que no se deben versionar:

- `backend/data/georoute/rm-foot.grt`
- `backend/data/georoute/rm-foot.chg`
- `backend/data/georoute/rm-car.grt`
- `backend/data/georoute/rm-car.chg`

Una vez disponibles los cuatro artefactos,
`docker compose --profile web up --build` inicia PostGIS, los dos servidores
georoute y el backend.

## Contrato

- `georoute-foot:8090` procesa isócronas peatonales y 2SFCA.
- `georoute-car:8090` procesa isócronas vehiculares.
- Ambos reciben y devuelven coordenadas WGS84 (`lon,lat`, EPSG:4326).
- Los cálculos de área/intersección existentes deben seguir usando EPSG:32719;
  la capa de datos debe entregar esas geometrías reproyectadas antes de medir.

El endpoint nuevo `GET /api/v1/population/accessibility` entrega puntajes 2SFCA
por manzana. `GET /api/v1/car/health-deserts` ahora usa el perfil `car` de
georoute y mantiene el mismo contrato `FeatureCollection`.

Ejemplo:

```text
/api/v1/population/accessibility?comuna=puente_alto&minutes=30&decay=step
```

## Validación y benchmark

1. Comparar visualmente Puente Alto contra el resultado publicado antes de
   aceptar cualquier cambio en la isócrona peatonal validada.
2. Ejecutar una petición de isócrona repetida contra cada servidor y registrar
   el tiempo total del backend. El servidor carga los grafos una sola vez.
3. Verificar que `/api/v1/car/health-deserts` y el endpoint de accesibilidad
   sean `FeatureCollection` consumibles por el mapa.

La isócrona peatonal original no se modificó: las reglas del proyecto exigen
revisión humana antes de reemplazar esa lógica validada.
