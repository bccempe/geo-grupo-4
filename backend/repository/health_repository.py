from sqlalchemy import inspect, text

from app.db.database import engine


class HealthRepository:
    """
    Repositorio para leer la tabla de establecimientos de salud
    almacenada en Postgres/PostGIS.
    """

    TABLE_NAME = "establecimientos_de_salud_chile_rm_establecimientos_de_492581f8"

    def __init__(self):
        self._cache = None

    def _get_table_columns(self) -> set[str]:
        """
        Obtiene las columnas de la tabla de establecimientos de salud.
        """
        inspector = inspect(engine)
        columns = inspector.get_columns(self.TABLE_NAME)
        return {column["name"] for column in columns}

    def _pick_column(self, available_columns: set[str], candidates: list[str], required: bool = True) -> str | None:
        """
        Selecciona la primera columna disponible entre candidatos posibles.
        """
        for candidate in candidates:
            if candidate in available_columns:
                return candidate

        if required:
            raise ValueError(
                f"No se encontró ninguna de estas columnas en {self.TABLE_NAME}: {candidates}"
            )
        return None

    def load_centers(self) -> list[dict]:
        """
        Carga todos los centros de salud disponibles en la tabla.
        Devuelve una lista de diccionarios con id, nombre, lon y lat.

        Esta versión soporta:
        - tablas con columnas lon/lat
        - tablas con geometría POINT
        - tablas con geometría POLYGON / MULTIPOLYGON / LINESTRING
        """
        if self._cache is not None:
            return self._cache

        columns = self._get_table_columns()

        id_col = self._pick_column(
            columns,
            ["id", "gid", "fid", "objectid", "osmid"],
            required=False
        )

        name_col = self._pick_column(
            columns,
            ["nombre_establecimiento", "nombre", "name", "nom_est", "establecimiento"],
            required=False
        )

        comuna_col = self._pick_column(
            columns,
            ["nom_comuna", "comuna", "NOM_COMUNA", "COMUNA"],
            required=False
        )

        lon_col = self._pick_column(
            columns,
            ["lon", "longitude", "x"],
            required=False
        )

        lat_col = self._pick_column(
            columns,
            ["lat", "latitude", "y"],
            required=False
        )

        geom_col = self._pick_column(
            columns,
            ["geom", "geometry", "the_geom"],
            required=False
        )

        # Construye la consulta según las columnas disponibles
        if lon_col and lat_col:
            select_parts = [
                f'"{lon_col}" AS lon',
                f'"{lat_col}" AS lat'
            ]
        elif geom_col:
            # Sirve para POINT, LINESTRING, POLYGON, MULTIPOLYGON, etc.
            # ST_MakeValid ayuda si alguna geometría viene inválida
            select_parts = [
                f'ST_X(ST_PointOnSurface(ST_MakeValid("{geom_col}"))) AS lon',
                f'ST_Y(ST_PointOnSurface(ST_MakeValid("{geom_col}"))) AS lat'
            ]
        else:
            raise ValueError(
                f"La tabla {self.TABLE_NAME} no tiene columnas de coordenadas ni geometría."
            )

        if id_col:
            select_parts.insert(0, f'"{id_col}" AS id')
        else:
            select_parts.insert(0, "ROW_NUMBER() OVER () AS id")

        if name_col:
            select_parts.insert(1, f'"{name_col}" AS name')
        else:
            select_parts.insert(1, "NULL::text AS name")

        if comuna_col:
            select_parts.append(f'"{comuna_col}" AS comuna')
        else:
            select_parts.append("NULL::text AS comuna")

        query = text(f"""
            SELECT
                {", ".join(select_parts)}
            FROM "{self.TABLE_NAME}"
            WHERE {"TRUE" if not geom_col else f'"{geom_col}" IS NOT NULL'}
        """)

        with engine.connect() as conn:
            rows = conn.execute(query).mappings().all()

        centers = []
        for row in rows:
            lon = row["lon"]
            lat = row["lat"]

            if lon is None or lat is None:
                continue

            centers.append({
                "id": row["id"],
                "name": row["name"],
                "comuna": row["comuna"],
                "lon": float(lon),
                "lat": float(lat)
            })

        self._cache = centers
        return centers
