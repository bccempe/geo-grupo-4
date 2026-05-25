import networkx as nx
from sqlalchemy import inspect, text
from shapely import wkb

from app.db.database import engine


class GraphRepository:
    """
    Repositorio encargado de leer tablas de nodos y aristas desde Postgres
    y construir un grafo NetworkX en memoria.
    """

    BOUNDARY_TABLE = "cartografia_rm_censo2024_manzanas"
    BOUNDARY_GEOM_COL = "geom"
    BOUNDARY_COMMUNE_COL = "comuna"

    def __init__(self):

        self._cache = {}
        self._boundary_cache = {}

    def _get_table_columns(self, table_name: str) -> set[str]:

        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)

        return {column["name"] for column in columns}

    def _pick_column(
        self,
        available_columns: set[str],
        candidates: list[str],
        required: bool = True
    ) -> str | None:

        for candidate in candidates:

            if candidate in available_columns:
                return candidate

        if required:

            raise ValueError(
                f"No se encontró ninguna de estas columnas: {candidates}"
            )

        return None

    def load_graph(self, comuna_slug: str) -> nx.MultiDiGraph:

        comuna_slug = comuna_slug.strip().lower()

        if comuna_slug in self._cache:
            return self._cache[comuna_slug]

        node_table = f"{comuna_slug}_nodes"
        edge_table = f"{comuna_slug}_edges"

        node_columns = self._get_table_columns(node_table)
        edge_columns = self._get_table_columns(edge_table)

        node_id_col = self._pick_column(
            node_columns,
            ["osmid", "id", "node_id", "gid", "fid"]
        )

        x_col = self._pick_column(
            node_columns,
            ["x", "lon", "longitude"]
        )

        y_col = self._pick_column(
            node_columns,
            ["y", "lat", "latitude"]
        )

        u_col = self._pick_column(
            edge_columns,
            ["u", "source", "from"]
        )

        v_col = self._pick_column(
            edge_columns,
            ["v", "target", "to"]
        )

        key_col = self._pick_column(
            edge_columns,
            ["key", "edge_key", "osmid"],
            required=False
        )

        length_col = self._pick_column(
            edge_columns,
            ["length", "cost", "dist"],
            required=True
        )

        if key_col:
            key_expr = f'"{key_col}"'
        else:
            key_expr = "ROW_NUMBER() OVER ()"

        graph = nx.MultiDiGraph()

        graph.graph["crs"] = "epsg:4326"

        # =========================
        # NODOS
        # =========================

        nodes_query = text(f"""
            SELECT
                "{node_id_col}" AS node_id,
                "{x_col}" AS x,
                "{y_col}" AS y
            FROM "{node_table}"
            WHERE "{x_col}" IS NOT NULL
              AND "{y_col}" IS NOT NULL
        """)

        with engine.connect() as conn:

            nodes_result = conn.execute(
                nodes_query
            ).mappings().all()

        for row in nodes_result:

            try:

                graph.add_node(
                    row["node_id"],
                    x=float(row["x"]),
                    y=float(row["y"])
                )

            except Exception:
                continue

        # =========================
        # EDGES
        # =========================

        edges_query = text(f"""
            SELECT
                "{u_col}" AS u,
                "{v_col}" AS v,
                {key_expr} AS edge_key,
                "{length_col}"::double precision AS length
            FROM "{edge_table}"
            WHERE "{u_col}" IS NOT NULL
              AND "{v_col}" IS NOT NULL
              AND "{length_col}" IS NOT NULL
        """)

        with engine.connect() as conn:

            edges_result = conn.execute(
                edges_query
            ).mappings().all()

        walking_speed_m_min = 83.33

        for row in edges_result:

            try:

                length = float(row["length"])

                if length <= 0:
                    continue

                time_minutes = length / walking_speed_m_min

                graph.add_edge(
                    row["u"],
                    row["v"],
                    key=row["edge_key"],
                    length=length,
                    time=time_minutes
                )

            except Exception:
                continue

        self._cache[comuna_slug] = graph

        return graph

    def load_boundary_polygon(self, comuna_slug):

        comuna_slug = comuna_slug.strip().lower()

        if comuna_slug in self._boundary_cache:
            return self._boundary_cache[comuna_slug]

        query = text(f"""
            WITH merged AS (

                SELECT

                    ST_MakeValid(
                        ST_UnaryUnion(
                            ST_Collect("{self.BOUNDARY_GEOM_COL}")
                        )
                    ) AS geom,

                    MAX(
                        ST_SRID("{self.BOUNDARY_GEOM_COL}")
                    ) AS srid

                FROM "{self.BOUNDARY_TABLE}"

                WHERE LOWER(
                    REGEXP_REPLACE(
                        TRANSLATE(
                            TRIM("{self.BOUNDARY_COMMUNE_COL}"),
                            'ÁÀÄÂáàäâÉÈËÊéèëêÍÌÏÎíìïîÓÒÖÔóòöôÚÙÜÛúùüûÑñ',
                            'AAAAaaaaEEEEeeeeIIIIiiiiOOOOooooUUUUuuuuNn'
                        ),
                        '\\s+',
                        '_',
                        'g'
                    )
                ) = :comuna
            )

            SELECT

                ST_AsBinary(

                    CASE

                        WHEN srid IS NULL
                             OR srid = 0
                             OR srid = 4326

                        THEN geom

                        ELSE ST_Transform(
                            geom,
                            4326
                        )

                    END

                ) AS geom

            FROM merged
        """)

        with engine.connect() as conn:

            result = conn.execute(
                query,
                {
                    "comuna": comuna_slug
                }
            ).fetchone()

        if result is None:

            raise ValueError(
                f"No se encontró comuna: {comuna_slug}"
            )

        if result.geom is None:

            raise ValueError(
                f"La comuna no devolvió geometría: {comuna_slug}"
            )

        polygon = wkb.loads(
            bytes(result.geom)
        )

        if polygon.is_empty:

            raise ValueError(
                f"Boundary vacío: {comuna_slug}"
            )

        if not polygon.is_valid:
            polygon = polygon.buffer(0)

        self._boundary_cache[comuna_slug] = polygon

        return polygon