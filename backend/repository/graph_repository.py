import networkx as nx
from sqlalchemy import inspect, text
from shapely.geometry import MultiPoint

from app.db.database import engine


class GraphRepository:
    """
    Repositorio encargado de leer tablas de nodos y aristas desde Postgres
    y construir un grafo NetworkX en memoria.
    """

    def __init__(self):
        # Cache simple en memoria para no reconstruir el grafo en cada request
        self._cache = {}

    def _get_table_columns(self, table_name: str) -> set[str]:
        """
        Obtiene los nombres de columnas de una tabla usando el inspector de SQLAlchemy.
        """
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        return {column["name"] for column in columns}

    def _pick_column(self, available_columns: set[str], candidates: list[str], required: bool = True) -> str | None:
        """
        Selecciona la primera columna existente desde una lista de candidatos.
        """
        for candidate in candidates:
            if candidate in available_columns:
                return candidate

        if required:
            raise ValueError(
                f"No se encontró ninguna de estas columnas: {candidates}"
            )
        return None

    def load_graph(self, comuna_slug: str) -> nx.MultiDiGraph:
        """
        Carga el grafo de una comuna a partir de sus tablas:
        - <comuna_slug>_node
        - <comuna_slug>_edge

        Ejemplo:
        - santiago_node
        - santiago_edge
        """
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
            required=False
        )

        # Si no existe columna de key, generamos una secuencia con ROW_NUMBER()
        if key_col:
            key_expr = f'"{key_col}"'
        else:
            key_expr = "ROW_NUMBER() OVER ()"

        # Si no existe columna length, usamos 0.0 para no romper el cálculo
        if length_col:
            length_expr = f'COALESCE("{length_col}", 0)'
        else:
            length_expr = "0.0"

        graph = nx.MultiDiGraph()
        graph.graph["crs"] = "epsg:4326"

        # Carga de nodos
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
            nodes_result = conn.execute(nodes_query).mappings().all()

        for row in nodes_result:
            graph.add_node(
                row["node_id"],
                x=float(row["x"]),
                y=float(row["y"])
            )

        # Carga de aristas
        edges_query = text(f"""
            SELECT
                "{u_col}" AS u,
                "{v_col}" AS v,
                {key_expr} AS edge_key,
                {length_expr}::double precision AS length
            FROM "{edge_table}"
            WHERE "{u_col}" IS NOT NULL
              AND "{v_col}" IS NOT NULL
        """)

        with engine.connect() as conn:
            edges_result = conn.execute(edges_query).mappings().all()

        walking_speed_m_min = 83.33  # 5 km/h aprox.

        for row in edges_result:
            length = float(row["length"]) if row["length"] is not None else 0.0
            time_minutes = length / walking_speed_m_min if length > 0 else 0.0

            graph.add_edge(
                row["u"],
                row["v"],
                key=row["edge_key"],
                length=length,
                time=time_minutes
            )

        # Guarda el grafo en memoria para reutilizarlo en futuras consultas
        self._cache[comuna_slug] = graph
        return graph
    
    def load_boundary_polygon(self, comuna_slug):

        graph = self.load_graph(comuna_slug)

        coords = []

        for _, data in graph.nodes(data=True):

            x = data.get("x")
            y = data.get("y")

            if x is not None and y is not None:
                coords.append((x, y))

        if not coords:
            raise ValueError("No hay coordenadas")

        polygon = MultiPoint(coords).convex_hull

        return polygon