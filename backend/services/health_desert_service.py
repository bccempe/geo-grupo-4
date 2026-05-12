import networkx as nx

from shapely.ops import unary_union
from shapely.geometry import Point

from repository.graph_repository import GraphRepository
from repository.health_repository import HealthRepository

from utils.comuna_util import normalize_to_slug
from utils.geojson_utils import (
    build_isochrone_polygon_from_graph,
    geometry_to_feature,
    feature_collection
)


class HealthDesertService:

    def __init__(self):

        self.graph_repository = GraphRepository()
        self.health_repository = HealthRepository()

    def _find_nearest_node(self, graph, lon, lat):

        nearest_node = None
        best_distance = float("inf")

        for node_id, data in graph.nodes(data=True):

            x = data.get("x")
            y = data.get("y")

            if x is None or y is None:
                continue

            distance = (x - lon) ** 2 + (y - lat) ** 2

            if distance < best_distance:
                best_distance = distance
                nearest_node = node_id

        return nearest_node

    def _build_single_isochrone(
        self,
        graph,
        lon,
        lat,
        minutes
    ):

        origin_node = self._find_nearest_node(
            graph,
            lon,
            lat
        )

        if origin_node is None:
            return None

        lengths = nx.single_source_dijkstra_path_length(
            graph,
            source=origin_node,
            cutoff=minutes,
            weight="time"
        )

        reachable_nodes = list(lengths.keys())

        if not reachable_nodes:
            return None

        subgraph = graph.subgraph(reachable_nodes).copy()

        polygon = build_isochrone_polygon_from_graph(subgraph)

        return polygon

    def build_health_deserts(
        self,
        comuna: str,
        minutes: float = 15
    ):

        comuna_slug = normalize_to_slug(comuna)

        print("1. CARGANDO GRAFO")

        graph = self.graph_repository.load_graph(comuna_slug)

        print("nodos:", graph.number_of_nodes())
        print("edges:", graph.number_of_edges())

        print("2. CARGANDO CENTROS DE SALUD")

        centers = self.health_repository.load_centers()

        print("centros:", len(centers))

        if not centers:
            raise ValueError("No hay centros de salud")

        print("3. GENERANDO ISOCRONAS")

        polygons = []

        for idx, center in enumerate(centers):

            lon = center.get("lon")
            lat = center.get("lat")

            if lon is None or lat is None:
                continue

            try:

                polygon = self._build_single_isochrone(
                    graph,
                    lon,
                    lat,
                    minutes
                )

                if polygon is not None:
                    polygons.append(polygon)

                print(f"centro {idx+1}/{len(centers)} procesado")

            except Exception as e:

                print("error centro:", e)

        if not polygons:
            raise ValueError("No se pudieron generar isócronas")

        print("4. UNIENDO COBERTURA")

        coverage_polygon = unary_union(polygons)

        print("5. OBTENIENDO LIMITE COMUNAL")

        boundary_polygon = self.graph_repository.load_boundary_polygon(
            comuna_slug
        )

        print("6. CALCULANDO DESIERTO")

        desert_polygon = boundary_polygon.difference(
            coverage_polygon
        )

        print("7. GENERANDO GEOJSON")

        features = []

        features.append(
            geometry_to_feature(
                coverage_polygon,
                properties={
                    "kind": "coverage",
                    "minutes": minutes
                }
            )
        )

        features.append(
            geometry_to_feature(
                desert_polygon,
                properties={
                    "kind": "health_desert",
                    "minutes": minutes
                }
            )
        )

        return feature_collection(
            features,
            metadata={
                "comuna": comuna_slug,
                "minutes": minutes,
                "centers": len(centers)
            }
        )