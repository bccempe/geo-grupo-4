import networkx as nx
from shapely.geometry import Point

from repository.graph_repository import GraphRepository
from repository.health_repository import HealthRepository
from utils.comuna_util import normalize_to_slug
from utils.geojson_utils import (
    build_isochrone_polygon_from_graph,
    feature_collection,
    geometry_to_feature,
    point_to_feature
)


class IsochroneService:
    """
    Servicio principal de isócronas.
    Carga el grafo de una comuna, calcula el alcance temporal
    y devuelve un GeoJSON listo para frontend.
    """

    def __init__(self):
        self.graph_repository = GraphRepository()
        self.health_repository = HealthRepository()

    def _find_nearest_node(self, graph, lon: float, lat: float):
        """
        Busca el nodo más cercano al punto de origen usando distancia euclidiana
        sobre las coordenadas almacenadas en el grafo.
        """
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

        if nearest_node is None:
            raise ValueError("No se pudo encontrar un nodo cercano al punto de origen")

        return nearest_node

    def _filter_centers_within_polygon(self, polygon, centers: list[dict]) -> list[dict]:
        """
        Filtra los centros de salud que caen dentro de la isócrona.
        Se usa covers() para incluir también puntos sobre el borde.
        """
        reachable = []

        for center in centers:
            lon = center.get("lon")
            lat = center.get("lat")

            if lon is None or lat is None:
                continue

            point = Point(lon, lat)

            if polygon.covers(point):
                reachable.append(center)

        return reachable
    def build_isochrone(self, comuna: str, lat: float, lon: float, minutes: float = 15, include_centers: bool = False):

        print("1. NORMALIZANDO COMUNA")

        comuna_slug = normalize_to_slug(comuna)

        print("comuna_slug:", comuna_slug)

        print("2. CARGANDO GRAFO")

        graph = self.graph_repository.load_graph(comuna_slug)

        print("grafo cargado")
        print("nodos:", graph.number_of_nodes())
        print("edges:", graph.number_of_edges())

        print("3. BUSCANDO NODO MAS CERCANO")

        origin_node = self._find_nearest_node(graph, lon, lat)

        print("origin_node:", origin_node)

        print("4. EJECUTANDO DIJKSTRA")

        lengths = nx.single_source_dijkstra_path_length(
            graph,
            source=origin_node,
            cutoff=minutes,
            weight="time"
        )

        print("nodos alcanzables:", len(lengths))

        reachable_nodes = list(lengths.keys())

        if not reachable_nodes:
            raise ValueError("No se encontraron nodos alcanzables con ese tiempo")

        print("5. CREANDO SUBGRAFO")

        reachable_subgraph = graph.subgraph(reachable_nodes).copy()

        print("subgrafo creado")

        print("6. CONSTRUYENDO POLIGONO")

        polygon = build_isochrone_polygon_from_graph(reachable_subgraph)

        print("poligono generado")

        if polygon is None or polygon.is_empty:
            raise ValueError("No se pudo construir la geometría de la isócrona")

        print("7. GENERANDO FEATURES")

        features = []

        features.append(
            geometry_to_feature(
                polygon,
                properties={
                    "kind": "isochrone",
                    "comuna": comuna_slug,
                    "minutes": minutes,
                    "reachable_nodes": len(reachable_nodes),
                    "reachable_edges": reachable_subgraph.number_of_edges()
                }
            )
        )

        print("8. AGREGANDO ORIGEN")

        features.append(
            point_to_feature(
                lon,
                lat,
                properties={
                    "kind": "origin",
                    "comuna": comuna_slug
                }
            )
        )

        if include_centers:

            print("9. CARGANDO CENTROS")

            all_centers = self.health_repository.load_centers()

            print("centros encontrados:", len(all_centers))

            reachable_centers = self._filter_centers_within_polygon(
                polygon,
                all_centers
            )

            print("centros alcanzables:", len(reachable_centers))

        print("10. FINALIZANDO GEOJSON")

        return feature_collection(
            features,
            metadata={
                "comuna": comuna_slug,
                "minutes": minutes
            }
        )