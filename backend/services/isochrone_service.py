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

    def _validate_input_coordinates(
        self,
        lon,
        lat
    ):

        if lon < -180 or lon > 180:
            raise ValueError(
                "Longitud invalida"
            )

        if lat < -90 or lat > 90:
            raise ValueError(
                "Latitud invalida"
            )

    def _find_nearest_node(
        self,
        graph,
        lon: float,
        lat: float
    ):

        nearest_node = None
        best_distance = float("inf")

        for node_id, data in graph.nodes(data=True):

            x = data.get("x")
            y = data.get("y")

            if x is None or y is None:
                continue

            distance = (
                (x - lon) ** 2 +
                (y - lat) ** 2
            )

            if distance < best_distance:

                best_distance = distance
                nearest_node = node_id

        if nearest_node is None:

            raise ValueError(
                "No se pudo encontrar un nodo cercano"
            )

        return nearest_node

    def _filter_centers_within_polygon(
        self,
        polygon,
        centers: list[dict]
    ) -> list[dict]:

        reachable = []

        for center in centers:

            lon = center.get("lon")
            lat = center.get("lat")

            if lon is None or lat is None:
                continue

            point = Point(lon, lat)

            try:

                if polygon.covers(point):
                    reachable.append(center)

            except Exception:
                continue

        return reachable

    def build_isochrone(
        self,
        comuna: str,
        lat: float,
        lon: float,
        minutes: float = 15,
        include_centers: bool = False
    ):

        self._validate_input_coordinates(
            lon,
            lat
        )

        comuna_slug = normalize_to_slug(comuna)

        graph = self.graph_repository.load_graph(
            comuna_slug
        )

        if graph.number_of_nodes() == 0:
            raise ValueError(
                "El grafo no tiene nodos"
            )

        if graph.number_of_edges() == 0:
            raise ValueError(
                "El grafo no tiene edges"
            )

        origin_node = self._find_nearest_node(
            graph,
            lon,
            lat
        )

        try:

            lengths = nx.single_source_dijkstra_path_length(
                graph,
                source=origin_node,
                cutoff=minutes,
                weight="time"
            )

        except Exception as e:

            raise ValueError(
                f"Error ejecutando Dijkstra: {e}"
            )

        reachable_nodes = list(lengths.keys())

        if not reachable_nodes:

            raise ValueError(
                "No se encontraron nodos alcanzables"
            )

        reachable_subgraph = graph.subgraph(
            reachable_nodes
        ).copy()

        try:

            polygon = build_isochrone_polygon_from_graph(
                reachable_subgraph
            )

        except Exception as e:

            raise ValueError(
                f"Error construyendo poligono: {e}"
            )

        if polygon is None:

            raise ValueError(
                "Polygon es None"
            )

        if polygon.is_empty:

            raise ValueError(
                "Polygon vacio"
            )

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

            all_centers = (
                self.health_repository.load_centers()
            )

            reachable_centers = (
                self._filter_centers_within_polygon(
                    polygon,
                    all_centers
                )
            )

            for center in reachable_centers:

                center_lon = center.get("lon")
                center_lat = center.get("lat")

                if center_lon is None or center_lat is None:
                    continue

                features.append(
                    point_to_feature(
                        center_lon,
                        center_lat,
                        properties={
                            "kind": "health_center",
                            "name": center.get("name"),
                            "comuna": center.get("comuna")
                        }
                    )
                )

        return feature_collection(
            features,
            metadata={
                "comuna": comuna_slug,
                "minutes": minutes,
                "reachable_nodes": len(reachable_nodes)
            }
        )