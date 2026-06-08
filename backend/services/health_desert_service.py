import networkx as nx

from shapely.ops import unary_union

from repository.gtfs_repository import GTFSRepository
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
        self.gtfs_repo = GTFSRepository()

    def _validate_graph(self, graph):

        missing_time = 0

        for _, _, edge_data in graph.edges(data=True):

            if "time" not in edge_data:
                missing_time += 1

        if missing_time > 0:

            raise ValueError(
                f"Existen {missing_time} edges sin atributo time"
            )

    def _validate_boundary(self, boundary_polygon):

        if boundary_polygon is None:

            raise ValueError(
                "Boundary polygon es None"
            )

        if boundary_polygon.is_empty:

            raise ValueError(
                "Boundary polygon vacío"
            )

    def _validate_centers(self, centers):

        if not centers:

            raise ValueError(
                "No hay centros"
            )

    def _find_nearest_node(self, graph, lon, lat):

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

        try:

            lengths = nx.single_source_dijkstra_path_length(
                graph,
                source=origin_node,
                cutoff=minutes,
                weight="time"
            )

        except Exception:
            return None

        reachable_nodes = list(lengths.keys())

        if not reachable_nodes:
            return None

        subgraph = graph.subgraph(
            reachable_nodes
        ).copy()

        try:

            polygon = build_isochrone_polygon_from_graph(
                subgraph
            )

        except Exception:
            return None

        if polygon is None:
            return None

        if polygon.is_empty:
            return None

        return polygon

    def build_health_deserts(
        self,
        comuna: str,
        minutes: float = 15
    ):

        comuna_slug = normalize_to_slug(comuna)

        graph = self.graph_repository.load_graph(
            comuna_slug
        )

        self._validate_graph(graph)

        centers = self.gtfs_repo.get_centers_by_comuna(
            comuna_slug
        )

        self._validate_centers(centers)

        polygons = []

        for center in centers:

            lon = center.get("lng")
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

            except Exception:
                continue

        if not polygons:

            raise ValueError(
                "No se pudieron generar isocronas"
            )

        coverage_polygon = unary_union(polygons)

        boundary_polygon = (
            self.graph_repository.load_boundary_polygon(
                comuna_slug
            )
        )

        self._validate_boundary(boundary_polygon)

        coverage_polygon = coverage_polygon.intersection(
            boundary_polygon
        )

        desert_polygon = boundary_polygon.difference(
            coverage_polygon
        )

        boundary_area = boundary_polygon.area

        desert_percentage = 0

        if boundary_area > 0:

            desert_percentage = (
                desert_polygon.area / boundary_area
            ) * 100

        print(
            f"[HealthDesert] comuna={comuna_slug} | "
            f"minutes={minutes} | "
            f"desert_percentage={desert_percentage:.2f}%"
        )

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
                    "minutes": minutes,
                    "desert_percentage": desert_percentage
                }
            )
        )

        return feature_collection(
            features,
            center_list=centers,
            metadata={
                "comuna": comuna_slug,
                "minutes": minutes,
                "centers_count": len(centers),
                "generated_isochrones": len(polygons),
                "desert_pct": desert_percentage
            }
        )