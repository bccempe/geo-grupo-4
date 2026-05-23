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

    def _validate_graph(self, graph):



        sample_nodes = list(graph.nodes(data=True))[:5]

    
        missing_time = 0

        for _, _, edge_data in graph.edges(data=True):

            if "time" not in edge_data:
                missing_time += 1

        print("edges sin atributo time:", missing_time)

        sample_edges = list(graph.edges(data=True))[:5]



    def _validate_boundary(self, boundary_polygon):

        print("VALIDANDO LIMITE COMUNAL")

        if boundary_polygon is None:
            raise ValueError("Boundary polygon es None")

    def _validate_centers(self, centers):

        print("VALIDANDO CENTROS")

        if not centers:
            raise ValueError("No hay centros")

        print("cantidad centros:", len(centers))

        for idx, center in enumerate(centers[:10]):

            print(
                f"centro {idx+1}:",
                center
            )

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

            print("No se encontro nodo cercano")

            return None

        try:

            lengths = nx.single_source_dijkstra_path_length(
                graph,
                source=origin_node,
                cutoff=minutes,
                weight="time"
            )

        except Exception as e:

            print("ERROR DIJKSTRA:", e)

            return None

        reachable_nodes = list(lengths.keys())

        print("reachable_nodes:", len(reachable_nodes))

        if not reachable_nodes:

            print("No hay reachable nodes")

            return None

        sample_lengths = list(lengths.items())[:10]


        subgraph = graph.subgraph(reachable_nodes).copy()


        try:

            polygon = build_isochrone_polygon_from_graph(
                subgraph
            )

        except Exception as e:

            print("ERROR CONSTRUYENDO POLIGONO:", e)

            return None

        if polygon is None:

            print("Polygon es None")

            return None


        return polygon

    def build_health_deserts(
        self,
        comuna: str,
        minutes: float = 15
    ):

        comuna_slug = normalize_to_slug(comuna)

        print("===================================")
        print("INICIO ANALISIS")
        print("comuna:", comuna_slug)
        print("minutes:", minutes)
        print("===================================")

        print("1. CARGANDO GRAFO")

        graph = self.graph_repository.load_graph(
            comuna_slug
        )

        print("nodos:", graph.number_of_nodes())
        print("edges:", graph.number_of_edges())

        self._validate_graph(graph)

        print("2. CARGANDO CENTROS")

        centers = self.health_repository.load_centers()

        self._validate_centers(centers)

        print("3. GENERANDO ISOCRONAS")

        polygons = []

        for idx, center in enumerate(centers):

            print("===================================")
            print(f"PROCESANDO CENTRO {idx+1}/{len(centers)}")

            lon = center.get("lon")
            lat = center.get("lat")

            print("lon:", lon)
            print("lat:", lat)

            if lon is None or lat is None:

                print("Centro sin coordenadas")

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

                    print("Poligono agregado")

                else:

                    print("Poligono no generado")

            except Exception as e:

                print("ERROR CENTRO:", e)

        print("===================================")
        print("TOTAL POLIGONOS:", len(polygons))

        if not polygons:
            raise ValueError(
                "No se pudieron generar isocronas"
            )

        print("4. UNIENDO COBERTURA")

        coverage_polygon = unary_union(polygons)

        print("coverage type:", coverage_polygon.geom_type)
        print("coverage area:", coverage_polygon.area)
        print("coverage bounds:", coverage_polygon.bounds)
        print("coverage valid:", coverage_polygon.is_valid)

        print("5. OBTENIENDO LIMITE COMUNAL")

        boundary_polygon = (
            self.graph_repository.load_boundary_polygon(
                comuna_slug
            )
        )

        self._validate_boundary(boundary_polygon)

        print("6. INTERSECTANDO COBERTURA CON LIMITE")

        coverage_polygon = coverage_polygon.intersection(
            boundary_polygon
        )

        print(
            "coverage clipped area:",
            coverage_polygon.area
        )

        print("7. CALCULANDO DESIERTO")

        desert_polygon = boundary_polygon.difference(
            coverage_polygon
        )

        print("desert type:", desert_polygon.geom_type)
        print("desert area:", desert_polygon.area)
        print("desert bounds:", desert_polygon.bounds)
        print("desert valid:", desert_polygon.is_valid)

        boundary_area = boundary_polygon.area

        if boundary_area > 0:

            desert_percentage = (
                desert_polygon.area / boundary_area
            ) * 100

            print(
                "PORCENTAJE DESIERTO:",
                desert_percentage
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
                    "minutes": minutes
                }
            )
        )

        print("===================================")
        print("ANALISIS FINALIZADO")
        print("===================================")

        return feature_collection(
            features,
            metadata={
                "comuna": comuna_slug,
                "minutes": minutes,
                "centers": len(centers),
                "generated_polygons": len(polygons)
            }
        )