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

    def _debug_graph_coordinates(self, graph):

        print("===================================")
        print("DEBUG COORDENADAS DEL GRAFO")

        xs = []
        ys = []

        for _, data in graph.nodes(data=True):

            x = data.get("x")
            y = data.get("y")

            if x is None or y is None:
                continue

            xs.append(x)
            ys.append(y)

        if not xs or not ys:
            raise ValueError(
                "El grafo no tiene coordenadas validas"
            )


        sample_nodes = list(graph.nodes(data=True))[:10]

      

    def _debug_edge_weights(self, graph):

        print("===================================")
        print("DEBUG PESOS DE EDGES")

        sample_edges = list(graph.edges(data=True))[:20]

        missing_time = 0

        for u, v, edge_data in sample_edges:

            time_value = edge_data.get("time")

            print(
                "edge:",
                u,
                v,
                "time:",
                time_value
            )

            if time_value is None:
                missing_time += 1

        if missing_time > 0:

            print("WARNING:")
            print(
                f"{missing_time} edges sin atributo time"
            )

        all_times = []

        for _, _, edge_data in graph.edges(data=True):

            time_value = edge_data.get("time")

            if time_value is not None:
                all_times.append(time_value)

        if all_times:

            print("time min:", min(all_times))
            print("time max:", max(all_times))

            avg_time = sum(all_times) / len(all_times)

            print("time promedio:", avg_time)

            if avg_time > 60:

                print("WARNING:")
                print(
                    "Los tiempos parecen estar en segundos"
                )

                print(
                    "cutoff=15 probablemente significa 15 segundos"
                )

    def _validate_input_coordinates(
        self,
        lon,
        lat
    ):

        print("===================================")
        print("VALIDANDO INPUT")

        print("lon:", lon)
        print("lat:", lat)

        if lon < -180 or lon > 180:
            raise ValueError(
                "Longitud invalida"
            )

        if lat < -90 or lat > 90:
            raise ValueError(
                "Latitud invalida"
            )

        if lon > 0:

            print(
                "WARNING: longitud positiva en Chile"
            )

        if lat > 0:

            print(
                "WARNING: latitud positiva en Chile"
            )

    def _find_nearest_node(
        self,
        graph,
        lon: float,
        lat: float
    ):

        print("===================================")
        print("BUSCANDO NODO MAS CERCANO")

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

        print("nearest_node:", nearest_node)
        print("best_distance:", best_distance)

     
          

        nearest_data = graph.nodes[nearest_node]

        print(
            "nearest node x:",
            nearest_data.get("x")
        )

        print(
            "nearest node y:",
            nearest_data.get("y")
        )

        return nearest_node

    def _filter_centers_within_polygon(
        self,
        polygon,
        centers: list[dict]
    ) -> list[dict]:

        print("===================================")
        print("FILTRANDO CENTROS")

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

            except Exception as e:

                print(
                    "ERROR VALIDANDO CENTRO:",
                    e
                )

        return reachable

    def build_isochrone(
        self,
        comuna: str,
        lat: float,
        lon: float,
        minutes: float = 15,
        include_centers: bool = False
    ):

        print("===================================")
        print("INICIO ANALISIS ISOCRONA")
        print("===================================")

        self._validate_input_coordinates(
            lon,
            lat
        )

        print("1. NORMALIZANDO COMUNA")

        comuna_slug = normalize_to_slug(comuna)

        print("comuna_slug:", comuna_slug)

        print("2. CARGANDO GRAFO")

        graph = self.graph_repository.load_graph(
            comuna_slug
        )

        print("grafo cargado")

        print(
            "nodos:",
            graph.number_of_nodes()
        )

        print(
            "edges:",
            graph.number_of_edges()
        )

        if graph.number_of_nodes() == 0:
            raise ValueError(
                "El grafo no tiene nodos"
            )

        if graph.number_of_edges() == 0:
            raise ValueError(
                "El grafo no tiene edges"
            )

        self._debug_graph_coordinates(graph)

        self._debug_edge_weights(graph)

        print("3. BUSCANDO NODO MAS CERCANO")

        origin_node = self._find_nearest_node(
            graph,
            lon,
            lat
        )

        print("origin_node:", origin_node)

        print("4. EJECUTANDO DIJKSTRA")

        try:

            lengths = nx.single_source_dijkstra_path_length(
                graph,
                source=origin_node,
                cutoff=minutes,
                weight="time"
            )

        except Exception as e:

            print("ERROR DIJKSTRA:", e)

            raise



        sample_lengths = list(lengths.items())[:20]


        reachable_nodes = list(lengths.keys())

        if not reachable_nodes:

            raise ValueError(
                "No se encontraron nodos alcanzables"
            )

        print("5. CREANDO SUBGRAFO")

        reachable_subgraph = graph.subgraph(
            reachable_nodes
        ).copy()



        try:

            polygon = build_isochrone_polygon_from_graph(
                reachable_subgraph
            )

        except Exception as e:

            print(
                "ERROR CONSTRUYENDO POLIGONO:",
                e
            )

            raise

        print("poligono generado")

        if polygon is None:

            raise ValueError(
                "Polygon es None"
            )

        if polygon.is_empty:

            raise ValueError(
                "Polygon vacio"
            )

    

        if polygon.area < 0.000001:

            print("WARNING:")
            print(
                "La isocrona tiene area extremadamente pequena"
            )

            print(
                "Probablemente hay un problema de:"
            )

            print("- time")
            print("- CRS")
            print("- nearest node")

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

            all_centers = (
                self.health_repository.load_centers()
            )

            print(
                "centros encontrados:",
                len(all_centers)
            )

            reachable_centers = (
                self._filter_centers_within_polygon(
                    polygon,
                    all_centers
                )
            )

            print(
                "centros alcanzables:",
                len(reachable_centers)
            )

            for idx, center in enumerate(
                reachable_centers[:10]
            ):

                print(
                    f"centro alcanzable {idx+1}:",
                    center
                )

        print("10. FINALIZANDO GEOJSON")

        print("===================================")
        print("ANALISIS FINALIZADO")
        print("===================================")

        return feature_collection(
            features,
            metadata={
                "comuna": comuna_slug,
                "minutes": minutes,
                "reachable_nodes": len(reachable_nodes)
            }
        )