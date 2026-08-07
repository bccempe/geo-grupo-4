import networkx as nx
from shapely.geometry import Point, MultiPoint

from repository.gtfs_repository import GTFSRepository
from repository.graph_repository import GraphRepository
from utils.comuna_util import normalize_to_slug
from utils.geojson_utils import (
    geometry_to_feature,
    point_to_feature,
    feature_collection
)

WALK_DIST_MAX = 800


class TransitIsochroneService:

    def __init__(self):

        self.gtfs_repo = GTFSRepository()
        self.graph_repo = GraphRepository()

    def _find_origin_stops(
        self,
        lat,
        lon
    ):

        return self.gtfs_repo.find_nearby_stops(
            lat,
            lon,
            max_dist_m=WALK_DIST_MAX
        )

    def _strip_suffix(self, node_id):

        if "__WAIT" in node_id:
            return node_id.replace("__WAIT", "")

        elif "__DEP" in node_id:
            return node_id.replace("__DEP", "")

        return node_id

    def _compute_reachable_stops(
        self,
        origin_stops,
        travel_graph,
        minutes
    ):

        cutoff_base_sec = minutes * 60

        reachable = set()

        for origin in origin_stops:

            stop_id = origin["stop_id"]
            walk_sec = origin["walk_time_min"] * 60

            source_node = f"{stop_id}__WAIT"

            if source_node not in travel_graph:

                reachable.add(stop_id)
                continue

            remaining = cutoff_base_sec - walk_sec

            if remaining <= 0:

                reachable.add(stop_id)
                continue

            try:

                lengths = nx.single_source_dijkstra_path_length(
                    travel_graph,
                    source=source_node,
                    cutoff=remaining,
                    weight="time"
                )

                for node_id in lengths:

                    clean = self._strip_suffix(node_id)
                    reachable.add(clean)

            except Exception:

                reachable.add(stop_id)

        return reachable

    def build_isochrone(
        self,
        comuna=None,
        lat=None,
        lon=None,
        minutes=30,
        departure_hour=None,
        include_centers=False
    ):

        print("===================================")
        print("ISOCRONA TRANSPORTE PUBLICO")
        print("comuna:", comuna)
        print("lat:", lat, "lon:", lon)
        print("minutes:", minutes)
        print("departure_hour:", departure_hour)
        print("===================================")

        if comuna is None:
            raw = self.graph_repo.find_comuna_by_coords(lat, lon)
            if raw is None:
                raise ValueError(
                    "No se pudo determinar la comuna "
                    "desde las coordenadas"
                )
            comuna = raw

        comuna_slug = normalize_to_slug(comuna)

        # =========================
        # 1. PARADAS CERCANAS
        # =========================

        print("1. BUSCANDO PARADAS CERCANAS")

        origin_stops = self._find_origin_stops(
            lat,
            lon
        )

        print(
            f"   paradas encontradas: "
            f"{len(origin_stops)}"
        )

        if not origin_stops:

            raise ValueError(
                "No hay paradas de transporte público cercanas"
            )

        # =========================
        # 2. GRAFO MULTIMODAL
        # =========================

        print("2. CONSTRUYENDO GRAFO MULTIMODAL")

        travel_graph = self.gtfs_repo.build_multimodal_graph(
            departure_hour=departure_hour
        )

        print(
            f"   nodos: "
            f"{travel_graph.number_of_nodes()}"
        )

        print(
            f"   edges: "
            f"{travel_graph.number_of_edges()}"
        )

        # =========================
        # 3. PARADAS ALCANZABLES
        # =========================

        print("3. CALCULANDO PARADAS ALCANZABLES")

        reachable_ids = self._compute_reachable_stops(
            origin_stops,
            travel_graph,
            minutes
        )

        print(
            f"   paradas alcanzables: "
            f"{len(reachable_ids)}"
        )

        if len(reachable_ids) < 3:

            raise ValueError(
                "Muy pocas paradas alcanzables "
                "para formar un polígono"
            )

        # =========================
        # 4. POLIGONO
        # =========================

        print("4. CONSTRUYENDO POLIGONO")

        stops_df = self.gtfs_repo.load_stops()

        reachable_stops = stops_df[
            stops_df["stop_id"].isin(reachable_ids)
        ]

        valid_stops = reachable_stops[
            reachable_stops["stop_lat"].notna()
        ]

        coords = list(
            zip(
                valid_stops["stop_lon"],
                valid_stops["stop_lat"]
            )
        )

        polygon = MultiPoint(coords).convex_hull

        if polygon.geom_type == "Point":

            polygon = polygon.buffer(0.002)

        elif polygon.geom_type == "LineString":

            polygon = polygon.buffer(0.002)

        # =========================
        # 5. RECORTE COMUNAL
        # =========================

        print("5. RECORTANDO CON LIMITE COMUNAL")

        boundary = self.graph_repo.load_boundary_polygon(
            comuna_slug
        )

        polygon = polygon.intersection(boundary)

        print(
            f"   area final: "
            f"{polygon.area:.6f}"
        )

        # =========================
        # 6. FEATURES
        # =========================

        print("6. GENERANDO FEATURES")

        features = []

        features.append(
            geometry_to_feature(
                polygon,
                properties={
                    "kind": "isochrone",
                    "mode": "transit",
                    "comuna": comuna_slug,
                    "minutes": minutes,
                    "departure_hour": departure_hour,
                    "reachable_stops": len(reachable_ids),
                    "origin_stops": len(origin_stops),
                    "engine": "python_networkx"
                }
            )
        )

        features.append(
            point_to_feature(
                lon,
                lat,
                properties={
                    "kind": "origin",
                    "comuna": comuna_slug,
                    "mode": "transit",
                    "engine": "python_networkx"
                }
            )
        )

        # =========================
        # 7. CENTROS DE SALUD
        # =========================

        if include_centers:

            print("7. FILTRANDO CENTROS")

            all_centers = self.gtfs_repo.get_centers_by_comuna(
                comuna_slug
            )

            reachable_centers = []

            for c in all_centers:

                pt = Point(
                    c["lng"],
                    c["lat"]
                )

                if polygon.covers(pt):

                    reachable_centers.append(c)

                    features.append(
                        point_to_feature(
                            c["lng"],
                            c["lat"],
                            properties={
                                "kind": "health_center",
                                "nombre": c.get(
                                    "nombre",
                                    "Centro de Salud"
                                ),
                                "comuna": comuna_slug,
                                "engine": "python_networkx"
                            }
                        )
                    )

            print(
                f"   centros alcanzables: "
                f"{len(reachable_centers)}"
            )

        print("===================================")
        print("ISOCRONA COMPLETADA")
        print("===================================")

        return feature_collection(
            features,
            metadata={
                "comuna": comuna_slug,
                "minutes": minutes,
                "departure_hour": departure_hour,
                "reachable_stops": len(reachable_ids),
                "origin_stops": len(origin_stops),
                "engine": "python_networkx"
            }
        )
