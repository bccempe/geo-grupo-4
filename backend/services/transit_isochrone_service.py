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
WAIT_TIME_MIN = 5
WALK_TIME_MIN = 10


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

    def _compute_reachable_stops(
        self,
        origin_stops,
        travel_graph,
        minutes
    ):

        remaining = (
            minutes
            - WALK_TIME_MIN
            - WAIT_TIME_MIN
        )

        if remaining <= 0:
            return set()

        cutoff_sec = remaining * 60

        reachable = set()

        for origin in origin_stops:

            stop_id = origin["stop_id"]

            if stop_id not in travel_graph:

                reachable.add(stop_id)
                continue

            try:

                lengths = nx.single_source_dijkstra_path_length(
                    travel_graph,
                    source=stop_id,
                    cutoff=cutoff_sec,
                    weight="time"
                )

                for s in lengths:

                    reachable.add(s)

            except Exception:

                reachable.add(stop_id)

        return reachable

    def build_isochrone(
        self,
        comuna,
        lat,
        lon,
        minutes=30,
        include_centers=False
    ):

        print("===================================")
        print("ISOCRONA TRANSPORTE PUBLICO")
        print("comuna:", comuna)
        print("lat:", lat, "lon:", lon)
        print("minutes:", minutes)
        print("===================================")

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
        # 2. GRAFO TRANSPORTE
        # =========================

        print("2. CONSTRUYENDO GRAFO DE VIAJE")

        travel_graph = self.gtfs_repo.build_travel_graph()

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

        coords = list(
            zip(
                reachable_stops["stop_lon"],
                reachable_stops["stop_lat"]
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
                    "reachable_stops": len(reachable_ids),
                    "origin_stops": len(origin_stops)
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
                    "mode": "transit"
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
                                "comuna": comuna_slug
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
                "reachable_stops": len(reachable_ids),
                "origin_stops": len(origin_stops)
            }
        )