import networkx as nx
from shapely.geometry import Point, MultiPoint

from repository.gtfs_repository import GTFSRepository
from repository.graph_repository import GraphRepository
from utils.comuna_util import normalize_to_slug
from utils.geojson_utils import (
    build_isochrone_polygon_from_graph,
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

    def _find_origin_stops(self, lat, lon):
        return self.gtfs_repo.find_nearby_stops(lat, lon, max_dist_m=WALK_DIST_MAX)

    def _compute_reachable_stops(self, origin_stops, travel_graph, minutes):
        remaining = minutes - WALK_TIME_MIN - WAIT_TIME_MIN
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

    def build_isochrone(self, comuna, lat, lon, minutes=30, include_centers=False):
        print("===================================")
        print("ISOCRONA TRANSPORTE PUBLICO")
        print("comuna:", comuna)
        print("lat:", lat, "lon:", lon)
        print("minutes:", minutes)
        print("===================================")

        comuna_slug = normalize_to_slug(comuna)

        print("1. BUSCANDO PARADAS CERCANAS")
        origin_stops = self._find_origin_stops(lat, lon)
        print(f"   paradas encontradas: {len(origin_stops)}")

        if not origin_stops:
            raise ValueError("No hay paradas de transporte público cercanas")

        print("2. CONSTRUYENDO GRAFO DE VIAJE")
        travel_graph = self.gtfs_repo.build_travel_graph()
        print(f"   nodos: {travel_graph.number_of_nodes()}")
        print(f"   edges: {travel_graph.number_of_edges()}")

        print("3. CALCULANDO PARADAS ALCANZABLES")
        reachable_ids = self._compute_reachable_stops(origin_stops, travel_graph, minutes)
        print(f"   paradas alcanzables: {len(reachable_ids)}")

        if len(reachable_ids) < 3:
            raise ValueError("Muy pocas paradas alcanzables para formar un polígono")

        print("4. CONSTRUYENDO POLIGONO")
        stops_df = self.gtfs_repo.load_stops()
        reachable_stops = stops_df[stops_df["stop_id"].isin(reachable_ids)]

        coords = list(zip(reachable_stops["stop_lon"], reachable_stops["stop_lat"]))
        polygon = MultiPoint(coords).convex_hull

        if polygon.geom_type == "Point":
            polygon = polygon.buffer(0.002)
        elif polygon.geom_type == "LineString":
            polygon = polygon.buffer(0.002)

        print("5. GENERANDO FEATURES")
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
                lon, lat,
                properties={
                    "kind": "origin",
                    "comuna": comuna_slug,
                    "mode": "transit"
                }
            )
        )

        if include_centers:
            print("6. FILTRANDO CENTROS")
            all_centers = self.gtfs_repo.load_primary_care_centers()
            reachable_centers = []
            for c in all_centers:
                pt = Point(c["lng"], c["lat"])
                if polygon.covers(pt):
                    reachable_centers.append(c)
            print(f"   centros alcanzables: {len(reachable_centers)}")

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
