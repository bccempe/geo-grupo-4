import networkx as nx
from shapely.geometry import MultiPoint
from shapely.ops import unary_union

from repository.gtfs_repository import GTFSRepository
from repository.graph_repository import GraphRepository
from utils.comuna_util import normalize_to_slug
from utils.geojson_utils import (
    geometry_to_feature,
    feature_collection
)


class TransitHealthDesertService:

    def __init__(self):

        self.gtfs_repo = GTFSRepository()
        self.graph_repo = GraphRepository()

    def _build_single_isochrone(
        self,
        travel_graph,
        lat,
        lon,
        minutes
    ):

        origin_stops = self.gtfs_repo.find_nearby_stops(
            lat,
            lon,
            max_dist_m=800
        )

        if not origin_stops:
            return None

        remaining = minutes - 5 - 10

        if remaining <= 0:
            return None

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

        if len(reachable) < 3:
            return None

        stops_df = self.gtfs_repo.load_stops()

        reachable_stops = stops_df[
            stops_df["stop_id"].isin(reachable)
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

        return polygon

    def build_health_deserts(
        self,
        comuna,
        minutes=30
    ):

        comuna_slug = normalize_to_slug(comuna)

        print("===================================")
        print("DESIERTOS DE SALUD - TRANSPORTE PUBLICO")
        print("comuna:", comuna_slug)
        print("minutes:", minutes)
        print("===================================")

        # =========================
        # 1. CARGAR CENTROS DESDE DB
        # =========================

        print("1. CARGANDO CENTROS DE SALUD")

        centers = self.gtfs_repo.get_centers_by_comuna(
            comuna_slug
        )

        print(f"   centros encontrados: {len(centers)}")

        if not centers:

            raise ValueError(
                f"No se encontraron centros de salud en {comuna_slug}"
            )

        # =========================
        # 2. GRAFO GTFS
        # =========================

        print("2. CONSTRUYENDO GRAFO DE TRANSPORTE")

        travel_graph = self.gtfs_repo.build_travel_graph()

        print(
            f"   nodos: {travel_graph.number_of_nodes()} paradas"
        )

        # =========================
        # 3. ISOCRONAS
        # =========================

        print("3. GENERANDO ISOCRONAS")

        polygons = []

        for idx, center in enumerate(centers):

            if idx % 10 == 0:

                print(
                    f"   progreso: {idx}/{len(centers)}"
                )

            try:

                poly = self._build_single_isochrone(
                    travel_graph,
                    center["lat"],
                    center["lng"],
                    minutes
                )

                if poly is not None:

                    polygons.append(poly)

            except Exception as e:

                print(
                    f"   error en centro "
                    f"{center.get('nombre', 'sin_nombre')}: {e}"
                )

        print(
            f"   isocronas generadas: {len(polygons)}"
        )

        if not polygons:

            raise ValueError(
                "No se pudieron generar isocronas"
            )

        # =========================
        # 4. UNION COBERTURA
        # =========================

        print("4. UNIENDO COBERTURA")

        coverage = unary_union(polygons)

        print(
            f"   area cobertura: "
            f"{coverage.area:.6f}"
        )

        # =========================
        # 5. LIMITE COMUNAL
        # =========================

        print("5. CARGANDO LIMITE COMUNAL")

        boundary = self.graph_repo.load_boundary_polygon(
            comuna_slug
        )

        print(
            f"   area comuna: "
            f"{boundary.area:.6f}"
        )

        # =========================
        # 6. RECORTE
        # =========================

        print("6. RECORTANDO COBERTURA")

        coverage = coverage.intersection(boundary)

        # =========================
        # 7. DESIERTO
        # =========================

        print("7. CALCULANDO DESIERTO")

        desert = boundary.difference(coverage)

        print(
            f"   area desierto: "
            f"{desert.area:.6f}"
        )

        if boundary.area > 0:

            pct = (
                desert.area / boundary.area
            ) * 100

            print(
                f"   porcentaje desierto: "
                f"{pct:.1f}%"
            )

        # =========================
        # 8. FEATURES
        # =========================

        print("8. GENERANDO FEATURES")

        features = []

        features.append(
            geometry_to_feature(
                coverage,
                properties={
                    "kind": "coverage",
                    "mode": "transit",
                    "minutes": minutes,
                    "comuna": comuna_slug
                }
            )
        )

        features.append(
            geometry_to_feature(
                desert,
                properties={
                    "kind": "health_desert",
                    "mode": "transit",
                    "minutes": minutes,
                    "comuna": comuna_slug
                }
            )
        )

        print("===================================")
        print("DESIERTOS COMPLETADOS")
        print("===================================")

        return feature_collection(
            features,
            center_list=centers,
            metadata={
                "comuna": comuna_slug,
                "minutes": minutes,
                "centers_count": len(centers),
                "generated_isochrones": len(polygons),
                "coverage_area": coverage.area,
                "desert_area": desert.area,
                "desert_pct": (
                    desert.area / boundary.area * 100
                ) if boundary.area > 0 else 0
            }
        )