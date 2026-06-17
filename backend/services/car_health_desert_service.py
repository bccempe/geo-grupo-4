import hashlib
import random

import networkx as nx
from shapely.ops import unary_union

from repository.graph_repository import GraphRepository
from repository.health_repository import HealthRepository
from utils.comuna_util import normalize_to_slug
from utils.geojson_utils import (
    build_isochrone_polygon_from_graph,
    geometry_to_feature,
    feature_collection
)


CAR_SPEED_MIN_KMH = 15.0
CAR_SPEED_MAX_KMH = 45.0

INTERSECTION_DEGREE_THRESHOLD = 3

INTERSECTION_BASE_PENALTY_MIN = 0.07
INTERSECTION_EXTRA_PER_EDGE_MIN = 0.04
INTERSECTION_MAX_PENALTY_MIN = 0.6

CAR_RANDOM_SEED = 1412

class CarHealthDesertService:

    def __init__(self):
        self.graph_repository = GraphRepository()
        self.health_repository = HealthRepository()

    def _stable_seed(self, *parts) -> int:
        payload = "|".join(str(p) for p in parts)
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        return int(digest[:16], 16)

    def _edge_rng(self, comuna_slug, u, v, key=None):
        seed = self._stable_seed(CAR_RANDOM_SEED, comuna_slug, u, v, key)
        return random.Random(seed)

    def _intersection_penalty_min(self, graph, node_id, rng):
        """
        Penaliza la entrada a una intersección.
        - Si el nodo tiene grado >= 3, se considera intersección.
        - Si tiene muchas conexiones, agrega penalización extra.
        """
        if graph is None or not graph.has_node(node_id):
            return 0.0

        degree = graph.degree(node_id)

        if degree < INTERSECTION_DEGREE_THRESHOLD:
            return 0.0

        extra_connections = degree - INTERSECTION_DEGREE_THRESHOLD

        penalty = (
            INTERSECTION_BASE_PENALTY_MIN
            + (extra_connections * INTERSECTION_EXTRA_PER_EDGE_MIN)
            + rng.uniform(0.0, 0.03)
        )

        return min(penalty, INTERSECTION_MAX_PENALTY_MIN)

    def _compute_car_edge_time_min(self, length_m, comuna_slug, u, v, graph, key=None):
        """
        Calcula el tiempo en minutos para una arista de automóvil.

        - Usa una velocidad aleatoria determinística entre 10 y 50 km/h.
        - Agrega penalización por intersección en el nodo destino.
        - Si la intersección tiene muchas conexiones, la penalización sube.
        """
        if length_m is None or length_m <= 0:
            return None

        rng = self._edge_rng(comuna_slug, u, v, key)

        speed_kmh = rng.uniform(CAR_SPEED_MIN_KMH, CAR_SPEED_MAX_KMH)
        drive_time_min = (length_m / 1000.0) / speed_kmh * 60.0

        if drive_time_min <= 0:
            return None

        intersection_penalty_min = self._intersection_penalty_min(
            graph,
            v,
            rng
        )

        return drive_time_min + intersection_penalty_min

    def _prepare_car_graph(self, graph, comuna_slug):
        """
        Crea una copia del grafo OSM y le asigna un peso car_time
        a cada arista.
        """
        car_graph = graph.copy()

        updated_edges = 0
        skipped_edges = 0

        for u, v, key, data in car_graph.edges(keys=True, data=True):
            length = data.get("length")
            if length is None:
                skipped_edges += 1
                continue

            car_time = self._compute_car_edge_time_min(
                length_m=float(length),
                comuna_slug=comuna_slug,
                u=u,
                v=v,
                graph=car_graph,
                key=key
            )

            if car_time is None:
                skipped_edges += 1
                continue

            data["car_time"] = car_time
            updated_edges += 1

        print("===================================")
        print("[CarHealthDesert] GRAFO AUTOMOVIL")
        print("===================================")
        print("nodos:", car_graph.number_of_nodes())
        print("edges:", car_graph.number_of_edges())
        print("edges con car_time:", updated_edges)
        print("edges omitidos:", skipped_edges)
        print("===================================")

        return car_graph

    def _validate_graph(self, graph):
        missing_time = 0

        for _, _, edge_data in graph.edges(data=True):
            if "length" not in edge_data:
                missing_time += 1

        if missing_time > 0:
            raise ValueError(
                f"Existen {missing_time} edges sin atributo length"
            )

    def _validate_boundary(self, boundary_polygon):
        if boundary_polygon is None:
            raise ValueError("Boundary polygon es None")

        if boundary_polygon.is_empty:
            raise ValueError("Boundary polygon vacío")

    def _validate_centers(self, centers):
        if not centers:
            raise ValueError("No hay centros")

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
            print("[CarHealthDesert] no se encontró nodo origen")
            return None

        try:
            lengths = nx.single_source_dijkstra_path_length(
                graph,
                source=origin_node,
                cutoff=minutes,
                weight="car_time"
            )
        except Exception as e:
            print("[CarHealthDesert] error Dijkstra:", e)
            return None

        reachable_nodes = list(lengths.keys())

        if not reachable_nodes:
            print("[CarHealthDesert] no hay nodos alcanzables")
            return None

        subgraph = graph.subgraph(
            reachable_nodes
        ).copy()

        try:
            polygon = build_isochrone_polygon_from_graph(
                subgraph
            )
        except Exception as e:
            print("[CarHealthDesert] error construyendo polígono:", e)
            return None

        if polygon is None:
            print("[CarHealthDesert] polígono None")
            return None

        if polygon.is_empty:
            print("[CarHealthDesert] polígono vacío")
            return None

        return polygon

    def build_health_deserts(
        self,
        comuna: str,
        minutes: float = 15
    ):
        comuna_slug = normalize_to_slug(comuna)

        print("===================================")
        print("DESIERTOS DE SALUD - AUTOMOVIL")
        print("comuna:", comuna_slug)
        print("minutes:", minutes)
        print("===================================")

        print("1. CARGANDO CENTROS DE SALUD")

        centers_raw = self.health_repository.load_centers()

        print(f"[DEBUG] Centros cargados desde BD: {len(centers_raw)}")

        if not centers_raw:
            raise ValueError("No se encontraron centros de salud en la base de datos")

        print("[DEBUG] Mostrando primer centro cargado:")
        print(centers_raw[0])

        centers = []
        missing_comuna_count = 0
        invalid_comuna_count = 0
        inferred_comuna_count = 0

        print("[DEBUG] Filtrando centros por comuna...")

        comuna_cache = {}

        for idx, center in enumerate(centers_raw):
            comuna_value = center.get("comuna")

            if not comuna_value:
                missing_comuna_count += 1

                lat = center.get("lat")
                lon = center.get("lon")
                if lon is None:
                    lon = center.get("lng")

                if lat is None or lon is None:
                    print(
                        f"[WARNING] Centro {idx} sin comuna y sin coordenadas válidas. Se omite."
                    )
                    continue

                cache_key = (round(float(lat), 5), round(float(lon), 5))
                inferred = comuna_cache.get(cache_key)

                if inferred is None:
                    try:
                        inferred = self.graph_repository.find_comuna_by_coords(
                            float(lat),
                            float(lon)
                        )
                        comuna_cache[cache_key] = inferred
                        inferred_comuna_count += 1
                    except Exception as e:
                        print(
                            f"[WARNING] No se pudo inferir comuna para centro {idx}: {e}"
                        )
                        print("[WARNING] Centro problemático:", center)
                        continue

                if not inferred:
                    print(
                        f"[WARNING] No se pudo inferir comuna para centro {idx}. Se omite."
                    )
                    print("[WARNING] Centro problemático:", center)
                    continue

                try:
                    center_slug = normalize_to_slug(inferred)
                except Exception as e:
                    invalid_comuna_count += 1
                    print(
                        f"[WARNING] Error normalizando comuna inferida en centro {idx}: {e}"
                    )
                    print("[WARNING] Centro problemático:", center)
                    continue

                if center_slug == comuna_slug:
                    center = dict(center)
                    center["comuna"] = inferred
                    centers.append(center)

                continue

            try:
                center_slug = normalize_to_slug(comuna_value)
            except Exception as e:
                invalid_comuna_count += 1
                print(
                    f"[WARNING] Error normalizando comuna en centro {idx}: {e}"
                )
                print("[WARNING] Centro problemático:", center)
                continue

            if center_slug == comuna_slug:
                centers.append(center)

        print(f"[DEBUG] Centros con comuna faltante: {missing_comuna_count}")
        print(f"[DEBUG] Centros con comuna inferida por coordenadas: {inferred_comuna_count}")
        print(f"[DEBUG] Centros con comuna inválida: {invalid_comuna_count}")
        print(f"[DEBUG] Centros tras filtro por comuna: {len(centers)}")

        if not centers:
            raise ValueError(
                f"No se encontraron centros de salud en {comuna_slug}"
            )

        print(f"   centros encontrados: {len(centers)}")

        print("2. CONSTRUYENDO GRAFO DE CALLES")

        graph = self.graph_repository.load_graph(
            comuna_slug
        )

        self._validate_graph(graph)

        print(f"   nodos: {graph.number_of_nodes()} nodos")
        print(f"   edges: {graph.number_of_edges()} edges")

        car_graph = self._prepare_car_graph(
            graph,
            comuna_slug
        )

        print("3. GENERANDO ISOCRONAS")

        polygons = []

        for idx, center in enumerate(centers):
            if idx % 10 == 0:
                print(f"   progreso: {idx}/{len(centers)}")

            lon = center.get("lon")
            if lon is None:
                lon = center.get("lng")

            lat = center.get("lat")

            if lon is None or lat is None:
                print(
                    f"[WARNING] Centro sin coordenadas válidas: {center.get('name', center.get('nombre', 'sin_nombre'))}"
                )
                print("[WARNING] Centro:", center)
                continue

            try:
                polygon = self._build_single_isochrone(
                    car_graph,
                    lon,
                    lat,
                    minutes
                )

                if polygon is not None:
                    polygons.append(polygon)
                else:
                    print(
                        f"   sin isocrona: {center.get('name', center.get('nombre', 'sin_nombre'))}"
                    )

            except Exception as e:
                print(
                    f"   error en centro {center.get('name', center.get('nombre', 'sin_nombre'))}: {e}"
                )

        print(f"   isocronas generadas: {len(polygons)}")

        if not polygons:
            raise ValueError("No se pudieron generar isocronas")

        print("4. UNIENDO COBERTURA")

        coverage_polygon = unary_union(polygons)

        print(f"   area cobertura: {coverage_polygon.area:.6f}")

        print("5. CARGANDO LIMITE COMUNAL")

        boundary_polygon = self.graph_repository.load_boundary_polygon(
            comuna_slug
        )

        self._validate_boundary(boundary_polygon)

        print(f"   area comuna: {boundary_polygon.area:.6f}")

        print("6. RECORTANDO COBERTURA")

        coverage_polygon = coverage_polygon.intersection(
            boundary_polygon
        )

        print("7. CALCULANDO DESIERTO")

        desert_polygon = boundary_polygon.difference(
            coverage_polygon
        )

        print(f"   area desierto: {desert_polygon.area:.6f}")

        boundary_area = boundary_polygon.area
        desert_percentage = 0

        if boundary_area > 0:
            desert_percentage = (
                desert_polygon.area / boundary_area
            ) * 100

            print(
                f"   porcentaje desierto: {desert_percentage:.1f}%"
            )

        print("8. GENERANDO FEATURES")

        features = []

        features.append(
            geometry_to_feature(
                coverage_polygon,
                properties={
                    "kind": "coverage",
                    "mode": "car",
                    "minutes": minutes,
                    "comuna": comuna_slug
                }
            )
        )

        features.append(
            geometry_to_feature(
                desert_polygon,
                properties={
                    "kind": "health_desert",
                    "mode": "car",
                    "minutes": minutes,
                    "comuna": comuna_slug,
                    "desert_percentage": desert_percentage
                }
            )
        )

        print("===================================")
        print("DESIERTOS COMPLETADOS")
        print("===================================")

        return feature_collection(
            features,
            metadata={
                "comuna": comuna_slug,
                "minutes": minutes,
                "centers_count": len(centers),
                "generated_isochrones": len(polygons),
                "desert_pct": desert_percentage
            }
        )