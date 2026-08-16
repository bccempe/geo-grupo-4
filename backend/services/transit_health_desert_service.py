from shapely.geometry import shape
from shapely.ops import unary_union

from repository.gtfs_repository import GTFSRepository
from repository.graph_repository import GraphRepository
from services.georoute_client import GeorouteTransitClient
from utils.comuna_util import normalize_to_slug
from utils.geojson_utils import feature_collection, geometry_to_feature


class TransitHealthDesertService:
    """Calcula cobertura de transporte con georoute multimodal y RAPTOR."""

    def __init__(self):
        self.gtfs_repo = GTFSRepository()
        self.graph_repo = GraphRepository()
        self.georoute_client = GeorouteTransitClient()

    def build_health_deserts(
        self,
        comuna,
        minutes=30,
        departure_hour=None,
    ):
        comuna_slug = normalize_to_slug(comuna)
        centers = self.gtfs_repo.get_centers_by_comuna(comuna_slug)
        if not centers:
            raise ValueError(f"No se encontraron centros de salud en {comuna_slug}")

        valid_centers = []
        origins = []
        for center in centers:
            lon = center.get("lng", center.get("lon"))
            lat = center.get("lat")
            if lon is None or lat is None:
                continue
            valid_centers.append(center)
            origins.append([float(lon), float(lat)])
        if not origins:
            raise ValueError("No hay centros con coordenadas validas")

        result = self.georoute_client.isochrones(
            origins=origins,
            minutes=minutes,
            departure_hour=departure_hour,
        )
        polygons = [
            shape(feature["geometry"])
            for feature in result.get("features", [])
            if feature.get("geometry")
        ]
        if not polygons:
            raise ValueError("georoute no genero isocronas de transporte")

        boundary = self.graph_repo.load_boundary_polygon(comuna_slug)
        if boundary is None or boundary.is_empty:
            raise ValueError(f"Limite comunal invalido para {comuna_slug}")

        coverage = unary_union(polygons).intersection(boundary)
        if not coverage.is_valid:
            coverage = coverage.buffer(0)
        if coverage.is_empty:
            raise ValueError("La cobertura de transporte quedo vacia")

        desert = boundary.difference(coverage)
        desert_pct = (desert.area / boundary.area * 100) if boundary.area > 0 else 0
        georoute_metadata = result.get("metadata", {})
        effective_hour = georoute_metadata.get("departure_hour", departure_hour)
        common = {
            "mode": "transit",
            "comuna": comuna_slug,
            "minutes": minutes,
            "departure_hour": effective_hour,
            "engine": "georoute",
            "profile": "transit",
            "algorithm": "raptor_multimodal",
        }
        features = [
            geometry_to_feature(coverage, {"kind": "coverage", **common}),
            geometry_to_feature(
                desert,
                {
                    "kind": "health_desert",
                    "desert_percentage": desert_pct,
                    **common,
                },
            ),
        ]

        return feature_collection(
            features,
            center_list=valid_centers,
            metadata={
                **common,
                "service_date": georoute_metadata.get("service_date"),
                "centers_count": len(valid_centers),
                "generated_isochrones": len(polygons),
                "failed_centers": georoute_metadata.get("failed_origins", []),
                "coverage_area": coverage.area,
                "desert_area": desert.area,
                "desert_pct": desert_pct,
            },
        )
