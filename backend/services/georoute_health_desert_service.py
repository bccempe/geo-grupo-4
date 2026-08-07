"""Desiertos de salud calculados por georoute."""

from shapely.geometry import shape
from shapely.ops import unary_union

from repository.graph_repository import GraphRepository
from repository.health_repository import HealthRepository
from services.georoute_client import GeorouteClient
from utils.comuna_util import normalize_to_slug
from utils.geojson_utils import feature_collection, geometry_to_feature


class GeorouteHealthDesertService:
    """Construye cobertura y desierto usando isócronas del perfil indicado."""

    def __init__(self, profile: str):
        self.client = GeorouteClient(profile)
        self.profile = profile
        self.graph_repository = GraphRepository()
        self.health_repository = HealthRepository()

    def build_health_deserts(self, comuna: str, minutes: float = 15):
        comuna_slug = normalize_to_slug(comuna)
        boundary = self.graph_repository.load_boundary_polygon(comuna_slug)
        if boundary is None or boundary.is_empty:
            raise ValueError(f"Límite comunal inválido para {comuna_slug}")

        centers = self._centers_in_comuna(comuna_slug)
        if not centers:
            raise ValueError(f"No se encontraron centros de salud en {comuna_slug}")

        polygons = []
        failed_centers = []
        for center in centers:
            try:
                result = self.client.isochrone(center["lon"], center["lat"], minutes)
                polygons.extend(
                    shape(feature["geometry"])
                    for feature in result.get("features", [])
                    if feature.get("geometry")
                )
            except ValueError:
                failed_centers.append(str(center.get("id", center.get("name", "unknown"))))

        if not polygons:
            raise ValueError("georoute no generó isócronas para los centros de salud")

        coverage = unary_union(polygons).intersection(boundary)
        if coverage.is_empty:
            raise ValueError("La cobertura georoute quedó vacía dentro de la comuna")

        desert = boundary.difference(coverage)
        desert_pct = (desert.area / boundary.area * 100) if boundary.area > 0 else 0
        features = [
            geometry_to_feature(coverage, {
                "kind": "coverage", "mode": self.profile, "comuna": comuna_slug,
                "minutes": minutes, "engine": "georoute",
            }),
            geometry_to_feature(desert, {
                "kind": "health_desert", "mode": self.profile, "comuna": comuna_slug,
                "minutes": minutes, "engine": "georoute",
                "desert_percentage": desert_pct,
            }),
        ]
        return feature_collection(features, metadata={
            "comuna": comuna_slug,
            "minutes": minutes,
            "engine": "georoute",
            "profile": self.profile,
            "centers_count": len(centers),
            "generated_isochrones": len(polygons),
            "desert_pct": desert_pct,
            "failed_centers": failed_centers,
        })

    def _centers_in_comuna(self, comuna_slug: str) -> list[dict]:
        centers = []
        for center in self.health_repository.load_centers():
            comuna = center.get("comuna")
            if comuna:
                center_comuna_slug = normalize_to_slug(comuna)
            else:
                center_comuna = self.graph_repository.find_comuna_by_coords(
                    center["lat"],
                    center["lon"],
                )
                center_comuna_slug = (
                    normalize_to_slug(center_comuna)
                    if center_comuna else None
                )
            if center_comuna_slug == comuna_slug:
                centers.append(center)
        return centers
