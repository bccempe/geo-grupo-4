"""Desiertos de salud calculados por georoute."""

from time import perf_counter

from shapely.geometry import Point, shape
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
        total_started = perf_counter()
        comuna_slug = normalize_to_slug(comuna)

        boundary_started = perf_counter()
        boundary = self.graph_repository.load_boundary_polygon(comuna_slug)
        if boundary is None or boundary.is_empty:
            raise ValueError(f"Límite comunal inválido para {comuna_slug}")
        boundary_seconds = perf_counter() - boundary_started

        centers_started = perf_counter()
        centers = self._centers_in_comuna(comuna_slug, boundary)
        if not centers:
            raise ValueError(f"No se encontraron centros de salud en {comuna_slug}")
        centers_seconds = perf_counter() - centers_started

        isochrones_started = perf_counter()
        polygons = []
        failed_centers = []
        for i, center in enumerate(centers):
            try:
                result = self.client.isochrone(center["lon"], center["lat"], minutes)
                polygons.extend(
                    shape(feature["geometry"])
                    for feature in result.get("features", [])
                    if feature.get("geometry")
                )
            except Exception:
                failed_centers.append(
                    str(center.get("id", center.get("name", "unknown")))
                )
        isochrones_seconds = perf_counter() - isochrones_started

        if not polygons:
            raise ValueError("georoute no generó isócronas para los centros de salud")

        geometry_started = perf_counter()
        coverage = unary_union(polygons).intersection(boundary)
        if coverage.is_empty:
            raise ValueError("La cobertura georoute quedó vacía dentro de la comuna")

        desert = boundary.difference(coverage)
        desert_pct = (desert.area / boundary.area * 100) if boundary.area > 0 else 0
        geometry_seconds = perf_counter() - geometry_started
        total_seconds = perf_counter() - total_started
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
        return feature_collection(features, center_list=centers, metadata={
            "comuna": comuna_slug,
            "minutes": minutes,
            "engine": "georoute",
            "profile": self.profile,
            "centers_count": len(centers),
            "generated_isochrones": len(polygons),
            "desert_pct": desert_pct,
            "failed_centers": failed_centers,
            "timings": {
                "boundary_seconds": round(boundary_seconds, 6),
                "centers_seconds": round(centers_seconds, 6),
                "isochrones_seconds": round(isochrones_seconds, 6),
                "geometry_seconds": round(geometry_seconds, 6),
                "total_seconds": round(total_seconds, 6),
            },
        })

    def _centers_in_comuna(self, comuna_slug: str, boundary) -> list[dict]:
        """Filtra centros por comuna usando el polígono de límite en memoria."""
        centers = []
        for center in self.health_repository.load_centers():
            comuna = center.get("comuna")
            if comuna:
                if normalize_to_slug(comuna) == comuna_slug:
                    centers.append(center)
                continue

            lon = center.get("lon", center.get("lng"))
            lat = center.get("lat")
            if lon is None or lat is None:
                continue

            point = Point(float(lon), float(lat))
            try:
                if boundary.intersects(point):
                    centers.append(center)
            except Exception:
                continue
        return centers
