"""Desiertos de salud calculados por georoute."""

import time

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
        t_total = time.perf_counter()
        comuna_slug = normalize_to_slug(comuna)

        t0 = time.perf_counter()
        boundary = self.graph_repository.load_boundary_polygon(comuna_slug)
        t_boundary = time.perf_counter() - t0
        if boundary is None or boundary.is_empty:
            raise ValueError(f"Límite comunal inválido para {comuna_slug}")

        t0 = time.perf_counter()
        centers = self._centers_in_comuna(comuna_slug, boundary)
        t_centers = time.perf_counter() - t0
        if not centers:
            raise ValueError(f"No se encontraron centros de salud en {comuna_slug}")

        t0 = time.perf_counter()
        polygons = []
        failed_centers = []
        for i, center in enumerate(centers):
            t_iso = time.perf_counter()
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
            if (i + 1) % 5 == 0 or i == 0 or i == len(centers) - 1:
                print(
                    f"[GeorouteHealthDesert] {comuna_slug} "
                    f"isocronas {i + 1}/{len(centers)} "
                    f"({time.perf_counter() - t_iso:.2f}s ultima)"
                )
        t_isochrones = time.perf_counter() - t0

        if not polygons:
            raise ValueError("georoute no generó isócronas para los centros de salud")

        t0 = time.perf_counter()
        coverage = unary_union(polygons).intersection(boundary)
        t_union = time.perf_counter() - t0
        if coverage.is_empty:
            raise ValueError("La cobertura georoute quedó vacía dentro de la comuna")

        t0 = time.perf_counter()
        desert = boundary.difference(coverage)
        desert_pct = (desert.area / boundary.area * 100) if boundary.area > 0 else 0
        t_desert = time.perf_counter() - t0

        print(
            f"[GeorouteHealthDesert] {comuna_slug} completado "
            f"en {(time.perf_counter() - t_total):.1f}s | "
            f"boundary={t_boundary:.1f}s centers={t_centers:.1f}s "
            f"isochrones={t_isochrones:.1f}s union={t_union:.1f}s "
            f"desert={t_desert:.1f}s"
        )

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
