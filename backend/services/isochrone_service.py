from shapely.geometry import Point, shape
from shapely.ops import unary_union

from repository.graph_repository import GraphRepository
from repository.health_repository import HealthRepository
from services.georoute_client import GeorouteClient

from utils.comuna_util import normalize_to_slug
from utils.geojson_utils import (
    feature_collection,
    geometry_to_feature,
    point_to_feature
)


class IsochroneService:
    """
    Servicio principal de isocronas.

    PostGIS sigue resolviendo comuna, limites y centros; el alcance temporal se
    calcula en el motor Rust georoute con perfil peatonal.
    """

    def __init__(self):

        self.graph_repository = GraphRepository()
        self.health_repository = HealthRepository()
        self.georoute_client = GeorouteClient(profile="foot")

    def _validate_input_coordinates(
        self,
        lon,
        lat
    ):

        if lon < -180 or lon > 180:
            raise ValueError(
                "Longitud invalida"
            )

        if lat < -90 or lat > 90:
            raise ValueError(
                "Latitud invalida"
            )

    def _filter_centers_within_polygon(
        self,
        polygon,
        centers: list[dict]
    ) -> list[dict]:

        reachable = []

        for center in centers:

            lon = center.get("lon") if center.get("lon") is not None else center.get("lng")
            lat = center.get("lat")

            if lon is None or lat is None:
                continue

            point = Point(lon, lat)

            try:

                if polygon.covers(point) or polygon.intersects(point):
                    reachable.append(center)

            except Exception:
                continue

        return reachable

    def _build_georoute_polygon(self, lon: float, lat: float, minutes: float):
        """Consulta georoute y consolida sus features en una geometria Shapely."""
        georoute_result = self.georoute_client.isochrone(
            float(lon),
            float(lat),
            minutes
        )
        polygons = [
            shape(feature["geometry"])
            for feature in georoute_result.get("features", [])
            if feature.get("geometry")
        ]

        if not polygons:
            raise ValueError("georoute no devolvio una isocrona")

        polygon = unary_union(polygons)

        if not polygon.is_valid:
            polygon = polygon.buffer(0)

        if polygon is None or polygon.is_empty:
            raise ValueError("georoute devolvio una isocrona vacia")

        return polygon, georoute_result.get("metadata", {})

    def build_isochrone(
        self,
        comuna=None,
        lat=None,
        lon=None,
        minutes: float = 15,
        include_centers: bool = True
    ):

        self._validate_input_coordinates(
            lon,
            lat
        )

        if comuna is None:
            raw = self.graph_repository.find_comuna_by_coords(
                lat,
                lon
            )
            if raw is None:
                raise ValueError(
                    "No se pudo determinar la comuna "
                    "desde las coordenadas"
                )
            comuna = raw

        comuna_slug = normalize_to_slug(comuna)

        try:
            polygon, georoute_metadata = self._build_georoute_polygon(
                lon,
                lat,
                minutes
            )
        except Exception as exc:
            raise ValueError(f"Error consultando georoute: {exc}") from exc

        boundary = self.graph_repository.load_boundary_polygon(
            comuna_slug
        )
        if boundary is not None:
            polygon = polygon.intersection(boundary)
            if not polygon.is_valid:
                polygon = polygon.buffer(0)

        if polygon is None or polygon.is_empty:
            raise ValueError("georoute devolvio una isocrona vacia")

        reachable_nodes = georoute_metadata.get(
            "reachable_nodes",
            georoute_metadata.get("nodes")
        )
        reachable_edges = georoute_metadata.get(
            "reachable_edges",
            georoute_metadata.get("edges")
        )

        isochrone_properties = {
            "kind": "isochrone",
            "comuna": comuna_slug,
            "minutes": minutes,
            "mode": "foot",
            "engine": "georoute"
        }

        if reachable_nodes is not None:
            isochrone_properties["reachable_nodes"] = reachable_nodes

        if reachable_edges is not None:
            isochrone_properties["reachable_edges"] = reachable_edges

        features = [
            geometry_to_feature(
                polygon,
                properties=isochrone_properties
            ),
            point_to_feature(
                lon,
                lat,
                properties={
                    "kind": "origin",
                    "comuna": comuna_slug,
                    "mode": "foot",
                    "engine": "georoute"
                }
            )
        ]

        if include_centers:

            try:
                all_centers = self.health_repository.load_centers()
            except Exception:
                all_centers = []

            if not all_centers:
                all_centers = self.gtfs_repo.get_centers_by_comuna(comuna_slug)

            reachable_centers = (
                self._filter_centers_within_polygon(
                    polygon,
                    all_centers
                )
            )

            for center in reachable_centers:

                center_lon = center.get("lon") if center.get("lon") is not None else center.get("lng")
                center_lat = center.get("lat")

                if center_lon is None or center_lat is None:
                    continue

                features.append(
                    point_to_feature(
                        center_lon,
                        center_lat,
                        properties={
                            "kind": "health_center",
                            "name": center.get("name") or center.get("nombre") or "Centro de Salud",
                            "comuna": center.get("comuna") or comuna_slug,
                            "engine": "georoute"
                        }
                    )
                )

        metadata = {
            "comuna": comuna_slug,
            "minutes": minutes,
            "engine": "georoute",
            "profile": "foot"
        }

        if reachable_nodes is not None:
            metadata["reachable_nodes"] = reachable_nodes

        return feature_collection(
            features,
            metadata=metadata
        )
