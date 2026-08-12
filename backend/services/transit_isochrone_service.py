from shapely.geometry import Point, shape
from shapely.ops import unary_union

from repository.gtfs_repository import GTFSRepository
from repository.graph_repository import GraphRepository
from services.georoute_client import GeorouteTransitClient
from utils.comuna_util import normalize_to_slug
from utils.geojson_utils import feature_collection, geometry_to_feature, point_to_feature


class TransitIsochroneService:
    """Genera isocronas multimodales con georoute y RAPTOR sobre GTFS."""

    def __init__(self):
        self.gtfs_repo = GTFSRepository()
        self.graph_repo = GraphRepository()
        self.georoute_client = GeorouteTransitClient()

    def build_isochrone(
        self,
        comuna=None,
        lat=None,
        lon=None,
        minutes=30,
        departure_hour=None,
        include_centers=False,
    ):
        if lat is None or lon is None:
            raise ValueError("Se requieren latitud y longitud")
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            raise ValueError("Coordenadas fuera de rango")

        if comuna is None:
            comuna = self.graph_repo.find_comuna_by_coords(lat, lon)
            if comuna is None:
                raise ValueError("No se pudo determinar la comuna desde las coordenadas")

        comuna_slug = normalize_to_slug(comuna)
        result = self.georoute_client.isochrone(
            lon=lon,
            lat=lat,
            minutes=minutes,
            departure_hour=departure_hour,
        )
        polygons = [
            shape(feature["geometry"])
            for feature in result.get("features", [])
            if feature.get("geometry")
        ]
        if not polygons:
            raise ValueError("georoute no devolvio una isocrona de transporte")

        polygon = unary_union(polygons)
        boundary = self.graph_repo.load_boundary_polygon(comuna_slug)
        if boundary is not None and not boundary.is_empty:
            polygon = polygon.intersection(boundary)
        if not polygon.is_valid:
            polygon = polygon.buffer(0)
        if polygon.is_empty:
            raise ValueError("La isocrona de transporte quedo vacia dentro de la comuna")

        georoute_metadata = result.get("metadata", {})
        effective_hour = georoute_metadata.get("departure_hour", departure_hour)
        properties = {
            "kind": "isochrone",
            "mode": "transit",
            "comuna": comuna_slug,
            "minutes": minutes,
            "departure_hour": effective_hour,
            "engine": "georoute",
            "profile": "transit",
            "algorithm": "raptor_multimodal",
        }
        features = [
            geometry_to_feature(polygon, properties=properties),
            point_to_feature(
                lon,
                lat,
                properties={
                    "kind": "origin",
                    "comuna": comuna_slug,
                    "mode": "transit",
                    "engine": "georoute",
                    "profile": "transit",
                },
            ),
        ]

        if include_centers:
            for center in self.gtfs_repo.get_centers_by_comuna(comuna_slug):
                center_lon = center.get("lng", center.get("lon"))
                center_lat = center.get("lat")
                if center_lon is None or center_lat is None:
                    continue
                if polygon.covers(Point(center_lon, center_lat)):
                    features.append(
                        point_to_feature(
                            center_lon,
                            center_lat,
                            properties={
                                "kind": "health_center",
                                "name": center.get("name")
                                or center.get("nombre")
                                or "Centro de Salud",
                                "comuna": comuna_slug,
                                "engine": "georoute",
                                "profile": "transit",
                            },
                        )
                    )

        return feature_collection(
            features,
            metadata={
                "comuna": comuna_slug,
                "minutes": minutes,
                "departure_hour": effective_hour,
                "engine": "georoute",
                "profile": "transit",
                "algorithm": "raptor_multimodal",
                "service_date": georoute_metadata.get("service_date"),
            },
        )
