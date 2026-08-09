import json
import pytest
from unittest.mock import patch, MagicMock
from shapely.geometry import Point, Polygon


MOCK_GEOJSON = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-70.6, -33.6],
                    [-70.6, -33.5],
                    [-70.5, -33.5],
                    [-70.5, -33.6],
                    [-70.6, -33.6],
                ]]
            },
            "properties": {}
        }
    ]
}


MOCK_CENTERS = [
    {"id": 1, "nombre": "CESFAM Test", "lat": -33.575, "lng": -70.575, "nom_comuna": "puente_alto"},
]


MOCK_BOUNDARY = Polygon([
    (-70.65, -33.65),
    (-70.65, -33.45),
    (-70.45, -33.45),
    (-70.45, -33.65),
    (-70.65, -33.65),
])


@pytest.fixture
def mock_graph_repo():
    with patch("services.isochrone_service.GraphRepository") as mock:
        instance = MagicMock()
        instance.load_boundary_polygon.return_value = MOCK_BOUNDARY
        mock.return_value = instance
        yield instance


@pytest.fixture
def mock_gtfs_repo():
    with patch("services.isochrone_service.GTFSRepository") as mock:
        instance = MagicMock()
        instance.get_centers_by_comuna.return_value = MOCK_CENTERS
        mock.return_value = instance
        yield instance


@pytest.fixture
def mock_health_repo():
    with patch("services.isochrone_service.HealthRepository") as mock:
        instance = MagicMock()
        mock.return_value = instance
        yield instance


class TestIsochroneService:
    """Tests para el servicio de isocronas."""

    def test_init_sets_repositories(self):
        from services.isochrone_service import IsochroneService

        svc = IsochroneService()
        assert svc.graph_repository is not None
        assert svc.gtfs_repo is not None
        assert svc.health_repository is not None
        assert svc.georoute_client is not None

    def test_validate_valid_coordinates(self):
        from services.isochrone_service import IsochroneService

        svc = IsochroneService()
        svc._validate_input_coordinates(lon=-70.5, lat=-33.5)

    def test_validate_invalid_lon_raises(self):
        from services.isochrone_service import IsochroneService

        svc = IsochroneService()
        with pytest.raises(ValueError):
            svc._validate_input_coordinates(lon=-200, lat=-33.5)

    def test_validate_invalid_lat_raises(self):
        from services.isochrone_service import IsochroneService

        svc = IsochroneService()
        with pytest.raises(ValueError):
            svc._validate_input_coordinates(lon=-70.5, lat=100)

    @patch("services.georoute_client.requests.request")
    @patch("services.isochrone_service.GraphRepository")
    @patch("services.isochrone_service.GTFSRepository")
    @patch("services.isochrone_service.HealthRepository")
    def test_build_isochrone_returns_feature_collection(
        self, mock_health, mock_gtfs, mock_graph, mock_request
    ):
        from services.isochrone_service import IsochroneService

        mock_boundary = Polygon([
            (-70.65, -33.65),
            (-70.65, -33.45),
            (-70.45, -33.45),
            (-70.45, -33.65),
            (-70.65, -33.65),
        ])

        graph_instance = MagicMock()
        graph_instance.load_boundary_polygon.return_value = mock_boundary
        mock_graph.return_value = graph_instance

        gtfs_instance = MagicMock()
        gtfs_instance.get_centers_by_comuna.return_value = MOCK_CENTERS
        mock_gtfs.return_value = gtfs_instance

        health_instance = MagicMock()
        health_instance.load_centers.return_value = MOCK_CENTERS
        mock_health.return_value = health_instance

        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.json.return_value = MOCK_GEOJSON
        mock_request.return_value = mock_response

        svc = IsochroneService()
        result = svc.build_isochrone(
            lon=-70.575,
            lat=-33.575,
            minutes=30,
            comuna="puente_alto",
            include_centers=True,
        )

        assert result["type"] == "FeatureCollection"
        assert "features" in result
        assert "metadata" in result

    @patch("services.georoute_client.requests.request")
    @patch("services.isochrone_service.GraphRepository")
    @patch("services.isochrone_service.GTFSRepository")
    @patch("services.isochrone_service.HealthRepository")
    def test_build_isochrone_georoute_failure_raises(
        self, mock_health, mock_gtfs, mock_graph, mock_request
    ):
        from services.isochrone_service import IsochroneService
        import requests as real_requests

        mock_boundary = Polygon([
            (-70.65, -33.65),
            (-70.65, -33.45),
            (-70.45, -33.45),
            (-70.45, -33.65),
            (-70.65, -33.65),
        ])

        graph_instance = MagicMock()
        graph_instance.load_boundary_polygon.return_value = mock_boundary
        mock_graph.return_value = graph_instance

        gtfs_instance = MagicMock()
        gtfs_instance.get_centers_by_comuna.return_value = MOCK_CENTERS
        mock_gtfs.return_value = gtfs_instance

        health_instance = MagicMock()
        health_instance.load_centers.return_value = MOCK_CENTERS
        mock_health.return_value = health_instance

        mock_request.side_effect = real_requests.ConnectionError("refused")

        svc = IsochroneService()
        with pytest.raises(ValueError, match="No se pudo conectar"):
            svc.build_isochrone(
                lon=-70.575,
                lat=-33.575,
                minutes=30,
                comuna="puente_alto",
            )
