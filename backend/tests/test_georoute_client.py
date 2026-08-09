import json
import pytest
from unittest.mock import patch, MagicMock
from shapely.geometry import Polygon


MOCK_ISOCHRONE_GEOJSON = {
    "type": "FeatureCollection",
    "features": [{
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
    }]
}


class TestGeorouteClient:
    """Tests para el cliente HTTP de georoute."""

    def test_init_default_url_foot(self):
        from services.georoute_client import GeorouteClient
        client = GeorouteClient(profile="foot")
        assert "georoute-foot" in client.base_url

    def test_init_default_url_car(self):
        from services.georoute_client import GeorouteClient
        client = GeorouteClient(profile="car")
        assert "georoute-car" in client.base_url

    def test_init_invalid_profile_raises(self):
        from services.georoute_client import GeorouteClient
        with pytest.raises(ValueError, match="foot.*car"):
            GeorouteClient(profile="bike")

    @patch("services.georoute_client.requests.request")
    def test_isochrone_returns_geojson(self, mock_request):
        from services.georoute_client import GeorouteClient

        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.json.return_value = MOCK_ISOCHRONE_GEOJSON
        mock_request.return_value = mock_response

        client = GeorouteClient(profile="foot")
        result = client.isochrone(lon=-70.6, lat=-33.5, minutes=30)

        assert result["type"] == "FeatureCollection"
        assert len(result["features"]) == 1
        mock_request.assert_called_once()

    @patch("services.georoute_client.requests.request")
    def test_isochrone_connection_error_raises_valueerror(self, mock_request):
        from services.georoute_client import GeorouteClient
        import requests as real_requests

        mock_request.side_effect = real_requests.ConnectionError("refused")

        client = GeorouteClient(profile="foot")
        with pytest.raises(ValueError, match="No se pudo conectar"):
            client.isochrone(lon=-70.6, lat=-33.5, minutes=30)

    @patch("services.georoute_client.requests.request")
    def test_isochrone_bad_status_raises_valueerror(self, mock_request):
        from services.georoute_client import GeorouteClient

        mock_response = MagicMock()
        mock_response.ok = False
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_request.return_value = mock_response

        client = GeorouteClient(profile="foot")
        with pytest.raises(ValueError, match="500"):
            client.isochrone(lon=-70.6, lat=-33.5, minutes=30)

    @patch("services.georoute_client.requests.request")
    def test_access_endpoint(self, mock_request):
        from services.georoute_client import GeorouteClient

        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.json.return_value = [{"score": 0.85}]
        mock_request.return_value = mock_response

        client = GeorouteClient(profile="foot")
        result = client.access(
            demand=[[-70.6, -33.5]],
            supply=[[-70.5, -33.5]],
            minutes=30,
        )

        assert isinstance(result, list)
        assert result[0]["score"] == 0.85

    @patch("services.georoute_client.requests.request")
    def test_non_json_response_raises(self, mock_request):
        from services.georoute_client import GeorouteClient

        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.json.side_effect = ValueError("not json")
        mock_request.return_value = mock_response

        client = GeorouteClient(profile="foot")
        with pytest.raises(ValueError, match="no es JSON"):
            client.isochrone(lon=-70.6, lat=-33.5, minutes=30)


class TestGeorouteClientConfig:
    """Tests de configuracion via variables de entorno."""

    def test_custom_url_from_env(self, monkeypatch):
        from services.georoute_client import GeorouteClient
        monkeypatch.setenv("GEOROUTE_FOOT_URL", "http://custom:9090")

        client = GeorouteClient(profile="foot")
        assert client.base_url == "http://custom:9090"

    def test_custom_timeout_from_env(self, monkeypatch):
        from services.georoute_client import GeorouteClient
        monkeypatch.setenv("GEOROUTE_TIMEOUT_SECONDS", "120")

        client = GeorouteClient(profile="foot")
        assert client.timeout == 120.0
