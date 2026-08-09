import pytest
from unittest.mock import patch, MagicMock
from shapely.geometry import Point, Polygon


MOCK_CENTERS = [
    {"id": 1, "nombre": "CESFAM A", "lat": -33.575, "lng": -70.575, "nom_comuna": "puente_alto"},
    {"id": 2, "nombre": "CESFAM B", "lat": -33.580, "lng": -70.580, "nom_comuna": "puente_alto"},
]

MOCK_BOUNDARY = Polygon([
    (-70.65, -33.65),
    (-70.65, -33.45),
    (-70.45, -33.45),
    (-70.45, -33.65),
    (-70.65, -33.65),
])

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
            "properties": {"kind": "coverage"}
        }
    ]
}


class TestHealthDesertService:
    """Tests para HealthDesertService (legacy + georoute paths)."""

    @patch("services.health_desert_service.GraphRepository")
    @patch("services.health_desert_service.GTFSRepository")
    def test_legacy_with_empty_centers_raises(self, mock_gtfs, mock_graph):
        from services.health_desert_service import HealthDesertService

        mock_graph_instance = MagicMock()
        mock_graph.return_value = mock_graph_instance

        mock_gtfs_instance = MagicMock()
        mock_gtfs_instance.get_centers_by_comuna.return_value = []
        mock_gtfs.return_value = mock_gtfs_instance

        svc = HealthDesertService()
        with pytest.raises(ValueError, match="No hay centros"):
            svc.build_health_deserts_legacy(comuna="puente_alto", minutes=30)

    @patch("services.health_desert_service.GraphRepository")
    @patch("services.health_desert_service.GTFSRepository")
    def test_legacy_validates_graph_without_time(self, mock_gtfs, mock_graph):
        from services.health_desert_service import HealthDesertService
        import networkx as nx

        graph = nx.MultiDiGraph()
        graph.add_node(0, x=-70.575, y=-33.575)
        graph.add_node(1, x=-70.580, y=-33.580)
        graph.add_edge(0, 1, length=100)

        mock_graph_instance = MagicMock()
        mock_graph_instance.load_graph.return_value = graph
        mock_graph.return_value = mock_graph_instance

        mock_gtfs_instance = MagicMock()
        mock_gtfs_instance.get_centers_by_comuna.return_value = MOCK_CENTERS
        mock_gtfs.return_value = mock_gtfs_instance

        svc = HealthDesertService()
        with pytest.raises(ValueError, match="sin atributo time"):
            svc.build_health_deserts_legacy(comuna="puente_alto", minutes=30)

    @patch("services.health_desert_service.GraphRepository")
    @patch("services.health_desert_service.GTFSRepository")
    @patch("services.health_desert_service.build_isochrone_polygon_from_graph")
    def test_legacy_with_valid_graph(self, mock_build_polygon, mock_gtfs, mock_graph):
        from services.health_desert_service import HealthDesertService
        import networkx as nx

        graph = nx.MultiDiGraph()
        graph.add_node(0, x=-70.575, y=-33.575)
        graph.add_node(1, x=-70.580, y=-33.580)
        graph.add_edge(0, 1, length=100, time=72)

        mock_graph_instance = MagicMock()
        mock_graph_instance.load_graph.return_value = graph
        mock_graph_instance.load_boundary_polygon.return_value = MOCK_BOUNDARY
        mock_graph.return_value = mock_graph_instance

        mock_gtfs_instance = MagicMock()
        mock_gtfs_instance.get_centers_by_comuna.return_value = MOCK_CENTERS
        mock_gtfs.return_value = mock_gtfs_instance

        mock_build_polygon.return_value = Polygon([
            (-70.6, -33.6), (-70.6, -33.5),
            (-70.5, -33.5), (-70.5, -33.6),
        ])

        svc = HealthDesertService()
        result = svc.build_health_deserts_legacy(comuna="puente_alto", minutes=30)

        assert result["type"] == "FeatureCollection"
        kinds = {f["properties"]["kind"] for f in result["features"]}
        assert "coverage" in kinds


class TestHealthDesertServiceLive:
    """Tests para el path live (georoute)."""

    @patch("services.health_desert_service.GeorouteHealthDesertService")
    def test_build_health_deserts_delegates_to_georoute(self, mock_georoute_svc):
        from services.health_desert_service import HealthDesertService

        mock_instance = MagicMock()
        mock_instance.build_health_deserts.return_value = {
            "type": "FeatureCollection",
            "features": [],
        }
        mock_georoute_svc.return_value = mock_instance

        svc = HealthDesertService()
        result = svc.build_health_deserts(comuna="puente_alto", minutes=30)

        assert result["type"] == "FeatureCollection"
        mock_instance.build_health_deserts.assert_called_once_with(
            comuna="puente_alto", minutes=30
        )
