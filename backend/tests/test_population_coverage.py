import pytest
from unittest.mock import patch, MagicMock
from shapely.geometry import Point, Polygon


MOCK_CENTERS = [
    {"id": 1, "nombre": "CESFAM Test", "lat": -33.575, "lng": -70.575, "nom_comuna": "puente_alto"},
]

MOCK_BLOCK_GEOM = Polygon([
    (-70.577, -33.577),
    (-70.577, -33.573),
    (-70.573, -33.573),
    (-70.573, -33.577),
    (-70.577, -33.577),
])

MOCK_COVERAGE_POLYGON = Polygon([
    (-70.6, -33.6),
    (-70.6, -33.5),
    (-70.5, -33.5),
    (-70.5, -33.6),
    (-70.6, -33.6),
])

MOCK_BLOCKS = [
    {
        "block_id": "block_001",
        "comuna": "puente_alto",
        "geometry": MOCK_BLOCK_GEOM,
        "population": 100,
        "elderly_population": 30,
    },
    {
        "block_id": "block_002",
        "comuna": "puente_alto",
        "geometry": MOCK_BLOCK_GEOM,
        "population": 50,
        "elderly_population": 10,
    },
]


class TestPopulationCoverageService:
    """Tests para PopulationCoverageService."""

    def test_fix_geometry_valid(self):
        from services.population_coverage_service import PopulationCoverageService
        svc = PopulationCoverageService()
        result = svc._fix_geometry(MOCK_BLOCK_GEOM)
        assert result is not None
        assert not result.is_empty

    def test_fix_geometry_none_returns_none(self):
        from services.population_coverage_service import PopulationCoverageService
        svc = PopulationCoverageService()
        assert svc._fix_geometry(None) is None

    def test_build_block_features_empty_blocks(self):
        from services.population_coverage_service import PopulationCoverageService

        svc = PopulationCoverageService()
        features, summary = svc._build_block_features(
            blocks=[],
            coverage_polygon=MOCK_COVERAGE_POLYGON,
            comuna_slug="puente_alto",
        )

        assert len(features) == 0
        assert summary["total_population"] == 0
        assert summary["covered_population"] == 0

    def test_build_block_features_coverage(self):
        from services.population_coverage_service import PopulationCoverageService

        svc = PopulationCoverageService()
        features, summary = svc._build_block_features(
            blocks=MOCK_BLOCKS,
            coverage_polygon=MOCK_COVERAGE_POLYGON,
            comuna_slug="puente_alto",
        )

        assert len(features) == 2
        assert summary["total_population"] == 150
        for f in features:
            assert f["properties"]["kind"] == "census_block"
            assert "coverage_ratio" in f["properties"]
            assert "status" in f["properties"]

    def test_build_block_features_no_coverage(self):
        from services.population_coverage_service import PopulationCoverageService

        far_polygon = Polygon([
            (-71.0, -34.0),
            (-71.0, -33.9),
            (-70.9, -33.9),
            (-70.9, -34.0),
            (-71.0, -34.0),
        ])

        svc = PopulationCoverageService()
        features, summary = svc._build_block_features(
            blocks=MOCK_BLOCKS,
            coverage_polygon=far_polygon,
            comuna_slug="puente_alto",
        )

        assert summary["covered_population"] == 0
        for f in features:
            assert f["properties"]["coverage_ratio"] == 0.0
            assert f["properties"]["status"] == "uncovered"

    @patch("services.population_coverage_service.GeorouteHealthDesertService")
    @patch("services.population_coverage_service.GTFSRepository")
    @patch("services.population_coverage_service.CensusRepository")
    def test_build_population_coverage_flow(
        self, mock_census, mock_gtfs, mock_geo_health
    ):
        from services.population_coverage_service import PopulationCoverageService

        mock_geo_instance = MagicMock()
        mock_geo_instance.build_health_deserts.return_value = {
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
                "properties": {"kind": "coverage"}
            }]
        }
        mock_geo_health.return_value = mock_geo_instance

        mock_gtfs_instance = MagicMock()
        mock_gtfs_instance.get_centers_by_comuna.return_value = MOCK_CENTERS
        mock_gtfs.return_value = mock_gtfs_instance

        mock_census_instance = MagicMock()
        mock_census_instance.load_blocks_by_comuna.return_value = MOCK_BLOCKS
        mock_census.return_value = mock_census_instance

        svc = PopulationCoverageService()
        result = svc.build_population_coverage(comuna="puente_alto", minutes=30)

        assert result["type"] == "FeatureCollection"
        assert "metadata" in result
        meta = result["metadata"]
        assert meta["scope"] == "comuna"
        assert meta["total_population"] == 150


class TestPopulationCoverageWithTransit:
    """Tests para cobertura de transporte publico."""

    @patch("services.population_coverage_service.TransitHealthDesertService")
    @patch("services.population_coverage_service.GTFSRepository")
    @patch("services.population_coverage_service.CensusRepository")
    def test_build_transit_coverage_flow(
        self, mock_census, mock_gtfs, mock_transit_health
    ):
        from services.population_coverage_service import PopulationCoverageService

        mock_transit_instance = MagicMock()
        mock_transit_instance.build_health_deserts.return_value = {
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
                "properties": {"kind": "coverage"}
            }]
        }
        mock_transit_health.return_value = mock_transit_instance

        mock_gtfs_instance = MagicMock()
        mock_gtfs_instance.get_centers_by_comuna.return_value = MOCK_CENTERS
        mock_gtfs.return_value = mock_gtfs_instance

        mock_census_instance = MagicMock()
        mock_census_instance.load_blocks_by_comuna.return_value = MOCK_BLOCKS
        mock_census.return_value = mock_census_instance

        svc = PopulationCoverageService()
        result = svc.build_transit_population_coverage(
            comuna="puente_alto", minutes=30, departure_hour=8
        )

        assert result["type"] == "FeatureCollection"
        assert "metadata" in result


class TestPopulationCoverageEdgeCases:
    """Tests de casos borde."""

    def test_build_coverage_no_centers_raises(self):
        from services.population_coverage_service import PopulationCoverageService

        with patch("services.population_coverage_service.GTFSRepository") as mock_gtfs:
            mock_gtfs_instance = MagicMock()
            mock_gtfs_instance.get_centers_by_comuna.return_value = []
            mock_gtfs.return_value = mock_gtfs_instance

            svc = PopulationCoverageService()
            with pytest.raises(ValueError, match="centros de salud"):
                svc.build_population_coverage(comuna="puente_alto", minutes=30)

    def test_build_coverage_no_blocks_raises(self):
        from services.population_coverage_service import PopulationCoverageService

        with patch("services.population_coverage_service.GeorouteHealthDesertService") as mock_geo, \
             patch("services.population_coverage_service.GTFSRepository") as mock_gtfs, \
             patch("services.population_coverage_service.CensusRepository") as mock_census:

            mock_geo_instance = MagicMock()
            mock_geo_instance.build_health_deserts.return_value = {
                "type": "FeatureCollection",
                "features": [{
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[-70.6, -33.6], [-70.6, -33.5], [-70.5, -33.5], [-70.5, -33.6], [-70.6, -33.6]]]
                    },
                    "properties": {"kind": "coverage"}
                }]
            }
            mock_geo.return_value = mock_geo_instance

            mock_gtfs_instance = MagicMock()
            mock_gtfs_instance.get_centers_by_comuna.return_value = MOCK_CENTERS
            mock_gtfs.return_value = mock_gtfs_instance

            mock_census_instance = MagicMock()
            mock_census_instance.load_blocks_by_comuna.return_value = []
            mock_census.return_value = mock_census_instance

            svc = PopulationCoverageService()
            with pytest.raises(ValueError, match="manzanas censales"):
                svc.build_population_coverage(comuna="puente_alto", minutes=30)
