import pytest
from unittest.mock import patch, MagicMock
from shapely.geometry import Point, Polygon


MOCK_CENTERS = [
    {"id": 1, "nombre": "CESFAM A", "lat": -33.575, "lng": -70.575, "nom_comuna": "puente_alto"},
]

MOCK_GEOJSON_COVERAGE = {
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
        },
        # Block with some uncovered population
        {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-70.52, -33.52],
                    [-70.52, -33.51],
                    [-70.51, -33.51],
                    [-70.51, -33.52],
                    [-70.52, -33.52],
                ]]
            },
            "properties": {
                "kind": "census_block",
                "block_id": "b001",
                "population": 500,
                "elderly_population": 100,
                "coverage_ratio": 0.3,
                "status": "partial",
            }
        },
        # Fully uncovered block
        {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-70.55, -33.55],
                    [-70.55, -33.54],
                    [-70.54, -33.54],
                    [-70.54, -33.55],
                    [-70.55, -33.55],
                ]]
            },
            "properties": {
                "kind": "census_block",
                "block_id": "b002",
                "population": 300,
                "elderly_population": 80,
                "coverage_ratio": 0.0,
                "status": "uncovered",
            }
        },
    ]
}

MOCK_COVERED_GEOJSON = {
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
        },
        {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-70.52, -33.52],
                    [-70.52, -33.51],
                    [-70.51, -33.51],
                    [-70.51, -33.52],
                    [-70.52, -33.52],
                ]]
            },
            "properties": {
                "kind": "census_block",
                "block_id": "b001",
                "population": 500,
                "elderly_population": 100,
                "coverage_ratio": 1.0,
                "status": "covered",
            }
        },
    ]
}


class TestLocationOptimizationService:
    """Tests para LocationOptimizationService."""

    def test_uncovered_blocks_extraction(self):
        from services.location_optimization_service import LocationOptimizationService

        svc = LocationOptimizationService()
        blocks = svc._uncovered_blocks(MOCK_GEOJSON_COVERAGE)

        assert len(blocks) == 2
        statuses = {"partial", "uncovered"}
        for b in blocks:
            assert "population" in b
            assert "elderly_population" in b

    def test_uncovered_blocks_when_all_covered(self):
        from services.location_optimization_service import LocationOptimizationService

        svc = LocationOptimizationService()
        blocks = svc._uncovered_blocks(MOCK_COVERED_GEOJSON)

        assert len(blocks) == 0

    def test_build_candidates_from_blocks(self):
        from services.location_optimization_service import LocationOptimizationService

        svc = LocationOptimizationService()
        blocks = [
            {
                "geometry": Polygon([
                    (-70.6, -33.6), (-70.6, -33.5),
                    (-70.5, -33.5), (-70.5, -33.6),
                ]),
                "population": 500,
                "elderly_population": 100,
                "block_id": "b001",
            },
            {
                "geometry": Polygon([
                    (-70.55, -33.55), (-70.55, -33.54),
                    (-70.54, -33.54), (-70.54, -33.55),
                ]),
                "population": 300,
                "elderly_population": 80,
                "block_id": "b002",
            },
        ]

        candidates = svc._build_candidates(blocks)
        assert len(candidates) > 0
        for c in candidates:
            assert "lon" in c
            assert "lat" in c
            assert "point" in c

    def test_build_candidates_empty(self):
        from services.location_optimization_service import LocationOptimizationService

        svc = LocationOptimizationService()
        candidates = svc._build_candidates([])
        assert len(candidates) == 0

    def test_empty_result_structure(self):
        from services.location_optimization_service import LocationOptimizationService

        svc = LocationOptimizationService()
        result = svc._empty_result("puente_alto", 30, MOCK_CENTERS)

        assert result["type"] == "FeatureCollection"
        assert result["metadata"]["proposals_count"] == 0
        assert "message" in result["metadata"]

    @patch("services.location_optimization_service.PopulationCoverageService")
    @patch("services.location_optimization_service.GTFSRepository")
    @patch("services.location_optimization_service.CensusRepository")
    def test_optimize_all_covered_returns_empty(
        self, mock_census, mock_gtfs, mock_coverage_svc
    ):
        from services.location_optimization_service import LocationOptimizationService

        mock_coverage_instance = MagicMock()
        mock_coverage_instance.build_population_coverage.return_value = MOCK_COVERED_GEOJSON
        mock_coverage_svc.return_value = mock_coverage_instance

        mock_gtfs_instance = MagicMock()
        mock_gtfs_instance.get_centers_by_comuna.return_value = MOCK_CENTERS
        mock_gtfs.return_value = mock_gtfs_instance

        svc = LocationOptimizationService()
        result = svc.optimize(comuna="puente_alto", minutes=30, max_centers=3)

        assert result["metadata"]["proposals_count"] == 0

    @patch("services.location_optimization_service.GeorouteClient")
    @patch("services.location_optimization_service.PopulationCoverageService")
    @patch("services.location_optimization_service.GTFSRepository")
    @patch("services.location_optimization_service.CensusRepository")
    def test_optimize_with_uncovered_blocks(
        self, mock_census, mock_gtfs, mock_coverage_svc, mock_georoute
    ):
        from services.location_optimization_service import LocationOptimizationService

        mock_coverage_instance = MagicMock()
        mock_coverage_instance.build_population_coverage.return_value = MOCK_GEOJSON_COVERAGE
        mock_coverage_svc.return_value = mock_coverage_instance

        mock_gtfs_instance = MagicMock()
        mock_gtfs_instance.get_centers_by_comuna.return_value = MOCK_CENTERS
        mock_gtfs.return_value = mock_gtfs_instance

        mock_geo_instance = MagicMock()
        mock_geo_instance.isochrone.return_value = {
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [-70.56, -33.56],
                        [-70.56, -33.50],
                        [-70.50, -33.50],
                        [-70.50, -33.56],
                        [-70.56, -33.56],
                    ]]
                },
                "properties": {}
            }]
        }
        mock_georoute.return_value = mock_geo_instance

        svc = LocationOptimizationService()
        result = svc.optimize(comuna="puente_alto", minutes=30, max_centers=3)

        assert result["type"] == "FeatureCollection"
        assert result["metadata"]["proposals_count"] >= 0
        assert "existing_centers_count" in result["metadata"]


class TestLocationOptimizationEdgeCases:
    """Tests de casos borde."""

    def test_non_dict_isochrone_response(self):
        from services.location_optimization_service import LocationOptimizationService

        svc = LocationOptimizationService()
        poly = svc._polygon_from_isochrone(None)
        assert poly is None

    def test_isochrone_with_geometry_field(self):
        from services.location_optimization_service import LocationOptimizationService

        svc = LocationOptimizationService()
        result = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-70.6, -33.6], [-70.6, -33.5],
                    [-70.5, -33.5], [-70.5, -33.6],
                    [-70.6, -33.6],
                ]]
            }
        }
        poly = svc._polygon_from_isochrone(result)
        assert poly is not None
        assert not poly.is_empty
