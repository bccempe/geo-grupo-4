from shapely.geometry import mapping
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union
from shapely.geometry import shape

from repository.cov_poblacional_repository import CensusRepository
from repository.gtfs_repository import GTFSRepository
from services.health_desert_service import HealthDesertService
from services.transit_health_desert_service import TransitHealthDesertService
from utils.comuna_util import normalize_to_slug
from utils.geojson_utils import geometry_to_feature, feature_collection


RM_COMUNAS = [
    "santiago",
    "conchali",
    "huechuraba",
    "independencia",
    "quilicura",
    "recoleta",
    "renca",
    "las_condes",
    "lo_barnechea",
    "providencia",
    "vitacura",
    "la_reina",
    "macul",
    "nunoa",
    "penalolen",
    "la_florida",
    "la_granja",
    "el_bosque",
    "la_cisterna",
    "la_pintana",
    "san_ramon",
    "lo_espejo",
    "pedro_aguirre_cerda",
    "san_joaquin",
    "san_miguel",
    "cerrillos",
    "estacion_central",
    "maipu",
    "cerro_navia",
    "lo_prado",
    "pudahuel",
    "quinta_normal",
    "puente_alto",
    "san_jose_de_maipo",
    "pirque",
    "colina",
    "lampa",
    "til_til",
    "san_bernardo",
    "buin",
    "calera_de_tango",
    "paine",
    "melipilla",
    "alhue",
    "curacavi",
    "maria_pinto",
    "san_pedro",
    "talagante",
    "el_monte",
    "isla_de_maipo",
    "padre_hurtado",
    "penaflor"
]


class PopulationCoverageService:
    """
    Calcula cobertura poblacional usando las isócronas caminando
    y los bloques censales por comuna.
    """

    def __init__(self):
        self.census_repository = CensusRepository()
        self.health_desert_service = HealthDesertService()
        self.transit_health_desert_service = TransitHealthDesertService()

        self.gtfs_repo = GTFSRepository()

    def _fix_geometry(self, geom: BaseGeometry | None) -> BaseGeometry | None:
        if geom is None:
            return None
        try:
            if not geom.is_valid:
                geom = geom.buffer(0)
        except Exception:
            return None
        return geom

    def _extract_coverage_polygon(self, comuna: str, minutes: float) -> BaseGeometry:
        """
        Reutiliza el servicio que ya funciona para caminata y extrae
        solo el polígono de cobertura.
        """
        result = self.health_desert_service.build_health_deserts(
            comuna=comuna,
            minutes=minutes
        )

        for feature in result["features"]:
            if feature["properties"].get("kind") == "coverage":
                geom = shape(feature["geometry"])
                geom = self._fix_geometry(geom)
                if geom is None or geom.is_empty:
                    raise ValueError(f"Cobertura vacía para comuna {comuna}")
                return geom

        raise ValueError(f"No se encontró cobertura para comuna {comuna}")

    def _build_block_features(
        self,
        blocks: list[dict],
        coverage_polygon: BaseGeometry,
        comuna_slug: str
    ) -> tuple[list[dict], dict]:
        features = []

        total_population = 0.0
        covered_population = 0.0

        total_elderly_population = 0.0
        covered_elderly_population = 0.0

        covered_blocks = 0

        for block in blocks:
            geom = block.get("geometry")
            if geom is None or geom.is_empty:
                continue

            population = float(block.get("population", 0) or 0)
            elderly_population = float(block.get("elderly_population", 0) or 0)

            total_population += population
            total_elderly_population += elderly_population

            block_area = geom.area
            coverage_ratio = 0.0

            if block_area > 0 and coverage_polygon.intersects(geom):
                overlap = coverage_polygon.intersection(geom)
                if overlap is not None and not overlap.is_empty:
                    coverage_ratio = min(1.0, float(overlap.area) / float(block_area))

            block_covered_population = population * coverage_ratio
            block_covered_elderly = elderly_population * coverage_ratio

            covered_population += block_covered_population
            covered_elderly_population += block_covered_elderly

            if coverage_ratio > 0:
                covered_blocks += 1

            status = "uncovered"
            if coverage_ratio >= 1:
                status = "covered"
            elif coverage_ratio > 0:
                status = "partial"

            features.append(
                geometry_to_feature(
                    geom,
                    properties={
                        "kind": "census_block",
                        "comuna": comuna_slug,
                        "block_id": block.get("block_id"),
                        "population": population,
                        "elderly_population": elderly_population,
                        "coverage_ratio": round(coverage_ratio, 6),
                        "covered_population": round(block_covered_population, 6),
                        "covered_elderly_population": round(block_covered_elderly, 6),
                        "status": status
                    }
                )
            )

        desert_population = max(0.0, total_population - covered_population)
        desert_elderly_population = max(0.0, total_elderly_population - covered_elderly_population)

        coverage_pct = 0.0
        elderly_coverage_pct = 0.0

        if total_population > 0:
            coverage_pct = (covered_population / total_population) * 100.0

        if total_elderly_population > 0:
            elderly_coverage_pct = (covered_elderly_population / total_elderly_population) * 100.0

        summary = {
            "comuna": comuna_slug,
            "total_population": round(total_population, 6),
            "covered_population": round(covered_population, 6),
            "desert_population": round(desert_population, 6),
            "coverage_pct": round(coverage_pct, 6),
            "total_elderly_population": round(total_elderly_population, 6),
            "covered_elderly_population": round(covered_elderly_population, 6),
            "desert_elderly_population": round(desert_elderly_population, 6),
            "elderly_coverage_pct": round(elderly_coverage_pct, 6),
            "block_count": len(blocks),
            "covered_block_count": covered_blocks
        }

        return features, summary

    def build_population_coverage(self, comuna: str, minutes: float = 15):
        comuna_slug = normalize_to_slug(comuna)

        centers = self.gtfs_repo.get_centers_by_comuna(comuna_slug)

        if not centers:
            raise ValueError(f"No se encontraron centros de salud para {comuna_slug}")

        coverage_polygon = self._extract_coverage_polygon(
            comuna=comuna_slug,
            minutes=minutes
        )

        blocks = self.census_repository.load_blocks_by_comuna(comuna_slug)

        if not blocks:
            raise ValueError(f"No se encontraron manzanas censales para {comuna_slug}")

        block_features, summary = self._build_block_features(
            blocks=blocks,
            coverage_polygon=coverage_polygon,
            comuna_slug=comuna_slug
        )

        features = [
            geometry_to_feature(
                coverage_polygon,
                properties={
                    "kind": "coverage",
                    "comuna": comuna_slug,
                    "minutes": minutes
                }
            )
        ]
        features.extend(block_features)

        return feature_collection(
            features,
            center_list=centers,
            metadata={
                "scope": "comuna",
                "minutes": minutes,
                **summary
            }
        )

    def build_population_coverage_rm(self, minutes: float = 15, comunas: list[str] | None = None):
        comunas = comunas or RM_COMUNAS

        coverage_polygons = []
        all_blocks = []
        failed_comunas = []

        for comuna in comunas:
            comuna_slug = normalize_to_slug(comuna)

            try:
                coverage_polygon = self._extract_coverage_polygon(
                    comuna=comuna_slug,
                    minutes=minutes
                )
                coverage_polygons.append(coverage_polygon)
            except Exception:
                failed_comunas.append(comuna_slug)
                continue

            try:
                blocks = self.census_repository.load_blocks_by_comuna(comuna_slug)
                all_blocks.extend(blocks)
            except Exception:
                failed_comunas.append(comuna_slug)
                continue

        if not coverage_polygons:
            raise ValueError("No se pudieron generar coberturas para la RM")

        if not all_blocks:
            raise ValueError("No se encontraron manzanas censales para la RM")

        rm_coverage = unary_union(coverage_polygons)
        rm_coverage = self._fix_geometry(rm_coverage)

        if rm_coverage is None or rm_coverage.is_empty:
            raise ValueError("La cobertura consolidada de la RM está vacía")

        block_features, summary = self._build_block_features(
            blocks=all_blocks,
            coverage_polygon=rm_coverage,
            comuna_slug="rm"
        )

        features = [
            geometry_to_feature(
                rm_coverage,
                properties={
                    "kind": "coverage",
                    "scope": "rm",
                    "minutes": minutes
                }
            )
        ]
        features.extend(block_features)

        summary.update({
            "scope": "rm",
            "comunas": comunas,
            "comunas_count": len(comunas),
            "failed_comunas": failed_comunas,
            "failed_count": len(failed_comunas)
        })

        return feature_collection(
            features,
            metadata=summary
        )

    def _extract_transit_coverage_polygon(
        self,
        comuna: str,
        minutes: float,
        departure_hour: int | None = None
    ) -> BaseGeometry:

        result = self.transit_health_desert_service.build_health_deserts(
            comuna=comuna,
            minutes=minutes,
            departure_hour=departure_hour
        )

        for feature in result["features"]:
            if feature["properties"].get("kind") == "coverage":
                geom = shape(feature["geometry"])
                geom = self._fix_geometry(geom)
                if geom is None or geom.is_empty:
                    raise ValueError(
                        f"Cobertura TP vacía para comuna {comuna}"
                    )
                return geom

        raise ValueError(
            f"No se encontró cobertura TP para comuna {comuna}"
        )

    def build_transit_population_coverage(
        self,
        comuna: str,
        minutes: float = 30,
        departure_hour: int | None = None
    ):

        comuna_slug = normalize_to_slug(comuna)

        centers = self.gtfs_repo.get_centers_by_comuna(comuna_slug)

        if not centers:
            raise ValueError(
                f"No se encontraron centros de salud para {comuna_slug}"
            )

        coverage_polygon = self._extract_transit_coverage_polygon(
            comuna=comuna_slug,
            minutes=minutes,
            departure_hour=departure_hour
        )

        blocks = self.census_repository.load_blocks_by_comuna(comuna_slug)

        if not blocks:
            raise ValueError(
                f"No se encontraron manzanas censales para {comuna_slug}"
            )

        block_features, summary = self._build_block_features(
            blocks=blocks,
            coverage_polygon=coverage_polygon,
            comuna_slug=comuna_slug
        )

        features = [
            geometry_to_feature(
                coverage_polygon,
                properties={
                    "kind": "coverage",
                    "mode": "transit",
                    "comuna": comuna_slug,
                    "minutes": minutes,
                    "departure_hour": departure_hour
                }
            )
        ]
        features.extend(block_features)

        return feature_collection(
            features,
            center_list=centers,
            metadata={
                "scope": "comuna",
                "mode": "transit",
                "minutes": minutes,
                "departure_hour": departure_hour,
                **summary
            }
        )

    def build_transit_population_coverage_rm(
        self,
        minutes: float = 30,
        departure_hour: int | None = None,
        comunas: list[str] | None = None,
    ):

        comunas = comunas or RM_COMUNAS

        coverage_polygons = []
        all_blocks = []
        failed_comunas = []

        for comuna in comunas:

            comuna_slug = normalize_to_slug(comuna)

            try:
                coverage_polygon = (
                    self._extract_transit_coverage_polygon(
                        comuna=comuna_slug,
                        minutes=minutes,
                        departure_hour=departure_hour
                    )
                )
                coverage_polygons.append(coverage_polygon)
            except Exception:
                failed_comunas.append(comuna_slug)
                continue

            try:
                blocks = (
                    self.census_repository.load_blocks_by_comuna(
                        comuna_slug
                    )
                )
                all_blocks.extend(blocks)
            except Exception:
                failed_comunas.append(comuna_slug)
                continue

        if not coverage_polygons:
            raise ValueError(
                "No se pudieron generar coberturas TP para la RM"
            )

        if not all_blocks:
            raise ValueError(
                "No se encontraron manzanas censales para la RM"
            )

        rm_coverage = unary_union(coverage_polygons)
        rm_coverage = self._fix_geometry(rm_coverage)

        if rm_coverage is None or rm_coverage.is_empty:
            raise ValueError(
                "La cobertura TP consolidada de la RM está vacía"
            )

        block_features, summary = self._build_block_features(
            blocks=all_blocks,
            coverage_polygon=rm_coverage,
            comuna_slug="rm"
        )

        features = [
            geometry_to_feature(
                rm_coverage,
                properties={
                    "kind": "coverage",
                    "mode": "transit",
                    "scope": "rm",
                    "minutes": minutes,
                    "departure_hour": departure_hour
                }
            )
        ]
        features.extend(block_features)

        summary.update({
            "scope": "rm",
            "mode": "transit",
            "comunas": comunas,
            "comunas_count": len(comunas),
            "failed_comunas": failed_comunas,
            "failed_count": len(failed_comunas)
        })

        return feature_collection(
            features,
            metadata=summary
        )