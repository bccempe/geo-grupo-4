from copy import deepcopy

import numpy as np
from shapely.geometry import Point, mapping, shape
from shapely.ops import unary_union

from repository.cov_poblacional_repository import CensusRepository
from repository.gtfs_repository import GTFSRepository
from services.georoute_client import GeorouteClient
from services.population_coverage_service import PopulationCoverageService
from utils.comuna_util import normalize_to_slug
from utils.geojson_utils import feature_collection, geometry_to_feature, point_to_feature


MAX_CANDIDATES = 200


class LocationOptimizationService:

    def __init__(self):
        self.census_repo = CensusRepository()
        self.gtfs_repo = GTFSRepository()
        self.coverage_service = PopulationCoverageService()

    def optimize(
        self,
        comuna: str,
        minutes: float = 30,
        max_centers: int = 3,
        prioritize_elderly: bool = True,
    ) -> dict:
        comuna_slug = normalize_to_slug(comuna)

        existing_centers = self.gtfs_repo.get_centers_by_comuna(comuna_slug)

        base_coverage = self.coverage_service.build_population_coverage(
            comuna=comuna_slug,
            minutes=minutes,
        )

        blocks = self._uncovered_blocks(base_coverage)
        if not blocks:
            return self._empty_result(comuna_slug, minutes, existing_centers)

        candidates = self._build_candidates(blocks)
        if not candidates:
            return self._empty_result(comuna_slug, minutes, existing_centers)

        remaining_blocks = deepcopy(blocks)
        proposals = []

        georoute = GeorouteClient(profile="foot")

        for _ in range(max_centers):
            best = self._select_best(
                candidates=candidates,
                uncovered_blocks=remaining_blocks,
                minutes=minutes,
                georoute=georoute,
                prioritize_elderly=prioritize_elderly,
            )
            if best is None or best["covered_population"] <= 0:
                break

            proposals.append(best)
            remaining_blocks = best["remaining_blocks"]

        return self._build_response(comuna_slug, minutes, existing_centers, proposals)

    def _uncovered_blocks(self, coverage_result: dict) -> list[dict]:
        blocks = []
        for feature in coverage_result.get("features", []):
            props = feature.get("properties", {})
            if props.get("kind") != "census_block":
                continue
            if props.get("status") in ("uncovered", "partial"):
                blocks.append({
                    "geometry": shape(feature["geometry"]),
                    "population": float(props.get("population", 0) or 0),
                    "elderly_population": float(props.get("elderly_population", 0) or 0),
                    "block_id": props.get("block_id"),
                })
        return blocks

    def _build_candidates(self, blocks: list[dict]) -> list[dict]:
        weighted = [(b["geometry"].area * b["population"], b) for b in blocks if b["population"] > 0]
        weighted.sort(key=lambda x: x[0], reverse=True)

        selected = [b for _, b in weighted[:MAX_CANDIDATES]]

        return [
            {
                "lon": b["geometry"].centroid.x,
                "lat": b["geometry"].centroid.y,
                "point": b["geometry"].centroid,
            }
            for b in selected
        ]

    def _select_best(
        self,
        candidates: list[dict],
        uncovered_blocks: list[dict],
        minutes: float,
        georoute: GeorouteClient,
        prioritize_elderly: bool,
    ) -> dict | None:
        best = None
        best_score = -1.0
        best_poly = None

        for candidate in candidates:
            try:
                result = georoute.isochrone(
                    lon=candidate["lon"],
                    lat=candidate["lat"],
                    minutes=minutes,
                )
            except Exception:
                continue

            coverage_poly = self._polygon_from_isochrone(result)
            if coverage_poly is None:
                continue

            total_pop, total_eld = self._eval_coverage(
                coverage_poly, uncovered_blocks
            )

            score = total_eld if prioritize_elderly else total_pop
            if score > best_score:
                best_score = score
                best_poly = coverage_poly
                best = {
                    "lon": candidate["lon"],
                    "lat": candidate["lat"],
                    "covered_population": round(total_pop, 1),
                    "covered_elderly": round(total_eld, 1),
                }

        if best is None:
            return None

        remaining = []
        for block in uncovered_blocks:
            geom = block["geometry"]
            if best_poly is not None and best_poly.intersects(geom):
                overlap = best_poly.intersection(geom)
                if overlap is not None and not overlap.is_empty:
                    ratio = min(1.0, float(overlap.area) / float(geom.area))
                    remaining_pop = block["population"] * (1.0 - ratio)
                    remaining_eld = block["elderly_population"] * (1.0 - ratio)
                    if remaining_pop <= 0 and remaining_eld <= 0:
                        continue
                    block = dict(block)
                    block["population"] = remaining_pop
                    block["elderly_population"] = remaining_eld
            remaining.append(block)

        best["coverage_polygon"] = best_poly
        best["remaining_blocks"] = remaining

        return best

    def _eval_coverage(self, poly, blocks):
        total_pop = 0.0
        total_eld = 0.0
        for block in blocks:
            geom = block["geometry"]
            if not poly.intersects(geom):
                continue
            overlap = poly.intersection(geom)
            if overlap is None or overlap.is_empty:
                continue
            ratio = min(1.0, float(overlap.area) / float(geom.area))
            total_pop += block["population"] * ratio
            total_eld += block["elderly_population"] * ratio
        return total_pop, total_eld

    def _polygon_from_isochrone(self, georoute_result: dict):
        if not isinstance(georoute_result, dict):
            return None
        features = georoute_result.get("features", [])
        if not features:
            poly = shape(georoute_result.get("geometry", None))
            return poly if poly is not None and not poly.is_empty else None
        polys = []
        for f in features:
            g = shape(f.get("geometry", None)) if "geometry" in f else None
            if g is not None and not g.is_empty:
                polys.append(g)
        if not polys:
            return None
        return unary_union(polys)

    def _build_response(
        self,
        comuna_slug: str,
        minutes: float,
        existing_centers: list,
        proposals: list,
    ) -> dict:
        features = []

        for i, proposal in enumerate(proposals, 1):
            features.append(
                point_to_feature(
                    proposal["lon"],
                    proposal["lat"],
                    properties={
                        "kind": "proposed_center",
                        "rank": i,
                        "covered_population": proposal["covered_population"],
                        "covered_elderly": proposal["covered_elderly"],
                    },
                )
            )

            poly = proposal.get("coverage_polygon")
            if poly is not None and not poly.is_empty:
                features.append(
                    geometry_to_feature(
                        poly,
                        properties={
                            "kind": "proposed_coverage",
                            "rank": i,
                            "covered_population": proposal["covered_population"],
                            "covered_elderly": proposal["covered_elderly"],
                        },
                    )
                )

        remaining = (proposals[-1]["remaining_blocks"]
                     if proposals else [])
        total_remaining_pop = sum(b["population"] for b in remaining)
        total_remaining_eld = sum(b["elderly_population"] for b in remaining)

        metadata = {
            "comuna": comuna_slug,
            "minutes": minutes,
            "proposals_count": len(proposals),
            "max_centers_requested": 3,
            "existing_centers_count": len(existing_centers),
            "remaining_uncovered_population": round(total_remaining_pop, 1),
            "remaining_uncovered_elderly": round(total_remaining_eld, 1),
        }

        return feature_collection(features, center_list=existing_centers, metadata=metadata)

    def _empty_result(self, comuna_slug: str, minutes: float, existing_centers: list) -> dict:
        return feature_collection(
            [],
            center_list=existing_centers,
            metadata={
                "comuna": comuna_slug,
                "minutes": minutes,
                "proposals_count": 0,
                "max_centers_requested": 3,
                "existing_centers_count": len(existing_centers),
                "message": "No hay manzanas no cubiertas en esta comuna.",
            },
        )
