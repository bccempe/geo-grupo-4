from fastapi import APIRouter, HTTPException, Query
from shapely.geometry import shape
from shapely.ops import unary_union
import traceback
import json
import time

from services.car_health_desert_service import CarHealthDesertService
from utils.geojson_utils import (
    geometry_to_feature,
    feature_collection
)

router = APIRouter(
    prefix="/api/v1/car",
    tags=["Car Health Desert"]
)

service = CarHealthDesertService()

RM_COMUNAS = [
    # Provincia de Santiago
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

    # Provincia de Cordillera
    "puente_alto",
    "san_jose_de_maipo",
    "pirque",

    # Provincia de Chacabuco
    "colina",
    "lampa",
    "til_til",

    # Provincia de Maipo
    "san_bernardo",
    "buin",
    "calera_de_tango",
    "paine",

    # Provincia de Melipilla
    "melipilla",
    "alhue",
    "curacavi",
    "maria_pinto",
    "san_pedro",

    # Provincia de Talagante
    "talagante",
    "el_monte",
    "isla_de_maipo",
    "padre_hurtado",
    "penaflor"
]


@router.get("/health-deserts")
def get_car_health_deserts(
    comuna: str = Query(...),
    minutes: float = Query(15, ge=1, le=180)
):
    """
    Calcula zonas cubiertas y desiertos de salud
    para una comuna usando automóvil.
    """
    try:
        print("===================================")
        print("CALCULANDO DESIERTOS DE SALUD EN AUTOMOVIL")
        print("comuna:", comuna)
        print("minutes:", minutes)
        print("===================================")

        start_time = time.perf_counter()

        result = service.build_health_deserts(
            comuna=comuna,
            minutes=minutes
        )

        elapsed = time.perf_counter() - start_time
        print(f"[CarDesertController] DESIERTOS EN AUTO CALCULADOS EN {elapsed:.4f}s")

        return result

    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=str(exc)
        )
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=str(exc)
        )


@router.get("/health-deserts-rm")
def get_rm_health_deserts(
    minutes: float = Query(30, ge=1, le=180)
):
    """
    Calcula desiertos de salud para el conjunto
    de comunas definido en RM_COMUNAS y retorna
    una única geometría consolidada.
    Además guarda el resultado en un JSON.
    """
    try:
        print("===================================")
        print("DESIERTOS DE SALUD RM")
        print("minutes:", minutes)
        print("===================================")

        start_time = time.perf_counter()

        coverage_polygons = []
        desert_polygons = []
        failed_comunas = []

        for comuna in RM_COMUNAS:
            print(f"Procesando comuna: {comuna}")

            try:
                result = service.build_health_deserts(
                    comuna=comuna,
                    minutes=minutes
                )

                for feature in result["features"]:
                    kind = feature["properties"].get("kind")
                    geom = shape(feature["geometry"])

                    if kind == "coverage":
                        coverage_polygons.append(geom)
                    elif kind == "health_desert":
                        desert_polygons.append(geom)

            except Exception as exc:
                failed_comunas.append(comuna)
                print(
                    f"[WARNING] Se omitió la comuna {comuna} por error: {exc}"
                )
                traceback.print_exc()
                continue

        if not coverage_polygons:
            raise ValueError("No se generaron coberturas")

        rm_coverage = unary_union(coverage_polygons)
        rm_desert = unary_union(desert_polygons)

        features = [
            geometry_to_feature(
                rm_coverage,
                properties={
                    "kind": "coverage",
                    "mode": "car",
                    "minutes": minutes,
                    "scope": "rm",
                    "engine": "georoute",
                    "profile": "car"
                }
            ),
            geometry_to_feature(
                rm_desert,
                properties={
                    "kind": "health_desert",
                    "mode": "car",
                    "minutes": minutes,
                    "scope": "rm",
                    "engine": "georoute",
                    "profile": "car"
                }
            )
        ]

        result = feature_collection(
            features,
            metadata={
                "scope": "rm",
                "minutes": minutes,
                "engine": "georoute",
                "profile": "car",
                "comunas": RM_COMUNAS,
                "comunas_count": len(RM_COMUNAS),
                "failed_comunas": failed_comunas,
                "failed_count": len(failed_comunas)
            }
        )

        output_file = f"rm_auto_{int(minutes)}.json"

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(
                result,
                f,
                ensure_ascii=False,
                indent=2
            )

        elapsed = time.perf_counter() - start_time
        print(f"[CarDesertController] DESIERTOS RM EN AUTO CALCULADOS EN {elapsed:.4f}s (Archivo: {output_file})")

        return result

    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=str(exc)
        )
