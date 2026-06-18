from fastapi import APIRouter, HTTPException, Query
import traceback

from services.population_coverage_service import PopulationCoverageService


router = APIRouter(
    prefix="/api/v1/population",
    tags=["Population Coverage"]
)

service = PopulationCoverageService()


@router.get("/coverage")
def get_population_coverage(
    comuna: str = Query(...),
    minutes: float = Query(15, ge=1, le=180)
):
    """
    Calcula cobertura poblacional por manzana para una comuna.
    """
    try:
        print("===================================")
        print("CALCULANDO COBERTURA POBLACIONAL")
        print("comuna:", comuna)
        print("minutes:", minutes)
        print("===================================")

        result = service.build_population_coverage(
            comuna=comuna,
            minutes=minutes
        )

        print("COBERTURA POBLACIONAL CALCULADA")
        return result

    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/coverage-rm")
def get_population_coverage_rm(
    minutes: float = Query(15, ge=1, le=180)
):
    """
    Calcula cobertura poblacional consolidada para toda la RM.
    """
    try:
        print("===================================")
        print("CALCULANDO COBERTURA POBLACIONAL RM")
        print("minutes:", minutes)
        print("===================================")

        result = service.build_population_coverage_rm(
            minutes=minutes
        )

        print("COBERTURA POBLACIONAL RM CALCULADA")
        return result

    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))