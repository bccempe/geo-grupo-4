from fastapi import APIRouter, HTTPException, Query
import traceback

from services.population_coverage_service import PopulationCoverageService


router = APIRouter(
    prefix="/api/v1/population",
    tags=["Population Coverage"]
)

service = PopulationCoverageService()


@router.get("/accessibility")
def get_population_accessibility(
    comuna: str = Query(...),
    minutes: float = Query(15, ge=1, le=180),
    decay: str = Query("step", pattern="^(step|gaussian|linear)$"),
):
    """Entrega accesibilidad 2SFCA por manzana usando georoute."""
    try:
        return service.build_population_accessibility(
            comuna=comuna,
            minutes=minutes,
            decay=decay,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/coverage")
def get_population_coverage(
    comuna: str = Query(...),
    minutes: float = Query(15, ge=1, le=180),
    profile: str = Query("foot", pattern="^(foot|car)$"),
):
    """
    Calcula cobertura poblacional por manzana para una comuna.
    """
    try:
        print("===================================")
        print("CALCULANDO COBERTURA POBLACIONAL")
        print("comuna:", comuna)
        print("minutes:", minutes)
        print("profile:", profile)
        print("===================================")

        import time
        start_time = time.perf_counter()

        result = service.build_population_coverage(
            comuna=comuna,
            minutes=minutes,
            profile=profile,
        )

        elapsed = time.perf_counter() - start_time
        print(f"[PopulationCoverageController] COBERTURA POBLACIONAL CALCULADA EN {elapsed:.4f}s")
        return result

    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/coverage-rm")
def get_population_coverage_rm(
    minutes: float = Query(15, ge=1, le=180),
    profile: str = Query("foot", pattern="^(foot|car)$"),
):
    """
    Calcula cobertura poblacional consolidada para toda la RM.
    """
    try:
        print("===================================")
        print("CALCULANDO COBERTURA POBLACIONAL RM")
        print("minutes:", minutes)
        print("profile:", profile)
        print("===================================")

        import time
        start_time = time.perf_counter()

        result = service.build_population_coverage_rm(
            minutes=minutes,
            profile=profile,
        )

        elapsed = time.perf_counter() - start_time
        print(f"[PopulationCoverageController] COBERTURA POBLACIONAL RM CALCULADA EN {elapsed:.4f}s")
        return result

    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/transit-coverage")
def get_transit_population_coverage(
    comuna: str = Query(...),
    minutes: float = Query(30, ge=1, le=180),
    departure_hour: int = Query(None, ge=0, le=23)
):
    """
    Calcula cobertura poblacional por manzana para una comuna
    utilizando transporte público.
    """
    try:
        print("===================================")
        print("CALCULANDO COBERTURA TP")
        print("comuna:", comuna)
        print("minutes:", minutes)
        print("departure_hour:", departure_hour)
        print("===================================")

        import time
        start_time = time.perf_counter()

        result = service.build_transit_population_coverage(
            comuna=comuna,
            minutes=minutes,
            departure_hour=departure_hour
        )

        elapsed = time.perf_counter() - start_time
        print(f"[PopulationCoverageController] COBERTURA TP CALCULADA EN {elapsed:.4f}s")
        return result

    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/transit-coverage-rm")
def get_transit_population_coverage_rm(
    minutes: float = Query(30, ge=1, le=180),
    departure_hour: int = Query(None, ge=0, le=23)
):
    """
    Calcula cobertura poblacional TP consolidada para toda la RM.
    """
    try:
        print("===================================")
        print("CALCULANDO COBERTURA TP RM")
        print("minutes:", minutes)
        print("departure_hour:", departure_hour)
        print("===================================")

        import time
        start_time = time.perf_counter()

        result = service.build_transit_population_coverage_rm(
            minutes=minutes,
            departure_hour=departure_hour
        )

        elapsed = time.perf_counter() - start_time
        print(f"[PopulationCoverageController] COBERTURA TP RM CALCULADA EN {elapsed:.4f}s")
        return result

    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))
