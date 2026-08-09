from fastapi import APIRouter, HTTPException, Query
import traceback

from services.location_optimization_service import LocationOptimizationService


router = APIRouter(
    prefix="/api/v1/location",
    tags=["Location Optimization"],
)

service = LocationOptimizationService()


@router.get("/optimize")
def optimize_locations(
    comuna: str = Query(...),
    minutes: float = Query(30, ge=1, le=180),
    max_centers: int = Query(3, ge=1, le=10),
    prioritize_elderly: bool = Query(True),
):
    try:
        print("=" * 40)
        print("OPTIMIZANDO UBICACIONES")
        print("comuna:", comuna)
        print("minutes:", minutes)
        print("max_centers:", max_centers)
        print("prioritize_elderly:", prioritize_elderly)
        print("=" * 40)

        import time
        start_time = time.perf_counter()

        result = service.optimize(
            comuna=comuna,
            minutes=minutes,
            max_centers=max_centers,
            prioritize_elderly=prioritize_elderly,
        )

        elapsed = time.perf_counter() - start_time
        print(f"[LocationOptimizationController] Ejecutado en {elapsed:.4f}s")
        return result

    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))
