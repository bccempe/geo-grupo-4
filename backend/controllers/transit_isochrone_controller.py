from fastapi import APIRouter, HTTPException, Query
import traceback

from services.transit_isochrone_service import TransitIsochroneService
from services.transit_health_desert_service import TransitHealthDesertService

router = APIRouter(prefix="/api/v1/transit", tags=["Transit Isochrones"])

isochrone_service = TransitIsochroneService()
desert_service = TransitHealthDesertService()


@router.get("/isochrone")
def get_transit_isochrone(
    comuna: str = Query(None),
    lat: float = Query(...),
    lon: float = Query(...),
    minutes: float = Query(30, ge=5, le=120),
    departure_hour: int = Query(None, ge=0, le=23),
    include_centers: bool = Query(False)
):
    try:
        result = isochrone_service.build_isochrone(
            comuna=comuna,
            lat=lat,
            lon=lon,
            minutes=minutes,
            departure_hour=departure_hour,
            include_centers=include_centers
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/health-deserts")
def get_transit_health_deserts(
    comuna: str = Query(...),
    minutes: float = Query(30, ge=5, le=120),
    departure_hour: int = Query(None, ge=0, le=23)
):
    try:
        result = desert_service.build_health_deserts(
            comuna=comuna,
            minutes=minutes,
            departure_hour=departure_hour
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))
