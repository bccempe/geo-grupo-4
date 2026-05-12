from fastapi import APIRouter, HTTPException, Query
import traceback

from services.isochrone_service import IsochroneService

router = APIRouter(prefix="/api/v1", tags=["Isochrones"])

service = IsochroneService()


@router.get("/isochrone")
def get_isochrone(
    comuna: str = Query(...),
    lat: float = Query(...),
    lon: float = Query(...),
    minutes: float = Query(15, ge=1, le=180),
    include_centers: bool = Query(False)
):

    try:

        print("===================================")
        print("INICIANDO CALCULO DE ISOCRONA")
        print("comuna:", comuna)
        print("lat:", lat)
        print("lon:", lon)
        print("minutes:", minutes)
        print("===================================")

        result = service.build_isochrone(
            comuna=comuna,
            lat=lat,
            lon=lon,
            minutes=minutes,
            include_centers=include_centers
        )

        print("ISOCRONA GENERADA CORRECTAMENTE")

        return result

    except ValueError as exc:

        print("VALUE ERROR:")
        print(str(exc))

        raise HTTPException(
            status_code=400,
            detail=str(exc)
        )

    except Exception as exc:

        print("ERROR INTERNO:")
        print(str(exc))

        traceback.print_exc()

        raise HTTPException(
            status_code=500,
            detail=str(exc)
        )