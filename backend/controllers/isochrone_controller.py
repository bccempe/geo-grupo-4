from fastapi import APIRouter, HTTPException, Query
import traceback

from services.isochrone_service import IsochroneService
from services.geocode_service import GeocodeService

router = APIRouter(prefix="/api/v1", tags=["Isochrones"])

service = IsochroneService()
geocode_service = GeocodeService()


@router.get("/isochrone")
def get_isochrone(
    comuna: str = Query(None),
    lat: float = Query(None),
    lon: float = Query(None),
    address: str = Query(None),
    minutes: float = Query(15, ge=1, le=180),
    include_centers: bool = Query(False)
):
    try:
        # Si se proporciona una dirección pero no coordenadas, traducir dirección a coordenadas
        if address and (lat is None or lon is None):
            results = geocode_service.search_address(address, limit=1)
            if not results:
                raise ValueError(f"No se pudo traducir la dirección '{address}' a coordenadas.")
            lat = results[0]["lat"]
            lon = results[0]["lon"]
            if not comuna and results[0].get("comuna"):
                comuna = results[0]["comuna"]

        if lat is None or lon is None:
            raise ValueError("Se requieren coordenadas (lat, lon) o una dirección (address) para calcular la isócrona.")

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