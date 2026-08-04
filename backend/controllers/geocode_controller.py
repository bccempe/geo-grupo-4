from fastapi import APIRouter, HTTPException, Query
import traceback
from typing import Optional

from services.geocode_service import GeocodeService

router = APIRouter(prefix="/api/v1/geocode", tags=["Geocoding"])
service = GeocodeService()


@router.get("/autocomplete")
@router.get("/search")
def search_address(
    q: str = Query(..., description="Dirección o lugar a buscar"),
    limit: int = Query(5, ge=1, le=20)
):
    """
    Busca direcciones para el autocompletado y traducción a coordenadas.
    """
    try:
        results = service.search_address(query=q, limit=limit)
        return {"query": q, "results": results}
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error en autocompletado de dirección: {str(exc)}"
        )


@router.get("/reverse")
def reverse_geocode(
    lat: float = Query(..., description="Latitud"),
    lon: float = Query(..., description="Longitud")
):
    """
    Traduce coordenadas (lat, lon) a una dirección formateada.
    """
    try:
        result = service.reverse_geocode(lat=lat, lon=lon)
        if not result:
            return {"lat": lat, "lon": lon, "display_name": f"{lat:.5f}, {lon:.5f}", "short_address": f"{lat:.5f}, {lon:.5f}"}
        return result
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error en traducción de coordenadas a dirección: {str(exc)}"
        )
