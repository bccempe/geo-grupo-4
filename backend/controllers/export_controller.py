from fastapi import APIRouter, HTTPException, Body
from fastapi.responses import Response
import traceback
from typing import Dict, Any, Optional

from services.export_service import ExportService

router = APIRouter(prefix="/api/v1/export", tags=["Map Export"])
service = ExportService()

@router.post("/health-desert")
def export_health_desert(payload: Dict[str, Any] = Body(...)):
    """
    Recibe la respuesta GeoJSON de desiertos de salud y genera una imagen PNG.
    """
    try:
        data = payload.get("data", {})
        minutes = int(payload.get("minutes", 30))
        comuna = str(payload.get("comuna", "Comuna"))
        mode = str(payload.get("mode", "caminando"))

        png_bytes = service.export_health_desert_map(
            data=data,
            minutes=minutes,
            comuna=comuna,
            mode=mode
        )
        return Response(content=png_bytes, media_type="image/png")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error exportando mapa de desiertos: {exc}")


@router.post("/population-coverage")
def export_population_coverage(payload: Dict[str, Any] = Body(...)):
    """
    Recibe la respuesta GeoJSON de cobertura poblacional y genera una imagen PNG.
    """
    try:
        data = payload.get("data", {})
        minutes = int(payload.get("minutes", 15))
        comuna = payload.get("comuna", "Región Metropolitana")

        png_bytes = service.export_population_coverage_map(
            data=data,
            minutes=minutes,
            comuna=comuna
        )
        return Response(content=png_bytes, media_type="image/png")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error exportando mapa de cobertura: {exc}")
