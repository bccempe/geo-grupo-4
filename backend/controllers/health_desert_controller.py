from fastapi import APIRouter, HTTPException, Query
import traceback

from services.health_desert_service import HealthDesertService

router = APIRouter(
    prefix="/api/v1",
    tags=["Health Desert"]
)

service = HealthDesertService()


@router.get("/health-deserts")
def get_health_deserts(
    comuna: str = Query(...),
    minutes: float = Query(15, ge=1, le=180)
):
    """
    Calcula zonas cubiertas y desiertos de salud
    para una comuna completa.
    """

    try:

        print("===================================")
        print("CALCULANDO DESIERTOS DE SALUD")
        print("comuna:", comuna)
        print("minutes:", minutes)
        print("===================================")

        result = service.build_health_deserts(
            comuna=comuna,
            minutes=minutes
        )

        print("DESIERTOS CALCULADOS")

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