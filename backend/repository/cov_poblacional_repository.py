import os
from typing import List, Dict

from shapely import wkb
from shapely.geometry.base import BaseGeometry
from sqlalchemy import text

from app.db.database import engine
from utils.comuna_util import normalize_to_slug


CENSUS_TABLE = os.getenv("CENSUS_TABLE", "cartografia_rm_censo2024_manzanas")
CENSUS_GEOM_COL = os.getenv("CENSUS_GEOM_COL", "geom")
CENSUS_COMUNA_COL = os.getenv("CENSUS_COMUNA_COL", "comuna")
CENSUS_POP_COL = os.getenv("CENSUS_POP_COL", "n_per")
CENSUS_ELDERLY_COL = os.getenv("CENSUS_ELDERLY_COL", "n_edad_60_mas")


class CensusRepository:
    """
    Repositorio para cargar manzanas censales y sus atributos poblacionales.
    """

    def __init__(self):
        self._cache_by_comuna: dict[str, List[Dict]] = {}

    def _fix_geometry(self, geom: BaseGeometry | None) -> BaseGeometry | None:
        if geom is None:
            return None
        try:
            if not geom.is_valid:
                geom = geom.buffer(0)
        except Exception:
            return None
        return geom

    def load_blocks_by_comuna(self, comuna: str) -> list[dict]:
        """
        Carga las manzanas censales de una comuna ya normalizada o no.
        Devuelve una lista de diccionarios con:
        - block_id
        - comuna
        - comuna_slug
        - population
        - elderly_population
        - geometry
        """
        comuna_slug = normalize_to_slug(comuna)

        if comuna_slug in self._cache_by_comuna:
            return self._cache_by_comuna[comuna_slug]

        query = text(f"""
            WITH filtered AS (
                SELECT
                    "fid" AS block_id,
                    "{CENSUS_COMUNA_COL}" AS comuna,
                    COALESCE("{CENSUS_POP_COL}", 0)::double precision AS population,
                    COALESCE("{CENSUS_ELDERLY_COL}", 0)::double precision AS elderly_population,
                    "{CENSUS_GEOM_COL}" AS geom,
                    ST_SRID("{CENSUS_GEOM_COL}") AS srid
                FROM "{CENSUS_TABLE}"
                WHERE "{CENSUS_GEOM_COL}" IS NOT NULL
                  AND LOWER(
                        REGEXP_REPLACE(
                            TRANSLATE(
                                TRIM("{CENSUS_COMUNA_COL}"),
                                'ÁÀÄÂáàäâÉÈËÊéèëêÍÌÏÎíìïîÓÒÖÔóòöôÚÙÜÛúùüûÑñ',
                                'AAAAaaaaEEEEeeeeIIIIiiiiOOOOooooUUUUuuuuNn'
                            ),
                            '\\s+',
                            '_',
                            'g'
                        )
                    ) = :comuna_slug
            )
            SELECT
                block_id,
                comuna,
                population,
                elderly_population,
                ST_AsBinary(
                    CASE
                        WHEN srid IS NULL OR srid = 0 OR srid = 4326
                            THEN geom
                        ELSE ST_Transform(geom, 4326)
                    END
                ) AS geom
            FROM filtered
        """)

        with engine.connect() as conn:
            rows = conn.execute(query, {"comuna_slug": comuna_slug}).mappings().all()

        blocks: list[dict] = []

        for row in rows:
            geom = wkb.loads(bytes(row["geom"])) if row["geom"] is not None else None
            geom = self._fix_geometry(geom)

            if geom is None or geom.is_empty:
                continue

            blocks.append({
                "block_id": row["block_id"],
                "comuna": str(row["comuna"]),
                "comuna_slug": comuna_slug,
                "population": float(row["population"] or 0),
                "elderly_population": float(row["elderly_population"] or 0),
                "geometry": geom
            })

        self._cache_by_comuna[comuna_slug] = blocks
        return blocks