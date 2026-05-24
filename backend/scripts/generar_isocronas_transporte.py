#!/usr/bin/env python3
"""
Script para pre-calcular isocronas de transporte público
para todos los centros de atención primaria de la RM.

Guarda resultados en:
  - PostGIS: tabla isocronas_transporte
  - GeoJSON: backend/data/isocronas_transporte.geojson

Uso:
  python backend/scripts/generar_isocronas_transporte.py
"""
import os
import sys
from pathlib import Path

import geopandas as gpd
import networkx as nx
from shapely.geometry import MultiPoint
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).parent.parent))

from repository.gtfs_repository import GTFSRepository

DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/salud_rm")
TIMES_MIN = [15, 30, 45, 60]
WALK_DIST_MAX = 800
WALK_TIME_MIN = 10
WAIT_TIME_MIN = 5
OUTPUT_DIR = Path(__file__).parent.parent / "data"
OUTPUT_GEOJSON = OUTPUT_DIR / "isocronas_transporte.geojson"


def log(msg):
    print(f"[ISOCRONAS-TP] {msg}", flush=True)


def calcular_isocrona(repo, travel_graph, centro, minutos):
    lat, lng = centro["lat"], centro["lng"]
    origin_stops = repo.find_nearby_stops(lat, lng, max_dist_m=WALK_DIST_MAX)
    if not origin_stops:
        return None

    remaining = minutos - WALK_TIME_MIN - WAIT_TIME_MIN
    if remaining <= 0:
        return None

    cutoff_sec = remaining * 60
    reachable = set()

    for origin in origin_stops:
        stop_id = origin["stop_id"]
        if stop_id not in travel_graph:
            reachable.add(stop_id)
            continue
        try:
            lengths = nx.single_source_dijkstra_path_length(
                travel_graph, source=stop_id, cutoff=cutoff_sec, weight="time"
            )
            for s in lengths:
                reachable.add(s)
        except Exception:
            reachable.add(stop_id)

    if len(reachable) < 3:
        return None

    stops_df = repo.load_stops()
    reachable_stops = stops_df[stops_df["stop_id"].isin(reachable)]
    coords = list(zip(reachable_stops["stop_lon"], reachable_stops["stop_lat"]))

    polygon = MultiPoint(coords).convex_hull
    if polygon.geom_type == "Point":
        polygon = polygon.buffer(0.002)
    elif polygon.geom_type == "LineString":
        polygon = polygon.buffer(0.002)

    return polygon


def main():
    log("===== GENERACION MASIVA DE ISOCRONAS - TRANSPORTE PUBLICO =====")

    repo = GTFSRepository()

    log("1. CARGANDO CENTROS DE ATENCION PRIMARIA")
    try:
        centers = repo.load_primary_care_centers_from_db()
        log(f"   desde DB salud_primaria: {len(centers)}")
    except Exception as e:
        log(f"   DB no disponible ({e}), usando shapefile")
        centers = repo.load_primary_care_centers()
        log(f"   total centros: {len(centers)}")

    log("2. CONSTRUYENDO GRAFO DE TRANSPORTE")
    travel_graph = repo.build_travel_graph()
    log(f"   nodos (paradas): {travel_graph.number_of_nodes()}")
    log(f"   aristas (conexiones): {travel_graph.number_of_edges()}")

    rows = []
    total = len(centers) * len(TIMES_MIN)
    done = 0

    log("3. CALCULANDO ISOCRONAS")
    for centro in centers:
        for t in TIMES_MIN:
            try:
                poly = calcular_isocrona(repo, travel_graph, centro, t)
                if poly is not None:
                    rows.append({
                        "id_centro": centro["id_orig"],
                        "nombre": centro["nombre"],
                        "tipo": centro["tipo"],
                        "comuna": centro["comuna"],
                        "tiempo_min": t,
                        "n_paradas": 0,
                        "geometry": poly
                    })
            except Exception as e:
                log(f"   error: {centro['nombre']} {t}min: {e}")

            done += 1
            if done % 500 == 0:
                log(f"   progreso: {done}/{total}")

    log(f"   isocronas generadas: {len(rows)}")

    if not rows:
        log("ERROR: No se generó ninguna isócrona")
        return

    log("4. GUARDANDO RESULTADOS")
    gdf = gpd.GeoDataFrame(rows, crs="EPSG:4326")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    gdf.to_file(OUTPUT_GEOJSON, driver="GeoJSON")
    log(f"   GeoJSON -> {OUTPUT_GEOJSON}")

    try:
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS isocronas_transporte CASCADE"))
        gdf.to_postgis("isocronas_transporte", engine, if_exists="replace", index=False)
        log("   PostGIS -> isocronas_transporte")

        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM isocronas_transporte"))
            count = result.scalar()
            log(f"   registros en DB: {count}")
    except Exception as e:
        log(f"   Error conectando a PostGIS: {e}")
        log("   (solo se guardó GeoJSON)")

    log("")
    log("RESUMEN:")
    log(f"   Total centros: {len(centers)}")
    log(f"   Total isocronas: {len(rows)}")
    for t in TIMES_MIN:
        n = len([r for r in rows if r["tiempo_min"] == t])
        log(f"   {t} min: {n} isocronas")

    log("===== COMPLETADO =====")


if __name__ == "__main__":
    main()
