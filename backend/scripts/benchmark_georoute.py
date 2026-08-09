#!/usr/bin/env python3
"""
Benchmark: NetworkX (PEP 1) vs georoute (PEP 2).

Compara tiempo de calculo de una isocrona peatonal de 30 minutos
desde un CESFAM de Puente Alto.

Salida:
  - resultados_informe/benchmark_georoute.tex  -> tabla LaTeX
  - resultados_informe/benchmark_georoute.json -> datos crudos
"""

import json
import os
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT / "backend"))

import networkx as nx

from repository.graph_repository import GraphRepository
from repository.gtfs_repository import GTFSRepository
from services.georoute_client import GeorouteClient
from utils.comuna_util import normalize_to_slug

OUTPUT_DIR = _REPO_ROOT / "resultados_informe"
COMUNA = "puente_alto"
MINUTES = 30


def benchmark_networkx(graph_repo, origin_lon, origin_lat):
    graph = graph_repo.load_graph(COMUNA)

    origin_node = _find_nearest_node(graph, origin_lon, origin_lat)
    if origin_node is None:
        return {"error": "No se encontro nodo cercano al origen"}

    t0 = time.perf_counter()
    lengths = nx.single_source_dijkstra_path_length(
        graph, origin_node,
        cutoff=MINUTES * 60,
        weight="time",
    )
    elapsed = time.perf_counter() - t0

    return {
        "wall_clock_s": round(elapsed, 4),
        "nodes_explored": len(lengths),
        "origin_node": origin_node,
    }


def benchmark_georoute(client, origin_lon, origin_lat):
    t0 = time.perf_counter()
    result = client.isochrone(lon=origin_lon, lat=origin_lat, minutes=MINUTES)
    elapsed = time.perf_counter() - t0

    features = result.get("features", [])
    if not features and "geometry" in result:
        features = [result]

    return {
        "wall_clock_s": round(elapsed, 4),
        "features_returned": len(features),
    }


def _find_nearest_node(graph, lon, lat):
    nearest = None
    best = float("inf")
    for nid, data in graph.nodes(data=True):
        x = data.get("x")
        y = data.get("y")
        if x is None or y is None:
            continue
        dist = ((x - lon) ** 2 + (y - lat) ** 2) ** 0.5
        if dist < best:
            best = dist
            nearest = nid
    return nearest


def generate_latex(nx_result, gr_result):
    nx_time = nx_result.get("wall_clock_s", "?")
    gr_time = gr_result.get("wall_clock_s", "?")
    nx_nodes = nx_result.get("nodes_explored", "?")

    speedup = ""
    if isinstance(nx_time, (int, float)) and isinstance(gr_time, (int, float)) and gr_time > 0:
        speedup = f"{nx_time / gr_time:.0f}$\\times$"

    return (
        r"\begin{table}[H]" + "\n"
        r"\centering" + "\n"
        r"\caption{Benchmark: NetworkX (PEP 1) vs \texttt{georoute} (PEP 2).}" + "\n"
        r"\label{tab:benchmark}" + "\n"
        r"\begin{tabular}{@{}lrr@{}}" + "\n"
        r"\toprule" + "\n"
        r"\textbf{Métrica} & \textbf{NetworkX (PEP 1)} & \textbf{\texttt{georoute} (PEP 2)}\\" + "\n"
        r"\midrule" + "\n"
        f"Tiempo de cálculo (wall-clock) & {nx_time} s & {gr_time} s \\\\\n"
        f"Nodos explorados & {nx_nodes} & --- \\\\\n"
        r"\midrule" + "\n"
        f"Speedup & \\multicolumn{{2}}{{c}}{{{speedup}}} \\\\\n"
        r"\bottomrule" + "\n"
        r"\end{tabular}" + "\n"
        r"\end{table}" + "\n"
    )


def main():
    print("=" * 50)
    print("BENCHMARK: NetworkX vs georoute")
    print(f"Comuna: {COMUNA}, {MINUTES} min")
    print("=" * 50)

    graph_repo = GraphRepository()
    gtfs_repo = GTFSRepository()
    georoute_client = GeorouteClient(profile="foot")

    centers = gtfs_repo.get_centers_by_comuna(normalize_to_slug(COMUNA))
    if not centers:
        print("ERROR: no se encontraron centros de salud en", COMUNA)
        return

    origin = centers[0]
    origin_lon = origin.get("lon") or origin.get("lng")
    origin_lat = origin.get("lat")
    center_name = origin.get("name", origin.get("nombre", "CESFAM"))
    print(f"Origen: {center_name} ({origin_lat}, {origin_lon})")

    print("\n[1/2] NetworkX Dijkstra ...")
    nx_result = benchmark_networkx(graph_repo, origin_lon, origin_lat)
    if "error" in nx_result:
        print(f"  ERROR: {nx_result['error']}")
        return
    print(f"  Tiempo: {nx_result['wall_clock_s']}s")
    print(f"  Nodos explorados: {nx_result['nodes_explored']}")

    print("\n[2/2] georoute isochrone ...")
    try:
        gr_result = benchmark_georoute(georoute_client, origin_lon, origin_lat)
        print(f"  Tiempo: {gr_result['wall_clock_s']}s")
        print(f"  Features: {gr_result['features_returned']}")
    except Exception as e:
        print(f"  ERROR: {e}")
        gr_result = {"wall_clock_s": "ERROR", "features_returned": 0}

    latex_table = generate_latex(nx_result, gr_result)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_DIR / "benchmark_georoute.tex", "w") as f:
        f.write(latex_table)
    print("\nOK: benchmark_georoute.tex")

    with open(OUTPUT_DIR / "benchmark_georoute.json", "w") as f:
        json.dump({
            "comuna": COMUNA,
            "minutes": MINUTES,
            "origin": {"name": center_name, "lat": origin_lat, "lon": origin_lon},
            "networkx": nx_result,
            "georoute": gr_result,
        }, f, indent=2)
    print("OK: benchmark_georoute.json")

    print("\n--- Tabla LaTeX para informe2.tex ---")
    print(latex_table)


if __name__ == "__main__":
    main()
