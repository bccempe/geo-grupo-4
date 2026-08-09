#!/usr/bin/env python3
"""
Pipeline de extraccion de metricas para la Region Metropolitana completa.

Genera:
  - tabla_cobertura_rm.tex       -> tabla comparativa walk vs transit (52 comunas)
  - tabla_cobertura_rm_consolidada.tex -> resumen RM consolidado
  - graficos/cobertura_walk_vs_transit.png
  - graficos/ranking_desiertos.png
  - graficos/distribucion_cobertura.png
  - graficos/adultos_mayores_walk_vs_transit.png
  - graficos/delta_brecha_comunas.png
  - graficos/top10_mas_desatendidas.png
  - graficos/cobertura_por_modo_hist.png
  - graficos/scatter_poblacion_vs_cobertura.png
  - figuras/desierto_puente_alto_30.png
  - figuras/cobertura_poblacional_puente_alto.png

Uso:
    python -m scripts.generate_metrics
    python -m scripts.generate_metrics --comuna puente_alto
    python -m scripts.generate_metrics --skip-plots
"""

import argparse
import os
import sys
import time
import traceback
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT / "backend"))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
from matplotlib_scalebar.scalebar import ScaleBar
import geopandas as gpd
from shapely.geometry import shape

from services.population_coverage_service import (
    PopulationCoverageService,
    RM_COMUNAS,
)
from services.health_desert_service import HealthDesertService
from services.export_service import build_population_coverage_gdfs

OUTPUT_DIR = _REPO_ROOT / "resultados_informe"
GRAFICOS_DIR = OUTPUT_DIR / "graficos"
FIGURAS_DIR = _REPO_ROOT / "figuras"

TABLE_CAPTION = r"""{0}
{1}
\label{{tab:{2}}}
"""


def ensure_dirs():
    GRAFICOS_DIR.mkdir(parents=True, exist_ok=True)
    FIGURAS_DIR.mkdir(parents=True, exist_ok=True)


def run_comuna(coverage_svc, comuna_slug, minutes=30):
    walk = coverage_svc.build_population_coverage(
        comuna=comuna_slug, minutes=minutes
    )
    transit = coverage_svc.build_transit_population_coverage(
        comuna=comuna_slug, minutes=minutes, departure_hour=8
    )

    w_stats = extract_summary(walk)
    t_stats = extract_summary(transit)

    return {
        "comuna": comuna_slug,
        "total_pop": w_stats.get("total_population", 0),
        "total_eld": w_stats.get("total_elderly_population", 0),
        "walk_cov": w_stats.get("covered_population", 0),
        "walk_cov_pct": w_stats.get("coverage_pct", 0),
        "walk_eld_cov": w_stats.get("covered_elderly_population", 0),
        "walk_eld_pct": w_stats.get("elderly_coverage_pct", 0),
        "transit_cov": t_stats.get("covered_population", 0),
        "transit_cov_pct": t_stats.get("coverage_pct", 0),
        "transit_eld_cov": t_stats.get("covered_elderly_population", 0),
        "transit_eld_pct": t_stats.get("elderly_coverage_pct", 0),
        "walk_ok": w_stats is not None,
        "transit_ok": t_stats is not None,
    }


def extract_summary(result):
    if result is None:
        return None
    meta = result.get("metadata", {})
    if not meta:
        return _extract_from_features(result)

    total_pop = meta.get("total_population") or meta.get("total_pop", 0)
    cov_pop = meta.get("covered_population") or meta.get("covered_pop", 0)
    total_eld = meta.get("total_elderly_population") or meta.get("total_elderly", 0)
    cov_eld = meta.get("covered_elderly_population") or meta.get("covered_elderly", 0)

    cov_pct = (cov_pop / total_pop * 100) if total_pop > 0 else 0
    eld_pct = (cov_eld / total_eld * 100) if total_eld > 0 else 0

    return {
        "total_population": total_pop,
        "covered_population": cov_pop,
        "coverage_pct": cov_pct,
        "total_elderly_population": total_eld,
        "covered_elderly_population": cov_eld,
        "elderly_coverage_pct": eld_pct,
    }


def _extract_from_features(result):
    total_pop = 0
    cov_pop = 0
    total_eld = 0
    cov_eld = 0
    for f in result.get("features", []):
        p = f.get("properties", {})
        if p.get("kind") != "census_block":
            continue
        total_pop += p.get("population", 0) or 0
        cov_pop += p.get("covered_population", 0) or 0
        total_eld += p.get("elderly_population", 0) or 0
        cov_eld += p.get("covered_elderly_population", 0) or 0
    cov_pct = (cov_pop / total_pop * 100) if total_pop > 0 else 0
    eld_pct = (cov_eld / total_eld * 100) if total_eld > 0 else 0
    return {
        "total_population": total_pop,
        "covered_population": cov_pop,
        "coverage_pct": cov_pct,
        "total_elderly_population": total_eld,
        "covered_elderly_population": cov_eld,
        "elderly_coverage_pct": eld_pct,
    }


def generate_latex_table(rows: list[dict]) -> str:
    header = (
        r"\begin{table}[H]" + "\n"
        r"\centering" + "\n"
        r"\caption{Cobertura poblacional comparativa: caminata vs transporte público (30 min).}" + "\n"
        r"\label{tab:cobertura-comparativa}" + "\n"
        r"\footnotesize" + "\n"
        r"\begin{tabularx}{\linewidth}{@{}lrrrrr@{}}" + "\n"
        r"\toprule" + "\n"
        r"\textbf{Comuna} & "
        r"\textbf{Pob. total} & "
        r"\textbf{Cob. walk (\%)} & "
        r"\textbf{Cob. TP (\%)} & "
        r"\textbf{$\Delta$ (pp)} & "
        r"\textbf{AM walk (\%)} \\" + "\n"
        r"\midrule" + "\n"
    )

    body = ""
    for r in sorted(rows, key=lambda x: x["total_pop"], reverse=True):
        delta = r["transit_cov_pct"] - r["walk_cov_pct"]
        body += (
            f"{r['comuna'].replace('_', ' ').title()} & "
            f"{int(r['total_pop']):,} & "
            f"{r['walk_cov_pct']:.1f} & "
            f"{r['transit_cov_pct']:.1f} & "
            f"{delta:+.1f} & "
            f"{r['walk_eld_pct']:.1f} \\\\\n"
        )

    footer = (
        r"\bottomrule" + "\n"
        r"\end{tabularx}" + "\n"
        r"\end{table}" + "\n"
    )

    return header + body + footer


def generate_consolidated_latex(rows: list[dict]) -> str:
    total_pop = sum(r["total_pop"] for r in rows)
    total_eld = sum(r["total_eld"] for r in rows)
    walk_cov = sum(r["walk_cov"] for r in rows)
    transit_cov = sum(r["transit_cov"] for r in rows)
    walk_eld = sum(r["walk_eld_cov"] for r in rows)
    transit_eld = sum(r["transit_eld_cov"] for r in rows)

    walk_pct = (walk_cov / total_pop * 100) if total_pop > 0 else 0
    transit_pct = (transit_cov / total_pop * 100) if total_pop > 0 else 0
    walk_eld_pct = (walk_eld / total_eld * 100) if total_eld > 0 else 0
    transit_eld_pct = (transit_eld / total_eld * 100) if total_eld > 0 else 0

    return (
        r"\begin{table}[H]" + "\n"
        r"\centering" + "\n"
        r"\caption{Cobertura poblacional consolidada Región Metropolitana "
        r"(52 comunas, 30 min, \texttt{departure\_hour}=8).}" + "\n"
        r"\label{tab:cobertura-rm}" + "\n"
        r"\footnotesize" + "\n"
        r"\begin{tabularx}{\linewidth}{@{}lXX@{}}" + "\n"
        r"\toprule" + "\n"
        r"\textbf{Indicador} & "
        r"\textbf{Caminata} & "
        r"\textbf{Transporte público}\\" + "\n"
        r"\midrule" + "\n"
        f"Población total RM & {int(total_pop):,} & {int(total_pop):,} \\\\\n"
        f"Población cubierta & {int(walk_cov):,} & {int(transit_cov):,} \\\\\n"
        f"Cobertura poblacional (\\%) & {walk_pct:.1f} & {transit_pct:.1f} \\\\\n"
        f"Población adulta mayor ($\\geq 60$ años) & {int(total_eld):,} & {int(total_eld):,} \\\\\n"
        f"Adultos mayores cubiertos & {int(walk_eld):,} & {int(transit_eld):,} \\\\\n"
        f"Cobertura adultos mayores (\\%) & {walk_eld_pct:.1f} & {transit_eld_pct:.1f} \\\\\n"
        r"\bottomrule" + "\n"
        r"\end{tabularx}" + "\n"
        r"\end{table}" + "\n"
    )


def plot_coverage_walk_vs_transit(df: pd.DataFrame):
    df = df.sort_values("total_pop", ascending=True).tail(20)
    x = range(len(df))
    width = 0.35

    fig, ax = plt.subplots(figsize=(12, 7))
    ax.barh([i - width / 2 for i in x], df["walk_cov_pct"], width,
            label="Caminata", color="#2b83ba")
    ax.barh([i + width / 2 for i in x], df["transit_cov_pct"], width,
            label="Transporte público", color="#d7191c")
    ax.set_yticks(x)
    ax.set_yticklabels(df["comuna"].str.replace("_", " ").str.title(), fontsize=8)
    ax.set_xlabel("Cobertura poblacional (%)")
    ax.set_title("Cobertura poblacional: caminata vs transporte público\n(20 comunas más pobladas, 30 min)")
    ax.legend()
    ax.set_xlim(0, 105)
    ax.grid(axis="x", alpha=0.3)
    plt.tight_layout()
    fig.savefig(GRAFICOS_DIR / "cobertura_walk_vs_transit.png", dpi=150)
    plt.close(fig)


def plot_ranking_desiertos(df: pd.DataFrame):
    df = df.copy()
    df["desert_walk_pct"] = 100 - df["walk_cov_pct"]
    df = df.sort_values("desert_walk_pct", ascending=False).head(15)

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(
        range(len(df)),
        df["desert_walk_pct"],
        color="#d73027",
        edgecolor="white",
    )
    ax.set_yticks(range(len(df)))
    ax.set_yticklabels(df["comuna"].str.replace("_", " ").str.title(), fontsize=9)
    ax.set_xlabel("Población no cubierta (%)")
    ax.set_title("Top 15 comunas con mayor desierto de salud\n(caminata, 30 min)")
    ax.invert_yaxis()
    ax.grid(axis="x", alpha=0.3)
    for bar, val in zip(bars, df["desert_walk_pct"]):
        ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height() / 2,
                f"{val:.1f}%", va="center", fontsize=8)
    plt.tight_layout()
    fig.savefig(GRAFICOS_DIR / "ranking_desiertos.png", dpi=150)
    plt.close(fig)


def plot_distribucion_cobertura(df: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.hist(df["walk_cov_pct"], bins=20, alpha=0.6, label="Caminata", color="#2b83ba", edgecolor="white")
    ax.hist(df["transit_cov_pct"], bins=20, alpha=0.6, label="Transporte público", color="#d7191c", edgecolor="white")
    ax.set_xlabel("Cobertura poblacional (%)")
    ax.set_ylabel("Número de comunas")
    ax.set_title("Distribución de cobertura poblacional por modo\n(52 comunas RM, 30 min)")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    fig.savefig(GRAFICOS_DIR / "distribucion_cobertura.png", dpi=150)
    plt.close(fig)


def plot_adultos_mayores_walk_vs_transit(df: pd.DataFrame):
    df = df.sort_values("total_eld", ascending=True).tail(20)
    x = range(len(df))
    width = 0.35

    fig, ax = plt.subplots(figsize=(12, 7))
    ax.barh([i - width / 2 for i in x], df["walk_eld_pct"], width,
            label="Caminata", color="#2b83ba")
    ax.barh([i + width / 2 for i in x], df["transit_eld_pct"], width,
            label="Transporte público", color="#d7191c")
    ax.set_yticks(x)
    ax.set_yticklabels(df["comuna"].str.replace("_", " ").str.title(), fontsize=8)
    ax.set_xlabel("Cobertura adultos mayores (%)")
    ax.set_title("Cobertura adultos mayores ($\\geq$60): caminata vs TP\n(20 comunas con más AM, 30 min)")
    ax.legend()
    ax.set_xlim(0, 105)
    ax.grid(axis="x", alpha=0.3)
    plt.tight_layout()
    fig.savefig(GRAFICOS_DIR / "adultos_mayores_walk_vs_transit.png", dpi=150)
    plt.close(fig)


def plot_delta_brecha(df: pd.DataFrame):
    df = df.copy()
    df["delta"] = df["transit_cov_pct"] - df["walk_cov_pct"]
    df = df.sort_values("delta", ascending=False).head(15)

    colors = ["#1a9641" if d > 0 else "#d7191c" for d in df["delta"]]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(range(len(df)), df["delta"], color=colors, edgecolor="white")
    ax.set_yticks(range(len(df)))
    ax.set_yticklabels(df["comuna"].str.replace("_", " ").str.title(), fontsize=9)
    ax.set_xlabel("Diferencia de cobertura TP - Walk (pp)")
    ax.set_title("Ganancia de cobertura al incorporar transporte público\n(15 comunas con mayor brecha, 30 min)")
    ax.invert_yaxis()
    ax.grid(axis="x", alpha=0.3)
    ax.axvline(0, color="black", linewidth=0.8)
    for bar, val in zip(ax.patches, df["delta"]):
        ax.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height() / 2,
                f"{val:+.1f}", va="center", fontsize=8)
    plt.tight_layout()
    fig.savefig(GRAFICOS_DIR / "delta_brecha_comunas.png", dpi=150)
    plt.close(fig)


def plot_top10_desatendidas(df: pd.DataFrame):
    df = df.copy()
    df["desert_walk_pop"] = df["total_pop"] - df["walk_cov"]
    df = df.sort_values("desert_walk_pop", ascending=False).head(10)

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(
        range(len(df)),
        df["desert_walk_pop"] / 1000,
        color="#fc4e2a",
        edgecolor="white",
    )
    ax.set_xticks(range(len(df)))
    ax.set_xticklabels(df["comuna"].str.replace("_", " ").str.title(),
                       fontsize=8, rotation=45, ha="right")
    ax.set_ylabel("Población no cubierta (miles)")
    ax.set_title("Top 10 comunas con mayor población no cubierta\n(caminata, 30 min)")
    ax.grid(axis="y", alpha=0.3)
    for bar, val in zip(bars, df["desert_walk_pop"]):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 2,
                f"{int(val/1000)}k", ha="center", fontsize=8)
    plt.tight_layout()
    fig.savefig(GRAFICOS_DIR / "top10_mas_desatendidas.png", dpi=150)
    plt.close(fig)


def plot_cobertura_hist(df: pd.DataFrame):
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    axes[0].hist(df["walk_cov_pct"], bins=15, color="#2b83ba", edgecolor="white")
    axes[0].set_title("Cobertura — Caminata")
    axes[0].set_xlabel("Cobertura (%)")
    axes[0].set_ylabel("Comunas")
    axes[0].grid(axis="y", alpha=0.3)

    axes[1].hist(df["transit_cov_pct"], bins=15, color="#d7191c", edgecolor="white")
    axes[1].set_title("Cobertura — Transporte público")
    axes[1].set_xlabel("Cobertura (%)")
    axes[1].set_ylabel("Comunas")
    axes[1].grid(axis="y", alpha=0.3)

    fig.suptitle("Distribución de cobertura poblacional por modo (52 comunas, 30 min)", fontsize=13)
    plt.tight_layout()
    fig.savefig(GRAFICOS_DIR / "cobertura_por_modo_hist.png", dpi=150)
    plt.close(fig)


def plot_scatter_poblacion_vs_cobertura(df: pd.DataFrame):
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    axes[0].scatter(df["total_pop"] / 1000, df["walk_cov_pct"],
                    c=100 - df["walk_cov_pct"], cmap="Reds", edgecolors="grey",
                    alpha=0.7, s=60)
    axes[0].set_xlabel("Población total (miles)")
    axes[0].set_ylabel("Cobertura (%)")
    axes[0].set_title("Caminata")
    axes[0].grid(alpha=0.3)

    axes[1].scatter(df["total_pop"] / 1000, df["transit_cov_pct"],
                    c=100 - df["transit_cov_pct"], cmap="Reds", edgecolors="grey",
                    alpha=0.7, s=60)
    axes[1].set_xlabel("Población total (miles)")
    axes[1].set_ylabel("Cobertura (%)")
    axes[1].set_title("Transporte público")
    axes[1].grid(alpha=0.3)

    fig.suptitle("Población total vs cobertura por comuna (30 min)", fontsize=13)
    plt.tight_layout()
    fig.savefig(GRAFICOS_DIR / "scatter_poblacion_vs_cobertura.png", dpi=150)
    plt.close(fig)


def generate_figures_from_results(health_desert_svc, coverage_svc):
    print("\n--- Generando figuras para el informe ---")

    try:
        result = health_desert_svc.build_health_deserts(
            comuna="puente_alto", minutes=30
        )
        _export_health_desert_png(result, "puente_alto", 30,
                                  FIGURAS_DIR / "desierto_puente_alto_30.png")
        print("  OK: desierto_puente_alto_30.png")
    except Exception as e:
        print(f"  ERROR desierto_puente_alto: {e}")

    try:
        result = coverage_svc.build_population_coverage(
            comuna="puente_alto", minutes=30
        )
        _export_coverage_png(result, "puente_alto", 30,
                             FIGURAS_DIR / "cobertura_poblacional_puente_alto.png")
        print("  OK: cobertura_poblacional_puente_alto.png")
    except Exception as e:
        print(f"  ERROR cobertura_puente_alto: {e}")


def _export_health_desert_png(result, comuna, minutes, path):
    features = result.get("features", [])
    coverage_geoms = []
    desert_geoms = []
    center_rows = []
    for f in features:
        props = f.get("properties", {})
        kind = props.get("kind")
        geom = shape(f["geometry"]) if f.get("geometry") else None
        if geom is None:
            continue
        if kind == "coverage":
            coverage_geoms.append(geom)
        elif kind == "desert":
            desert_geoms.append(geom)
        elif kind == "health_center":
            center_rows.append({"geometry": geom, "name": props.get("name", "")})

    fig, ax = plt.subplots(figsize=(10, 10))
    if coverage_geoms:
        gpd.GeoSeries(coverage_geoms, crs="EPSG:4326").to_crs("EPSG:32719").plot(
            ax=ax, color="#2ca02c", alpha=0.4, edgecolor="green", linewidth=0.5
        )
    if desert_geoms:
        gpd.GeoSeries(desert_geoms, crs="EPSG:4326").to_crs("EPSG:32719").plot(
            ax=ax, color="#d62728", alpha=0.4, edgecolor="red", linewidth=0.5
        )
    if center_rows:
        gdf_c = gpd.GeoDataFrame(center_rows, geometry="geometry", crs="EPSG:4326").to_crs("EPSG:32719")
        gdf_c.plot(ax=ax, color="#ffd700", markersize=40, marker="*", edgecolor="black")

    ax.set_title(f"Desierto de salud — {comuna.replace('_', ' ').title()} ({minutes} min)", fontsize=12)
    ax.set_xlabel("UTM Este (m)")
    ax.set_ylabel("UTM Norte (m)")
    ax.add_artist(ScaleBar(1, location="lower right"))
    from matplotlib.patches import Patch
    ax.legend(handles=[
        Patch(color="#2ca02c", alpha=0.4, label="Cobertura"),
        Patch(color="#d62728", alpha=0.4, label="Desierto"),
    ], loc="upper right")
    plt.tight_layout()
    fig.savefig(path, dpi=200)
    plt.close(fig)


def _export_coverage_png(result, comuna, minutes, path):
    gdfs = build_population_coverage_gdfs(result)
    blocks_gdf = gdfs[0]
    centers_gdf = gdfs[1] if len(gdfs) > 1 else None

    blocks_gdf = blocks_gdf.to_crs("EPSG:32719")
    if centers_gdf is not None and len(centers_gdf) > 0:
        centers_gdf = centers_gdf.to_crs("EPSG:32719")

    fig, ax = plt.subplots(figsize=(10, 10))
    blocks_gdf.plot(
        ax=ax, column="coverage_ratio", cmap="Blues",
        legend=True, legend_kwds={"shrink": 0.6},
        edgecolor="grey", linewidth=0.1,
    )
    if centers_gdf is not None:
        centers_gdf.plot(ax=ax, color="red", markersize=30, marker="*")

    ax.set_title(f"Cobertura poblacional — {comuna.replace('_', ' ').title()} ({minutes} min)", fontsize=12)
    ax.set_xlabel("UTM Este (m)")
    ax.set_ylabel("UTM Norte (m)")
    ax.add_artist(ScaleBar(1, location="lower right"))
    plt.tight_layout()
    fig.savefig(path, dpi=200)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description="Pipeline de metricas RM")
    parser.add_argument("--comuna", type=str, default=None,
                        help="Ejecutar solo para una comuna especifica")
    parser.add_argument("--minutes", type=float, default=30)
    parser.add_argument("--skip-plots", action="store_true",
                        help="No generar graficos ni figuras")
    parser.add_argument("--skip-figures", action="store_true",
                        help="No generar figuras del informe")
    args = parser.parse_args()

    ensure_dirs()

    print("=" * 60)
    print("PIPELINE DE METRICAS — Region Metropolitana")
    print(f"minutes: {args.minutes}")
    print(f"skip-plots: {args.skip_plots}")
    print("=" * 60)

    coverage_svc = PopulationCoverageService()
    health_desert_svc = HealthDesertService()

    comunas = [args.comuna] if args.comuna else RM_COMUNAS
    rows = []
    failed = []

    for i, comuna in enumerate(comunas, 1):
        print(f"\n[{i}/{len(comunas)}] {comuna} ...", end=" ", flush=True)
        t0 = time.perf_counter()
        try:
            row = run_comuna(coverage_svc, comuna, args.minutes)
            rows.append(row)
            elapsed = time.perf_counter() - t0
            walk_s = "OK" if row["walk_ok"] else "ERR"
            transit_s = "OK" if row["transit_ok"] else "ERR"
            print(f"walk={walk_s} transit={transit_s} ({elapsed:.1f}s)")
        except Exception as exc:
            print(f"ERROR: {exc}")
            failed.append(comuna)

    print(f"\n--- {len(rows)} comunas procesadas, {len(failed)} fallaron ---")
    if failed:
        print("Fallaron:", failed)

    df = pd.DataFrame(rows)

    table_latex = generate_latex_table(rows)
    consolidated_latex = generate_consolidated_latex(rows)

    with open(OUTPUT_DIR / "tabla_cobertura_rm.tex", "w") as f:
        f.write(table_latex)
    print("OK: tabla_cobertura_rm.tex")

    with open(OUTPUT_DIR / "tabla_cobertura_rm_consolidada.tex", "w") as f:
        f.write(consolidated_latex)
    print("OK: tabla_cobertura_rm_consolidada.tex")

    if not args.skip_plots and len(df) >= 1:
        print("\n--- Generando 8 graficos estadisticos ---")
        plot_functions = [
            ("cobertura_walk_vs_transit", plot_coverage_walk_vs_transit),
            ("ranking_desiertos", plot_ranking_desiertos),
            ("distribucion_cobertura", plot_distribucion_cobertura),
            ("adultos_mayores_walk_vs_transit", plot_adultos_mayores_walk_vs_transit),
            ("delta_brecha_comunas", plot_delta_brecha),
            ("top10_mas_desatendidas", plot_top10_desatendidas),
            ("cobertura_por_modo_hist", plot_cobertura_hist),
            ("scatter_poblacion_vs_cobertura", plot_scatter_poblacion_vs_cobertura),
        ]
        for name, func in plot_functions:
            try:
                func(df)
                print(f"  OK: {name}.png")
            except Exception as e:
                print(f"  ERROR {name}: {e}")

    if not args.skip_figures:
        print("\n--- Generando figuras para informe ---")
        generate_figures_from_results(health_desert_svc, coverage_svc)

    print("\n--- Pipeline completado ---")
    print(f"Tablas LaTeX: {OUTPUT_DIR}/")
    print(f"Graficos:     {GRAFICOS_DIR}/")
    print(f"Figuras:      {FIGURAS_DIR}/")


if __name__ == "__main__":
    main()
