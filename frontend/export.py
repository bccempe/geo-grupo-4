import io
import geopandas as gpd
from shapely.geometry import shape
import matplotlib.pyplot as plt
from matplotlib_scalebar.scalebar import ScaleBar
import streamlit as st
import pandas as pd
import textwrap

from ui_components import build_population_coverage_gdfs

def _add_cartographic_elements(ax, source="OpenStreetMap", crs_text="EPSG:32719", elaboration="Elab.: Grupo 4"):
    scalebar = ScaleBar(1, units="m", location="lower right", box_alpha=0.6, border_pad=2)
    ax.add_artist(scalebar)

    ax.annotate(
        "N",
        xy=(0.95, 0.85),
        xytext=(0.95, 0.78),
        xycoords="axes fraction",
        textcoords="axes fraction",
        fontsize=14,
        fontweight="bold",
        ha="center",
        va="center",
        arrowprops=dict(facecolor="black", width=4, headwidth=12)
    )

    ax.text(
        0.02,
        0.02,
        f"Fuente: {source} | CRS: {crs_text} | {elaboration}",
        transform=ax.transAxes,
        fontsize=8,
        bbox=dict(boxstyle="round", facecolor="white", alpha=0.7)
    )

def export_health_desert_map(data: dict, minutes: int, comuna: str, mode: str):
    """Exporta desierto de salud como PNG estilizado"""
    features = data.get("features", [])
    if not features:
        st.error("No hay datos para exportar")
        return None

    geoms = [shape(f["geometry"]) for f in features]
    deserts_gdf = gpd.GeoDataFrame(
        [f.get("properties", {}) for f in features],
        geometry=geoms,
        crs="EPSG:4326"
    )

    try:
        deserts_gdf = deserts_gdf.to_crs(epsg=32719)
    except Exception:
        st.warning("No se pudo reproyectar la geometría para el scalebar.")

    coverage = deserts_gdf[deserts_gdf["kind"] == "coverage"]
    health_desert = deserts_gdf[deserts_gdf["kind"] == "health_desert"]
    points = deserts_gdf[deserts_gdf["kind"] == "health_center"]

    fig, ax = plt.subplots(figsize=(12, 10))

    if not coverage.empty:
        coverage.plot(ax=ax, alpha=0.6, edgecolor="#85b6b2", color="#6a9f58")

    if not health_desert.empty:
        health_desert.plot(ax=ax, alpha=0.6, edgecolor="#d1615d", color="#f1a2a9")

    if not points.empty:
        points.plot(ax=ax, color="green", markersize=100, marker="*")

    title = f"Desiertos de salud en {comuna} ({mode}) - {minutes} minutos"
    ax.set_title(textwrap.fill(title, width=50), fontsize=14, fontweight="bold")

    ax.set_axis_off()
    _add_cartographic_elements(ax, "OpenStreetMap, DTPM, Censo 2024, MINSAL")

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=300, bbox_inches="tight", facecolor="white")
    buf.seek(0)
    plt.close(fig)
    return buf

def export_population_coverage_map(
    data: dict,
    minutes: int,
    title: str = "Cobertura poblacional",
    comuna: str | None = None,
    include_cartographic_elements: bool = False,
):
    blocks_gdf, centers_gdf = build_population_coverage_gdfs(data)

    if blocks_gdf.empty:
        return None

    try:
        blocks_gdf = blocks_gdf.to_crs(epsg=32719)
        centers_gdf = centers_gdf.to_crs(epsg=32719)
    except Exception:
        pass

    fig, ax = plt.subplots(figsize=(16, 16))

    blocks_gdf["coverage_pct"] = (
        blocks_gdf["coverage_ratio"].fillna(0) * 100
    ).clip(0, 100)

    blocks_gdf.plot(
        ax=ax,
        column="coverage_pct",
        cmap="Blues",
        scheme="NaturalBreaks",
        k=4,
        edgecolor="none",
        legend=True,
        legend_kwds={
            "title": "Nivel de cobertura (%)",
            "loc": "upper left",
            "fontsize": 9,
        },
    )

    if not centers_gdf.empty:
        centers_gdf.plot(
            ax=ax,
            color="#2563eb",
            markersize=150,
            marker="*",
            label="Centro de salud",
        )

    if comuna:
        title = f"{title} - {comuna}"

    ax.set_title(f"{title} ({minutes} minutos)", fontsize=14, fontweight="bold")
    ax.set_axis_off()

    if include_cartographic_elements:
        _add_cartographic_elements(ax, "OpenStreetMap, DTPM, Censo 2024, MINSAL")

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=300, bbox_inches="tight", facecolor="white")
    buf.seek(0)
    plt.close(fig)
    return buf

def export_full_population_coverage_map(data: dict, minutes: int):
    return export_population_coverage_map(
        data,
        minutes,
        title="Cobertura poblacional",
        comuna="Región Metropolitana",
        include_cartographic_elements=False,
    )