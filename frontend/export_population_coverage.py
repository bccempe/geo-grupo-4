import io

import geopandas as gpd
import matplotlib.pyplot as plt

from shapely.geometry import shape
from matplotlib_scalebar.scalebar import ScaleBar


def export_population_coverage_map(
    data: dict,
    minutes: int,
    title: str = "Cobertura Poblacional"
):
    features = data.get("features", [])

    if not features:
        return None

    block_rows = []
    center_rows = []

    for feature in features:
        props = feature.get("properties", {})
        kind = props.get("kind")
        geom_data = feature.get("geometry")

        if not geom_data:
            continue

        geom = shape(geom_data)

        if kind == "census_block":
            block_rows.append({
                **props,
                "geometry": geom
            })

        elif kind == "health_center":
            center_rows.append({
                **props,
                "geometry": geom
            })

    if not block_rows:
        return None

    blocks_gdf = gpd.GeoDataFrame(
        block_rows,
        geometry="geometry",
        crs="EPSG:4326"
    )

    if center_rows:
        centers_gdf = gpd.GeoDataFrame(
            center_rows,
            geometry="geometry",
            crs="EPSG:4326"
        )
    else:
        centers_gdf = gpd.GeoDataFrame(
            columns=["geometry"],
            geometry="geometry",
            crs="EPSG:4326"
        )

    try:
        blocks_gdf = blocks_gdf.to_crs(epsg=32719)
    except Exception:
        pass

    if not centers_gdf.empty:
        try:
            centers_gdf = centers_gdf.to_crs(epsg=32719)
        except Exception:
            pass

    for col in [
        "population",
        "covered_population",
        "elderly_population",
        "covered_elderly_population",
        "coverage_ratio"
    ]:
        if col not in blocks_gdf.columns:
            blocks_gdf[col] = 0

    total_population = blocks_gdf["population"].fillna(0).sum()
    covered_population = blocks_gdf["covered_population"].fillna(0).sum()
    elderly_population = blocks_gdf["elderly_population"].fillna(0).sum()
    covered_elderly = blocks_gdf["covered_elderly_population"].fillna(0).sum()

    coverage_pct = 0
    if total_population > 0:
        coverage_pct = (covered_population / total_population) * 100

    fig, ax = plt.subplots(figsize=(16, 16))

    # =====================
    # SIN COBERTURA
    # =====================
    uncovered = blocks_gdf[blocks_gdf["coverage_ratio"] == 0]
    if not uncovered.empty:
        uncovered.plot(
            ax=ax,
            color="#fecaca",
            edgecolor="none",
            label="Sin cobertura"
        )

    # =====================
    # COBERTURA PARCIAL
    # =====================
    partial = blocks_gdf[
        (blocks_gdf["coverage_ratio"] > 0) &
        (blocks_gdf["coverage_ratio"] < 1)
    ]
    if not partial.empty:
        partial.plot(
            ax=ax,
            color="#fde68a",
            edgecolor="none",
            label="Cobertura parcial"
        )

    # =====================
    # COBERTURA TOTAL
    # =====================
    covered = blocks_gdf[blocks_gdf["coverage_ratio"] >= 1]
    if not covered.empty:
        covered.plot(
            ax=ax,
            color="#bbf7d0",
            edgecolor="none",
            label="Cobertura total"
        )

    # =====================
    # CENTROS DE SALUD
    # =====================
    if not centers_gdf.empty:
        centers_gdf.plot(
            ax=ax,
            marker="*",
            markersize=150,
            color="#2563eb",
            label="Centro de salud"
        )

    # =====================
    # NORTE
    # =====================
    ax.annotate(
        "N",
        xy=(0.95, 0.90),
        xytext=(0.95, 0.80),
        xycoords="axes fraction",
        fontsize=18,
        ha="center",
        fontweight="bold",
        arrowprops=dict(
            facecolor="black",
            width=4,
            headwidth=14
        )
    )

    # =====================
    # ESCALA
    # =====================
    scalebar = ScaleBar(
        1,
        units="m",
        location="lower right"
    )
    ax.add_artist(scalebar)

    # =====================
    # TITULO
    # =====================
    ax.set_title(
        f"{title}\nCobertura poblacional ({minutes} minutos)",
        fontsize=18,
        fontweight="bold"
    )

    # =====================
    # RESUMEN
    # =====================
    stats_text = (
        f"Población total: {int(total_population):,}\n"
        f"Población cubierta: {int(covered_population):,}\n"
        f"Cobertura: {coverage_pct:.1f}%\n"
        f"Adultos mayores: {int(elderly_population):,}\n"
        f"Adultos mayores cubiertos: {int(covered_elderly):,}"
    )

    ax.text(
        0.02,
        0.98,
        stats_text,
        transform=ax.transAxes,
        verticalalignment="top",
        bbox=dict(
            facecolor="white",
            alpha=0.9
        )
    )

    # =====================
    # LEYENDA
    # =====================
    ax.legend(loc="lower left")

    # =====================
    # FUENTE
    # =====================
    ax.text(
        0.01,
        0.01,
        "Fuente: Censo 2024, OpenStreetMap, MINSAL",
        transform=ax.transAxes,
        fontsize=8,
        bbox=dict(
            facecolor="white",
            alpha=0.8
        )
    )

    ax.set_axis_off()

    buf = io.BytesIO()
    plt.savefig(
        buf,
        format="png",
        dpi=300,
        bbox_inches="tight",
        facecolor="white"
    )
    buf.seek(0)
    plt.close(fig)

    return buf