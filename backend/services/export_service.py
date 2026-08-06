import io
import geopandas as gpd
from shapely.geometry import shape
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib_scalebar.scalebar import ScaleBar
from matplotlib.lines import Line2D
import textwrap

def build_population_coverage_gdfs(data: dict):
    features = data.get("features", [])
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
                "geometry": geom,
            })
        elif kind == "health_center":
            center_rows.append({
                **props,
                "geometry": geom,
            })

    if block_rows:
        blocks_gdf = gpd.GeoDataFrame(
            block_rows,
            geometry="geometry",
            crs="EPSG:4326",
        )
    else:
        blocks_gdf = gpd.GeoDataFrame(
            columns=["geometry"],
            geometry="geometry",
            crs="EPSG:4326",
        )

    if center_rows:
        centers_gdf = gpd.GeoDataFrame(
            center_rows,
            geometry="geometry",
            crs="EPSG:4326",
        )
    else:
        centers_gdf = gpd.GeoDataFrame(
            columns=["geometry"],
            geometry="geometry",
            crs="EPSG:4326",
        )

    return blocks_gdf, centers_gdf

class ExportService:
    @staticmethod
    def _add_cartographic_elements(
        ax,
        source="OpenStreetMap",
        crs_text="EPSG:32719",
        elaboration="Elaboración propia"
    ):
        scalebar = ScaleBar(
            dx=1,
            units="m",
            location="lower right",
            box_alpha=0.85,
            scale_loc="bottom"
        )
        ax.add_artist(scalebar)

        ax.annotate(
            "N",
            xy=(0.95, 0.90),
            xytext=(0.95, 0.80),
            xycoords="axes fraction",
            fontsize=16,
            ha="center",
            fontweight="bold",
            arrowprops=dict(
                facecolor="black",
                width=4,
                headwidth=12
            )
        )

        ax.text(
            0.01,
            0.01,
            (
                f"Fuente: {source}\n"
                f"Sistema de referencia: {crs_text}\n"
                f"{elaboration}"
            ),
            transform=ax.transAxes,
            fontsize=8,
            bbox=dict(
                facecolor="white",
                alpha=0.9
            )
        )

    @staticmethod
    def _add_health_desert_legend(ax):
        legend = [
            Line2D([0], [0], marker="s", color="w", label="Cobertura", markerfacecolor="#6a9f58", markersize=14),
            Line2D([0], [0], marker="s", color="w", label="Desierto de salud", markerfacecolor="#f1a2a9", markersize=14),
            Line2D([0], [0], marker="*", color="green", label="Centro de salud", linestyle="None", markersize=14)
        ]
        ax.legend(handles=legend, title="Simbología", loc="upper left", framealpha=0.95)

    @staticmethod
    def _add_population_legend(ax):
        legend = [
            Line2D([0], [0], marker="s", color="w", label="0 - 25 %", markerfacecolor="#deebf7", markersize=14),
            Line2D([0], [0], marker="s", color="w", label="25 - 50 %", markerfacecolor="#9ecae1", markersize=14),
            Line2D([0], [0], marker="s", color="w", label="50 - 75 %", markerfacecolor="#4292c6", markersize=14),
            Line2D([0], [0], marker="s", color="w", label="75 - 100 %", markerfacecolor="#08519c", markersize=14),
            Line2D([0], [0], marker="*", color="green", label="Centro de salud", linestyle="None", markersize=14)
        ]
        ax.legend(handles=legend, title="Simbología", loc="upper left", framealpha=0.95)

    def export_health_desert_map(self, data: dict, minutes: int, comuna: str, mode: str = "caminando") -> bytes:
        features = data.get("features", [])
        if not features:
            raise ValueError("No hay datos para exportar")

        geoms = [shape(f["geometry"]) for f in features]
        deserts_gdf = gpd.GeoDataFrame(
            [f.get("properties", {}) for f in features],
            geometry=geoms,
            crs="EPSG:4326"
        )

        try:
            deserts_gdf = deserts_gdf.to_crs(epsg=32719)
        except Exception:
            pass

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

        self._add_health_desert_legend(ax)

        title = f"Desiertos de salud en {comuna} ({mode}) - {minutes} minutos"
        ax.set_title(textwrap.fill(title, width=50), fontsize=14, fontweight="bold")
        ax.set_axis_off()
        self._add_cartographic_elements(ax, "OpenStreetMap, DTPM, Censo 2024, MINSAL")

        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=300, bbox_inches="tight", facecolor="white")
        buf.seek(0)
        plt.close(fig)
        return buf.getvalue()

    def export_population_coverage_map(
        self,
        data: dict,
        minutes: int,
        title: str = "Cobertura poblacional",
        comuna: str = None,
        include_cartographic_elements: bool = True
    ) -> bytes:
        blocks_gdf, centers_gdf = build_population_coverage_gdfs(data)

        if blocks_gdf.empty:
            raise ValueError("No hay manzanas para exportar")

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
            edgecolor="#5b6b7a",
            legend=False
        )

        if not centers_gdf.empty:
            centers_gdf.plot(
                ax=ax,
                color="green",
                markersize=150,
                marker="*",
                label="Centro de salud"
            )

        self._add_population_legend(ax)
        if comuna:
            title = f"{title} - {comuna}"

        ax.set_title(f"{title} ({minutes} minutos)", fontsize=14, fontweight="bold")
        ax.set_axis_off()

        if include_cartographic_elements:
            self._add_cartographic_elements(ax, "OpenStreetMap, DTPM, Censo 2024, MINSAL")

        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=300, bbox_inches="tight", facecolor="white")
        buf.seek(0)
        plt.close(fig)
        return buf.getvalue()
