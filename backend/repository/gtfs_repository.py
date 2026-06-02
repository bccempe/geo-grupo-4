import os
import math
import networkx as nx
import pandas as pd

from pathlib import Path
from sqlalchemy import inspect, text

from app.db.database import engine
from utils.comuna_util import normalize_to_slug


# ==========================================================
# PATHS
# ==========================================================

_REPO_DIR = Path(__file__).parent

_DEFAULT_GTFS = str(
    _REPO_DIR.parent / "data" / "GTFS_20260321_v3"
)

GTFS_DIR = os.getenv(
    "GTFS_DATA_PATH",
    _DEFAULT_GTFS
)

if not Path(GTFS_DIR).exists():
    GTFS_DIR = _DEFAULT_GTFS


# ==========================================================
# CONFIG
# ==========================================================

WALK_SPEED_M_S = 1.2


HEALTH_TABLE = os.getenv("HEALTH_TABLE", "establecimientos_de_salud_chile_rm_establecimientos_de_492581f8")
HEALTH_GEOM_COL = os.getenv("HEALTH_GEOM_COL", "geom")
HEALTH_COMUNA_COL = os.getenv("HEALTH_COMUNA_COL", "nom_comuna")


# ==========================================================
# UTILS
# ==========================================================

def haversine(
    lat1,
    lon1,
    lat2,
    lon2
):

    R = 6371000

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)

    a = (
        math.sin(
            math.radians(lat2 - lat1) / 2
        ) ** 2
        +
        math.cos(phi1)
        *
        math.cos(phi2)
        *
        math.sin(
            math.radians(lon2 - lon1) / 2
        ) ** 2
    )

    return (
        R
        * 2
        * math.atan2(
            math.sqrt(a),
            math.sqrt(1 - a)
        )
    )


def parse_time(t):

    if pd.isna(t):
        return None

    try:

        parts = str(t).split(":")

        return (
            int(parts[0]) * 3600
            +
            int(parts[1]) * 60
            +
            int(parts[2].split(".")[0])
        )

    except Exception:

        return None


# ==========================================================
# REPOSITORY
# ==========================================================

class GTFSRepository:

    def __init__(self):

        self._cache = {}

        self._stops = None
        self._stop_times = None
        self._trips = None
        self._frequencies = None
        self._routes = None

        self._health_cache = None

    def _get_table_columns(self, table_name: str) -> set[str]:
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        return {column["name"] for column in columns}

    def _pick_column(
        self,
        available_columns: set[str],
        candidates: list[str],
        required: bool = True
    ) -> str | None:
        for candidate in candidates:
            if candidate in available_columns:
                return candidate

        if required:
            raise ValueError(
                f"No se encontró ninguna de estas columnas: {candidates}"
            )

        return None

    # ======================================================
    # STOPS
    # ======================================================

    def load_stops(self):

        if self._stops is not None:
            return self._stops

        df = pd.read_csv(
            Path(GTFS_DIR) / "stops.txt"
        )

        self._stops = df

        return df

    # ======================================================
    # STOP TIMES
    # ======================================================

    def load_stop_times(self):

        if self._stop_times is not None:
            return self._stop_times

        df = pd.read_csv(
            Path(GTFS_DIR) / "stop_times.txt"
        )

        df["arrival_sec"] = (
            df["arrival_time"]
            .apply(parse_time)
        )

        df["departure_sec"] = (
            df["departure_time"]
            .apply(parse_time)
        )

        self._stop_times = df

        return df

    # ======================================================
    # TRIPS
    # ======================================================

    def load_trips(self):

        if self._trips is not None:
            return self._trips

        df = pd.read_csv(
            Path(GTFS_DIR) / "trips.txt"
        )

        self._trips = df

        return df

    # ======================================================
    # FREQUENCIES
    # ======================================================

    def load_frequencies(self):

        if self._frequencies is not None:
            return self._frequencies

        df = pd.read_csv(
            Path(GTFS_DIR) / "frequencies.txt"
        )

        self._frequencies = df

        return df

    # ======================================================
    # ROUTES
    # ======================================================

    def load_routes(self):

        if self._routes is not None:
            return self._routes

        df = pd.read_csv(
            Path(GTFS_DIR) / "routes.txt"
        )

        self._routes = df

        return df

    # ======================================================
    # BUILD GRAPH
    # ======================================================

    def build_travel_graph(
        self,
        service_ids=None
    ):

        cache_key = (
            tuple(sorted(service_ids))
            if service_ids
            else "all"
        )

        if cache_key in self._cache:
            return self._cache[cache_key]

        stop_times = self.load_stop_times()

        trips = self.load_trips()

        valid_times = stop_times[
            stop_times["arrival_sec"].notna()
            &
            stop_times["departure_sec"].notna()
        ].copy()

        if service_ids:

            valid_trips = trips[
                trips["service_id"].isin(service_ids)
            ]

            valid_trip_ids = set(
                valid_trips["trip_id"]
            )

            valid_times = valid_times[
                valid_times["trip_id"].isin(
                    valid_trip_ids
                )
            ]

        valid_times = valid_times.sort_values(
            ["trip_id", "stop_sequence"]
        )

        graph = nx.DiGraph()

        for trip_id, group in valid_times.groupby(
            "trip_id",
            sort=False
        ):

            group = group.sort_values(
                "stop_sequence"
            )

            stops_in_trip = (
                group["stop_id"].values
            )

            arrivals = (
                group["arrival_sec"].values
            )

            departures = (
                group["departure_sec"].values
            )

            for i in range(
                len(stops_in_trip)
            ):

                s = stops_in_trip[i]

                if not graph.has_node(s):

                    graph.add_node(s)

            for i in range(
                len(stops_in_trip) - 1
            ):

                u = stops_in_trip[i]

                v = stops_in_trip[i + 1]

                travel_time = (
                    arrivals[i + 1]
                    -
                    departures[i]
                )

                if 10 < travel_time < 7200:

                    if graph.has_edge(u, v):

                        existing = graph[u][v]["time"]

                        if travel_time < existing:

                            graph[u][v]["time"] = (
                                travel_time
                            )

                            graph[u][v]["count"] = 1

                    else:

                        graph.add_edge(
                            u,
                            v,
                            time=travel_time,
                            count=1
                        )

        self._cache[cache_key] = graph

        return graph

    # ======================================================
    # NEARBY STOPS
    # ======================================================

    def find_nearby_stops(
        self,
        lat,
        lon,
        max_dist_m=800
    ):

        stops = self.load_stops()

        nearby = []

        for _, row in stops.iterrows():

            d = haversine(
                lat,
                lon,
                row["stop_lat"],
                row["stop_lon"]
            )

            if d <= max_dist_m:

                walk_time_min = (
                    d
                    /
                    WALK_SPEED_M_S
                    /
                    60
                )

                nearby.append({

                    "stop_id": row["stop_id"],

                    "lat": float(
                        row["stop_lat"]
                    ),

                    "lon": float(
                        row["stop_lon"]
                    ),

                    "distance_m": d,

                    "walk_time_min": walk_time_min
                })

        return sorted(
            nearby,
            key=lambda x: x["distance_m"]
        )

    # ======================================================
    # HEADWAY
    # ======================================================

    def get_headway_at_hour(
        self,
        trip_id,
        hour
    ):

        frequencies = self.load_frequencies()

        matching = frequencies[
            (
                frequencies["trip_id"]
                == trip_id
            )
            &
            (
                frequencies["start_time"]
                .apply(parse_time)
                <= hour * 3600
            )
            &
            (
                frequencies["end_time"]
                .apply(parse_time)
                >= hour * 3600
            )
        ]

        if len(matching) > 0:

            return int(
                matching["headway_secs"]
                .iloc[0]
            )

        return 1200

    # ======================================================
    # HEALTH CENTERS FROM POSTGIS
    # ======================================================

    def load_health_centers(self, region_filter="13"):

        if self._health_cache is not None:
            return self._health_cache

        columns = self._get_table_columns(HEALTH_TABLE)

        geom_col = self._pick_column(
            columns,
            ["geom", "geometry", "the_geom", "wkb_geometry"]
        )

        comuna_col = self._pick_column(
            columns,
            ["comuna", "COMUNA", "NOM_COMUNA", "nom_comuna"],
            required=False
        )

        id_col = self._pick_column(
            columns,
            ["id_orig", "ID_ORIG", "id"],
            required=False
        )

        nombre_col = self._pick_column(
            columns,
            ["nombre", "NOMBRE"],
            required=False
        )

        tipo_col = self._pick_column(
            columns,
            ["tipo", "TIPO"],
            required=False
        )

        region_col = self._pick_column(
            columns,
            ["cut_region", "CUT_REGION", "region", "REGION"],
            required=False
        )

        select_parts = []

        if id_col:
            select_parts.append(f'"{id_col}" AS id')
        else:
            select_parts.append("NULL AS id")

        if nombre_col:
            select_parts.append(f'"{nombre_col}" AS nombre')
        else:
            select_parts.append("NULL AS nombre")

        if tipo_col:
            select_parts.append(f'"{tipo_col}" AS tipo')
        else:
            select_parts.append("NULL AS tipo")

        if comuna_col:
            select_parts.append(f'"{comuna_col}" AS comuna')
        else:
            select_parts.append("NULL AS comuna")

        select_parts.append(
            f'ST_X(ST_Centroid("{geom_col}")) AS lng'
        )

        select_parts.append(
            f'ST_Y(ST_Centroid("{geom_col}")) AS lat'
        )

        where_clauses = [
            f'"{geom_col}" IS NOT NULL'
        ]

        params = {}

        if region_filter is not None and region_col is not None:
            where_clauses.append(
                f'CAST("{region_col}" AS TEXT) = :region_filter'
            )
            params["region_filter"] = str(region_filter)

        query = text(f"""
            SELECT
                {", ".join(select_parts)}
            FROM "{HEALTH_TABLE}"
            WHERE {" AND ".join(where_clauses)}
        """)

        with engine.connect() as conn:
            rows = conn.execute(
                query,
                params
            ).mappings().all()

        centers = []

        for row in rows:

            comuna_original = str(
                row.get(
                    "comuna",
                    ""
                )
            )

            comuna_slug = normalize_to_slug(
                comuna_original
            )

            centers.append({

                "id": str(
                    row.get("id", "")
                ),

                "nombre": str(
                    row.get("nombre", "")
                ),

                "tipo": str(
                    row.get("tipo", "")
                ),

                "comuna": comuna_original,

                "comuna_slug": comuna_slug,

                "lat": float(
                    row["lat"]
                ),

                "lng": float(
                    row["lng"]
                )
            })

        self._health_cache = centers

        return centers

    def load_primary_care_centers(self, region_filter="13"):
        return self.load_health_centers(region_filter)

    def load_primary_care_centers_from_db(self, region_filter="13"):
        return self.load_health_centers(region_filter)

    # ======================================================
    # FILTER BY COMUNA
    # ======================================================

    def get_centers_by_comuna(
        self,
        comuna
    ):

        comuna_slug = normalize_to_slug(
            comuna
        )

        centers = self.load_health_centers()

        return [
            c for c in centers
            if c["comuna_slug"] == comuna_slug
        ]