import os
import math
import networkx as nx
import pandas as pd
from pathlib import Path

_REPO_DIR = Path(__file__).parent
_DEFAULT_GTFS = str(_REPO_DIR.parent / "data" / "GTFS_20260321_v3")
GTFS_DIR = os.getenv("GTFS_DATA_PATH", _DEFAULT_GTFS)
if not Path(GTFS_DIR).exists():
    GTFS_DIR = _DEFAULT_GTFS

WALK_SPEED_M_S = 1.2

TIPOS_PRIMARIA = [
    "Centro de Salud Familiar (CESFAM)",
    "Centro Comunitario de Salud Familiar (CECOSF)",
    "Servicio de Atención Primaria de Urgencia (SAPU)",
    "Servicio de Atención Primaria de Urgencia de Alta Resolutividad (SAR)",
    "Posta de Salud Rural (PSR)"
]


def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    a = math.sin(math.radians(lat2 - lat1) / 2) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * \
        math.sin(math.radians(lon2 - lon1) / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def parse_time(t):
    if pd.isna(t):
        return None
    try:
        parts = str(t).split(":")
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2].split(".")[0])
    except Exception:
        return None


def es_primaria(tipo):
    if pd.isna(tipo):
        return False
    return any(p in str(tipo) for p in TIPOS_PRIMARIA)


class GTFSRepository:

    def __init__(self):
        self._cache = {}
        self._stops = None
        self._stop_times = None
        self._trips = None
        self._frequencies = None
        self._routes = None

    def load_stops(self):
        if self._stops is not None:
            return self._stops
        df = pd.read_csv(Path(GTFS_DIR) / "stops.txt")
        self._stops = df
        return df

    def load_stop_times(self):
        if self._stop_times is not None:
            return self._stop_times
        df = pd.read_csv(Path(GTFS_DIR) / "stop_times.txt")
        df["arrival_sec"] = df["arrival_time"].apply(parse_time)
        df["departure_sec"] = df["departure_time"].apply(parse_time)
        self._stop_times = df
        return df

    def load_trips(self):
        if self._trips is not None:
            return self._trips
        df = pd.read_csv(Path(GTFS_DIR) / "trips.txt")
        self._trips = df
        return df

    def load_frequencies(self):
        if self._frequencies is not None:
            return self._frequencies
        df = pd.read_csv(Path(GTFS_DIR) / "frequencies.txt")
        self._frequencies = df
        return df

    def load_routes(self):
        if self._routes is not None:
            return self._routes
        df = pd.read_csv(Path(GTFS_DIR) / "routes.txt")
        self._routes = df
        return df

    def build_travel_graph(self, service_ids=None):
        cache_key = tuple(sorted(service_ids)) if service_ids else "all"
        if cache_key in self._cache:
            return self._cache[cache_key]

        stop_times = self.load_stop_times()
        trips = self.load_trips()

        valid_times = stop_times[stop_times["arrival_sec"].notna() & stop_times["departure_sec"].notna()].copy()

        if service_ids:
            valid_trips = trips[trips["service_id"].isin(service_ids)]
            valid_trip_ids = set(valid_trips["trip_id"])
            valid_times = valid_times[valid_times["trip_id"].isin(valid_trip_ids)]

        valid_times = valid_times.sort_values(["trip_id", "stop_sequence"])

        graph = nx.DiGraph()

        for trip_id, group in valid_times.groupby("trip_id", sort=False):
            group = group.sort_values("stop_sequence")
            stops_in_trip = group["stop_id"].values
            arrivals = group["arrival_sec"].values
            departures = group["departure_sec"].values

            for i in range(len(stops_in_trip)):
                s = stops_in_trip[i]
                if not graph.has_node(s):
                    graph.add_node(s)

            for i in range(len(stops_in_trip) - 1):
                u = stops_in_trip[i]
                v = stops_in_trip[i + 1]
                travel_time = arrivals[i + 1] - departures[i]
                if 10 < travel_time < 7200:
                    if graph.has_edge(u, v):
                        existing = graph[u][v]["time"]
                        if travel_time < existing:
                            graph[u][v]["time"] = travel_time
                            graph[u][v]["count"] = 1
                    else:
                        graph.add_edge(u, v, time=travel_time, count=1)

        self._cache[cache_key] = graph
        return graph

    def find_nearby_stops(self, lat, lon, max_dist_m=800):
        stops = self.load_stops()
        nearby = []
        for _, row in stops.iterrows():
            d = haversine(lat, lon, row["stop_lat"], row["stop_lon"])
            if d <= max_dist_m:
                walk_time_min = d / WALK_SPEED_M_S / 60
                nearby.append({
                    "stop_id": row["stop_id"],
                    "lat": float(row["stop_lat"]),
                    "lon": float(row["stop_lon"]),
                    "distance_m": d,
                    "walk_time_min": walk_time_min
                })
        return sorted(nearby, key=lambda x: x["distance_m"])

    def get_headway_at_hour(self, trip_id, hour):
        frequencies = self.load_frequencies()
        matching = frequencies[
            (frequencies["trip_id"] == trip_id) &
            (frequencies["start_time"].apply(parse_time) <= hour * 3600) &
            (frequencies["end_time"].apply(parse_time) >= hour * 3600)
        ]
        if len(matching) > 0:
            return int(matching["headway_secs"].iloc[0])
        return 1200

    def load_primary_care_centers(self, region_filter="13"):
        shp_dir = os.getenv("MINSAL_DATA_PATH", str(_REPO_DIR.parent / "data" / "datos_minsal_establecimientos_salud"))
        if not Path(shp_dir).exists():
            shp_dir = str(_REPO_DIR.parent / "data" / "datos_minsal_establecimientos_salud")
        shp_files = list(Path(shp_dir).glob("*.shp"))
        if not shp_files:
            raise FileNotFoundError(f"No se encontró archivo shapefile en {shp_dir}")

        import geopandas as gpd
        gdf = gpd.read_file(shp_files[0])
        gdf = gdf[gdf["TIPO"].apply(es_primaria)].copy()
        gdf = gdf[gdf["LATITUD"].notna() & gdf["LONGITUD"].notna()].copy()

        if region_filter:
            gdf = gdf[gdf["CUT_REGION"].astype(str) == str(region_filter)].copy()

        centers = []
        for _, row in gdf.iterrows():
            centers.append({
                "id_orig": str(row.get("ID_ORIG", "")),
                "nombre": str(row.get("NOMBRE", "")),
                "tipo": str(row.get("TIPO", "")),
                "comuna": str(row.get("NOM_COMUNA", "")),
                "lat": float(row["LATITUD"]),
                "lng": float(row["LONGITUD"]),
                "cut_comuna": str(row.get("CUT_COMUNA", "")),
            })
        return centers

    def get_centers_by_comuna(self, comuna):
        centers = self.load_primary_care_centers()
        return [c for c in centers if c["comuna"].upper() == comuna.upper()]

    def load_primary_care_centers_from_db(self, region_filter="13"):
        from app.db.database import engine
        query = """
            SELECT id_orig, nombre, tipo, comuna,
                   ST_X(geometry) AS lng,
                   ST_Y(geometry) AS lat
            FROM salud_primaria
        """
        try:
            df = pd.read_sql(query, engine)
        except Exception:
            return self.load_primary_care_centers(region_filter)

        centers = []
        for _, row in df.iterrows():
            centers.append({
                "id_orig": str(row["id_orig"]),
                "nombre": str(row["nombre"]),
                "tipo": str(row["tipo"]),
                "comuna": str(row["comuna"]).upper(),
                "lat": float(row["lat"]),
                "lng": float(row["lng"]),
            })
        return centers
