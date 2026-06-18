import os
import requests
import streamlit as st
import folium
from streamlit_folium import st_folium
from shapely.geometry import shape

API_URL = os.getenv("API_URL", "http://localhost:8000")


def render_location_picker(
    map_key: str,
    origin_key: str = "iso_origin",
    default=(-33.46803, -70.67045)
):
    if origin_key not in st.session_state:
        st.session_state[origin_key] = {"lat": default[0], "lon": default[1]}

    m = folium.Map(
        location=[st.session_state[origin_key]["lat"], st.session_state[origin_key]["lon"]],
        zoom_start=14,
        height=300
    )

    folium.Marker(
        [st.session_state[origin_key]["lat"], st.session_state[origin_key]["lon"]],
        popup="Origen",
        icon=folium.Icon(color="red", icon="info-sign")
    ).add_to(m)

    map_data = st_folium(m, width="100%", height=300, key=map_key)

    if map_data and map_data.get("last_clicked"):
        st.session_state[origin_key] = {
            "lat": map_data["last_clicked"]["lat"],
            "lon": map_data["last_clicked"]["lng"]
        }

    lat = st.session_state[origin_key]["lat"]
    lon = st.session_state[origin_key]["lon"]
    return lat, lon


def call_isochrone_api(
    endpoint: str,
    lat: float,
    lon: float,
    minutes: int,
    include_centers: bool = True,
    api_url: str = None,
    spinner_text: str = "Calculando...",
):
    api = api_url or API_URL
    url = f"{api}{endpoint}"
    try:
        with st.spinner(spinner_text):
            resp = requests.get(
                url,
                params={
                    "lat": lat,
                    "lon": lon,
                    "minutes": minutes,
                    "include_centers": include_centers,
                },
                timeout=1800,
            )
        if resp.status_code != 200:
            try:
                st.error(resp.json().get("detail", "Error desconocido"))
            except Exception:
                st.error(resp.text)
            return None
        return resp.json()
    except requests.exceptions.ConnectionError:
        st.error(f"No se pudo conectar a la API en {api}")
        return None
    except Exception as e:
        st.error(f"Error: {e}")
        return None


def calculate_isochrone_geometry(iso_feature: dict):
    coords = iso_feature.get("geometry", {}).get("coordinates", [])
    geom_type = iso_feature.get("geometry", {}).get("type")
    if not coords:
        return None, None, None, geom_type
    if geom_type == "MultiPolygon":
        bounds = coords[0][0]
    else:
        bounds = coords[0]
    lats = [p[1] for p in bounds]
    lons = [p[0] for p in bounds]
    center_lat = sum(lats) / len(lats)
    center_lon = sum(lons) / len(lons)
    return bounds, lats, lons, (center_lat, center_lon, geom_type)


def render_isochrone_map(
    iso_feature: dict,
    origin_feature: dict,
    health_centers: list,
    center_lat: float,
    center_lon: float,
    minutes: int,
    map_key: str
):
    m = folium.Map(location=[center_lat, center_lon], zoom_start=12, tiles="cartodbpositron")

    folium.GeoJson(
        iso_feature,
        name="Isócrona",
        style_function=lambda x: {
            "fillColor": "#93c5fd",
            "color": "#2563eb",
            "weight": 2,
            "fillOpacity": 0.35,
        },
        tooltip=f"{minutes} min",
    ).add_to(m)

    folium.Marker(
        [origin_feature["geometry"]["coordinates"][1], origin_feature["geometry"]["coordinates"][0]],
        popup="Origen",
        icon=folium.Icon(color="red", icon="info-sign"),
    ).add_to(m)

    for center in health_centers:
        folium.Marker(
            [center["geometry"]["coordinates"][1], center["geometry"]["coordinates"][0]],
            popup=center["properties"].get("name", center["properties"].get("nombre", "Centro de salud")),
            icon=folium.Icon(color="green", icon="heart"),
        ).add_to(m)

    st_folium(m, width="100%", height=500, key=map_key)


def process_isochrone_result(result: dict, minutes: int, mode: str, map_key: str):
    if not result:
        return

    features = result.get("features", [])
    health_centers = []

    if len(features) >= 2:
        iso_feature = features[0]
        origin_feature = features[1]

        for f in features[2:]:
            if f.get("geometry", {}).get("type") == "Point" and f.get("properties", {}).get("kind") == "health_center":
                health_centers.append(f)

        coords = iso_feature.get("geometry", {}).get("coordinates", [])
        if not coords:
            st.warning("No se pudo calcular la isócrona (sin geometría)")
            return

        geom_type = iso_feature.get("geometry", {}).get("type")
        if geom_type in ("Point", "LineString"):
            st.warning("El área calculada es muy pequeña para mostrarse en el mapa")
            return

        _, _, _, center_info = calculate_isochrone_geometry(iso_feature)
        if not center_info:
            st.warning("No hay geometría válida")
            return

        center_lat, center_lon, _ = center_info
        render_isochrone_map(
            iso_feature,
            origin_feature,
            health_centers,
            center_lat,
            center_lon,
            minutes,
            map_key,
        )
    else:
        st.warning("No se pudo calcular la isócrona (sin datos de origen)")
        return


def call_health_desert_api(
    endpoint: str,
    comuna: str,
    minutes: int,
    api_url: str = None,
    spinner_text: str = "Calculando desiertos...",
):
    api = api_url or API_URL
    url = f"{api}{endpoint}"
    try:
        with st.spinner(spinner_text):
            resp = requests.get(
                url,
                params={
                    "comuna": comuna,
                    "minutes": minutes,
                },
                timeout=1800,
            )
        if resp.status_code != 200:
            try:
                st.error(resp.json().get("detail", "Error desconocido"))
            except Exception:
                st.error(resp.text)
            return None
        return resp.json()
    except requests.exceptions.ConnectionError:
        st.error(f"No se pudo conectar a la API en {api}")
        return None
    except Exception as e:
        st.error(f"Error: {e}")
        return None


def render_health_deserts(data: dict, map_key: str = "desert_transit"):
    features = data.get("features", [])
    meta = data.get("metadata", {})

    if not features:
        st.warning("No hay desiertos para mostrar")
        return

    m = folium.Map(location=[-33.45, -70.65], zoom_start=11, tiles="cartodbpositron")

    for feature in features:
        kind = feature.get("properties", {}).get("kind", "")
        style = {}
        tooltip = ""

        if kind == "coverage":
            style = {
                "fillColor": "#bbf7d0",
                "color": "#22c55e",
                "weight": 2,
                "fillOpacity": 0.45,
            }
            tooltip = "Cobertura"

        elif kind == "health_desert":
            style = {
                "fillColor": "#fecaca",
                "color": "#ef4444",
                "weight": 2,
                "fillOpacity": 0.45,
            }
            tooltip = "Desierto de salud"

        elif kind == "health_center":
            folium.Marker(
                [feature["geometry"]["coordinates"][1], feature["geometry"]["coordinates"][0]],
                popup=feature["properties"].get("name", feature["properties"].get("nombre", "Centro de salud")),
                icon=folium.Icon(color="green", icon="heart"),
            ).add_to(m)
            continue

        if style:
            folium.GeoJson(
                feature,
                name=tooltip,
                style_function=lambda x, s=style: s,
                tooltip=tooltip,
            ).add_to(m)

    folium.Marker(
        [-33.45, -70.65],
        icon=folium.DivIcon(
            html="""
            <div style="
                position: fixed;
                top: 15px;
                right: 15px;
                z-index:9999;
                font-size:28px;
                font-weight:bold;
                background:white;
                padding:8px 10px;
                border-radius:8px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.15);
            ">↑ N</div>
            """
        ),
    ).add_to(m)

    legend = """
    <div style="
        position: fixed;
        bottom: 45px;
        left: 45px;
        width: 250px;
        background-color: white;
        z-index:9999;
        padding: 10px 12px;
        border-radius: 10px;
        border: 1px solid #cbd5e1;
        box-shadow: 0 2px 10px rgba(0,0,0,0.12);
        font-size: 13px;
    ">
        <b>Desiertos de salud</b><br><br>
        <i style="background:#bbf7d0;width:16px;height:16px;float:left;margin-right:8px;border:1px solid #22c55e"></i>
        Cobertura<br><br>
        <i style="background:#fecaca;width:16px;height:16px;float:left;margin-right:8px;border:1px solid #ef4444"></i>
        Desierto de salud<br><br>
        <i style="background:#86efac;width:16px;height:16px;float:left;margin-right:8px;border:1px solid #16a34a"></i>
        Centro de salud
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend))

    st_folium(m, width="100%", height=500, key=map_key)

    cols = st.columns(2)
    cols[0].metric("Centros en la comuna", meta.get("centers_count", 0))
    cols[1].metric("Nivel de desiertos", f"{meta.get('desert_pct', 0):.1f}%")


def call_population_coverage_api(
    endpoint: str,
    minutes: int,
    comuna: str | None = None,
    api_url: str = None,
    spinner_text: str = "Calculando cobertura...",
    show_spinner: bool = True,
):
    api = api_url or API_URL
    url = f"{api}{endpoint}"

    params = {"minutes": minutes}
    if comuna:
        params["comuna"] = comuna

    try:
        if show_spinner:
            with st.spinner(spinner_text):
                resp = requests.get(url, params=params, timeout=1800)
        else:
            resp = requests.get(url, params=params, timeout=1800)

        if resp.status_code != 200:
            try:
                st.error(resp.json().get("detail", "Error desconocido"))
            except Exception:
                st.error(resp.text)
            return None

        return resp.json()

    except requests.exceptions.ConnectionError:
        st.error(f"No se pudo conectar a la API en {api}")
        return None
    except Exception as e:
        st.error(f"Error: {e}")
        return None


def call_population_coverage_rm(comunas: list[str], minutes: int):
    """
    Calcula la cobertura RM llamando una vez por comuna al endpoint individual.
    Evita timeout del backend.
    """
    all_features = []
    processed = []
    failed = []

    total = len(comunas)
    if total == 0:
        return {"type": "FeatureCollection", "features": [], "metadata": {}}

    progress = st.progress(0.0)
    status = st.empty()

    for idx, comuna in enumerate(comunas, start=1):
        status.write(f"Procesando {comuna} ({idx}/{total})...")
        progress.progress(idx / total)

        try:
            result = call_population_coverage_api(
                endpoint="/api/v1/population/coverage",
                comuna=comuna,
                minutes=minutes,
                spinner_text=f"Procesando {comuna}...",
                show_spinner=False,
            )

            if result and result.get("features"):
                all_features.extend(result["features"])
                processed.append(comuna)
            else:
                failed.append(comuna)

        except Exception:
            failed.append(comuna)

    progress.empty()
    status.empty()

    return {
        "type": "FeatureCollection",
        "features": all_features,
        "metadata": {
            "scope": "rm",
            "minutes": minutes,
            "processed_comunas": processed,
            "failed_comunas": failed,
            "processed_count": len(processed),
            "failed_count": len(failed),
        },
    }


def render_population_coverage(data: dict, map_key: str = "population_map"):
    features = data.get("features", [])

    if not features:
        st.warning("No hay datos")
        return

    m = folium.Map(
        location=[-33.45, -70.65],
        zoom_start=11,
        tiles="cartodbpositron",
    )

    def style_block(feature):
        props = feature.get("properties", {})
        ratio = float(props.get("coverage_ratio", 0) or 0)

        if ratio <= 0:
            color = "#fecaca"  # rojo claro
        elif ratio <= 0.25:
            color = "#fca5a5"
        elif ratio <= 0.50:
            color = "#fde68a"  # amarillo claro
        elif ratio <= 0.75:
            color = "#93c5fd"  # celeste claro
        else:
            color = "#86efac"  # verde claro

        return {
            "fillColor": color,
            "color": "#64748b",
            "weight": 0.5,
            "fillOpacity": 0.75,
        }

    for feature in features:
        props = feature.get("properties", {})
        kind = props.get("kind", "")

        if kind == "coverage":
            folium.GeoJson(
                feature,
                name="Cobertura",
                style_function=lambda x: {
                    "fillColor": "#bfdbfe",
                    "color": "#2563eb",
                    "weight": 2,
                    "fillOpacity": 0.22,
                },
                tooltip="Cobertura general",
            ).add_to(m)
            continue

        if kind == "health_center":
            coords = feature.get("geometry", {}).get("coordinates", [])
            if len(coords) >= 2:
                folium.Marker(
                    [coords[1], coords[0]],
                    popup=props.get("name", props.get("nombre", "Centro de salud")),
                    icon=folium.Icon(color="green", icon="heart"),
                ).add_to(m)
            continue

        if kind == "census_block":
            population = props.get("population", 0)
            elderly = props.get("elderly_population", 0)
            ratio = props.get("coverage_ratio", 0)

            tooltip = f"""
            <b>Manzana:</b> {props.get('block_id', '')}<br>
            <b>Población:</b> {float(population):.0f}<br>
            <b>Adultos mayores:</b> {float(elderly):.0f}<br>
            <b>Cobertura:</b> {float(ratio) * 100:.1f}%<br>
            <b>Estado:</b> {props.get('status', '')}
            """

            folium.GeoJson(
                feature,
                style_function=style_block,
                tooltip=tooltip,
            ).add_to(m)
            continue

    folium.Marker(
        [-33.45, -70.65],
        icon=folium.DivIcon(
            html="""
            <div style="
                position: fixed;
                top: 15px;
                right: 15px;
                z-index:9999;
                font-size:28px;
                font-weight:bold;
                background:white;
                padding:8px 10px;
                border-radius:8px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.15);
            ">↑ N</div>
            """
        ),
    ).add_to(m)

    legend = """
    <div style="
        position: fixed;
        bottom: 45px;
        left: 45px;
        width: 260px;
        background-color: white;
        z-index:9999;
        padding: 10px 12px;
        border-radius: 10px;
        border: 1px solid #cbd5e1;
        box-shadow: 0 2px 10px rgba(0,0,0,0.12);
        font-size: 13px;
    ">
        <b>Cobertura poblacional</b><br><br>
        <i style="background:#86efac;width:16px;height:16px;float:left;margin-right:8px;border:1px solid #22c55e"></i>
        Alta (&gt;75%)<br><br>
        <i style="background:#93c5fd;width:16px;height:16px;float:left;margin-right:8px;border:1px solid #3b82f6"></i>
        Media (50-75%)<br><br>
        <i style="background:#fde68a;width:16px;height:16px;float:left;margin-right:8px;border:1px solid #eab308"></i>
        Baja (25-50%)<br><br>
        <i style="background:#fca5a5;width:16px;height:16px;float:left;margin-right:8px;border:1px solid #ef4444"></i>
        Muy baja (0-25%)<br><br>
        <i style="background:#bfdbfe;width:16px;height:16px;float:left;margin-right:8px;border:1px solid #2563eb"></i>
        Cobertura general<br><br>
        <i style="background:#86efac;width:16px;height:16px;float:left;margin-right:8px;border:1px solid #16a34a"></i>
        Centro de salud
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend))

    st_folium(m, width="100%", height=700, key=map_key)