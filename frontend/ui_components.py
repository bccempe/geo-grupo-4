import os
import requests
import streamlit as st
import folium
from streamlit_folium import st_folium

API_URL = os.getenv("API_URL", "http://localhost:8000")

def render_location_picker(map_key: str, origin_key: str = "iso_origin", default=(-33.46803, -70.67045)):
    if origin_key not in st.session_state:
        st.session_state[origin_key] = {"lat": default[0], "lon": default[1]}

    m = folium.Map(
        location=[st.session_state[origin_key]["lat"], st.session_state[origin_key]["lon"]],
        zoom_start=14, height=300
    )
    folium.Marker(
        [st.session_state[origin_key]["lat"], st.session_state[origin_key]["lon"]],
        popup="Origen", icon=folium.Icon(color="red", icon="info-sign")
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

def call_isochrone_api(endpoint: str, lat: float, lon: float, minutes: int, include_centers: bool = True, api_url: str = None, spinner_text: str = "Calculando..."):
    api = api_url or API_URL
    url = f"{api}{endpoint}"
    try:
        with st.spinner(spinner_text):
            resp = requests.get(url, params={
                "lat": lat, "lon": lon, "minutes": minutes, "include_centers": include_centers
            })
        if resp.status_code != 200:
            st.error(resp.json().get("detail", "Error desconocido"))
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

def render_isochrone_map(iso_feature: dict, origin_feature: dict, health_centers: list, center_lat: float, center_lon: float, minutes: int, map_key: str):
    m = folium.Map(location=[center_lat, center_lon], zoom_start=12)
    folium.GeoJson(
        iso_feature,
        name="Isócrona",
        style_function=lambda x: {
            "fillColor": "#2563eb",
            "color": "#1d4ed8",
            "weight": 2,
            "fillOpacity": 0.3
        },
        tooltip=f"{minutes} min"
    ).add_to(m)

    folium.Marker(
        [origin_feature["geometry"]["coordinates"][1],
         origin_feature["geometry"]["coordinates"][0]],
        popup="Origen",
        icon=folium.Icon(color="red", icon="info-sign")
    ).add_to(m)

    for center in health_centers:
        folium.Marker(
            [center["geometry"]["coordinates"][1], center["geometry"]["coordinates"][0]],
            popup=center["properties"].get("name", center["properties"].get("nombre", "Centro de salud")),
            icon=folium.Icon(color="green", icon="heart")
        ).add_to(m)

    st_folium(m, width="100%", height=500, key=map_key)

def process_isochrone_result(result: dict, minutes: int, mode: str, map_key: str):
    if not result:
        return
    features = result.get("features", [])
    meta = result.get("metadata", {})
    health_centers = []

    if len(features) >= 2:
        for f in features:
            if f == features[0]:
                iso_feature = f
            elif f == features[1]:
                origin_feature = f
            else:
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
        bounds, lats, lons, center_info = calculate_isochrone_geometry(iso_feature)
        if not center_info:
            st.warning("No hay geometría válida")
            return
        center_lat, center_lon, _ = center_info
        render_isochrone_map(iso_feature, origin_feature, health_centers, center_lat, center_lon, minutes, map_key)
    else:
        st.warning("No se pudo calcular la isócrona (sin datos de origen)")
        return

def call_health_desert_api(endpoint: str, comuna: str, minutes: int, api_url: str = None, spinner_text: str = "Calculando desiertos..."):
    api = api_url or API_URL
    url = f"{api}{endpoint}"
    try:
        with st.spinner(spinner_text):
            resp = requests.get(url, params={
                "comuna": comuna,
                "minutes": minutes
            })
        if resp.status_code != 200:
            st.error(resp.json().get("detail", "Error desconocido"))
            return None
        return resp.json()
    except requests.exceptions.ConnectionError:
        st.error(f"No se pudo conectar a la API en {API_URL}")
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

    m = folium.Map(location=[-33.45, -70.65], zoom_start=11)

    for feature in features:
        kind = feature["properties"].get("kind", "")
        style = {}
        tooltip = ""
        if kind == "coverage":
            style = {
                "fillColor": "#10b981",
                "color": "#059669",
                "weight": 2,
                "fillOpacity": 0.4
            }
            tooltip = "Cobertura"
        elif kind == "health_desert":
            style = {
                "fillColor": "#ef4444",
                "color": "#dc2626",
                "weight": 2,
                "fillOpacity": 0.4
            }
            tooltip = "Desierto de salud"
        elif kind == "health_center":
            folium.Marker(
                [feature["geometry"]["coordinates"][1], feature["geometry"]["coordinates"][0]],
                popup=feature["properties"].get("name", feature["properties"].get("nombre", "Centro de salud")),
                icon=folium.Icon(color="green", icon="heart")
            ).add_to(m)
            

        if style:
            folium.GeoJson(
                feature,
                name=tooltip,
                style_function=lambda x, s=style: s,
                tooltip=tooltip
            ).add_to(m)

    st_folium(m, width="100%", height=500, key=map_key)

    cols = st.columns(2)
    cols[0].metric("Centros en la comuna", meta.get("centers_count", 0))
    cols[1].metric("Nivel de desiertos", f"{meta.get('desert_pct', 0):.1f}%")