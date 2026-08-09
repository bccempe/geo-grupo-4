import os
import requests
import streamlit as st
import folium
import branca
import geopandas as gpd
import numpy as np
from streamlit_folium import st_folium
from shapely.geometry import shape

API_URL = os.getenv("API_URL", "http://localhost:8000")

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

def calculate_coverage_statistics(data: dict) -> dict:
    blocks_gdf, _ = build_population_coverage_gdfs(data)

    total_population = blocks_gdf["population"].fillna(0).sum()
    covered_population = blocks_gdf["covered_population"].fillna(0).sum()
    elderly_population = blocks_gdf["elderly_population"].fillna(0).sum()
    covered_elderly = blocks_gdf["covered_elderly_population"].fillna(0).sum()

    coverage_pct = 0
    if total_population > 0:
        coverage_pct = (covered_population / total_population) * 100

    return {
        "total_population": int(total_population),
        "covered_population": int(covered_population),
        "elderly_population": int(elderly_population),
        "covered_elderly_population": int(covered_elderly),
        "coverage_pct": coverage_pct,
    }

def render_coverage_statistics(stats: dict):
    col_data, col_coverage = st.columns([2,1])

    with col_data:
        col_population, col_elderly = st.columns(2)

        with col_population:
            st.metric("Población total", f"{stats['total_population']:,}")
            st.metric("Población cubierta", f"{stats['covered_population']:,}")
        
        with col_elderly:
            st.metric("Adultos mayores", f"{stats['elderly_population']:,}")
            st.metric("Mayores cubiertos", f"{stats['covered_elderly_population']:,}")
    
    with col_coverage:
        st.metric("Cobertura", f"{stats['coverage_pct']:.1f}%")

def call_geocode_autocomplete_api(query: str, limit: int = 5, api_url: str = None):
    api = api_url or API_URL
    url = f"{api}/api/v1/geocode/autocomplete"
    try:
        resp = requests.get(url, params={"q": query, "limit": limit}, timeout=10)
        if resp.status_code == 200:
            return resp.json().get("results", [])
    except Exception as e:
        print(f"Error llamando autocomplete API: {e}")
    return []

def call_reverse_geocode_api(lat: float, lon: float, api_url: str = None):
    api = api_url or API_URL
    url = f"{api}/api/v1/geocode/reverse"
    try:
        resp = requests.get(url, params={"lat": lat, "lon": lon}, timeout=10)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"Error llamando reverse geocode API: {e}")
    return None

def render_location_picker(
    map_key: str,
    origin_key: str = "iso_origin",
    default=(-33.46803, -70.67045)
):
    if origin_key not in st.session_state:
        st.session_state[origin_key] = {
            "lat": default[0],
            "lon": default[1],
            "address": "Av. Libertador Bernardo O'Higgins, Santiago"
        }

    st.markdown("**🔍 Buscar dirección (Autocompletado):**")
    search_col, btn_col = st.columns([3, 1])

    with search_col:
        search_query = st.text_input(
            "Dirección",
            key=f"{map_key}_address_input",
            placeholder="Ej: Alameda 1050, Santiago",
            label_visibility="collapsed"
        )

    with btn_col:
        search_pressed = st.button("Buscar", key=f"{map_key}_search_btn", icon=":material/search:")

    if (search_pressed or search_query) and search_query.strip():
        # Autocompletado via Backend Controller
        results = call_geocode_autocomplete_api(search_query)
        if results:
            options_map = {res["display_name"]: res for res in results}
            selected_display = st.selectbox(
                "Direcciones encontradas:",
                options=list(options_map.keys()),
                key=f"{map_key}_select_address"
            )
            if selected_display:
                selected_item = options_map[selected_display]
                new_lat = selected_item["lat"]
                new_lon = selected_item["lon"]
                if (new_lat != st.session_state[origin_key]["lat"] or
                    new_lon != st.session_state[origin_key]["lon"]):
                    st.session_state[origin_key]["lat"] = new_lat
                    st.session_state[origin_key]["lon"] = new_lon
                    st.session_state[origin_key]["address"] = selected_item["display_name"]
        else:
            if search_pressed:
                st.warning("No se encontraron resultados para la dirección ingresada.")

    curr_lat = st.session_state[origin_key]["lat"]
    curr_lon = st.session_state[origin_key]["lon"]
    curr_addr = st.session_state[origin_key].get("address", "Punto seleccionado")

    m = folium.Map(
        location=[curr_lat, curr_lon],
        zoom_start=14,
        height=300
    )

    folium.Marker(
        [curr_lat, curr_lon],
        popup=curr_addr,
        tooltip=curr_addr,
        icon=folium.Icon(color="red", icon="info-sign")
    ).add_to(m)

    map_data = st_folium(m, width="100%", height=300, key=map_key)

    if map_data and map_data.get("last_clicked"):
        click_lat = map_data["last_clicked"]["lat"]
        click_lon = map_data["last_clicked"]["lng"]

        # Si cambió el punto al hacer click en el mapa, traducir coordenadas -> dirección
        if round(click_lat, 5) != round(curr_lat, 5) or round(click_lon, 5) != round(curr_lon, 5):
            st.session_state[origin_key]["lat"] = click_lat
            st.session_state[origin_key]["lon"] = click_lon

            rev_result = call_reverse_geocode_api(click_lat, click_lon)
            if rev_result and rev_result.get("display_name"):
                st.session_state[origin_key]["address"] = rev_result.get("short_address") or rev_result.get("display_name")
            else:
                st.session_state[origin_key]["address"] = f"{click_lat:.5f}, {click_lon:.5f}"

            st.rerun()

    if st.session_state[origin_key].get("address"):
        st.caption(f"📍 **Ubicación:** {st.session_state[origin_key]['address']}")

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
                "fillColor": "#6a9f58",
                "color": "#85b6b2",
                "weight": 2,
                "fillOpacity": 0.45,
            }
            tooltip = "Cobertura"

        elif kind == "health_desert":
            style = {
                "fillColor": "#f1a2a9",
                "color": "#d1615d",
                "weight": 2,
                "fillOpacity": 0.45,
            }
            tooltip = "Desierto de salud"

        elif kind == "health_center":
            folium.Marker(
                [feature["geometry"]["coordinates"][1], feature["geometry"]["coordinates"][0]],
                popup=feature["properties"].get("name", feature["properties"].get("nombre", "Centro de salud")),
                icon=folium.Icon(color="green", icon="glyphicon-plus"),
            ).add_to(m)
            continue

        if style:
            folium.GeoJson(
                feature,
                name=tooltip,
                style_function=lambda x, s=style: s,
                tooltip=tooltip,
            ).add_to(m)


    legend = """
    <div style="
        position: fixed;
        bottom: 45px;
        left: 45px;
        width: 200px;
        background-color: rgba(255,255,255,0.95);
        color: #111;
        z-index:9999;
        padding: 10px 12px;
        border-radius: 10px;
        border: 1px solid #cbd5e1;
        box-shadow: 0 2px 10px rgba(0,0,0,0.12);
        font-size: 13px;
    ">
        <b>Desiertos de salud</b><br><br>
        <i style="background:#85b6b2;width:16px;height:16px;float:left;margin-right:8px;border:1px solid #6a9f58"></i>
        Cobertura<br><br>
        <i style="background:#f1a2a9;width:16px;height:16px;float:left;margin-right:8px;border:1px solid #d1615d"></i>
        Desierto de salud<br><br>
        <span style="display:inline-block;width:16px;height:24px;margin-right:8px;vertical-align:middle;">
            <svg viewBox="0 0 24 24" width="16" height="24" xmlns="http://www.w3.org/2000/svg">
                <path fill="#16a34a" d="M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7zM12 11.5c-1.38 0-2.5-1.12-2.5-2.5S10.62 6.5 12 6.5s2.5 1.12 2.5 2.5S13.38 11.5 12 11.5z"/>
            </svg>
        </span>
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
    departure_hour: int | None = None,
    decay: str | None = None,
    api_url: str = None,
    spinner_text: str = "Calculando cobertura...",
    show_spinner: bool = True,
):
    api = api_url or API_URL
    url = f"{api}{endpoint}"

    params = {"minutes": minutes}
    if comuna:
        params["comuna"] = comuna
    if departure_hour is not None:
        params["departure_hour"] = departure_hour
    if decay is not None:
        params["decay"] = decay

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


def call_population_coverage_rm(comunas: list[str], minutes: int, mode: str = "walk", departure_hour: int | None = None):
    """
    Calcula la cobertura RM usando el endpoint consolidado del backend.
    """
    endpoint = (
        "/api/v1/population/coverage-rm"
        if mode == "walk"
        else "/api/v1/population/transit-coverage-rm"
    )

    try:
        result = call_population_coverage_api(
            endpoint=endpoint,
            minutes=minutes,
            departure_hour=departure_hour,
            spinner_text=f"Calculando cobertura RM ({mode})...",
            show_spinner=True,
        )

        if result:
            return result

    except Exception:
        pass

    return {
        "type": "FeatureCollection",
        "features": [],
        "metadata": {
            "scope": "rm",
            "mode": mode,
            "minutes": minutes,
            "departure_hour": departure_hour,
            "error": "No se pudo calcular la cobertura RM",
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

        if ratio >= 0 and ratio <= 0.25:
            color = "#9ecae1"  
        elif ratio <= 0.50:
            color = "#6baed6" 
        elif ratio <= 0.75:
            color = "#3182bd"  
        else:
            color = "#08519c"  

        return {
            "fillColor": color,
            "color": "#64748b",
            "weight": 0.5,
            "fillOpacity": 0.75,
        }

    for feature in features:
        props = feature.get("properties", {})
        kind = props.get("kind", "")
        style = {}
        tooltip = ""

        if kind == "health_center":
            coords = feature.get("geometry", {}).get("coordinates", [])
            if len(coords) >= 2:
                folium.Marker(
                    [coords[1], coords[0]],
                    popup=props.get("name", props.get("nombre", "Centro de salud")),
                    icon=folium.Icon(color="green", icon="glyphicon-plus"),
                ).add_to(m)
            continue

        if kind == "census_block":
            population = props.get("population", 0)
            elderly = props.get("elderly_population", 0)
            ratio = props.get("coverage_ratio", 0)

            tooltip = f"""
            <b>Población:</b> {float(population):.0f}<br>
            <b>Adultos mayores:</b> {float(elderly):.0f}<br>
            <b>Cobertura:</b> {float(ratio) * 100:.1f}%<br>
            """

            folium.GeoJson(
                feature,
                style_function=style_block,
                tooltip=tooltip,
            ).add_to(m)
            continue

        if style:
            folium.GeoJson(
                feature,
                name=tooltip,
                style_function=lambda x, s=style: s,
                tooltip=tooltip,
            ).add_to(m)

    legend = """
    <div style="
        position: fixed;
        bottom: 45px;
        left: 45px;
        width: 260px;
        background-color: rgba(255,255,255,0.95);
        color: #111;
        z-index:9999;
        padding: 10px 12px;
        border-radius: 10px;
        border: 1px solid #cbd5e1;
        box-shadow: 0 2px 10px rgba(0,0,0,0.12);
        font-size: 13px;
    ">
        <b>Cobertura por manzana</b><br><br>
        <i style="background:#08519c;width:16px;height:16px;float:left;margin-right:8px;border:1px solid #08519c"></i>
        Alta (&gt;75%)<br><br>
        <i style="background:#3182bd;width:16px;height:16px;float:left;margin-right:8px;border:1px solid #3182bd"></i>
        Media (50-75%)<br><br>
        <i style="background:#6baed6;width:16px;height:16px;float:left;margin-right:8px;border:1px solid #6baed6"></i>
        Baja (25-50%)<br><br>
        <i style="background:#9ecae1;width:16px;height:16px;float:left;margin-right:8px;border:1px solid #9ecae1"></i>
        Muy baja (0-25%)<br><br>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend))

    st_folium(m, width="100%", height=700, key=map_key)


# ======================================================================
# Accesibilidad 2SFCA (Bloque B)
# ======================================================================

def build_accessibility_gdfs(data: dict):
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

        if kind == "census_block_accessibility":
            block_rows.append({**props, "geometry": geom})
        elif kind == "health_center":
            center_rows.append({**props, "geometry": geom})

    if block_rows:
        blocks_gdf = gpd.GeoDataFrame(block_rows, geometry="geometry", crs="EPSG:4326")
    else:
        blocks_gdf = gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs="EPSG:4326")

    if center_rows:
        centers_gdf = gpd.GeoDataFrame(center_rows, geometry="geometry", crs="EPSG:4326")
    else:
        centers_gdf = gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs="EPSG:4326")

    return blocks_gdf, centers_gdf


def calculate_accessibility_statistics(data: dict) -> dict:
    blocks_gdf, _ = build_accessibility_gdfs(data)

    total_population = blocks_gdf["population"].fillna(0).sum()
    desert_mask = blocks_gdf["status"] == "health_desert"
    desert_population = blocks_gdf.loc[desert_mask, "population"].fillna(0).sum()
    desert_blocks = int(desert_mask.sum())
    total_blocks = len(blocks_gdf)

    scores = blocks_gdf["accessibility_2sfca"].dropna()
    mean_score = float(scores.mean()) if len(scores) > 0 else 0.0
    median_score = float(scores.median()) if len(scores) > 0 else 0.0

    desert_pct = (desert_population / total_population * 100) if total_population > 0 else 0

    return {
        "total_population": int(total_population),
        "desert_population": int(desert_population),
        "desert_pct": desert_pct,
        "total_blocks": total_blocks,
        "desert_blocks": desert_blocks,
        "mean_score": mean_score,
        "median_score": median_score,
    }


def render_accessibility_statistics(stats: dict):
    col_pop, col_score = st.columns(2)

    with col_pop:
        st.metric("Población total", f"{stats['total_population']:,}")
        st.metric("Población en desierto", f"{stats['desert_population']:,}")

    with col_score:
        st.metric("Manzanas en desierto", f"{stats['desert_pct']:.1f}%")
        st.metric("Score 2SFCA promedio", f"{stats['mean_score']:.6f}")


def render_accessibility_coverage(data: dict, map_key: str = "accessibility_map"):
    features = data.get("features", [])

    if not features:
        st.warning("No hay datos")
        return

    blocks_gdf, centers_gdf = build_accessibility_gdfs(data)

    if blocks_gdf.empty:
        st.warning("No hay manzanas con datos de accesibilidad")
        return

    scores = blocks_gdf["accessibility_2sfca"].dropna()
    has_scores = len(scores) > 0

    if has_scores:
        q25 = scores.quantile(0.25)
        q50 = scores.quantile(0.50)
        q75 = scores.quantile(0.75)

    m = folium.Map(location=[-33.45, -70.65], zoom_start=11, tiles="cartodbpositron")

    def style_accessibility(feature):
        props = feature.get("properties", {})
        status = props.get("status", "")
        if status == "health_desert":
            return {
                "fillColor": "#d73027",
                "color": "#a50f15",
                "weight": 0.5,
                "fillOpacity": 0.85,
            }
        if not has_scores:
            return {
                "fillColor": "#cccccc",
                "color": "#999999",
                "weight": 0.5,
                "fillOpacity": 0.75,
            }
        score = float(props.get("accessibility_2sfca", 0) or 0)
        if score <= q25:
            color = "#fdae61"
        elif score <= q50:
            color = "#a6d96a"
        elif score <= q75:
            color = "#1a9641"
        else:
            color = "#005a32"
        return {
            "fillColor": color,
            "color": "#666666",
            "weight": 0.5,
            "fillOpacity": 0.75,
        }

    for feature in features:
        props = feature.get("properties", {})
        kind = props.get("kind", "")

        if kind == "health_center":
            coords = feature.get("geometry", {}).get("coordinates", [])
            if len(coords) >= 2:
                folium.Marker(
                    [coords[1], coords[0]],
                    popup=props.get("name", props.get("nombre", "Centro de salud")),
                    icon=folium.Icon(color="green", icon="glyphicon-plus"),
                ).add_to(m)
            continue

        if kind == "census_block_accessibility":
            score = props.get("accessibility_2sfca", 0)
            population = props.get("population", 0)
            status = props.get("status", "")

            tooltip = (
                f"<b>Población:</b> {float(population):.0f}<br>"
                f"<b>Score 2SFCA:</b> {float(score):.6f}<br>"
                f"<b>Estado:</b> {status}<br>"
            )

            folium.GeoJson(
                feature,
                style_function=style_accessibility,
                tooltip=tooltip,
            ).add_to(m)
            continue

    legend_items = [
        '<i style="background:#d73027;width:16px;height:16px;float:left;margin-right:8px;border:1px solid #a50f15"></i>'
        " Desierto de salud",
    ]
    if has_scores:
        legend_items += [
            f'<br><i style="background:#fdae61;width:16px;height:16px;float:left;margin-right:8px;border:1px solid #fdae61"></i>'
            f" Baja (≤{q25:.4f})",
            f'<br><i style="background:#a6d96a;width:16px;height:16px;float:left;margin-right:8px;border:1px solid #a6d96a"></i>'
            f" Media-baja (≤{q50:.4f})",
            f'<br><i style="background:#1a9641;width:16px;height:16px;float:left;margin-right:8px;border:1px solid #1a9641"></i>'
            f" Media-alta (≤{q75:.4f})",
            f'<br><i style="background:#005a32;width:16px;height:16px;float:left;margin-right:8px;border:1px solid #005a32"></i>'
            f" Alta (>{q75:.4f})",
        ]

    legend_html = f"""
    <div style="
        position: fixed;
        bottom: 45px;
        left: 45px;
        width: 260px;
        background-color: rgba(255,255,255,0.95);
        color: #111;
        z-index:9999;
        padding: 10px 12px;
        border-radius: 10px;
        border: 1px solid #cbd5e1;
        box-shadow: 0 2px 10px rgba(0,0,0,0.12);
        font-size: 13px;
    ">
        <b>Accesibilidad 2SFCA</b><br><br>
        {''.join(legend_items)}
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    st_folium(m, width="100%", height=700, key=map_key)