import streamlit as st
import requests
import os

API_URL = os.getenv("API_URL", "http://localhost:8000")

st.title("Accesibilidad a Salud RM")

tab1, tab2, tab3, tab4 = st.tabs(["Datasets", "Centros de Salud", "Censo", "Isocronas TP"])

with tab1:
    st.header("Explorador de Datasets")

    if "datasets" not in st.session_state:
        st.session_state.datasets = None
        st.session_state.current_files = None

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Cargar datasets"):
            with st.spinner("Cargando lista de datasets..."):
                try:
                    res = requests.get(f"{API_URL}/datasets")
                    if res.status_code == 200:
                        st.session_state.datasets = res.json().get("datasets", [])
                        st.success(f"Cargados {len(st.session_state.datasets)} datasets")
                    else:
                        st.error("Error al cargar datasets")
                except Exception as e:
                    st.error(f"Error: {e}")

    with col2:
        if st.button("Ver resumen"):
            with st.spinner("Cargando resumen..."):
                try:
                    res = requests.get(f"{API_URL}/summary")
                    if res.status_code == 200:
                        summary = res.json()
                        st.json(summary)
                    else:
                        st.error("Error al cargar resumen")
                except Exception as e:
                    st.error(f"Error: {e}")

    if st.session_state.datasets:
        st.subheader("Seleccionar Dataset")
        selected_dataset = st.selectbox("Dataset", st.session_state.datasets)

        if st.button("Ver archivos"):
            with st.spinner("Cargando archivos..."):
                try:
                    res = requests.get(f"{API_URL}/datasets/{selected_dataset}")
                    if res.status_code == 200:
                        data = res.json()
                        st.session_state.current_files = data.get("files", [])
                        st.success(f"Archivos: {st.session_state.current_files}")
                    else:
                        st.error("Error al cargar archivos")
                except Exception as e:
                    st.error(f"Error: {e}")

        if st.session_state.get("current_files"):
            st.subheader("Seleccionar Archivo")
            selected_file = st.selectbox("Archivo", st.session_state.current_files)

            limit = st.number_input("Límite de registros", min_value=5, max_value=1000, value=10)

            if st.button("Cargar datos"):
                with st.spinner("Cargando datos..."):
                    try:
                        res = requests.get(f"{API_URL}/datasets/{selected_dataset}/{selected_file}?limit={limit}")
                        if res.status_code == 200:
                            data = res.json()
                            st.success(f"Cargados {len(data)} registros")
                            st.dataframe(data, width='stretch')
                        else:
                            st.error(f"Error: {res.json()}")
                    except Exception as e:
                        st.error(f"Error: {e}")

with tab2:
    st.header("Centros de Salud")
    if st.button("Cargar centros", key="cargar_centros"):
        res = requests.get(f"{API_URL}/centros")
        st.json(res.json())

with tab3:
    st.header("Datos Censo 2024")

    if "censo_comunas" not in st.session_state:
        st.session_state.censo_comunas = []

    col1, col2 = st.columns([1, 3])

    with col1:
        if st.button("Cargar comunas"):
            with st.spinner("Cargando comunas..."):
                try:
                    res = requests.get(f"{API_URL}/censo")
                    if res.status_code == 200:
                        data = res.json()
                        st.session_state.censo_comunas = data.get("comunas", [])
                        st.success(f"{len(st.session_state.censo_comunas)} comunas disponibles")
                    else:
                        st.error("Error al cargar comunas")
                except Exception as e:
                    st.error(f"Error: {e}")

    if st.session_state.censo_comunas:
        with col2:
            selected_comuna = st.selectbox("Seleccionar comuna", st.session_state.censo_comunas)

        limit = st.number_input("Registros a mostrar", min_value=5, max_value=100, value=10, key="censo_limit")

        if st.button("Ver datos"):
            with st.spinner("Cargando datos..."):
                try:
                    res = requests.get(f"{API_URL}/censo/{selected_comuna}?limit={limit}")
                    if res.status_code == 200:
                        data = res.json()
                        st.success(f"Datos de {selected_comuna}")
                        st.dataframe(data, width='stretch')
                    else:
                        st.error(f"Error: {res.json()}")
                except Exception as e:
                    st.error(f"Error: {e}")

with tab4:
    st.header("Isocronas Transporte Público")

    col_f1, col_f2 = st.columns([1, 1])

    with col_f1:
        comuna = st.text_input("Comuna (ej. SANTIAGO, LA FLORIDA)", "SANTIAGO")
        lat = st.number_input("Latitud", value=-33.46803, format="%.5f")
        lon = st.number_input("Longitud", value=-70.67045, format="%.5f")
        minutes = st.slider("Minutos", 15, 60, 30)

    with col_f2:
        st.subheader("Centros de la comuna")
        if st.button("Cargar centros", key="cargar_centros_tp"):
            try:
                resp = requests.get(f"{API_URL}/api/v1/transit/health-deserts", params={
                    "comuna": comuna, "minutes": minutes
                })
                if resp.status_code == 200:
                    meta = resp.json().get("metadata", {})
                    st.session_state.centers_count = meta.get("centers", 0)
                    st.success(f"{meta.get('centers', 0)} centros en {comuna}")
            except Exception as e:
                st.warning(f"Error: {e}")

        if "centers_count" in st.session_state:
            st.info(f"Total: {st.session_state.centers_count} centros de atención primaria")

    calc = st.button("Calcular isócrona", type="primary")

    if calc:
        with st.spinner("Calculando isócrona de transporte público..."):
            try:
                resp = requests.get(f"{API_URL}/api/v1/transit/isochrone", params={
                    "comuna": comuna, "lat": lat, "lon": lon,
                    "minutes": minutes, "include_centers": False
                })
                if resp.status_code != 200:
                    st.error(resp.json().get("detail", "Error desconocido"))
                    st.session_state.iso_result = None
                else:
                    st.session_state.iso_result = resp.json()
            except requests.exceptions.ConnectionError:
                st.error(f"No se pudo conectar a la API en {API_URL}")
                st.session_state.iso_result = None
            except Exception as e:
                st.error(f"Error: {e}")
                st.session_state.iso_result = None

    if st.session_state.get("iso_result"):
        data = st.session_state.iso_result
        features = data.get("features", [])
        meta = data.get("metadata", {})

        if features:
            from streamlit_folium import st_folium
            import folium

            iso_feature = features[0]
            origin_feature = features[1]

            bounds = iso_feature["geometry"]["coordinates"][0]
            lats = [p[1] for p in bounds]
            lons = [p[0] for p in bounds]
            center_lat = sum(lats) / len(lats)
            center_lon = sum(lons) / len(lons)

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

            st_folium(m, width="100%", height=500)

            col_a, col_b, col_c = st.columns(3)
            col_a.metric("Paradas cercanas", meta.get("origin_stops", 0))
            col_b.metric("Paradas alcanzables", meta.get("reachable_stops", 0))
            col_c.metric("Minutos", meta.get("minutes", minutes))

        if st.button("Limpiar mapa", key="clear_iso"):
            del st.session_state.iso_result
            st.rerun()

    st.divider()
    st.subheader("Desiertos de Salud (por comuna)")

    desert_comuna = st.text_input("Comuna para desierto", "LA FLORIDA", key="desert_comuna")
    desert_minutes = st.slider("Minutos para desierto", 15, 60, 30, key="desert_min")

    if st.button("Calcular desierto", type="secondary", key="calc_desert"):
        with st.spinner("Calculando desiertos de salud..."):
            try:
                resp = requests.get(f"{API_URL}/api/v1/transit/health-deserts", params={
                    "comuna": desert_comuna, "minutes": desert_minutes
                })
                if resp.status_code != 200:
                    st.error(resp.json().get("detail", "Error desconocido"))
                    st.session_state.desert_result = None
                else:
                    st.session_state.desert_result = resp.json()
            except requests.exceptions.ConnectionError:
                st.error(f"No se pudo conectar a la API en {API_URL}")
                st.session_state.desert_result = None
            except Exception as e:
                st.error(f"Error: {e}")
                st.session_state.desert_result = None

    if st.session_state.get("desert_result"):
        data = st.session_state.desert_result
        features = data.get("features", [])
        meta = data.get("metadata", {})

        if features:
            from streamlit_folium import st_folium
            import folium

            m = folium.Map(location=[-33.45, -70.65], zoom_start=11)

            for f in features:
                kind = f["properties"].get("kind", "")
                if kind == "coverage":
                    folium.GeoJson(
                        f,
                        name="Cobertura",
                        style_function=lambda x: {
                            "fillColor": "#10b981",
                            "color": "#059669",
                            "weight": 2,
                            "fillOpacity": 0.4
                        },
                        tooltip="Cobertura"
                    ).add_to(m)
                elif kind == "health_desert":
                    folium.GeoJson(
                        f,
                        name="Desierto",
                        style_function=lambda x: {
                            "fillColor": "#ef4444",
                            "color": "#dc2626",
                            "weight": 2,
                            "fillOpacity": 0.4
                        },
                        tooltip="Desierto de salud"
                    ).add_to(m)

            st_folium(m, width="100%", height=500)

            cols = st.columns(4)
            cols[0].metric("Centros", meta.get("centers", 0))
            cols[1].metric("Isocronas", meta.get("generated_isochrones", 0))
            cols[2].metric("Cobertura", f"{meta.get('coverage_area', 0):.4f}°²")
            cols[3].metric("Desierto", f"{meta.get('desert_pct', 0):.1f}%")

        if st.button("Limpiar mapa", key="clear_desert"):
            del st.session_state.desert_result
            st.rerun()
