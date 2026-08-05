from ui_components import (
    render_location_picker,
    call_isochrone_api,
    process_isochrone_result,
    call_health_desert_api,
    render_health_deserts,
    call_population_coverage_api,
    render_population_coverage,
    call_population_coverage_rm,
    calculate_coverage_statistics,
    render_coverage_statistics
)

from export import (
    export_health_desert_map, 
    export_population_coverage_map,
    export_full_population_coverage_map
)

import streamlit as st
import unicodedata
import re


def remove_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFD", text)
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def normalize_comuna(text: str) -> str:
    if not text:
        return ""
    text = remove_accents(text)
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


comunas_disponibles = sorted([
    "Alhué", "Buin", "Calera de Tango", "Cerrillos", "Cerro Navia", "Colina", "Conchalí",
    "Curacaví", "El Bosque", "El Monte", "Estación Central", "Huechuraba", "Independencia",
    "Isla de Maipo", "La Cisterna", "La Florida", "La Granja", "La Pintana", "La Reina",
    "Lampa", "Las Condes", "Lo Barnechea", "Lo Espejo", "Lo Prado", "Macul", "Maipú",
    "María Pinto", "Melipilla", "Ñuñoa", "Padre Hurtado", "Paine", "Pedro Aguirre Cerda",
    "Peñaflor", "Peñalolén", "Pirque", "Providencia", "Pudahuel", "Puente Alto",
    "Quilicura", "Quinta Normal", "Recoleta", "Renca", "San Bernardo", "San Joaquín",
    "San José de Maipo", "San Miguel", "San Pedro", "San Ramón", "Santiago", "Talagante", "Vitacura"
])

st.title("Accesibilidad a Centros de Salud en la Región Metropolitana")

iso_tab, desert_tab, coverage_tab = st.tabs([
    "Isócronas",
    "Desiertos de salud",
    "Cobertura poblacional"
])

with iso_tab:
    st.subheader("Mapa de Isócronas")
    st.caption("Haz click en cualquier punto del mapa para definir el punto de partida de la isócrona")

    col_iso_map, col_iso_slider = st.columns([2, 1])

    with col_iso_map:
        lat, lon = render_location_picker(map_key="picker_iso", origin_key="iso_origin")

    with col_iso_slider:
        isochrone_minutes = st.slider("Minutos", 30, 60, 30, key="iso_minutes")
        st.metric("Latitud", f"{lat:.5f}")
        st.metric("Longitud", f"{lon:.5f}")

    if st.button(
        "Calcular isócronas",
        type="primary",
        key="calc_iso",
        icon=":material/map_search:",
        width="stretch"
    ):
        st.session_state.iso_walk_result = call_isochrone_api(
            "/api/v1/isochrone",
            lat,
            lon,
            isochrone_minutes,
            spinner_text=f"Calculando isócrona caminando {isochrone_minutes} minutos..."
        )
        st.session_state.iso_transit_result = call_isochrone_api(
            "/api/v1/transit/isochrone",
            lat,
            lon,
            isochrone_minutes,
            spinner_text=f"Calculando isócrona usando transporte público durante {isochrone_minutes} minutos..."
        )

    if st.session_state.get("iso_walk_result"):
        st.subheader(f"Caminando {isochrone_minutes} minutos")
        process_isochrone_result(
            st.session_state.iso_walk_result,
            isochrone_minutes,
            mode="walk",
            map_key="iso_walk_map"
        )

    if st.session_state.get("iso_transit_result"):
        st.subheader(f"Usando transporte público durante {isochrone_minutes} minutos")
        process_isochrone_result(
            st.session_state.iso_transit_result,
            isochrone_minutes,
            mode="transit",
            map_key="iso_transit_map"
        )

with desert_tab:
    st.subheader("Desiertos de salud por comuna")
    st.caption("Selecciona una comuna y un tiempo estimado para calcular los desiertos de salud")

    col_des_comuna, col_des_slider = st.columns([2, 1])

    with col_des_comuna:
        desert_comuna = st.selectbox("Comuna a calcular", comunas_disponibles, key="desert_comuna")
        query_comuna = normalize_comuna(desert_comuna)

    with col_des_slider:
        desert_minutes = st.slider("Minutos estimados", 30, 60, 30, key="desert_minutes")

    if st.button(
        "Calcular desiertos",
        type="primary",
        key="calc_desert",
        icon=":material/map_search:",
        width="stretch"
    ):
        st.session_state.desert_walk_result = call_health_desert_api(
            "/api/v1/health-deserts",
            query_comuna,
            desert_minutes,
            spinner_text=f"Calculando desiertos de salud caminando {desert_minutes} minutos..."
        )
        st.session_state.desert_transit_result = call_health_desert_api(
            "/api/v1/transit/health-deserts",
            query_comuna,
            desert_minutes,
            spinner_text=f"Calculando desiertos de salud utilizando transporte público durante {desert_minutes} minutos..."
        )

    if st.session_state.get("desert_walk_result") and st.session_state.get("desert_transit_result"):
        st.subheader(f"Caminando {desert_minutes} minutos")
        render_health_deserts(
            st.session_state.desert_walk_result,
            map_key="desert_walk_map"
        )

        buf_walk = export_health_desert_map(
            st.session_state.desert_walk_result,
            desert_minutes,
            desert_comuna,
            "caminando"
        )
        if buf_walk:
            st.download_button(
                label="Exportar a PNG",
                type="primary",
                data=buf_walk,
                file_name=f"health_desert_walk_{desert_minutes}min.png",
                mime="image/png",
                key="export_desert_walk",
                icon=":material/file_export:",
                width="stretch"
            )

        st.subheader(f"Usando transporte público durante {desert_minutes} minutos")
        render_health_deserts(
            st.session_state.desert_transit_result,
            map_key="desert_transit_map"
        )

        buf_transit = export_health_desert_map(
            st.session_state.desert_transit_result,
            desert_minutes,
            desert_comuna,
            "usando transporte público"
        )
        if buf_transit:
            st.download_button(
                label="Exportar a PNG",
                type="primary",
                data=buf_transit,
                file_name=f"health_desert_transit_{desert_minutes}min.png",
                mime="image/png",
                key="export_desert_transit",
                icon=":material/file_export:",
                width="stretch"
            )
            
with coverage_tab:

    st.subheader("Cobertura poblacional")

    st.info("La cobertura poblacional se refiere al porcentaje de la población que puede acceder a un centro de salud primaria (CESFAM, SAPU) movilizándose durante una determinada cantidad de tiempo.")

    col_cov_scope, col_cov_mode = st.columns([1, 1])

    with col_cov_scope:
        coverage_scope = st.radio(
            "Alcance",
            [
                "Por comuna",
                "Región Metropolitana completa"
            ],
            horizontal=True
        )

    with col_cov_mode:
        coverage_transport = st.radio(
            "Modo de transporte",
            [
                "Caminata",
                "Transporte público"
            ],
            horizontal=True,
            key="coverage_transport"
        )

    col_cov_minutes, col_cov_hour = st.columns([1, 1])

    with col_cov_minutes:
        coverage_minutes = st.slider(
            "Minutos",
            5,
            60,
            15 if coverage_transport == "Caminata" else 30,
            key="coverage_minutes"
        )

    with col_cov_hour:
        if coverage_transport == "Transporte público":
            departure_hour = st.slider(
                "Hora de salida",
                0,
                23,
                8,
                key="coverage_departure_hour"
            )
        else:
            departure_hour = None

    transit_mode = coverage_transport == "Transporte público"

    # ==================================================
    # COBERTURA POR COMUNA
    # ==================================================
    if coverage_scope == "Por comuna":

        coverage_comuna = st.selectbox(
            "Comuna",
            comunas_disponibles,
            key="coverage_comuna"
        )

        query_comuna = normalize_comuna(
            coverage_comuna
        )

        endpoint = (
            "/api/v1/population/transit-coverage"
            if transit_mode
            else "/api/v1/population/coverage"
        )

        if st.button(
            "Calcular cobertura",
            type="primary",
            key="coverage_btn",
            icon=":material/map_search:"
        ):

            st.session_state.coverage_result = (
                call_population_coverage_api(
                    endpoint=endpoint,
                    comuna=query_comuna,
                    minutes=coverage_minutes,
                    departure_hour=departure_hour if transit_mode else None,
                    spinner_text=(
                        f"Calculando cobertura poblacional "
                        f"en {coverage_comuna}..."
                    )
                )
            )

        if st.session_state.get("coverage_result"):

            render_population_coverage(
                st.session_state.coverage_result,
                map_key="coverage_map"
            )

            stats = calculate_coverage_statistics(
                st.session_state.coverage_result
            )
            render_coverage_statistics(stats)

            buf_coverage_comuna = export_population_coverage_map(
                st.session_state.coverage_result,
                minutes=coverage_minutes,
                comuna=coverage_comuna,
                include_cartographic_elements=True
            )
            if buf_coverage_comuna:
                st.download_button(
                    label="Exportar a PNG",
                    type="primary",
                    data=buf_coverage_comuna,
                    file_name=f"population_coverage_{query_comuna}_{coverage_minutes}min.png",
                    mime="image/png",
                    key="export_coverage_comuna",
                    icon=":material/file_export:",
                    width="stretch"
                )

    # ==================================================
    # COBERTURA RM
    # ==================================================
    else:

        st.warning(
            "La cobertura de toda la Región Metropolitana "
            "contiene millones de vértices y no puede "
            "visualizarse de forma interactiva en Streamlit."
        )

        if st.button(
            "Generar mapa RM",
            key="coverage_rm_btn",
            type="primary",
            icon=":material/map_search:"
        ):

            mode_label = (
                "transporte público"
                if transit_mode
                else "caminata"
            )

            with st.spinner(
                f"Calculando cobertura RM ({mode_label})..."
            ):

                rm_result = call_population_coverage_rm(
                    comunas=[
                        normalize_comuna(c)
                        for c in comunas_disponibles
                    ],
                    minutes=coverage_minutes,
                    mode="transit" if transit_mode else "walk",
                    departure_hour=departure_hour if transit_mode else None
                )

                if rm_result:
                    st.session_state.rm_result = rm_result
                    st.session_state.stats_rm = calculate_coverage_statistics(rm_result)
                    st.session_state.png_buffer = (
                        export_full_population_coverage_map(
                            rm_result,
                            minutes=coverage_minutes
                        )
                    )

                else:
                    st.session_state.rm_result = None
                    st.session_state.stats_rm = None
                    st.session_state.png_buffer = None

        if st.session_state.get("rm_result"):
            render_coverage_statistics(st.session_state.stats_rm)

            if st.session_state.get("png_buffer"):
                st.download_button(
                    label="Exportar a PNG",
                    data=st.session_state.png_buffer,
                    file_name=(
                        f"cobertura_rm_"
                        f"{coverage_minutes}min.png"
                        ),
                    mime="image/png",
                    icon=":material/file_export:",
                    type="primary",
                    width="stretch"
                )