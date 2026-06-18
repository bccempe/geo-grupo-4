from ui_components import (
    render_location_picker,
    call_isochrone_api,
    process_isochrone_result,
    call_health_desert_api,
    render_health_deserts,
    call_population_coverage_api,
    render_population_coverage,
    call_population_coverage_rm,
)

from export import export_health_desert_map

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
    "Desiertos de Salud",
    "Cobertura Poblacional"
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

    if st.session_state.get("iso_walk_result") and st.session_state.get("iso_transit_result"):
        st.subheader(f"Caminando {isochrone_minutes} minutos")
        process_isochrone_result(
            st.session_state.iso_walk_result,
            isochrone_minutes,
            mode="walk",
            map_key="iso_walk_map"
        )

        st.subheader(f"Usando transporte público durante {isochrone_minutes} minutos")
        process_isochrone_result(
            st.session_state.iso_transit_result,
            isochrone_minutes,
            mode="transit",
            map_key="iso_transit_map"
        )

with desert_tab:
    st.subheader("Desiertos de Salud por Comuna")
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

    st.subheader("Cobertura Poblacional")

    coverage_mode = st.radio(
        "Modo",
        [
            "Comuna",
            "Región Metropolitana"
        ],
        horizontal=True
    )

    coverage_minutes = st.slider(
        "Minutos",
        5,
        60,
        15,
        key="coverage_minutes"
    )

    # ==================================================
    # COBERTURA POR COMUNA
    # ==================================================
    if coverage_mode == "Comuna":

        coverage_comuna = st.selectbox(
            "Comuna",
            comunas_disponibles,
            key="coverage_comuna"
        )

        query_comuna = normalize_comuna(
            coverage_comuna
        )

        if st.button(
            "Calcular cobertura",
            key="coverage_btn"
        ):

            st.session_state.coverage_result = (
                call_population_coverage_api(
                    endpoint="/api/v1/population/coverage",
                    comuna=query_comuna,
                    minutes=coverage_minutes,
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
            key="coverage_rm_btn"
        ):

            with st.spinner(
                "Calculando cobertura de toda la RM..."
            ):

                rm_result = call_population_coverage_rm(
                    comunas=[
                        normalize_comuna(c)
                        for c in comunas_disponibles
                    ],
                    minutes=coverage_minutes
                )

                if rm_result:

                    from export_population_coverage import (
                        export_population_coverage_map
                    )

                    png_buffer = (
                        export_population_coverage_map(
                            rm_result,
                            minutes=coverage_minutes,
                            title="Región Metropolitana"
                        )
                    )

                    st.success(
                        "Mapa generado correctamente."
                    )

                    st.download_button(
                        label="Descargar mapa PNG",
                        data=png_buffer,
                        file_name=(
                            f"cobertura_rm_"
                            f"{coverage_minutes}min.png"
                        ),
                        mime="image/png",
                        use_container_width=True
                    )