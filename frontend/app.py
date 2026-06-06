from ui_components import (
    render_location_picker,
    call_isochrone_api,
    process_isochrone_result,
    call_health_desert_api,
    render_health_deserts,
    handle_cleanup
)

import streamlit as st
import unicodedata

def remove_accents(text: str) -> str:
    normalized = unicodedata.normalize('NFD', text)
    return ''.join(ch for ch in normalized if unicodedata.category(ch) != "Mn")

comunas_disponibles = sorted([
    "Alhué", "Buin", "Calera de Tango", "Cerrillos", "Cerro Navia", "Colina", "Conchalí", 
    "Curacaví", "El Bosque", "El Monte", "Estación Central", "Huechuraba", "Independencia", 
    "Isla de Maipo", "La Cisterna", "La Florida", "La Granja", "La Pintana", "La Reina", 
    "Lampa", "Las Condes", "Lo Barnechea", "Lo Espejo", "Lo Prado", "Macul", "Maipú", 
    "María Pinto", "Melipilla", "Ñuñoa", "Padre Hurtado", "Paine", "Pedro Aguirre Cerda", 
    "Peñaflor", "Peñalolén", "Pirque", "Providencia", "Pudahuel", "Puente Alto", 
    "Quilicura", "Quinta Normal", "Recoleta", "Renca", "San Bernardo", "San Joaquín", 
    "San José de Maipo", "San Miguel", "San Pedro", "San Ramón", "Santiago", "Talagante", 
    "Til Til", "Vitacura"
])

comuna_map = {
    comuna: remove_accents(comuna)
    for comuna in comunas_disponibles
}

st.title("Accesibilidad a Centros de Salud en la Región Metropolitana")

iso_tab, desert_tab = st.tabs(["Isócronas", "Desiertos de Salud"])

with iso_tab:
    st.subheader("Mapa de Isócronas")
    st.caption("Haz click en cualquier punto del mapa para definir el punto de partida de la isócrona")

    col_iso_map, col_iso_slider = st.columns([2,1])

    with col_iso_map:
        lat, lon = render_location_picker(map_key="picker_iso", origin_key="iso_origin")
    with col_iso_slider:
        isochrone_minutes = st.slider("Minutos", 30, 60, 30, key="iso_minutes")
  
        st.metric("Latitud", f"{lat:.5f}")
        st.metric("Longitud", f"{lon:.5f}")

    col_iso_btn1, col_iso_btn2 = st.columns(2)
    with col_iso_btn1:
        if st.button("Calcular isócronas", type="primary", key="calc_iso", icon=":material/map_search:", width="stretch"):
            st.session_state.iso_walk_result = call_isochrone_api("/api/v1/isochrone", lat, lon, isochrone_minutes, spinner_text="Calculando isócrona caminando {} minutos...".format(isochrone_minutes))
            st.session_state.iso_transit_result = call_isochrone_api("/api/v1/transit/isochrone", lat, lon, isochrone_minutes, spinner_text="Calculando isócrona usando transporte público durante {} minutos...".format(isochrone_minutes))

    with col_iso_btn2:
        if st.button("Limpiar mapas", key="clear_iso", icon=":material/delete:", width="stretch"):
            handle_cleanup("iso_walk_result")
            handle_cleanup("iso_transit_result")
        
    if st.session_state.get("iso_walk_result") and st.session_state.get("iso_transit_result"):
        st.subheader('Caminando {} minutos'.format(isochrone_minutes))
        process_isochrone_result(st.session_state.iso_walk_result, isochrone_minutes, mode="walk", map_key="iso_walk_map")
        st.subheader('Usando transporte público durante {} minutos'.format(isochrone_minutes))
        process_isochrone_result(st.session_state.iso_transit_result, isochrone_minutes, mode="transit", map_key="iso_transit_map")

with desert_tab:
    st.subheader("Desiertos de Salud por Comuna")
    st.caption("Selecciona una comuna y un tiempo estimado para calcular los desiertos de salud")

    col_des_comuna, col_des_slider = st.columns([2,1])
    with col_des_comuna:
        desert_comuna = st.selectbox("Comuna a calcular", comunas_disponibles, key="desert_comuna")
        query_comuna = comuna_map[desert_comuna]
    with col_des_slider:
        desert_minutes = st.slider("Minutos estimados", 30, 60, 30, key="desert_minutes")

    col_des_btn1, col_des_btn2 = st.columns(2)
    with col_des_btn1:
        if st.button("Calcular desiertos", type="primary", key="calc_desert", icon=":material/map_search:", width="stretch"):
            st.session_state.desert_walk_result = call_health_desert_api("/api/v1/health-deserts", query_comuna, desert_minutes, spinner_text="Calculando desiertos de salud caminando {} minutos...".format(desert_minutes))
            st.session_state.desert_transit_result = call_health_desert_api("/api/v1/transit/health-deserts", query_comuna, desert_minutes, spinner_text="Calculando desiertos de salud utilizando transporte público durante {} minutos...".format(desert_minutes))

    with col_des_btn2:
        if st.button("Limpiar mapas", key="clear_desert", icon=":material/delete:", width="stretch"):
            handle_cleanup("desert_walk_result")
            handle_cleanup("desert_transit_result")

    if st.session_state.get("desert_walk_result") and st.session_state.get("desert_transit_result"):
        st.subheader('Caminando {} minutos'.format(desert_minutes))
        render_health_deserts(st.session_state.desert_walk_result, map_key="desert_walk_map")
        st.subheader('Usando transporte público durante {} minutos'.format(desert_minutes))
        render_health_deserts(st.session_state.desert_transit_result, map_key="desert_transit_map")