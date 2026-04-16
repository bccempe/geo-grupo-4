import streamlit as st
import requests
import os

API_URL = os.getenv("API_URL", "http://localhost:8000")

st.title("Accesibilidad a Salud RM")

tab1, tab2, tab3 = st.tabs(["Datasets", "Centros de Salud", "Censo"])

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
    if st.button("Cargar centros"):
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