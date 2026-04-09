import streamlit as st
import requests
import os

API_URL = os.getenv("API_URL", "http://localhost:8000")

st.title("Accesibilidad a Salud RM")

if st.button("Cargar centros"):
    res = requests.get(f"{API_URL}/centros")
    st.json(res.json())