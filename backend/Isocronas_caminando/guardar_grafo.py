import os
import re
import time
import unicodedata
import subprocess

import psycopg2
import osmnx as ox

from dotenv import load_dotenv
from sqlalchemy import create_engine


# =======================
# NORMALIZADOR
# =======================
def normalize_name(text: str) -> str:
    """
    Convierte:
    - minúsculas
    - elimina tildes
    - espacios -> _
    - elimina caracteres raros
    """

    if text is None:
        return ""

    text = text.strip().lower()

    # eliminar tildes
    text = unicodedata.normalize("NFD", text)

    text = "".join(
        c
        for c in text
        if unicodedata.category(c) != "Mn"
    )

    # reemplazar espacios por _
    text = re.sub(r"\s+", "_", text)

    # dejar solo letras numeros y _
    text = re.sub(r"[^a-z0-9_]", "", text)

    return text


# =======================
# CONFIG
# =======================
load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": "db",
    "port": "5432"
}

DATA_DIR = "/data/osm"

ENGINE = create_engine(
    f"postgresql://"
    f"{DB_CONFIG['user']}:"
    f"{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:"
    f"{DB_CONFIG['port']}/"
    f"{DB_CONFIG['dbname']}"
)


# =======================
# WAIT DB
# =======================
def wait_for_db():

    print("Esperando DB...")

    while True:

        try:

            conn = psycopg2.connect(**DB_CONFIG)
            conn.close()

            print("DB lista")

            break

        except Exception:

            time.sleep(2)


# =======================
# LISTAR ARCHIVOS
# =======================
def get_graph_files():

    files = [
        os.path.join(DATA_DIR, f)
        for f in os.listdir(DATA_DIR)
        if f.endswith(".graphml")
    ]

    if not files:

        raise RuntimeError(
            "No hay archivos .graphml"
        )

    print(f"{len(files)} grafos encontrados")

    return files


# =======================
# COSTOS
# =======================
def add_cost_to_table(table_name):

    conn = psycopg2.connect(**DB_CONFIG)

    cur = conn.cursor()

    print(f"Calculando costos en {table_name}...")

    cur.execute(f"""
        ALTER TABLE "{table_name}"
        ADD COLUMN IF NOT EXISTS cost DOUBLE PRECISION;

        ALTER TABLE "{table_name}"
        ADD COLUMN IF NOT EXISTS reverse_cost DOUBLE PRECISION;

        UPDATE "{table_name}"
        SET cost = (
            ST_Length(geometry::geography)/1000.0
        ) / 5.0 * 60,
        reverse_cost = (
            ST_Length(geometry::geography)/1000.0
        ) / 5.0 * 60;
    """)

    conn.commit()
    conn.close()

    print(f"Costos listos en {table_name}")


# =======================
# CARGAR GRAFO
# =======================
def save_graph_to_postgis(path):

    # nombre original
    original_name = (
        os.path.basename(path)
        .replace(".graphml", "")
    )

    # nombre normalizado
    nombre = normalize_name(original_name)

    print("===================================")
    print(f"Procesando: {original_name}")
    print(f"Normalizado: {nombre}")
    print("===================================")

    # cargar grafo
    G = ox.load_graphml(path)

    # convertir a GeoDataFrames
    nodes, edges = ox.graph_to_gdfs(G)

    # nombres de tablas
    nodes_table = f"{nombre}_nodes"
    edges_table = f"{nombre}_edges"

    # guardar en PostGIS
    nodes.to_postgis(
        nodes_table,
        ENGINE,
        if_exists="replace",
        index=True
    )

    edges.to_postgis(
        edges_table,
        ENGINE,
        if_exists="replace",
        index=True
    )

    print(f"Guardado:")
    print(f"- {nodes_table}")
    print(f"- {edges_table}")

    # calcular costos
    add_cost_to_table(edges_table)


# =======================
# MAIN
# =======================
if __name__ == "__main__":

    print("===================================")
    print("CARGANDO GRAFOS A POSTGIS")
    print("===================================")

    wait_for_db()

    files = get_graph_files()

    for f in files:

        try:

            save_graph_to_postgis(f)

        except Exception as e:

            print("===================================")
            print(f"ERROR CON {f}")
            print(e)
            print("===================================")

    print("Proceso terminado")