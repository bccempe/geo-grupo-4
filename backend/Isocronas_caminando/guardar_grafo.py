import os
import time
import subprocess
import psycopg2
from dotenv import load_dotenv
import osmnx as ox
from sqlalchemy import create_engine


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

OSM_PATH = "/data/osm/rm_walk.osm"

# =======================
# DB WAIT
# =======================
def wait_for_db():
    print(" Esperando DB...")
    while True:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.close()
            print(" DB lista")
            break
        except:
            time.sleep(2)

# =======================
# DOWNLOAD OSM
# =======================
def download_osm():
    if os.path.exists(OSM_PATH):
        print(" OSM ya existe, se reutiliza")
        return

    print("⬇ Descargando red caminable...")
    os.makedirs("/data/osm", exist_ok=True)

    ox.settings.all_oneway = True

    G = ox.graph_from_place(
        "Santiago, Chile",
        network_type="walk",
        simplify=False
    )

    ox.save_graph_xml(G, filepath=OSM_PATH)
    print(" OSM descargado")

# =======================
# CHECK TABLE
# =======================
def table_exists():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'ways'
        );
    """)

    exists = cur.fetchone()[0]
    conn.close()
    return exists

# =======================
# IMPORT
# =======================
def run_osm2pgrouting():
    print(" Importando a PostgreSQL...")

    cmd = [
        "osm2pgrouting",
        "-f", OSM_PATH,
        "-d", DB_CONFIG["dbname"],
        "-U", DB_CONFIG["user"],
        "-W", DB_CONFIG["password"],
        "-h", DB_CONFIG["host"],
        "-p", DB_CONFIG["port"],
        "--clean"
    ]

    subprocess.run(cmd, check=True)
    print(" Grafo creado")


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
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
)

# =======================
# WAIT DB
# =======================
def wait_for_db():
    print(" Esperando DB...")
    while True:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.close()
            print(" DB lista")
            break
        except:
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
        raise RuntimeError("No hay archivos .graphml")

    print(f"{len(files)} grafos encontrados")
    return files

# =======================
# COSTOS EN POSTGIS
# =======================
def add_cost_to_table(table_name):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    print(f"Calculando costos en {table_name}...")

    cur.execute(f"""
        ALTER TABLE {table_name}
        ADD COLUMN IF NOT EXISTS cost DOUBLE PRECISION;

        ALTER TABLE {table_name}
        ADD COLUMN IF NOT EXISTS reverse_cost DOUBLE PRECISION;

        UPDATE {table_name}
        SET cost = (ST_Length(geometry::geography)/1000.0)/5.0*60,
            reverse_cost = cost;
    """)

    conn.commit()
    conn.close()

    print(f" Costos listos en {table_name}")

# =======================
# CARGAR A POSTGIS
# =======================
def save_graph_to_postgis(path):
    nombre = os.path.basename(path).replace(".graphml", "").lower()

    print(f"⬆ Procesando {nombre}")

    G = ox.load_graphml(path)

    # convertir a GeoDataFrames
    nodes, edges = ox.graph_to_gdfs(G)

    # nombres de tabla
    nodes_table = f"{nombre}_nodes"
    edges_table = f"{nombre}_edges"

    # guardar
    nodes.to_postgis(nodes_table, ENGINE, if_exists="replace", index=True)
    edges.to_postgis(edges_table, ENGINE, if_exists="replace", index=True)

    print(f" Guardado en: {nodes_table}, {edges_table}")

    #  calcular costos en edges
    add_cost_to_table(edges_table)

# =======================
# MAIN
# =======================
if __name__ == "__main__":
    print(" Cargando grafos a PostGIS...")

    wait_for_db()

    files = get_graph_files()

    for f in files:
        try:
            save_graph_to_postgis(f)
        except Exception as e:
            print(f" Error con {f}: {e}")

    print(" Proceso terminado")