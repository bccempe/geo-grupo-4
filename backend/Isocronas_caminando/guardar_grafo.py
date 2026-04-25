import os
import time
import subprocess
import psycopg2
from dotenv import load_dotenv
import osmnx as ox

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

OSM_PATH = "/data/osm/santiago_walk.osm"

# =======================
# DB WAIT
# =======================
def wait_for_db():
    print("⏳ Esperando DB...")
    while True:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.close()
            print("✅ DB lista")
            break
        except:
            time.sleep(2)

# =======================
# DOWNLOAD OSM
# =======================
def download_osm():
    if os.path.exists(OSM_PATH):
        print("⚠️ OSM ya existe, se reutiliza")
        return

    print("⬇️ Descargando red caminable...")
    os.makedirs("/data/osm", exist_ok=True)

    ox.settings.all_oneway = True

    G = ox.graph_from_place(
        "Santiago, Chile",
        network_type="walk",
        simplify=False
    )

    ox.save_graph_xml(G, filepath=OSM_PATH)
    print("✅ OSM descargado")

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
    print("🚀 Importando a PostgreSQL...")

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
    print("✅ Grafo creado")

# =======================
# COST
# =======================
def add_cost():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        ALTER TABLE ways
        ADD COLUMN IF NOT EXISTS cost DOUBLE PRECISION;

        ALTER TABLE ways
        ADD COLUMN IF NOT EXISTS reverse_cost DOUBLE PRECISION;

        UPDATE ways
        SET cost = (ST_Length(geom::geography)/1000.0)/5.0*60,
            reverse_cost = cost;
    """)

    conn.commit()
    conn.close()
    print("✅ Costos listos")

# =======================
# MAIN
# =======================
print("🚀 Iniciando pipeline completo...")

wait_for_db()
download_osm()

if table_exists():
    print("⚠️ Grafo ya existe, se omite importación")
else:
    run_osm2pgrouting()
    add_cost()

print("🏁 Pipeline terminado")