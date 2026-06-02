import subprocess
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
import time
import traceback
import geopandas as gpd
import os

DATA_DIR = Path("/data")


DB_URL = os.getenv("DATABASE_URL")


DB_HOST = os.getenv("POSTGRES_HOST", "db")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")


OGR_CONN = (
    f"PG:host={DB_HOST} "
    f"user={DB_USER} "
    f"dbname={DB_NAME} "
    f"password={DB_PASSWORD}"
)

def log(msg):
    print(f"[ETL-DEBUG] {msg}", flush=True)


CARPETAS_ESPERADAS = [
    "GTFS_20260321_v3",
    "datos_minsal_establecimientos_salud",
    "datos_censo",
]

ESPERA_MAX = 600
INTERVALO = 10


def esperar_datos():
    log("Iniciando espera de datos descargados...")

    if not DATA_DIR.exists():
        DATA_DIR.mkdir(parents=True, exist_ok=True)

    tiempo_espera = 0
    carpetas_faltantes = set(CARPETAS_ESPERADAS)

    while carpetas_faltantes and tiempo_espera < ESPERA_MAX:
        carpetas_encontradas = {p.name for p in DATA_DIR.iterdir() if p.is_dir()}
        carpetas_faltantes = set(CARPETAS_ESPERADAS) - carpetas_encontradas

        if carpetas_faltantes:
            log(f"Carpetas pendientes: {carpetas_faltantes} | espera: {tiempo_espera}s")
            time.sleep(INTERVALO)
            tiempo_espera += INTERVALO

    if carpetas_faltantes:
        raise TimeoutError(f"Timeout: no se completaron las descargas. Faltan: {carpetas_faltantes}")

    log(f"Todas las carpetas esperadas descargadas. Total: {len(CARPETAS_ESPERADAS)}")
    log(f"Carpetas: {CARPETAS_ESPERADAS}")


def esperar_postgis():
    log("Esperando conexión con PostGIS...")
    while True:
        try:
            engine = create_engine(DB_URL)
            with engine.connect():
                log("Conexión con PostGIS exitosa ")
                return
        except Exception as e:
            log(f"PostGIS aún no disponible: {str(e)}")
            time.sleep(5)


def importar_tabla_sin_geom(archivo, tabla):
    try:
        df = pd.read_csv(archivo)
        engine = create_engine(DB_URL)
        df.to_sql(tabla, engine, if_exists="replace", index=False)
        log(f" Tabla no espacial importada -> {tabla}")
    except Exception as e:
        log(f" ERROR tabla no espacial: {e}")



def importar_vector(archivo):
    tabla_base = archivo.stem.lower().replace(" ", "_")
    ext = archivo.suffix.lower()

    log(f"Importando VECTOR: {archivo}")

    try:
        capas = []

        if ext == ".gpkg":
            resultado = subprocess.check_output(
                ["ogrinfo", str(archivo)],
                text=True,
                stderr=subprocess.STDOUT
            )

            for linea in resultado.splitlines():
                linea = linea.strip()


                if linea and linea[0].isdigit() and ":" in linea:
                    try:
                        parte = linea.split(":", 1)[1].strip()

                    
                        capa = parte.split("(")[0].strip()

                        if not capa:
                            continue

                   
                        if any(x in capa.lower() for x in ["style", "metadata"]):
                            continue

                        capas.append(capa)

                    except:
                        continue

        #  SHP y otros → una sola capa
        if not capas:
            capas = [None]

        #  recorrer capas
        for capa in capas:

            tabla = f"{tabla_base}_{capa.lower()}" if capa else tabla_base

            if archivo.stem in [
                "Cartografia_censo2024_R13",
                "Cartografia_censo2024_Pais.gdb",
                "Cartografia_censo2024_Pais"
            ]:
                tipo_geom = "MULTISURFACE"
            else:
                tipo_geom = "PROMOTE_TO_MULTI"

            comando = [
                "ogr2ogr",
                "-f", "PostgreSQL",
                OGR_CONN,
                str(archivo)
            ]

            if capa:
                comando.append(capa)

            comando += [
                "-nln", tabla,
                "-nlt", tipo_geom,
                "-makevalid",
                "-dim", "XY",
                "-overwrite"
            ]

            log("--------------------------------------------------")
            log(f"Capa: {capa if capa else 'UNICA'}")
            log(f"Tabla destino: {tabla}")
            log(f"Tipo geometría: {tipo_geom}")
            log(f"Comando: {' '.join(comando)}")

            subprocess.run(comando, check=True)

            log(f" Importado correctamente -> {tabla}")

    except Exception as e:
        log(f" ERROR importando vector {archivo}: {e}")
        traceback.print_exc()



def importar_gtfs(archivo):
    tabla = f"gtfs_{archivo.stem.lower()}"

    log(f"Importando GTFS TXT: {archivo}")
    log(f"Tabla destino: {tabla}")

    try:
        engine = create_engine(DB_URL)
        df = pd.read_csv(archivo)

        log(f"Filas detectadas en {archivo.name}: {len(df)}")
        log(f"Columnas: {list(df.columns)}")

        df.to_sql(tabla, engine, if_exists="replace", index=False)

        log(f"GTFS importado correctamente  -> {tabla}")

    except Exception as e:
        log(f"ERROR importando GTFS {archivo}: {e}")
        traceback.print_exc()


def recorrer_archivos():
    log("Iniciando recorrido de archivos...")
    total_importados = 0

    for archivo in DATA_DIR.rglob("*"):
        if not archivo.is_file():
            continue

        ext = archivo.suffix.lower()
        log(f"Archivo detectado: {archivo} | extensión: {ext}")

        if ext in [".gpkg", ".shp", ".geojson"]:
            importar_vector(archivo)
            total_importados += 1

        elif ext == ".txt":
            importar_gtfs(archivo)
            total_importados += 1

        else:
            log(f"Ignorado (no compatible): {archivo.name}")

    log(f"Proceso ETL finalizado. Total datasets importados: {total_importados}")


if __name__ == "__main__":
    log("===== INICIO ETL AUTOIMPORT =====")
    esperar_datos()
    esperar_postgis()
    recorrer_archivos()
    log("===== DB POBLADA COMPLETAMENTE =====")