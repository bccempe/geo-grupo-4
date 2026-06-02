import os
import time
import osmnx as ox
import networkx as nx
from concurrent.futures import ThreadPoolExecutor, as_completed

#  Carpeta donde guardar
DATA_DIR = "/data/osm"
os.makedirs(DATA_DIR, exist_ok=True)

#  Configuración OSMnx
ox.settings.use_cache = True
ox.settings.log_console = True
ox.settings.timeout = 180

# Comunas Región Metropolitana de Santiago (RM) - Chile
COMUNAS_RM = [
    # Provincia de Santiago
    "Santiago, Chile",
    "Conchalí, Chile",
    "Huechuraba, Chile",
    "Independencia, Chile",
    "Quilicura, Chile",
    "Recoleta, Chile",
    "Renca, Chile",
    "Las Condes, Chile",
    "Lo Barnechea, Chile",
    "Providencia, Chile",
    "Vitacura, Chile",
    "La Reina, Chile",
    "Macul, Chile",
    "Ñuñoa, Chile",
    "Peñalolén, Chile",
    "La Florida, Chile",
    "La Granja, Chile",
    "El Bosque, Chile",
    "La Cisterna, Chile",
    "La Pintana, Chile",
    "San Ramón, Chile",
    "Lo Espejo, Chile",
    "Pedro Aguirre Cerda, Chile",
    "San Joaquín, Chile",
    "San Miguel, Chile",
    "Cerrillos, Chile",
    "Estación Central, Chile",
    "Maipú, Chile",
    "Cerro Navia, Chile",
    "Lo Prado, Chile",
    "Pudahuel, Chile",
    "Quinta Normal, Chile",

    # Provincia de Cordillera
    "Puente Alto, Chile",
    "San José de Maipo, Chile",
    "Pirque, Chile",

    # Provincia de Chacabuco
    "Colina, Chile",
    "Lampa, Chile",
    "Til Til, Chile",

    # Provincia de Maipo
    "San Bernardo, Chile",
    "Buin, Chile",
    "Calera de Tango, Chile",
    "Paine, Chile",

    # Provincia de Melipilla
    "Melipilla, Chile",
    "Alhué, Chile",
    "Curacaví, Chile",
    "María Pinto, Chile",
    "San Pedro, Chile",

    # Provincia de Talagante
    "Talagante, Chile",
    "El Monte, Chile",
    "Isla de Maipo, Chile",
    "Padre Hurtado, Chile",
    "Peñaflor, Chile",
]

# -----------------------------
#  OBTENER POLÍGONO (OPCIÓN A)
# -----------------------------
def obtener_poligono_comuna(comuna):

    gdf = ox.geocode_to_gdf(comuna)

    poly = gdf.geometry.iloc[0]

    #  buffer pequeño para evitar cortes en bordes
    poly = poly.buffer(0.01)  # aprox ~1km (depende latitud)

    return poly


# -----------------------------
#  DESCARGA DE GRAFO 
# -----------------------------
def descargar_comuna(comuna, reintentos=3):

    nombre = comuna.replace(", Chile", "").replace(" ", "_")
    path = f"{DATA_DIR}/{nombre}.graphml"

    if os.path.exists(path):
        print(f" Ya existe: {comuna}")
        return path

    for intento in range(reintentos):
        try:
            print(f" Descargando {comuna} (intento {intento+1})")


            poly = obtener_poligono_comuna(comuna)

            G = ox.graph_from_polygon(
                poly,
                network_type="walk",
                simplify=True
            )

            #  limpieza topológica
            G = ox.utils_graph.get_largest_component(G, strongly=True)

            ox.save_graphml(G, path)

            print(f" Guardado: {path}")
            return path

        except Exception as e:
            print(f" Error en {comuna}: {e}")
            time.sleep(5 * (intento + 1))

    print(f" Falló definitivamente: {comuna}")
    return None


# -----------------------------
# DESCARGA MASIVA
# -----------------------------
def descargar_todo():
    resultados = []

    with ThreadPoolExecutor(max_workers=1) as executor:
        futures = [executor.submit(descargar_comuna, c) for c in COMUNAS_RM]

        for future in as_completed(futures):
            resultados.append(future.result())

    return resultados


# -----------------------------
#  DEBUG DE GRAFOS
# -----------------------------
def debug_grafo(path, comuna):

    print(f"\n DEBUG: {comuna}")

    G = ox.load_graphml(path)

    num_nodos = len(G.nodes)
    num_aristas = len(G.edges)

    componentes = list(nx.weakly_connected_components(G))
    num_componentes = len(componentes)

    mayor = max(len(c) for c in componentes)

    fragmentacion = (num_nodos - mayor) / num_nodos * 100

    print(f"Nodos: {num_nodos}")
    print(f" Aristas: {num_aristas}")
    print(f" Componentes desconectados: {num_componentes}")
    print(f" Mayor componente: {mayor} nodos")
    print(f" Fragmentación: {fragmentacion:.2f}%")


    return {
        "comuna": comuna,
        "nodos": num_nodos,
        "aristas": num_aristas,
        "componentes": num_componentes,
        "fragmentacion": fragmentacion
    }


# -----------------------------
#  PIPELINE PRINCIPAL
# -----------------------------
if __name__ == "__main__":

    print("Iniciando descarga por comunas...")

    paths = descargar_todo()

    print("\n Iniciando debug de grafos...\n")

    resultados = []

    for comuna, path in zip(COMUNAS_RM, paths):
        if path:
            res = debug_grafo(path, comuna)
            resultados.append(res)

    print("\nProceso terminado")