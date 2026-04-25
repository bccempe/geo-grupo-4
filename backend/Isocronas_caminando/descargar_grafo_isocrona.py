import os
import time
import osmnx as ox

#  inicio
start_time = time.time()

print(" Iniciando proceso...")

#  Carpeta de salida
output_dir = "data/osm"
print(f" Creando/verificando carpeta: {output_dir}")
os.makedirs(output_dir, exist_ok=True)

#  Lugar
place = "Santiago, Chile"
print(f" Lugar definido: {place}")

#  Configuración OSMnx
print(" Configurando OSMnx (all_oneway=True)...")
ox.settings.all_oneway = True

#  Descargar red
print("⬇ Descargando red caminable desde OSM...")
download_start = time.time()

G_walk = ox.graph_from_place(
    place,
    network_type="walk",
    simplify=False
)

download_end = time.time()
print(f"Descarga completada en {download_end - download_start:.2f} segundos")

#  Info del grafo
print(" Información del grafo:")
print(f"   - Nodos: {len(G_walk.nodes)}")
print(f"   - Aristas: {len(G_walk.edges)}")

#  Guardar archivo
walk_path = os.path.join(output_dir, "santiago_walk.osm")
print(f" Guardando archivo en: {walk_path}")

save_start = time.time()

try:
    ox.save_graph_xml(G_walk, filepath=walk_path)
    print(" Archivo guardado correctamente")
except Exception as e:
    print(" Error al guardar el archivo:")
    print(e)

save_end = time.time()
print(f" Tiempo de guardado: {save_end - save_start:.2f} segundos")

#  fin total
end_time = time.time()
print(f" Proceso finalizado en {end_time - start_time:.2f} segundos")