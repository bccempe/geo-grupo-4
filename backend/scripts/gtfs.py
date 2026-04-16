import pandas as pd
import os
from pathlib import Path

gtfs_path = os.getenv("GTFS_DATA_PATH", "backend/data/GTFS_20260321_v3")
DATA_PATH = Path(gtfs_path) if isinstance(gtfs_path, str) else gtfs_path


def get_data_from_source():
    """Cargar archivos GTFS reales"""
    files = ["stops", "stop_times", "routes", "trips"]
    data = {}
    for f in files:
        path = DATA_PATH / f"{f}.txt"
        if path.exists():
            data[f] = pd.read_csv(path)
            print(f"Cargado {f}: {len(data[f])} registros")
    return data


def get_head(data, table, n=10):
    """Obtener head de una tabla específica"""
    if table in data:
        return data[table].head(n)
    return None


if __name__ == "__main__":
    from utils import normalize_gtfs
    
    print("=" * 50)
    print("GTFS Data Processing Script")
    print("=" * 50)
    
    print("\n1. Cargando datos...")
    raw_data = get_data_from_source()
    
    print("\n2. Normalizando datos...")
    cleaned_data = normalize_gtfs(raw_data)
    
    print("\n3. Resumen:")
    for name, df in cleaned_data.items():
        print(f"   {name}: {len(df)} registros")
    
    print("\n4. Preview - Stops (head):")
    print(cleaned_data["stops"].head())