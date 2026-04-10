import pandas as pd
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import clean_dataset

DATA_BASE_PATH = os.getenv("DATA_BASE_PATH", "backend/data")


def get_available_datasets():
    """Lista todas las carpetas de datasets disponibles"""
    base = Path(DATA_BASE_PATH)
    if not base.exists():
        return []
    return [d.name for d in base.iterdir() if d.is_dir()]


def get_dataset_files(dataset_name):
    """Lista los archivos disponibles en un dataset"""
    dataset_path = Path(DATA_BASE_PATH) / dataset_name
    if not dataset_path.exists():
        return []
    return [f.name for f in dataset_path.iterdir() if f.is_file() and f.suffix == '.txt']


def load_raw_dataset(dataset_name, file_name):
    """Carga un archivo CSV raw de un dataset"""
    path = Path(DATA_BASE_PATH) / dataset_name / file_name
    if path.exists():
        return pd.read_csv(path)
    return None


def load_cleaned_dataset(dataset_name, file_name):
    """Carga y limpia un dataset"""
    df = load_raw_dataset(dataset_name, file_name)
    if df is not None:
        df = clean_dataset(df)
    return df


def get_data_summary():
    """Resumen de todos los datasets disponibles"""
    datasets = {}
    for name in get_available_datasets():
        files = get_dataset_files(name)
        datasets[name] = {}
        for f in files:
            df = load_raw_dataset(name, f)
            if df is not None:
                datasets[name][f] = {
                    "rows": len(df),
                    "columns": len(df.columns)
                }
    return datasets


if __name__ == "__main__":
    print("=" * 50)
    print("Data Loader - Available Datasets")
    print("=" * 50)
    
    datasets = get_available_datasets()
    print(f"\nDatasets disponibles: {datasets}")
    
    for ds in datasets:
        print(f"\n{ds}:")
        files = get_dataset_files(ds)
        for f in files:
            print(f"  - {f}")