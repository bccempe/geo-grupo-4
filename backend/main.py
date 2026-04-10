from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "backend"))

from utils.data_loader import (
    get_available_datasets,
    get_dataset_files,
    load_cleaned_dataset,
    get_data_summary
)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return {"msg": "API Salud RM funcionando"}


@app.get("/centros")
def get_centros():
    return {"data": "centros de salud (placeholder)"}


@app.get("/datasets")
def list_datasets():
    """Lista todos los datasets disponibles"""
    return {
        "datasets": get_available_datasets()
    }


@app.get("/datasets/{dataset_name}")
def get_dataset_info(dataset_name: str):
    """Lista archivos en un dataset"""
    files = get_dataset_files(dataset_name)
    if not files:
        return {"error": f"Dataset {dataset_name} no encontrado"}
    return {
        "dataset": dataset_name,
        "files": files
    }


@app.get("/datasets/{dataset_name}/{file_name}")
def get_dataset_file(dataset_name: str, file_name: str, limit: int = None):
    """Obtiene los datos de un archivo específico"""
    df = load_cleaned_dataset(dataset_name, file_name)
    if df is None:
        return {"error": f"Archivo {file_name} no encontrado en dataset {dataset_name}"}
    
    if limit:
        df = df.head(limit)
    
    return df.to_dict(orient="records")


@app.get("/datasets/{dataset_name}/{file_name}/head")
def get_dataset_file_head(dataset_name: str, file_name: str, n: int = 10):
    """Obtiene los primeros n registros de un archivo"""
    df = load_cleaned_dataset(dataset_name, file_name)
    if df is None:
        return {"error": f"Archivo {file_name} no encontrado en dataset {dataset_name}"}
    
    return df.head(n).to_dict(orient="records")


@app.get("/summary")
def get_summary():
    """Resumen de todos los datasets y sus archivos"""
    return get_data_summary()