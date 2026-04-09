import pandas as pd
import numpy as np
from pathlib import Path


def clean_df_for_json(df):
    """Limpia valores no serializables en JSON (NaN, inf, -inf)"""
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == 'float64' or df[col].dtype == 'float32':
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)
    df = df.replace({np.nan: None, pd.NA: None, 'NaN': None, 'nan': None, '': None})
    return df


def remove_empty_columns(df):
    """Elimina columnas que están completamente vacías"""
    df = df.copy()
    empty_cols = []
    for col in df.columns:
        non_null = df[col].notna().sum()
        if non_null == 0:
            empty_cols.append(col)
    if empty_cols:
        df = df.drop(columns=empty_cols)
    return df


def remove_columns_with_all_none(df):
    """Elimina columnas donde todos los valores son None/null después de la limpieza"""
    df = df.copy()
    cols_to_drop = []
    for col in df.columns:
        all_none = df[col].isna().all()
        if all_none:
            cols_to_drop.append(col)
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)
    return df


def standardize_string_columns(df):
    """Estandariza columnas de texto: strip y title para nombres"""
    df = df.copy()
    text_cols = [col for col in df.columns if 'name' in col.lower() and df[col].dtype == 'object']
    for col in text_cols:
        df[col] = df[col].astype(str).str.strip().str.title()
    return df


def convert_id_columns(df):
    """Convierte columnas ID a string"""
    df = df.copy()
    id_cols = [col for col in df.columns if 'id' in col.lower()]
    for col in id_cols:
        df[col] = df[col].astype(str)
    return df


def clean_dataset(df, dataset_type=None):
    """
    Limpia un dataset según su tipo.
    dataset_type: 'gtfs_stops', 'gtfs_routes', 'gtfs_trips', 'gtfs_stop_times', 'generic'
    """
    df = df.copy()
    
    df = remove_empty_columns(df)
    df = convert_id_columns(df)
    df = standardize_string_columns(df)
    df = clean_df_for_json(df)
    df = remove_columns_with_all_none(df)
    
    return df


def load_csv_dataset(folder_path, file_name):
    """Carga un archivo CSV desde una carpeta de dataset"""
    path = Path(folder_path) / file_name
    if path.exists():
        return pd.read_csv(path)
    return None


def list_csv_files(folder_path):
    """Lista todos los archivos CSV en una carpeta"""
    path = Path(folder_path)
    if path.exists():
        return [f.name for f in path.glob("*.txt")]
    return []