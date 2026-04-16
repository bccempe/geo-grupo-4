import pandas as pd
import os
from pathlib import Path

CENSO_DATA_PATH = os.getenv("CENSO_DATA_PATH", "./datos_censo_por_manzana_por_comuna")


code_to_comuna = {
    13101: "SANTIAGO_CENTRO",
    13102: "CERRILLOS",
    13103: "CERRO_NAVIA",
    13104: "CONCHALI",
    13105: "EL_BOSQUE",
    13106: "ESTACION_CENTRAL",
    13107: "HUECHURABA",
    13108: "INDEPENDENCIA",
    13109: "LA_CISTERNA",
    13110: "LA_FLORIDA",
    13111: "LA_GRANJA",
    13112: "LA_PINTANA",
    13113: "LA_REINA",
    13114: "LAS_CONDES",
    13115: "LO_BARNECHEA",
    13116: "LO_ESPEJO",
    13117: "LO_PRADO",
    13118: "MACUL",
    13119: "MAIPU",
    13120: "NUNOA",
    13121: "PEDRO_AGUIRRE_CERDA",
    13122: "PENALOLEN",
    13123: "PROVIDENCIA",
    13124: "PUDAHUEL",
    13125: "QUILICURA",
    13126: "QUINTA_NORMAL",
    13127: "RECOLETA",
    13128: "RENCA",
    13129: "SAN_JOAQUIN",
    13130: "SAN_MIGUEL",
    13131: "SAN_RAMON",
    13132: "VITACURA",
    13201: "PUENTE_ALTO",
    13202: "PIRQUE",
    13203: "SAN_JOSE_DE_MAIPO",
    13301: "COLINA",
    13302: "LAMPA",
    13303: "TILTIL",
    13401: "SAN_BERNARDO",
    13402: "BUIN",
    13403: "CALERA_DE_TANGO",
    13404: "PAINE",
    13501: "MELIPILLA",
    13502: "ALHUE",
    13503: "CURACAVI",
    13504: "MARIA_PINTO",
    13505: "SAN_PEDRO",
    13601: "TALAGANTE",
    13602: "EL_MONTE",
    13603: "ISLA_DE_MAIPO",
    13604: "PADRE_HURTADO",
    13605: "PENAFLOR",
}

def _get_default_cache_path():
    try:
        base = Path(CENSO_DATA_PATH)
        if base.exists() and base.is_dir():
            return base / "censo_data_cache.pkl"
    except Exception:
        pass
    return Path("censo_data_cache.pkl")


def _load_cache(path):
    try:
        return pd.read_pickle(path)
    except Exception as e:
        print(f"No se pudo cargar cache {path}: {e}")
        return None


def _save_cache(data, path):
    try:
        pd.to_pickle(data, path)
        print(f"Cache guardada en {path}")
    except Exception as e:
        print(f"No se pudo guardar cache {path}: {e}")


def get_data_from_source(use_cache=True, cache_path: str | Path | None = None, force_reload: bool = False):
    """Por cada uno de los archivos en la carpeta CENSO_DATA_PATH, carga los archivos, crea
    un DataFrame por comuna, y retorna un diccionario. Si `use_cache` es True intentará
    cargar/guardar un fichero cache para evitar reprocesar los archivos.
    """
    cache_file = Path(cache_path) if cache_path else _get_default_cache_path()

    # intentar cargar cache
    if use_cache and cache_file.exists() and not force_reload:
        print(f"Cargando datos desde cache: {cache_file}")
        data = _load_cache(cache_file)
        if isinstance(data, dict):
            return data
        print("Cache inválida, se procederá a reprocesar los archivos.")

    data = {}
    p = Path(CENSO_DATA_PATH)
    print(f"Buscando archivos en: {p.resolve()}")
    for file in p.rglob('*.xlsx'):
        print(f"Cargando {file}...")
        try:
            df = pd.read_excel(file)
        except Exception as e:
            print(f"Error leyendo {file}: {e}")
            continue
        if 'CUT' not in df.columns or df['CUT'].empty:
            print(f"Archivo {file} no tiene columna 'CUT' o está vacío, se omite.")
            continue
        comuna_code = df['CUT'].iloc[0]
        comuna_name = code_to_comuna.get(comuna_code, f"COMUNA_{comuna_code}")
        # intentar eliminar columnas si existen
        drop_cols = [
            'CONTENEDOR_COMUNAL', 'COD_REGION', 'REGION', 'CUT', 'COMUNA', 'n_hombres',
            'n_mujeres', 'n_edad_0_5', 'n_edad_6_13', 'n_edad_14_17', 'n_edad_18_24',
            'n_edad_25_44', 'n_edad_45_59', 'prom_edad', 'n_inmigrantes', 'n_nacionalidad',
            'n_pueblos_orig', 'n_afrodescendencia', 'n_lengua_indigena', 'n_religion',
            'n_dificultad_ver', 'n_dificultad_oir', 'n_dificultad_mover', 'n_dificultad_cogni',
            'n_dificultad_cuidado', 'n_dificultad_comunic', 'n_discapacidad',
            'n_estcivcon_casado', 'n_estcivcon_conviviente', 'n_estcivcon_conv_civil',
            'n_estcivcon_anul_sep_div', 'n_estcivcon_viudo', 'n_estcivcon_soltero',
            'prom_escolaridad18', 'n_asistencia_parv', 'n_asistencia_basica',
            'n_asistencia_media', 'n_asistencia_superior', 'n_cine_nunca_curso_primera_infancia',
            'n_cine_primaria', 'n_cine_secundaria', 'n_cine_terciaria_maestria_doctorado',
            'n_cine_especial_diferencial', 'n_analfabet', 'n_transporte_publico',
            'n_transporte_camina', 'n_transporte_bicicleta', 'n_transporte_motocicleta',
            'n_transporte_cab_lan_bote', 'n_transporte_otros', 'n_tipo_viv_casa',
            'n_tipo_viv_depto', 'n_tipo_viv_indigena', 'n_tipo_viv_pieza',
            'n_tipo_viv_mediagua', 'n_tipo_viv_movil', 'n_tipo_viv_otro', 'n_dormitorios_1',
            'n_dormitorios_2', 'n_dormitorios_3', 'n_dormitorios_4', 'n_dormitorios_5',
            'n_dormitorios_6_o_mas', 'n_viv_hacinadas', 'n_viv_irrecuperables',
            'n_hog_allegados', 'n_nucleos_hacinados_allegados', 'n_viv_no_ampliables',
            'n_deficit_cuantitativo', 'n_mat_paredes_hormigon', 'n_mat_paredes_albanileria',
            'n_mat_paredes_tabique_forrado', 'n_mat_paredes_tabique_sin_forro',
            'n_mat_paredes_artesanal', 'n_mat_paredes_precarios', 'n_mat_techo_tejas',
            'n_mat_techo_hormigon', 'n_mat_techo_zinc', 'n_mat_techo_fibrocemento',
            'n_mat_techo_fonolita', 'n_mat_techo_paja', 'n_mat_techo_precarios',
            'n_mat_techo_sin_cubierta', 'n_mat_piso_radier_con_revestimiento',
            'n_mat_piso_radier_sin_revestimiento', 'n_mat_piso_baldosa_cemento',
            'n_mat_piso_capa_cemento', 'n_mat_piso_tierra', 'n_fuente_agua_publica',
            'n_fuente_agua_pozo', 'n_fuente_agua_camion', 'n_fuente_agua_rio',
            'n_distrib_agua_llave', 'n_distrib_agua_llave_fuera', 'n_distrib_agua_acarreo',
            'n_serv_hig_alc_dentro', 'n_serv_hig_alc_fuera', 'n_serv_hig_fosa',
            'n_serv_hig_pozo', 'n_serv_hig_acequia_canal', 'n_serv_hig_cajon_otro',
            'n_serv_hig_bano_quimico', 'n_serv_hig_bano_seco', 'n_serv_hig_no_tiene',
            'n_fuente_elect_publica', 'n_fuente_elect_diesel', 'n_fuente_elect_solar',
            'n_fuente_elect_eolica', 'n_fuente_elect_otro', 'n_fuente_elect_no_tiene',
            'n_basura_servicios', 'n_basura_entierra', 'n_basura_eriazo', 'n_basura_rio',
            'n_basura_otro'
        ]
        # conservar solo las columnas existentes antes de hacer drop
        cols_to_drop = [c for c in drop_cols if c in df.columns]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)
        data[comuna_name] = df

    # intentar guardar cache
    if use_cache:
        try:
            cache_file.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        _save_cache(data, cache_file)

    return data

def main():
    data = get_data_from_source()
    print(f"Cargadas {len(data)} comunas.")

if __name__ == "__main__":
    main()