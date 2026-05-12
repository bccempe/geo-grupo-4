import re
import unicodedata


def normalize_to_slug(value: str) -> str:
    """
    Convierte un nombre de comuna a un slug compatible con nombres de tabla.

    Ejemplos:
    - "Santiago, Chile" -> "santiago"
    - "Ñuñoa, Chile" -> "nunoa"
    - "San José de Maipo" -> "san_jose_de_maipo"
    """
    if not value:
        raise ValueError("El nombre de comuna no puede estar vacío")

    text = value.strip().lower()

    # Quita acentos y caracteres diacríticos
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")

    # Elimina la parte ", chile" si viene en el texto
    text = text.replace(", chile", "")

    # Reemplaza todo lo que no sea alfanumérico por guion bajo
    text = re.sub(r"[^a-z0-9]+", "_", text)

    # Limpieza final de guiones bajos repetidos
    text = re.sub(r"_+", "_", text).strip("_")

    return text