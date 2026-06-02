import subprocess
from pathlib import Path

FOLDER_ID = "1GDd0x2kXG-wzo3s48ar9c81za9obMleQ"


def sincronizar_drive():
     # validacion
    config_path = Path("/data/.rclone/rclone.conf")

    if not config_path.exists():
        raise FileNotFoundError(f"No existe config en {config_path}")

    carpeta_destino = Path("/data")
    carpeta_destino.mkdir(parents=True, exist_ok=True)

    # Ruta al rclone.conf que ahora está en tu proyecto/Docker
    config_path = "/data/.rclone/rclone.conf"

    comando = [
        "rclone",
        "--config", config_path,
        "copy",
        "gdrive:",
        str(carpeta_destino),
        "--drive-root-folder-id",
        FOLDER_ID,
        "-P"
    ]

    # Capturar salida para debug real
    result = subprocess.run(comando, capture_output=True, text=True)

    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"Error ejecutando rclone (code {result.returncode})")

    print("Descarga completada")


if __name__ == "__main__":
    sincronizar_drive()