import os
import subprocess
from pathlib import Path

FOLDER_ID = "1GDd0x2kXG-wzo3s48ar9c81za9obMleQ"


def generar_rclone_conf():
    config_dir = Path("/data/.rclone")
    config_dir.mkdir(parents=True, exist_ok=True)

    token = os.getenv("RCLONE_TOKEN", "")

    config_content = f"""[gdrive]
type = drive
scope = drive
token = {token}
team_drive =
"""
    config_file = config_dir / "rclone.conf"
    config_file.write_text(config_content)
    print(f"Configuración rclone generada en {config_file}")


def sincronizar_drive():
    carpeta_destino = Path("/data")
    carpeta_destino.mkdir(exist_ok=True)

    comando = [
        "rclone",
        "--config", "/data/.rclone/rclone.conf",
        "copy",
        "gdrive:",
        str(carpeta_destino),
        "--drive-root-folder-id",
        FOLDER_ID,
        "-P"
    ]

    subprocess.run(comando, check=True)
    print("✅ Descarga completada")


if __name__ == "__main__":
    generar_rclone_conf()
    sincronizar_drive()
