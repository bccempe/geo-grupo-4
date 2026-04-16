import subprocess
from pathlib import Path

FOLDER_ID = "1GDd0x2kXG-wzo3s48ar9c81za9obMleQ"


def sincronizar_drive():
    carpeta_destino = Path("/data")
    carpeta_destino.mkdir(exist_ok=True)

    comando = [
        "rclone",
        "copy",
        "gdrive:",
        str(carpeta_destino),
        "--drive-root-folder-id",
        FOLDER_ID,
        "-P"
    ]

    subprocess.run(comando, check=True)
    print(" Descarga completada")


if __name__ == "__main__":
    sincronizar_drive()