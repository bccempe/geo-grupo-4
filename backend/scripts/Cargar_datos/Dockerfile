FROM python:3.13-slim

RUN apt-get update && apt-get install -y \
    curl unzip gdal-bin gcc \
    && curl https://rclone.org/install.sh | bash \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY descargar_datos.py .
COPY auto_import.py .
COPY rclone.conf /root/.config/rclone/rclone.conf