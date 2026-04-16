FROM python:3.13-slim

RUN apt-get update && apt-get install -y curl unzip \
    && curl https://rclone.org/install.sh | bash \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY descargar_datos.py .

CMD ["python", "descargar_datos.py"]