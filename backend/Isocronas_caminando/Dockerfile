FROM python:3.11-slim

WORKDIR /app

# deps sistema
RUN apt-get update && apt-get install -y \
    osm2pgrouting \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# copiar código
COPY . .

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "guardar_grafo.py"]