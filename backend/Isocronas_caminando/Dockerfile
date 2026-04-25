FROM python:3.11-slim

WORKDIR /app

# deps sistema
RUN apt-get update && apt-get install -y \
    osm2pgrouting \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# copiar código
COPY . .

# deps python
RUN pip install osmnx psycopg2-binary python-dotenv

CMD ["python", "guardar_grafo.py"]