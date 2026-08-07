#!/bin/sh
set -eu

OUTPUT_DIR="${GEOROUTE_DATA_DIR:-/data/georoute}"
CHILE_PBF="$OUTPUT_DIR/chile-latest.osm.pbf"
RM_PBF="$OUTPUT_DIR/rm.osm.pbf"
PBF_URL="${GEOROUTE_PBF_URL:-https://download.geofabrik.de/south-america/chile-latest.osm.pbf}"
# Caja envolvente de las 52 comunas de la Región Metropolitana.
RM_BBOX="${GEOROUTE_RM_BBOX:--71.60,-34.90,-69.60,-32.80}"

mkdir -p "$OUTPUT_DIR"

if [ ! -s "$CHILE_PBF" ]; then
    echo "Descargando extracto OSM de Chile..."
    curl --fail --location --retry 3 --retry-delay 5 \
        --output "$CHILE_PBF" "$PBF_URL"
fi

if [ ! -s "$RM_PBF" ]; then
    echo "Recortando Región Metropolitana (bbox: $RM_BBOX)..."
    osmium extract --bbox "$RM_BBOX" --strategy=complete_ways \
        --output "$RM_PBF" "$CHILE_PBF"
fi

if [ ! -s "$OUTPUT_DIR/rm-foot.grt" ]; then
    georoute build "$RM_PBF" --profile foot --output "$OUTPUT_DIR/rm-foot.grt"
fi

if [ ! -s "$OUTPUT_DIR/rm-car.grt" ]; then
    georoute build "$RM_PBF" --profile car --output "$OUTPUT_DIR/rm-car.grt"
fi

if [ ! -s "$OUTPUT_DIR/rm-foot.chg" ]; then
    georoute ch --graph "$OUTPUT_DIR/rm-foot.grt" --metric time --output "$OUTPUT_DIR/rm-foot.chg"
fi

if [ ! -s "$OUTPUT_DIR/rm-car.chg" ]; then
    georoute ch --graph "$OUTPUT_DIR/rm-car.grt" --metric time --output "$OUTPUT_DIR/rm-car.chg"
fi
