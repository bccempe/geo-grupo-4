# Figuras faltantes — Instrucciones de generación

## Figuras que faltan
1. `figuras/desierto_puente_alto_30.png` — referenciada en informe.tex línea 446 e informe2.tex
2. `figuras/cobertura_poblacional_puente_alto.png` — referenciada en informe.tex línea 474 e informe2.tex

## Cómo generarlas
```bash
# 1. Levantar infraestructura
docker compose --profile web up -d

# 2. Generar figuras
cd backend && python -m scripts.generate_metrics --skip-plots

# Las figuras se guardan automáticamente en figuras/
```

## O alternativamente, generarlas manualmente
```bash
cd backend && python -m scripts.generate_metrics --comuna puente_alto --skip-plots
```
