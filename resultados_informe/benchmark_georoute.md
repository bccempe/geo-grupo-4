# Benchmark georoute — Notas para informe PEP 2

## Uso
```bash
cd backend && python -m scripts.benchmark_georoute
```

Requiere que georoute-foot esté corriendo (`docker compose --profile web up -d georoute-foot`).

## Archivos generados
| Archivo | Contenido |
|---------|-----------|
| `benchmark_georoute.tex` | Tabla LaTeX para sección 4.3 de informe2.tex |
| `benchmark_georoute.json` | Datos crudos (tiempos, nodos) |

## Cómo pegar en informe2.tex
```latex
\input{resultados_informe/benchmark_georoute.tex}
```
Reemplaza la tabla vacía en líneas 811-823.
