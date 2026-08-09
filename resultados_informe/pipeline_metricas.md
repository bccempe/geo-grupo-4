# Pipeline de métricas — Notas para informe PEP 2

## Uso
```bash
# Pipeline completo (52 comunas + 8 gráficos + figuras)
cd backend && python -m scripts.generate_metrics

# Solo una comuna (prueba rápida)
cd backend && python -m scripts.generate_metrics --comuna puente_alto

# Solo tablas LaTeX, sin gráficos
cd backend && python -m scripts.generate_metrics --skip-plots --skip-figures
```

## Archivos generados
| Archivo | Tipo | Ubicación en informe |
|---------|------|---------------------|
| `tabla_cobertura_rm.tex` | Tabla LaTeX | Reemplaza tabla en sección 3.3 "Resultados comparativos por comuna" |
| `tabla_cobertura_rm_consolidada.tex` | Tabla LaTeX | Reemplaza tabla en sección 3.3 "Resultado consolidado RM" |
| `graficos/cobertura_walk_vs_transit.png` | Gráfico | Figura comparativa para sección 3.3 |
| `graficos/ranking_desiertos.png` | Gráfico | Figura para sección de desiertos |
| `graficos/distribucion_cobertura.png` | Gráfico | Figura de distribución |
| `graficos/adultos_mayores_walk_vs_transit.png` | Gráfico | Figura demográfica |
| `graficos/delta_brecha_comunas.png` | Gráfico | Figura de ganancia TP |
| `graficos/top10_mas_desatendidas.png` | Gráfico | Figura priorización |
| `graficos/cobertura_por_modo_hist.png` | Gráfico | Histograma comparativo |
| `graficos/scatter_poblacion_vs_cobertura.png` | Gráfico | Scatter plot |
| `figuras/desierto_puente_alto_30.png` | Mapa | Figura PEP1 (faltante) |
| `figuras/cobertura_poblacional_puente_alto.png` | Mapa | Figura PEP1 (faltante) |

## Cómo pegar en informe2.tex
Las tablas generadas son archivos `.tex` autocontenidos. Para insertarlas:
```latex
\input{resultados_informe/tabla_cobertura_rm.tex}
\input{resultados_informe/tabla_cobertura_rm_consolidada.tex}
```

Los gráficos se insertan como figuras normales:
```latex
\includegraphics[width=\textwidth]{resultados_informe/graficos/cobertura_walk_vs_transit.png}
```
