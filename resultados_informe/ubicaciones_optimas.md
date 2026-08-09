# Ubicaciones óptimas — Notas para informe PEP 2

## Endpoint
```
GET /api/v1/location/optimize?comuna=puente_alto&minutes=30&max_centers=3&prioritize_elderly=true
```

## Algoritmo
Greedy Maximal Coverage Location Problem (MCLP):
1. Obtiene manzanas no cubiertas de `PopulationCoverageService`
2. Genera candidatos a partir de centroides de manzanas no cubiertas (top 200 por población×área)
3. Para cada candidato calcula isócrona vía georoute y evalúa población adicional cubierta
4. Selecciona el mejor, marca esas manzanas como cubiertas, repite N veces
5. Retorna ranking de ubicaciones propuestas con población cubierta estimada

## Sección del informe
- **Ubicación en informe2.tex:** nueva subsección en "Resultados finales"
- **Figura sugerida:** mapa con centros existentes + centros propuestos + cobertura adicional
- **Tabla sugerida:** ranking de ubicaciones con población total y adulta mayor cubierta por cada nuevo centro
