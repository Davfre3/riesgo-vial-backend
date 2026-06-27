# Riesgo Vial Backend

Backend serverless para Vercel. Consulta TomTom y Open-Meteo en tiempo real y entrega un JSON para el mapa de riesgo vial.

## Estructura

```text
package.json
api/
  tiempo-real.js
```

## Variable de entorno obligatoria

En Vercel debes configurar:

```text
TOMTOM_API_KEY
```

No subas tu clave al repositorio.

## Endpoint

Cuando despliegues este repositorio en Vercel, tendrás una URL parecida a:

```text
https://riesgo-vial-backend.vercel.app/api/tiempo-real
```

La respuesta tendrá esta forma:

```json
{
  "ok": true,
  "actualizado_en": "2026-06-27T15:00:00.000Z",
  "total_puntos": 4,
  "data": [
    {
      "COD_CARRETERA": "PE-1S",
      "NIVEL_RIESGO_ACTUAL": "MEDIO",
      "score_actual_html": 0.35,
      "currentSpeed": 42,
      "freeFlowSpeed": 70,
      "om_temperatura_c": 18.5,
      "om_lluvia_mm": 0
    }
  ]
}
```

## Pasos en Vercel

1. Importa este repositorio en Vercel.
2. En Settings → Environment Variables agrega `TOMTOM_API_KEY`.
3. Despliega.
4. Copia la URL final del endpoint y úsala en el repo del mapa.
