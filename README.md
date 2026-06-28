# Riesgo Vial Backend

Backend para riesgo vial en tiempo real. Puede ejecutarse localmente con Docker y expone datos de TomTom, OpenWeather y el modelo Spark MLlib para que el mapa los consuma.

## Levantar con Docker

1. Copia las variables de entorno:

```powershell
cp model_service/.env.example model_service/.env
```

2. Edita `model_service/.env` y coloca tus claves:

```text
TOMTOM_API_KEY=tu_key_tomtom
OPENWEATHER_API_KEY=tu_key_openweather
```

3. Verifica que existan los artifacts del modelo:

```text
model_service/artifacts/modelo_riesgo_vial_pipeline/
model_service/artifacts/schema_inferencia.json
model_service/artifacts/historico_features_para_backend.json
model_service/artifacts/zonas_tiempo_real.json
```

4. Levanta el servicio:

```powershell
docker compose up --build
```

5. Prueba el endpoint:

```text
http://localhost:8080/api/tiempo-real
```

El endpoint usa zonas de tiempo real y cache por 5 minutos para evitar demasiadas llamadas a TomTom/OpenWeather.

## Endpoints

```text
GET /                    salud del servicio
GET /api/tiempo-real     datos por zonas para el mapa
GET /api/tiempo-real-punto?codigo=...&lat=...&lon=... consulta puntual opcional
POST /predict            prediccion con filas enviadas manualmente
```

## Regenerar zonas

Si actualizas el GeoJSON del mapa, regenera las zonas desde la raiz del backend:

```powershell
node scripts\generar_puntos_carreteras.cjs
node scripts\generar_zonas_tiempo_real.cjs
```

Luego reinicia Docker:

```powershell
docker compose restart riesgo-vial-mllib
```
