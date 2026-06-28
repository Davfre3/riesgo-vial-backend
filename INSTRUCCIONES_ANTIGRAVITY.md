# Riesgo vial en Docker para Antigravity

Este repositorio queda preparado para ejecutar todo en un solo Docker:

- Consulta TomTom.
- Consulta OpenWeather.
- Carga el modelo Spark MLlib.
- Une datos actuales con histórico.
- Devuelve la predicción BAJO, MEDIO o ALTO.

## 1. Clonar el repositorio

git clone https://github.com/Davfre3/riesgo-vial-backend.git
cd riesgo-vial-backend

## 2. Crear archivo de variables

Copia el ejemplo:

cp model_service/.env.example model_service/.env

Luego edita model_service/.env y coloca tus claves:

TOMTOM_API_KEY=tu_key_tomtom
OPENWEATHER_API_KEY=tu_key_openweather

## 3. Pegar los archivos del modelo

Dentro de esta carpeta:

model_service/artifacts/

coloca estos archivos generados por el notebook:

modelo_riesgo_vial_pipeline/
schema_inferencia.json
historico_features_para_backend.json

La estructura debe quedar así:

model_service/artifacts/modelo_riesgo_vial_pipeline/
model_service/artifacts/schema_inferencia.json
model_service/artifacts/historico_features_para_backend.json

## 4. Levantar Docker

Desde la raíz del repo ejecuta:

docker compose up --build

## 5. Probar endpoints

Salud del servicio:

http://localhost:8080/

Tiempo real con modelo:

http://localhost:8080/api/tiempo-real

## 6. Usar con el mapa

El frontend debe consumir:

http://localhost:8080/api/tiempo-real

Si el modelo y los archivos están bien cargados, la respuesta debe indicar:

proveedor_modelo: Spark MLlib PipelineModel en Docker
fuente_prediccion: mllib_pipeline_docker

Si falta algún archivo del modelo, el contenedor no podrá cargar el PipelineModel.
