FROM python:3.11-slim

RUN apt-get update && apt-get install -y openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py ./app.py
COPY modelo_riesgo_vial_pipeline ./modelo_riesgo_vial_pipeline
COPY schema_inferencia.json ./schema_inferencia.json
COPY historico_features_para_backend.json ./historico_features_para_backend.json

EXPOSE 8080
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
