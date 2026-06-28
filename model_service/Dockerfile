FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y openjdk-17-jre-headless procps && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py ./app.py

RUN mkdir -p /app/artifacts

ENV MODEL_PATH=/app/artifacts/modelo_riesgo_vial_pipeline
ENV SCHEMA_PATH=/app/artifacts/schema_inferencia.json
ENV HISTORICO_FEATURES_PATH=/app/artifacts/historico_features_para_backend.json

EXPOSE 8080
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
