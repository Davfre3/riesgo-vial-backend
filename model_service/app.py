import json
import os
from typing import Any, Dict, List

from fastapi import FastAPI
from pydantic import BaseModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from pyspark.ml import PipelineModel

MODEL_PATH = os.getenv("MODEL_PATH", "./modelo_riesgo_vial_pipeline")
SCHEMA_PATH = os.getenv("SCHEMA_PATH", "./schema_inferencia.json")
HISTORICO_PATH = os.getenv("HISTORICO_FEATURES_PATH", "./historico_features_para_backend.json")

app = FastAPI(title="Riesgo Vial MLlib Model Service")

spark = None
model = None
schema_cfg = None
historico_por_codigo: Dict[str, Dict[str, Any]] = {}


class PredictRequest(BaseModel):
    rows: List[Dict[str, Any]]


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def cargar_recursos() -> None:
    global spark, model, schema_cfg, historico_por_codigo

    if spark is not None and model is not None and schema_cfg is not None:
        return

    spark = (
        SparkSession.builder
        .appName("riesgo_vial_mllib_inference")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    model = PipelineModel.load(MODEL_PATH)

    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        schema_cfg = json.load(f)

    if os.path.exists(HISTORICO_PATH):
        with open(HISTORICO_PATH, "r", encoding="utf-8") as f:
            historico_rows = json.load(f)
        historico_por_codigo = {
            str(r.get("COD_CARRETERA", "")).upper(): r
            for r in historico_rows
            if r.get("COD_CARRETERA")
        }
    else:
        historico_por_codigo = {}


def construir_fila(row_actual: Dict[str, Any]) -> Dict[str, Any]:
    codigo = str(row_actual.get("COD_CARRETERA", "")).upper()
    base = dict(historico_por_codigo.get(codigo, {}))
    base["COD_CARRETERA"] = codigo

    # Actualizar variables de clima con datos en tiempo real.
    # Estas columnas sí existen en el entrenamiento según schema_inferencia.json.
    for col in [
        "om_temperatura_c",
        "om_humedad_pct",
        "om_precipitacion_mm",
        "om_lluvia_mm",
        "om_nubosidad_pct",
        "om_viento_kmh",
        "om_racha_viento_kmh",
    ]:
        if row_actual.get(col) is not None:
            base[col] = safe_float(row_actual.get(col))

    # Si se desea usar tráfico TomTom como proxy, se puede mapear a variables existentes.
    # El modelo fue entrenado con tráfico mensual histórico, por eso no se reemplaza todo.
    # Solo se conservan los datos actuales para explicación y auditoría.
    base["factor_congestion_actual"] = safe_float(row_actual.get("factor_congestion_html"))
    base["currentSpeed"] = safe_float(row_actual.get("currentSpeed"))
    base["freeFlowSpeed"] = safe_float(row_actual.get("freeFlowSpeed"))

    for c in schema_cfg.get("features_categoricas", []):
        base[c] = str(base.get(c) or "SIN_DATO")

    for c in schema_cfg.get("features_numericas", []):
        base[c] = safe_float(base.get(c), 0.0)

    return base


def obtener_probabilidades(pred_row: Dict[str, Any]) -> Any:
    prob = pred_row.get("probability")
    if prob is None:
        return None
    try:
        values = prob.toArray().tolist()
        labels = schema_cfg.get("labels", [])
        return {labels[i] if i < len(labels) else str(i): round(float(v), 6) for i, v in enumerate(values)}
    except Exception:
        return None


@app.get("/")
def health() -> Dict[str, Any]:
    cargar_recursos()
    return {
        "ok": True,
        "modelo_cargado": True,
        "model_path": MODEL_PATH,
        "schema_path": SCHEMA_PATH,
        "historico_total": len(historico_por_codigo),
    }


@app.post("/predict")
def predict(payload: PredictRequest) -> Dict[str, Any]:
    cargar_recursos()

    filas = [construir_fila(r) for r in payload.rows]
    if not filas:
        return {"ok": True, "predicciones": [], "detalle": "Sin filas para predecir."}

    fields = []
    for c in schema_cfg.get("features_categoricas", []):
        fields.append(StructField(c, StringType(), True))
    for c in schema_cfg.get("features_numericas", []):
        fields.append(StructField(c, DoubleType(), True))
    fields.append(StructField("COD_CARRETERA", StringType(), True))

    schema = StructType(fields)
    rows_schema = []
    for r in filas:
        item = {c: r.get(c) for c in schema_cfg.get("features_categoricas", [])}
        item.update({c: safe_float(r.get(c)) for c in schema_cfg.get("features_numericas", [])})
        item["COD_CARRETERA"] = r.get("COD_CARRETERA")
        rows_schema.append(item)

    df = spark.createDataFrame(rows_schema, schema=schema)
    pred = model.transform(df)

    labels = schema_cfg.get("labels", [])
    pred_rows = pred.select("COD_CARRETERA", "prediction", *(["probability"] if "probability" in pred.columns else [])).collect()

    out = []
    for pr in pred_rows:
        d = pr.asDict(recursive=True)
        pred_idx = int(float(d.get("prediction", 0)))
        nivel = labels[pred_idx] if pred_idx < len(labels) else str(pred_idx)
        probs = obtener_probabilidades(d)
        score = max(probs.values()) if isinstance(probs, dict) and probs else None
        out.append({
            "COD_CARRETERA": d.get("COD_CARRETERA"),
            "prediction": pred_idx,
            "NIVEL_RIESGO_ACTUAL": nivel,
            "score_actual_html": score,
            "probabilidades": probs,
            "motivo": "Predicción generada por PipelineModel MLlib con variables históricas y datos actuales de clima/API."
        })

    return {
        "ok": True,
        "predicciones": out,
        "detalle": "Predicción realizada por Spark MLlib PipelineModel."
    }
