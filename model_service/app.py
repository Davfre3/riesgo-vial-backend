import json
import os
import time
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

MODEL_PATH = os.getenv("MODEL_PATH", "./modelo_riesgo_vial_pipeline")
SCHEMA_PATH = os.getenv("SCHEMA_PATH", "./schema_inferencia.json")
HISTORICO_PATH = os.getenv("HISTORICO_FEATURES_PATH", "./historico_features_para_backend.json")
TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY", "")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", os.getenv("OPENWEATHER_KEY", ""))

PUNTOS_CARRETERAS = [
    {"COD_CARRETERA": "PE-1N", "nombre": "Panamericana Norte - Lima", "DEPARTAMENTO": "LIMA", "lat": -11.8715, "lon": -77.0767},
    {"COD_CARRETERA": "PE-1S", "nombre": "Panamericana Sur - Lima", "DEPARTAMENTO": "LIMA", "lat": -12.1736, "lon": -76.9561},
    {"COD_CARRETERA": "PE-22", "nombre": "Carretera Central", "DEPARTAMENTO": "LIMA", "lat": -12.0191, "lon": -76.8227},
    {"COD_CARRETERA": "PE-34", "nombre": "Carretera PE-34", "DEPARTAMENTO": "PUNO", "lat": -15.8402, "lon": -70.0219},
]

app = FastAPI(title="Riesgo Vial MLlib API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

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


def obtener_tomtom(lat: float, lon: float) -> Dict[str, Any]:
    if not TOMTOM_API_KEY:
        return {"tomtom_disponible": False, "tomtom_error": "Falta TOMTOM_API_KEY"}
    url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
    try:
        r = requests.get(url, params={"key": TOMTOM_API_KEY, "point": f"{lat},{lon}", "unit": "KMPH"}, timeout=8)
        if not r.ok:
            return {"tomtom_disponible": False, "tomtom_error": f"TomTom error {r.status_code}"}
        flow = r.json().get("flowSegmentData", {})
        current = safe_float(flow.get("currentSpeed"))
        free = safe_float(flow.get("freeFlowSpeed"))
        congestion = 0 if free <= 0 else max(0, min(1, 1 - current / free))
        return {
            "tomtom_disponible": True,
            "fuente_trafico": "tomtom_current",
            "currentSpeed": current,
            "freeFlowSpeed": free,
            "currentTravelTime": safe_float(flow.get("currentTravelTime")),
            "freeFlowTravelTime": safe_float(flow.get("freeFlowTravelTime")),
            "confidence": safe_float(flow.get("confidence")),
            "roadClosure": bool(flow.get("roadClosure", False)),
            "factor_congestion_html": round(congestion, 4),
        }
    except Exception as e:
        return {"tomtom_disponible": False, "tomtom_error": str(e)}


def obtener_openweather(lat: float, lon: float) -> Dict[str, Any]:
    if not OPENWEATHER_API_KEY:
        return {"clima_disponible": False, "clima_error": "Falta OPENWEATHER_API_KEY"}
    url = "https://api.openweathermap.org/data/2.5/weather"
    try:
        r = requests.get(url, params={"lat": lat, "lon": lon, "appid": OPENWEATHER_API_KEY, "units": "metric", "lang": "es"}, timeout=8)
        if not r.ok:
            return {"clima_disponible": False, "clima_error": f"OpenWeather error {r.status_code}"}
        data = r.json()
        main = data.get("main", {})
        rain = data.get("rain", {})
        clouds = data.get("clouds", {})
        wind = data.get("wind", {})
        weather = data.get("weather", [{}])[0] if data.get("weather") else {}
        viento = safe_float(wind.get("speed")) * 3.6
        racha = safe_float(wind.get("gust")) * 3.6
        return {
            "clima_disponible": True,
            "fuente_clima": "openweather_current",
            "fecha_hora_clima": data.get("dt"),
            "descripcion_clima": weather.get("description") or weather.get("main"),
            "om_temperatura_c": safe_float(main.get("temp")),
            "om_humedad_pct": safe_float(main.get("humidity")),
            "om_precipitacion_mm": safe_float(rain.get("1h", rain.get("3h", 0))),
            "om_lluvia_mm": safe_float(rain.get("1h", rain.get("3h", 0))),
            "om_nubosidad_pct": safe_float(clouds.get("all")),
            "om_viento_kmh": round(viento, 2),
            "om_racha_viento_kmh": round(racha, 2),
            "ow_presion_hpa": safe_float(main.get("pressure")),
            "ow_sensacion_termica_c": safe_float(main.get("feels_like")),
        }
    except Exception as e:
        return {"clima_disponible": False, "clima_error": str(e)}


def calcular_factor_clima(row: Dict[str, Any]) -> float:
    factor = 0.0
    lluvia = safe_float(row.get("om_lluvia_mm"))
    precipitacion = safe_float(row.get("om_precipitacion_mm"))
    viento = safe_float(row.get("om_viento_kmh"))
    racha = safe_float(row.get("om_racha_viento_kmh"))
    if lluvia > 0 or precipitacion > 0:
        factor += 0.25
    if lluvia >= 2 or precipitacion >= 2:
        factor += 0.25
    if viento >= 30:
        factor += 0.20
    if racha >= 45:
        factor += 0.20
    return max(0, min(1, factor))


def construir_fila(row_actual: Dict[str, Any]) -> Dict[str, Any]:
    codigo = str(row_actual.get("COD_CARRETERA", "")).upper()
    base = dict(historico_por_codigo.get(codigo, {}))
    base["COD_CARRETERA"] = codigo

    for col in ["om_temperatura_c", "om_humedad_pct", "om_precipitacion_mm", "om_lluvia_mm", "om_nubosidad_pct", "om_viento_kmh", "om_racha_viento_kmh"]:
        if row_actual.get(col) is not None:
            base[col] = safe_float(row_actual.get(col))

    for c in schema_cfg.get("features_categoricas", []):
        base[c] = str(base.get(c) or "SIN_DATO")
    for c in schema_cfg.get("features_numericas", []):
        base[c] = safe_float(base.get(c), 0.0)
    return base


def obtener_probabilidades(pred_row: Dict[str, Any]) -> Optional[Dict[str, float]]:
    prob = pred_row.get("probability")
    if prob is None:
        return None
    try:
        values = prob.toArray().tolist()
        labels = schema_cfg.get("labels", [])
        return {labels[i] if i < len(labels) else str(i): round(float(v), 6) for i, v in enumerate(values)}
    except Exception:
        return None


def predecir_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cargar_recursos()
    filas = [construir_fila(r) for r in rows]
    if not filas:
        return []

    fields = [StructField(c, StringType(), True) for c in schema_cfg.get("features_categoricas", [])]
    fields += [StructField(c, DoubleType(), True) for c in schema_cfg.get("features_numericas", [])]
    fields.append(StructField("COD_CARRETERA", StringType(), True))
    schema = StructType(fields)

    rows_schema = []
    for r in filas:
        item = {c: r.get(c) for c in schema_cfg.get("features_categoricas", [])}
        item.update({c: safe_float(r.get(c)) for c in schema_cfg.get("features_numericas", [])})
        item["COD_CARRETERA"] = r.get("COD_CARRETERA")
        rows_schema.append(item)

    pred = model.transform(spark.createDataFrame(rows_schema, schema=schema))
    pred_rows = pred.select("COD_CARRETERA", "prediction", *(["probability"] if "probability" in pred.columns else [])).collect()
    labels = schema_cfg.get("labels", [])

    salida = []
    for pr in pred_rows:
        d = pr.asDict(recursive=True)
        pred_idx = int(float(d.get("prediction", 0)))
        nivel = labels[pred_idx] if pred_idx < len(labels) else str(pred_idx)
        probs = obtener_probabilidades(d)
        score = max(probs.values()) if isinstance(probs, dict) and probs else None
        salida.append({
            "COD_CARRETERA": d.get("COD_CARRETERA"),
            "prediction": pred_idx,
            "NIVEL_RIESGO_ACTUAL": nivel,
            "score_actual_html": score,
            "probabilidades": probs,
            "modelo_disponible": True,
            "fuente_prediccion": "mllib_pipeline_docker",
            "modelo_motivo": "Predicción generada por PipelineModel MLlib con histórico y datos actuales de API."
        })
    return salida


@app.get("/")
def health() -> Dict[str, Any]:
    cargar_recursos()
    return {"ok": True, "modelo_cargado": True, "historico_total": len(historico_por_codigo)}


@app.post("/predict")
def predict(payload: PredictRequest) -> Dict[str, Any]:
    return {"ok": True, "predicciones": predecir_rows(payload.rows), "detalle": "Predicción realizada por Spark MLlib PipelineModel."}


@app.get("/api/tiempo-real")
def tiempo_real() -> Dict[str, Any]:
    filas = []
    for punto in PUNTOS_CARRETERAS:
        row = dict(punto)
        row.update(obtener_tomtom(punto["lat"], punto["lon"]))
        row.update(obtener_openweather(punto["lat"], punto["lon"]))
        row["factor_clima_html"] = round(calcular_factor_clima(row), 4)
        row["actualizado_en"] = int(time.time())
        filas.append(row)

    predicciones = predecir_rows(filas)
    pred_por_codigo = {str(p["COD_CARRETERA"]).upper(): p for p in predicciones}

    data = []
    for row in filas:
        pred = pred_por_codigo.get(str(row.get("COD_CARRETERA", "")).upper(), {})
        data.append({**row, **pred})

    return {
        "ok": True,
        "proveedor_trafico": "TomTom Traffic Flow actual",
        "proveedor_clima": "OpenWeather Current Weather actual",
        "proveedor_modelo": "Spark MLlib PipelineModel en Docker",
        "requiere_env": {
            "TOMTOM_API_KEY": bool(TOMTOM_API_KEY),
            "OPENWEATHER_API_KEY": bool(OPENWEATHER_API_KEY),
        },
        "total_puntos": len(data),
        "data": data,
    }
