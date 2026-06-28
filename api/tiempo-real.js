// api/tiempo-real.js
// Backend serverless para Vercel.
// Tiempo real: TomTom para tráfico actual y OpenWeather para clima actual.
// Si MODEL_API_URL está configurado, envía los datos actuales al servicio Python/Spark
// para ejecutar el PipelineModel MLlib real.

const PUNTOS_CARRETERAS = [
  { COD_CARRETERA: "PE-1N", nombre: "Panamericana Norte - Lima", departamento: "LIMA", lat: -11.8715, lon: -77.0767 },
  { COD_CARRETERA: "PE-1S", nombre: "Panamericana Sur - Lima", departamento: "LIMA", lat: -12.1736, lon: -76.9561 },
  { COD_CARRETERA: "PE-22", nombre: "Carretera Central", departamento: "LIMA", lat: -12.0191, lon: -76.8227 },
  { COD_CARRETERA: "PE-34", nombre: "Carretera PE-34", departamento: "PUNO", lat: -15.8402, lon: -70.0219 }
];

const FETCH_TIMEOUT_MS = 7000;
const MODEL_TIMEOUT_MS = 20000;

function aplicarCors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  res.setHeader("Cache-Control", "no-store");
}

function obtenerOpenWeatherKey() {
  return (
    process.env.OPENWEATHER_API_KEY ||
    process.env.OPENWEATHER_KEY ||
    process.env.WEATHER_API_KEY ||
    ""
  );
}

async function fetchConTimeout(url, options = {}, timeoutMs = FETCH_TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

async function obtenerTomTom(lat, lon) {
  const apiKey = process.env.TOMTOM_API_KEY;
  if (!apiKey) return { disponible: false, error: "Falta TOMTOM_API_KEY" };

  const url = `https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?key=${apiKey}&point=${lat},${lon}&unit=KMPH`;

  try {
    const response = await fetchConTimeout(url);
    if (!response.ok) return { disponible: false, error: `TomTom error ${response.status}` };

    const data = await response.json();
    const flow = data.flowSegmentData || {};

    const currentSpeed = Number(flow.currentSpeed || 0);
    const freeFlowSpeed = Number(flow.freeFlowSpeed || 0);
    const currentTravelTime = Number(flow.currentTravelTime || 0);
    const freeFlowTravelTime = Number(flow.freeFlowTravelTime || 0);

    let congestionRatio = 0;
    if (freeFlowSpeed > 0) congestionRatio = 1 - currentSpeed / freeFlowSpeed;
    congestionRatio = Math.max(0, Math.min(1, congestionRatio));

    return {
      disponible: true,
      fuente_trafico: "tomtom_current",
      currentSpeed,
      freeFlowSpeed,
      currentTravelTime,
      freeFlowTravelTime,
      confidence: Number(flow.confidence || 0),
      roadClosure: Boolean(flow.roadClosure || false),
      congestionRatio
    };
  } catch (error) {
    return {
      disponible: false,
      fuente_trafico: "tomtom_current",
      error: error.name === "AbortError" ? "TomTom timeout" : error.message
    };
  }
}

async function obtenerOpenWeatherActual(lat, lon) {
  const apiKey = obtenerOpenWeatherKey();

  if (!apiKey) {
    return {
      disponible: false,
      fuente_clima: "openweather_current",
      error: "Falta OPENWEATHER_API_KEY en variables de entorno de Vercel"
    };
  }

  const params = new URLSearchParams({
    lat: String(lat),
    lon: String(lon),
    appid: apiKey,
    units: "metric",
    lang: "es"
  });

  const url = `https://api.openweathermap.org/data/2.5/weather?${params.toString()}`;

  try {
    const response = await fetchConTimeout(url);

    if (!response.ok) {
      let detalle = "";
      try {
        const errorBody = await response.json();
        detalle = errorBody.message ? `: ${errorBody.message}` : "";
      } catch (_) {}

      return {
        disponible: false,
        fuente_clima: "openweather_current",
        error: `OpenWeather error ${response.status}${detalle}`
      };
    }

    const data = await response.json();
    const main = data.main || {};
    const rain = data.rain || {};
    const clouds = data.clouds || {};
    const wind = data.wind || {};
    const weather = Array.isArray(data.weather) && data.weather.length > 0 ? data.weather[0] : {};

    const vientoMs = Number(wind.speed ?? 0);
    const rachaMs = Number(wind.gust ?? 0);

    return {
      disponible: true,
      fuente_clima: "openweather_current",
      fecha_hora_clima: data.dt ? new Date(Number(data.dt) * 1000).toISOString() : null,
      descripcion_clima: weather.description || weather.main || null,
      om_temperatura_c: Number(main.temp ?? 0),
      om_humedad_pct: Number(main.humidity ?? 0),
      om_precipitacion_mm: Number(rain["1h"] ?? rain["3h"] ?? 0),
      om_lluvia_mm: Number(rain["1h"] ?? rain["3h"] ?? 0),
      om_nubosidad_pct: Number(clouds.all ?? 0),
      om_viento_kmh: Number((vientoMs * 3.6).toFixed(2)),
      om_racha_viento_kmh: Number((rachaMs * 3.6).toFixed(2)),
      ow_presion_hpa: Number(main.pressure ?? 0),
      ow_sensacion_termica_c: Number(main.feels_like ?? 0)
    };
  } catch (error) {
    return {
      disponible: false,
      fuente_clima: "openweather_current",
      error: error.name === "AbortError" ? "OpenWeather timeout" : error.message
    };
  }
}

function calcularRiesgoRegla(tomtom, clima) {
  let factorCongestion = 0;
  let factorClima = 0;

  if (tomtom.disponible) {
    factorCongestion = tomtom.congestionRatio || 0;
    if (tomtom.roadClosure) factorCongestion = 1;
  }

  if (clima.disponible) {
    const lluvia = clima.om_lluvia_mm || 0;
    const precipitacion = clima.om_precipitacion_mm || 0;
    const viento = clima.om_viento_kmh || 0;
    const racha = clima.om_racha_viento_kmh || 0;

    if (lluvia > 0 || precipitacion > 0) factorClima += 0.25;
    if (lluvia >= 2 || precipitacion >= 2) factorClima += 0.25;
    if (viento >= 30) factorClima += 0.2;
    if (racha >= 45) factorClima += 0.2;
  }

  factorClima = Math.max(0, Math.min(1, factorClima));
  const scoreActual = 0.6 * factorCongestion + 0.4 * factorClima;

  let nivel = "BAJO";
  if (scoreActual >= 0.65) nivel = "ALTO";
  else if (scoreActual >= 0.25) nivel = "MEDIO";

  return {
    score_actual_html: Number(scoreActual.toFixed(4)),
    factor_congestion_html: Number(factorCongestion.toFixed(4)),
    factor_clima_html: Number(factorClima.toFixed(4)),
    NIVEL_RIESGO_ACTUAL: nivel,
    modelo_disponible: false,
    fuente_prediccion: "regla_respaldo"
  };
}

async function obtenerDatosPunto(punto) {
  const [tomtom, clima] = await Promise.all([
    obtenerTomTom(punto.lat, punto.lon),
    obtenerOpenWeatherActual(punto.lat, punto.lon)
  ]);

  const riesgoRegla = calcularRiesgoRegla(tomtom, clima);

  return {
    COD_CARRETERA: punto.COD_CARRETERA,
    nombre: punto.nombre,
    DEPARTAMENTO: punto.departamento,
    lat: punto.lat,
    lon: punto.lon,
    actualizado_en: new Date().toISOString(),
    ...riesgoRegla,
    tomtom_disponible: tomtom.disponible,
    fuente_trafico: tomtom.fuente_trafico ?? "tomtom_current",
    tomtom_error: tomtom.error ?? null,
    currentSpeed: tomtom.currentSpeed ?? null,
    freeFlowSpeed: tomtom.freeFlowSpeed ?? null,
    currentTravelTime: tomtom.currentTravelTime ?? null,
    freeFlowTravelTime: tomtom.freeFlowTravelTime ?? null,
    confidence: tomtom.confidence ?? null,
    roadClosure: tomtom.roadClosure ?? false,
    clima_disponible: clima.disponible,
    fuente_clima: clima.fuente_clima ?? "openweather_current",
    clima_error: clima.error ?? null,
    descripcion_clima: clima.descripcion_clima ?? null,
    fecha_hora_clima: clima.fecha_hora_clima ?? null,
    om_temperatura_c: clima.om_temperatura_c ?? null,
    om_humedad_pct: clima.om_humedad_pct ?? null,
    om_precipitacion_mm: clima.om_precipitacion_mm ?? null,
    om_lluvia_mm: clima.om_lluvia_mm ?? null,
    om_nubosidad_pct: clima.om_nubosidad_pct ?? null,
    om_viento_kmh: clima.om_viento_kmh ?? null,
    om_racha_viento_kmh: clima.om_racha_viento_kmh ?? null,
    ow_presion_hpa: clima.ow_presion_hpa ?? null,
    ow_sensacion_termica_c: clima.ow_sensacion_termica_c ?? null
  };
}

async function aplicarModeloMLlib(resultados) {
  const modelApiUrl = process.env.MODEL_API_URL;
  if (!modelApiUrl) {
    return {
      resultados,
      modelo: {
        usado: false,
        motivo: "Falta MODEL_API_URL. Se usa regla de respaldo."
      }
    };
  }

  try {
    const response = await fetchConTimeout(
      `${modelApiUrl.replace(/\/$/, "")}/predict`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ rows: resultados })
      },
      MODEL_TIMEOUT_MS
    );

    if (!response.ok) {
      return {
        resultados,
        modelo: {
          usado: false,
          motivo: `MODEL_API_URL respondió ${response.status}. Se usa regla de respaldo.`
        }
      };
    }

    const body = await response.json();
    const predicciones = Array.isArray(body.predicciones) ? body.predicciones : [];
    const porCodigo = new Map(predicciones.map(p => [String(p.COD_CARRETERA || "").toUpperCase(), p]));

    const mezclados = resultados.map(row => {
      const pred = porCodigo.get(String(row.COD_CARRETERA || "").toUpperCase());
      if (!pred) return row;

      return {
        ...row,
        modelo_disponible: true,
        fuente_prediccion: "mllib_pipeline_backend",
        modelo_prediccion_label: pred.prediction,
        modelo_nivel_riesgo: pred.NIVEL_RIESGO_ACTUAL,
        modelo_probabilidades: pred.probabilidades ?? null,
        modelo_motivo: pred.motivo ?? "Predicción generada por PipelineModel MLlib.",
        NIVEL_RIESGO_ACTUAL: pred.NIVEL_RIESGO_ACTUAL || row.NIVEL_RIESGO_ACTUAL,
        score_actual_html: Number(pred.score_actual_html ?? row.score_actual_html ?? 0)
      };
    });

    return {
      resultados: mezclados,
      modelo: {
        usado: true,
        url_configurada: true,
        total_predicciones: predicciones.length,
        detalle: body.detalle ?? "Predicción realizada por servicio MLlib."
      }
    };
  } catch (error) {
    return {
      resultados,
      modelo: {
        usado: false,
        motivo: `Error llamando MODEL_API_URL: ${error.message}. Se usa regla de respaldo.`
      }
    };
  }
}

export default async function handler(req, res) {
  aplicarCors(res);

  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "GET") return res.status(405).json({ ok: false, error: "Método no permitido" });

  try {
    const datosApi = await Promise.all(PUNTOS_CARRETERAS.map(obtenerDatosPunto));
    const prediccion = await aplicarModeloMLlib(datosApi);

    return res.status(200).json({
      ok: true,
      actualizado_en: new Date().toISOString(),
      proveedor_trafico: "TomTom Traffic Flow actual",
      proveedor_clima: "OpenWeather Current Weather actual",
      proveedor_modelo: prediccion.modelo.usado ? "Spark MLlib PipelineModel" : "regla de respaldo sin MLlib",
      requiere_env: {
        TOMTOM_API_KEY: Boolean(process.env.TOMTOM_API_KEY),
        OPENWEATHER_API_KEY: Boolean(obtenerOpenWeatherKey()),
        MODEL_API_URL: Boolean(process.env.MODEL_API_URL)
      },
      modelo: prediccion.modelo,
      total_puntos: prediccion.resultados.length,
      data: prediccion.resultados
    });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message });
  }
}
