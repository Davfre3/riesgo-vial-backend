// api/tiempo-real.js
// Backend serverless para Vercel. Oculta TOMTOM_API_KEY y consulta TomTom + Open-Meteo.

const PUNTOS_CARRETERAS = [
  { COD_CARRETERA: "PE-1N", nombre: "Panamericana Norte - Lima", departamento: "LIMA", lat: -11.8715, lon: -77.0767 },
  { COD_CARRETERA: "PE-1S", nombre: "Panamericana Sur - Lima", departamento: "LIMA", lat: -12.1736, lon: -76.9561 },
  { COD_CARRETERA: "PE-22", nombre: "Carretera Central", departamento: "LIMA", lat: -12.0191, lon: -76.8227 },
  { COD_CARRETERA: "PE-34", nombre: "Carretera PE-34", departamento: "PUNO", lat: -15.8402, lon: -70.0219 }
];

function aplicarCors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  res.setHeader("Cache-Control", "no-store");
}

async function obtenerTomTom(lat, lon) {
  const apiKey = process.env.TOMTOM_API_KEY;
  if (!apiKey) return { disponible: false, error: "Falta TOMTOM_API_KEY" };

  const url = `https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?key=${apiKey}&point=${lat},${lon}&unit=KMPH`;

  try {
    const response = await fetch(url);
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
      currentSpeed,
      freeFlowSpeed,
      currentTravelTime,
      freeFlowTravelTime,
      confidence: Number(flow.confidence || 0),
      roadClosure: Boolean(flow.roadClosure || false),
      congestionRatio
    };
  } catch (error) {
    return { disponible: false, error: error.message };
  }
}

async function obtenerOpenMeteo(lat, lon) {
  const params = new URLSearchParams({
    latitude: String(lat),
    longitude: String(lon),
    current: [
      "temperature_2m",
      "relative_humidity_2m",
      "precipitation",
      "rain",
      "cloud_cover",
      "wind_speed_10m",
      "wind_gusts_10m"
    ].join(","),
    timezone: "America/Lima"
  });

  try {
    const response = await fetch(`https://api.open-meteo.com/v1/forecast?${params.toString()}`);
    if (!response.ok) return { disponible: false, error: `Open-Meteo error ${response.status}` };

    const data = await response.json();
    const current = data.current || {};

    return {
      disponible: true,
      fecha_hora_clima: current.time || null,
      om_temperatura_c: Number(current.temperature_2m || 0),
      om_humedad_pct: Number(current.relative_humidity_2m || 0),
      om_precipitacion_mm: Number(current.precipitation || 0),
      om_lluvia_mm: Number(current.rain || 0),
      om_nubosidad_pct: Number(current.cloud_cover || 0),
      om_viento_kmh: Number(current.wind_speed_10m || 0),
      om_racha_viento_kmh: Number(current.wind_gusts_10m || 0)
    };
  } catch (error) {
    return { disponible: false, error: error.message };
  }
}

function calcularRiesgoActual(tomtom, clima) {
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
    NIVEL_RIESGO_ACTUAL: nivel
  };
}

export default async function handler(req, res) {
  aplicarCors(res);

  if (req.method === "OPTIONS") return res.status(200).end();

  try {
    const resultados = [];

    for (const punto of PUNTOS_CARRETERAS) {
      const [tomtom, clima] = await Promise.all([
        obtenerTomTom(punto.lat, punto.lon),
        obtenerOpenMeteo(punto.lat, punto.lon)
      ]);

      const riesgo = calcularRiesgoActual(tomtom, clima);

      resultados.push({
        COD_CARRETERA: punto.COD_CARRETERA,
        nombre: punto.nombre,
        DEPARTAMENTO: punto.departamento,
        lat: punto.lat,
        lon: punto.lon,
        actualizado_en: new Date().toISOString(),
        ...riesgo,
        tomtom_disponible: tomtom.disponible,
        currentSpeed: tomtom.currentSpeed ?? null,
        freeFlowSpeed: tomtom.freeFlowSpeed ?? null,
        currentTravelTime: tomtom.currentTravelTime ?? null,
        freeFlowTravelTime: tomtom.freeFlowTravelTime ?? null,
        confidence: tomtom.confidence ?? null,
        roadClosure: tomtom.roadClosure ?? false,
        clima_disponible: clima.disponible,
        fecha_hora_clima: clima.fecha_hora_clima ?? null,
        om_temperatura_c: clima.om_temperatura_c ?? null,
        om_humedad_pct: clima.om_humedad_pct ?? null,
        om_precipitacion_mm: clima.om_precipitacion_mm ?? null,
        om_lluvia_mm: clima.om_lluvia_mm ?? null,
        om_nubosidad_pct: clima.om_nubosidad_pct ?? null,
        om_viento_kmh: clima.om_viento_kmh ?? null,
        om_racha_viento_kmh: clima.om_racha_viento_kmh ?? null
      });
    }

    return res.status(200).json({
      ok: true,
      actualizado_en: new Date().toISOString(),
      total_puntos: resultados.length,
      data: resultados
    });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message });
  }
}
