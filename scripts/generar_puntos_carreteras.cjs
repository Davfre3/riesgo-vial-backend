const fs = require("fs");
const path = require("path");

const geoPath = path.resolve(__dirname, "../riesgo-vial-mapa/data/carreteras_peru.geojson");
const outPath = path.resolve(__dirname, "../model_service/artifacts/puntos_carreteras.json");

const depCodigos = {
  AM: "AMAZONAS",
  AN: "ANCASH",
  AP: "APURIMAC",
  AR: "AREQUIPA",
  AY: "AYACUCHO",
  CA: "CAJAMARCA",
  CL: "CALLAO",
  CU: "CUSCO",
  HU: "HUANUCO",
  HV: "HUANCAVELICA",
  IC: "ICA",
  JU: "JUNIN",
  LA: "LAMBAYEQUE",
  LI: "LAMBAYEQUE",
  LL: "LA LIBERTAD",
  LM: "LIMA",
  LO: "LORETO",
  PA: "PASCO",
  PI: "PIURA",
  PU: "PUNO",
  SM: "SAN MARTIN",
  TA: "TACNA",
  TU: "TUMBES",
  UC: "UCAYALI",
};

function norm(value) {
  return String(value || "").trim().toUpperCase().replace(/\s+/g, "");
}

function dep(value) {
  const raw = String(value || "").trim().toUpperCase().replace(/^PE[-_]?/, "");
  return depCodigos[raw] || raw || "SIN DEPARTAMENTO";
}

function collectCoords(value, points) {
  if (Array.isArray(value) && typeof value[0] === "number" && typeof value[1] === "number") {
    points.push(value);
    return;
  }
  if (Array.isArray(value)) value.forEach((item) => collectCoords(item, points));
}

const geo = JSON.parse(fs.readFileSync(geoPath, "utf8"));
const byCode = new Map();

for (const feature of geo.features || []) {
  const props = feature.properties || {};
  const code = norm(props.COD_CARRETERA || props.RUTA || props.CODIGO || props.codigo);
  if (!code) continue;

  const points = [];
  collectCoords(feature.geometry && feature.geometry.coordinates, points);
  if (!points.length) continue;

  const current = byCode.get(code) || {
    COD_CARRETERA: code,
    nombre: props.NOMBRE || props.nombre || code,
    DEPARTAMENTO: dep(props.DEPARTAMENTO || props.departamento || props.DPTO || props.region),
    points: [],
  };

  current.points.push(...points);
  byCode.set(code, current);
}

const puntos = [...byCode.values()]
  .map((item) => {
    const lon = item.points.reduce((sum, point) => sum + point[0], 0) / item.points.length;
    const lat = item.points.reduce((sum, point) => sum + point[1], 0) / item.points.length;
    return {
      COD_CARRETERA: item.COD_CARRETERA,
      nombre: item.nombre,
      DEPARTAMENTO: item.DEPARTAMENTO,
      lat: Number(lat.toFixed(6)),
      lon: Number(lon.toFixed(6)),
    };
  })
  .sort((a, b) => a.COD_CARRETERA.localeCompare(b.COD_CARRETERA));

fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, `${JSON.stringify(puntos, null, 2)}\n`);
console.log(`Generados ${puntos.length} puntos en ${outPath}`);
