const fs = require("fs");
const path = require("path");

const inPath = path.resolve(__dirname, "../model_service/artifacts/puntos_carreteras.json");
const outPath = path.resolve(__dirname, "../model_service/artifacts/zonas_tiempo_real.json");

const puntos = JSON.parse(fs.readFileSync(inPath, "utf8"));
const grupos = new Map();

for (const punto of puntos) {
  const departamento = String(punto.DEPARTAMENTO || "SIN DEPARTAMENTO").trim().toUpperCase();
  const key = departamento || "SIN DEPARTAMENTO";
  const grupo = grupos.get(key) || {
    COD_CARRETERA: `ZONA-${key.replace(/[^A-Z0-9]+/g, "-")}`,
    nombre: `Zona ${key}`,
    DEPARTAMENTO: key,
    tipo_punto: "zona",
    carreteras_representadas: 0,
    puntos: [],
    latSum: 0,
    lonSum: 0,
  };

  grupo.carreteras_representadas += 1;
  grupo.latSum += Number(punto.lat);
  grupo.lonSum += Number(punto.lon);
  grupo.puntos.push(punto);
  grupos.set(key, grupo);
}

const zonas = [...grupos.values()]
  .map((grupo) => {
    const lat = grupo.latSum / grupo.carreteras_representadas;
    const lon = grupo.lonSum / grupo.carreteras_representadas;
    const representante = grupo.puntos.reduce((mejor, punto) => {
      const dist = (Number(punto.lat) - lat) ** 2 + (Number(punto.lon) - lon) ** 2;
      return !mejor || dist < mejor.dist ? { punto, dist } : mejor;
    }, null).punto;

    return {
      COD_CARRETERA: grupo.COD_CARRETERA,
      nombre: `${grupo.nombre} (${representante.COD_CARRETERA})`,
      DEPARTAMENTO: grupo.DEPARTAMENTO,
      tipo_punto: grupo.tipo_punto,
      punto_representativo: representante.COD_CARRETERA,
      carreteras_representadas: grupo.carreteras_representadas,
      lat: Number(Number(representante.lat).toFixed(6)),
      lon: Number(Number(representante.lon).toFixed(6)),
    };
  })
  .sort((a, b) => a.DEPARTAMENTO.localeCompare(b.DEPARTAMENTO));

fs.writeFileSync(outPath, `${JSON.stringify(zonas, null, 2)}\n`);
console.log(`Generadas ${zonas.length} zonas en ${outPath}`);
