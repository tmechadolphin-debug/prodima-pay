#!/usr/bin/env node
/**
 * FIX Render PTYDrive - evita crash por /api/rides/active
 *
 * Problema:
 *   La app llama /api/rides/active.
 *   El backend tenía /api/rides/:id y tomaba "active" como si fuera UUID.
 *   PostgreSQL explotaba con:
 *     invalid input syntax for type uuid: "active"
 *
 * Uso local:
 *   cd "RUTA_DE_TU_REPO\prodima-pay"
 *   node fix_ptydrive_active_route_uuid.cjs
 *
 * Luego:
 *   git add ptydrive/server.js
 *   git commit -m "Fix PTYDrive active ride route and UUID validation"
 *   git push
 */

const fs = require("fs");
const path = require("path");

const root = process.cwd();
const serverPath = path.join(root, "ptydrive", "server.js");

function fail(msg) {
  console.error("ERROR:", msg);
  process.exit(1);
}

function backup(file) {
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const bak = `${file}.bak_${stamp}`;
  fs.copyFileSync(file, bak);
  console.log("Backup:", path.relative(root, bak));
}

if (!fs.existsSync(serverPath)) {
  fail(`No encontré ${serverPath}. Ejecuta este script desde la raíz del repo prodima-pay.`);
}

backup(serverPath);

let src = fs.readFileSync(serverPath, "utf8");
let changes = 0;

// 1) Agregar helper isUuid después de asNum()
if (!src.includes("function isUuid(value)")) {
  const needle = `function asNum(v, fallback = null) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}
`;
  const insert = `${needle}
function isUuid(value) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(String(value || "").trim());
}
`;
  if (!src.includes(needle)) {
    fail("No encontré el bloque asNum() para insertar isUuid().");
  }
  src = src.replace(needle, insert);
  changes++;
  console.log("OK: helper isUuid agregado");
} else {
  console.log("OK ya existe: isUuid");
}

// 2) Proteger getRideById()
const oldGetRide = `async function getRideById(id) {
  const r = await db(\`SELECT * FROM ride_rides WHERE id=$1 LIMIT 1\`, [id]);
  return r.rows[0] || null;
}`;
const newGetRide = `async function getRideById(id) {
  if (!isUuid(id)) return null;
  const r = await db(\`SELECT * FROM ride_rides WHERE id=$1 LIMIT 1\`, [id]);
  return r.rows[0] || null;
}`;
if (src.includes(oldGetRide)) {
  src = src.replace(oldGetRide, newGetRide);
  changes++;
  console.log("OK: getRideById protegido contra IDs no UUID");
} else {
  console.log("AVISO: no encontré getRideById exacto; puede que ya esté modificado");
}

// 3) Insertar endpoint /api/rides/active antes de /api/rides/:id
if (!src.includes('app.get("/api/rides/active"')) {
  const marker = `app.get("/api/rides/:id", authRequired, async (req, res) => {`;
  const activeRoute = `app.get("/api/rides/active", authRequired, async (req, res) => {
  try {
    await expireOldRides();

    const r = await db(
      \`SELECT * FROM ride_rides
       WHERE (rider_id=$1 OR driver_id=$1)
         AND status IN ('requested','searching','assigned','in_progress')
       ORDER BY created_at DESC
       LIMIT 1\`,
      [req.user.id]
    );

    return safeJson(res, 200, {
      ok: true,
      ride: r.rows[0] ? normalizeRide(r.rows[0]) : null,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/carreras/active", authRequired, async (req, res) => {
  try {
    await expireOldRides();

    const r = await db(
      \`SELECT * FROM ride_rides
       WHERE (rider_id=$1 OR driver_id=$1)
         AND status IN ('requested','searching','assigned','in_progress')
       ORDER BY created_at DESC
       LIMIT 1\`,
      [req.user.id]
    );

    return safeJson(res, 200, {
      ok: true,
      ride: r.rows[0] ? normalizeRide(r.rows[0]) : null,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/carrera-lite/active", authRequired, async (req, res) => {
  try {
    await expireOldRides();

    const r = await db(
      \`SELECT * FROM ride_rides
       WHERE (rider_id=$1 OR driver_id=$1)
         AND status IN ('requested','searching','assigned','in_progress')
       ORDER BY created_at DESC
       LIMIT 1\`,
      [req.user.id]
    );

    return safeJson(res, 200, {
      ok: true,
      ride: r.rows[0] ? normalizeRide(r.rows[0]) : null,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

`;
  if (!src.includes(marker)) {
    fail('No encontré app.get("/api/rides/:id"... para insertar /active antes.');
  }
  src = src.replace(marker, activeRoute + marker);
  changes++;
  console.log("OK: rutas /api/rides/active agregadas antes de /:id");
} else {
  console.log("OK ya existe: /api/rides/active");
}

// 4) Hacer /api/rides/:id más seguro con try/catch
const oldRoute = `app.get("/api/rides/:id", authRequired, async (req, res) => {
  const ride = await getRideById(req.params.id);
  if (!ride) return safeJson(res, 404, { ok: false, message: "Carrera no encontrada" });
  return safeJson(res, 200, { ok: true, ride: normalizeRide(ride) });
});`;

const newRoute = `app.get("/api/rides/:id", authRequired, async (req, res) => {
  try {
    if (!isUuid(req.params.id)) {
      return safeJson(res, 400, { ok: false, message: "ID de carrera inválido" });
    }
    const ride = await getRideById(req.params.id);
    if (!ride) return safeJson(res, 404, { ok: false, message: "Carrera no encontrada" });
    return safeJson(res, 200, { ok: true, ride: normalizeRide(ride) });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});`;

if (src.includes(oldRoute)) {
  src = src.replace(oldRoute, newRoute);
  changes++;
  console.log("OK: /api/rides/:id con validación UUID y try/catch");
} else {
  console.log("AVISO: no encontré ruta /api/rides/:id exacta para reemplazo seguro");
}

fs.writeFileSync(serverPath, src, "utf8");

console.log("");
console.log(`Listo. Cambios aplicados: ${changes}`);
console.log("");
console.log("Ahora ejecuta:");
console.log("git add ptydrive/server.js");
console.log('git commit -m "Fix PTYDrive active ride route and UUID validation"');
console.log("git push");
console.log("");
console.log("Render debe hacer deploy automático. Si no, usa Manual Deploy > Deploy latest commit.");
