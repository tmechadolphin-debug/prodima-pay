#!/usr/bin/env node
/**
 * PTYDrive FIX - Driver no recibe solicitud del rider.
 *
 * Causa probable:
 *   La app driver depende de GET /api/driver/pending para llenar "Carreras disponibles".
 *   Si ese endpoint falta, falla o devuelve vacío, el driver no ve la carrera aunque el rider la haya creado.
 *
 * Este fix agrega endpoints seguros y rápidos:
 *   GET /api/driver/pending
 *   GET /api/rides/pending
 *   GET /api/driver/rides/pending
 *
 * Uso en Codespaces:
 *   cd /workspaces/prodima-pay
 *   node fix_ptydrive_driver_pending_backend_v1.cjs
 *   git add ptydrive/server.js
 *   git commit -m "Fix driver pending ride feed"
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
  const bak = `${file}.bak_driver_pending_${stamp}`;
  fs.copyFileSync(file, bak);
  console.log("Backup:", path.relative(root, bak));
}

if (!fs.existsSync(serverPath)) {
  fail(`No encontré ${serverPath}. Ejecuta desde la raíz del repo prodima-pay.`);
}

backup(serverPath);

let src = fs.readFileSync(serverPath, "utf8");
let changes = 0;

const marker = "/* PTY DRIVER PENDING FEED FIX V1 */";

if (!src.includes(marker)) {
  const candidates = [
    'app.get("/api/rides/active"',
    'app.get("/api/rides/:id"',
    'app.post("/api/rides"',
    '/* =========================================================\n   Rides'
  ];

  let idx = -1;
  for (const c of candidates) {
    idx = src.indexOf(c);
    if (idx !== -1) break;
  }
  if (idx === -1) fail("No encontré dónde insertar el feed /api/driver/pending.");

  const block = `
${marker}
async function ptyDriverPendingRows() {
  try { if (typeof expireOldRides === "function") await expireOldRides(); } catch {}

  const r = await db(
    \`SELECT
        rr.*,
        ru.id AS rider_user_id,
        ru.name AS rider_name,
        ru.email AS rider_email,
        ru.phone AS rider_phone,
        ru.marker_icon AS rider_marker_icon
      FROM ride_rides rr
      LEFT JOIN ride_users ru ON ru.id::text = rr.rider_id::text
      WHERE LOWER(COALESCE(rr.status,'')) IN ('requested','searching','assigned','pending')
        AND (rr.driver_id IS NULL OR rr.driver_id::text = '')
        AND COALESCE(rr.created_at, NOW()) >= NOW() - (($1::int || ' minutes')::interval)
      ORDER BY rr.created_at DESC
      LIMIT 100\`,
    [Number(process.env.RIDE_EXPIRE_MINUTES || 10)]
  );

  return r.rows.map((row) => {
    const normalized = typeof normalizeRide === "function" ? normalizeRide(row) : row;
    return {
      ...normalized,
      id: normalized.id || row.id,
      status: normalized.status || row.status || "requested",
      riderId: normalized.riderId || row.rider_id || row.rider_user_id || "",
      rider: {
        ...(normalized.rider || {}),
        id: row.rider_user_id || row.rider_id || normalized.rider?.id || "",
        name: row.rider_name || normalized.rider?.name || "Rider",
        email: row.rider_email || normalized.rider?.email || "",
        phone: row.rider_phone || normalized.rider?.phone || "",
        markerIcon: row.rider_marker_icon || normalized.rider?.markerIcon || "📍",
      },
    };
  });
}

async function ptyDriverPendingHandler(req, res) {
  try {
    const rides = await ptyDriverPendingRows();
    return safeJson(res, 200, {
      ok: true,
      rides,
      count: rides.length,
      serverTime: new Date().toISOString(),
    });
  } catch (e) {
    console.error("[PTY_DRIVER_PENDING_ERROR]", e);
    return safeJson(res, 500, { ok: false, message: String(e?.message || e), rides: [] });
  }
}

app.get("/api/driver/pending", authOptional, ptyDriverPendingHandler);
app.get("/api/rides/pending", authOptional, ptyDriverPendingHandler);
app.get("/api/driver/rides/pending", authOptional, ptyDriverPendingHandler);

`;

  src = src.slice(0, idx) + block + src.slice(idx);
  changes++;
  console.log("OK: endpoints /api/driver/pending agregados");
} else {
  console.log("OK ya existe: bloque driver pending V1");
}

fs.writeFileSync(serverPath, src, "utf8");

console.log("");
console.log(`Listo. Cambios aplicados: ${changes}`);
console.log("");
console.log("Ahora ejecuta:");
console.log("git add ptydrive/server.js");
console.log('git commit -m "Fix driver pending ride feed"');
console.log("git push");
console.log("");
console.log("Luego Render hará deploy automático. Si no, usa Manual Deploy > Deploy latest commit.");
