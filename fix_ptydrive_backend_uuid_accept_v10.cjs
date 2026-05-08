#!/usr/bin/env node
/**
 * PTYDrive Backend V10 - Fix UUID driver_id on accept
 *
 * Corrige:
 * - PostgreSQL: column "driver_id" is of type uuid but expression is of type text.
 *
 * Causa:
 * - Rutas rápidas V8/V9 hacían:
 *     SET driver_id=$2
 *   y PostgreSQL no castea automáticamente text -> uuid en UPDATE.
 *
 * Hace:
 * - Cambia SET driver_id=$2 por SET driver_id=$2::uuid en rutas accept.
 * - Agrega validación UUID antes de aceptar.
 * - NO toca /api/routes/drive.
 * - NO toca Google routes.
 * - NO toca lógica del mapa.
 *
 * Uso en Codespaces:
 *   cd /workspaces/prodima-pay
 *   node fix_ptydrive_backend_uuid_accept_v10.cjs
 *   git add ptydrive/server.js
 *   git commit -m "Fix accept ride driver UUID cast"
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

function backup(file, tag) {
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const bak = `${file}.bak_${tag}_${stamp}`;
  fs.copyFileSync(file, bak);
  console.log("Backup:", path.relative(root, bak));
}

if (!fs.existsSync(serverPath)) {
  fail(`No encontré ${serverPath}. Ejecuta desde la raíz del repo prodima-pay.`);
}

backup(serverPath, "uuid_accept_v10");

let src = fs.readFileSync(serverPath, "utf8");
let changes = 0;

// 1) Cast general en todos los accept handlers existentes.
// Es seguro porque driverId se valida/no debe estar vacío cuando se acepta.
const replacements = [
  {
    label: "SET driver_id=$2 -> $2::uuid",
    from: /SET driver_id=\$2,\s*\n\s*status='accepted'/g,
    to: "SET driver_id=$2::uuid,\n              status='accepted'",
  },
  {
    label: "SET driver_id=$2 con espacios -> $2::uuid",
    from: /SET\s+driver_id=\$2\s*,\s*status='accepted'/g,
    to: "SET driver_id=$2::uuid, status='accepted'",
  },
  {
    label: "SET driver_id=$2 línea suelta -> $2::uuid",
    from: /SET driver_id=\$2,\s*\n\s*updated_at=NOW\(\)/g,
    to: "SET driver_id=$2::uuid,\n              updated_at=NOW()",
  },
];

for (const item of replacements) {
  const before = src;
  src = src.replace(item.from, item.to);
  if (src !== before) {
    changes++;
    console.log("OK:", item.label);
  }
}

// 2) Validación UUID en ptyV9AcceptRide.
const v9Needle = `    if (!rideId || !driverId) return safeJson(res, 400, { ok: false, message: "rideId/driverId requerido" });`;
const v9Insert = `    if (!rideId || !driverId) return safeJson(res, 400, { ok: false, message: "rideId/driverId requerido" });
    if (typeof isUuid === "function" && !isUuid(driverId)) return safeJson(res, 400, { ok: false, message: "driverId inválido" });
    if (typeof isUuid === "function" && !isUuid(rideId)) return safeJson(res, 400, { ok: false, message: "rideId inválido" });`;

if (src.includes(v9Needle) && !src.includes('message: "driverId inválido"')) {
  src = src.replace(v9Needle, v9Insert);
  changes++;
  console.log("OK: validación UUID agregada a ptyV9AcceptRide");
} else if (src.includes('message: "driverId inválido"')) {
  console.log("OK ya existe: validación UUID accept");
} else {
  console.log("AVISO: no encontré punto exacto para validar ptyV9AcceptRide.");
}

// 3) Validación en V8 accept si existe.
const v8Needle = `    if (!rideId || !driverId) return ptyV8SafeJson(res, 400, { ok: false, message: "rideId/driverId requerido" });`;
const v8Insert = `    if (!rideId || !driverId) return ptyV8SafeJson(res, 400, { ok: false, message: "rideId/driverId requerido" });
    if (typeof isUuid === "function" && !isUuid(driverId)) return ptyV8SafeJson(res, 400, { ok: false, message: "driverId inválido" });
    if (typeof isUuid === "function" && !isUuid(rideId)) return ptyV8SafeJson(res, 400, { ok: false, message: "rideId inválido" });`;

if (src.includes(v8Needle) && !src.includes('ptyV8SafeJson(res, 400, { ok: false, message: "driverId inválido"')) {
  src = src.replace(v8Needle, v8Insert);
  changes++;
  console.log("OK: validación UUID agregada a ptyV8 accept");
}

// 4) Validación en V6 accept si existe.
const v6Needle = `    if (!driverId) return safeJson(res, 400, { ok: false, message: "driverId requerido" });`;
const v6Insert = `    if (!driverId) return safeJson(res, 400, { ok: false, message: "driverId requerido" });
    if (typeof isUuid === "function" && !isUuid(driverId)) return safeJson(res, 400, { ok: false, message: "driverId inválido" });
    if (typeof isUuid === "function" && !isUuid(rideId)) return safeJson(res, 400, { ok: false, message: "rideId inválido" });`;

if (src.includes(v6Needle) && !src.includes('message: "rideId inválido"')) {
  src = src.replace(v6Needle, v6Insert);
  changes++;
  console.log("OK: validación UUID agregada a accept clásico");
}

// 5) Parche defensivo: si quedó "driver_id=$2" en un UPDATE de accepted, lo reporta.
const suspicious = src.match(/UPDATE ride_rides[\s\S]{0,500}SET[\s\S]{0,200}driver_id=\$2(?!::uuid)[\s\S]{0,300}status='accepted'/g);
if (suspicious?.length) {
  console.log("AVISO: aún hay posibles SET driver_id=$2 sin cast en accepted:", suspicious.length);
  console.log("El script no lo modificó porque el patrón era distinto. Revisa server.js alrededor de /accept.");
}

fs.writeFileSync(serverPath, src, "utf8");

console.log("");
console.log(`Listo. Cambios aplicados: ${changes}`);
console.log("");
console.log("Ahora ejecuta:");
console.log("git add ptydrive/server.js");
console.log('git commit -m "Fix accept ride driver UUID cast"');
console.log("git push");
console.log("");
console.log("Luego espera Render o usa Manual Deploy > Deploy latest commit.");
