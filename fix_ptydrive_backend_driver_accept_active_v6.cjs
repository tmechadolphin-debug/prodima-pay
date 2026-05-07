#!/usr/bin/env node
/**
 * PTYDrive Backend V6 - restaura aceptación driver + carrera activa.
 *
 * Corrige:
 * 1) Al aceptar, el driver no debe perder la carrera ni volver a "carreras disponibles".
 * 2) /api/rides/active debe devolver accepted/assigned/arrived/in_progress.
 * 3) PATCH /api/rides/:id/accept devuelve siempre { ok:true, ride } con driver/vehículo.
 * 4) Evita emitir "ride.unavailable" al conductor aceptante, que podía borrar la carrera localmente.
 *
 * Uso en Codespaces:
 *   cd /workspaces/prodima-pay
 *   node fix_ptydrive_backend_driver_accept_active_v6.cjs
 *   git add ptydrive/server.js
 *   git commit -m "Fix driver accept active ride state"
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
  const bak = `${file}.bak_accept_active_v6_${stamp}`;
  fs.copyFileSync(file, bak);
  console.log("Backup:", path.relative(root, bak));
}

if (!fs.existsSync(serverPath)) {
  fail(`No encontré ${serverPath}. Ejecuta desde la raíz del repo prodima-pay.`);
}

backup(serverPath);

let src = fs.readFileSync(serverPath, "utf8");
let changes = 0;

const marker = "/* PTY DRIVER ACCEPT + ACTIVE STATE V6 */";

if (!src.includes(marker)) {
  const candidates = [
    "/* PTY BACKEND ROUTES CHAT LIVE SUPPORT V5 */",
    "/* PTY DRIVER PENDING FEED FIX V1 */",
    'app.patch("/api/rides/:id/accept"',
    'app.get("/api/rides/active"',
    'app.get("/api/rides/:id"'
  ];

  let idx = -1;
  for (const c of candidates) {
    idx = src.indexOf(c);
    if (idx !== -1) break;
  }
  if (idx === -1) {
    fail("No encontré punto para insertar V6 antes de rutas de rides.");
  }

  const block = `
${marker}
function ptyV6Text(value = "") {
  try {
    if (typeof asText === "function") return asText(value);
  } catch {}
  return String(value ?? "").trim();
}

function ptyV6Json(value, fallback = {}) {
  if (!value) return fallback;
  if (typeof value === "object") return value;
  try { return JSON.parse(value); } catch { return fallback; }
}

function ptyV6NormalizePoint(value = {}) {
  const lat = Number(value?.lat ?? value?.latitude);
  const lng = Number(value?.lng ?? value?.lon ?? value?.longitude);
  return Number.isFinite(lat) && Number.isFinite(lng) ? { lat, lng } : null;
}

function ptyV6NormalizeRide(row = {}) {
  const base = typeof normalizeRide === "function" ? normalizeRide(row) : { ...row };
  return {
    ...base,
    id: base.id || row.id,
    riderId: base.riderId || row.rider_id || "",
    driverId: base.driverId || row.driver_id || "",
    status: String(base.status || row.status || "").toLowerCase(),
    pickup: base.pickup || ptyV6NormalizePoint(row.pickup || row.pickup_location || {}) || {
      lat: Number(row.pickup_lat || row.origin_lat),
      lng: Number(row.pickup_lng || row.origin_lng),
    },
    destination: base.destination || ptyV6NormalizePoint(row.destination || row.destination_location || {}) || {
      lat: Number(row.destination_lat || row.dest_lat),
      lng: Number(row.destination_lng || row.dest_lng),
    },
  };
}

function ptyV6DriverFromUser(row = {}, payloadDriver = {}) {
  const docs = ptyV6Json(row.driver_docs || row.driverDocs || {}, {});
  const payloadVehicle = payloadDriver?.vehicle || {};
  const vehicle = {
    ...(docs.vehicle || docs.vehiculo || {}),
    ...payloadVehicle,
    brand: payloadVehicle.brand || payloadVehicle.make || docs.marcaVehiculo || docs.marca || docs.brand || docs.make || "",
    make: payloadVehicle.make || payloadVehicle.brand || docs.marcaVehiculo || docs.marca || docs.make || docs.brand || "",
    model: payloadVehicle.model || docs.modeloVehiculo || docs.modelo || docs.model || "Vehículo",
    plate: payloadVehicle.plate || docs.placa || docs.plate || row.plate || "---",
    color: payloadVehicle.color || docs.colorVehiculo || docs.color || "",
    year: payloadVehicle.year || docs.anioVehiculo || docs.year || "",
  };
  vehicle.label = [vehicle.brand || vehicle.make, vehicle.model, vehicle.plate].filter(Boolean).join(" · ") || "Vehículo";

  const photo =
    payloadDriver?.photoUrl ||
    payloadDriver?.avatarUrl ||
    docs.fotoPerfilConductor ||
    docs.profilePhoto ||
    docs.driverProfilePhoto ||
    row.photo_url ||
    row.avatar_url ||
    "";

  return {
    id: row.id || payloadDriver?.id || payloadDriver?.driverId || "",
    userId: row.id || payloadDriver?.userId || "",
    role: "driver",
    name: row.name || payloadDriver?.name || row.email || "Conductor",
    email: row.email || payloadDriver?.email || "",
    phone: row.phone || payloadDriver?.phone || "",
    photoUrl: photo,
    avatarUrl: photo,
    profilePhoto: photo,
    rating: Number(docs.rating || docs.averageRating || row.rating || 5) || 5,
    reviewsCount: Number(docs.reviewsCount || docs.reviewCount || row.reviews_count || 0) || 0,
    reviews: Array.isArray(docs.reviews) ? docs.reviews : [],
    vehicle,
    plate: vehicle.plate,
    currentLocation: payloadDriver?.currentLocation || ptyV6NormalizePoint(docs.currentLocation || {}) || null,
  };
}

async function ptyV6EnrichRide(row = {}, payloadDriver = {}) {
  const ride = ptyV6NormalizeRide(row);
  let driver = null;
  if (ride.driverId) {
    const d = await db(
      \`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1\`,
      [String(ride.driverId)]
    ).catch(() => ({ rows: [] }));
    driver = ptyV6DriverFromUser(d.rows?.[0] || {}, payloadDriver);
  } else if (payloadDriver?.id || payloadDriver?.driverId) {
    driver = ptyV6DriverFromUser({}, payloadDriver);
  }

  if (driver) {
    ride.driver = { ...(ride.driver || {}), ...driver };
    ride.driverPhoto = driver.photoUrl || "";
    ride.driverPhotoUrl = driver.photoUrl || "";
    ride.driverRating = driver.rating;
    ride.driverReviewsCount = driver.reviewsCount;
    ride.driverReviews = driver.reviews;
    ride.vehicle = driver.vehicle;
    ride.driverVehicle = driver.vehicle;
  }
  return ride;
}

const PTY_V6_ACTIVE_STATUSES = [
  "requested",
  "searching",
  "pending",
  "assigned",
  "accepted",
  "arrived",
  "driver_arrived",
  "in_progress",
  "on_trip",
  "en_curso"
];

app.get("/api/rides/active", authOptional, async (req, res) => {
  try {
    try { if (typeof expireOldRides === "function") await expireOldRides(); } catch {}
    const userId = String(req.user?.id || req.query.userId || req.query.driverId || req.query.riderId || "").trim();
    if (!userId) return safeJson(res, 200, { ok: true, ride: null });

    const r = await db(
      \`SELECT * FROM ride_rides
       WHERE (rider_id::text=$1::text OR driver_id::text=$1::text)
         AND LOWER(COALESCE(status,'')) = ANY($2::text[])
       ORDER BY updated_at DESC NULLS LAST, created_at DESC
       LIMIT 1\`,
      [userId, PTY_V6_ACTIVE_STATUSES]
    );

    const ride = r.rows[0] ? await ptyV6EnrichRide(r.rows[0]) : null;
    return safeJson(res, 200, { ok: true, ride });
  } catch (e) {
    console.error("[PTY_V6_ACTIVE_ERROR]", e);
    return safeJson(res, 500, { ok: false, message: String(e?.message || e), ride: null });
  }
});

app.get("/api/driver/active", authOptional, async (req, res) => {
  req.query.userId = req.query.userId || req.query.driverId || req.user?.id || "";
  return app._router.handle(Object.assign(req, { url: "/api/rides/active", originalUrl: "/api/rides/active" }), res);
});

app.get("/api/rider/active", authOptional, async (req, res) => {
  req.query.userId = req.query.userId || req.query.riderId || req.user?.id || "";
  return app._router.handle(Object.assign(req, { url: "/api/rides/active", originalUrl: "/api/rides/active" }), res);
});

app.patch("/api/rides/:id/accept", authOptional, async (req, res) => {
  try {
    const rideId = String(req.params.id || "").trim();
    const driverId = String(req.user?.id || req.body.driverId || req.body.driver?.id || req.body.driver?.driverId || "").trim();
    const payloadDriver = req.body.driver || {};
    if (!rideId) return safeJson(res, 400, { ok: false, message: "rideId requerido" });
    if (!driverId) return safeJson(res, 400, { ok: false, message: "driverId requerido" });

    const current = await db(
      \`SELECT * FROM ride_rides WHERE id::text=$1::text LIMIT 1\`,
      [rideId]
    );
    if (!current.rows.length) return safeJson(res, 404, { ok: false, message: "Carrera no encontrada" });

    const row = current.rows[0];
    const currentStatus = String(row.status || "").toLowerCase();
    const currentDriver = String(row.driver_id || "").trim();

    if (["completed","cancelled","expired","driver_cancelled","rider_cancelled"].includes(currentStatus)) {
      return safeJson(res, 409, { ok: false, message: "La carrera ya no está disponible" });
    }

    if (currentDriver && currentDriver !== driverId) {
      return safeJson(res, 409, { ok: false, message: "La carrera ya fue aceptada por otro conductor" });
    }

    let updated;
    try {
      updated = await db(
        \`UPDATE ride_rides
         SET driver_id=$2,
             status='accepted',
             updated_at=NOW()
         WHERE id::text=$1::text
         RETURNING *\`,
        [rideId, driverId]
      );
    } catch {
      updated = await db(
        \`UPDATE ride_rides
         SET driver_id=$2,
             status='accepted'
         WHERE id::text=$1::text
         RETURNING *\`,
        [rideId, driverId]
      );
    }

    const ride = await ptyV6EnrichRide(updated.rows[0], { ...payloadDriver, id: driverId, driverId });

    try {
      io.to("admins").emit("ride.accepted", { ride, rideId, driverId });
      io.to(\`ride:\${rideId}\`).emit("ride.accepted", { ride, rideId, driverId });
      io.emit("ride:accepted", { ride, rideId, driverId });
      io.emit("ride.assigned", { ride, rideId, driverId });
    } catch {}

    return safeJson(res, 200, {
      ok: true,
      ride,
      status: "accepted",
      message: "Carrera aceptada",
    });
  } catch (e) {
    console.error("[PTY_V6_ACCEPT_ERROR]", e);
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.patch("/api/rides/:id/start", authOptional, async (req, res) => {
  try {
    const rideId = String(req.params.id || "").trim();
    const driverId = String(req.user?.id || req.body.driverId || "").trim();
    let updated;
    try {
      updated = await db(
        \`UPDATE ride_rides
         SET status='in_progress',
             updated_at=NOW()
         WHERE id::text=$1::text
           AND ($2::text='' OR driver_id::text=$2::text)
         RETURNING *\`,
        [rideId, driverId]
      );
    } catch {
      updated = await db(
        \`UPDATE ride_rides
         SET status='in_progress'
         WHERE id::text=$1::text
         RETURNING *\`,
        [rideId]
      );
    }
    if (!updated.rows.length) return safeJson(res, 404, { ok: false, message: "Carrera no encontrada" });
    const ride = await ptyV6EnrichRide(updated.rows[0]);
    try { io.emit("ride.started", { ride, rideId }); io.to("admins").emit("ride.started", { ride, rideId }); } catch {}
    return safeJson(res, 200, { ok: true, ride });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

`;
  src = src.slice(0, idx) + block + src.slice(idx);
  changes++;
  console.log("OK: bloque V6 accept/active insertado antes de rutas existentes");
} else {
  console.log("OK ya existe: bloque V6 accept/active");
}

fs.writeFileSync(serverPath, src, "utf8");

console.log("");
console.log(`Listo. Cambios aplicados: ${changes}`);
console.log("");
console.log("Ahora ejecuta:");
console.log("git add ptydrive/server.js");
console.log('git commit -m "Fix driver accept active ride state"');
console.log("git push");
console.log("");
console.log("Luego Render hará deploy automático. Si no: Manual Deploy > Deploy latest commit.");
