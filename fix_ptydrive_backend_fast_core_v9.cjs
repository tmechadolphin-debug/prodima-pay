#!/usr/bin/env node
/**
 * PTYDrive Backend V9 - rutas rápidas y menos carga por consulta
 *
 * Corrige:
 * 1) Aceptar/iniciar/llegar/completar más rápido: una sola operación principal.
 * 2) /api/rides/active más liviano.
 * 3) /api/account/session liviano.
 * 4) /api/admin/live reduce joins pesados y devuelve lo necesario.
 * 5) Índices reales para status/user/update.
 *
 * No toca:
 * - /api/routes/drive
 * - Google Places / Geocoding
 * - CORS
 *
 * Uso en Codespaces:
 *   cd /workspaces/prodima-pay
 *   node fix_ptydrive_backend_fast_core_v9.cjs
 *   git add ptydrive/server.js
 *   git commit -m "Optimize PTYDrive core ride endpoints"
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

backup(serverPath, "backend_fast_core_v9");

let src = fs.readFileSync(serverPath, "utf8");
let changes = 0;

const marker = "/* PTY BACKEND FAST CORE V9 - LIGHTWEIGHT ROUTES */";

if (!src.includes(marker)) {
  const insertBeforeCandidates = [
    "/* PTY BACKEND FAST PROFILE REVIEWS V8 - NO ROUTE TOUCH */",
    "/* PTY BACKEND CORE SERVICES V7 - NO ROUTE TOUCH */",
    'app.get("/api/rides/active"',
    'app.patch("/api/rides/:id/accept"',
  ];

  let idx = -1;
  for (const c of insertBeforeCandidates) {
    idx = src.indexOf(c);
    if (idx !== -1) break;
  }
  if (idx === -1) fail("No encontré punto de inserción antes de rutas de rides.");

  const block = `
${marker}
const PTY_V9_ACTIVE_STATUSES = [
  "requested","searching","pending","assigned","accepted","arrived","driver_arrived","in_progress","on_trip","en_curso"
];

function ptyV9Text(v = "") {
  try { if (typeof asText === "function") return asText(v); } catch {}
  return String(v ?? "").trim();
}
function ptyV9Num(v, fallback = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}
function ptyV9Json(v, fallback = {}) {
  if (!v) return fallback;
  if (typeof v === "object") return v;
  try { return JSON.parse(v); } catch { return fallback; }
}
function ptyV9First(...vals) {
  for (const v of vals) {
    const s = ptyV9Text(v);
    if (s) return s;
  }
  return "";
}
function ptyV9Point(v = {}) {
  const raw = ptyV9Json(v, v || {});
  const lat = Number(raw.lat ?? raw.latitude);
  const lng = Number(raw.lng ?? raw.lon ?? raw.longitude);
  return {
    ...raw,
    ...(Number.isFinite(lat) ? { lat } : {}),
    ...(Number.isFinite(lng) ? { lng } : {}),
  };
}
function ptyV9VehicleFromDocs(docs = {}, fallback = {}) {
  const raw = { ...(ptyV9Json(docs.vehicle, {}) || {}), ...(fallback.vehicle || fallback.driverVehicle || {}) };
  const tier = ptyV9First(raw.serviceTier, docs.serviceTier, docs.enrollmentType, "viaje");
  const typeLabel = tier === "moto" ? "Moto" : tier === "comfort" ? "Comfort" : "Auto";
  const brand = ptyV9First(raw.brand, raw.make, raw.marca, docs.marcaVehiculo, docs.marca);
  const model = ptyV9First(raw.model, raw.modelo, docs.modeloVehiculo, docs.modelo);
  const plate = ptyV9First(raw.plate, raw.placa, docs.placa);
  const year = ptyV9First(raw.year, raw.anio, docs.anioVehiculo, docs.year);
  const color = ptyV9First(raw.color, docs.colorVehiculo, docs.color);
  return {
    ...raw,
    serviceTier: tier,
    vehicleType: tier === "moto" ? "moto" : "car",
    typeLabel,
    brand,
    make: brand,
    model,
    plate,
    year,
    color,
    label: [typeLabel, brand, model, year, plate ? \`Placa \${plate}\` : ""].filter(Boolean).join(" · ") || "Vehículo",
  };
}
function ptyV9DriverFromJoined(row = {}) {
  const docs = ptyV9Json(row.driver_docs, {});
  const photo = ptyV9First(
    row.driver_photo,
    docs.fotoPerfilConductor,
    docs.driverProfilePhoto,
    docs.profilePhoto,
    docs.fotoConductor,
    docs.selfieConLicencia,
    docs.selfie,
    docs.avatarUrl
  );
  const vehicle = ptyV9VehicleFromDocs(docs, ptyV9Json(row.driver_snapshot, {}));
  return row.driver_id ? {
    id: row.driver_id,
    userId: row.driver_id,
    role: "driver",
    name: row.driver_name || row.driver_email || "Conductor",
    fullName: row.driver_name || row.driver_email || "Conductor",
    email: row.driver_email || "",
    phone: row.driver_phone || "",
    markerIcon: row.driver_marker_icon || "🚗",
    photoUrl: photo,
    avatarUrl: photo,
    profilePhoto: photo,
    driverPhoto: photo,
    driverPhotoUrl: photo,
    rating: Number(docs.rating || docs.averageRating || 5) || 5,
    reviewsCount: Number(docs.reviewsCount || docs.reviewCount || 0) || 0,
    reviews: Array.isArray(docs.reviews) ? docs.reviews : [],
    vehicle,
    driverVehicle: vehicle,
    driverDocs: docs,
  } : null;
}
function ptyV9RiderFromJoined(row = {}) {
  return row.rider_id ? {
    id: row.rider_id,
    userId: row.rider_id,
    role: "rider",
    name: row.rider_name || row.rider_email || "Rider",
    fullName: row.rider_name || row.rider_email || "Rider",
    email: row.rider_email || "",
    phone: row.rider_phone || "",
    markerIcon: row.rider_marker_icon || "📍",
  } : null;
}
function ptyV9NormalizeJoinedRide(row = {}) {
  const pickup = ptyV9Point(row.pickup);
  const destination = ptyV9Point(row.destination);
  const driver = ptyV9DriverFromJoined(row);
  const rider = ptyV9RiderFromJoined(row);
  const vehicle = driver?.vehicle || ptyV9VehicleFromDocs({}, ptyV9Json(row.driver_snapshot, {}));
  return {
    id: row.id,
    riderId: row.rider_id || "",
    driverId: row.driver_id || "",
    status: row.status || "",
    pickup,
    destination,
    route: ptyV9Json(row.route, {}),
    pickupAddress: pickup.address || pickup.title || pickup.short || "Recogida",
    destinationAddress: destination.address || destination.title || destination.short || "Destino",
    fare: Number(row.fare || 0),
    price: Number(row.fare || 0),
    total: Number(row.fare || 0),
    distanceKm: Number(row.distance_km || 0),
    routeDistanceKm: Number(row.distance_km || 0),
    durationMin: Number(row.duration_min || 0),
    paymentMethod: row.payment_method || "cash",
    rider: rider || ptyV9Json(row.rider_snapshot, {}),
    driver: driver || ptyV9Json(row.driver_snapshot, {}),
    riderSnapshot: rider || ptyV9Json(row.rider_snapshot, {}),
    driverSnapshot: driver || ptyV9Json(row.driver_snapshot, {}),
    driverPhoto: driver?.photoUrl || "",
    driverPhotoUrl: driver?.photoUrl || "",
    driverRating: driver?.rating || 5,
    driverReviewsCount: driver?.reviewsCount || 0,
    driverReviews: driver?.reviews || [],
    vehicle,
    driverVehicle: vehicle,
    cancelReason: row.cancel_reason || "",
    createdAt: row.created_at,
    expiresAt: row.expires_at,
    acceptedAt: row.accepted_at,
    startedAt: row.started_at,
    completedAt: row.completed_at,
    cancelledAt: row.cancelled_at,
    updatedAt: row.updated_at,
  };
}
async function ptyV9GetJoinedRide(rideId) {
  const r = await db(
    \`SELECT rr.*,
            d.name AS driver_name, d.email AS driver_email, d.phone AS driver_phone,
            d.marker_icon AS driver_marker_icon, d.driver_docs AS driver_docs,
            r.name AS rider_name, r.email AS rider_email, r.phone AS rider_phone,
            r.marker_icon AS rider_marker_icon
       FROM ride_rides rr
       LEFT JOIN ride_users d ON d.id = rr.driver_id
       LEFT JOIN ride_users r ON r.id = rr.rider_id
      WHERE rr.id::text=$1::text
      LIMIT 1\`,
    [String(rideId)]
  );
  return r.rows?.[0] || null;
}
function ptyV9EmitRide(ride, event = "ride:update") {
  try {
    io.to("admins").emit(event, ride);
    io.to("drivers").emit(event, ride);
    if (ride?.riderId) io.to(\`user:\${ride.riderId}\`).emit(event, ride);
    if (ride?.driverId) io.to(\`user:\${ride.driverId}\`).emit(event, ride);
    if (ride?.id) io.to(\`ride:\${ride.id}\`).emit(event, ride);
  } catch {}
}
async function ptyV9EnsureIndexes() {
  if (globalThis.__PTY_V9_INDEXES_READY__) return;
  await db(\`CREATE INDEX IF NOT EXISTS idx_pty_v9_rides_rider_status_updated ON ride_rides(rider_id, status, updated_at DESC)\`).catch(() => null);
  await db(\`CREATE INDEX IF NOT EXISTS idx_pty_v9_rides_driver_status_updated ON ride_rides(driver_id, status, updated_at DESC)\`).catch(() => null);
  await db(\`CREATE INDEX IF NOT EXISTS idx_pty_v9_rides_status_updated ON ride_rides(status, updated_at DESC)\`).catch(() => null);
  await db(\`CREATE INDEX IF NOT EXISTS idx_pty_v9_locations_role_updated ON ride_locations(role, updated_at DESC)\`).catch(() => null);
  globalThis.__PTY_V9_INDEXES_READY__ = true;
}

app.get("/api/account/session", authOptional, async (req, res) => {
  try {
    const user = req.user;
    if (!user?.id) return safeJson(res, 401, { ok: false, message: "Sesión inválida" });
    const docs = ptyV9Json(user.driver_docs, {});
    const out = {
      id: user.id,
      userId: user.id,
      email: user.email || "",
      name: user.name || "",
      fullName: user.name || "",
      role: user.role || "rider",
      phone: user.phone || "",
      markerIcon: user.marker_icon || "📍",
      documentStatus: user.document_status || "pending",
      driverDocs: docs,
      vehicle: ptyV9VehicleFromDocs(docs, {}),
      driverVehicle: ptyV9VehicleFromDocs(docs, {}),
      rating: Number(docs.rating || 5) || 5,
      reviewsCount: Number(docs.reviewsCount || 0) || 0,
    };
    return safeJson(res, 200, { ok: true, user: out, role: out.role, wallet: docs.wallet || null });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/rides/active", authOptional, async (req, res) => {
  try {
    await ptyV9EnsureIndexes();
    const userId = ptyV9Text(req.user?.id || req.query.userId || req.query.driverId || req.query.riderId || "");
    if (!userId) return safeJson(res, 200, { ok: true, ride: null });
    const r = await db(
      \`SELECT id
         FROM ride_rides
        WHERE (rider_id::text=$1::text OR driver_id::text=$1::text)
          AND LOWER(COALESCE(status,'')) = ANY($2::text[])
        ORDER BY updated_at DESC NULLS LAST, created_at DESC
        LIMIT 1\`,
      [userId, PTY_V9_ACTIVE_STATUSES]
    );
    if (!r.rows?.[0]?.id) return safeJson(res, 200, { ok: true, ride: null });
    const row = await ptyV9GetJoinedRide(r.rows[0].id);
    return safeJson(res, 200, { ok: true, ride: row ? ptyV9NormalizeJoinedRide(row) : null });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e), ride: null });
  }
});

async function ptyV9AcceptRide(req, res) {
  try {
    await ptyV9EnsureIndexes();
    const rideId = ptyV9Text(req.params.id || req.params.rideId);
    const driverId = ptyV9Text(req.user?.id || req.body.driverId || req.body.driver?.id || req.body.driver?.driverId || "");
    if (!rideId || !driverId) return safeJson(res, 400, { ok: false, message: "rideId/driverId requerido" });

    const upd = await db(
      \`UPDATE ride_rides
          SET driver_id=$2,
              status='accepted',
              accepted_at=COALESCE(accepted_at, NOW()),
              updated_at=NOW()
        WHERE id::text=$1::text
          AND LOWER(COALESCE(status,'')) NOT IN ('completed','cancelled','expired','driver_cancelled','rider_cancelled','auto_cancelled')
          AND (driver_id IS NULL OR driver_id::text='' OR driver_id::text=$2::text)
        RETURNING id\`,
      [rideId, driverId]
    );
    if (!upd.rows.length) {
      const existing = await ptyV9GetJoinedRide(rideId).catch(() => null);
      return safeJson(res, 409, { ok: false, message: "La carrera ya no está disponible", ride: existing ? ptyV9NormalizeJoinedRide(existing) : null });
    }

    const row = await ptyV9GetJoinedRide(rideId);
    const ride = ptyV9NormalizeJoinedRide(row);
    ptyV9EmitRide(ride, "ride:accepted");
    ptyV9EmitRide(ride, "ride.accepted");
    return safeJson(res, 200, { ok: true, ride, status: "accepted" });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
}

app.patch("/api/rides/:id/accept", authOptional, ptyV9AcceptRide);
app.patch("/api/carrera-lite/:id/accept", authOptional, ptyV9AcceptRide);
app.patch("/api/carreras/:id/accept", authOptional, ptyV9AcceptRide);

async function ptyV9PatchRideStatus(req, res, status, eventName) {
  try {
    const rideId = ptyV9Text(req.params.id || req.params.rideId);
    const userId = ptyV9Text(req.user?.id || req.body.driverId || req.body.riderId || "");
    const stampField =
      status === "in_progress" ? ", started_at=COALESCE(started_at,NOW())" :
      status === "completed" ? ", completed_at=COALESCE(completed_at,NOW())" :
      status === "arrived" ? "" : "";
    const upd = await db(
      \`UPDATE ride_rides
          SET status=$2,
              updated_at=NOW()
              \${stampField}
        WHERE id::text=$1::text
          AND ($3::text='' OR driver_id::text=$3::text OR rider_id::text=$3::text)
        RETURNING id\`,
      [rideId, status, userId]
    );
    if (!upd.rows.length) return safeJson(res, 404, { ok: false, message: "Carrera no encontrada" });
    const row = await ptyV9GetJoinedRide(rideId);
    const ride = ptyV9NormalizeJoinedRide(row);
    ptyV9EmitRide(ride, eventName);
    return safeJson(res, 200, { ok: true, ride, status });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
}
app.patch("/api/rides/:id/start", authOptional, (req, res) => ptyV9PatchRideStatus(req, res, "in_progress", "ride:started"));
app.post("/api/rides/:id/start", authOptional, (req, res) => ptyV9PatchRideStatus(req, res, "in_progress", "ride:started"));
app.patch("/api/rides/:id/arrive", authOptional, (req, res) => ptyV9PatchRideStatus(req, res, "arrived", "ride:arrived"));
app.post("/api/rides/:id/arrive", authOptional, (req, res) => ptyV9PatchRideStatus(req, res, "arrived", "ride:arrived"));
app.patch("/api/rides/:id/complete", authOptional, (req, res) => ptyV9PatchRideStatus(req, res, "completed", "ride:completed"));
app.post("/api/rides/:id/complete", authOptional, (req, res) => ptyV9PatchRideStatus(req, res, "completed", "ride:completed"));

app.get("/api/admin/live", authRequired, requireAdmin, async (_req, res) => {
  try {
    await ptyV9EnsureIndexes();
    const driversR = await db(
      \`SELECT u.id, u.name, u.email, u.phone, u.marker_icon, u.role, u.driver_docs,
              l.lat, l.lng, l.heading, l.speed, l.updated_at
         FROM ride_locations l
         JOIN ride_users u ON u.id = l.user_id
        WHERE l.role='driver'
        ORDER BY l.updated_at DESC
        LIMIT 300\`
    ).catch(() => ({ rows: [] }));
    const ridersR = await db(
      \`SELECT u.id, u.name, u.email, u.phone, u.marker_icon, u.role, u.driver_docs,
              l.lat, l.lng, l.heading, l.speed, l.updated_at
         FROM ride_locations l
         JOIN ride_users u ON u.id = l.user_id
        WHERE l.role='rider'
        ORDER BY l.updated_at DESC
        LIMIT 300\`
    ).catch(() => ({ rows: [] }));
    const ridesR = await db(
      \`SELECT id FROM ride_rides
        WHERE LOWER(COALESCE(status,'')) = ANY($1::text[])
        ORDER BY updated_at DESC NULLS LAST, created_at DESC
        LIMIT 120\`,
      [PTY_V9_ACTIVE_STATUSES]
    ).catch(() => ({ rows: [] }));

    const drivers = driversR.rows.map((x) => ({
      id: x.id, userId: x.id, name: x.name || x.email || "Driver", email: x.email || "", phone: x.phone || "",
      role: "driver", markerIcon: x.marker_icon || "🚗", lat: Number(x.lat), lng: Number(x.lng),
      currentLocation: { lat: Number(x.lat), lng: Number(x.lng), heading: x.heading, speed: x.speed },
      updatedAt: x.updated_at, vehicle: ptyV9VehicleFromDocs(ptyV9Json(x.driver_docs, {}), {}),
    }));
    const riders = ridersR.rows.map((x) => ({
      id: x.id, userId: x.id, name: x.name || x.email || "Rider", email: x.email || "", phone: x.phone || "",
      role: "rider", markerIcon: x.marker_icon || "📍", lat: Number(x.lat), lng: Number(x.lng),
      currentLocation: { lat: Number(x.lat), lng: Number(x.lng), heading: x.heading, speed: x.speed },
      updatedAt: x.updated_at,
    }));
    const activeRides = [];
    for (const item of ridesR.rows) {
      const row = await ptyV9GetJoinedRide(item.id).catch(() => null);
      if (row) activeRides.push(ptyV9NormalizeJoinedRide(row));
    }
    return safeJson(res, 200, { ok: true, drivers, riders, activeRides, rides: activeRides });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e), drivers: [], riders: [], activeRides: [] });
  }
});

`;

  src = src.slice(0, idx) + block + src.slice(idx);
  changes++;
  console.log("OK: bloque V9 rutas rápidas insertado antes de rutas existentes");
} else {
  console.log("OK ya existe: bloque V9");
}

// Memoizar ptyV8EnsureFastTables si existe, para evitar CREATE INDEX en cada request.
if (src.includes("async function ptyV8EnsureFastTables() {") && !src.includes("__PTY_V8_FAST_TABLES_READY__")) {
  src = src.replace(
    "async function ptyV8EnsureFastTables() {",
    `async function ptyV8EnsureFastTables() {
  if (globalThis.__PTY_V8_FAST_TABLES_READY__) return;`
  );
  src = src.replace(
    "  await db(`CREATE INDEX IF NOT EXISTS idx_pty_v8_rides_status_created ON ride_rides(status, created_at DESC);`).catch(() => null);\n}",
    "  await db(`CREATE INDEX IF NOT EXISTS idx_pty_v8_rides_status_created ON ride_rides(status, created_at DESC);`).catch(() => null);\n  globalThis.__PTY_V8_FAST_TABLES_READY__ = true;\n}"
  );
  changes++;
  console.log("OK: ptyV8EnsureFastTables memoizado");
}

fs.writeFileSync(serverPath, src, "utf8");

console.log("");
console.log(`Listo. Cambios aplicados: ${changes}`);
console.log("");
console.log("Ahora ejecuta:");
console.log("git add ptydrive/server.js");
console.log('git commit -m "Optimize PTYDrive core ride endpoints"');
console.log("git push");
console.log("");
console.log("Render hará deploy automático. Si no: Manual Deploy > Deploy latest commit.");
