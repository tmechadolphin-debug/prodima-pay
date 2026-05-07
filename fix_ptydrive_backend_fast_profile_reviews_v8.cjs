#!/usr/bin/env node
/**
 * PTYDrive Backend V8 - FAST ACTIONS + DRIVER PROFILE/REVIEWS
 *
 * Objetivo:
 * - No toca /api/routes/drive ni la lógica de rutas Google que ya funciona.
 * - Hace más rápido aceptar/iniciar/finalizar carreras.
 * - Agrega reseñas reales en BD: POST /api/rides/:id/rate y GET /api/drivers/:id/reviews.
 * - Refuerza foto/perfil del conductor desde driver_docs y desde /api/driver/documents.
 * - Agrega índices para acelerar consultas de carreras activas/pendientes.
 *
 * Uso en Codespaces:
 *   cd /workspaces/prodima-pay
 *   node fix_ptydrive_backend_fast_profile_reviews_v8.cjs
 *   git add ptydrive/server.js
 *   git commit -m "Optimize ride actions and add driver reviews profile"
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
  const bak = `${file}.bak_fast_profile_reviews_v8_${stamp}`;
  fs.copyFileSync(file, bak);
  console.log("Backup:", path.relative(root, bak));
}

if (!fs.existsSync(serverPath)) {
  fail(`No encontré ${serverPath}. Ejecuta desde la raíz prodima-pay.`);
}

backup(serverPath);

let src = fs.readFileSync(serverPath, "utf8");
let changes = 0;

const marker = "/* PTY BACKEND FAST PROFILE REVIEWS V8 - NO ROUTE TOUCH */";

if (!src.includes(marker)) {
  const candidates = [
    "/* PTY BACKEND CORE SERVICES V7 - NO ROUTE TOUCH */",
    "/* PTY DRIVER ACCEPT + ACTIVE STATE V6 */",
    "/* PTY BACKEND ROUTES CHAT LIVE SUPPORT V5 */",
    'app.patch("/api/rides/:id/accept"',
    'app.get("/api/rides/active"',
    'app.get("/api/rides/:id"'
  ];

  let idx = -1;
  for (const c of candidates) {
    idx = src.indexOf(c);
    if (idx !== -1) break;
  }
  if (idx === -1) fail("No encontré dónde insertar V8 antes de rutas de rides.");

  const block = `
${marker}
const ptyV8AuthOptional = typeof authOptional === "function" ? authOptional : ((req, _res, next) => next());
const ptyV8AuthRequired = typeof authRequired === "function" ? authRequired : ptyV8AuthOptional;

function ptyV8Txt(v = "") {
  try { if (typeof asText === "function") return asText(v); } catch {}
  return String(v ?? "").trim();
}
function ptyV8Num(v, fallback = null) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}
function ptyV8Json(v, fallback = {}) {
  if (!v) return fallback;
  if (typeof v === "object") return v;
  try { return JSON.parse(v); } catch { return fallback; }
}
function ptyV8Point(v = {}) {
  const lat = ptyV8Num(v?.lat ?? v?.latitude);
  const lng = ptyV8Num(v?.lng ?? v?.lon ?? v?.longitude);
  return lat === null || lng === null ? null : { lat, lng };
}
function ptyV8SafeJson(res, status, payload) {
  try { if (typeof safeJson === "function") return safeJson(res, status, payload); } catch {}
  return res.status(status).json(payload);
}
function ptyV8Status(v = "") {
  return String(v || "").toLowerCase().trim();
}
function ptyV8First(...vals) {
  for (const v of vals) {
    const s = ptyV8Txt(v);
    if (s) return s;
  }
  return "";
}

async function ptyV8EnsureFastTables() {
  await db(\`
    CREATE TABLE IF NOT EXISTS ride_reviews (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      ride_id TEXT DEFAULT '',
      reviewer_id TEXT DEFAULT '',
      reviewer_role TEXT DEFAULT '',
      target_id TEXT DEFAULT '',
      target_role TEXT DEFAULT '',
      rating NUMERIC DEFAULT 5,
      comment TEXT DEFAULT '',
      meta JSONB DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  \`);
  await db(\`CREATE INDEX IF NOT EXISTS idx_pty_v8_reviews_target ON ride_reviews(target_id, target_role, created_at DESC);\`);
  await db(\`CREATE INDEX IF NOT EXISTS idx_pty_v8_reviews_ride ON ride_reviews(ride_id);\`);
  await db(\`CREATE INDEX IF NOT EXISTS idx_pty_v8_rides_driver_status ON ride_rides(driver_id, status);\`).catch(() => null);
  await db(\`CREATE INDEX IF NOT EXISTS idx_pty_v8_rides_rider_status ON ride_rides(rider_id, status);\`).catch(() => null);
  await db(\`CREATE INDEX IF NOT EXISTS idx_pty_v8_rides_status_created ON ride_rides(status, created_at DESC);\`).catch(() => null);
}

function ptyV8DriverPhotoFromDocs(docs = {}, row = {}, fallback = {}) {
  return ptyV8First(
    fallback.driverPhoto,
    fallback.driverPhotoUrl,
    fallback.photoUrl,
    fallback.avatarUrl,
    docs.fotoPerfilConductor,
    docs.driverProfilePhoto,
    docs.profilePhoto,
    docs.fotoConductor,
    docs.selfieConLicencia,
    docs.selfie,
    docs.avatarUrl,
    row.photo_url,
    row.avatar_url,
    row.profile_photo
  );
}

function ptyV8DriverVehicle(docs = {}, fallback = {}) {
  const raw = {
    ...(docs.vehicle || docs.vehiculo || {}),
    ...(fallback.vehicle || fallback.driverVehicle || {}),
  };
  const tier = ptyV8First(raw.serviceTier, docs.serviceTier, docs.enrollmentType, "viaje");
  const typeLabel = tier === "moto" ? "Moto" : tier === "comfort" ? "Comfort" : "Auto";
  const brand = ptyV8First(raw.brand, raw.make, raw.marca, docs.marcaVehiculo, docs.marca);
  const model = ptyV8First(raw.model, raw.modelo, docs.modeloVehiculo, docs.modelo);
  const plate = ptyV8First(raw.plate, raw.placa, docs.placa);
  const color = ptyV8First(raw.color, docs.colorVehiculo, docs.color);
  const year = ptyV8First(raw.year, raw.anio, docs.anioVehiculo, docs.year);
  return {
    ...raw,
    typeLabel,
    brand,
    make: brand,
    model,
    plate,
    color,
    year,
    serviceTier: tier,
    vehicleType: tier === "moto" ? "moto" : "car",
    label: [typeLabel, brand, model, year ? String(year) : "", plate ? \`Placa \${plate}\` : ""].filter(Boolean).join(" · ") || "Vehículo",
  };
}

async function ptyV8DriverStats(driverId = "") {
  if (!driverId) return { rating: 5, reviewsCount: 0, reviews: [] };
  await ptyV8EnsureFastTables();
  const r = await db(
    \`SELECT id, ride_id, reviewer_id, reviewer_role, rating, comment, created_at
     FROM ride_reviews
     WHERE target_id::text=$1::text AND target_role='driver'
     ORDER BY created_at DESC
     LIMIT 30\`,
    [String(driverId)]
  ).catch(() => ({ rows: [] }));
  const reviews = (r.rows || []).map((x) => ({
    id: x.id,
    rideId: x.ride_id,
    reviewerId: x.reviewer_id,
    reviewerRole: x.reviewer_role,
    rating: Number(x.rating || 5),
    comment: x.comment || "",
    createdAt: x.created_at,
  }));
  const avg = reviews.length
    ? reviews.reduce((a, x) => a + Number(x.rating || 0), 0) / reviews.length
    : 5;
  return {
    rating: Number(avg.toFixed(1)),
    averageRating: Number(avg.toFixed(1)),
    avgRating: Number(avg.toFixed(1)),
    reviewsCount: reviews.length,
    reviewCount: reviews.length,
    reviews,
  };
}

function ptyV8RideBase(row = {}) {
  const base = typeof normalizeRide === "function" ? normalizeRide(row) : { ...row };
  const pickup = base.pickup || ptyV8Point(row.pickup || row.pickup_location || {}) || {
    lat: ptyV8Num(row.pickup_lat || row.origin_lat),
    lng: ptyV8Num(row.pickup_lng || row.origin_lng),
  };
  const destination = base.destination || ptyV8Point(row.destination || row.destination_location || {}) || {
    lat: ptyV8Num(row.destination_lat || row.dest_lat),
    lng: ptyV8Num(row.destination_lng || row.dest_lng),
  };
  return {
    ...base,
    id: base.id || row.id,
    riderId: base.riderId || row.rider_id || "",
    driverId: base.driverId || row.driver_id || "",
    status: base.status || row.status || "",
    pickup,
    destination,
  };
}

async function ptyV8BuildDriver(driverId = "", fallback = {}) {
  if (!driverId && !fallback?.id) return null;
  const id = String(driverId || fallback.id || fallback.driverId || "");
  const userR = await db(\`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1\`, [id]).catch(() => ({ rows: [] }));
  const row = userR.rows?.[0] || {};
  const docs = ptyV8Json(row.driver_docs || fallback.driverDocs || fallback.documents || {}, {});
  const vehicle = ptyV8DriverVehicle(docs, fallback);
  const stats = await ptyV8DriverStats(id).catch(() => ({ rating: 5, reviewsCount: 0, reviews: [] }));
  const photo = ptyV8DriverPhotoFromDocs(docs, row, fallback);
  return {
    id,
    userId: id,
    role: "driver",
    name: ptyV8First(row.name, fallback.name, row.email, "Conductor"),
    fullName: ptyV8First(row.name, fallback.name, row.email, "Conductor"),
    email: row.email || fallback.email || "",
    phone: row.phone || fallback.phone || "",
    photoUrl: photo,
    avatarUrl: photo,
    profilePhoto: photo,
    driverPhoto: photo,
    driverPhotoUrl: photo,
    rating: stats.rating,
    averageRating: stats.averageRating || stats.rating,
    avgRating: stats.avgRating || stats.rating,
    reviewsCount: stats.reviewsCount || 0,
    reviewCount: stats.reviewCount || stats.reviewsCount || 0,
    reviews: stats.reviews || [],
    vehicle,
    driverVehicle: vehicle,
    driverDocs: docs,
    documents: docs,
  };
}

async function ptyV8EnrichRideFast(row = {}, fallbackDriver = {}) {
  const ride = ptyV8RideBase(row);
  const driverId = ptyV8Txt(ride.driverId || row.driver_id || fallbackDriver.driverId || fallbackDriver.id);
  if (driverId) {
    const driver = await ptyV8BuildDriver(driverId, fallbackDriver).catch(() => null);
    if (driver) {
      ride.driver = { ...(ride.driver || {}), ...driver };
      ride.driverSnapshot = { ...(ride.driverSnapshot || {}), ...driver };
      ride.driverPhoto = driver.photoUrl || "";
      ride.driverPhotoUrl = driver.photoUrl || "";
      ride.driverRating = driver.rating;
      ride.driverReviewsCount = driver.reviewsCount;
      ride.driverReviews = driver.reviews;
      ride.vehicle = driver.vehicle;
      ride.driverVehicle = driver.vehicle;
    }
  }
  return ride;
}

const PTY_V8_ACTIVE_STATUSES = ["requested","searching","pending","assigned","accepted","arrived","driver_arrived","in_progress","on_trip","en_curso"];

app.get("/api/drivers/:id/reviews", ptyV8AuthOptional, async (req, res) => {
  try {
    const driverId = ptyV8Txt(req.params.id);
    const stats = await ptyV8DriverStats(driverId);
    return ptyV8SafeJson(res, 200, { ok: true, driverId, ...stats });
  } catch (e) {
    return ptyV8SafeJson(res, 500, { ok: false, message: String(e?.message || e), reviews: [] });
  }
});

app.post("/api/rides/:id/rate", ptyV8AuthOptional, async (req, res) => {
  try {
    await ptyV8EnsureFastTables();
    const rideId = ptyV8Txt(req.params.id);
    const targetRole = ptyV8Status(req.body.targetRole || req.body.role || "driver") === "rider" ? "rider" : "driver";
    const rating = Math.max(1, Math.min(5, Number(req.body.rating || req.body.stars || 5)));
    const comment = ptyV8Txt(req.body.comment || req.body.note || "");
    const rideR = await db(\`SELECT * FROM ride_rides WHERE id::text=$1::text LIMIT 1\`, [rideId]).catch(() => ({ rows: [] }));
    const ride = rideR.rows?.[0] || {};
    const targetId = targetRole === "driver" ? ptyV8Txt(ride.driver_id || req.body.targetId) : ptyV8Txt(ride.rider_id || req.body.targetId);
    const reviewerId = ptyV8Txt(req.user?.id || req.body.reviewerId || req.body.userId || "");
    const reviewerRole = ptyV8Txt(req.user?.role || req.body.reviewerRole || "");
    const ins = await db(
      \`INSERT INTO ride_reviews(ride_id, reviewer_id, reviewer_role, target_id, target_role, rating, comment, meta)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
       RETURNING *\`,
      [rideId, reviewerId, reviewerRole, targetId, targetRole, rating, comment, JSON.stringify(req.body || {})]
    );
    const stats = targetRole === "driver" ? await ptyV8DriverStats(targetId) : { rating, reviewsCount: 1, reviews: [] };
    return ptyV8SafeJson(res, 201, { ok: true, review: ins.rows[0], ...stats });
  } catch (e) {
    return ptyV8SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/rides/active", ptyV8AuthOptional, async (req, res) => {
  try {
    const userId = ptyV8Txt(req.user?.id || req.query.userId || req.query.driverId || req.query.riderId || "");
    if (!userId) return ptyV8SafeJson(res, 200, { ok: true, ride: null });
    const r = await db(
      \`SELECT * FROM ride_rides
       WHERE (rider_id::text=$1::text OR driver_id::text=$1::text)
         AND LOWER(COALESCE(status,'')) = ANY($2::text[])
       ORDER BY updated_at DESC NULLS LAST, created_at DESC
       LIMIT 1\`,
      [userId, PTY_V8_ACTIVE_STATUSES]
    );
    const ride = r.rows?.[0] ? await ptyV8EnrichRideFast(r.rows[0]) : null;
    return ptyV8SafeJson(res, 200, { ok: true, ride });
  } catch (e) {
    return ptyV8SafeJson(res, 500, { ok: false, message: String(e?.message || e), ride: null });
  }
});

app.patch("/api/rides/:id/accept", ptyV8AuthOptional, async (req, res) => {
  try {
    const rideId = ptyV8Txt(req.params.id);
    const driverId = ptyV8Txt(req.user?.id || req.body.driverId || req.body.driver?.id || req.body.driver?.driverId || "");
    if (!rideId || !driverId) return ptyV8SafeJson(res, 400, { ok: false, message: "rideId/driverId requerido" });

    const upd = await db(
      \`UPDATE ride_rides
       SET driver_id=$2,
           status='accepted',
           updated_at=NOW()
       WHERE id::text=$1::text
         AND LOWER(COALESCE(status,'')) NOT IN ('completed','cancelled','expired','driver_cancelled','rider_cancelled')
         AND (driver_id IS NULL OR driver_id::text='' OR driver_id::text=$2::text)
       RETURNING *\`,
      [rideId, driverId]
    );

    if (!upd.rows.length) return ptyV8SafeJson(res, 409, { ok: false, message: "La carrera ya no está disponible" });
    const ride = await ptyV8EnrichRideFast(upd.rows[0], { ...(req.body.driver || {}), id: driverId });
    try {
      io.to("admins").emit("ride.accepted", { ride, rideId, driverId });
      io.to(\`ride:\${rideId}\`).emit("ride.accepted", { ride, rideId, driverId });
      io.emit("ride.accepted", { ride, rideId, driverId });
      io.emit("ride:accepted", { ride, rideId, driverId });
    } catch {}
    return ptyV8SafeJson(res, 200, { ok: true, ride, status: "accepted" });
  } catch (e) {
    return ptyV8SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

async function ptyV8UpdateRideStatus(req, res, status) {
  try {
    const rideId = ptyV8Txt(req.params.id);
    const driverId = ptyV8Txt(req.user?.id || req.body.driverId || req.query.driverId || "");
    const upd = await db(
      \`UPDATE ride_rides
       SET status=$2,
           updated_at=NOW()
       WHERE id::text=$1::text
         AND ($3::text='' OR driver_id::text=$3::text OR rider_id::text=$3::text)
       RETURNING *\`,
      [rideId, status, driverId]
    );
    if (!upd.rows.length) return ptyV8SafeJson(res, 404, { ok: false, message: "Carrera no encontrada" });
    const ride = await ptyV8EnrichRideFast(upd.rows[0]);
    try { io.emit(\`ride.\${status}\`, { ride, rideId }); io.to("admins").emit(\`ride.\${status}\`, { ride, rideId }); } catch {}
    return ptyV8SafeJson(res, 200, { ok: true, ride, status });
  } catch (e) {
    return ptyV8SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
}

app.patch("/api/rides/:id/start", ptyV8AuthOptional, (req, res) => ptyV8UpdateRideStatus(req, res, "in_progress"));
app.post("/api/rides/:id/start", ptyV8AuthOptional, (req, res) => ptyV8UpdateRideStatus(req, res, "in_progress"));
app.patch("/api/rides/:id/arrive", ptyV8AuthOptional, (req, res) => ptyV8UpdateRideStatus(req, res, "arrived"));
app.post("/api/rides/:id/arrive", ptyV8AuthOptional, (req, res) => ptyV8UpdateRideStatus(req, res, "arrived"));
app.patch("/api/rides/:id/complete", ptyV8AuthOptional, (req, res) => ptyV8UpdateRideStatus(req, res, "completed"));
app.post("/api/rides/:id/complete", ptyV8AuthOptional, (req, res) => ptyV8UpdateRideStatus(req, res, "completed"));

app.post("/api/driver/documents", ptyV8AuthOptional, async (req, res) => {
  try {
    const driverId = ptyV8Txt(req.user?.id || req.body.userId || req.body.driverId || "");
    if (!driverId) return ptyV8SafeJson(res, 400, { ok: false, message: "driverId requerido" });
    const vehicle = {
      serviceTier: ptyV8First(req.body.serviceTier, req.body.enrollmentType, "viaje"),
      vehicleType: ptyV8First(req.body.vehicleType, req.body.enrollmentType === "moto" ? "moto" : "car"),
      brand: ptyV8First(req.body.marcaVehiculo, req.body.brand, req.body.make),
      make: ptyV8First(req.body.marcaVehiculo, req.body.make, req.body.brand),
      model: ptyV8First(req.body.modeloVehiculo, req.body.model),
      plate: ptyV8First(req.body.placa, req.body.plate),
      color: ptyV8First(req.body.colorVehiculo, req.body.color),
      year: ptyV8First(req.body.anioVehiculo, req.body.year),
    };
    const patch = {
      ...req.body,
      vehicle,
      fotoPerfilConductor: ptyV8First(req.body.fotoPerfilConductor, req.body.profilePhoto, req.body.driverPhoto, req.body.selfieConLicencia),
      profilePhoto: ptyV8First(req.body.profilePhoto, req.body.fotoPerfilConductor, req.body.driverPhoto, req.body.selfieConLicencia),
      driverProfilePhoto: ptyV8First(req.body.driverProfilePhoto, req.body.fotoPerfilConductor, req.body.profilePhoto, req.body.selfieConLicencia),
      updatedAt: new Date().toISOString(),
    };
    const r = await db(
      \`UPDATE ride_users
       SET driver_docs=COALESCE(driver_docs,'{}'::jsonb) || $2::jsonb,
           updated_at=NOW()
       WHERE id::text=$1::text
       RETURNING *\`,
      [driverId, JSON.stringify(patch)]
    );
    const user = r.rows?.[0] ? await ptyV8BuildDriver(driverId) : null;
    return ptyV8SafeJson(res, 200, { ok: true, user, driver: user, vehicle: user?.vehicle || vehicle });
  } catch (e) {
    return ptyV8SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

`;

  src = src.slice(0, idx) + block + src.slice(idx);
  changes++;
  console.log("OK: bloque V8 performance/profile/reviews insertado");
} else {
  console.log("OK ya existe: bloque V8");
}

fs.writeFileSync(serverPath, src, "utf8");

console.log("");
console.log(`Listo. Cambios aplicados: ${changes}`);
console.log("");
console.log("Ahora ejecuta:");
console.log("git add ptydrive/server.js");
console.log('git commit -m "Optimize ride actions and add driver reviews profile"');
console.log("git push");
console.log("");
console.log("Luego Render hará deploy automático. Si no: Manual Deploy > Deploy latest commit.");
