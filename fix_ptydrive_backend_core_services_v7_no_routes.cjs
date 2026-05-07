#!/usr/bin/env node
/**
 * PTYDrive Backend V7 CORE SERVICES - SIN TOCAR RUTAS DEL DRIVER
 *
 * Corrige backend:
 * 1) /api/account/session para que la app pueda restaurar sesión.
 * 2) Chat por carrera separado por ride_id: /api/rides/:id/chat y /api/rides/:id/messages.
 * 3) Soporte/Incidentes/Objetos perdidos/SOS guardados en PostgreSQL.
 * 4) /api/admin/live con riders, drivers, ubicaciones y carreras activas.
 * 5) /api/rides/active enriquecido con driver, foto, reseñas y vehículo.
 * 6) /api/driver/location y /api/rider/location para mapa en vivo.
 *
 * IMPORTANTE:
 * - Este fix NO modifica /api/routes/drive.
 * - Este fix NO toca la lógica de línea/ruta del driver que ya funciona.
 *
 * Uso:
 *   cd /workspaces/prodima-pay
 *   node fix_ptydrive_backend_core_services_v7_no_routes.cjs
 *   git add ptydrive/server.js
 *   git commit -m "Add core session chat support live services"
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
  const bak = `${file}.bak_core_v7_${stamp}`;
  fs.copyFileSync(file, bak);
  console.log("Backup:", path.relative(root, bak));
}

if (!fs.existsSync(serverPath)) {
  fail(`No encontré ${serverPath}. Ejecuta desde la raíz prodima-pay.`);
}

backup(serverPath);
let src = fs.readFileSync(serverPath, "utf8");
let changes = 0;

const marker = "/* PTY BACKEND CORE SERVICES V7 - NO ROUTE TOUCH */";

if (!src.includes(marker)) {
  const candidates = [
    "/* PTY DRIVER ACCEPT + ACTIVE STATE V6 */",
    "/* PTY BACKEND ROUTES CHAT LIVE SUPPORT V5 */",
    'app.get("/api/rides/active"',
    'app.patch("/api/rides/:id/accept"',
    'app.get("/api/rides/:id"'
  ];

  let idx = -1;
  for (const c of candidates) {
    idx = src.indexOf(c);
    if (idx !== -1) break;
  }
  if (idx === -1) fail("No encontré punto para insertar V7 antes de rutas de rides.");

  const block = `
${marker}
const ptyV7AuthOptional = typeof authOptional === "function" ? authOptional : ((req, _res, next) => next());
const ptyV7AuthRequired = typeof authRequired === "function" ? authRequired : ptyV7AuthOptional;
const ptyV7RequireAdmin = typeof requireAdmin === "function" ? requireAdmin : ((req, _res, next) => next());

const PTY_V7_LIVE = globalThis.PTY_V7_LIVE || (globalThis.PTY_V7_LIVE = {
  drivers: new Map(),
  riders: new Map(),
});

function ptyV7Text(v = "") {
  try { if (typeof asText === "function") return asText(v); } catch {}
  return String(v ?? "").trim();
}

function ptyV7Num(v, fallback = null) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function ptyV7Json(value, fallback = {}) {
  if (!value) return fallback;
  if (typeof value === "object") return value;
  try { return JSON.parse(value); } catch { return fallback; }
}

function ptyV7Point(raw = {}) {
  const lat = ptyV7Num(raw?.lat ?? raw?.latitude ?? raw?.currentLat ?? raw?.driverLat ?? raw?.riderLat);
  const lng = ptyV7Num(raw?.lng ?? raw?.lon ?? raw?.longitude ?? raw?.currentLng ?? raw?.driverLng ?? raw?.riderLng);
  return lat === null || lng === null ? null : { lat, lng };
}

function ptyV7Status(value = "") {
  return String(value || "").toLowerCase().trim();
}

function ptyV7SafeJson(res, status, payload) {
  try {
    if (typeof safeJson === "function") return safeJson(res, status, payload);
  } catch {}
  return res.status(status).json(payload);
}

function ptyV7PublicUser(row = {}) {
  const docs = ptyV7Json(row.driver_docs || row.driverDocs || {}, {});
  const loc = ptyV7Point(docs.currentLocation || row.current_location || {});
  return {
    id: row.id || "",
    userId: row.id || "",
    name: row.name || row.full_name || row.email || "Usuario",
    fullName: row.name || row.full_name || row.email || "Usuario",
    email: row.email || "",
    phone: row.phone || "",
    role: row.role || "",
    markerIcon: row.marker_icon || docs.markerIcon || docs.userMarker || "📍",
    currentLocation: loc,
    lat: loc?.lat,
    lng: loc?.lng,
    updatedAt: row.updated_at || row.created_at || "",
    driverDocs: docs,
  };
}

function ptyV7DriverProfile(row = {}, fallback = {}) {
  const docs = ptyV7Json(row.driver_docs || fallback.driverDocs || fallback.documents || {}, {});
  const vehicleRaw = {
    ...(docs.vehicle || docs.vehiculo || {}),
    ...(fallback.vehicle || fallback.driverVehicle || {}),
  };
  const make = ptyV7Text(vehicleRaw.make || vehicleRaw.brand || vehicleRaw.marca || docs.marcaVehiculo || docs.marca || docs.make || docs.brand);
  const model = ptyV7Text(vehicleRaw.model || vehicleRaw.modelo || docs.modeloVehiculo || docs.modelo || docs.model);
  const plate = ptyV7Text(vehicleRaw.plate || vehicleRaw.placa || docs.placa || docs.plate);
  const color = ptyV7Text(vehicleRaw.color || docs.colorVehiculo || docs.color);
  const year = ptyV7Text(vehicleRaw.year || vehicleRaw.anio || docs.anioVehiculo || docs.year);
  const label = [make, model, plate].filter(Boolean).join(" · ") || "Vehículo";

  const photo =
    fallback.driverPhoto ||
    fallback.driverPhotoUrl ||
    fallback.photoUrl ||
    fallback.avatarUrl ||
    docs.fotoPerfilConductor ||
    docs.driverProfilePhoto ||
    docs.profilePhoto ||
    docs.selfieConLicencia ||
    row.photo_url ||
    row.avatar_url ||
    row.profile_photo ||
    "";

  const rating = Number(docs.rating || docs.averageRating || row.rating || fallback.rating || 5) || 5;
  const reviewsCount = Number(docs.reviewsCount || docs.reviewCount || row.reviews_count || fallback.reviewsCount || 0) || 0;

  return {
    id: row.id || fallback.id || fallback.driverId || "",
    userId: row.id || fallback.userId || fallback.driverId || "",
    role: "driver",
    name: row.name || fallback.name || row.email || "Conductor",
    fullName: row.name || fallback.name || row.email || "Conductor",
    email: row.email || fallback.email || "",
    phone: row.phone || fallback.phone || "",
    photoUrl: photo,
    avatarUrl: photo,
    profilePhoto: photo,
    driverPhoto: photo,
    rating,
    reviewsCount,
    reviews: Array.isArray(docs.reviews) ? docs.reviews : Array.isArray(fallback.reviews) ? fallback.reviews : [],
    vehicle: { ...vehicleRaw, make, brand: make, model, plate, color, year, label },
    driverVehicle: { ...vehicleRaw, make, brand: make, model, plate, color, year, label },
    driverDocs: docs,
    documents: docs,
    currentLocation: ptyV7Point(docs.currentLocation || fallback.currentLocation || {}),
  };
}

function ptyV7NormalizeRide(row = {}) {
  const base = typeof normalizeRide === "function" ? normalizeRide(row) : { ...row };
  const pickup = ptyV7Point(base.pickup || row.pickup || row.pickup_location || {}) || {
    lat: ptyV7Num(base.pickupLat || row.pickup_lat || row.origin_lat),
    lng: ptyV7Num(base.pickupLng || row.pickup_lng || row.origin_lng),
  };
  const destination = ptyV7Point(base.destination || row.destination || row.destination_location || {}) || {
    lat: ptyV7Num(base.destinationLat || row.destination_lat || row.dest_lat),
    lng: ptyV7Num(base.destinationLng || row.destination_lng || row.dest_lng),
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

async function ptyV7EnrichRide(row = {}) {
  const ride = ptyV7NormalizeRide(row);
  const riderId = ptyV7Text(ride.riderId || row.rider_id);
  const driverId = ptyV7Text(ride.driverId || row.driver_id);
  if (riderId) {
    const r = await db(\`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1\`, [riderId]).catch(() => ({ rows: [] }));
    if (r.rows?.[0]) ride.rider = { ...(ride.rider || {}), ...ptyV7PublicUser(r.rows[0]) };
  }
  if (driverId) {
    const d = await db(\`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1\`, [driverId]).catch(() => ({ rows: [] }));
    const driver = ptyV7DriverProfile(d.rows?.[0] || {}, ride.driver || {});
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
  return ride;
}

async function ptyV7EnsureTables() {
  await db(\`
    CREATE TABLE IF NOT EXISTS ride_chat_messages (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      ride_id TEXT NOT NULL,
      sender_id TEXT DEFAULT '',
      sender_role TEXT DEFAULT '',
      sender_name TEXT DEFAULT '',
      text TEXT DEFAULT '',
      meta JSONB DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  \`);
  await db(\`CREATE INDEX IF NOT EXISTS idx_pty_v7_chat_ride ON ride_chat_messages(ride_id, created_at);\`);

  await db(\`
    CREATE TABLE IF NOT EXISTS ride_support_reports (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      user_id TEXT DEFAULT '',
      user_email TEXT DEFAULT '',
      user_name TEXT DEFAULT '',
      user_phone TEXT DEFAULT '',
      role TEXT DEFAULT '',
      ride_id TEXT DEFAULT '',
      type TEXT DEFAULT 'incident',
      title TEXT DEFAULT '',
      detail TEXT DEFAULT '',
      status TEXT DEFAULT 'open',
      location JSONB DEFAULT '{}'::jsonb,
      meta JSONB DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  \`);
  await db(\`CREATE INDEX IF NOT EXISTS idx_pty_v7_reports ON ride_support_reports(status, created_at DESC);\`);

  await db(\`
    CREATE TABLE IF NOT EXISTS ride_sos_alerts (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      user_id TEXT DEFAULT '',
      user_email TEXT DEFAULT '',
      user_name TEXT DEFAULT '',
      user_phone TEXT DEFAULT '',
      role TEXT DEFAULT '',
      ride_id TEXT DEFAULT '',
      message TEXT DEFAULT '',
      status TEXT DEFAULT 'open',
      location JSONB DEFAULT '{}'::jsonb,
      trusted_contact JSONB DEFAULT '{}'::jsonb,
      ride JSONB DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  \`);
  await db(\`CREATE INDEX IF NOT EXISTS idx_pty_v7_sos ON ride_sos_alerts(status, created_at DESC);\`);

  await db(\`
    CREATE TABLE IF NOT EXISTS ride_map_reports (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      reporter_id TEXT DEFAULT '',
      reporter_role TEXT DEFAULT '',
      reporter_name TEXT DEFAULT '',
      type TEXT DEFAULT '',
      title TEXT DEFAULT '',
      detail TEXT DEFAULT '',
      icon TEXT DEFAULT '',
      color TEXT DEFAULT '',
      status TEXT DEFAULT 'active',
      lat DOUBLE PRECISION,
      lng DOUBLE PRECISION,
      confirmations INT DEFAULT 0,
      denials INT DEFAULT 0,
      meta JSONB DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  \`);
}

async function ptyV7GetUserById(id = "") {
  if (!id) return null;
  const r = await db(\`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1\`, [String(id)]).catch(() => ({ rows: [] }));
  return r.rows?.[0] || null;
}

app.get("/api/account/session", ptyV7AuthOptional, async (req, res) => {
  try {
    const userId = ptyV7Text(req.user?.id || req.query.userId || "");
    if (!userId) return ptyV7SafeJson(res, 401, { ok: false, message: "Sesión inválida" });
    const row = await ptyV7GetUserById(userId);
    if (!row) return ptyV7SafeJson(res, 401, { ok: false, message: "Usuario no encontrado" });
    const role = ptyV7Status(row.role || req.user?.role || "rider");
    const user = role === "driver" ? ptyV7DriverProfile(row) : ptyV7PublicUser(row);
    return ptyV7SafeJson(res, 200, { ok: true, user, role, wallet: user.driverDocs?.wallet || row.wallet || null });
  } catch (e) {
    return ptyV7SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/auth/me", ptyV7AuthOptional, async (req, res) => {
  try {
    const userId = ptyV7Text(req.user?.id || req.query.userId || "");
    if (!userId) return ptyV7SafeJson(res, 401, { ok: false, message: "Sesión inválida" });
    const row = await ptyV7GetUserById(userId);
    if (!row) return ptyV7SafeJson(res, 401, { ok: false, message: "Usuario no encontrado" });
    const user = ptyV7Status(row.role) === "driver" ? ptyV7DriverProfile(row) : ptyV7PublicUser(row);
    return ptyV7SafeJson(res, 200, { ok: true, user });
  } catch (e) {
    return ptyV7SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

const PTY_V7_ACTIVE_STATUSES = [
  "requested","searching","pending","assigned","accepted","arrived","driver_arrived","in_progress","on_trip","en_curso"
];

app.get("/api/rides/active", ptyV7AuthOptional, async (req, res) => {
  try {
    try { if (typeof expireOldRides === "function") await expireOldRides(); } catch {}
    const userId = ptyV7Text(req.user?.id || req.query.userId || req.query.driverId || req.query.riderId || "");
    if (!userId) return ptyV7SafeJson(res, 200, { ok: true, ride: null });
    const r = await db(
      \`SELECT * FROM ride_rides
       WHERE (rider_id::text=$1::text OR driver_id::text=$1::text)
         AND LOWER(COALESCE(status,'')) = ANY($2::text[])
       ORDER BY updated_at DESC NULLS LAST, created_at DESC
       LIMIT 1\`,
      [userId, PTY_V7_ACTIVE_STATUSES]
    );
    const ride = r.rows?.[0] ? await ptyV7EnrichRide(r.rows[0]) : null;
    return ptyV7SafeJson(res, 200, { ok: true, ride });
  } catch (e) {
    return ptyV7SafeJson(res, 500, { ok: false, message: String(e?.message || e), ride: null });
  }
});

app.get("/api/rides/:id", ptyV7AuthOptional, async (req, res) => {
  try {
    const rideId = ptyV7Text(req.params.id);
    if (!rideId || ["active","pending"].includes(rideId)) return ptyV7SafeJson(res, 400, { ok: false, message: "ID inválido" });
    const r = await db(\`SELECT * FROM ride_rides WHERE id::text=$1::text LIMIT 1\`, [rideId]);
    if (!r.rows.length) return ptyV7SafeJson(res, 404, { ok: false, message: "Carrera no encontrada" });
    return ptyV7SafeJson(res, 200, { ok: true, ride: await ptyV7EnrichRide(r.rows[0]) });
  } catch (e) {
    return ptyV7SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/rides/:id/chat", ptyV7AuthOptional, async (req, res) => {
  try {
    await ptyV7EnsureTables();
    const rideId = ptyV7Text(req.params.id);
    const r = await db(\`SELECT * FROM ride_chat_messages WHERE ride_id::text=$1::text ORDER BY created_at ASC LIMIT 500\`, [rideId]);
    const messages = r.rows.map((m) => ({
      id: m.id,
      rideId: m.ride_id,
      senderId: m.sender_id,
      senderRole: m.sender_role,
      senderName: m.sender_name,
      author: m.sender_name || m.sender_role || "Usuario",
      text: m.text,
      message: m.text,
      createdAt: m.created_at,
      at: m.created_at,
      meta: ptyV7Json(m.meta, {}),
    }));
    return ptyV7SafeJson(res, 200, { ok: true, messages, chat: messages });
  } catch (e) {
    return ptyV7SafeJson(res, 500, { ok: false, message: String(e?.message || e), messages: [] });
  }
});

app.post("/api/rides/:id/chat", ptyV7AuthOptional, async (req, res) => {
  try {
    await ptyV7EnsureTables();
    const rideId = ptyV7Text(req.params.id);
    const text = ptyV7Text(req.body.text || req.body.message || "");
    if (!rideId || !text) return ptyV7SafeJson(res, 400, { ok: false, message: "rideId/text requerido" });
    const senderId = ptyV7Text(req.user?.id || req.body.senderId || req.body.userId || "");
    const senderRole = ptyV7Text(req.body.senderRole || req.body.role || req.user?.role || "user");
    const senderName = ptyV7Text(req.body.senderName || req.body.name || req.user?.name || senderRole);
    const r = await db(
      \`INSERT INTO ride_chat_messages(ride_id,sender_id,sender_role,sender_name,text,meta)
       VALUES($1,$2,$3,$4,$5,$6::jsonb) RETURNING *\`,
      [rideId, senderId, senderRole, senderName, text, JSON.stringify(req.body.meta || {})]
    );
    const message = {
      id: r.rows[0].id,
      rideId,
      senderId,
      senderRole,
      senderName,
      author: senderName,
      text,
      message: text,
      createdAt: r.rows[0].created_at,
      at: r.rows[0].created_at,
    };
    try {
      io.to(\`ride:\${rideId}\`).emit("ride.chat.message", message);
      io.emit("ride.chat.message", message);
      io.to("admins").emit("admin.chat.message", message);
    } catch {}
    return ptyV7SafeJson(res, 201, { ok: true, message, chatMessage: message });
  } catch (e) {
    return ptyV7SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/rides/:id/messages", ptyV7AuthOptional, async (req, res) => {
  req.url = \`/api/rides/\${req.params.id}/chat\`;
  return app._router.handle(req, res);
});
app.post("/api/rides/:id/messages", ptyV7AuthOptional, async (req, res) => {
  req.url = \`/api/rides/\${req.params.id}/chat\`;
  return app._router.handle(req, res);
});

async function ptyV7SupportReport(req, res, forcedType = "") {
  try {
    await ptyV7EnsureTables();
    const user = req.user || {};
    const location = ptyV7Point(req.body.location || req.body || {});
    const type = ptyV7Text(forcedType || req.body.type || req.body.reportType || req.body.incidentType || "incident");
    const title = ptyV7Text(req.body.title || (type === "lost_item" ? "Objeto perdido" : "Reporte de soporte"));
    const detail = ptyV7Text(req.body.detail || req.body.note || req.body.description || req.body.message || "");
    const row = await db(
      \`INSERT INTO ride_support_reports(user_id,user_email,user_name,user_phone,role,ride_id,type,title,detail,status,location,meta)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'open',$10::jsonb,$11::jsonb) RETURNING *\`,
      [
        req.body.userId || user.id || "",
        req.body.userEmail || user.email || "",
        req.body.userName || user.name || "",
        req.body.userPhone || user.phone || "",
        req.body.role || user.role || "",
        req.body.rideId || "",
        type,
        title,
        detail,
        JSON.stringify(location || {}),
        JSON.stringify(req.body || {}),
      ]
    );
    try { io.to("admins").emit("support.report", row.rows[0]); } catch {}
    return ptyV7SafeJson(res, 201, { ok: true, report: row.rows[0] });
  } catch (e) {
    return ptyV7SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
}

app.post("/api/support/report", ptyV7AuthOptional, ptyV7SupportReport);
app.post("/api/support/incidents", ptyV7AuthOptional, (req, res) => ptyV7SupportReport(req, res, "incident"));
app.post("/api/support/lost-items", ptyV7AuthOptional, (req, res) => ptyV7SupportReport(req, res, "lost_item"));
app.post("/api/reports", ptyV7AuthOptional, ptyV7SupportReport);

app.post("/api/admin/support/messages", ptyV7AuthOptional, async (req, res) => {
  return ptyV7SupportReport(req, res, req.body.type || "admin_message");
});

app.get("/api/admin/support/reports", ptyV7AuthRequired, ptyV7RequireAdmin, async (_req, res) => {
  try {
    await ptyV7EnsureTables();
    const r = await db(\`SELECT * FROM ride_support_reports ORDER BY created_at DESC LIMIT 500\`);
    return ptyV7SafeJson(res, 200, { ok: true, reports: r.rows });
  } catch (e) {
    return ptyV7SafeJson(res, 500, { ok: false, message: String(e?.message || e), reports: [] });
  }
});

app.patch("/api/admin/support/reports/:id/status", ptyV7AuthRequired, ptyV7RequireAdmin, async (req, res) => {
  try {
    await ptyV7EnsureTables();
    const r = await db(\`UPDATE ride_support_reports SET status=$2, updated_at=NOW() WHERE id::text=$1::text RETURNING *\`, [req.params.id, req.body.status || "closed"]);
    return ptyV7SafeJson(res, 200, { ok: true, report: r.rows[0] || null });
  } catch (e) {
    return ptyV7SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.post("/api/map-reports", ptyV7AuthOptional, async (req, res) => {
  try {
    await ptyV7EnsureTables();
    const point = ptyV7Point(req.body || {});
    if (!point) return ptyV7SafeJson(res, 400, { ok: false, message: "Ubicación requerida" });
    const r = await db(
      \`INSERT INTO ride_map_reports(reporter_id,reporter_role,reporter_name,type,title,detail,icon,color,lat,lng,meta)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb) RETURNING *\`,
      [
        req.body.reporterId || req.body.userId || req.user?.id || "",
        req.body.reporterRole || req.body.role || req.user?.role || "",
        req.body.reporterName || req.body.userName || req.user?.name || "",
        req.body.type || "",
        req.body.title || "",
        req.body.detail || req.body.message || "",
        req.body.icon || "",
        req.body.color || "",
        point.lat,
        point.lng,
        JSON.stringify(req.body || {}),
      ]
    );
    const report = { ...r.rows[0], lat: point.lat, lng: point.lng };
    try { io.emit("map.report", report); io.to("admins").emit("map.report", report); } catch {}
    return ptyV7SafeJson(res, 201, { ok: true, report, item: report });
  } catch (e) {
    return ptyV7SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/map-reports", ptyV7AuthOptional, async (_req, res) => {
  try {
    await ptyV7EnsureTables();
    const r = await db(\`SELECT * FROM ride_map_reports WHERE status='active' ORDER BY created_at DESC LIMIT 500\`);
    return ptyV7SafeJson(res, 200, { ok: true, reports: r.rows, items: r.rows });
  } catch (e) {
    return ptyV7SafeJson(res, 500, { ok: false, message: String(e?.message || e), reports: [] });
  }
});

app.patch("/api/map-reports/:id/confirm", ptyV7AuthOptional, async (req, res) => {
  try {
    await ptyV7EnsureTables();
    const r = await db(\`UPDATE ride_map_reports SET confirmations=COALESCE(confirmations,0)+1, updated_at=NOW() WHERE id::text=$1::text RETURNING *\`, [req.params.id]);
    return ptyV7SafeJson(res, 200, { ok: true, report: r.rows[0] || null });
  } catch (e) {
    return ptyV7SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.patch("/api/map-reports/:id/clear", ptyV7AuthOptional, async (req, res) => {
  try {
    await ptyV7EnsureTables();
    const r = await db(\`UPDATE ride_map_reports SET status='cleared', updated_at=NOW() WHERE id::text=$1::text RETURNING *\`, [req.params.id]);
    return ptyV7SafeJson(res, 200, { ok: true, report: r.rows[0] || null });
  } catch (e) {
    return ptyV7SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

async function ptyV7Sos(req, res) {
  try {
    await ptyV7EnsureTables();
    const user = req.user || {};
    const location = ptyV7Point(req.body.location || req.body || {});
    let ridePayload = req.body.ride || {};
    if (req.body.rideId) {
      const r = await db(\`SELECT * FROM ride_rides WHERE id::text=$1::text LIMIT 1\`, [String(req.body.rideId)]).catch(() => ({ rows: [] }));
      if (r.rows?.[0]) ridePayload = await ptyV7EnrichRide(r.rows[0]);
    }
    const row = await db(
      \`INSERT INTO ride_sos_alerts(user_id,user_email,user_name,user_phone,role,ride_id,message,status,location,trusted_contact,ride)
       VALUES($1,$2,$3,$4,$5,$6,$7,'open',$8::jsonb,$9::jsonb,$10::jsonb) RETURNING *\`,
      [
        req.body.userId || user.id || "",
        req.body.userEmail || user.email || "",
        req.body.userName || user.name || "",
        req.body.userPhone || user.phone || "",
        req.body.role || user.role || "",
        req.body.rideId || "",
        req.body.message || "SOS activado desde la app",
        JSON.stringify(location || {}),
        JSON.stringify(req.body.trustedContact || {}),
        JSON.stringify(ridePayload || {}),
      ]
    );
    try { io.to("admins").emit("sos.alert", row.rows[0]); io.emit("sos.alert", row.rows[0]); } catch {}
    return ptyV7SafeJson(res, 201, { ok: true, alert: row.rows[0] });
  } catch (e) {
    return ptyV7SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
}
app.post("/api/sos", ptyV7AuthOptional, ptyV7Sos);
app.post("/api/security/sos", ptyV7AuthOptional, ptyV7Sos);

app.get("/api/admin/security/sos", ptyV7AuthRequired, ptyV7RequireAdmin, async (_req, res) => {
  try {
    await ptyV7EnsureTables();
    const r = await db(\`SELECT * FROM ride_sos_alerts ORDER BY created_at DESC LIMIT 100\`);
    return ptyV7SafeJson(res, 200, { ok: true, alerts: r.rows, sosAlerts: r.rows });
  } catch (e) {
    return ptyV7SafeJson(res, 500, { ok: false, message: String(e?.message || e), alerts: [] });
  }
});

app.patch("/api/admin/security/sos/:id/status", ptyV7AuthRequired, ptyV7RequireAdmin, async (req, res) => {
  try {
    await ptyV7EnsureTables();
    const r = await db(\`UPDATE ride_sos_alerts SET status=$2, updated_at=NOW() WHERE id::text=$1::text RETURNING *\`, [req.params.id, req.body.status || "closed"]);
    return ptyV7SafeJson(res, 200, { ok: true, alert: r.rows[0] || null });
  } catch (e) {
    return ptyV7SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

async function ptyV7Location(req, res, role) {
  try {
    const userId = ptyV7Text(req.user?.id || req.body.userId || req.body.driverId || req.body.riderId || req.query.userId || "");
    const location = ptyV7Point(req.body.location || req.body.currentLocation || req.body || req.query);
    if (!userId || !location) return ptyV7SafeJson(res, 400, { ok: false, message: "userId/location requerido" });
    const item = { userId, id: userId, role, currentLocation: location, lat: location.lat, lng: location.lng, rideId: req.body.rideId || req.query.rideId || "", at: new Date().toISOString() };
    if (role === "driver") PTY_V7_LIVE.drivers.set(userId, item);
    else PTY_V7_LIVE.riders.set(userId, item);

    await db(
      \`UPDATE ride_users
       SET driver_docs=COALESCE(driver_docs,'{}'::jsonb) || $2::jsonb,
           updated_at=NOW()
       WHERE id::text=$1::text\`,
      [userId, JSON.stringify({ currentLocation: location, lastLocationAt: item.at })]
    ).catch(() => null);

    try { io.to("admins").emit("live.location", item); io.emit(\`\${role}.location.updated\`, item); } catch {}
    return ptyV7SafeJson(res, 200, { ok: true, location: item });
  } catch (e) {
    return ptyV7SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
}
app.post("/api/driver/location", ptyV7AuthOptional, (req, res) => ptyV7Location(req, res, "driver"));
app.patch("/api/driver/location", ptyV7AuthOptional, (req, res) => ptyV7Location(req, res, "driver"));
app.post("/api/rider/location", ptyV7AuthOptional, (req, res) => ptyV7Location(req, res, "rider"));
app.patch("/api/rider/location", ptyV7AuthOptional, (req, res) => ptyV7Location(req, res, "rider"));

app.get("/api/admin/live", ptyV7AuthRequired, ptyV7RequireAdmin, async (_req, res) => {
  try {
    const usersR = await db(\`SELECT * FROM ride_users ORDER BY updated_at DESC NULLS LAST, created_at DESC LIMIT 1000\`).catch(() => ({ rows: [] }));
    const ridesR = await db(
      \`SELECT * FROM ride_rides
       WHERE LOWER(COALESCE(status,'')) = ANY($1::text[])
       ORDER BY updated_at DESC NULLS LAST, created_at DESC
       LIMIT 300\`,
      [PTY_V7_ACTIVE_STATUSES]
    ).catch(() => ({ rows: [] }));

    const users = usersR.rows.map(ptyV7PublicUser);
    const drivers = users.filter((u) => ptyV7Status(u.role) === "driver" && u.currentLocation);
    const riders = users.filter((u) => ptyV7Status(u.role) === "rider" && u.currentLocation);

    for (const item of PTY_V7_LIVE.drivers.values()) {
      if (!drivers.some((d) => String(d.id) === String(item.userId))) drivers.push(item);
    }
    for (const item of PTY_V7_LIVE.riders.values()) {
      if (!riders.some((r) => String(r.id) === String(item.userId))) riders.push(item);
    }

    const activeRides = [];
    for (const row of ridesR.rows) activeRides.push(await ptyV7EnrichRide(row));
    return ptyV7SafeJson(res, 200, { ok: true, drivers, riders, activeRides, rides: activeRides });
  } catch (e) {
    return ptyV7SafeJson(res, 500, { ok: false, message: String(e?.message || e), drivers: [], riders: [], activeRides: [] });
  }
});

app.get("/api/admin/chats", ptyV7AuthRequired, ptyV7RequireAdmin, async (_req, res) => {
  try {
    await ptyV7EnsureTables();
    const r = await db(\`
      SELECT ride_id,
             MAX(created_at) AS updated_at,
             COUNT(*) AS count,
             (ARRAY_AGG(text ORDER BY created_at DESC))[1] AS last_message
      FROM ride_chat_messages
      GROUP BY ride_id
      ORDER BY updated_at DESC
      LIMIT 200
    \`);
    const chats = r.rows.map((row) => ({
      id: row.ride_id,
      rideId: row.ride_id,
      title: \`Viaje \${String(row.ride_id).slice(-6)}\`,
      lastMessage: row.last_message || "",
      updatedAt: row.updated_at,
      count: Number(row.count || 0),
    }));
    return ptyV7SafeJson(res, 200, { ok: true, chats, threads: chats });
  } catch (e) {
    return ptyV7SafeJson(res, 500, { ok: false, message: String(e?.message || e), chats: [] });
  }
});

app.patch("/api/admin/chats/:id/read", ptyV7AuthRequired, ptyV7RequireAdmin, async (_req, res) => {
  return ptyV7SafeJson(res, 200, { ok: true });
});

app.get("/api/admin/support/threads", ptyV7AuthRequired, ptyV7RequireAdmin, async (_req, res) => {
  try {
    await ptyV7EnsureTables();
    const r = await db(\`SELECT * FROM ride_support_reports ORDER BY created_at DESC LIMIT 200\`);
    return ptyV7SafeJson(res, 200, { ok: true, threads: r.rows, reports: r.rows });
  } catch (e) {
    return ptyV7SafeJson(res, 500, { ok: false, message: String(e?.message || e), threads: [] });
  }
});

`;

  src = src.slice(0, idx) + block + src.slice(idx);
  changes++;
  console.log("OK: bloque V7 core services insertado SIN tocar rutas del driver");
} else {
  console.log("OK ya existe: bloque V7 core services");
}

fs.writeFileSync(serverPath, src, "utf8");

console.log("");
console.log(`Listo. Cambios aplicados: ${changes}`);
console.log("");
console.log("Ahora ejecuta:");
console.log("git add ptydrive/server.js");
console.log('git commit -m "Add core session chat support live services"');
console.log("git push");
console.log("");
console.log("Luego Render hará deploy automático. Si no: Manual Deploy > Deploy latest commit.");
