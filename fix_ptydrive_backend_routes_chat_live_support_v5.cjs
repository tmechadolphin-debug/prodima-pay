#!/usr/bin/env node
/**
 * PTYDrive backend V5 - rutas Google exactas + chat/soporte/live/driver profile.
 *
 * Corrige en Render:
 * 1) Google Routes API: /api/routes/drive y aliases. Devuelve polyline real por carretera.
 * 2) Chat de carrera: /api/rides/:id/chat separado por ride_id para que no se mezclen conversaciones.
 * 3) SOS/incidentes/objetos perdidos/reportes: endpoints para botones de ayuda.
 * 4) Live map admin: /api/admin/live con riders, drivers y carreras activas.
 * 5) Ubicación driver/rider: /api/driver/location y /api/rider/location.
 * 6) Ride detail enriquecido: conductor con foto, reseñas/rating y vehículo desde driver_docs.
 *
 * REQUIERE en Google Cloud: Routes API habilitada.
 * REQUIERE en Render: GOOGLE_MAPS_API_KEY con Routes API + Places API + Geocoding API.
 *
 * Uso:
 *   cd /workspaces/prodima-pay
 *   node fix_ptydrive_backend_routes_chat_live_support_v5.cjs
 *   git add ptydrive/server.js
 *   git commit -m "Add routes chat live support driver profile backend"
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
  const bak = `${file}.bak_backend_v5_${stamp}`;
  fs.copyFileSync(file, bak);
  console.log("Backup:", path.relative(root, bak));
}

if (!fs.existsSync(serverPath)) fail(`No encontré ${serverPath}. Ejecuta desde la raíz prodima-pay.`);
backup(serverPath);

let src = fs.readFileSync(serverPath, "utf8");
let changes = 0;

const marker = "/* PTY BACKEND ROUTES CHAT LIVE SUPPORT V5 */";
if (!src.includes(marker)) {
  const candidates = [
    "/* PTY DRIVER PENDING FEED FIX V1 */",
    "/* PTY GOOGLE PLACES + GEOCODING V2 CLEAN HUMAN NAMES */",
    'app.get("/api/rides/active"',
    'app.get("/api/rides/:id"',
    'app.post("/api/rides"'
  ];
  let idx = -1;
  for (const c of candidates) {
    idx = src.indexOf(c);
    if (idx !== -1) break;
  }
  if (idx === -1) fail("No encontré punto para insertar el bloque V5.");

  const block = `
${marker}
const PTY_V5_LIVE = globalThis.PTY_V5_LIVE || (globalThis.PTY_V5_LIVE = {
  drivers: new Map(),
  riders: new Map(),
});

function ptyV5Text(v = "") {
  try {
    if (typeof asText === "function") return asText(v);
  } catch {}
  return String(v ?? "").trim();
}
function ptyV5Num(v, fallback = null) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}
function ptyV5IsUuid(v = "") {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{12}$/i.test(String(v || "").trim());
}
function ptyV5CleanJson(value, fallback = {}) {
  if (!value) return fallback;
  if (typeof value === "object") return value;
  try { return JSON.parse(value); } catch { return fallback; }
}
function ptyV5Point(input = {}) {
  const raw = input || {};
  const lat = ptyV5Num(raw.lat ?? raw.latitude ?? raw[0]);
  const lng = ptyV5Num(raw.lng ?? raw.lon ?? raw.longitude ?? raw[1]);
  if (lat === null || lng === null) return null;
  return { lat, lng };
}
function ptyV5DecodePolyline(encoded = "") {
  let index = 0, lat = 0, lng = 0;
  const coordinates = [];
  const str = String(encoded || "");
  while (index < str.length) {
    let b, shift = 0, result = 0;
    do {
      b = str.charCodeAt(index++) - 63;
      result |= (b & 0x1f) << shift;
      shift += 5;
    } while (b >= 0x20 && index < str.length);
    const dlat = result & 1 ? ~(result >> 1) : result >> 1;
    lat += dlat;

    shift = 0;
    result = 0;
    do {
      b = str.charCodeAt(index++) - 63;
      result |= (b & 0x1f) << shift;
      shift += 5;
    } while (b >= 0x20 && index < str.length);
    const dlng = result & 1 ? ~(result >> 1) : result >> 1;
    lng += dlng;

    coordinates.push({ lat: lat / 1e5, lng: lng / 1e5 });
  }
  return coordinates;
}
function ptyV5GetGoogleKey() {
  return String(process.env.GOOGLE_MAPS_API_KEY || process.env.GOOGLE_API_KEY || "").trim();
}

async function ptyV5EnsureSupportTables() {
  await db(\`
    CREATE TABLE IF NOT EXISTS ride_support_reports (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      user_id TEXT DEFAULT '',
      user_email TEXT DEFAULT '',
      user_name TEXT DEFAULT '',
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
  await db(\`CREATE INDEX IF NOT EXISTS idx_v5_chat_ride ON ride_chat_messages(ride_id, created_at);\`);
  await db(\`CREATE INDEX IF NOT EXISTS idx_v5_reports_status ON ride_support_reports(status, created_at DESC);\`);
  await db(\`CREATE INDEX IF NOT EXISTS idx_v5_sos_status ON ride_sos_alerts(status, created_at DESC);\`);
}

function ptyV5DriverProfileFromUser(row = {}) {
  const docs = ptyV5CleanJson(row.driver_docs || row.driverDocs || {}, {});
  const vehicle = docs.vehicle || docs.vehiculo || {
    make: docs.marcaVehiculo || docs.marca || docs.make || docs.brand || "",
    brand: docs.marcaVehiculo || docs.marca || docs.brand || docs.make || "",
    model: docs.modeloVehiculo || docs.modelo || docs.model || "",
    plate: docs.placa || docs.plate || "",
    color: docs.colorVehiculo || docs.color || "",
    year: docs.anioVehiculo || docs.year || "",
    vehicleType: docs.vehicleType || docs.tipoVehiculo || "car",
  };
  const photo =
    docs.fotoPerfilConductor ||
    docs.foto_conductor ||
    docs.driverProfilePhoto ||
    docs.profilePhoto ||
    row.photo_url ||
    row.avatar_url ||
    row.profile_photo ||
    "";
  const rating = Number(docs.rating || docs.averageRating || row.rating || 5);
  const reviewsCount = Number(docs.reviewsCount || docs.reviewCount || row.reviews_count || 0);
  return {
    id: row.id || "",
    name: row.name || row.email || "Conductor",
    email: row.email || "",
    phone: row.phone || "",
    photoUrl: photo,
    avatarUrl: photo,
    profilePhoto: photo,
    rating: Number.isFinite(rating) ? rating : 5,
    reviewsCount: Number.isFinite(reviewsCount) ? reviewsCount : 0,
    reviews: Array.isArray(docs.reviews) ? docs.reviews : [],
    vehicle: {
      ...vehicle,
      label: [vehicle.brand || vehicle.make || vehicle.marca, vehicle.model || vehicle.modelo, vehicle.plate || vehicle.placa]
        .filter(Boolean)
        .join(" · ") || "Vehículo",
    },
    currentLocation: ptyV5Point(docs.currentLocation || row.current_location || {}) || null,
    driverDocs: docs,
  };
}

function ptyV5PublicUser(row = {}) {
  const docs = ptyV5CleanJson(row.driver_docs || {}, {});
  const currentLocation = ptyV5Point(docs.currentLocation || row.current_location || {}) || null;
  return {
    id: row.id,
    email: row.email || "",
    name: row.name || row.email || "",
    role: row.role || "",
    phone: row.phone || "",
    markerIcon: row.marker_icon || docs.markerIcon || "📍",
    currentLocation,
    lat: currentLocation?.lat,
    lng: currentLocation?.lng,
    driverDocs: docs,
    vehicle: docs.vehicle || docs.vehiculo || null,
    photoUrl: docs.fotoPerfilConductor || docs.profilePhoto || docs.driverProfilePhoto || "",
    avatarUrl: docs.fotoPerfilConductor || docs.profilePhoto || docs.driverProfilePhoto || "",
  };
}

async function ptyV5EnrichRide(row = {}) {
  const ride = typeof normalizeRide === "function" ? normalizeRide(row) : { ...row };
  const riderId = ride.riderId || row.rider_id || row.rider_user_id || "";
  const driverId = ride.driverId || row.driver_id || row.driver_user_id || "";

  let rider = null;
  let driver = null;
  if (riderId) {
    const r = await db(\`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1\`, [String(riderId)]).catch(() => ({ rows: [] }));
    rider = r.rows?.[0] ? ptyV5PublicUser(r.rows[0]) : null;
  }
  if (driverId) {
    const r = await db(\`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1\`, [String(driverId)]).catch(() => ({ rows: [] }));
    driver = r.rows?.[0] ? ptyV5DriverProfileFromUser(r.rows[0]) : null;
  }
  return {
    ...ride,
    rider: { ...(ride.rider || {}), ...(rider || {}) },
    driver: { ...(ride.driver || {}), ...(driver || {}) },
    driverPhoto: driver?.photoUrl || ride.driverPhoto || "",
    driverPhotoUrl: driver?.photoUrl || ride.driverPhotoUrl || "",
    driverRating: driver?.rating || ride.driverRating || 5,
    driverReviewsCount: driver?.reviewsCount || ride.driverReviewsCount || 0,
    driverReviews: driver?.reviews || ride.driverReviews || [],
    vehicle: driver?.vehicle || ride.vehicle || ride.driverVehicle || null,
    driverVehicle: driver?.vehicle || ride.driverVehicle || ride.vehicle || null,
  };
}

async function ptyV5ComputeGoogleRoute(origin, destination) {
  const key = ptyV5GetGoogleKey();
  if (!key) throw new Error("GOOGLE_MAPS_API_KEY no configurada");
  const o = ptyV5Point(origin);
  const d = ptyV5Point(destination);
  if (!o || !d) throw new Error("origin/destination requeridos");

  const res = await fetch("https://routes.googleapis.com/directions/v2:computeRoutes", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Goog-Api-Key": key,
      "X-Goog-FieldMask": "routes.duration,routes.distanceMeters,routes.polyline.encodedPolyline,routes.legs.polyline.encodedPolyline",
    },
    body: JSON.stringify({
      origin: { location: { latLng: { latitude: o.lat, longitude: o.lng } } },
      destination: { location: { latLng: { latitude: d.lat, longitude: d.lng } } },
      travelMode: "DRIVE",
      routingPreference: "TRAFFIC_AWARE",
      computeAlternativeRoutes: false,
      languageCode: "es",
      units: "METRIC",
      polylineQuality: "HIGH_QUALITY",
      polylineEncoding: "ENCODED_POLYLINE",
    }),
  });

  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json?.error?.message || "Google Routes no respondió correctamente");
  const route = json.routes?.[0] || {};
  const encoded = route.polyline?.encodedPolyline || route.legs?.[0]?.polyline?.encodedPolyline || "";
  const coords = ptyV5DecodePolyline(encoded);
  const distanceMeters = Number(route.distanceMeters || 0);
  const seconds = Number(String(route.duration || "0s").replace(/s$/, "") || 0);
  return {
    ok: true,
    provider: "google_routes",
    encodedPolyline: encoded,
    coords,
    routeCoords: coords,
    coordinates: coords,
    distanceMeters,
    distanceKm: Number((distanceMeters / 1000).toFixed(3)),
    durationSeconds: seconds,
    durationMin: Math.max(1, Math.round(seconds / 60)),
  };
}

async function ptyV5RouteHandler(req, res) {
  try {
    const body = req.body || {};
    const origin = body.origin || body.from || {
      lat: req.query.originLat || req.query.lat1 || req.query.fromLat,
      lng: req.query.originLng || req.query.lng1 || req.query.fromLng,
    };
    const destination = body.destination || body.to || {
      lat: req.query.destinationLat || req.query.lat2 || req.query.toLat,
      lng: req.query.destinationLng || req.query.lng2 || req.query.toLng,
    };
    const out = await ptyV5ComputeGoogleRoute(origin, destination);
    return safeJson(res, 200, out);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e), coords: [] });
  }
}

app.get("/api/routes/drive", authOptional, ptyV5RouteHandler);
app.post("/api/routes/drive", authOptional, ptyV5RouteHandler);
app.get("/api/google/routes/drive", authOptional, ptyV5RouteHandler);
app.post("/api/google/routes/drive", authOptional, ptyV5RouteHandler);
app.get("/api/directions/drive", authOptional, ptyV5RouteHandler);
app.post("/api/directions/drive", authOptional, ptyV5RouteHandler);

async function ptyV5LocationHandler(req, res, role) {
  try {
    const userId = req.user?.id || ptyV5Text(req.body.userId || req.body.driverId || req.body.riderId || req.query.userId);
    const rideId = ptyV5Text(req.body.rideId || req.query.rideId || "");
    const location = ptyV5Point(req.body.location || req.body.currentLocation || req.body || req.query);
    if (!userId || !location) return safeJson(res, 400, { ok: false, message: "userId/location requerido" });

    const item = { userId, id: userId, rideId, role, currentLocation: location, lat: location.lat, lng: location.lng, at: new Date().toISOString() };
    if (role === "driver") PTY_V5_LIVE.drivers.set(String(userId), item);
    else PTY_V5_LIVE.riders.set(String(userId), item);

    try {
      await db(
        \`UPDATE ride_users
         SET driver_docs=COALESCE(driver_docs,'{}'::jsonb) || $2::jsonb,
             updated_at=NOW()
         WHERE id::text=$1::text\`,
        [String(userId), JSON.stringify({ currentLocation: location, lastLocationAt: item.at })]
      );
    } catch {}

    try { io.to("admins").emit("live.location", item); io.emit(\`\${role}.location.updated\`, item); } catch {}
    return safeJson(res, 200, { ok: true, location: item });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
}
app.patch("/api/driver/location", authOptional, (req, res) => ptyV5LocationHandler(req, res, "driver"));
app.post("/api/driver/location", authOptional, (req, res) => ptyV5LocationHandler(req, res, "driver"));
app.patch("/api/rider/location", authOptional, (req, res) => ptyV5LocationHandler(req, res, "rider"));
app.post("/api/rider/location", authOptional, (req, res) => ptyV5LocationHandler(req, res, "rider"));

app.get("/api/admin/live", authRequired, requireAdmin, async (_req, res) => {
  try {
    const usersR = await db(\`SELECT * FROM ride_users ORDER BY updated_at DESC LIMIT 1000\`).catch(() => ({ rows: [] }));
    const ridesR = await db(
      \`SELECT * FROM ride_rides
       WHERE LOWER(COALESCE(status,'')) IN ('requested','searching','assigned','accepted','arrived','in_progress')
       ORDER BY created_at DESC LIMIT 250\`
    ).catch(() => ({ rows: [] }));

    const users = usersR.rows.map(ptyV5PublicUser);
    const drivers = users.filter((u) => String(u.role || "").toLowerCase() === "driver" && u.currentLocation);
    const riders = users.filter((u) => String(u.role || "").toLowerCase() === "rider" && u.currentLocation);

    for (const item of PTY_V5_LIVE.drivers.values()) {
      if (!drivers.some((d) => String(d.id) === String(item.userId))) drivers.push(item);
    }
    for (const item of PTY_V5_LIVE.riders.values()) {
      if (!riders.some((r) => String(r.id) === String(item.userId))) riders.push(item);
    }

    const activeRides = [];
    for (const row of ridesR.rows) activeRides.push(await ptyV5EnrichRide(row));
    return safeJson(res, 200, { ok: true, drivers, riders, activeRides, rides: activeRides });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e), drivers: [], riders: [], activeRides: [] });
  }
});

app.get("/api/rides/:id", authOptional, async (req, res) => {
  try {
    if (!ptyV5IsUuid(req.params.id)) return safeJson(res, 400, { ok: false, message: "ID de carrera inválido" });
    const r = await db(\`SELECT * FROM ride_rides WHERE id::text=$1::text LIMIT 1\`, [String(req.params.id)]);
    if (!r.rows.length) return safeJson(res, 404, { ok: false, message: "Carrera no encontrada" });
    const ride = await ptyV5EnrichRide(r.rows[0]);
    return safeJson(res, 200, { ok: true, ride });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/rides/:id/chat", authOptional, async (req, res) => {
  try {
    await ptyV5EnsureSupportTables();
    const rideId = ptyV5Text(req.params.id);
    const r = await db(\`SELECT * FROM ride_chat_messages WHERE ride_id::text=$1::text ORDER BY created_at ASC LIMIT 500\`, [rideId]);
    const messages = r.rows.map((m) => ({
      id: m.id,
      rideId: m.ride_id,
      senderId: m.sender_id,
      senderRole: m.sender_role,
      senderName: m.sender_name,
      author: m.sender_name || m.sender_role || "Usuario",
      text: m.text,
      at: m.created_at,
      createdAt: m.created_at,
      meta: ptyV5CleanJson(m.meta, {}),
    }));
    return safeJson(res, 200, { ok: true, messages, chat: messages });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e), messages: [] });
  }
});

app.post("/api/rides/:id/chat", authOptional, async (req, res) => {
  try {
    await ptyV5EnsureSupportTables();
    const rideId = ptyV5Text(req.params.id);
    const text = ptyV5Text(req.body.text || req.body.message || "");
    if (!rideId || !text) return safeJson(res, 400, { ok: false, message: "rideId/text requerido" });
    const senderId = req.user?.id || ptyV5Text(req.body.senderId || req.body.userId || "");
    const senderRole = ptyV5Text(req.body.senderRole || req.body.role || req.user?.role || "admin");
    const senderName = ptyV5Text(req.body.senderName || req.body.name || req.user?.name || senderRole);
    const r = await db(
      \`INSERT INTO ride_chat_messages(ride_id, sender_id, sender_role, sender_name, text, meta)
       VALUES($1,$2,$3,$4,$5,$6::jsonb) RETURNING *\`,
      [rideId, senderId, senderRole, senderName, text, JSON.stringify(req.body.meta || {})]
    );
    const message = { id: r.rows[0].id, rideId, senderId, senderRole, senderName, author: senderName, text, at: r.rows[0].created_at };
    try { io.to(\`ride:\${rideId}\`).emit("ride.chat.message", message); io.emit("ride.chat.message", message); } catch {}
    return safeJson(res, 201, { ok: true, message });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});
app.get("/api/rides/:id/messages", authOptional, (req, res) => req.app._router.handle(Object.assign(req, { url: \`/api/rides/\${req.params.id}/chat\` }), res));

async function ptyV5ReportHandler(req, res) {
  try {
    await ptyV5EnsureSupportTables();
    const user = req.user || {};
    const location = ptyV5Point(req.body.location || req.body.currentLocation || req.body) || {};
    const type = ptyV5Text(req.body.type || req.body.reportType || "incident");
    const title = ptyV5Text(req.body.title || (type === "lost_item" ? "Objeto perdido" : "Reporte de incidente"));
    const detail = ptyV5Text(req.body.detail || req.body.description || req.body.message || "");
    const r = await db(
      \`INSERT INTO ride_support_reports(user_id,user_email,user_name,role,ride_id,type,title,detail,status,location,meta)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,'open',$9::jsonb,$10::jsonb) RETURNING *\`,
      [
        req.body.userId || user.id || "",
        req.body.userEmail || user.email || "",
        req.body.userName || user.name || "",
        req.body.role || user.role || "",
        req.body.rideId || "",
        type,
        title,
        detail,
        JSON.stringify(location || {}),
        JSON.stringify(req.body || {}),
      ]
    );
    try { io.to("admins").emit("support.report", r.rows[0]); } catch {}
    return safeJson(res, 201, { ok: true, report: r.rows[0] });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
}
app.post("/api/support/report", authOptional, ptyV5ReportHandler);
app.post("/api/reports", authOptional, ptyV5ReportHandler);
app.post("/api/map/reports", authOptional, ptyV5ReportHandler);
app.get("/api/admin/support/reports", authRequired, requireAdmin, async (_req, res) => {
  try {
    await ptyV5EnsureSupportTables();
    const r = await db(\`SELECT * FROM ride_support_reports ORDER BY created_at DESC LIMIT 500\`);
    return safeJson(res, 200, { ok: true, reports: r.rows });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e), reports: [] });
  }
});

async function ptyV5SosHandler(req, res) {
  try {
    await ptyV5EnsureSupportTables();
    const user = req.user || {};
    const location = ptyV5Point(req.body.location || req.body.currentLocation || req.body) || {};
    const r = await db(
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
        JSON.stringify(req.body.ride || {}),
      ]
    );
    try { io.to("admins").emit("sos.alert", r.rows[0]); io.emit("sos.alert", r.rows[0]); } catch {}
    return safeJson(res, 201, { ok: true, alert: r.rows[0] });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
}
app.post("/api/sos", authOptional, ptyV5SosHandler);
app.post("/api/security/sos", authOptional, ptyV5SosHandler);
app.get("/api/admin/security/sos", authRequired, requireAdmin, async (_req, res) => {
  try {
    await ptyV5EnsureSupportTables();
    const r = await db(\`SELECT * FROM ride_sos_alerts ORDER BY created_at DESC LIMIT 100\`);
    return safeJson(res, 200, { ok: true, alerts: r.rows, sosAlerts: r.rows });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e), alerts: [] });
  }
});

app.post("/api/account/trusted-contact", authOptional, async (req, res) => {
  try {
    const userId = req.user?.id || req.body.userId || "";
    const name = ptyV5Text(req.body.name);
    const phone = ptyV5Text(req.body.phone);
    if (!userId || !name || !phone) return safeJson(res, 400, { ok: false, message: "userId/name/phone requerido" });
    const r = await db(
      \`UPDATE ride_users
       SET trusted_contact=$2::jsonb,
           updated_at=NOW()
       WHERE id::text=$1::text RETURNING *\`,
      [String(userId), JSON.stringify({ name, phone, updatedAt: new Date().toISOString() })]
    ).catch(() => ({ rows: [] }));
    return safeJson(res, 200, { ok: true, trustedContact: { name, phone }, user: r.rows?.[0] ? ptyV5PublicUser(r.rows[0]) : null });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

`;
  src = src.slice(0, idx) + block + src.slice(idx);
  changes++;
  console.log("OK: bloque backend V5 insertado");
} else {
  console.log("OK ya existe: bloque backend V5");
}

fs.writeFileSync(serverPath, src, "utf8");
console.log("");
console.log(`Listo. Cambios aplicados: ${changes}`);
console.log("");
console.log("Ahora ejecuta:");
console.log("git add ptydrive/server.js");
console.log('git commit -m "Add routes chat live support driver profile backend"');
console.log("git push");
console.log("");
console.log("Después en Render: habilita Routes API y redeploy.");
