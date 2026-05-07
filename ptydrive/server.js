import express from "express";
import http from "http";
import pg from "pg";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";
import crypto from "crypto";
import multer from "multer";
import { Server as SocketIOServer } from "socket.io";

const { Pool } = pg;

const {
  PORT = 4100,
  NODE_ENV = "development",
  DATABASE_URL = "",
  JWT_SECRET = "",
  CORS_ORIGIN = "*",
  ADMIN_EMAIL = "",
  ADMIN_PASSWORD = "",
  ADMIN_NAME = "Administrador",
  RIDE_EXPIRE_MINUTES = "10",
  SOS_COOLDOWN_MINUTES = "5",
} = process.env;

if (!DATABASE_URL) {
  console.warn("[WARN] DATABASE_URL vacío. Configúralo en Render.");
}
if (!JWT_SECRET || JWT_SECRET.length < 24) {
  console.warn("[WARN] JWT_SECRET debe ser largo y seguro en producción.");
}

const app = express();
const server = http.createServer(app);

app.use(express.json({ limit: "20mb" }));

const documentUpload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: Math.max(1, Number(process.env.MAX_DOCUMENT_UPLOAD_MB || 25)) * 1024 * 1024,
  },
});

/* =========================================================
   CORS
========================================================= */
const allowedOrigins = new Set(
  String(CORS_ORIGIN || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)
);
const allowAllOrigins = !CORS_ORIGIN || CORS_ORIGIN === "*";

app.use((req, res, next) => {
  const origin = req.headers.origin;
  if (allowAllOrigins && origin) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
  } else if (origin && allowedOrigins.has(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
  } else if (allowAllOrigins && !origin) {
    res.setHeader("Access-Control-Allow-Origin", "*");
  }

  res.setHeader("Access-Control-Allow-Methods", "GET,POST,PATCH,PUT,DELETE,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, x-admin-token");
  res.setHeader("Access-Control-Max-Age", "86400");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

/* =========================================================
   Socket.io
========================================================= */
const io = new SocketIOServer(server, {
  cors: {
    origin: allowAllOrigins ? "*" : Array.from(allowedOrigins),
    methods: ["GET", "POST", "PATCH", "PUT", "DELETE"],
  },
  transports: ["websocket", "polling"],
  pingInterval: 25000,
  pingTimeout: 20000,
});

/* =========================================================
   DB
========================================================= */
const pool = new Pool({
  connectionString: DATABASE_URL || undefined,
  ssl: DATABASE_URL ? { rejectUnauthorized: false } : undefined,
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

async function db(text, params = []) {
  return pool.query(text, params);
}

function uuid() {
  return crypto.randomUUID();
}

function nowIso() {
  return new Date().toISOString();
}

function safeJson(res, status, obj) {
  return res.status(status).json(obj);
}

function asText(v) {
  return String(v ?? "").trim();
}

function asNum(v, fallback = null) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function isUuid(value) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(String(value || "").trim());
}

function cleanPhone(v) {
  return asText(v).replace(/[^0-9+]/g, "").trim();
}

function publicUser(u = {}) {
  if (!u) return null;
  return {
    id: u.id,
    email: u.email,
    name: u.name || "",
    role: u.role || "rider",
    phone: u.phone || "",
    markerIcon: u.marker_icon || "📍",
    documentStatus: u.document_status || "pending",
    trustedContact: u.trusted_contact || {},
    createdAt: u.created_at,
    updatedAt: u.updated_at,
  };
}

function documentToPublic(row = {}) {
  if (!row) return null;
  return {
    id: row.id,
    userId: row.user_id,
    role: row.role || "driver",
    type: row.type || "",
    key: row.type || "",
    documentType: row.type || "",
    status: row.status || "pending",
    reason: row.reason || "",
    url: row.url || "",
    fileUrl: row.url || "",
    previewUrl: row.url || "",
    filename: row.filename || "",
    fileName: row.filename || "",
    mimeType: row.mime_type || "",
    sizeBytes: Number(row.size_bytes || 0),
    meta: row.meta || {},
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

const DRIVER_REQUIRED_DOCUMENT_TYPES = [
  "fotoPerfilConductor",
  "selfieConLicencia",
  "cedulaFoto",
  "licenciaFoto",
  "recordPolicivo",
  "registroVehicular",
  "seguroVehicular",
  "inspeccionVehicular",
  "vehiculoFrontal",
  "vehiculoTrasero",
  "vehiculoLateral",
];

function requiredDocumentTypesForRole(role = "driver") {
  return String(role || "driver").toLowerCase() === "rider"
    ? ["identity"]
    : DRIVER_REQUIRED_DOCUMENT_TYPES;
}

async function listUserDocuments(userId, role = "") {
  const params = [userId];
  let where = "user_id::text=$1::text";
  if (role) {
    params.push(role);
    where += ` AND role=${params.length}`;
  }
  const r = await db(
    `SELECT * FROM ride_documents WHERE ${where} ORDER BY created_at DESC`,
    params
  );
  return r.rows.map(documentToPublic);
}

function buildStatusMap(documents = []) {
  return documents.reduce((acc, doc) => {
    const type = String(doc.type || doc.documentType || doc.key || "").trim();
    if (type) acc[type] = String(doc.status || "pending").toLowerCase();
    return acc;
  }, {});
}

function areDocumentsApproved(documents = [], role = "driver") {
  const required = requiredDocumentTypesForRole(role);
  const statusMap = buildStatusMap(documents);
  const missing = required.filter((type) => statusMap[type] !== "approved");
  return { approved: missing.length === 0, missing, statusMap };
}

function buildDocumentUserPayload(userRow, role, approved, extra = {}) {
  const base = publicUser(userRow || {});
  const normalizedRole = String(role || base?.role || "").toLowerCase();
  return {
    ...base,
    ...extra,
    driverDocumentsApproved: normalizedRole === "driver" ? Boolean(approved) : Boolean(extra.driverDocumentsApproved),
    canAcceptRides: normalizedRole === "driver" ? Boolean(approved) : Boolean(extra.canAcceptRides),
    riderVerified: normalizedRole === "rider" ? Boolean(approved) : Boolean(extra.riderVerified),
    identityVerified: normalizedRole === "rider" ? Boolean(approved) : Boolean(extra.identityVerified),
    verificationStatus: approved ? "approved" : (base?.documentStatus || "pending"),
  };
}

function signToken(user) {
  return jwt.sign(
    {
      id: user.id,
      email: user.email,
      name: user.name || "",
      role: user.role || "rider",
    },
    JWT_SECRET || "dev_secret_change_me_now_ptydrive",
    { expiresIn: "30d" }
  );
}

function readBearer(req) {
  const h = asText(req.headers.authorization);
  const m = h.match(/^Bearer\s+(.+)$/i);
  return m ? m[1] : "";
}

async function authOptional(req, _res, next) {
  const token = readBearer(req);
  if (!token) return next();
  try {
    const payload = jwt.verify(token, JWT_SECRET || "dev_secret_change_me_now_ptydrive");
    const r = await db(`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1`, [payload.id]);
    req.user = r.rows[0] || null;
  } catch {
    req.user = null;
  }
  next();
}

async function authRequired(req, res, next) {
  const token = readBearer(req);
  if (!token) return safeJson(res, 401, { ok: false, message: "Missing Bearer token" });
  try {
    const payload = jwt.verify(token, JWT_SECRET || "dev_secret_change_me_now_ptydrive");
    const r = await db(`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1`, [payload.id]);
    const user = r.rows[0];
    if (!user) return safeJson(res, 401, { ok: false, message: "Invalid user" });
    req.user = user;
    next();
  } catch {
    return safeJson(res, 401, { ok: false, message: "Invalid token" });
  }
}

function requireAdmin(req, res, next) {
  if (req.user?.role !== "admin") {
    return safeJson(res, 403, { ok: false, message: "Admin requerido" });
  }
  next();
}

async function ensureDb() {
  await db(`CREATE EXTENSION IF NOT EXISTS pgcrypto;`);

  await db(`
    CREATE TABLE IF NOT EXISTS ride_users (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      email TEXT UNIQUE NOT NULL,
      password_hash TEXT NOT NULL,
      name TEXT DEFAULT '',
      role TEXT DEFAULT 'rider',
      phone TEXT DEFAULT '',
      marker_icon TEXT DEFAULT '📍',
      trusted_contact JSONB DEFAULT '{}'::jsonb,
      driver_docs JSONB DEFAULT '{}'::jsonb,
      document_status TEXT DEFAULT 'pending',
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  await db(`
    CREATE TABLE IF NOT EXISTS ride_documents (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      user_id UUID REFERENCES ride_users(id) ON DELETE CASCADE,
      role TEXT DEFAULT 'driver',
      type TEXT NOT NULL,
      status TEXT DEFAULT 'pending',
      reason TEXT DEFAULT '',
      url TEXT DEFAULT '',
      filename TEXT DEFAULT '',
      mime_type TEXT DEFAULT '',
      size_bytes INTEGER DEFAULT 0,
      meta JSONB DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  await db(`
    CREATE TABLE IF NOT EXISTS ride_rides (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      rider_id UUID REFERENCES ride_users(id) ON DELETE SET NULL,
      driver_id UUID REFERENCES ride_users(id) ON DELETE SET NULL,
      status TEXT DEFAULT 'requested',
      pickup JSONB DEFAULT '{}'::jsonb,
      destination JSONB DEFAULT '{}'::jsonb,
      route JSONB DEFAULT '{}'::jsonb,
      fare NUMERIC(12,2) DEFAULT 2.00,
      distance_km NUMERIC(12,3) DEFAULT 0,
      duration_min NUMERIC(12,2) DEFAULT 0,
      payment_method TEXT DEFAULT 'cash',
      rider_snapshot JSONB DEFAULT '{}'::jsonb,
      driver_snapshot JSONB DEFAULT '{}'::jsonb,
      cancel_reason TEXT DEFAULT '',
      created_at TIMESTAMPTZ DEFAULT NOW(),
      expires_at TIMESTAMPTZ DEFAULT NOW() + INTERVAL '10 minutes',
      accepted_at TIMESTAMPTZ,
      started_at TIMESTAMPTZ,
      completed_at TIMESTAMPTZ,
      cancelled_at TIMESTAMPTZ,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  await db(`
    CREATE TABLE IF NOT EXISTS ride_locations (
      user_id UUID REFERENCES ride_users(id) ON DELETE CASCADE,
      role TEXT DEFAULT '',
      lat NUMERIC(12,8) NOT NULL,
      lng NUMERIC(12,8) NOT NULL,
      heading NUMERIC(12,4),
      speed NUMERIC(12,4),
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      PRIMARY KEY(user_id, role)
    );
  `);

  await db(`
    CREATE TABLE IF NOT EXISTS ride_chat_messages (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      ride_id UUID REFERENCES ride_rides(id) ON DELETE CASCADE,
      sender_id UUID REFERENCES ride_users(id) ON DELETE SET NULL,
      sender_role TEXT DEFAULT '',
      target_user_id UUID REFERENCES ride_users(id) ON DELETE SET NULL,
      message TEXT NOT NULL,
      source TEXT DEFAULT 'ride',
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  await db(`
    CREATE TABLE IF NOT EXISTS ride_sos_alerts (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      ride_id UUID REFERENCES ride_rides(id) ON DELETE SET NULL,
      user_id UUID REFERENCES ride_users(id) ON DELETE SET NULL,
      user_role TEXT DEFAULT '',
      status TEXT DEFAULT 'open',
      location JSONB DEFAULT '{}'::jsonb,
      route JSONB DEFAULT '{}'::jsonb,
      ride_snapshot JSONB DEFAULT '{}'::jsonb,
      user_snapshot JSONB DEFAULT '{}'::jsonb,
      message TEXT DEFAULT '',
      created_at TIMESTAMPTZ DEFAULT NOW(),
      closed_at TIMESTAMPTZ
    );
  `);

  await db(`
    CREATE TABLE IF NOT EXISTS ride_events (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      ride_id UUID REFERENCES ride_rides(id) ON DELETE CASCADE,
      user_id UUID REFERENCES ride_users(id) ON DELETE SET NULL,
      type TEXT NOT NULL,
      payload JSONB DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  await db(`CREATE INDEX IF NOT EXISTS idx_ride_rides_status ON ride_rides(status);`);
  await db(`CREATE INDEX IF NOT EXISTS idx_ride_rides_rider ON ride_rides(rider_id);`);
  await db(`CREATE INDEX IF NOT EXISTS idx_ride_rides_driver ON ride_rides(driver_id);`);
  await db(`CREATE INDEX IF NOT EXISTS idx_ride_rides_expires ON ride_rides(expires_at);`);
  await db(`CREATE INDEX IF NOT EXISTS idx_ride_locations_role ON ride_locations(role, updated_at);`);
  await db(`CREATE INDEX IF NOT EXISTS idx_ride_sos_status ON ride_sos_alerts(status, created_at);`);
  await db(`CREATE INDEX IF NOT EXISTS idx_ride_chat_ride ON ride_chat_messages(ride_id, created_at);`);
  await db(`CREATE INDEX IF NOT EXISTS idx_ride_documents_user ON ride_documents(user_id, role, type);`);
  await db(`CREATE INDEX IF NOT EXISTS idx_ride_documents_status ON ride_documents(status, created_at DESC);`);

  if (ADMIN_EMAIL && ADMIN_PASSWORD) {
    const email = ADMIN_EMAIL.toLowerCase().trim();
    const exists = await db(`SELECT id FROM ride_users WHERE email=$1 LIMIT 1`, [email]);
    if (!exists.rows.length) {
      const hash = await bcrypt.hash(ADMIN_PASSWORD, 10);
      await db(
        `INSERT INTO ride_users(email, password_hash, name, role, phone, document_status)
         VALUES($1,$2,$3,'admin','', 'approved')`,
        [email, hash, ADMIN_NAME]
      );
      console.log("[DB] Admin inicial creado:", email);
    }
  }
}

async function expireOldRides() {
  const r = await db(
    `UPDATE ride_rides
     SET status='expired', cancel_reason='auto_expired_10min', cancelled_at=NOW(), updated_at=NOW()
     WHERE status IN ('requested','searching')
       AND expires_at < NOW()
     RETURNING *`
  );

  for (const ride of r.rows) {
    emitRide(ride, "ride:expired");
  }
  return r.rows.length;
}

function normalizePoint(input = {}) {
  if (!input || typeof input !== "object") return {};
  const lat = asNum(input.lat ?? input.latitude);
  const lng = asNum(input.lng ?? input.longitude);
  return {
    ...input,
    ...(lat !== null ? { lat } : {}),
    ...(lng !== null ? { lng } : {}),
    address: asText(input.address || input.title || input.short || input.name),
  };
}

function getSnapshotUser(user = {}) {
  return {
    id: user.id,
    name: user.name || "",
    email: user.email || "",
    phone: user.phone || "",
    role: user.role || "",
    markerIcon: user.marker_icon || "📍",
  };
}

function normalizeRide(row = {}) {
  const pickup = row.pickup || {};
  const destination = row.destination || {};
  const rider = row.rider_snapshot || {};
  const driver = row.driver_snapshot || {};
  return {
    id: row.id,
    riderId: row.rider_id,
    driverId: row.driver_id,
    status: row.status,
    pickup,
    destination,
    route: row.route || {},
    pickupAddress: pickup.address || pickup.title || pickup.short || "Recogida",
    destinationAddress: destination.address || destination.title || destination.short || "Destino",
    fare: Number(row.fare || 0),
    price: Number(row.fare || 0),
    total: Number(row.fare || 0),
    distanceKm: Number(row.distance_km || 0),
    routeDistanceKm: Number(row.distance_km || 0),
    durationMin: Number(row.duration_min || 0),
    paymentMethod: row.payment_method || "cash",
    rider,
    driver,
    riderSnapshot: rider,
    driverSnapshot: driver,
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

function emitToUser(userId, event, payload) {
  if (!userId) return;
  io.to(`user:${userId}`).emit(event, payload);
}

function emitRide(row, event = "ride:update") {
  const ride = normalizeRide(row);
  io.to("drivers").emit(event, ride);
  if (ride.riderId) emitToUser(ride.riderId, event, ride);
  if (ride.driverId) emitToUser(ride.driverId, event, ride);
  io.to("admins").emit(event, ride);
}

async function getRideById(id) {
  if (!isUuid(id)) return null;
  const r = await db(`SELECT * FROM ride_rides WHERE id::text=$1::text LIMIT 1`, [id]);
  return r.rows[0] || null;
}

async function cancelOpenRidesForRider(riderId, reason = "new_request_replaces_open") {
  const r = await db(
    `UPDATE ride_rides
     SET status='auto_cancelled', cancel_reason=$2, cancelled_at=NOW(), updated_at=NOW()
     WHERE rider_id=$1 AND status IN ('requested','searching','assigned')
     RETURNING *`,
    [riderId, reason]
  );
  for (const ride of r.rows) emitRide(ride, "ride:cancelled");
  return r.rows;
}

/* =========================================================
   Health
========================================================= */
app.get("/", (_req, res) => {
  res.json({
    ok: true,
    app: "PTY Drive API",
    env: NODE_ENV,
    time: nowIso(),
  });
});

app.get("/api/health", async (_req, res) => {
  let dbOk = false;
  try {
    await db(`SELECT 1`);
    dbOk = true;
  } catch {}
  res.json({
    ok: true,
    app: "PTY Drive API",
    db: dbOk ? "on" : "off",
    socket: "on",
    time: nowIso(),
  });
});

/* =========================================================
   Auth
========================================================= */
app.post("/api/auth/register", async (req, res) => {
  try {
    const email = asText(req.body.email || req.body.username).toLowerCase();
    const password = asText(req.body.password || req.body.pass);
    const name = asText(req.body.name || req.body.fullName || req.body.full_name);
    const role = asText(req.body.role || "rider").toLowerCase();
    const phone = cleanPhone(req.body.phone);

    if (!email || !email.includes("@")) {
      return safeJson(res, 400, { ok: false, message: "Email requerido" });
    }
    if (!password || password.length < 4) {
      return safeJson(res, 400, { ok: false, message: "Contraseña requerida" });
    }
    if (!phone || phone.length < 7) {
      return safeJson(res, 400, { ok: false, message: "Teléfono obligatorio para registrar/verificar cuenta" });
    }

    const hash = await bcrypt.hash(password, 10);
    const r = await db(
      `INSERT INTO ride_users(email, password_hash, name, role, phone, document_status)
       VALUES($1,$2,$3,$4,$5,'pending')
       ON CONFLICT(email) DO UPDATE SET
         name=COALESCE(NULLIF(EXCLUDED.name,''), ride_users.name),
         phone=COALESCE(NULLIF(EXCLUDED.phone,''), ride_users.phone),
         updated_at=NOW()
       RETURNING *`,
      [email, hash, name, ["rider", "driver", "admin"].includes(role) ? role : "rider", phone]
    );

    const user = r.rows[0];
    const token = signToken(user);
    return safeJson(res, 200, { ok: true, token, user: publicUser(user) });
  } catch (e) {
    const msg = String(e?.message || e);
    return safeJson(res, 500, { ok: false, message: msg });
  }
});

app.post("/api/auth/login", async (req, res) => {
  try {
    const email = asText(req.body.email || req.body.username).toLowerCase();
    const password = asText(req.body.password || req.body.pass);
    const r = await db(`SELECT * FROM ride_users WHERE email=$1 LIMIT 1`, [email]);
    const user = r.rows[0];

    if (!user) return safeJson(res, 401, { ok: false, message: "Credenciales inválidas" });

    const ok = await bcrypt.compare(password, user.password_hash);
    if (!ok) return safeJson(res, 401, { ok: false, message: "Credenciales inválidas" });

    return safeJson(res, 200, { ok: true, token: signToken(user), user: publicUser(user) });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/auth/me", authRequired, async (req, res) => {
  return safeJson(res, 200, { ok: true, user: publicUser(req.user) });
});

app.patch("/api/users/profile", authRequired, async (req, res) => {
  try {
    const phone = req.body.phone !== undefined ? cleanPhone(req.body.phone) : undefined;
    const markerIcon = req.body.markerIcon !== undefined ? asText(req.body.markerIcon) : undefined;
    const trustedContact = req.body.trustedContact !== undefined ? req.body.trustedContact : undefined;
    const name = req.body.name !== undefined ? asText(req.body.name) : undefined;

    const r = await db(
      `UPDATE ride_users SET
        name=COALESCE($2, name),
        phone=COALESCE($3, phone),
        marker_icon=COALESCE($4, marker_icon),
        trusted_contact=COALESCE($5::jsonb, trusted_contact),
        updated_at=NOW()
       WHERE id::text=$1::text
       RETURNING *`,
      [
        req.user.id,
        name ?? null,
        phone ?? null,
        markerIcon ?? null,
        trustedContact !== undefined ? JSON.stringify(trustedContact || {}) : null,
      ]
    );

    return safeJson(res, 200, { ok: true, user: publicUser(r.rows[0]) });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.patch("/api/profile", authRequired, async (req, res) => {
  req.url = "/api/users/profile";
  return app._router.handle(req, res);
});



/* PTY GOOGLE PLACES + GEOCODING V2 CLEAN HUMAN NAMES */
function ptyGv2Key() {
  return String(process.env.GOOGLE_MAPS_API_KEY || process.env.GOOGLE_API_KEY || "").trim();
}

function ptyGv2Num(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function ptyGv2LooksLikeCode(value = "") {
  const text = String(value || "").trim();
  if (!text) return true;
  if (/^[23456789CFGHJMPQRVWX]{4,}\+[23456789CFGHJMPQRVWX]{2,}/i.test(text)) return true; // Plus Code
  if (/^[A-Z0-9]{3,}\+[A-Z0-9]{2,}/i.test(text)) return true; // XFQM+C22
  if (/^\d{3,}[-–]\d{1,}$/i.test(text)) return true; // 114375-3
  if (/^\d+\.\d+\s*,\s*-?\d+\.\d+/.test(text)) return true; // coordenadas
  if (/^(destino|recogida)\s+-?\d+\.\d+\s*,\s*-?\d+\.\d+/i.test(text)) return true;
  return false;
}

function ptyGv2CleanAddress(value = "", fallback = "") {
  let text = String(value || "").trim();
  text = text
    .replace(/^[A-Z0-9]{3,}\+[A-Z0-9]{2,}\s*,\s*/i, "")
    .replace(/^\d{3,}[-–]\d{1,}\s*,\s*/i, "")
    .replace(/,\s*Panamá\s*,\s*Panamá$/i, ", Panamá")
    .replace(/,\s*Panama\s*,\s*Panama$/i, ", Panamá")
    .replace(/,\s*Provincia de Panamá\s*,\s*Panamá$/i, ", Panamá")
    .replace(/\s+/g, " ")
    .trim();
  if (!text || ptyGv2LooksLikeCode(text)) return String(fallback || "").trim();
  return text;
}

function ptyGv2PickComponent(components = []) {
  const preferredTypes = [
    "establishment",
    "point_of_interest",
    "premise",
    "subpremise",
    "street_address",
    "route",
    "neighborhood",
    "sublocality",
    "locality",
  ];
  for (const type of preferredTypes) {
    const found = components.find((c) => Array.isArray(c.types) && c.types.includes(type));
    const name = ptyGv2CleanAddress(found?.long_name || found?.short_name || "");
    if (name && !ptyGv2LooksLikeCode(name)) return name;
  }
  return "";
}

function ptyGv2ScorePlace(place = {}) {
  const types = Array.isArray(place.types) ? place.types : [];
  const name = ptyGv2CleanAddress(place.name || place.title || place.formatted_address || "");
  let score = 0;
  if (name && !ptyGv2LooksLikeCode(name)) score += 1000;
  if (types.includes("establishment")) score += 280;
  if (types.includes("point_of_interest")) score += 260;
  if (types.includes("premise")) score += 240;
  if (types.includes("shopping_mall")) score += 220;
  if (types.includes("store")) score += 120;
  if (types.includes("parking")) score -= 70;
  if (types.includes("route")) score -= 180;
  if (types.includes("plus_code")) score -= 500;
  if (String(place.business_status || "").toUpperCase() === "OPERATIONAL") score += 40;
  if (ptyGv2LooksLikeCode(name)) score -= 1000;
  return score;
}

function ptyGv2PlaceFromGoogle(result = {}, fallbackName = "") {
  const loc = result?.geometry?.location || {};
  const lat = ptyGv2Num(typeof loc.lat === "function" ? loc.lat() : loc.lat);
  const lng = ptyGv2Num(typeof loc.lng === "function" ? loc.lng() : loc.lng);
  const address = ptyGv2CleanAddress(result.formatted_address || result.vicinity || result.description || "", fallbackName);
  let name = ptyGv2CleanAddress(result.name || result.structured_formatting?.main_text || fallbackName || "", "");
  if (!name || ptyGv2LooksLikeCode(name)) name = ptyGv2PickComponent(result.address_components || []);
  if (!name || ptyGv2LooksLikeCode(name)) name = address || fallbackName || "Destino";
  if (ptyGv2LooksLikeCode(name) && address) name = address;

  return {
    id: result.place_id || `google_${lat}_${lng}`,
    placeId: result.place_id || "",
    googlePlaceId: result.place_id || "",
    source: "google_places",
    title: name,
    short: name,
    name,
    placeName: name,
    exactName: name,
    destinationName: name,
    destinationLabel: name,
    destinationTitle: name,
    lat,
    lng,
    address: address || name,
    fullAddress: address || name,
    display_name: address || name,
    searchSubtitle: address || name,
    subtitle: address || name,
    types: result.types || [],
    businessStatus: result.business_status || "",
    _score: ptyGv2ScorePlace({ ...result, name }),
  };
}

async function ptyGv2Json(url) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 8500);
  try {
    const r = await fetch(url, { signal: controller.signal, headers: { Accept: "application/json" } });
    const json = await r.json().catch(() => ({}));
    return { ok: r.ok, status: r.status, json };
  } finally {
    clearTimeout(timeout);
  }
}

async function ptyGv2Details(placeId = "", fallbackName = "") {
  const key = ptyGv2Key();
  if (!key) throw new Error("GOOGLE_MAPS_API_KEY no configurada en Render");
  if (!placeId) throw new Error("placeId requerido");
  const url = new URL("https://maps.googleapis.com/maps/api/place/details/json");
  url.searchParams.set("place_id", placeId);
  url.searchParams.set("language", "es");
  url.searchParams.set("fields", "place_id,name,formatted_address,geometry,types,business_status,address_components,vicinity");
  url.searchParams.set("key", key);
  const { json } = await ptyGv2Json(url);
  if (json.status && json.status !== "OK") throw new Error(json.error_message || `Google details: ${json.status}`);
  return ptyGv2PlaceFromGoogle(json.result || {}, fallbackName);
}

async function ptyGv2Autocomplete({ q, lat, lng, limit }) {
  const key = ptyGv2Key();
  if (!key) throw new Error("GOOGLE_MAPS_API_KEY no configurada en Render");
  const cleanQ = String(q || "").trim();
  if (!cleanQ) return [];

  const max = Math.max(1, Math.min(12, Number(limit || 8)));
  const url = new URL("https://maps.googleapis.com/maps/api/place/autocomplete/json");
  url.searchParams.set("input", cleanQ);
  url.searchParams.set("language", "es");
  url.searchParams.set("components", "country:pa");
  url.searchParams.set("key", key);
  const latN = ptyGv2Num(lat);
  const lngN = ptyGv2Num(lng);
  if (latN !== null && lngN !== null) {
    url.searchParams.set("location", `${latN},${lngN}`);
    url.searchParams.set("radius", "60000");
  }

  const { json } = await ptyGv2Json(url);
  if (json.status && !["OK", "ZERO_RESULTS"].includes(json.status)) throw new Error(json.error_message || `Google autocomplete: ${json.status}`);
  const predictions = Array.isArray(json.predictions) ? json.predictions.slice(0, max) : [];
  const places = [];
  for (const p of predictions) {
    try {
      places.push(await ptyGv2Details(p.place_id, p.structured_formatting?.main_text || p.description || cleanQ));
    } catch {}
  }
  return places;
}

async function ptyGv2TextSearch({ q, lat, lng, limit }) {
  const key = ptyGv2Key();
  if (!key) throw new Error("GOOGLE_MAPS_API_KEY no configurada en Render");
  const cleanQ = String(q || "").trim();
  if (!cleanQ) return [];

  const max = Math.max(1, Math.min(12, Number(limit || 8)));
  const url = new URL("https://maps.googleapis.com/maps/api/place/textsearch/json");
  url.searchParams.set("query", /panam[áa]/i.test(cleanQ) ? cleanQ : `${cleanQ}, Panamá`);
  url.searchParams.set("language", "es");
  url.searchParams.set("region", "pa");
  url.searchParams.set("key", key);
  const latN = ptyGv2Num(lat);
  const lngN = ptyGv2Num(lng);
  if (latN !== null && lngN !== null) {
    url.searchParams.set("location", `${latN},${lngN}`);
    url.searchParams.set("radius", "60000");
  }
  const { json } = await ptyGv2Json(url);
  if (json.status && !["OK", "ZERO_RESULTS"].includes(json.status)) throw new Error(json.error_message || `Google text search: ${json.status}`);
  return (Array.isArray(json.results) ? json.results : []).slice(0, max).map((r) => ptyGv2PlaceFromGoogle(r, cleanQ));
}

async function ptyGv2Nearby(lat, lng, radius = 90) {
  const key = ptyGv2Key();
  if (!key) throw new Error("GOOGLE_MAPS_API_KEY no configurada en Render");
  const latN = ptyGv2Num(lat);
  const lngN = ptyGv2Num(lng);
  if (latN === null || lngN === null) return [];

  const url = new URL("https://maps.googleapis.com/maps/api/place/nearbysearch/json");
  url.searchParams.set("location", `${latN},${lngN}`);
  url.searchParams.set("radius", String(Math.max(25, Math.min(180, Number(radius || 90)))));
  url.searchParams.set("language", "es");
  url.searchParams.set("key", key);
  const { json } = await ptyGv2Json(url);
  if (json.status && !["OK", "ZERO_RESULTS"].includes(json.status)) throw new Error(json.error_message || `Google nearby: ${json.status}`);
  return (Array.isArray(json.results) ? json.results : [])
    .map((r) => ptyGv2PlaceFromGoogle(r))
    .filter((p) => p.title && !ptyGv2LooksLikeCode(p.title))
    .sort((a, b) => Number(b._score || 0) - Number(a._score || 0))
    .slice(0, 5);
}

async function ptyGv2Reverse(lat, lng) {
  const key = ptyGv2Key();
  if (!key) throw new Error("GOOGLE_MAPS_API_KEY no configurada en Render");
  const latN = ptyGv2Num(lat);
  const lngN = ptyGv2Num(lng);
  if (latN === null || lngN === null) throw new Error("lat/lng requeridos");

  // Primero intentamos POI cercano; esto evita códigos tipo XFQM+C22 cuando el pin cae sobre un mall, hospital, local, etc.
  let nearby = [];
  try {
    nearby = await ptyGv2Nearby(latN, lngN, 100);
  } catch (e) {
    console.warn("[GOOGLE_NEARBY_WARN]", e?.message || e);
  }
  if (nearby.length) {
    const best = nearby[0];
    return {
      label: best.title,
      title: best.title,
      name: best.title,
      address: best.address || best.title,
      display_name: best.address || best.title,
      place: { ...best, lat: latN, lng: lngN },
      results: nearby,
      provider: "google_nearby",
    };
  }

  const url = new URL("https://maps.googleapis.com/maps/api/geocode/json");
  url.searchParams.set("latlng", `${latN},${lngN}`);
  url.searchParams.set("language", "es");
  url.searchParams.set("region", "pa");
  url.searchParams.set("key", key);
  const { json } = await ptyGv2Json(url);
  if (json.status && !["OK", "ZERO_RESULTS"].includes(json.status)) throw new Error(json.error_message || `Google geocode: ${json.status}`);

  const results = Array.isArray(json.results) ? json.results : [];
  const filtered = results
    .map((r) => ({ raw: r, place: ptyGv2PlaceFromGoogle(r) }))
    .filter((x) => x.place.address || x.place.title)
    .sort((a, b) => Number(b.place._score || 0) - Number(a.place._score || 0));

  const selected = filtered[0]?.place || {
    title: "Punto seleccionado",
    short: "Punto seleccionado",
    name: "Punto seleccionado",
    address: "Punto seleccionado",
    fullAddress: "Punto seleccionado",
    display_name: "Punto seleccionado",
    lat: latN,
    lng: lngN,
    source: "google_geocode",
  };

  if (ptyGv2LooksLikeCode(selected.title) && selected.address) {
    selected.title = selected.address;
    selected.short = selected.address;
    selected.name = selected.address;
  }

  return {
    label: selected.title || selected.address || "Punto seleccionado",
    title: selected.title || selected.address || "Punto seleccionado",
    name: selected.title || selected.address || "Punto seleccionado",
    address: selected.address || selected.title || "Punto seleccionado",
    display_name: selected.address || selected.title || "Punto seleccionado",
    place: { ...selected, lat: latN, lng: lngN },
    results: filtered.slice(0, 5).map((x) => x.place),
    provider: "google_geocode",
  };
}

async function ptyGv2SearchHandler(req, res) {
  try {
    const q = asText(req.query.q || req.query.input || req.query.query || "");
    if (!q) return safeJson(res, 200, { ok: true, provider: "google_v2", places: [], results: [] });
    let places = [];
    try {
      places = await ptyGv2Autocomplete({ q, lat: req.query.lat, lng: req.query.lng, limit: req.query.limit || 8 });
    } catch (e) {
      console.warn("[GOOGLE_V2_AUTOCOMPLETE_WARN]", e?.message || e);
    }
    if (!places.length) places = await ptyGv2TextSearch({ q, lat: req.query.lat, lng: req.query.lng, limit: req.query.limit || 8 });

    const seen = new Set();
    const clean = places
      .filter((p) => p && Number.isFinite(Number(p.lat)) && Number.isFinite(Number(p.lng)))
      .filter((p) => p.title && !ptyGv2LooksLikeCode(p.title))
      .sort((a, b) => Number(b._score || 0) - Number(a._score || 0))
      .filter((p) => {
        const key = String(p.placeId || `${p.title}_${Number(p.lat).toFixed(5)}_${Number(p.lng).toFixed(5)}`).toLowerCase();
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      })
      .map(({ _score, ...p }) => p);

    return safeJson(res, 200, { ok: true, provider: "google_v2", places: clean, results: clean });
  } catch (e) {
    console.error("[GOOGLE_V2_PLACES_ERROR]", e);
    return safeJson(res, 500, { ok: false, message: String(e?.message || e), places: [], results: [] });
  }
}

async function ptyGv2ReverseHandler(req, res) {
  try {
    const out = await ptyGv2Reverse(req.query.lat, req.query.lng);
    return safeJson(res, 200, { ok: true, ...out });
  } catch (e) {
    console.error("[GOOGLE_V2_REVERSE_ERROR]", e);
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
}

app.get("/api/places/search", authOptional, ptyGv2SearchHandler);
app.get("/api/places/autocomplete", authOptional, ptyGv2SearchHandler);
app.get("/api/google/places/autocomplete", authOptional, ptyGv2SearchHandler);

app.get("/api/places/details", authOptional, async (req, res) => {
  try {
    const placeId = asText(req.query.placeId || req.query.place_id || "");
    const place = await ptyGv2Details(placeId);
    return safeJson(res, 200, { ok: true, place, result: place });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/google/places/details", authOptional, async (req, res) => {
  try {
    const placeId = asText(req.query.placeId || req.query.place_id || "");
    const place = await ptyGv2Details(placeId);
    return safeJson(res, 200, { ok: true, place, result: place });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/places/reverse", authOptional, ptyGv2ReverseHandler);
app.get("/api/geocode/reverse", authOptional, ptyGv2ReverseHandler);
app.get("/api/google/geocode/reverse", authOptional, ptyGv2ReverseHandler);

/* PTY GOOGLE PLACES + GEOCODING V1 */
function ptyGoogleKey() {
  return String(process.env.GOOGLE_MAPS_API_KEY || process.env.GOOGLE_API_KEY || "").trim();
}

function ptyCompactPlaceLabel(value = "", fallback = "") {
  const text = String(value || "").trim();
  if (!text) return String(fallback || "").trim();
  return text
    .replace(/,\s*Panamá\s*,\s*Panamá$/i, ", Panamá")
    .replace(/,\s*Panama\s*,\s*Panama$/i, ", Panamá")
    .replace(/\s+/g, " ")
    .trim();
}

function ptyAsNum(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function ptyGooglePlaceFromDetails(result = {}, fallbackName = "") {
  const loc = result?.geometry?.location || {};
  const lat = ptyAsNum(loc.lat);
  const lng = ptyAsNum(loc.lng);
  const name = ptyCompactPlaceLabel(result.name || fallbackName || result.formatted_address || "Destino");
  const address = ptyCompactPlaceLabel(result.formatted_address || name, name);
  return {
    id: result.place_id || `google_${lat}_${lng}`,
    placeId: result.place_id || "",
    googlePlaceId: result.place_id || "",
    source: "google_places",
    title: name,
    short: name,
    name,
    placeName: name,
    exactName: name,
    destinationName: name,
    destinationLabel: name,
    destinationTitle: name,
    lat,
    lng,
    address,
    fullAddress: address,
    display_name: address,
    searchSubtitle: address,
    subtitle: address,
    types: result.types || [],
    businessStatus: result.business_status || "",
  };
}

async function ptyGoogleJson(url) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 8500);
  try {
    const r = await fetch(url, {
      signal: controller.signal,
      headers: { Accept: "application/json" },
    });
    const json = await r.json().catch(() => ({}));
    return { ok: r.ok, status: r.status, json };
  } finally {
    clearTimeout(timeout);
  }
}

async function ptyGooglePlaceDetails(placeId = "") {
  const key = ptyGoogleKey();
  if (!key) throw new Error("GOOGLE_MAPS_API_KEY no configurada en Render");
  if (!placeId) throw new Error("placeId requerido");

  const url = new URL("https://maps.googleapis.com/maps/api/place/details/json");
  url.searchParams.set("place_id", placeId);
  url.searchParams.set("language", "es");
  url.searchParams.set("fields", "place_id,name,formatted_address,geometry,types,business_status");
  url.searchParams.set("key", key);

  const { json } = await ptyGoogleJson(url);
  if (json.status && !["OK"].includes(json.status)) {
    throw new Error(json.error_message || `Google details: ${json.status}`);
  }
  return ptyGooglePlaceFromDetails(json.result || {});
}

async function ptyGoogleAutocompleteWithDetails({ q, lat, lng, limit }) {
  const key = ptyGoogleKey();
  if (!key) throw new Error("GOOGLE_MAPS_API_KEY no configurada en Render");
  const cleanQ = String(q || "").trim();
  if (!cleanQ) return [];

  const max = Math.max(1, Math.min(12, Number(limit || 8)));
  const url = new URL("https://maps.googleapis.com/maps/api/place/autocomplete/json");
  url.searchParams.set("input", cleanQ);
  url.searchParams.set("language", "es");
  url.searchParams.set("components", "country:pa");
  url.searchParams.set("types", "establishment|geocode");
  url.searchParams.set("key", key);
  const latN = ptyAsNum(lat);
  const lngN = ptyAsNum(lng);
  if (latN !== null && lngN !== null) {
    url.searchParams.set("location", `${latN},${lngN}`);
    url.searchParams.set("radius", "60000");
  }

  const { json } = await ptyGoogleJson(url);
  if (json.status && !["OK", "ZERO_RESULTS"].includes(json.status)) {
    throw new Error(json.error_message || `Google autocomplete: ${json.status}`);
  }

  const predictions = Array.isArray(json.predictions) ? json.predictions.slice(0, max) : [];
  const places = [];
  for (const p of predictions) {
    try {
      const details = await ptyGooglePlaceDetails(p.place_id);
      const name = ptyCompactPlaceLabel(details.name || p.structured_formatting?.main_text || p.description || cleanQ, cleanQ);
      places.push({
        ...details,
        title: name,
        short: name,
        name,
        placeName: name,
        exactName: name,
        predictionDescription: p.description || "",
        searchSubtitle: details.fullAddress || p.structured_formatting?.secondary_text || p.description || "",
      });
    } catch {}
  }

  return places.filter((p) => Number.isFinite(Number(p.lat)) && Number.isFinite(Number(p.lng)));
}

async function ptyGoogleTextSearch({ q, lat, lng, limit }) {
  const key = ptyGoogleKey();
  if (!key) throw new Error("GOOGLE_MAPS_API_KEY no configurada en Render");
  const cleanQ = String(q || "").trim();
  if (!cleanQ) return [];

  const max = Math.max(1, Math.min(12, Number(limit || 8)));
  const url = new URL("https://maps.googleapis.com/maps/api/place/textsearch/json");
  url.searchParams.set("query", cleanQ.toLowerCase().includes("panama") || cleanQ.toLowerCase().includes("panamá") ? cleanQ : `${cleanQ}, Panamá`);
  url.searchParams.set("language", "es");
  url.searchParams.set("region", "pa");
  url.searchParams.set("key", key);
  const latN = ptyAsNum(lat);
  const lngN = ptyAsNum(lng);
  if (latN !== null && lngN !== null) {
    url.searchParams.set("location", `${latN},${lngN}`);
    url.searchParams.set("radius", "60000");
  }

  const { json } = await ptyGoogleJson(url);
  if (json.status && !["OK", "ZERO_RESULTS"].includes(json.status)) {
    throw new Error(json.error_message || `Google text search: ${json.status}`);
  }

  return (Array.isArray(json.results) ? json.results : [])
    .slice(0, max)
    .map((r) => ptyGooglePlaceFromDetails(r, cleanQ))
    .filter((p) => Number.isFinite(Number(p.lat)) && Number.isFinite(Number(p.lng)));
}

async function ptyGoogleReverse({ lat, lng }) {
  const key = ptyGoogleKey();
  if (!key) throw new Error("GOOGLE_MAPS_API_KEY no configurada en Render");
  const latN = ptyAsNum(lat);
  const lngN = ptyAsNum(lng);
  if (latN === null || lngN === null) throw new Error("lat/lng requeridos");

  const url = new URL("https://maps.googleapis.com/maps/api/geocode/json");
  url.searchParams.set("latlng", `${latN},${lngN}`);
  url.searchParams.set("language", "es");
  url.searchParams.set("region", "pa");
  url.searchParams.set("key", key);

  const { json } = await ptyGoogleJson(url);
  if (json.status && !["OK", "ZERO_RESULTS"].includes(json.status)) {
    throw new Error(json.error_message || `Google geocode: ${json.status}`);
  }

  const results = Array.isArray(json.results) ? json.results : [];
  const best =
    results.find((r) => (r.types || []).includes("premise")) ||
    results.find((r) => (r.types || []).includes("establishment")) ||
    results.find((r) => (r.types || []).includes("street_address")) ||
    results.find((r) => (r.types || []).includes("route")) ||
    results[0] ||
    {};

  const address = ptyCompactPlaceLabel(best.formatted_address, `${latN.toFixed(5)}, ${lngN.toFixed(5)}`);
  const nameComponent =
    (best.address_components || []).find((c) => (c.types || []).includes("premise")) ||
    (best.address_components || []).find((c) => (c.types || []).includes("establishment")) ||
    (best.address_components || [])[0] ||
    {};
  const name = ptyCompactPlaceLabel(nameComponent.long_name || address, address);

  const place = {
    id: best.place_id || `reverse_${latN}_${lngN}`,
    placeId: best.place_id || "",
    googlePlaceId: best.place_id || "",
    source: "google_geocode",
    title: name,
    short: name,
    name,
    placeName: name,
    exactName: name,
    destinationName: name,
    destinationLabel: name,
    destinationTitle: name,
    lat: latN,
    lng: lngN,
    address,
    fullAddress: address,
    display_name: address,
    searchSubtitle: address,
    subtitle: address,
    types: best.types || [],
  };

  return {
    label: name,
    title: name,
    name,
    address,
    display_name: address,
    place,
    results: results.slice(0, 5),
  };
}

async function ptyPlacesSearchHandler(req, res) {
  try {
    const q = asText(req.query.q || req.query.input || req.query.query || "");
    const lat = req.query.lat;
    const lng = req.query.lng;
    const limit = req.query.limit || 8;
    if (!q) return safeJson(res, 200, { ok: true, places: [], results: [] });

    let places = [];
    try {
      places = await ptyGoogleAutocompleteWithDetails({ q, lat, lng, limit });
    } catch (e) {
      console.warn("[GOOGLE_AUTOCOMPLETE_WARN]", e?.message || e);
    }

    if (!places.length) {
      places = await ptyGoogleTextSearch({ q, lat, lng, limit });
    }

    const seen = new Set();
    const clean = places.filter((p) => {
      const key = String(p.placeId || `${p.title}_${Number(p.lat).toFixed(5)}_${Number(p.lng).toFixed(5)}`).toLowerCase();
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });

    return safeJson(res, 200, { ok: true, provider: "google", places: clean, results: clean });
  } catch (e) {
    console.error("[GOOGLE_PLACES_SEARCH_ERROR]", e);
    return safeJson(res, 500, { ok: false, message: String(e?.message || e), places: [], results: [] });
  }
}

app.get("/api/places/search", authOptional, ptyPlacesSearchHandler);
app.get("/api/places/autocomplete", authOptional, ptyPlacesSearchHandler);
app.get("/api/google/places/autocomplete", authOptional, ptyPlacesSearchHandler);

app.get("/api/places/details", authOptional, async (req, res) => {
  try {
    const placeId = asText(req.query.placeId || req.query.place_id || "");
    const place = await ptyGooglePlaceDetails(placeId);
    return safeJson(res, 200, { ok: true, place, result: place });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/google/places/details", authOptional, async (req, res) => {
  try {
    const placeId = asText(req.query.placeId || req.query.place_id || "");
    const place = await ptyGooglePlaceDetails(placeId);
    return safeJson(res, 200, { ok: true, place, result: place });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

async function ptyReverseHandler(req, res) {
  try {
    const out = await ptyGoogleReverse({ lat: req.query.lat, lng: req.query.lng });
    return safeJson(res, 200, { ok: true, ...out });
  } catch (e) {
    console.error("[GOOGLE_REVERSE_ERROR]", e);
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
}

app.get("/api/places/reverse", authOptional, ptyReverseHandler);
app.get("/api/geocode/reverse", authOptional, ptyReverseHandler);
app.get("/api/google/geocode/reverse", authOptional, ptyReverseHandler);

/* =========================================================
   Rides
========================================================= */
app.post("/api/rides", authRequired, async (req, res) => {
  try {
    await expireOldRides();

    const hasActive = await db(
      `SELECT id, status FROM ride_rides
       WHERE rider_id=$1 AND status IN ('requested','searching','assigned','in_progress')
       LIMIT 1`,
      [req.user.id]
    );

    if (hasActive.rows.length) {
      return safeJson(res, 409, {
        ok: false,
        message: "Ya tienes una carrera activa. Cancélala o finalízala antes de solicitar otra.",
        activeRideId: hasActive.rows[0].id,
      });
    }

    const pickup = normalizePoint(req.body.pickup || req.body.origin || req.body.from || {});
    const destination = normalizePoint(req.body.destination || req.body.destino || req.body.to || {});
    const distanceKm = Math.max(0, asNum(req.body.distanceKm ?? req.body.routeDistanceKm, 0));
    const durationMin = Math.max(0, asNum(req.body.durationMin, 0));
    const fareRaw = asNum(req.body.fare ?? req.body.price ?? req.body.total, 2);
    const fare = Math.max(2.0, Number(fareRaw || 2));
    const paymentMethod = asText(req.body.paymentMethod || req.body.payment || "cash");
    const route = req.body.route || {
      polyline: req.body.polyline || "",
      coordinates: req.body.coordinates || [],
      distanceKm,
      durationMin,
    };

    if (pickup.lat == null || pickup.lng == null || destination.lat == null || destination.lng == null) {
      return safeJson(res, 400, { ok: false, message: "pickup y destination con lat/lng son requeridos" });
    }

    const expiresMinutes = Math.max(1, Number(RIDE_EXPIRE_MINUTES || 10));

    const r = await db(
      `INSERT INTO ride_rides(
        rider_id, status, pickup, destination, route, fare, distance_km, duration_min,
        payment_method, rider_snapshot, expires_at
       )
       VALUES(
        $1,'requested',$2::jsonb,$3::jsonb,$4::jsonb,$5,$6,$7,$8,$9::jsonb,
        NOW() + ($10::text || ' minutes')::interval
       )
       RETURNING *`,
      [
        req.user.id,
        JSON.stringify(pickup),
        JSON.stringify(destination),
        JSON.stringify(route || {}),
        fare,
        distanceKm,
        durationMin,
        paymentMethod,
        JSON.stringify(getSnapshotUser(req.user)),
        String(expiresMinutes),
      ]
    );

    const ride = r.rows[0];
    await db(
      `INSERT INTO ride_events(ride_id, user_id, type, payload)
       VALUES($1,$2,'ride_requested',$3::jsonb)`,
      [ride.id, req.user.id, JSON.stringify({ fare, distanceKm, paymentMethod })]
    );

    emitRide(ride, "ride:new");
    io.to("drivers").emit("ride:available", normalizeRide(ride));

    return safeJson(res, 201, { ok: true, ride: normalizeRide(ride) });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/rides", authRequired, async (req, res) => {
  try {
    await expireOldRides();

    const status = asText(req.query.status || "");
    const role = asText(req.query.role || req.user.role || "");
    let q = `SELECT * FROM ride_rides`;
    let params = [];
    let where = [];

    if (status === "open" || status === "pending" || role === "driver") {
      where.push(`status IN ('requested','searching')`);
      where.push(`expires_at > NOW()`);
    } else if (role === "rider") {
      params.push(req.user.id);
      where.push(`rider_id=$${params.length}`);
    } else if (role === "admin" || req.user.role === "admin") {
      // admin ve todo
    } else {
      params.push(req.user.id);
      where.push(`(rider_id=$${params.length} OR driver_id=$${params.length})`);
    }

    if (where.length) q += ` WHERE ${where.join(" AND ")}`;
    q += ` ORDER BY created_at DESC LIMIT 200`;

    const r = await db(q, params);
    return safeJson(res, 200, { ok: true, rides: r.rows.map(normalizeRide) });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});





/* PTY BACKEND CORE SERVICES V7 - NO ROUTE TOUCH */
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
    const r = await db(`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1`, [riderId]).catch(() => ({ rows: [] }));
    if (r.rows?.[0]) ride.rider = { ...(ride.rider || {}), ...ptyV7PublicUser(r.rows[0]) };
  }
  if (driverId) {
    const d = await db(`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1`, [driverId]).catch(() => ({ rows: [] }));
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
  await db(`
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
  `);
  await db(`CREATE INDEX IF NOT EXISTS idx_pty_v7_chat_ride ON ride_chat_messages(ride_id, created_at);`);

  await db(`
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
  `);
  await db(`CREATE INDEX IF NOT EXISTS idx_pty_v7_reports ON ride_support_reports(status, created_at DESC);`);

  await db(`
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
  `);
  await db(`CREATE INDEX IF NOT EXISTS idx_pty_v7_sos ON ride_sos_alerts(status, created_at DESC);`);

  await db(`
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
  `);
}

async function ptyV7GetUserById(id = "") {
  if (!id) return null;
  const r = await db(`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1`, [String(id)]).catch(() => ({ rows: [] }));
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
      `SELECT * FROM ride_rides
       WHERE (rider_id::text=$1::text OR driver_id::text=$1::text)
         AND LOWER(COALESCE(status,'')) = ANY($2::text[])
       ORDER BY updated_at DESC NULLS LAST, created_at DESC
       LIMIT 1`,
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
    const r = await db(`SELECT * FROM ride_rides WHERE id::text=$1::text LIMIT 1`, [rideId]);
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
    const r = await db(`SELECT * FROM ride_chat_messages WHERE ride_id::text=$1::text ORDER BY created_at ASC LIMIT 500`, [rideId]);
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
      `INSERT INTO ride_chat_messages(ride_id,sender_id,sender_role,sender_name,text,meta)
       VALUES($1,$2,$3,$4,$5,$6::jsonb) RETURNING *`,
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
      io.to(`ride:${rideId}`).emit("ride.chat.message", message);
      io.emit("ride.chat.message", message);
      io.to("admins").emit("admin.chat.message", message);
    } catch {}
    return ptyV7SafeJson(res, 201, { ok: true, message, chatMessage: message });
  } catch (e) {
    return ptyV7SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/rides/:id/messages", ptyV7AuthOptional, async (req, res) => {
  req.url = `/api/rides/${req.params.id}/chat`;
  return app._router.handle(req, res);
});
app.post("/api/rides/:id/messages", ptyV7AuthOptional, async (req, res) => {
  req.url = `/api/rides/${req.params.id}/chat`;
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
      `INSERT INTO ride_support_reports(user_id,user_email,user_name,user_phone,role,ride_id,type,title,detail,status,location,meta)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'open',$10::jsonb,$11::jsonb) RETURNING *`,
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
    const r = await db(`SELECT * FROM ride_support_reports ORDER BY created_at DESC LIMIT 500`);
    return ptyV7SafeJson(res, 200, { ok: true, reports: r.rows });
  } catch (e) {
    return ptyV7SafeJson(res, 500, { ok: false, message: String(e?.message || e), reports: [] });
  }
});

app.patch("/api/admin/support/reports/:id/status", ptyV7AuthRequired, ptyV7RequireAdmin, async (req, res) => {
  try {
    await ptyV7EnsureTables();
    const r = await db(`UPDATE ride_support_reports SET status=$2, updated_at=NOW() WHERE id::text=$1::text RETURNING *`, [req.params.id, req.body.status || "closed"]);
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
      `INSERT INTO ride_map_reports(reporter_id,reporter_role,reporter_name,type,title,detail,icon,color,lat,lng,meta)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb) RETURNING *`,
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
    const r = await db(`SELECT * FROM ride_map_reports WHERE status='active' ORDER BY created_at DESC LIMIT 500`);
    return ptyV7SafeJson(res, 200, { ok: true, reports: r.rows, items: r.rows });
  } catch (e) {
    return ptyV7SafeJson(res, 500, { ok: false, message: String(e?.message || e), reports: [] });
  }
});

app.patch("/api/map-reports/:id/confirm", ptyV7AuthOptional, async (req, res) => {
  try {
    await ptyV7EnsureTables();
    const r = await db(`UPDATE ride_map_reports SET confirmations=COALESCE(confirmations,0)+1, updated_at=NOW() WHERE id::text=$1::text RETURNING *`, [req.params.id]);
    return ptyV7SafeJson(res, 200, { ok: true, report: r.rows[0] || null });
  } catch (e) {
    return ptyV7SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.patch("/api/map-reports/:id/clear", ptyV7AuthOptional, async (req, res) => {
  try {
    await ptyV7EnsureTables();
    const r = await db(`UPDATE ride_map_reports SET status='cleared', updated_at=NOW() WHERE id::text=$1::text RETURNING *`, [req.params.id]);
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
      const r = await db(`SELECT * FROM ride_rides WHERE id::text=$1::text LIMIT 1`, [String(req.body.rideId)]).catch(() => ({ rows: [] }));
      if (r.rows?.[0]) ridePayload = await ptyV7EnrichRide(r.rows[0]);
    }
    const row = await db(
      `INSERT INTO ride_sos_alerts(user_id,user_email,user_name,user_phone,role,ride_id,message,status,location,trusted_contact,ride)
       VALUES($1,$2,$3,$4,$5,$6,$7,'open',$8::jsonb,$9::jsonb,$10::jsonb) RETURNING *`,
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
    const r = await db(`SELECT * FROM ride_sos_alerts ORDER BY created_at DESC LIMIT 100`);
    return ptyV7SafeJson(res, 200, { ok: true, alerts: r.rows, sosAlerts: r.rows });
  } catch (e) {
    return ptyV7SafeJson(res, 500, { ok: false, message: String(e?.message || e), alerts: [] });
  }
});

app.patch("/api/admin/security/sos/:id/status", ptyV7AuthRequired, ptyV7RequireAdmin, async (req, res) => {
  try {
    await ptyV7EnsureTables();
    const r = await db(`UPDATE ride_sos_alerts SET status=$2, updated_at=NOW() WHERE id::text=$1::text RETURNING *`, [req.params.id, req.body.status || "closed"]);
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
      `UPDATE ride_users
       SET driver_docs=COALESCE(driver_docs,'{}'::jsonb) || $2::jsonb,
           updated_at=NOW()
       WHERE id::text=$1::text`,
      [userId, JSON.stringify({ currentLocation: location, lastLocationAt: item.at })]
    ).catch(() => null);

    try { io.to("admins").emit("live.location", item); io.emit(`${role}.location.updated`, item); } catch {}
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
    const usersR = await db(`SELECT * FROM ride_users ORDER BY updated_at DESC NULLS LAST, created_at DESC LIMIT 1000`).catch(() => ({ rows: [] }));
    const ridesR = await db(
      `SELECT * FROM ride_rides
       WHERE LOWER(COALESCE(status,'')) = ANY($1::text[])
       ORDER BY updated_at DESC NULLS LAST, created_at DESC
       LIMIT 300`,
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
    const r = await db(`
      SELECT ride_id,
             MAX(created_at) AS updated_at,
             COUNT(*) AS count,
             (ARRAY_AGG(text ORDER BY created_at DESC))[1] AS last_message
      FROM ride_chat_messages
      GROUP BY ride_id
      ORDER BY updated_at DESC
      LIMIT 200
    `);
    const chats = r.rows.map((row) => ({
      id: row.ride_id,
      rideId: row.ride_id,
      title: `Viaje ${String(row.ride_id).slice(-6)}`,
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
    const r = await db(`SELECT * FROM ride_support_reports ORDER BY created_at DESC LIMIT 200`);
    return ptyV7SafeJson(res, 200, { ok: true, threads: r.rows, reports: r.rows });
  } catch (e) {
    return ptyV7SafeJson(res, 500, { ok: false, message: String(e?.message || e), threads: [] });
  }
});

/* PTY DRIVER ACCEPT + ACTIVE STATE V6 */
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
      `SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1`,
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
      `SELECT * FROM ride_rides
       WHERE (rider_id::text=$1::text OR driver_id::text=$1::text)
         AND LOWER(COALESCE(status,'')) = ANY($2::text[])
       ORDER BY updated_at DESC NULLS LAST, created_at DESC
       LIMIT 1`,
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
      `SELECT * FROM ride_rides WHERE id::text=$1::text LIMIT 1`,
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
        `UPDATE ride_rides
         SET driver_id=$2,
             status='accepted',
             updated_at=NOW()
         WHERE id::text=$1::text
         RETURNING *`,
        [rideId, driverId]
      );
    } catch {
      updated = await db(
        `UPDATE ride_rides
         SET driver_id=$2,
             status='accepted'
         WHERE id::text=$1::text
         RETURNING *`,
        [rideId, driverId]
      );
    }

    const ride = await ptyV6EnrichRide(updated.rows[0], { ...payloadDriver, id: driverId, driverId });

    try {
      io.to("admins").emit("ride.accepted", { ride, rideId, driverId });
      io.to(`ride:${rideId}`).emit("ride.accepted", { ride, rideId, driverId });
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
        `UPDATE ride_rides
         SET status='in_progress',
             updated_at=NOW()
         WHERE id::text=$1::text
           AND ($2::text='' OR driver_id::text=$2::text)
         RETURNING *`,
        [rideId, driverId]
      );
    } catch {
      updated = await db(
        `UPDATE ride_rides
         SET status='in_progress'
         WHERE id::text=$1::text
         RETURNING *`,
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

/* PTY BACKEND ROUTES CHAT LIVE SUPPORT V5 */
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
  await db(`
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
  `);
  await db(`
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
  `);
  await db(`
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
  `);
  await db(`CREATE INDEX IF NOT EXISTS idx_v5_chat_ride ON ride_chat_messages(ride_id, created_at);`);
  await db(`CREATE INDEX IF NOT EXISTS idx_v5_reports_status ON ride_support_reports(status, created_at DESC);`);
  await db(`CREATE INDEX IF NOT EXISTS idx_v5_sos_status ON ride_sos_alerts(status, created_at DESC);`);
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
    const r = await db(`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1`, [String(riderId)]).catch(() => ({ rows: [] }));
    rider = r.rows?.[0] ? ptyV5PublicUser(r.rows[0]) : null;
  }
  if (driverId) {
    const r = await db(`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1`, [String(driverId)]).catch(() => ({ rows: [] }));
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
        `UPDATE ride_users
         SET driver_docs=COALESCE(driver_docs,'{}'::jsonb) || $2::jsonb,
             updated_at=NOW()
         WHERE id::text=$1::text`,
        [String(userId), JSON.stringify({ currentLocation: location, lastLocationAt: item.at })]
      );
    } catch {}

    try { io.to("admins").emit("live.location", item); io.emit(`${role}.location.updated`, item); } catch {}
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
    const usersR = await db(`SELECT * FROM ride_users ORDER BY updated_at DESC LIMIT 1000`).catch(() => ({ rows: [] }));
    const ridesR = await db(
      `SELECT * FROM ride_rides
       WHERE LOWER(COALESCE(status,'')) IN ('requested','searching','assigned','accepted','arrived','in_progress')
       ORDER BY created_at DESC LIMIT 250`
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
    const r = await db(`SELECT * FROM ride_rides WHERE id::text=$1::text LIMIT 1`, [String(req.params.id)]);
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
    const r = await db(`SELECT * FROM ride_chat_messages WHERE ride_id::text=$1::text ORDER BY created_at ASC LIMIT 500`, [rideId]);
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
      `INSERT INTO ride_chat_messages(ride_id, sender_id, sender_role, sender_name, text, meta)
       VALUES($1,$2,$3,$4,$5,$6::jsonb) RETURNING *`,
      [rideId, senderId, senderRole, senderName, text, JSON.stringify(req.body.meta || {})]
    );
    const message = { id: r.rows[0].id, rideId, senderId, senderRole, senderName, author: senderName, text, at: r.rows[0].created_at };
    try { io.to(`ride:${rideId}`).emit("ride.chat.message", message); io.emit("ride.chat.message", message); } catch {}
    return safeJson(res, 201, { ok: true, message });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});
app.get("/api/rides/:id/messages", authOptional, (req, res) => req.app._router.handle(Object.assign(req, { url: `/api/rides/${req.params.id}/chat` }), res));

async function ptyV5ReportHandler(req, res) {
  try {
    await ptyV5EnsureSupportTables();
    const user = req.user || {};
    const location = ptyV5Point(req.body.location || req.body.currentLocation || req.body) || {};
    const type = ptyV5Text(req.body.type || req.body.reportType || "incident");
    const title = ptyV5Text(req.body.title || (type === "lost_item" ? "Objeto perdido" : "Reporte de incidente"));
    const detail = ptyV5Text(req.body.detail || req.body.description || req.body.message || "");
    const r = await db(
      `INSERT INTO ride_support_reports(user_id,user_email,user_name,role,ride_id,type,title,detail,status,location,meta)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,'open',$9::jsonb,$10::jsonb) RETURNING *`,
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
    const r = await db(`SELECT * FROM ride_support_reports ORDER BY created_at DESC LIMIT 500`);
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
      `INSERT INTO ride_sos_alerts(user_id,user_email,user_name,user_phone,role,ride_id,message,status,location,trusted_contact,ride)
       VALUES($1,$2,$3,$4,$5,$6,$7,'open',$8::jsonb,$9::jsonb,$10::jsonb) RETURNING *`,
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
    const r = await db(`SELECT * FROM ride_sos_alerts ORDER BY created_at DESC LIMIT 100`);
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
      `UPDATE ride_users
       SET trusted_contact=$2::jsonb,
           updated_at=NOW()
       WHERE id::text=$1::text RETURNING *`,
      [String(userId), JSON.stringify({ name, phone, updatedAt: new Date().toISOString() })]
    ).catch(() => ({ rows: [] }));
    return safeJson(res, 200, { ok: true, trustedContact: { name, phone }, user: r.rows?.[0] ? ptyV5PublicUser(r.rows[0]) : null });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

/* PTY DRIVER PENDING FEED FIX V1 */
async function ptyDriverPendingRows() {
  try { if (typeof expireOldRides === "function") await expireOldRides(); } catch {}

  const r = await db(
    `SELECT
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
      LIMIT 100`,
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

app.get("/api/rides/active", authRequired, async (req, res) => {
  try {
    await expireOldRides();

    const r = await db(
      `SELECT * FROM ride_rides
       WHERE (rider_id=$1 OR driver_id=$1)
         AND status IN ('requested','searching','assigned','in_progress')
       ORDER BY created_at DESC
       LIMIT 1`,
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
      `SELECT * FROM ride_rides
       WHERE (rider_id=$1 OR driver_id=$1)
         AND status IN ('requested','searching','assigned','in_progress')
       ORDER BY created_at DESC
       LIMIT 1`,
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
      `SELECT * FROM ride_rides
       WHERE (rider_id=$1 OR driver_id=$1)
         AND status IN ('requested','searching','assigned','in_progress')
       ORDER BY created_at DESC
       LIMIT 1`,
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

app.get("/api/rides/:id", authRequired, async (req, res) => {
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
});

async function acceptRideHandler(req, res) {
  try {
    await expireOldRides();

    const rideId = asText(req.params.id || req.params.rideId);
    const driverId = req.user?.id || asText(req.body.driverId);
    const driverSnapshot = req.user
      ? getSnapshotUser(req.user)
      : {
          id: driverId,
          name: asText(req.body.driverName || "Driver"),
          email: asText(req.body.driverEmail),
          phone: cleanPhone(req.body.driverPhone),
          role: "driver",
        };

    if (!rideId || !driverId) {
      return safeJson(res, 400, { ok: false, message: "rideId y driverId requeridos" });
    }

    const r = await db(
      `UPDATE ride_rides
       SET status='assigned',
           driver_id=$1,
           driver_snapshot=$2::jsonb,
           accepted_at=NOW(),
           updated_at=NOW()
       WHERE id=$3
         AND status IN ('requested','searching')
         AND expires_at > NOW()
       RETURNING *`,
      [driverId, JSON.stringify(driverSnapshot), rideId]
    );

    if (!r.rows.length) {
      const current = await getRideById(rideId);
      return safeJson(res, 409, {
        ok: false,
        message: current
          ? `La carrera ya no está disponible. Estado actual: ${current.status}`
          : "La carrera no existe",
        ride: current ? normalizeRide(current) : null,
      });
    }

    const ride = r.rows[0];
    await db(
      `INSERT INTO ride_events(ride_id, user_id, type, payload)
       VALUES($1,$2,'ride_accepted',$3::jsonb)`,
      [ride.id, driverId, JSON.stringify({ driver: driverSnapshot })]
    );

    emitRide(ride, "ride:accepted");
    return safeJson(res, 200, { ok: true, ride: normalizeRide(ride) });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
}

app.patch("/api/rides/:id/accept", authOptional, acceptRideHandler);
app.patch("/api/carrera-lite/:id/accept", authOptional, acceptRideHandler);
app.patch("/api/carreras/:id/accept", authOptional, acceptRideHandler);

async function rideStatusPatch(req, res, nextStatus, eventName) {
  try {
    const rideId = asText(req.params.id || req.params.rideId);
    const ride = await getRideById(rideId);
    if (!ride) return safeJson(res, 404, { ok: false, message: "Carrera no encontrada" });

    const fields =
      nextStatus === "in_progress"
        ? `status='in_progress', started_at=NOW(), updated_at=NOW()`
        : nextStatus === "completed"
          ? `status='completed', completed_at=NOW(), updated_at=NOW()`
          : `status=$2, updated_at=NOW()`;

    const r = nextStatus === "in_progress" || nextStatus === "completed"
      ? await db(`UPDATE ride_rides SET ${fields} WHERE id::text=$1::text RETURNING *`, [rideId])
      : await db(`UPDATE ride_rides SET ${fields} WHERE id::text=$1::text RETURNING *`, [rideId, nextStatus]);

    emitRide(r.rows[0], eventName);
    return safeJson(res, 200, { ok: true, ride: normalizeRide(r.rows[0]) });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
}

app.patch("/api/rides/:id/start", authRequired, (req, res) => rideStatusPatch(req, res, "in_progress", "ride:started"));
app.patch("/api/rides/:id/complete", authRequired, (req, res) => rideStatusPatch(req, res, "completed", "ride:completed"));

app.patch("/api/rides/:id/cancel", authRequired, async (req, res) => {
  try {
    const reason = asText(req.body.reason || "cancelled_by_user");
    const r = await db(
      `UPDATE ride_rides
       SET status='cancelled', cancel_reason=$2, cancelled_at=NOW(), updated_at=NOW()
       WHERE id::text=$1::text
         AND status NOT IN ('completed','cancelled','expired','auto_cancelled')
       RETURNING *`,
      [req.params.id, reason]
    );

    if (!r.rows.length) {
      const current = await getRideById(req.params.id);
      return safeJson(res, 409, { ok: false, message: "No se pudo cancelar", ride: current ? normalizeRide(current) : null });
    }

    emitRide(r.rows[0], "ride:cancelled");
    return safeJson(res, 200, { ok: true, ride: normalizeRide(r.rows[0]) });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

/* Aliases */
app.post("/api/carreras", authRequired, (req, res) => app._router.handle({ ...req, url: "/api/rides" }, res));
app.get("/api/carreras", authRequired, (req, res) => app._router.handle({ ...req, url: "/api/rides" }, res));

/* =========================================================
   Locations
========================================================= */
app.post("/api/locations", authRequired, async (req, res) => {
  try {
    const role = asText(req.body.role || req.user.role || "rider");
    const lat = asNum(req.body.lat ?? req.body.latitude);
    const lng = asNum(req.body.lng ?? req.body.longitude);
    const heading = asNum(req.body.heading);
    const speed = asNum(req.body.speed);

    if (lat === null || lng === null) return safeJson(res, 400, { ok: false, message: "lat/lng requeridos" });

    const r = await db(
      `INSERT INTO ride_locations(user_id, role, lat, lng, heading, speed, updated_at)
       VALUES($1,$2,$3,$4,$5,$6,NOW())
       ON CONFLICT(user_id, role) DO UPDATE SET
         lat=EXCLUDED.lat, lng=EXCLUDED.lng,
         heading=EXCLUDED.heading, speed=EXCLUDED.speed,
         updated_at=NOW()
       RETURNING *`,
      [req.user.id, role, lat, lng, heading, speed]
    );

    const payload = { userId: req.user.id, role, lat, lng, heading, speed, updatedAt: r.rows[0].updated_at };
    io.to(role === "driver" ? "riders" : "drivers").emit("location:update", payload);
    io.to("admins").emit("location:update", payload);

    return safeJson(res, 200, { ok: true, location: payload });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.post("/api/location", authRequired, (req, res) => app._router.handle({ ...req, url: "/api/locations" }, res));

/* =========================================================
   Profile compatibility
========================================================= */
app.patch("/api/users/me/profile", authOptional, async (req, res) => {
  try {
    const userId = req.user?.id || asText(req.body.userId);
    if (!userId) return safeJson(res, 401, { ok: false, message: "Usuario requerido" });

    const phone = req.body.phone !== undefined ? cleanPhone(req.body.phone) : undefined;
    const markerIcon = req.body.markerIcon !== undefined
      ? asText(req.body.markerIcon)
      : req.body.riderMarkerIcon !== undefined
        ? asText(req.body.riderMarkerIcon)
        : req.body.userMarkerIcon !== undefined
          ? asText(req.body.userMarkerIcon)
          : undefined;
    const trustedContact = req.body.trustedContact !== undefined ? req.body.trustedContact : undefined;
    const name = req.body.name !== undefined ? asText(req.body.name) : undefined;

    const r = await db(
      `UPDATE ride_users SET
        name=COALESCE($2, name),
        phone=COALESCE($3, phone),
        marker_icon=COALESCE($4, marker_icon),
        trusted_contact=COALESCE($5::jsonb, trusted_contact),
        updated_at=NOW()
       WHERE id::text=$1::text
       RETURNING *`,
      [
        userId,
        name ?? null,
        phone ?? null,
        markerIcon ?? null,
        trustedContact !== undefined ? JSON.stringify(trustedContact || {}) : null,
      ]
    );

    if (!r.rows.length) return safeJson(res, 404, { ok: false, message: "Usuario no encontrado" });
    return safeJson(res, 200, { ok: true, user: publicUser(r.rows[0]) });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

/* =========================================================
   Documents upload / driver verification compatibility
========================================================= */
app.post("/api/documents/upload", authOptional, documentUpload.single("file"), async (req, res) => {
  try {
    const userId = req.user?.id || asText(req.body.userId);
    const type = asText(req.body.type || "documento");
    const role = asText(req.body.role || req.user?.role || "driver").toLowerCase();
    if (!userId) return safeJson(res, 400, { ok: false, message: "userId requerido" });
    if (!type) return safeJson(res, 400, { ok: false, message: "type requerido" });
    if (!req.file) return safeJson(res, 400, { ok: false, message: "file requerido" });

    const mime = req.file.mimetype || "image/jpeg";
    const dataUrl = `data:${mime};base64,${req.file.buffer.toString("base64")}`;

    const meta = { ...req.body };
    delete meta.userId;
    delete meta.type;
    delete meta.role;

    const inserted = await db(
      `INSERT INTO ride_documents(user_id, role, type, status, url, filename, mime_type, size_bytes, meta)
       VALUES($1,$2,$3,'pending',$4,$5,$6,$7,$8::jsonb)
       RETURNING *`,
      [
        userId,
        role,
        type,
        dataUrl,
        req.file.originalname || `${type}.jpg`,
        mime,
        Number(req.file.size || 0),
        JSON.stringify(meta || {}),
      ]
    );

    const document = documentToPublic(inserted.rows[0]);

    await db(
      `UPDATE ride_users
       SET document_status='pending',
           driver_docs=COALESCE(driver_docs,'{}'::jsonb) || $2::jsonb,
           updated_at=NOW()
       WHERE id::text=$1::text`,
      [
        userId,
        JSON.stringify({
          [type]: dataUrl,
          [`${type}Status`]: "pending",
          ...meta,
        }),
      ]
    );

    const userR = await db(`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1`, [userId]);
    io.to("admins").emit("documents:pending", { document, user: publicUser(userR.rows[0]) });

    return safeJson(res, 201, { ok: true, document, user: publicUser(userR.rows[0]) });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.post("/api/driver/documents", authOptional, async (req, res) => {
  try {
    const userId = req.user?.id || asText(req.body.driverId || req.body.userId);
    const docs = req.body.documents || req.body.docs || {};
    if (!userId) return safeJson(res, 400, { ok: false, message: "driverId requerido" });

    const vehicle = {
      brand: docs.marcaVehiculo || docs.brand || docs.make || "",
      make: docs.marcaVehiculo || docs.make || docs.brand || "",
      model: docs.modeloVehiculo || docs.model || "",
      plate: docs.placa || docs.plate || "",
      color: docs.colorVehiculo || docs.color || "",
      year: docs.anioVehiculo || docs.year || "",
      serviceTier: docs.serviceTier || docs.enrollmentType || "viaje",
      vehicleType: docs.vehicleType || (docs.enrollmentType === "moto" ? "moto" : "car"),
    };

    const r = await db(
      `UPDATE ride_users
       SET document_status='pending',
           phone=COALESCE(NULLIF($2,''), phone),
           driver_docs=COALESCE(driver_docs,'{}'::jsonb) || $3::jsonb,
           updated_at=NOW()
       WHERE id::text=$1::text
       RETURNING *`,
      [
        userId,
        cleanPhone(docs.telefonoContacto || ""),
        JSON.stringify({
          ...docs,
          vehicle,
          driverDocumentsApproved: false,
          canAcceptRides: false,
        }),
      ]
    );

    const documents = await listUserDocuments(userId, "driver");
    io.to("admins").emit("documents:pending", { user: publicUser(r.rows[0]), documents });
    return safeJson(res, 200, { ok: true, user: publicUser(r.rows[0]), documents });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});


/* DOCUMENT STATUS ROUTES FOR MOBILE APP V3 */
app.get("/api/driver/documents/status", authOptional, async (req, res) => {
  try {
    const userId = req.user?.id || asText(req.query.userId || req.body?.userId);
    if (!userId) return safeJson(res, 400, { ok: false, message: "userId requerido" });

    const docsR = await db(
      `SELECT * FROM ride_documents
       WHERE user_id::text=$1::text
         AND role::text='driver'
       ORDER BY created_at DESC`,
      [String(userId)]
    );

    const documents = docsR.rows.map(documentToPublic);
    const result = areDocumentsApproved(documents, "driver");
    const userR = await db(`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1`, [String(userId)]);
    const user = buildDocumentUserPayload(userR.rows[0], "driver", result.approved);

    return safeJson(res, 200, {
      ok: true,
      approved: result.approved,
      status: result.approved ? "approved" : (documents.some((d) => d.status === "rejected") ? "rejected" : documents.length ? "pending" : "missing"),
      driverDocumentsApproved: result.approved,
      canAcceptRides: result.approved,
      missing: result.missing,
      statusMap: result.statusMap,
      documents,
      user,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/rider/documents/status", authOptional, async (req, res) => {
  try {
    const userId = req.user?.id || asText(req.query.userId || req.body?.userId);
    if (!userId) return safeJson(res, 400, { ok: false, message: "userId requerido" });

    const docsR = await db(
      `SELECT * FROM ride_documents
       WHERE user_id::text=$1::text
         AND role::text='rider'
       ORDER BY created_at DESC`,
      [String(userId)]
    );

    const documents = docsR.rows.map(documentToPublic);
    const result = areDocumentsApproved(documents, "rider");
    const status = result.approved
      ? "approved"
      : documents.some((d) => d.status === "rejected")
        ? "rejected"
        : documents.length
          ? "pending"
          : "missing";
    const userR = await db(`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1`, [String(userId)]);
    const user = buildDocumentUserPayload(userR.rows[0], "rider", result.approved, {
      riderVerified: result.approved,
      identityVerified: result.approved,
      verificationStatus: status,
    });

    return safeJson(res, 200, {
      ok: true,
      verified: result.approved,
      approved: result.approved,
      riderVerified: result.approved,
      identityVerified: result.approved,
      status,
      missing: result.missing,
      statusMap: result.statusMap,
      documents,
      user,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/documents/status", authOptional, async (req, res) => {
  try {
    const role = asText(req.query.role || req.body?.role || req.user?.role || "driver").toLowerCase() === "rider" ? "rider" : "driver";
    const userId = req.user?.id || asText(req.query.userId || req.body?.userId);
    if (!userId) return safeJson(res, 400, { ok: false, message: "userId requerido" });

    const docsR = await db(
      `SELECT * FROM ride_documents
       WHERE user_id::text=$1::text
         AND role::text=$2::text
       ORDER BY created_at DESC`,
      [String(userId), role]
    );

    const documents = docsR.rows.map(documentToPublic);
    const result = areDocumentsApproved(documents, role);
    const status = result.approved
      ? "approved"
      : documents.some((d) => d.status === "rejected")
        ? "rejected"
        : documents.length
          ? "pending"
          : "missing";
    const userR = await db(`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1`, [String(userId)]);
    const user = buildDocumentUserPayload(userR.rows[0], role, result.approved);

    return safeJson(res, 200, {
      ok: true,
      approved: result.approved,
      verified: result.approved,
      status,
      missing: result.missing,
      statusMap: result.statusMap,
      documents,
      user,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/driver/documents/approved", authOptional, async (req, res) => {
  try {
    const userId = req.user?.id || asText(req.query.userId || req.body?.userId);
    if (!userId) return safeJson(res, 400, { ok: false, message: "userId requerido" });
    const documents = await listUserDocuments(userId, "driver");
    const result = areDocumentsApproved(documents, "driver");
    const userR = await db(`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1`, [userId]);
    return safeJson(res, 200, {
      ok: true,
      approved: result.approved,
      driverDocumentsApproved: result.approved,
      canAcceptRides: result.approved,
      missing: result.missing,
      statusMap: result.statusMap,
      documents,
      user: userR.rows[0] ? publicUser(userR.rows[0]) : null,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/rider/documents/approved", authOptional, async (req, res) => {
  try {
    const userId = req.user?.id || asText(req.query.userId || req.body?.userId);
    if (!userId) return safeJson(res, 400, { ok: false, message: "userId requerido" });
    const documents = await listUserDocuments(userId, "rider");
    const result = areDocumentsApproved(documents, "rider");
    const userR = await db(`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1`, [userId]);
    return safeJson(res, 200, {
      ok: true,
      verified: result.approved,
      approved: result.approved,
      status: result.approved ? "approved" : (documents[0]?.status || "pending"),
      missing: result.missing,
      statusMap: result.statusMap,
      documents,
      user: userR.rows[0] ? publicUser(userR.rows[0]) : null,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/admin/documents", authRequired, requireAdmin, async (_req, res) => {
  try {
    const r = await db(
      `SELECT d.*, u.email AS user_email, u.name AS user_name, u.role AS user_role
       FROM ride_documents d
       LEFT JOIN ride_users u ON u.id=d.user_id
       ORDER BY d.created_at DESC
       LIMIT 1000`
    );
    const documents = r.rows.map((row) => ({
      ...documentToPublic(row),
      userEmail: row.user_email || "",
      userName: row.user_name || "",
      userRole: row.user_role || row.role || "",
      user: { id: row.user_id, email: row.user_email || "", name: row.user_name || "", role: row.user_role || "" },
    }));
    return safeJson(res, 200, { ok: true, documents });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});


/* SAFE DOC STATUS ROUTE CAST FIX V2 */

/* SAFE DOC STATUS ROUTE V3 APPROVAL NOTIFY */

/* PTY DOCS V4 FAST STATUS ROUTES */
const PTY_DOCS_V4_DRIVER_TYPES = [
  "fotoPerfilConductor",
  "selfieConLicencia",
  "cedulaFoto",
  "licenciaFoto",
  "recordPolicivo",
  "registroVehicular",
  "seguroVehicular",
  "inspeccionVehicular",
  "vehiculoFrontal",
  "vehiculoTrasero",
  "vehiculoLateral",
];

function ptyDocsV4Required(role = "driver") {
  const r = String(role || "driver").toLowerCase();
  if (typeof requiredDocumentTypesForRole === "function") {
    try { return requiredDocumentTypesForRole(r); } catch {}
  }
  return r === "rider" ? ["identity"] : PTY_DOCS_V4_DRIVER_TYPES;
}

function ptyDocsV4PublicLean(row = {}) {
  const d = typeof documentToPublic === "function" ? documentToPublic(row) : {
    id: row.id,
    userId: row.user_id,
    role: row.role,
    type: row.type,
    status: row.status,
    url: row.url,
    filename: row.filename,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
  return d;
}

function ptyDocsV4StatusSummary(documents = [], role = "driver") {
  const required = ptyDocsV4Required(role);
  const byType = {};
  for (const doc of documents || []) {
    const type = String(doc.type || doc.documentType || doc.key || "").trim();
    if (!type) continue;
    const status = String(doc.status || "pending").toLowerCase();
    const prev = byType[type] || { type, total: 0, approved: 0, pending: 0, rejected: 0, status: "missing" };
    prev.total += 1;
    if (status === "approved") prev.approved += 1;
    else if (status === "rejected") prev.rejected += 1;
    else prev.pending += 1;

    // Regla: un tipo queda aprobado si al menos una versión enviada está aprobada.
    if (prev.approved > 0) prev.status = "approved";
    else if (prev.rejected > 0 && prev.pending === 0) prev.status = "rejected";
    else prev.status = "pending";
    byType[type] = prev;
  }

  const statusMap = {};
  for (const type of Object.keys(byType)) statusMap[type] = byType[type].status;

  const missing = required.filter((type) => statusMap[type] !== "approved");
  const anyRejected = Object.values(byType).some((it) => it.status === "rejected");
  const anyPending = Object.values(byType).some((it) => it.status === "pending");
  const approved = missing.length === 0;
  const documentStatus = approved ? "approved" : anyRejected ? "rejected" : anyPending ? "pending" : "missing";

  return { approved, missing, statusMap, byType, documentStatus, required };
}

async function ptyDocsV4LoadDocuments(userId, role = "") {
  const params = [String(userId || "")];
  let where = "user_id::text=$1::text";
  if (role) {
    params.push(String(role || ""));
    where += ` AND role::text=$${params.length}::text`;
  }
  const r = await db(
    `SELECT * FROM ride_documents
     WHERE ${where}
     ORDER BY created_at DESC`,
    params
  );
  return r.rows.map(ptyDocsV4PublicLean);
}

async function ptyDocsV4SetUserStatus(userId, role, summary) {
  const finalStatus = summary.documentStatus || (summary.approved ? "approved" : "pending");
  const payload = {
    documentStatus: finalStatus,
    verificationStatus: finalStatus,
    statusMap: summary.statusMap || {},
    driverDocumentsApproved: role === "driver" ? Boolean(summary.approved) : undefined,
    canAcceptRides: role === "driver" ? Boolean(summary.approved) : undefined,
    riderVerified: role === "rider" ? Boolean(summary.approved) : undefined,
    identityVerified: role === "rider" ? Boolean(summary.approved) : undefined,
  };
  const r = await db(
    `UPDATE ride_users
     SET document_status=$2::text,
         driver_docs=COALESCE(driver_docs,'{}'::jsonb) || $3::jsonb,
         updated_at=NOW()
     WHERE id::text=$1::text
     RETURNING *`,
    [String(userId || ""), finalStatus, JSON.stringify(payload)]
  );
  return r.rows[0] || null;
}

function ptyDocsV4UserPayload(userRow, role, summary) {
  const base = typeof publicUser === "function" ? publicUser(userRow || {}) : userRow || {};
  return {
    ...base,
    documentStatus: summary.documentStatus,
    verificationStatus: summary.documentStatus,
    driverDocumentsApproved: role === "driver" ? Boolean(summary.approved) : Boolean(base.driverDocumentsApproved),
    canAcceptRides: role === "driver" ? Boolean(summary.approved) : Boolean(base.canAcceptRides),
    riderVerified: role === "rider" ? Boolean(summary.approved) : Boolean(base.riderVerified),
    identityVerified: role === "rider" ? Boolean(summary.approved) : Boolean(base.identityVerified),
  };
}

async function ptyDocsV4StatusResponse(req, res, roleOverride = "") {
  try {
    const role = String(roleOverride || req.query.role || req.body?.role || req.user?.role || "driver").toLowerCase() === "rider" ? "rider" : "driver";
    const userId = req.user?.id || asText(req.query.userId || req.body?.userId);
    if (!userId) return safeJson(res, 400, { ok: false, message: "userId requerido" });

    const documents = await ptyDocsV4LoadDocuments(userId, role);
    const summary = ptyDocsV4StatusSummary(documents, role);
    const userR = await db(`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1`, [String(userId)]);
    const user = ptyDocsV4UserPayload(userR.rows[0], role, summary);

    return safeJson(res, 200, {
      ok: true,
      approved: summary.approved,
      verified: summary.approved,
      status: summary.documentStatus,
      driverDocumentsApproved: role === "driver" ? summary.approved : undefined,
      canAcceptRides: role === "driver" ? summary.approved : undefined,
      riderVerified: role === "rider" ? summary.approved : undefined,
      identityVerified: role === "rider" ? summary.approved : undefined,
      missing: summary.missing,
      statusMap: summary.statusMap,
      byType: summary.byType,
      documents,
      user,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
}

app.get("/api/rider/verification/status", authOptional, async (req, res) => {
  return ptyDocsV4StatusResponse(req, res, "rider");
});

app.get("/api/rider/documents/status", authOptional, async (req, res) => {
  return ptyDocsV4StatusResponse(req, res, "rider");
});

app.get("/api/driver/documents/status", authOptional, async (req, res) => {
  return ptyDocsV4StatusResponse(req, res, "driver");
});

app.get("/api/driver/documents/approved", authOptional, async (req, res) => {
  return ptyDocsV4StatusResponse(req, res, "driver");
});

app.get("/api/documents/status", authOptional, async (req, res) => {
  return ptyDocsV4StatusResponse(req, res, "");
});

app.patch("/api/admin/documents/:id/status", authRequired, requireAdmin, async (req, res) => {
  try {
    const started = Date.now();
    const docId = asText(req.params.id);
    const status = asText(req.body.status || "pending").toLowerCase();
    const reason = asText(req.body.reason || "");
    const allowed = new Set(["pending", "approved", "rejected"]);

    if (!docId) return safeJson(res, 400, { ok: false, message: "Documento requerido" });
    if (!allowed.has(status)) return safeJson(res, 400, { ok: false, message: "status inválido" });

    const updated = await db(
      `UPDATE ride_documents
       SET status=$2::text,
           reason=$3::text,
           updated_at=NOW()
       WHERE id::text=$1::text
       RETURNING id, user_id, role, type, status, reason, filename, mime_type, size_bytes, meta, created_at, updated_at`,
      [String(docId), status, reason]
    );

    if (!updated.rows.length) {
      return safeJson(res, 404, { ok: false, message: "Documento no encontrado" });
    }

    const rawDoc = updated.rows[0];
    const document = ptyDocsV4PublicLean(rawDoc);
    const userId = String(document.userId || rawDoc.user_id || "");
    const role = String(document.role || rawDoc.role || "driver").toLowerCase() === "rider" ? "rider" : "driver";

    const documents = await ptyDocsV4LoadDocuments(userId, role);
    const summary = ptyDocsV4StatusSummary(documents, role);
    const userRow = await ptyDocsV4SetUserStatus(userId, role, summary);
    const user = ptyDocsV4UserPayload(userRow, role, summary);

    const payload = {
      ok: true,
      document,
      status,
      approved: summary.approved,
      documentStatus: summary.documentStatus,
      verificationStatus: summary.documentStatus,
      missing: summary.missing,
      statusMap: summary.statusMap,
      byType: summary.byType,
      user,
      elapsedMs: Date.now() - started,
      title: summary.approved ? "Documentos aprobados" : status === "approved" ? "Documento aprobado" : status === "rejected" ? "Documento rechazado" : "Documento pendiente",
      message: summary.approved
        ? "Tus documentos han sido aprobados exitosamente."
        : status === "approved"
          ? "Documento aprobado. Aún pueden quedar documentos pendientes."
          : status === "rejected"
            ? "Documento rechazado. Revisa el motivo en la app."
            : "Documento marcado como pendiente.",
    };

    try {
      emitToUser(userId, "documents:status", payload);
      emitToUser(userId, "documents:approved", payload);
      io.to("admins").emit("documents:status", payload);
    } catch {}

    return safeJson(res, 200, payload);
  } catch (e) {
    console.error("[PTY_DOCS_V4_STATUS_ERROR]", e);
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.patch("/api/admin/documents/:id/status", authRequired, requireAdmin, async (req, res) => {
  try {
    const docId = asText(req.params.id);
    const status = asText(req.body.status || "pending").toLowerCase();
    const reason = asText(req.body.reason || "");
    const allowed = new Set(["pending", "approved", "rejected"]);

    if (!docId) return safeJson(res, 400, { ok: false, message: "Documento requerido" });
    if (!allowed.has(status)) return safeJson(res, 400, { ok: false, message: "status inválido" });

    const updated = await db(
      `UPDATE ride_documents
       SET status=$2::text,
           reason=$3::text,
           updated_at=NOW()
       WHERE id::text=$1::text
       RETURNING *`,
      [String(docId), status, reason]
    );

    if (!updated.rows.length) {
      return safeJson(res, 404, { ok: false, message: "Documento no encontrado" });
    }

    const rawDoc = updated.rows[0];
    const document = documentToPublic(rawDoc);
    const userId = String(document.userId || rawDoc.user_id || "");
    const role = String(document.role || rawDoc.role || "driver").toLowerCase() === "rider" ? "rider" : "driver";

    const docsR = await db(
      `SELECT * FROM ride_documents
       WHERE user_id::text=$1::text
         AND role::text=$2::text
       ORDER BY created_at DESC`,
      [userId, role]
    );

    const documents = docsR.rows.map(documentToPublic);
    const result = areDocumentsApproved(documents, role);
    const finalStatus = result.approved
      ? "approved"
      : documents.some((d) => String(d.status || "").toLowerCase() === "rejected")
        ? "rejected"
        : documents.length
          ? "pending"
          : "missing";

    await db(
      `UPDATE ride_users
       SET document_status=$2::text,
           driver_docs=COALESCE(driver_docs,'{}'::jsonb) || $3::jsonb,
           updated_at=NOW()
       WHERE id::text=$1::text`,
      [
        userId,
        finalStatus,
        JSON.stringify({
          [`${document.type}Status`]: status,
          documentStatus: finalStatus,
          verificationStatus: finalStatus,
          driverDocumentsApproved: role === "driver" ? result.approved : undefined,
          canAcceptRides: role === "driver" ? result.approved : undefined,
          riderVerified: role === "rider" ? result.approved : undefined,
          identityVerified: role === "rider" ? result.approved : undefined,
        }),
      ]
    );

    const userR = await db(`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1`, [userId]);
    const user = buildDocumentUserPayload(userR.rows[0], role, result.approved, {
      verificationStatus: finalStatus,
      riderVerified: role === "rider" ? result.approved : false,
      identityVerified: role === "rider" ? result.approved : false,
      driverDocumentsApproved: role === "driver" ? result.approved : false,
      canAcceptRides: role === "driver" ? result.approved : false,
    });

    const payload = {
      ok: true,
      document,
      documents,
      status,
      documentStatus: finalStatus,
      approved: result.approved,
      missing: result.missing,
      statusMap: result.statusMap,
      user,
      title: result.approved ? "Documentos aprobados" : status === "approved" ? "Documento aprobado" : status === "rejected" ? "Documento rechazado" : "Documento pendiente",
      message: result.approved
        ? "Tus documentos han sido aprobados exitosamente."
        : status === "approved"
          ? "Uno de tus documentos fue aprobado. Aún pueden quedar documentos pendientes."
          : status === "rejected"
            ? "Uno de tus documentos fue rechazado. Revisa el panel de verificación."
            : "Tu documento quedó pendiente de revisión.",
    };

    emitToUser(userId, "documents:status", payload);
    emitToUser(userId, "documents:approved", payload);
    io.to("admins").emit("documents:status", payload);

    return safeJson(res, 200, payload);
  } catch (e) {
    console.error("[ADMIN_DOCUMENT_STATUS_V3_ERROR]", e);
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.patch("/api/admin/documents/:id/status", authRequired, requireAdmin, async (req, res) => {
  try {
    const docId = asText(req.params.id);
    const status = asText(req.body.status || "pending").toLowerCase();
    const reason = asText(req.body.reason || "");
    const allowed = new Set(["pending", "approved", "rejected"]);

    if (!docId) return safeJson(res, 400, { ok: false, message: "Documento requerido" });
    if (!allowed.has(status)) return safeJson(res, 400, { ok: false, message: "status inválido" });

    const r = await db(
      `UPDATE ride_documents
       SET status=$2::text,
           reason=$3::text,
           updated_at=NOW()
       WHERE id::text=$1::text
       RETURNING *`,
      [String(docId), status, reason]
    );

    if (!r.rows.length) {
      return safeJson(res, 404, { ok: false, message: "Documento no encontrado" });
    }

    const rawDoc = r.rows[0];
    const doc = documentToPublic(rawDoc);
    const userId = String(doc.userId || rawDoc.user_id || "");
    const role = String(doc.role || rawDoc.role || "driver").toLowerCase();

    const docsR = await db(
      `SELECT * FROM ride_documents
       WHERE user_id::text=$1::text
         AND role::text=$2::text
       ORDER BY created_at DESC`,
      [userId, role]
    );

    const allDocs = docsR.rows.map(documentToPublic);
    const result = areDocumentsApproved(allDocs, role);
    const nextUserStatus = result.approved
      ? "approved"
      : allDocs.some((d) => String(d.status || "").toLowerCase() === "rejected")
        ? "rejected"
        : "pending";

    if (userId) {
      await db(
        `UPDATE ride_users
         SET document_status=$2::text,
             driver_docs=COALESCE(driver_docs,'{}'::jsonb) || $3::jsonb,
             updated_at=NOW()
         WHERE id::text=$1::text`,
        [
          userId,
          nextUserStatus,
          JSON.stringify({
            [`${doc.type}Status`]: status,
            driverDocumentsApproved: role === "driver" ? result.approved : undefined,
            canAcceptRides: role === "driver" ? result.approved : undefined,
            riderVerified: role === "rider" ? result.approved : undefined,
            identityVerified: role === "rider" ? result.approved : undefined,
          }),
        ]
      );
    }

    emitToUser(userId, "documents:status", {
      document: doc,
      status,
      approved: result.approved,
      missing: result.missing,
    });

    io.to("admins").emit("documents:status", {
      document: doc,
      status,
      approved: result.approved,
      missing: result.missing,
    });

    return safeJson(res, 200, {
      ok: true,
      document: doc,
      status,
      approved: result.approved,
      missing: result.missing,
      userDocumentStatus: nextUserStatus,
    });
  } catch (e) {
    console.error("[ADMIN_DOCUMENT_STATUS_ERROR]", e);
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.patch("/api/admin/documents/:id/status", authRequired, requireAdmin, async (req, res) => {
  try {
    const status = asText(req.body.status || "pending").toLowerCase();
    const reason = asText(req.body.reason || "");
    const allowed = new Set(["pending", "approved", "rejected"]);
    if (!allowed.has(status)) return safeJson(res, 400, { ok: false, message: "status inválido" });

    const r = await db(
      `UPDATE ride_documents
       SET status=$2, reason=$3, updated_at=NOW()
       WHERE id::text=$1::text
       RETURNING *`,
      [req.params.id, status, reason]
    );
    if (!r.rows.length) return safeJson(res, 404, { ok: false, message: "Documento no encontrado" });

    const doc = documentToPublic(r.rows[0]);
    const allDocs = await listUserDocuments(doc.userId, doc.role);
    const result = areDocumentsApproved(allDocs, doc.role);
    await db(
      `UPDATE ride_users
       SET document_status=$2,
           driver_docs=COALESCE(driver_docs,'{}'::jsonb) || $3::jsonb,
           updated_at=NOW()
       WHERE id::text=$1::text`,
      [
        doc.userId,
        result.approved ? "approved" : (allDocs.some((d) => d.status === "rejected") ? "rejected" : "pending"),
        JSON.stringify({
          [`${doc.type}Status`]: status,
          driverDocumentsApproved: doc.role === "driver" ? result.approved : undefined,
          canAcceptRides: doc.role === "driver" ? result.approved : undefined,
        }),
      ]
    );

    emitToUser(doc.userId, "documents:status", { document: doc, status, approved: result.approved, missing: result.missing });
    io.to("admins").emit("documents:status", { document: doc, status, approved: result.approved });

    return safeJson(res, 200, { ok: true, document: doc, approved: result.approved, missing: result.missing });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

/* =========================================================
   Documents / verification
========================================================= */
app.post("/api/documents", authRequired, async (req, res) => {
  try {
    const docs = req.body.documents || req.body.docs || req.body;
    const r = await db(
      `UPDATE ride_users
       SET driver_docs=COALESCE(driver_docs,'{}'::jsonb) || $2::jsonb,
           document_status='pending',
           updated_at=NOW()
       WHERE id::text=$1::text
       RETURNING *`,
      [req.user.id, JSON.stringify(docs || {})]
    );
    io.to("admins").emit("documents:pending", publicUser(r.rows[0]));
    return safeJson(res, 200, { ok: true, user: publicUser(r.rows[0]) });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/documents/status", authRequired, async (req, res) => {
  return safeJson(res, 200, {
    ok: true,
    status: req.user.document_status || "pending",
    user: publicUser(req.user),
  });
});

app.patch("/api/admin/users/:id/document-status", authRequired, requireAdmin, async (req, res) => {
  const status = asText(req.body.status || "approved");
  const r = await db(
    `UPDATE ride_users SET document_status=$2, updated_at=NOW() WHERE id::text=$1::text RETURNING *`,
    [req.params.id, status]
  );
  if (!r.rows.length) return safeJson(res, 404, { ok: false, message: "Usuario no encontrado" });
  emitToUser(req.params.id, "documents:status", { status, user: publicUser(r.rows[0]) });
  return safeJson(res, 200, { ok: true, user: publicUser(r.rows[0]) });
});

/* =========================================================
   SOS
========================================================= */
app.post("/api/sos", authRequired, async (req, res) => {
  try {
    const cooldown = Math.max(1, Number(SOS_COOLDOWN_MINUTES || 5));
    const recent = await db(
      `SELECT id, created_at FROM ride_sos_alerts
       WHERE user_id=$1 AND created_at > NOW() - ($2::text || ' minutes')::interval
       ORDER BY created_at DESC LIMIT 1`,
      [req.user.id, String(cooldown)]
    );

    if (recent.rows.length) {
      return safeJson(res, 429, {
        ok: false,
        message: `SOS ya enviado. Espera ${cooldown} minutos antes de enviar otro.`,
        lastSosId: recent.rows[0].id,
      });
    }

    const rideId = asText(req.body.rideId || req.body.ride_id);
    const ride = rideId ? await getRideById(rideId) : null;
    const location = normalizePoint(req.body.location || req.body);
    const route = req.body.route || ride?.route || {};
    const message = asText(req.body.message || "SOS activado");

    const r = await db(
      `INSERT INTO ride_sos_alerts(
        ride_id, user_id, user_role, location, route, ride_snapshot, user_snapshot, message
       )
       VALUES($1,$2,$3,$4::jsonb,$5::jsonb,$6::jsonb,$7::jsonb,$8)
       RETURNING *`,
      [
        ride?.id || null,
        req.user.id,
        asText(req.body.role || req.user.role || ""),
        JSON.stringify(location || {}),
        JSON.stringify(route || {}),
        JSON.stringify(ride ? normalizeRide(ride) : {}),
        JSON.stringify(getSnapshotUser(req.user)),
        message,
      ]
    );

    const alert = r.rows[0];
    const payload = { ok: true, sos: alert };
    io.to("admins").emit("sos:new", payload);
    emitToUser(req.user.id, "sos:received", {
      message: "Tu seguridad es lo primero. El centro de ayuda se pondrá en contacto contigo, estamos monitoreando tu carrera.",
      sos: alert,
    });

    return safeJson(res, 201, payload);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/admin/sos", authRequired, requireAdmin, async (_req, res) => {
  const r = await db(`SELECT * FROM ride_sos_alerts ORDER BY created_at DESC LIMIT 100`);
  return safeJson(res, 200, { ok: true, alerts: r.rows });
});

app.patch("/api/admin/sos/:id/close", authRequired, requireAdmin, async (req, res) => {
  const r = await db(
    `UPDATE ride_sos_alerts SET status='closed', closed_at=NOW() WHERE id::text=$1::text RETURNING *`,
    [req.params.id]
  );
  if (!r.rows.length) return safeJson(res, 404, { ok: false, message: "SOS no encontrado" });
  io.to("admins").emit("sos:closed", r.rows[0]);
  return safeJson(res, 200, { ok: true, sos: r.rows[0] });
});

/* =========================================================
   Chat
========================================================= */
app.get("/api/chats/:rideId/messages", authRequired, async (req, res) => {
  const r = await db(
    `SELECT * FROM ride_chat_messages WHERE ride_id=$1 ORDER BY created_at ASC LIMIT 500`,
    [req.params.rideId]
  );
  return safeJson(res, 200, { ok: true, messages: r.rows });
});

app.post("/api/chats/:rideId/messages", authRequired, async (req, res) => {
  try {
    const ride = await getRideById(req.params.rideId);
    if (!ride) return safeJson(res, 404, { ok: false, message: "Carrera no encontrada" });

    const message = asText(req.body.message || req.body.text);
    if (!message) return safeJson(res, 400, { ok: false, message: "Mensaje vacío" });

    const targetUserId =
      asText(req.body.targetUserId) ||
      (req.user.role === "admin"
        ? (ride.rider_id || ride.driver_id)
        : req.user.id === ride.rider_id
          ? ride.driver_id
          : ride.rider_id);

    const r = await db(
      `INSERT INTO ride_chat_messages(ride_id, sender_id, sender_role, target_user_id, message, source)
       VALUES($1,$2,$3,$4,$5,$6)
       RETURNING *`,
      [ride.id, req.user.id, req.user.role || "", targetUserId || null, message, asText(req.body.source || "ride")]
    );

    const payload = { ok: true, message: r.rows[0], ride: normalizeRide(ride) };
    emitToUser(ride.rider_id, "chat:message", payload);
    emitToUser(ride.driver_id, "chat:message", payload);
    if (targetUserId) emitToUser(targetUserId, "support:message", payload);
    io.to("admins").emit("chat:message", payload);

    return safeJson(res, 201, payload);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

/* =========================================================
   Admin
========================================================= */
app.get("/api/admin/users", authRequired, requireAdmin, async (_req, res) => {
  const r = await db(`SELECT * FROM ride_users ORDER BY created_at DESC LIMIT 500`);
  return safeJson(res, 200, { ok: true, users: r.rows.map(publicUser) });
});

app.get("/api/admin/rides", authRequired, requireAdmin, async (_req, res) => {
  await expireOldRides();
  const r = await db(`SELECT * FROM ride_rides ORDER BY created_at DESC LIMIT 500`);
  return safeJson(res, 200, { ok: true, rides: r.rows.map(normalizeRide) });
});

/* =========================================================
   Socket handlers
========================================================= */
io.use(async (socket, next) => {
  try {
    const token =
      socket.handshake.auth?.token ||
      socket.handshake.query?.token ||
      "";
    if (!token) return next();

    const payload = jwt.verify(String(token), JWT_SECRET || "dev_secret_change_me_now_ptydrive");
    const r = await db(`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1`, [payload.id]);
    socket.user = r.rows[0] || null;
    next();
  } catch {
    next();
  }
});

io.on("connection", (socket) => {
  const user = socket.user;
  if (user?.id) {
    socket.join(`user:${user.id}`);
    if (user.role === "driver") socket.join("drivers");
    if (user.role === "rider") socket.join("riders");
    if (user.role === "admin") socket.join("admins");
  }

  socket.on("join", (payload = {}) => {
    if (payload.userId) socket.join(`user:${payload.userId}`);
    if (payload.role === "driver") socket.join("drivers");
    if (payload.role === "rider") socket.join("riders");
    if (payload.role === "admin") socket.join("admins");
    if (payload.rideId) socket.join(`ride:${payload.rideId}`);
  });

  socket.on("driver:online", () => socket.join("drivers"));
  socket.on("admin:online", () => socket.join("admins"));

  socket.on("location:update", async (payload = {}) => {
    try {
      if (!user?.id) return;
      const role = asText(payload.role || user.role || "");
      const lat = asNum(payload.lat ?? payload.latitude);
      const lng = asNum(payload.lng ?? payload.longitude);
      if (lat === null || lng === null) return;

      await db(
        `INSERT INTO ride_locations(user_id, role, lat, lng, heading, speed, updated_at)
         VALUES($1,$2,$3,$4,$5,$6,NOW())
         ON CONFLICT(user_id, role) DO UPDATE SET
          lat=EXCLUDED.lat,lng=EXCLUDED.lng,heading=EXCLUDED.heading,speed=EXCLUDED.speed,updated_at=NOW()`,
        [user.id, role, lat, lng, asNum(payload.heading), asNum(payload.speed)]
      );

      const out = { userId: user.id, role, lat, lng, heading: payload.heading, speed: payload.speed, updatedAt: nowIso() };
      socket.broadcast.emit("location:update", out);
    } catch (e) {
      socket.emit("error:server", { message: String(e?.message || e) });
    }
  });

  socket.on("disconnect", () => {});
});

/* =========================================================
   Boot
========================================================= */
async function boot() {
  await ensureDb();
  await expireOldRides();

  server.listen(Number(PORT), "0.0.0.0", () => {
    console.log(`[PTY Drive] API lista en puerto ${PORT}`);
  });
}

boot().catch((e) => {
  console.error("[BOOT ERROR]", e);
  process.exit(1);
});
