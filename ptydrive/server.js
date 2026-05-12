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

/* PTY PERFORMANCE: medir endpoints lentos sin romper respuesta */
app.use((req, res, next) => {
  const started = Date.now();
  res.on("finish", () => {
    const ms = Date.now() - started;
    if (ms > Number(process.env.PTY_SLOW_REQUEST_MS || 1500)) {
      console.warn(`[HTTP SLOW] ${req.method} ${req.originalUrl || req.url} ${res.statusCode} ${ms}ms`);
    }
  });
  next();
});


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
  max: Number(process.env.PG_POOL_MAX || 20),
  idleTimeoutMillis: Number(process.env.PG_IDLE_TIMEOUT_MS || 30000),
  connectionTimeoutMillis: Number(process.env.PG_CONNECT_TIMEOUT_MS || 5000),
  keepAlive: true,
  keepAliveInitialDelayMillis: 10000,
  statement_timeout: Number(process.env.PG_STATEMENT_TIMEOUT_MS || 8000),
  query_timeout: Number(process.env.PG_QUERY_TIMEOUT_MS || 9000),
});

async function db(text, params = []) {
  const started = Date.now();
  try {
    const result = await pool.query(text, params);
    const ms = Date.now() - started;
    if (ms > Number(process.env.PTY_SLOW_QUERY_MS || 1200)) {
      const compact = String(text || "").replace(/\s+/g, " ").trim().slice(0, 220);
      console.warn(`[DB SLOW] ${ms}ms ${compact}`);
    }
    return result;
  } catch (error) {
    const ms = Date.now() - started;
    const compact = String(text || "").replace(/\s+/g, " ").trim().slice(0, 220);
    console.error(`[DB ERROR] ${ms}ms ${compact}`, error?.message || error);
    throw error;
  }
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
   PTY V11 CLEAN CHAT + FAST THREADS
   Respuesta mínima, chat persistente y socket rooms reales.
========================================================= */
const PTY_V11_CHAT_PATCH = "v11-clean-chat";

function ptyV11Text(value = "") {
  return String(value ?? "").trim();
}
function ptyV11Json(value, fallback = {}) {
  if (!value) return fallback;
  if (typeof value === "object") return value;
  try { return JSON.parse(value); } catch { return fallback; }
}
function ptyV11PointLabel(value = {}, fallback = "") {
  const source = ptyV11Json(value, value || {});
  return ptyV11Text(source.address || source.title || source.short || source.name || source.label || fallback);
}
function ptyV11PublicPerson(snapshot = {}, fallbackId = "", fallbackRole = "") {
  const source = ptyV11Json(snapshot, snapshot || {});
  const vehicle = source.vehicle || source.driverVehicle || {};
  return {
    id: ptyV11Text(source.id || source.userId || source.driverId || source.riderId || fallbackId),
    name: ptyV11Text(source.name || source.fullName || source.displayName || (fallbackRole === "driver" ? "Conductor" : "Usuario")),
    email: ptyV11Text(source.email || ""),
    phone: ptyV11Text(source.phone || source.telefonoContacto || ""),
    role: ptyV11Text(source.role || fallbackRole),
    photoUrl: ptyV11Text(source.photoUrl || source.driverPhotoUrl || source.driverPhoto || source.profilePhoto || source.avatarUrl || ""),
    rating: Number(source.rating || 5),
    reviewsCount: Number(source.reviewsCount || 0),
    vehicle: fallbackRole === "driver" ? {
      type: ptyV11Text(vehicle.type || vehicle.vehicleType || source.vehicleType || source.enrollmentType || "viaje"),
      brand: ptyV11Text(vehicle.brand || vehicle.make || source.marcaVehiculo || ""),
      model: ptyV11Text(vehicle.model || source.modeloVehiculo || ""),
      color: ptyV11Text(vehicle.color || vehicle.colorName || source.colorVehiculo || ""),
      year: ptyV11Text(vehicle.year || source.anioVehiculo || ""),
      plate: ptyV11Text(vehicle.plate || source.plate || source.placa || ""),
    } : undefined,
  };
}
function ptyV11CompactThreadFromRide(row = {}, last = {}) {
  const riderId = ptyV11Text(row.rider_id || row.riderId || "");
  const driverId = ptyV11Text(row.driver_id || row.driverId || "");
  const rider = ptyV11PublicPerson(row.rider_snapshot, riderId, "rider");
  const driver = ptyV11PublicPerson(row.driver_snapshot, driverId, "driver");
  const pickupAddress = ptyV11PointLabel(row.pickup, "Recogida");
  const destinationAddress = ptyV11PointLabel(row.destination, "Destino");
  const rideId = ptyV11Text(row.id || row.ride_id || row.rideId || last.ride_id || "");
  return {
    id: rideId,
    rideId,
    riderId,
    driverId,
    rider,
    driver,
    riderName: rider.name,
    driverName: driver.name,
    pickupAddress,
    destinationAddress,
    routeLabel: `${pickupAddress} → ${destinationAddress}`,
    title: driver.name || rider.name || `Viaje ${rideId.slice(-6)}`,
    subtitle: `${pickupAddress} → ${destinationAddress}`,
    status: row.status || "",
    lastMessage: last.message || last.text || "",
    updatedAt: last.created_at || row.updated_at || row.created_at || new Date().toISOString(),
    unread: false,
  };
}
function ptyV11Message(row = {}) {
  return {
    id: ptyV11Text(row.id || ""),
    rideId: ptyV11Text(row.ride_id || row.rideId || ""),
    senderId: ptyV11Text(row.sender_id || row.senderId || ""),
    senderRole: ptyV11Text(row.sender_role || row.senderRole || "user"),
    senderName: ptyV11Text(row.sender_name || row.senderName || row.author || "Usuario"),
    author: ptyV11Text(row.sender_name || row.senderName || row.author || "Usuario"),
    text: ptyV11Text(row.message || row.text || ""),
    message: ptyV11Text(row.message || row.text || ""),
    createdAt: row.created_at || row.createdAt || new Date().toISOString(),
    at: row.created_at || row.createdAt || new Date().toISOString(),
  };
}
async function ptyV11EnsureChatTables() {
  if (globalThis.__PTY_V11_CHAT_READY__) return;
  await db(`CREATE EXTENSION IF NOT EXISTS pgcrypto;`);
  await db(`
    CREATE TABLE IF NOT EXISTS ride_chat_v11_messages (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      ride_id TEXT NOT NULL,
      sender_id TEXT DEFAULT '',
      sender_role TEXT DEFAULT '',
      sender_name TEXT DEFAULT '',
      message TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
  await db(`
    CREATE TABLE IF NOT EXISTS ride_chat_v11_threads (
      ride_id TEXT PRIMARY KEY,
      rider_id TEXT DEFAULT '',
      driver_id TEXT DEFAULT '',
      last_message TEXT DEFAULT '',
      last_at TIMESTAMPTZ DEFAULT NOW(),
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
  await db(`CREATE INDEX IF NOT EXISTS idx_chat_v11_msg_ride_time ON ride_chat_v11_messages(ride_id, created_at);`);
  await db(`CREATE INDEX IF NOT EXISTS idx_chat_v11_threads_rider ON ride_chat_v11_threads(rider_id, updated_at DESC);`);
  await db(`CREATE INDEX IF NOT EXISTS idx_chat_v11_threads_driver ON ride_chat_v11_threads(driver_id, updated_at DESC);`);
  await db(`CREATE INDEX IF NOT EXISTS idx_rides_user_fast_v11 ON ride_rides(rider_id, driver_id, updated_at DESC);`).catch(() => null);
  globalThis.__PTY_V11_CHAT_READY__ = true;
}
function ptyV11AuthOptional(req, _res, next) {
  try {
    if (typeof authOptional === "function") return authOptional(req, _res, next);
  } catch {}
  return next();
}
function ptyV11EmitMessage(rideRow = {}, message = {}) {
  const rideId = ptyV11Text(rideRow.id || message.rideId || message.ride_id || "");
  const payload = { ok: true, chatPatch: PTY_V11_CHAT_PATCH, rideId, message: ptyV11Message(message) };
  try {
    io.to(`ride:${rideId}`).emit("chat.message", payload);
    io.to(`chat:${rideId}`).emit("chat.message", payload);
    if (rideRow.rider_id) io.to(`user:${rideRow.rider_id}`).emit("chat.message", payload);
    if (rideRow.driver_id) io.to(`user:${rideRow.driver_id}`).emit("chat.message", payload);
    io.emit("chat.v11.message", payload);
  } catch (e) {
    console.warn("[PTY_V11_SOCKET_WARN]", e?.message || e);
  }
}
try {
  io.on("connection", (socket) => {
    const joinRooms = (raw = {}) => {
      const userId = ptyV11Text(raw.userId || raw.id || raw.driverId || raw.riderId || "");
      const rideId = ptyV11Text(raw.rideId || raw.chatId || raw.threadId || "");
      if (userId) socket.join(`user:${userId}`);
      if (rideId) {
        socket.join(`ride:${rideId}`);
        socket.join(`chat:${rideId}`);
      }
    };
    socket.on("join", joinRooms);
    socket.on("ride.join", joinRooms);
    socket.on("chat.join", joinRooms);
  });
} catch {}

app.get("/api/chat/health", async (_req, res) => {
  let dbOk = false;
  try { await ptyV11EnsureChatTables(); await db("SELECT 1"); dbOk = true; } catch {}
  return safeJson(res, 200, { ok: true, chatPatch: PTY_V11_CHAT_PATCH, db: dbOk ? "on" : "off", time: nowIso() });
});

app.get("/api/chat/threads", ptyV11AuthOptional, async (req, res) => {
  try {
    await ptyV11EnsureChatTables();
    const userId = ptyV11Text(req.user?.id || req.query.userId || req.query.driverId || req.query.riderId || "");
    const email = ptyV11Text(req.user?.email || req.query.email || "").toLowerCase();
    const limit = Math.max(1, Math.min(100, Number(req.query.limit || 80)));
    if (!userId && !email) return safeJson(res, 200, { ok: true, chatPatch: PTY_V11_CHAT_PATCH, threads: [] });
    const rows = await db(`
      WITH user_rides AS (
        SELECT *
        FROM ride_rides
        WHERE ($1::text <> '' AND (rider_id::text=$1::text OR driver_id::text=$1::text))
           OR ($2::text <> '' AND (LOWER(COALESCE(rider_snapshot->>'email',''))=$2::text OR LOWER(COALESCE(driver_snapshot->>'email',''))=$2::text))
        ORDER BY updated_at DESC NULLS LAST, created_at DESC
        LIMIT $3
      ), last_msg AS (
        SELECT DISTINCT ON (ride_id) *
        FROM ride_chat_v11_messages
        WHERE ride_id IN (SELECT id::text FROM user_rides)
        ORDER BY ride_id, created_at DESC
      )
      SELECT r.*, m.id AS msg_id, m.message AS msg_message, m.sender_id AS msg_sender_id, m.sender_role AS msg_sender_role, m.sender_name AS msg_sender_name, m.created_at AS msg_created_at
      FROM user_rides r
      LEFT JOIN last_msg m ON m.ride_id = r.id::text
      ORDER BY COALESCE(m.created_at, r.updated_at, r.created_at) DESC
      LIMIT $3
    `, [userId, email, limit]);
    const threads = rows.rows.map((row) => ptyV11CompactThreadFromRide(row, {
      id: row.msg_id,
      ride_id: row.id,
      message: row.msg_message,
      sender_id: row.msg_sender_id,
      sender_role: row.msg_sender_role,
      sender_name: row.msg_sender_name,
      created_at: row.msg_created_at,
    }));
    return safeJson(res, 200, { ok: true, chatPatch: PTY_V11_CHAT_PATCH, threads });
  } catch (e) {
    console.error("[PTY_V11_THREADS_ERROR]", e?.message || e);
    return safeJson(res, 500, { ok: false, chatPatch: PTY_V11_CHAT_PATCH, message: String(e?.message || e), threads: [] });
  }
});

app.get("/api/chat/rides/:rideId/messages", ptyV11AuthOptional, async (req, res) => {
  try {
    await ptyV11EnsureChatTables();
    const rideId = ptyV11Text(req.params.rideId || "");
    const limit = Math.max(1, Math.min(300, Number(req.query.limit || 250)));
    const r = await db(`
      SELECT * FROM ride_chat_v11_messages
      WHERE ride_id=$1::text
      ORDER BY created_at ASC
      LIMIT $2
    `, [rideId, limit]);
    const messages = r.rows.map(ptyV11Message);
    return safeJson(res, 200, { ok: true, chatPatch: PTY_V11_CHAT_PATCH, rideId, messages, chat: messages });
  } catch (e) {
    console.error("[PTY_V11_MESSAGES_ERROR]", e?.message || e);
    return safeJson(res, 500, { ok: false, chatPatch: PTY_V11_CHAT_PATCH, message: String(e?.message || e), messages: [] });
  }
});

app.post("/api/chat/rides/:rideId/messages", ptyV11AuthOptional, async (req, res) => {
  try {
    await ptyV11EnsureChatTables();
    const rideId = ptyV11Text(req.params.rideId || req.body.rideId || "");
    const text = ptyV11Text(req.body.text || req.body.message || req.body.body || "");
    if (!rideId || !text) return safeJson(res, 400, { ok: false, message: "rideId y message son requeridos" });
    const rideResult = await db(`SELECT * FROM ride_rides WHERE id::text=$1::text LIMIT 1`, [rideId]);
    const rideRow = rideResult.rows?.[0] || { id: rideId, rider_id: "", driver_id: "" };
    const senderId = ptyV11Text(req.user?.id || req.body.senderId || req.body.userId || "");
    const senderRole = ptyV11Text(req.body.senderRole || req.user?.role || "user");
    const senderName = ptyV11Text(req.body.senderName || req.user?.name || (senderRole === "driver" ? "Conductor" : "Rider"));
    const inserted = await db(`
      INSERT INTO ride_chat_v11_messages(ride_id, sender_id, sender_role, sender_name, message)
      VALUES($1,$2,$3,$4,$5)
      RETURNING *
    `, [rideId, senderId, senderRole, senderName, text]);
    const message = inserted.rows[0];
    await db(`
      INSERT INTO ride_chat_v11_threads(ride_id, rider_id, driver_id, last_message, last_at, updated_at)
      VALUES($1,$2,$3,$4,NOW(),NOW())
      ON CONFLICT(ride_id) DO UPDATE SET
        rider_id=COALESCE(NULLIF(EXCLUDED.rider_id,''), ride_chat_v11_threads.rider_id),
        driver_id=COALESCE(NULLIF(EXCLUDED.driver_id,''), ride_chat_v11_threads.driver_id),
        last_message=EXCLUDED.last_message,
        last_at=NOW(),
        updated_at=NOW()
    `, [rideId, ptyV11Text(rideRow.rider_id || ""), ptyV11Text(rideRow.driver_id || ""), text]);
    ptyV11EmitMessage(rideRow, message);
    return safeJson(res, 201, { ok: true, chatPatch: PTY_V11_CHAT_PATCH, rideId, message: ptyV11Message(message) });
  } catch (e) {
    console.error("[PTY_V11_SEND_ERROR]", e?.message || e);
    return safeJson(res, 500, { ok: false, chatPatch: PTY_V11_CHAT_PATCH, message: String(e?.message || e) });
  }
});

// Compatibilidad: las rutas viejas ahora caen en el mismo motor V11.
app.get("/api/rides/:rideId/chat", ptyV11AuthOptional, (req, res) => {
  req.url = `/api/chat/rides/${encodeURIComponent(req.params.rideId)}/messages`;
  return app._router.handle(req, res);
});
app.post("/api/rides/:rideId/chat", ptyV11AuthOptional, (req, res) => {
  req.url = `/api/chat/rides/${encodeURIComponent(req.params.rideId)}/messages`;
  return app._router.handle(req, res);
});

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


/* =========================================================
   PTY DRIVE BACKEND PERFORMANCE HOTFIX V5
   Rutas críticas registradas ANTES de los handlers antiguos.
   Objetivo: aceptar / iniciar / cancelar / finalizar en < 1s cuando DB responde.
========================================================= */
const PTY_FAST_V5_ACTIVE_STATUSES = [
  "requested",
  "searching",
  "accepted",
  "assigned",
  "arrived",
  "in_progress",
];

const PTY_FAST_V5_FINAL_STATUSES = [
  "completed",
  "cancelled",
  "expired",
  "driver_cancelled",
  "rider_cancelled",
  "auto_cancelled",
];

const ptyFastV5Cache = new Map();

function ptyFastV5AuthOptional(req, _res, next) {
  if (req.user?.id) return next();
  const token = readBearer(req);
  if (!token) return next();
  try {
    req.user = jwt.verify(token, JWT_SECRET || "dev_secret_change_me_now_ptydrive");
  } catch {
    req.user = null;
  }
  return next();
}

function ptyFastV5AuthRequired(req, res, next) {
  if (req.user?.id) return next();
  const token = readBearer(req);
  if (!token) return safeJson(res, 401, { ok: false, message: "Missing Bearer token" });
  try {
    req.user = jwt.verify(token, JWT_SECRET || "dev_secret_change_me_now_ptydrive");
    return next();
  } catch {
    return safeJson(res, 401, { ok: false, message: "Invalid token" });
  }
}


function ptyFastV5CacheGet(key) {
  const item = ptyFastV5Cache.get(key);
  if (!item) return null;
  if (Date.now() > item.expiresAt) {
    ptyFastV5Cache.delete(key);
    return null;
  }
  return item.value;
}

function ptyFastV5CacheSet(key, value, ttlMs = 30000) {
  if (ptyFastV5Cache.size > 500) ptyFastV5Cache.clear();
  ptyFastV5Cache.set(key, { value, expiresAt: Date.now() + ttlMs });
  return value;
}

function ptyFastV5Text(v = "") {
  return String(v ?? "").trim();
}

function ptyFastV5Num(v, fallback = null) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function ptyFastV5Point(input = {}) {
  const raw = input && typeof input === "object" ? input : {};
  const lat = ptyFastV5Num(raw.lat ?? raw.latitude);
  const lng = ptyFastV5Num(raw.lng ?? raw.lon ?? raw.longitude);
  if (lat === null || lng === null) return null;
  return {
    ...raw,
    lat,
    lng,
    address: ptyFastV5Text(raw.address || raw.title || raw.short || raw.name || raw.label),
    title: ptyFastV5Text(raw.title || raw.name || raw.address || raw.label),
    name: ptyFastV5Text(raw.name || raw.title || raw.address || raw.label),
  };
}

function ptyFastV5Snapshot(user = {}, fallbackRole = "") {
  return {
    id: user.id || user.userId || "",
    name: user.name || user.fullName || "",
    email: user.email || "",
    phone: user.phone || "",
    role: user.role || fallbackRole || "",
    markerIcon: user.marker_icon || user.markerIcon || "📍",
  };
}

function ptyFastV5DistanceKm(a, b) {
  const p1 = ptyFastV5Point(a);
  const p2 = ptyFastV5Point(b);
  if (!p1 || !p2) return 0;
  const R = 6371;
  const dLat = (p2.lat - p1.lat) * Math.PI / 180;
  const dLng = (p2.lng - p1.lng) * Math.PI / 180;
  const s1 = Math.sin(dLat / 2) ** 2;
  const s2 = Math.cos(p1.lat * Math.PI / 180) * Math.cos(p2.lat * Math.PI / 180) * Math.sin(dLng / 2) ** 2;
  return Number((R * 2 * Math.atan2(Math.sqrt(s1 + s2), Math.sqrt(1 - s1 - s2))).toFixed(3));
}

function ptyFastV5FallbackRoute(origin, destination) {
  const o = ptyFastV5Point(origin);
  const d = ptyFastV5Point(destination);
  const distanceKm = ptyFastV5DistanceKm(o, d);
  const durationMin = Math.max(1, Math.round((distanceKm / 28) * 60));
  const coords = o && d ? [
    { lat: o.lat, lng: o.lng },
    { lat: d.lat, lng: d.lng },
  ] : [];
  return {
    ok: true,
    provider: "fast_fallback",
    fallback: true,
    coords,
    routeCoords: coords,
    coordinates: coords,
    distanceKm,
    distanceMeters: Math.round(distanceKm * 1000),
    durationMin,
    durationSeconds: durationMin * 60,
    encodedPolyline: "",
  };
}

async function ptyFastV5FetchJson(url, options = {}, timeoutMs = 2200) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { ...options, signal: controller.signal });
    const json = await response.json().catch(() => ({}));
    return { ok: response.ok, status: response.status, json };
  } finally {
    clearTimeout(timeout);
  }
}

function ptyFastV5GoogleKey() {
  return String(process.env.GOOGLE_MAPS_API_KEY || process.env.GOOGLE_API_KEY || "").trim();
}

function ptyFastV5PlaceFromGoogle(result = {}, fallbackName = "") {
  const loc = result?.geometry?.location || {};
  const lat = ptyFastV5Num(loc.lat);
  const lng = ptyFastV5Num(loc.lng);
  const clean = (value, fallback = "") => {
    const text = ptyFastV5Text(value || fallback)
      .replace(/^[A-Z0-9]{3,}\+[A-Z0-9]{2,}\s*,\s*/i, "")
      .replace(/,\s*Panamá\s*,\s*Panamá$/i, ", Panamá")
      .replace(/,\s*Panama\s*,\s*Panama$/i, ", Panamá")
      .replace(/\s+/g, " ")
      .trim();
    return text;
  };
  const name = clean(result.name || result.formatted_address || fallbackName || "Destino", fallbackName || "Destino");
  const address = clean(result.formatted_address || result.vicinity || name, name);
  return {
    id: result.place_id || `google_${lat}_${lng}`,
    placeId: result.place_id || "",
    googlePlaceId: result.place_id || "",
    source: "google_fast",
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

async function ptyFastV5PlacesSearch(req, res) {
  const q = ptyFastV5Text(req.query.q || req.query.input || req.query.query || "");
  if (!q) return safeJson(res, 200, { ok: true, provider: "google_fast", places: [], results: [] });

  const cacheKey = `places:${q}:${req.query.lat || ""}:${req.query.lng || ""}:${req.query.limit || ""}`.toLowerCase();
  const cached = ptyFastV5CacheGet(cacheKey);
  if (cached) return safeJson(res, 200, cached);

  try {
    const key = ptyFastV5GoogleKey();
    if (!key) return safeJson(res, 200, { ok: true, provider: "local_fallback", places: [], results: [] });

    const max = Math.max(1, Math.min(10, Number(req.query.limit || 8)));
    const url = new URL("https://maps.googleapis.com/maps/api/place/textsearch/json");
    url.searchParams.set("query", /panam[áa]/i.test(q) ? q : `${q}, Panamá`);
    url.searchParams.set("language", "es");
    url.searchParams.set("region", "pa");
    url.searchParams.set("key", key);
    const lat = ptyFastV5Num(req.query.lat);
    const lng = ptyFastV5Num(req.query.lng);
    if (lat !== null && lng !== null) {
      url.searchParams.set("location", `${lat},${lng}`);
      url.searchParams.set("radius", "60000");
    }

    const { json } = await ptyFastV5FetchJson(url, {}, Number(process.env.PTY_GOOGLE_TIMEOUT_MS || 2200));
    const places = (Array.isArray(json.results) ? json.results : [])
      .slice(0, max)
      .map((r) => ptyFastV5PlaceFromGoogle(r, q))
      .filter((p) => Number.isFinite(Number(p.lat)) && Number.isFinite(Number(p.lng)));

    const payload = { ok: true, provider: "google_fast_textsearch", places, results: places };
    return safeJson(res, 200, ptyFastV5CacheSet(cacheKey, payload, 45000));
  } catch (error) {
    return safeJson(res, 200, { ok: true, provider: "fast_timeout_fallback", places: [], results: [], warning: String(error?.message || error) });
  }
}

async function ptyFastV5Reverse(req, res) {
  const lat = ptyFastV5Num(req.query.lat ?? req.body?.lat);
  const lng = ptyFastV5Num(req.query.lng ?? req.body?.lng);
  if (lat === null || lng === null) return safeJson(res, 400, { ok: false, message: "lat/lng requeridos" });

  const cacheKey = `reverse:${lat.toFixed(6)},${lng.toFixed(6)}`;
  const cached = ptyFastV5CacheGet(cacheKey);
  if (cached) return safeJson(res, 200, cached);

  try {
    const key = ptyFastV5GoogleKey();
    if (!key) throw new Error("GOOGLE_MAPS_API_KEY no configurada");

    const url = new URL("https://maps.googleapis.com/maps/api/geocode/json");
    url.searchParams.set("latlng", `${lat},${lng}`);
    url.searchParams.set("language", "es");
    url.searchParams.set("region", "pa");
    url.searchParams.set("key", key);

    const { json } = await ptyFastV5FetchJson(url, {}, Number(process.env.PTY_GOOGLE_TIMEOUT_MS || 2200));
    const best = Array.isArray(json.results) ? json.results[0] : null;
    const place = best ? ptyFastV5PlaceFromGoogle({ ...best, geometry: { location: { lat, lng } } }, "Punto seleccionado") : null;
    const fallback = {
      title: "Punto seleccionado",
      name: "Punto seleccionado",
      address: `${lat.toFixed(5)}, ${lng.toFixed(5)}`,
      lat,
      lng,
    };
    const selected = place || fallback;
    const payload = {
      ok: true,
      provider: place ? "google_fast_geocode" : "fast_fallback",
      label: selected.title || selected.address,
      title: selected.title || selected.address,
      name: selected.name || selected.title || selected.address,
      address: selected.address || selected.title,
      display_name: selected.address || selected.title,
      place: { ...selected, lat, lng },
      results: place ? [place] : [],
    };
    return safeJson(res, 200, ptyFastV5CacheSet(cacheKey, payload, 90000));
  } catch (error) {
    const payload = {
      ok: true,
      provider: "fast_timeout_fallback",
      label: "Punto seleccionado",
      title: "Punto seleccionado",
      name: "Punto seleccionado",
      address: `${lat.toFixed(5)}, ${lng.toFixed(5)}`,
      display_name: `${lat.toFixed(5)}, ${lng.toFixed(5)}`,
      place: { title: "Punto seleccionado", name: "Punto seleccionado", address: `${lat.toFixed(5)}, ${lng.toFixed(5)}`, lat, lng },
      results: [],
      warning: String(error?.message || error),
    };
    return safeJson(res, 200, ptyFastV5CacheSet(cacheKey, payload, 15000));
  }
}

async function ptyFastV5Route(req, res) {
  const body = req.body || {};
  const origin = body.origin || body.from || {
    lat: req.query.originLat || req.query.lat1 || req.query.fromLat,
    lng: req.query.originLng || req.query.lng1 || req.query.fromLng,
  };
  const destination = body.destination || body.to || {
    lat: req.query.destinationLat || req.query.lat2 || req.query.toLat,
    lng: req.query.destinationLng || req.query.lng2 || req.query.toLng,
  };
  const fallback = ptyFastV5FallbackRoute(origin, destination);

  const cacheKey = `route:${fallback.coordinates.map((p) => `${p.lat.toFixed(5)},${p.lng.toFixed(5)}`).join("|")}`;
  const cached = ptyFastV5CacheGet(cacheKey);
  if (cached) return safeJson(res, 200, cached);

  try {
    const key = ptyFastV5GoogleKey();
    if (!key || !fallback.coordinates.length) return safeJson(res, 200, fallback);

    const o = ptyFastV5Point(origin);
    const d = ptyFastV5Point(destination);
    const { ok, json } = await ptyFastV5FetchJson("https://routes.googleapis.com/directions/v2:computeRoutes", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": key,
        "X-Goog-FieldMask": "routes.duration,routes.distanceMeters,routes.polyline.encodedPolyline",
      },
      body: JSON.stringify({
        origin: { location: { latLng: { latitude: o.lat, longitude: o.lng } } },
        destination: { location: { latLng: { latitude: d.lat, longitude: d.lng } } },
        travelMode: "DRIVE",
        routingPreference: "TRAFFIC_AWARE",
        computeAlternativeRoutes: false,
        languageCode: "es",
        units: "METRIC",
        polylineQuality: "OVERVIEW",
        polylineEncoding: "ENCODED_POLYLINE",
      }),
    }, Number(process.env.PTY_ROUTE_TIMEOUT_MS || 2500));

    if (!ok || !json?.routes?.[0]) return safeJson(res, 200, fallback);
    const route = json.routes[0];
    const encodedPolyline = route.polyline?.encodedPolyline || "";
    const distanceMeters = Number(route.distanceMeters || fallback.distanceMeters || 0);
    const seconds = Number(String(route.duration || "0s").replace(/s$/, "") || fallback.durationSeconds || 0);
    const payload = {
      ...fallback,
      provider: "google_routes_fast",
      fallback: false,
      encodedPolyline,
      distanceMeters,
      distanceKm: Number((distanceMeters / 1000).toFixed(3)),
      durationSeconds: seconds,
      durationMin: Math.max(1, Math.round(seconds / 60)),
    };
    return safeJson(res, 200, ptyFastV5CacheSet(cacheKey, payload, 60000));
  } catch (error) {
    return safeJson(res, 200, { ...fallback, warning: String(error?.message || error) });
  }
}

function ptyFastV5EmitRide(row, event = "ride:update") {
  const ride = normalizeRide(row);
  try {
    emitRide(row, event);
    io.to(`ride:${ride.id}`).emit(event, ride);
    io.to(`ride:${ride.id}`).emit("ride:update", ride);
    if (event !== "ride:update") {
      io.to("drivers").emit("ride:update", ride);
      if (ride.riderId) emitToUser(ride.riderId, "ride:update", ride);
      if (ride.driverId) emitToUser(ride.driverId, "ride:update", ride);
    }
  } catch (error) {
    console.warn("[PTY_FAST_V5_EMIT_WARN]", error?.message || error);
  }
  return ride;
}

async function ptyFastV5CreateRide(req, res) {
  try {
    const user = req.user || {};
    if (!user?.id) return safeJson(res, 401, { ok: false, message: "Sesión requerida" });

    const pickup = normalizePoint(req.body.pickup || req.body.origin || req.body.from || {});
    const destination = normalizePoint(req.body.destination || req.body.destino || req.body.to || {});
    if (pickup.lat == null || pickup.lng == null || destination.lat == null || destination.lng == null) {
      return safeJson(res, 400, { ok: false, message: "pickup y destination con lat/lng son requeridos" });
    }

    const active = await db(
      `SELECT id, status
         FROM ride_rides
        WHERE rider_id::text=$1::text
          AND status = ANY($2::text[])
        ORDER BY updated_at DESC
        LIMIT 1`,
      [user.id, PTY_FAST_V5_ACTIVE_STATUSES]
    );
    if (active.rows.length) {
      return safeJson(res, 409, {
        ok: false,
        message: "Ya tienes una carrera activa. Cancélala o finalízala antes de solicitar otra.",
        activeRideId: active.rows[0].id,
      });
    }

    const distanceKm = Math.max(0, asNum(req.body.distanceKm ?? req.body.routeDistanceKm, ptyFastV5DistanceKm(pickup, destination)));
    const durationMin = Math.max(1, asNum(req.body.durationMin, Math.round((distanceKm / 28) * 60) || 1));
    const fare = Math.max(2.0, asNum(req.body.fare ?? req.body.price ?? req.body.total, 2));
    const route = req.body.route || {
      coordinates: [
        { lat: pickup.lat, lng: pickup.lng },
        { lat: destination.lat, lng: destination.lng },
      ],
      distanceKm,
      durationMin,
      provider: "fast_create",
    };
    const expiresMinutes = Math.max(1, Number(RIDE_EXPIRE_MINUTES || 10));

    const r = await db(
      `INSERT INTO ride_rides(
        rider_id, status, pickup, destination, route, fare, distance_km, duration_min,
        payment_method, rider_snapshot, expires_at, updated_at
       )
       VALUES(
        $1::uuid,'requested',$2::jsonb,$3::jsonb,$4::jsonb,$5,$6,$7,$8,$9::jsonb,
        NOW() + ($10::text || ' minutes')::interval, NOW()
       )
       RETURNING *`,
      [
        user.id,
        JSON.stringify(pickup),
        JSON.stringify(destination),
        JSON.stringify(route || {}),
        fare,
        distanceKm,
        durationMin,
        asText(req.body.paymentMethod || req.body.payment || "cash"),
        JSON.stringify(getSnapshotUser(user)),
        String(expiresMinutes),
      ]
    );

    const ride = ptyFastV5EmitRide(r.rows[0], "ride:new");
    io.to("drivers").emit("ride:available", ride);

    // Auditoría no debe bloquear al usuario.
    db(
      `INSERT INTO ride_events(ride_id, user_id, type, payload)
       VALUES($1,$2,'ride_requested',$3::jsonb)`,
      [r.rows[0].id, user.id, JSON.stringify({ fare, distanceKm, fast: true })]
    ).catch((e) => console.warn("[PTY_FAST_V5_EVENT_WARN]", e?.message || e));

    return safeJson(res, 201, { ok: true, ride, fast: true });
  } catch (error) {
    return safeJson(res, 500, { ok: false, message: String(error?.message || error) });
  }
}

async function ptyFastV5ListRides(req, res) {
  try {
    const status = asText(req.query.status || "");
    const role = asText(req.query.role || req.user?.role || "");
    const userId = asText(req.user?.id || req.query.userId || req.query.riderId || req.query.driverId || "");
    let r;

    if (status === "open" || status === "pending" || role === "driver") {
      r = await db(
        `SELECT * FROM ride_rides
          WHERE status IN ('requested','searching')
            AND expires_at > NOW()
          ORDER BY created_at DESC
          LIMIT 80`
      );
    } else if (role === "rider" && userId) {
      r = await db(
        `SELECT * FROM ride_rides
          WHERE rider_id::text=$1::text
          ORDER BY updated_at DESC
          LIMIT 80`,
        [userId]
      );
    } else if (role === "admin" || req.user?.role === "admin") {
      r = await db(`SELECT * FROM ride_rides ORDER BY updated_at DESC LIMIT 150`);
    } else if (userId) {
      r = await db(
        `SELECT * FROM ride_rides
          WHERE rider_id::text=$1::text OR driver_id::text=$1::text
          ORDER BY updated_at DESC
          LIMIT 80`,
        [userId]
      );
    } else {
      r = { rows: [] };
    }

    return safeJson(res, 200, { ok: true, rides: r.rows.map(normalizeRide), fast: true });
  } catch (error) {
    return safeJson(res, 500, { ok: false, message: String(error?.message || error), rides: [] });
  }
}

async function ptyFastV5ActiveRide(req, res) {
  try {
    const userId = asText(req.user?.id || req.query.userId || req.query.driverId || req.query.riderId || "");
    if (!userId) return safeJson(res, 200, { ok: true, ride: null, fast: true });
    const r = await db(
      `SELECT *
         FROM ride_rides
        WHERE (rider_id::text=$1::text OR driver_id::text=$1::text)
          AND status = ANY($2::text[])
        ORDER BY updated_at DESC NULLS LAST, created_at DESC
        LIMIT 1`,
      [userId, PTY_FAST_V5_ACTIVE_STATUSES]
    );
    return safeJson(res, 200, { ok: true, ride: r.rows[0] ? normalizeRide(r.rows[0]) : null, fast: true });
  } catch (error) {
    return safeJson(res, 500, { ok: false, message: String(error?.message || error), ride: null });
  }
}

async function ptyFastV5AcceptRide(req, res) {
  try {
    const rideId = asText(req.params.id || req.params.rideId || req.body.rideId);
    const driverId = asText(req.user?.id || req.body.driverId || req.body.userId || req.body.driver?.id || req.body.driver?.driverId || "");
    if (!rideId || !driverId) return safeJson(res, 400, { ok: false, message: "rideId/driverId requerido" });
    if (!isUuid(rideId) || !isUuid(driverId)) return safeJson(res, 400, { ok: false, message: "ID inválido" });

    const driverSnapshot = {
      ...ptyFastV5Snapshot(req.user || {}, "driver"),
      ...(req.body.driver && typeof req.body.driver === "object" ? req.body.driver : {}),
      id: driverId,
      role: "driver",
    };

    const r = await db(
      `UPDATE ride_rides
          SET driver_id=$2::uuid,
              status='accepted',
              accepted_at=COALESCE(accepted_at, NOW()),
              driver_snapshot=CASE
                WHEN driver_snapshot IS NULL OR driver_snapshot='{}'::jsonb THEN $3::jsonb
                ELSE driver_snapshot || $3::jsonb
              END,
              updated_at=NOW()
        WHERE id::text=$1::text
          AND status = ANY($4::text[])
          AND (driver_id IS NULL OR driver_id::text=$2::text)
        RETURNING *`,
      [rideId, driverId, JSON.stringify(driverSnapshot), ["requested", "searching", "accepted", "assigned"]]
    );

    if (!r.rows.length) {
      const existing = await db(`SELECT * FROM ride_rides WHERE id::text=$1::text LIMIT 1`, [rideId]).catch(() => ({ rows: [] }));
      return safeJson(res, 409, {
        ok: false,
        message: "La carrera ya no está disponible",
        ride: existing.rows[0] ? normalizeRide(existing.rows[0]) : null,
      });
    }

    const ride = ptyFastV5EmitRide(r.rows[0], "ride:accepted");
    ptyFastV5EmitRide(r.rows[0], "ride.accepted");
    return safeJson(res, 200, { ok: true, ride, status: "accepted", fast: true });
  } catch (error) {
    return safeJson(res, 500, { ok: false, message: String(error?.message || error) });
  }
}

function ptyFastV5StatusFromAction(action, body = {}) {
  const raw = asText(body.status || body.nextStatus || action).toLowerCase();
  if (["start", "started", "iniciar", "in_progress", "progress"].includes(raw)) return "in_progress";
  if (["arrive", "arrived", "llegar", "llegue"].includes(raw)) return "arrived";
  if (["complete", "completed", "finish", "finished", "finalizar", "finalizada"].includes(raw)) return "completed";
  if (["cancel", "cancelled", "canceled", "cancelar"].includes(raw)) {
    const role = asText(body.role || "").toLowerCase();
    if (role === "driver") return "driver_cancelled";
    if (role === "rider") return "rider_cancelled";
    return "cancelled";
  }
  if (["accepted", "assigned"].includes(raw)) return raw;
  return raw || "in_progress";
}

function ptyFastV5EventFromStatus(status) {
  if (status === "in_progress") return "ride:started";
  if (status === "arrived") return "ride:arrived";
  if (status === "completed") return "ride:completed";
  if (status.includes("cancel")) return "ride:cancelled";
  if (status === "accepted" || status === "assigned") return "ride:accepted";
  return "ride:update";
}

async function ptyFastV5PatchStatus(req, res, forcedAction = "") {
  try {
    const rideId = asText(req.params.id || req.params.rideId || req.body.rideId);
    const userId = asText(req.user?.id || req.body.userId || req.body.driverId || req.body.riderId || "");
    const status = ptyFastV5StatusFromAction(forcedAction, req.body || {});
    if (!rideId || !isUuid(rideId)) return safeJson(res, 400, { ok: false, message: "rideId inválido" });
    if (!PTY_FAST_V5_ACTIVE_STATUSES.includes(status) && !PTY_FAST_V5_FINAL_STATUSES.includes(status)) {
      return safeJson(res, 400, { ok: false, message: `status inválido: ${status}` });
    }

    const stampSql =
      status === "in_progress" ? ", started_at=COALESCE(started_at,NOW())" :
      status === "completed" ? ", completed_at=COALESCE(completed_at,NOW())" :
      status.includes("cancel") ? ", cancelled_at=COALESCE(cancelled_at,NOW()), cancel_reason=COALESCE(NULLIF($4,''), cancel_reason)" :
      "";

    const r = await db(
      `UPDATE ride_rides
          SET status=$2,
              updated_at=NOW()
              ${stampSql}
        WHERE id::text=$1::text
          AND ($3::text='' OR rider_id::text=$3::text OR driver_id::text=$3::text OR $5::text='admin')
        RETURNING *`,
      [
        rideId,
        status,
        userId,
        asText(req.body.reason || req.body.cancelReason || ""),
        asText(req.user?.role || req.body.role || ""),
      ]
    );

    if (!r.rows.length) {
      return safeJson(res, 404, { ok: false, message: "Carrera no encontrada o no autorizada" });
    }

    const event = ptyFastV5EventFromStatus(status);
    const ride = ptyFastV5EmitRide(r.rows[0], event);
    db(
      `INSERT INTO ride_events(ride_id, user_id, type, payload)
       VALUES($1, NULLIF($2,'')::uuid, $3, $4::jsonb)`,
      [rideId, userId, event.replace(":", "_"), JSON.stringify({ status, fast: true })]
    ).catch(() => null);

    return safeJson(res, 200, { ok: true, ride, status, fast: true });
  } catch (error) {
    return safeJson(res, 500, { ok: false, message: String(error?.message || error) });
  }
}

async function ptyFastV5Location(req, res, role) {
  try {
    const userId = asText(req.user?.id || req.body.userId || req.body.driverId || req.body.riderId || req.query.userId || "");
    const point = ptyFastV5Point(req.body.location || req.body.currentLocation || req.body || req.query);
    if (!userId || !point) return safeJson(res, 400, { ok: false, message: "userId/location requerido" });

    const heading = ptyFastV5Num(req.body.heading ?? req.query.heading);
    const speed = ptyFastV5Num(req.body.speed ?? req.query.speed);

    await db(
      `INSERT INTO ride_locations(user_id, role, lat, lng, heading, speed, updated_at)
       VALUES($1::uuid,$2,$3,$4,$5,$6,NOW())
       ON CONFLICT(user_id, role) DO UPDATE SET
         lat=EXCLUDED.lat,
         lng=EXCLUDED.lng,
         heading=EXCLUDED.heading,
         speed=EXCLUDED.speed,
         updated_at=NOW()`,
      [userId, role, point.lat, point.lng, heading, speed]
    );

    const payload = { ok: true, userId, role, lat: point.lat, lng: point.lng, heading, speed, updatedAt: nowIso(), fast: true };
    io.emit("location:update", payload);
    return safeJson(res, 200, payload);
  } catch (error) {
    return safeJson(res, 500, { ok: false, message: String(error?.message || error) });
  }
}

app.get("/api/perf/ping", async (_req, res) => {
  const started = Date.now();
  let dbMs = null;
  let dbOk = false;
  try {
    const t = Date.now();
    await db("SELECT 1");
    dbMs = Date.now() - t;
    dbOk = true;
  } catch {}
  return safeJson(res, 200, {
    ok: true,
    pong: true,
    fastPatch: "v11-clean-chat",
    db: dbOk ? "on" : "off",
    dbMs,
    totalMs: Date.now() - started,
    time: nowIso(),
  });
});

// Lugares y rutas rápidos: evitan bloquear la app por Google/Routes.
app.get("/api/places/search", ptyFastV5AuthOptional, ptyFastV5PlacesSearch);
app.get("/api/places/autocomplete", ptyFastV5AuthOptional, ptyFastV5PlacesSearch);
app.get("/api/google/places/autocomplete", ptyFastV5AuthOptional, ptyFastV5PlacesSearch);
app.get("/api/places/reverse", ptyFastV5AuthOptional, ptyFastV5Reverse);
app.get("/api/geocode/reverse", ptyFastV5AuthOptional, ptyFastV5Reverse);
app.get("/api/google/geocode/reverse", ptyFastV5AuthOptional, ptyFastV5Reverse);
app.get("/api/routes/drive", ptyFastV5AuthOptional, ptyFastV5Route);
app.post("/api/routes/drive", ptyFastV5AuthOptional, ptyFastV5Route);
app.get("/api/google/routes/drive", ptyFastV5AuthOptional, ptyFastV5Route);
app.post("/api/google/routes/drive", ptyFastV5AuthOptional, ptyFastV5Route);
app.get("/api/directions/drive", ptyFastV5AuthOptional, ptyFastV5Route);
app.post("/api/directions/drive", ptyFastV5AuthOptional, ptyFastV5Route);

// Rutas críticas de carrera.
app.post("/api/rides", ptyFastV5AuthRequired, ptyFastV5CreateRide);
app.get("/api/rides", ptyFastV5AuthOptional, ptyFastV5ListRides);
app.get("/api/rides/active", ptyFastV5AuthOptional, ptyFastV5ActiveRide);
app.patch("/api/rides/:id/accept", ptyFastV5AuthOptional, ptyFastV5AcceptRide);
app.post("/api/rides/:id/accept", ptyFastV5AuthOptional, ptyFastV5AcceptRide);
app.patch("/api/carrera-lite/:id/accept", ptyFastV5AuthOptional, ptyFastV5AcceptRide);
app.patch("/api/carreras/:id/accept", ptyFastV5AuthOptional, ptyFastV5AcceptRide);
app.patch("/api/rides/:id/start", ptyFastV5AuthOptional, (req, res) => ptyFastV5PatchStatus(req, res, "start"));
app.post("/api/rides/:id/start", ptyFastV5AuthOptional, (req, res) => ptyFastV5PatchStatus(req, res, "start"));
app.patch("/api/rides/:id/arrive", ptyFastV5AuthOptional, (req, res) => ptyFastV5PatchStatus(req, res, "arrive"));
app.post("/api/rides/:id/arrive", ptyFastV5AuthOptional, (req, res) => ptyFastV5PatchStatus(req, res, "arrive"));
app.patch("/api/rides/:id/complete", ptyFastV5AuthOptional, (req, res) => ptyFastV5PatchStatus(req, res, "complete"));
app.post("/api/rides/:id/complete", ptyFastV5AuthOptional, (req, res) => ptyFastV5PatchStatus(req, res, "complete"));
app.patch("/api/rides/:id/cancel", ptyFastV5AuthOptional, (req, res) => ptyFastV5PatchStatus(req, res, "cancel"));
app.post("/api/rides/:id/cancel", ptyFastV5AuthOptional, (req, res) => ptyFastV5PatchStatus(req, res, "cancel"));
app.patch("/api/rides/:id/status", ptyFastV5AuthOptional, (req, res) => ptyFastV5PatchStatus(req, res, ""));
app.post("/api/rides/:id/status", ptyFastV5AuthOptional, (req, res) => ptyFastV5PatchStatus(req, res, ""));
app.patch("/api/rides/:id/fast-status", ptyFastV5AuthOptional, (req, res) => ptyFastV5PatchStatus(req, res, ""));
app.post("/api/rides/:id/fast-status", ptyFastV5AuthOptional, (req, res) => ptyFastV5PatchStatus(req, res, ""));

// Ubicación rápida para evitar lag del mapa.
app.post("/api/driver/location", ptyFastV5AuthOptional, (req, res) => ptyFastV5Location(req, res, "driver"));
app.patch("/api/driver/location", ptyFastV5AuthOptional, (req, res) => ptyFastV5Location(req, res, "driver"));
app.post("/api/rider/location", ptyFastV5AuthOptional, (req, res) => ptyFastV5Location(req, res, "rider"));
app.patch("/api/rider/location", ptyFastV5AuthOptional, (req, res) => ptyFastV5Location(req, res, "rider"));

/* FIN PTY DRIVE BACKEND PERFORMANCE HOTFIX V5 */

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








/* PTY BACKEND CHAT SOS V15 REUPLOAD */
const ptyV15AuthOptional = typeof authOptional === "function" ? authOptional : ((req, _res, next) => next());

function ptyV15Text(v = "") { return String(v ?? "").trim(); }
function ptyV15Json(v, fallback = {}) {
  if (!v) return fallback;
  if (typeof v === "object") return v;
  try { return JSON.parse(v); } catch { return fallback; }
}
function ptyV15Point(v = {}) {
  const raw = ptyV15Json(v, v || {});
  const lat = Number(raw.lat ?? raw.latitude);
  const lng = Number(raw.lng ?? raw.lon ?? raw.longitude);
  return Number.isFinite(lat) && Number.isFinite(lng) ? { ...raw, lat, lng } : raw;
}
function ptyV15SafeJson(res, status, payload) {
  try { if (typeof safeJson === "function") return safeJson(res, status, payload); } catch {}
  return res.status(status).json(payload);
}
function ptyV15ThreadKey({ userId = "", userEmail = "", rideId = "", role = "" } = {}) {
  return [ptyV15Text(userId || userEmail || "anon").toLowerCase(), ptyV15Text(rideId || "general").toLowerCase(), ptyV15Text(role || "user").toLowerCase()].join(":");
}

async function ptyV15EnsureTables() {
  if (globalThis.__PTY_V15_CHAT_SOS_READY__) return;

  await db(`
    CREATE TABLE IF NOT EXISTS ride_support_threads (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      thread_key TEXT UNIQUE,
      user_id TEXT DEFAULT '',
      user_email TEXT DEFAULT '',
      user_name TEXT DEFAULT '',
      user_phone TEXT DEFAULT '',
      role TEXT DEFAULT '',
      ride_id TEXT DEFAULT '',
      status TEXT DEFAULT 'open',
      title TEXT DEFAULT '',
      last_message TEXT DEFAULT '',
      last_at TIMESTAMPTZ DEFAULT NOW(),
      meta JSONB DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
  await db(`
    CREATE TABLE IF NOT EXISTS ride_support_messages (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      thread_id UUID REFERENCES ride_support_threads(id) ON DELETE CASCADE,
      user_id TEXT DEFAULT '',
      sender_id TEXT DEFAULT '',
      sender_role TEXT DEFAULT '',
      sender_name TEXT DEFAULT '',
      author TEXT DEFAULT '',
      text TEXT DEFAULT '',
      meta JSONB DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
  await db(`CREATE INDEX IF NOT EXISTS idx_pty_v15_support_messages_thread ON ride_support_messages(thread_id, created_at);`);
  await db(`CREATE INDEX IF NOT EXISTS idx_pty_v15_support_threads_user ON ride_support_threads(user_id, ride_id, updated_at DESC);`);
  await db(`CREATE INDEX IF NOT EXISTS idx_pty_v15_support_threads_status ON ride_support_threads(status, updated_at DESC);`);

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
      thread_id TEXT DEFAULT '',
      meta JSONB DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
  await db(`ALTER TABLE ride_sos_alerts ADD COLUMN IF NOT EXISTS thread_id TEXT DEFAULT '';`).catch(() => null);
  await db(`ALTER TABLE ride_sos_alerts ADD COLUMN IF NOT EXISTS meta JSONB DEFAULT '{}'::jsonb;`).catch(() => null);
  await db(`CREATE INDEX IF NOT EXISTS idx_pty_v15_sos_status ON ride_sos_alerts(status, created_at DESC);`);

  globalThis.__PTY_V15_CHAT_SOS_READY__ = true;
}

async function ptyV15UpsertThread({ userId = "", userEmail = "", userName = "", userPhone = "", role = "", rideId = "", title = "", lastMessage = "", meta = {} } = {}) {
  await ptyV15EnsureTables();
  const key = ptyV15ThreadKey({ userId, userEmail, rideId, role });
  const r = await db(`
    INSERT INTO ride_support_threads(thread_key,user_id,user_email,user_name,user_phone,role,ride_id,title,last_message,last_at,meta,updated_at)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW(),$10::jsonb,NOW())
    ON CONFLICT(thread_key) DO UPDATE SET
      user_id=COALESCE(NULLIF(EXCLUDED.user_id,''), ride_support_threads.user_id),
      user_email=COALESCE(NULLIF(EXCLUDED.user_email,''), ride_support_threads.user_email),
      user_name=COALESCE(NULLIF(EXCLUDED.user_name,''), ride_support_threads.user_name),
      user_phone=COALESCE(NULLIF(EXCLUDED.user_phone,''), ride_support_threads.user_phone),
      role=COALESCE(NULLIF(EXCLUDED.role,''), ride_support_threads.role),
      ride_id=COALESCE(NULLIF(EXCLUDED.ride_id,''), ride_support_threads.ride_id),
      title=COALESCE(NULLIF(EXCLUDED.title,''), ride_support_threads.title),
      last_message=COALESCE(NULLIF(EXCLUDED.last_message,''), ride_support_threads.last_message),
      last_at=NOW(),
      updated_at=NOW(),
      meta=ride_support_threads.meta || EXCLUDED.meta
    RETURNING *`,
    [key, userId, userEmail, userName, userPhone, role, rideId, title || "Soporte PTY", lastMessage, JSON.stringify(meta || {})]
  );
  return r.rows[0];
}

function ptyV15NormalizeSupportThread(row = {}) {
  return {
    id: String(row.id || ""),
    threadId: String(row.id || ""),
    type: "support",
    title: row.title || row.user_name || row.user_email || "Soporte",
    userId: row.user_id || "",
    userEmail: row.user_email || "",
    userName: row.user_name || "",
    userPhone: row.user_phone || "",
    role: row.role || "",
    rideId: row.ride_id || "",
    status: row.status || "open",
    lastMessage: row.last_message || "",
    updatedAt: row.updated_at || row.last_at || row.created_at,
    createdAt: row.created_at,
    unread: true,
  };
}

function ptyV15NormalizeSupportMessage(row = {}, thread = {}) {
  return {
    id: String(row.id || ""),
    threadId: String(row.thread_id || thread.id || ""),
    userId: row.user_id || thread.user_id || "",
    senderId: row.sender_id || "",
    senderRole: row.sender_role || "",
    senderName: row.sender_name || "",
    author: row.author || row.sender_name || "Usuario",
    text: row.text || "",
    message: row.text || "",
    createdAt: row.created_at,
    at: row.created_at,
  };
}

async function ptyV15AddSupportMessage({ thread, userId = "", senderId = "", senderRole = "", senderName = "", author = "", text = "", meta = {} } = {}) {
  const clean = ptyV15Text(text);
  if (!clean || !thread?.id) return null;
  const r = await db(`
    INSERT INTO ride_support_messages(thread_id,user_id,sender_id,sender_role,sender_name,author,text,meta)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
    RETURNING *`,
    [thread.id, userId || thread.user_id || "", senderId, senderRole, senderName, author || senderName || senderRole || "Usuario", clean, JSON.stringify(meta || {})]
  );
  await db(`UPDATE ride_support_threads SET last_message=$2,last_at=NOW(),updated_at=NOW() WHERE id=$1`, [thread.id, clean]).catch(() => null);
  const msg = ptyV15NormalizeSupportMessage(r.rows[0], thread);
  try {
    io.to("admins").emit("support.message", { thread: ptyV15NormalizeSupportThread(thread), message: msg });
    if (thread.user_id) io.to(`user:${thread.user_id}`).emit("support.message", { thread: ptyV15NormalizeSupportThread(thread), message: msg });
    io.emit("support.message", { thread: ptyV15NormalizeSupportThread(thread), message: msg });
  } catch {}
  return msg;
}

app.post("/api/support/messages", ptyV15AuthOptional, async (req, res) => {
  try {
    const user = req.user || {};
    const body = req.body || {};
    const userId = ptyV15Text(body.userId || user.id || "");
    const userEmail = ptyV15Text(body.userEmail || user.email || "");
    const role = ptyV15Text(body.role || user.role || "");
    const text = ptyV15Text(body.message || body.text || body.detail || "");
    const thread = await ptyV15UpsertThread({
      userId,
      userEmail,
      userName: body.userName || user.name || "",
      userPhone: body.userPhone || user.phone || "",
      role,
      rideId: body.rideId || "",
      title: body.title || "Soporte PTY",
      lastMessage: text,
      meta: body,
    });
    const message = await ptyV15AddSupportMessage({
      thread,
      userId,
      senderId: userId,
      senderRole: role || "user",
      senderName: body.userName || user.name || (role === "driver" ? "Conductor" : "Usuario"),
      author: body.author || body.userName || user.name || (role === "driver" ? "Conductor" : "Usuario"),
      text,
      meta: body,
    });
    return ptyV15SafeJson(res, 201, { ok: true, thread: ptyV15NormalizeSupportThread(thread), message });
  } catch (e) {
    return ptyV15SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/support/messages", ptyV15AuthOptional, async (req, res) => {
  try {
    await ptyV15EnsureTables();
    const userId = ptyV15Text(req.query.userId || req.user?.id || "");
    const rideId = ptyV15Text(req.query.rideId || "");
    const params = [userId];
    let where = `WHERE (t.user_id::text=$1::text OR $1::text='')`;
    if (rideId) { params.push(rideId); where += ` AND t.ride_id::text=$2::text`; }
    const r = await db(`
      SELECT m.*, t.user_id AS t_user_id
      FROM ride_support_messages m
      JOIN ride_support_threads t ON t.id=m.thread_id
      ${where}
      ORDER BY m.created_at ASC
      LIMIT 500`, params);
    const messages = r.rows.map((row) => ptyV15NormalizeSupportMessage(row, { user_id: row.t_user_id }));
    return ptyV15SafeJson(res, 200, { ok: true, messages, data: messages });
  } catch (e) {
    return ptyV15SafeJson(res, 500, { ok: false, message: String(e?.message || e), messages: [] });
  }
});

app.post("/api/admin/support/messages", ptyV15AuthOptional, async (req, res) => {
  try {
    const body = req.body || {};
    const text = ptyV15Text(body.message || body.text || body.detail || "");
    const thread = await ptyV15UpsertThread({
      userId: body.userId || "",
      userEmail: body.userEmail || "",
      userName: body.userName || "",
      userPhone: body.userPhone || "",
      role: body.role || "",
      rideId: body.rideId || "",
      title: body.title || "Soporte PTY",
      lastMessage: text,
      meta: body,
    });
    const message = await ptyV15AddSupportMessage({
      thread,
      userId: body.userId || "",
      senderId: body.senderId || "admin",
      senderRole: body.senderRole || "admin",
      senderName: body.senderName || "PTY Drive",
      author: body.author || "PTY Drive",
      text,
      meta: body,
    });
    return ptyV15SafeJson(res, 201, { ok: true, thread: ptyV15NormalizeSupportThread(thread), message });
  } catch (e) {
    return ptyV15SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/admin/support/threads", ptyV15AuthOptional, async (_req, res) => {
  try {
    await ptyV15EnsureTables();
    const r = await db(`SELECT * FROM ride_support_threads ORDER BY updated_at DESC LIMIT 300`);
    const threads = r.rows.map(ptyV15NormalizeSupportThread);
    return ptyV15SafeJson(res, 200, { ok: true, threads, reports: threads });
  } catch (e) {
    return ptyV15SafeJson(res, 500, { ok: false, message: String(e?.message || e), threads: [] });
  }
});

app.get("/api/admin/chats", ptyV15AuthOptional, async (_req, res) => {
  try {
    await ptyV15EnsureTables();
    const supportR = await db(`SELECT * FROM ride_support_threads ORDER BY updated_at DESC LIMIT 200`).catch(() => ({ rows: [] }));
    const rideR = await db(`
      SELECT ride_id,
             MAX(created_at) AS updated_at,
             COUNT(*) AS count,
             (ARRAY_AGG(text ORDER BY created_at DESC))[1] AS last_message
      FROM ride_chat_messages
      GROUP BY ride_id
      ORDER BY updated_at DESC
      LIMIT 200`).catch(() => ({ rows: [] }));

    const supportChats = supportR.rows.map((row) => ({ ...ptyV15NormalizeSupportThread(row), id: `support:${row.id}`, chatType: "support" }));
    const rideChats = rideR.rows.map((row) => ({
      id: `ride:${row.ride_id}`,
      rideId: row.ride_id,
      chatType: "ride",
      title: `Viaje ${String(row.ride_id).slice(-6)}`,
      lastMessage: row.last_message || "",
      updatedAt: row.updated_at,
      count: Number(row.count || 0),
      unread: true,
    }));
    const chats = [...supportChats, ...rideChats].sort((a,b)=> new Date(b.updatedAt || 0) - new Date(a.updatedAt || 0));
    return ptyV15SafeJson(res, 200, { ok: true, chats, threads: chats });
  } catch (e) {
    return ptyV15SafeJson(res, 500, { ok: false, message: String(e?.message || e), chats: [] });
  }
});

async function ptyV15CreateSos(req, res) {
  try {
    await ptyV15EnsureTables();
    const user = req.user || {};
    const body = req.body || {};
    const userId = ptyV15Text(body.userId || user.id || "");
    const role = ptyV15Text(body.role || user.role || "");
    const msg = ptyV15Text(body.message || "SOS activado desde la app");
    const thread = await ptyV15UpsertThread({
      userId,
      userEmail: body.userEmail || user.email || "",
      userName: body.userName || user.name || "",
      userPhone: body.userPhone || user.phone || "",
      role,
      rideId: body.rideId || "",
      title: "🚨 SOS",
      lastMessage: msg,
      meta: body,
    });
    await ptyV15AddSupportMessage({
      thread,
      userId,
      senderId: userId,
      senderRole: role || "user",
      senderName: body.userName || user.name || "Usuario",
      author: body.userName || user.name || "Usuario",
      text: msg,
      meta: { sos: true, ...body },
    });
    const r = await db(`
      INSERT INTO ride_sos_alerts(user_id,user_email,user_name,user_phone,role,ride_id,message,status,location,trusted_contact,ride,thread_id,meta)
      VALUES($1,$2,$3,$4,$5,$6,$7,'open',$8::jsonb,$9::jsonb,$10::jsonb,$11,$12::jsonb)
      RETURNING *`,
      [
        userId,
        body.userEmail || user.email || "",
        body.userName || user.name || "",
        body.userPhone || user.phone || "",
        role,
        body.rideId || "",
        msg,
        JSON.stringify(ptyV15Point(body.location || body || {})),
        JSON.stringify(body.trustedContact || {}),
        JSON.stringify(body.ride || {}),
        String(thread.id),
        JSON.stringify(body),
      ]
    );
    const alert = { ...r.rows[0], threadId: String(thread.id), location: ptyV15Json(r.rows[0].location, {}) };
    try { io.to("admins").emit("sos.alert", { alert }); io.emit("sos.alert", { alert }); } catch {}
    return ptyV15SafeJson(res, 201, { ok: true, alert, thread: ptyV15NormalizeSupportThread(thread) });
  } catch (e) {
    return ptyV15SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
}
app.post("/api/sos", ptyV15AuthOptional, ptyV15CreateSos);
app.post("/api/security/sos", ptyV15AuthOptional, ptyV15CreateSos);

async function ptyV15ListSos(_req, res) {
  try {
    await ptyV15EnsureTables();
    const r = await db(`SELECT * FROM ride_sos_alerts ORDER BY created_at DESC LIMIT 200`);
    const alerts = r.rows.map((a) => ({ ...a, threadId: a.thread_id || "", location: ptyV15Json(a.location, {}) }));
    return ptyV15SafeJson(res, 200, { ok: true, alerts, sosAlerts: alerts });
  } catch (e) {
    return ptyV15SafeJson(res, 500, { ok: false, message: String(e?.message || e), alerts: [] });
  }
}
app.get("/api/admin/sos", ptyV15AuthOptional, ptyV15ListSos);
app.get("/api/admin/security/sos", ptyV15AuthOptional, ptyV15ListSos);

app.patch("/api/admin/sos/:id/status", ptyV15AuthOptional, async (req, res) => {
  try {
    await ptyV15EnsureTables();
    const status = ptyV15Text(req.body?.status || "resolved");
    const r = await db(`UPDATE ride_sos_alerts SET status=$2, updated_at=NOW() WHERE id::text=$1::text RETURNING *`, [req.params.id, status]);
    return ptyV15SafeJson(res, 200, { ok: true, alert: r.rows[0] || null });
  } catch (e) {
    return ptyV15SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.post("/api/admin/sos/:id/message", ptyV15AuthOptional, async (req, res) => {
  try {
    await ptyV15EnsureTables();
    const alertR = await db(`SELECT * FROM ride_sos_alerts WHERE id::text=$1::text LIMIT 1`, [req.params.id]);
    const alert = alertR.rows[0];
    if (!alert) return ptyV15SafeJson(res, 404, { ok: false, message: "SOS no encontrado" });
    let thread = null;
    if (alert.thread_id) {
      const tr = await db(`SELECT * FROM ride_support_threads WHERE id::text=$1::text LIMIT 1`, [alert.thread_id]).catch(() => ({ rows: [] }));
      thread = tr.rows[0] || null;
    }
    if (!thread) {
      thread = await ptyV15UpsertThread({
        userId: alert.user_id || "",
        userEmail: alert.user_email || "",
        userName: alert.user_name || "",
        userPhone: alert.user_phone || "",
        role: alert.role || "",
        rideId: alert.ride_id || "",
        title: "🚨 SOS",
        lastMessage: req.body?.message || "",
        meta: { sosId: req.params.id },
      });
      await db(`UPDATE ride_sos_alerts SET thread_id=$2 WHERE id::text=$1::text`, [req.params.id, thread.id]).catch(() => null);
    }
    const message = await ptyV15AddSupportMessage({
      thread,
      userId: alert.user_id || "",
      senderId: "admin",
      senderRole: "admin",
      senderName: "PTY Drive",
      author: "PTY Drive",
      text: req.body?.message || "",
      meta: { sosId: req.params.id, fromAdmin: true },
    });
    return ptyV15SafeJson(res, 201, { ok: true, thread: ptyV15NormalizeSupportThread(thread), message });
  } catch (e) {
    return ptyV15SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

/* PTY BACKEND FAST CORE V9 - LIGHTWEIGHT ROUTES */
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
    label: [typeLabel, brand, model, year, plate ? `Placa ${plate}` : ""].filter(Boolean).join(" · ") || "Vehículo",
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
    `SELECT rr.*,
            d.name AS driver_name, d.email AS driver_email, d.phone AS driver_phone,
            d.marker_icon AS driver_marker_icon, d.driver_docs AS driver_docs,
            r.name AS rider_name, r.email AS rider_email, r.phone AS rider_phone,
            r.marker_icon AS rider_marker_icon
       FROM ride_rides rr
       LEFT JOIN ride_users d ON d.id = rr.driver_id
       LEFT JOIN ride_users r ON r.id = rr.rider_id
      WHERE rr.id::text=$1::text
      LIMIT 1`,
    [String(rideId)]
  );
  return r.rows?.[0] || null;
}
function ptyV9EmitRide(ride, event = "ride:update") {
  try {
    io.to("admins").emit(event, ride);
    io.to("drivers").emit(event, ride);
    if (ride?.riderId) io.to(`user:${ride.riderId}`).emit(event, ride);
    if (ride?.driverId) io.to(`user:${ride.driverId}`).emit(event, ride);
    if (ride?.id) io.to(`ride:${ride.id}`).emit(event, ride);
  } catch {}
}
async function ptyV9EnsureIndexes() {
  if (globalThis.__PTY_V9_INDEXES_READY__) return;
  await db(`CREATE INDEX IF NOT EXISTS idx_pty_v9_rides_rider_status_updated ON ride_rides(rider_id, status, updated_at DESC)`).catch(() => null);
  await db(`CREATE INDEX IF NOT EXISTS idx_pty_v9_rides_driver_status_updated ON ride_rides(driver_id, status, updated_at DESC)`).catch(() => null);
  await db(`CREATE INDEX IF NOT EXISTS idx_pty_v9_rides_status_updated ON ride_rides(status, updated_at DESC)`).catch(() => null);
  await db(`CREATE INDEX IF NOT EXISTS idx_pty_v9_locations_role_updated ON ride_locations(role, updated_at DESC)`).catch(() => null);
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
      `SELECT id
         FROM ride_rides
        WHERE (rider_id::text=$1::text OR driver_id::text=$1::text)
          AND LOWER(COALESCE(status,'')) = ANY($2::text[])
        ORDER BY updated_at DESC NULLS LAST, created_at DESC
        LIMIT 1`,
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
    if (typeof isUuid === "function" && !isUuid(driverId)) return safeJson(res, 400, { ok: false, message: "driverId inválido" });
    if (typeof isUuid === "function" && !isUuid(rideId)) return safeJson(res, 400, { ok: false, message: "rideId inválido" });

    const upd = await db(
      `UPDATE ride_rides
          SET driver_id=$2::uuid,
              status='accepted',
              accepted_at=COALESCE(accepted_at, NOW()),
              updated_at=NOW()
        WHERE id::text=$1::text
          AND LOWER(COALESCE(status,'')) NOT IN ('completed','cancelled','expired','driver_cancelled','rider_cancelled','auto_cancelled')
          AND (driver_id IS NULL OR driver_id::text='' OR driver_id::text=$2::text)
        RETURNING id`,
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
      `UPDATE ride_rides
          SET status=$2,
              updated_at=NOW()
              ${stampField}
        WHERE id::text=$1::text
          AND ($3::text='' OR driver_id::text=$3::text OR rider_id::text=$3::text)
        RETURNING id`,
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
      `SELECT u.id, u.name, u.email, u.phone, u.marker_icon, u.role, u.driver_docs,
              l.lat, l.lng, l.heading, l.speed, l.updated_at
         FROM ride_locations l
         JOIN ride_users u ON u.id = l.user_id
        WHERE l.role='driver'
        ORDER BY l.updated_at DESC
        LIMIT 300`
    ).catch(() => ({ rows: [] }));
    const ridersR = await db(
      `SELECT u.id, u.name, u.email, u.phone, u.marker_icon, u.role, u.driver_docs,
              l.lat, l.lng, l.heading, l.speed, l.updated_at
         FROM ride_locations l
         JOIN ride_users u ON u.id = l.user_id
        WHERE l.role='rider'
        ORDER BY l.updated_at DESC
        LIMIT 300`
    ).catch(() => ({ rows: [] }));
    const ridesR = await db(
      `SELECT id FROM ride_rides
        WHERE LOWER(COALESCE(status,'')) = ANY($1::text[])
        ORDER BY updated_at DESC NULLS LAST, created_at DESC
        LIMIT 120`,
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

/* PTY BACKEND FAST PROFILE REVIEWS V8 - NO ROUTE TOUCH */
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
  if (globalThis.__PTY_V8_FAST_TABLES_READY__) return;
  await db(`
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
  `);
  await db(`CREATE INDEX IF NOT EXISTS idx_pty_v8_reviews_target ON ride_reviews(target_id, target_role, created_at DESC);`);
  await db(`CREATE INDEX IF NOT EXISTS idx_pty_v8_reviews_ride ON ride_reviews(ride_id);`);
  await db(`CREATE INDEX IF NOT EXISTS idx_pty_v8_rides_driver_status ON ride_rides(driver_id, status);`).catch(() => null);
  await db(`CREATE INDEX IF NOT EXISTS idx_pty_v8_rides_rider_status ON ride_rides(rider_id, status);`).catch(() => null);
  await db(`CREATE INDEX IF NOT EXISTS idx_pty_v8_rides_status_created ON ride_rides(status, created_at DESC);`).catch(() => null);
  globalThis.__PTY_V8_FAST_TABLES_READY__ = true;
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
    label: [typeLabel, brand, model, year ? String(year) : "", plate ? `Placa ${plate}` : ""].filter(Boolean).join(" · ") || "Vehículo",
  };
}

async function ptyV8DriverStats(driverId = "") {
  if (!driverId) return { rating: 5, reviewsCount: 0, reviews: [] };
  await ptyV8EnsureFastTables();
  const r = await db(
    `SELECT id, ride_id, reviewer_id, reviewer_role, rating, comment, created_at
     FROM ride_reviews
     WHERE target_id::text=$1::text AND target_role='driver'
     ORDER BY created_at DESC
     LIMIT 30`,
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
  const userR = await db(`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1`, [id]).catch(() => ({ rows: [] }));
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
    const rideR = await db(`SELECT * FROM ride_rides WHERE id::text=$1::text LIMIT 1`, [rideId]).catch(() => ({ rows: [] }));
    const ride = rideR.rows?.[0] || {};
    const targetId = targetRole === "driver" ? ptyV8Txt(ride.driver_id || req.body.targetId) : ptyV8Txt(ride.rider_id || req.body.targetId);
    const reviewerId = ptyV8Txt(req.user?.id || req.body.reviewerId || req.body.userId || "");
    const reviewerRole = ptyV8Txt(req.user?.role || req.body.reviewerRole || "");
    const ins = await db(
      `INSERT INTO ride_reviews(ride_id, reviewer_id, reviewer_role, target_id, target_role, rating, comment, meta)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
       RETURNING *`,
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
      `SELECT * FROM ride_rides
       WHERE (rider_id::text=$1::text OR driver_id::text=$1::text)
         AND LOWER(COALESCE(status,'')) = ANY($2::text[])
       ORDER BY updated_at DESC NULLS LAST, created_at DESC
       LIMIT 1`,
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
    if (typeof isUuid === "function" && !isUuid(driverId)) return ptyV8SafeJson(res, 400, { ok: false, message: "driverId inválido" });
    if (typeof isUuid === "function" && !isUuid(rideId)) return ptyV8SafeJson(res, 400, { ok: false, message: "rideId inválido" });

    const upd = await db(
      `UPDATE ride_rides
       SET driver_id=$2::uuid,
              status='accepted',
           updated_at=NOW()
       WHERE id::text=$1::text
         AND LOWER(COALESCE(status,'')) NOT IN ('completed','cancelled','expired','driver_cancelled','rider_cancelled')
         AND (driver_id IS NULL OR driver_id::text='' OR driver_id::text=$2::text)
       RETURNING *`,
      [rideId, driverId]
    );

    if (!upd.rows.length) return ptyV8SafeJson(res, 409, { ok: false, message: "La carrera ya no está disponible" });
    const ride = await ptyV8EnrichRideFast(upd.rows[0], { ...(req.body.driver || {}), id: driverId });
    try {
      io.to("admins").emit("ride.accepted", { ride, rideId, driverId });
      io.to(`ride:${rideId}`).emit("ride.accepted", { ride, rideId, driverId });
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
      `UPDATE ride_rides
       SET status=$2,
           updated_at=NOW()
       WHERE id::text=$1::text
         AND ($3::text='' OR driver_id::text=$3::text OR rider_id::text=$3::text)
       RETURNING *`,
      [rideId, status, driverId]
    );
    if (!upd.rows.length) return ptyV8SafeJson(res, 404, { ok: false, message: "Carrera no encontrada" });
    const ride = await ptyV8EnrichRideFast(upd.rows[0]);
    try { io.emit(`ride.${status}`, { ride, rideId }); io.to("admins").emit(`ride.${status}`, { ride, rideId }); } catch {}
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
      `UPDATE ride_users
       SET driver_docs=COALESCE(driver_docs,'{}'::jsonb) || $2::jsonb,
           updated_at=NOW()
       WHERE id::text=$1::text
       RETURNING *`,
      [driverId, JSON.stringify(patch)]
    );
    const user = r.rows?.[0] ? await ptyV8BuildDriver(driverId) : null;
    return ptyV8SafeJson(res, 200, { ok: true, user, driver: user, vehicle: user?.vehicle || vehicle });
  } catch (e) {
    return ptyV8SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
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
         SET driver_id=$2::uuid,
              status='accepted',
             updated_at=NOW()
         WHERE id::text=$1::text
         RETURNING *`,
        [rideId, driverId]
      );
    } catch {
      updated = await db(
        `UPDATE ride_rides
         SET driver_id=$2::uuid,
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
