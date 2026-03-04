// server.js (Mensajería Interna PRODIMA) — Notificaciones por Google Apps Script + fechas sin desfase
import express from "express";
import pg from "pg";
import jwt from "jsonwebtoken";
import crypto from "crypto";

const { Pool } = pg;

const app = express();
app.use(express.json({ limit: "2mb" }));

/* =========================
   ENV
========================= */
const {
  PORT = 3000,
  DATABASE_URL = "",
  JWT_SECRET = "change_me",

  ADMIN_USER = "PRODIMA",
  ADMIN_PASS = "ADMINISTRADOR",

  // CORS_ORIGIN=https://prodima.com.pa,https://www.prodima.com.pa
  CORS_ORIGIN = "",

  // ✅ Google Apps Script Webhook
  GAS_WEBHOOK_URL = "",
  GAS_WEBHOOK_SECRET = "",

  // ✅ Buzón/grupo mensajería
  COURIER_MAILBOX = "mensajeria@prodima.com.pa",

  // ✅ Supervisores
  SUPERVISOR_NOTIFY_TO = "logistica2@prodima.com.pa,melanie.choy@prodima.com.pa,malena.torrero@prodima.com.pa",
} = process.env;

/* =========================
   CORS ROBUSTO
========================= */
const ALLOWED_ORIGINS = new Set(
  String(CORS_ORIGIN || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)
);
const allowAll = ALLOWED_ORIGINS.size === 0;

app.use((req, res, next) => {
  const origin = req.headers.origin;

  if (allowAll && origin) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
  } else if (origin && ALLOWED_ORIGINS.has(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
  }

  res.setHeader("Access-Control-Allow-Methods", "GET,POST,PATCH,DELETE,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.setHeader("Access-Control-Max-Age", "86400");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

/* =========================
   DB (Supabase Postgres)
========================= */
const pool = new Pool({
  connectionString: DATABASE_URL || undefined,
  ssl: DATABASE_URL ? { rejectUnauthorized: false } : undefined,
});

function hasDb() {
  return Boolean(DATABASE_URL);
}
async function dbQuery(text, params = []) {
  return pool.query(text, params);
}

async function ensureDb() {
  if (!hasDb()) return;

  // Asegura zona horaria de la sesión DB (Panamá)
  pool.on("connect", (client) => {
    client.query("SET TIME ZONE 'America/Panama'").catch(() => {});
  });

  // Usuarios de mensajería (solicitantes + mensajeros)
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS msg_users (
      id BIGSERIAL PRIMARY KEY,
      username TEXT UNIQUE NOT NULL,
      full_name TEXT NOT NULL DEFAULT '',
      department TEXT NOT NULL DEFAULT '',
      role TEXT NOT NULL DEFAULT 'requester', -- requester | courier
      email TEXT DEFAULT '',
      phone TEXT DEFAULT '',
      pin_hash TEXT NOT NULL,
      is_active BOOLEAN DEFAULT TRUE,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_msg_users_dept ON msg_users(department);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_msg_users_role ON msg_users(role);`);

  // Solicitudes de mensajería
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS msg_requests (
      id BIGSERIAL PRIMARY KEY,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW(),

      created_by_user_id BIGINT NOT NULL,
      created_by_username TEXT NOT NULL DEFAULT '',
      requester_name TEXT NOT NULL DEFAULT '',
      requester_department TEXT NOT NULL DEFAULT '',
      requester_email TEXT DEFAULT '',
      requester_phone TEXT DEFAULT '',

      request_type TEXT NOT NULL DEFAULT '',
      contact_person_phone TEXT NOT NULL DEFAULT '',
      address_details TEXT NOT NULL DEFAULT '',
      description TEXT NOT NULL DEFAULT '',
      priority TEXT NOT NULL DEFAULT 'Media', -- Alta|Media|Baja

      status TEXT NOT NULL DEFAULT 'open', -- open|in_progress|closed|cancelled
      status_updated_at TIMESTAMP DEFAULT NOW(),

      assigned_to_user_id BIGINT,
      assigned_to_name TEXT DEFAULT '',
      assigned_at TIMESTAMP
    );
  `);

  // ✅ columnas comentario del mensajero + fecha cierre
  await dbQuery(`ALTER TABLE msg_requests ADD COLUMN IF NOT EXISTS courier_comment TEXT DEFAULT '';`);
  await dbQuery(`ALTER TABLE msg_requests ADD COLUMN IF NOT EXISTS closed_at TIMESTAMP;`);

  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_msg_requests_created_at ON msg_requests(created_at);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_msg_requests_status ON msg_requests(status);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_msg_requests_dept ON msg_requests(requester_department);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_msg_requests_created_by ON msg_requests(created_by_user_id);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_msg_requests_assigned_to ON msg_requests(assigned_to_user_id);`);
}

/* =========================
   Helpers
========================= */
function safeJson(res, status, obj) {
  res.status(status).json(obj);
}
function signToken(payload, ttl = "12h") {
  return jwt.sign(payload, JWT_SECRET, { expiresIn: ttl });
}
function readBearer(req) {
  const auth = String(req.headers.authorization || "");
  const m = auth.match(/^Bearer\s+(.+)$/i);
  return m ? m[1] : "";
}
function verifyAdmin(req, res, next) {
  const token = readBearer(req);
  if (!token) return safeJson(res, 401, { ok: false, message: "Missing Bearer token" });
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    if (decoded?.role !== "admin") return safeJson(res, 403, { ok: false, message: "Forbidden" });
    req.admin = decoded;
    next();
  } catch {
    return safeJson(res, 401, { ok: false, message: "Invalid token" });
  }
}
function verifyUser(req, res, next) {
  const token = readBearer(req);
  if (!token) return safeJson(res, 401, { ok: false, message: "Missing Bearer token" });
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    if (decoded?.role !== "user") return safeJson(res, 403, { ok: false, message: "Forbidden" });
    req.user = decoded;
    next();
  } catch {
    return safeJson(res, 401, { ok: false, message: "Invalid token" });
  }
}
function isCourierRole(role) {
  const r = String(role || "").trim().toLowerCase();
  return r === "courier" || r === "mercaderista";
}

/* =========================
   PIN hashing (crypto pbkdf2)
========================= */
const PIN_ITER = 120_000;

function hashPin(pin) {
  const p = String(pin || "").trim();
  if (p.length < 4) throw new Error("PIN muy corto (mín 4).");
  const salt = crypto.randomBytes(16).toString("hex");
  const dk = crypto.pbkdf2Sync(p, salt, PIN_ITER, 32, "sha256").toString("hex");
  return `pbkdf2$${PIN_ITER}$${salt}$${dk}`;
}
function verifyPin(pin, stored) {
  const p = String(pin || "").trim();
  const s = String(stored || "");
  const parts = s.split("$");
  if (parts.length !== 4 || parts[0] !== "pbkdf2") return false;
  const iter = Number(parts[1] || 0);
  const salt = String(parts[2] || "");
  const hash = String(parts[3] || "");
  if (!iter || !salt || !hash) return false;

  const dk = crypto.pbkdf2Sync(p, salt, iter, 32, "sha256").toString("hex");
  return crypto.timingSafeEqual(Buffer.from(dk, "hex"), Buffer.from(hash, "hex"));
}

/* =========================
   Normalizadores
========================= */
function normStatus(s) {
  const t = String(s || "").trim().toLowerCase();
  if (["open", "abierta", "abierto"].includes(t)) return "open";
  if (["in_progress", "en progreso", "progreso", "in progress"].includes(t)) return "in_progress";
  if (["closed", "cerrada", "cerrado"].includes(t)) return "closed";
  if (["cancelled", "canceled", "cancelada", "cancelado"].includes(t)) return "cancelled";
  return t;
}
function isISODate(s) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(s || ""));
}

/* =========================
   FECHAS “LOCAL PANAMÁ” (evita desfase 5 horas)
   Retornamos strings sin "Z" para que el frontend no las interprete como UTC.
========================= */
const SQL_DT_FMT = `YYYY-MM-DD"T"HH24:MI:SS`;
function fmtTsSql(col) {
  return `to_char(${col}, '${SQL_DT_FMT}')`;
}

/* =========================
   EMAIL NOTIFICATIONS via Google Apps Script (Webhook)
========================= */
function isEmail(s) {
  return /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i.test(String(s || "").trim());
}

function parseEmailList(csv) {
  return String(csv || "")
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter((x) => isEmail(x));
}

// Fetch helper: Node 18+ tiene fetch; Node <18 usa node-fetch si lo instalas.
async function _getFetch() {
  if (typeof fetch !== "undefined") return fetch;
  const mod = await import("node-fetch"); // npm i node-fetch
  return mod.default;
}

async function getActiveCourierEmails() {
  const r = await dbQuery(
    `SELECT email
     FROM msg_users
     WHERE is_active=TRUE
       AND role='courier'
       AND COALESCE(email,'') <> ''`
  );
  return (r.rows || [])
    .map((x) => String(x.email || "").trim().toLowerCase())
    .filter((x) => isEmail(x));
}

async function buildNotifyRecipientsForCouriersAndSupervisors() {
  const base = [];
  if (COURIER_MAILBOX && isEmail(COURIER_MAILBOX)) base.push(String(COURIER_MAILBOX).trim().toLowerCase());

  const supervisors = parseEmailList(SUPERVISOR_NOTIFY_TO);
  const couriers = await getActiveCourierEmails();

  const all = [...base, ...supervisors, ...couriers];
  const uniq = [...new Set(all.map((x) => x.trim().toLowerCase()).filter(Boolean))];

  return uniq.join(",");
}

async function notifyViaGAS({ event, requesterEmail, notifyTo, data }) {
  if (!GAS_WEBHOOK_URL || !GAS_WEBHOOK_SECRET) return;

  const payload = {
    secret: GAS_WEBHOOK_SECRET,
    event,
    requesterEmail: requesterEmail || "",
    notifyTo: notifyTo || "",
    data: data || {},
  };

  try {
    const f = await _getFetch();
    const resp = await f(GAS_WEBHOOK_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!resp.ok) {
      const t = await resp.text().catch(() => "");
      console.error("GAS notify failed:", resp.status, t);
    }
  } catch (err) {
    console.error("GAS notify error:", err?.message || err);
  }
}

/* =========================
   HEALTH
========================= */
app.get("/api/health", async (req, res) => {
  safeJson(res, 200, {
    ok: true,
    message: "✅ PRODIMA MENSAJERÍA API activa",
    db: hasDb() ? "on" : "off",
  });
});

/* =========================
   AUTH
========================= */
app.post("/api/admin/login", async (req, res) => {
  const user = String(req.body?.user || "").trim();
  const pass = String(req.body?.pass || "").trim();
  if (user !== ADMIN_USER || pass !== ADMIN_PASS) {
    return safeJson(res, 401, { ok: false, message: "Credenciales inválidas" });
  }
  const token = signToken({ role: "admin", user }, "12h");
  return safeJson(res, 200, { ok: true, token });
});

app.post("/api/user/login", async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const username = String(req.body?.username || "").trim().toLowerCase();
    const pin = String(req.body?.pin || "").trim();
    if (!username || !pin) return safeJson(res, 400, { ok: false, message: "Falta username/pin" });

    const r = await dbQuery(
      `SELECT id, username, full_name, department, role, email, phone, pin_hash, is_active
       FROM msg_users
       WHERE LOWER(username)=LOWER($1)
       LIMIT 1`,
      [username]
    );
    const u = r.rows?.[0];
    if (!u || !u.is_active) return safeJson(res, 401, { ok: false, message: "Usuario no válido/inactivo" });
    if (!verifyPin(pin, u.pin_hash)) return safeJson(res, 401, { ok: false, message: "PIN incorrecto" });

    const token = signToken({ role: "user", userId: u.id, username: u.username }, "12h");
    return safeJson(res, 200, {
      ok: true,
      token,
      profile: {
        id: u.id,
        username: u.username,
        fullName: u.full_name,
        department: u.department,
        role: u.role,
        email: u.email || "",
        phone: u.phone || "",
      },
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get("/api/user/me", verifyUser, async (req, res) => {
  try {
    const r = await dbQuery(
      `SELECT id, username, full_name, department, role, email, phone, is_active
       FROM msg_users WHERE id=$1 LIMIT 1`,
      [Number(req.user.userId)]
    );
    const u = r.rows?.[0];
    if (!u || !u.is_active) return safeJson(res, 401, { ok: false, message: "Usuario no válido/inactivo" });

    return safeJson(res, 200, {
      ok: true,
      profile: {
        id: u.id,
        username: u.username,
        fullName: u.full_name,
        department: u.department,
        role: u.role,
        email: u.email || "",
        phone: u.phone || "",
      },
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================
   ADMIN: USERS
========================= */
app.get("/api/admin/messaging/users", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const r = await dbQuery(
      `SELECT
         id, username, full_name, department, role, email, phone, is_active,
         ${fmtTsSql("created_at")} AS created_at_local
       FROM msg_users
       ORDER BY id DESC
       LIMIT 2000`
    );

    const users = (r.rows || []).map((u) => ({
      id: Number(u.id),
      username: u.username,
      fullName: u.full_name,
      department: u.department,
      role: u.role,
      email: u.email || "",
      phone: u.phone || "",
      isActive: Boolean(u.is_active),
      createdAt: u.created_at_local || null,
    }));

    return safeJson(res, 200, { ok: true, users });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.post("/api/admin/messaging/users", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const username = String(req.body?.username || "").trim().toLowerCase();
    const fullName = String(req.body?.fullName || "").trim();
    const department = String(req.body?.department || "").trim();
    const roleRaw = String(req.body?.role || "requester").trim().toLowerCase();
    const role = roleRaw === "courier" || roleRaw === "mercaderista" ? "courier" : "requester";
    const email = String(req.body?.email || "").trim();
    const phone = String(req.body?.phone || "").trim();
    const pin = String(req.body?.pin || "").trim();

    if (!username) return safeJson(res, 400, { ok: false, message: "Falta username" });
    if (!fullName) return safeJson(res, 400, { ok: false, message: "Falta fullName" });
    if (!department) return safeJson(res, 400, { ok: false, message: "Falta department" });
    if (!pin) return safeJson(res, 400, { ok: false, message: "Falta pin" });

    const pinHash = hashPin(pin);

    const r = await dbQuery(
      `INSERT INTO msg_users(username, full_name, department, role, email, phone, pin_hash, is_active, created_at, updated_at)
       VALUES($1,$2,$3,$4,$5,$6,$7,TRUE,NOW(),NOW())
       RETURNING id`,
      [username, fullName, department, role, email, phone, pinHash]
    );

    return safeJson(res, 200, { ok: true, id: Number(r.rows?.[0]?.id || 0) });
  } catch (e) {
    const msg = String(e.message || e);
    if (msg.toLowerCase().includes("duplicate") || msg.toLowerCase().includes("unique")) {
      return safeJson(res, 400, { ok: false, message: "Username ya existe" });
    }
    return safeJson(res, 500, { ok: false, message: msg });
  }
});

app.patch("/api/admin/messaging/users/:id", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });
    const id = Number(req.params.id || 0);
    if (!id) return safeJson(res, 400, { ok: false, message: "id inválido" });

    const isActive = req.body?.isActive;
    if (typeof isActive !== "boolean") return safeJson(res, 400, { ok: false, message: "isActive requerido (boolean)" });

    await dbQuery(`UPDATE msg_users SET is_active=$2, updated_at=NOW() WHERE id=$1`, [id, isActive]);
    return safeJson(res, 200, { ok: true });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.patch("/api/admin/messaging/users/:id/pin", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });
    const id = Number(req.params.id || 0);
    const pin = String(req.body?.pin || "").trim();
    if (!id) return safeJson(res, 400, { ok: false, message: "id inválido" });
    if (!pin) return safeJson(res, 400, { ok: false, message: "pin requerido" });

    const pinHash = hashPin(pin);
    await dbQuery(`UPDATE msg_users SET pin_hash=$2, updated_at=NOW() WHERE id=$1`, [id, pinHash]);
    return safeJson(res, 200, { ok: true });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================
   USER (SOLICITANTE): CREATE REQUEST + LIST MY REQUESTS
========================= */
app.post("/api/user/requests", verifyUser, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const userId = Number(req.user.userId);
    const uR = await dbQuery(
      `SELECT id, username, full_name, department, email, phone, is_active, role
       FROM msg_users WHERE id=$1 LIMIT 1`,
      [userId]
    );
    const u = uR.rows?.[0];
    if (!u || !u.is_active) return safeJson(res, 401, { ok: false, message: "Usuario inactivo" });

    if (isCourierRole(u.role)) {
      return safeJson(res, 403, { ok: false, message: "Mensajero no crea solicitudes desde este módulo." });
    }

    const requestType = String(req.body?.requestType || "").trim();
    const contactPersonPhone = String(req.body?.contactPersonPhone || "").trim();
    const addressDetails = String(req.body?.addressDetails || "").trim();
    const description = String(req.body?.description || "").trim();
    const priority = String(req.body?.priority || "Media").trim();

    if (!requestType) return safeJson(res, 400, { ok: false, message: "Falta requestType" });
    if (!contactPersonPhone) return safeJson(res, 400, { ok: false, message: "Falta contactPersonPhone" });
    if (!addressDetails) return safeJson(res, 400, { ok: false, message: "Falta addressDetails" });
    if (!description) return safeJson(res, 400, { ok: false, message: "Falta description" });

    const r = await dbQuery(
      `INSERT INTO msg_requests(
        created_by_user_id, created_by_username,
        requester_name, requester_department, requester_email, requester_phone,
        request_type, contact_person_phone, address_details, description, priority,
        status, status_updated_at, updated_at,
        courier_comment, closed_at
      )
      VALUES(
        $1,$2,
        $3,$4,$5,$6,
        $7,$8,$9,$10,$11,
        'open', NOW(), NOW(),
        '', NULL
      )
      RETURNING
        id,
        ${fmtTsSql("created_at")} AS created_at_local,
        ${fmtTsSql("status_updated_at")} AS status_updated_at_local`,
      [
        Number(u.id),
        String(u.username || ""),
        String(u.full_name || ""),
        String(u.department || ""),
        String(u.email || ""),
        String(u.phone || ""),
        requestType,
        contactPersonPhone,
        addressDetails,
        description,
        priority,
      ]
    );

    const newId = Number(r.rows?.[0]?.id || 0);
    const createdAtLocal = r.rows?.[0]?.created_at_local || null;
    const statusUpdatedAtLocal = r.rows?.[0]?.status_updated_at_local || null;

    // ✅ Notificación: creada (solicitante + mensajeros + supervisores + buzón)
    try {
      const notifyTo = await buildNotifyRecipientsForCouriersAndSupervisors();
      void notifyViaGAS({
        event: "created",
        requesterEmail: String(u.email || ""),
        notifyTo,
        data: {
          id: newId,
          status: "open",
          requestType,
          priority,
          department: String(u.department || ""),
          requesterName: String(u.full_name || ""),
          contactPersonPhone,
          addressDetails,
          description,
          assignedToName: "",
          courierComment: "",
          createdAt: createdAtLocal,
          closedAt: null,
          statusUpdatedAt: statusUpdatedAtLocal,
        },
      });
    } catch (_) {}

    return safeJson(res, 200, { ok: true, id: newId });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get("/api/user/requests", verifyUser, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const userId = Number(req.user.userId);
    const status = normStatus(req.query?.status || "");

    const where = [`created_by_user_id=$1`];
    const params = [userId];
    let p = 2;

    if (status && status !== "__all__") {
      where.push(`status=$${p++}`);
      params.push(status);
    }

    const r = await dbQuery(
      `SELECT
        id,
        ${fmtTsSql("created_at")} AS created_at_local,
        request_type, priority, status,
        assigned_to_name, assigned_to_user_id, address_details,
        courier_comment,
        ${fmtTsSql("closed_at")} AS closed_at_local
       FROM msg_requests
       WHERE ${where.join(" AND ")}
       ORDER BY created_at DESC
       LIMIT 500`,
      params
    );

    const requests = (r.rows || []).map((x) => ({
      id: Number(x.id),
      createdAt: x.created_at_local || null,
      requestType: x.request_type,
      priority: x.priority,
      status: x.status,
      assignedToName: x.assigned_to_name || "",
      assignedToUserId: x.assigned_to_user_id != null ? Number(x.assigned_to_user_id) : null,
      addressDetails: x.address_details || "",
      courierComment: x.courier_comment || "",
      closedAt: x.closed_at_local || null,
    }));

    return safeJson(res, 200, { ok: true, requests });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================
   USER (MENSAJERO): LIST + UPDATE
========================= */

// GET /api/user/courier/requests?scope=assigned|all&status=open|...
app.get("/api/user/courier/requests", verifyUser, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const userId = Number(req.user.userId);

    const uR = await dbQuery(
      `SELECT id, role, is_active FROM msg_users WHERE id=$1 LIMIT 1`,
      [userId]
    );
    const u = uR.rows?.[0];
    if (!u || !u.is_active) return safeJson(res, 401, { ok: false, message: "Usuario inactivo" });
    if (!isCourierRole(u.role)) return safeJson(res, 403, { ok: false, message: "Solo mensajeros." });

    const scope = String(req.query?.scope || "assigned").trim().toLowerCase(); // assigned | all
    const status = normStatus(req.query?.status || "");

    const where = [];
    const params = [];
    let p = 1;

    if (scope !== "all") {
      where.push(`r.assigned_to_user_id=$${p++}`);
      params.push(userId);
    }

    if (status && status !== "__all__") {
      where.push(`r.status=$${p++}`);
      params.push(status);
    }

    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

    const r = await dbQuery(
      `SELECT
        r.id,
        ${fmtTsSql("r.created_at")} AS created_at_local,
        ${fmtTsSql("r.updated_at")} AS updated_at_local,
        r.created_by_username,
        r.requester_name, r.requester_department,
        r.request_type, r.priority, r.status,
        r.contact_person_phone, r.address_details, r.description,
        r.assigned_to_user_id, r.assigned_to_name,
        r.courier_comment,
        ${fmtTsSql("r.closed_at")} AS closed_at_local
       FROM msg_requests r
       ${whereSql}
       ORDER BY r.created_at DESC
       LIMIT 500`,
      params
    );

    const requests = (r.rows || []).map((x) => ({
      id: Number(x.id),
      createdAt: x.created_at_local || null,
      updatedAt: x.updated_at_local || null,
      createdByUsername: x.created_by_username || "",
      requesterName: x.requester_name || "",
      department: x.requester_department || "",
      requestType: x.request_type || "",
      priority: x.priority || "",
      status: x.status || "open",
      contactPersonPhone: x.contact_person_phone || "",
      addressDetails: x.address_details || "",
      description: x.description || "",
      assignedToUserId: x.assigned_to_user_id != null ? Number(x.assigned_to_user_id) : null,
      assignedToName: x.assigned_to_name || "",
      courierComment: x.courier_comment || "",
      closedAt: x.closed_at_local || null,
    }));

    return safeJson(res, 200, { ok: true, scope, status: status || "__ALL__", requests });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

// PATCH /api/user/courier/requests/:id  { status, comment }
app.patch("/api/user/courier/requests/:id", verifyUser, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const userId = Number(req.user.userId);
    const id = Number(req.params.id || 0);
    if (!id) return safeJson(res, 400, { ok: false, message: "id inválido" });

    const uR = await dbQuery(`SELECT id, role, is_active FROM msg_users WHERE id=$1 LIMIT 1`, [userId]);
    const u = uR.rows?.[0];
    if (!u || !u.is_active) return safeJson(res, 401, { ok: false, message: "Usuario inactivo" });
    if (!isCourierRole(u.role)) return safeJson(res, 403, { ok: false, message: "Solo mensajeros." });

    const curR = await dbQuery(
      `SELECT id, assigned_to_user_id, status, courier_comment
       FROM msg_requests WHERE id=$1 LIMIT 1`,
      [id]
    );
    const cur = curR.rows?.[0];
    if (!cur) return safeJson(res, 404, { ok: false, message: "Solicitud no encontrada" });

    const assignedTo = cur.assigned_to_user_id != null ? Number(cur.assigned_to_user_id) : null;
    if (!assignedTo || assignedTo !== userId) {
      return safeJson(res, 403, { ok: false, message: "Solo puedes actualizar solicitudes asignadas a ti." });
    }

    const oldStatus = String(cur.status || "");
    const oldComment = String(cur.courier_comment || "");

    const status = normStatus(req.body?.status || "");
    const comment = String(req.body?.comment || "").trim();

    if (!["open", "in_progress", "closed", "cancelled"].includes(status)) {
      return safeJson(res, 400, { ok: false, message: "status inválido" });
    }

    const closedAtSql = status === "closed" ? "NOW()" : "NULL";

    await dbQuery(
      `UPDATE msg_requests
       SET status=$2,
           courier_comment=$3,
           closed_at=${closedAtSql},
           status_updated_at=NOW(),
           updated_at=NOW()
       WHERE id=$1`,
      [id, status, comment]
    );

    // ✅ Notificación si cambió estado o comentario
    const changed = (oldStatus !== status) || (oldComment !== comment);
    if (changed) {
      const reqR = await dbQuery(
        `SELECT
          id, requester_email, requester_name, requester_department,
          request_type, priority, contact_person_phone, address_details, description,
          assigned_to_name, courier_comment,
          ${fmtTsSql("created_at")} AS created_at_local,
          ${fmtTsSql("closed_at")} AS closed_at_local,
          ${fmtTsSql("status_updated_at")} AS status_updated_at_local
         FROM msg_requests
         WHERE id=$1 LIMIT 1`,
        [id]
      );
      const reqRow = reqR.rows?.[0];
      if (reqRow) {
        const notifyTo = await buildNotifyRecipientsForCouriersAndSupervisors();
        void notifyViaGAS({
          event: "status_changed",
          requesterEmail: reqRow.requester_email || "",
          notifyTo,
          data: {
            id,
            oldStatus,
            newStatus: status,
            status,
            requestType: reqRow.request_type,
            priority: reqRow.priority,
            department: reqRow.requester_department,
            requesterName: reqRow.requester_name,
            contactPersonPhone: reqRow.contact_person_phone,
            addressDetails: reqRow.address_details,
            description: reqRow.description,
            assignedToName: reqRow.assigned_to_name || "",
            courierComment: comment || reqRow.courier_comment || "",
            createdAt: reqRow.created_at_local || null,
            closedAt: reqRow.closed_at_local || null,
            statusUpdatedAt: reqRow.status_updated_at_local || null,
          },
        });
      }
    }

    return safeJson(res, 200, { ok: true });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================
   ADMIN: REQUESTS LIST + ASSIGN + STATUS
========================= */
app.get("/api/admin/messaging/requests", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const pageRaw = Number(req.query?.page || 1);
    const limitRaw = Number(req.query?.limit || 20);
    const page = Math.max(1, Number.isFinite(pageRaw) ? Math.trunc(pageRaw) : 1);
    const limit = Math.max(1, Math.min(200, Number.isFinite(limitRaw) ? Math.trunc(limitRaw) : 20));
    const skip = (page - 1) * limit;

    const status = normStatus(req.query?.status || "");
    const department = String(req.query?.department || "").trim();
    const q = String(req.query?.q || "").trim().toLowerCase();

    const where = [];
    const params = [];
    let p = 1;

    if (status && status !== "__all__") {
      where.push(`r.status=$${p++}`);
      params.push(status);
    }
    if (department && department !== "__ALL__") {
      where.push(`r.requester_department=$${p++}`);
      params.push(department);
    }
    if (q) {
      where.push(
        `(CAST(r.id AS TEXT) ILIKE $${p++} OR LOWER(COALESCE(r.created_by_username,'')) ILIKE $${p++} OR LOWER(COALESCE(r.description,'')) ILIKE $${p++})`
      );
      params.push(`%${q}%`, `%${q}%`, `%${q}%`);
    }

    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

    const totalR = await dbQuery(`SELECT COUNT(*)::int AS total FROM msg_requests r ${whereSql}`, params);
    const total = Number(totalR.rows?.[0]?.total || 0);

    const dataR = await dbQuery(
      `SELECT
        r.id,
        ${fmtTsSql("r.created_at")} AS created_at_local,
        ${fmtTsSql("r.updated_at")} AS updated_at_local,
        r.created_by_username,
        r.requester_name, r.requester_department,
        r.request_type, r.priority, r.status,
        r.assigned_to_user_id, r.assigned_to_name,
        r.courier_comment,
        ${fmtTsSql("r.closed_at")} AS closed_at_local
       FROM msg_requests r
       ${whereSql}
       ORDER BY r.created_at DESC
       OFFSET $${p++} LIMIT $${p++}`,
      [...params, skip, limit]
    );

    const requests = (dataR.rows || []).map((r) => ({
      id: Number(r.id),
      createdAt: r.created_at_local || null,
      updatedAt: r.updated_at_local || null,
      createdByUsername: r.created_by_username,
      requesterName: r.requester_name,
      department: r.requester_department,
      requestType: r.request_type,
      priority: r.priority,
      status: r.status,
      assignedToUserId: r.assigned_to_user_id != null ? Number(r.assigned_to_user_id) : null,
      assignedToName: r.assigned_to_name || "",
      courierComment: r.courier_comment || "",
      closedAt: r.closed_at_local || null,
    }));

    return safeJson(res, 200, { ok: true, total, page, limit, requests });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.patch("/api/admin/messaging/requests/:id/assign", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const id = Number(req.params.id || 0);
    if (!id) return safeJson(res, 400, { ok: false, message: "id inválido" });

    const courierUserIdRaw = req.body?.courierUserId;
    const courierUserId = courierUserIdRaw == null ? null : Number(courierUserIdRaw);

    let assignedName = "";
    if (courierUserId) {
      const uR = await dbQuery(
        `SELECT id, full_name, role, is_active FROM msg_users WHERE id=$1 LIMIT 1`,
        [courierUserId]
      );
      const u = uR.rows?.[0];
      if (!u || !u.is_active || !isCourierRole(u.role)) {
        return safeJson(res, 400, { ok: false, message: "Mensajero inválido/inactivo" });
      }
      assignedName = String(u.full_name || "");
    }

    await dbQuery(
      `UPDATE msg_requests
       SET assigned_to_user_id=$2,
           assigned_to_name=$3,
           assigned_at=CASE WHEN $2::bigint IS NULL THEN NULL ELSE NOW() END,
           updated_at=NOW()
       WHERE id=$1`,
      [id, courierUserId, assignedName]
    );

    // ✅ Notificación: asignación
    try {
      const reqR = await dbQuery(
        `SELECT
           id, requester_email, requester_name, requester_department,
           request_type, priority, contact_person_phone, address_details, description,
           assigned_to_name,
           ${fmtTsSql("created_at")} AS created_at_local,
           ${fmtTsSql("closed_at")} AS closed_at_local,
           ${fmtTsSql("status_updated_at")} AS status_updated_at_local
         FROM msg_requests WHERE id=$1 LIMIT 1`,
        [id]
      );
      const reqRow = reqR.rows?.[0];
      if (reqRow) {
        const notifyTo = await buildNotifyRecipientsForCouriersAndSupervisors();
        void notifyViaGAS({
          event: "assigned",
          requesterEmail: reqRow.requester_email || "",
          notifyTo,
          data: {
            id,
            requestType: reqRow.request_type,
            priority: reqRow.priority,
            department: reqRow.requester_department,
            requesterName: reqRow.requester_name,
            contactPersonPhone: reqRow.contact_person_phone,
            addressDetails: reqRow.address_details,
            description: reqRow.description,
            assignedToName: reqRow.assigned_to_name || "",
            createdAt: reqRow.created_at_local || null,
            closedAt: reqRow.closed_at_local || null,
            statusUpdatedAt: reqRow.status_updated_at_local || null,
          },
        });
      }
    } catch (_) {}

    return safeJson(res, 200, { ok: true });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.patch("/api/admin/messaging/requests/:id/status", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const id = Number(req.params.id || 0);
    if (!id) return safeJson(res, 400, { ok: false, message: "id inválido" });

    const status = normStatus(req.body?.status || "");
    if (!["open", "in_progress", "closed", "cancelled"].includes(status)) {
      return safeJson(res, 400, { ok: false, message: "status inválido" });
    }

    const beforeR = await dbQuery(
      `SELECT
        id, status, requester_email, requester_name, requester_department,
        request_type, priority, contact_person_phone, address_details, description,
        assigned_to_name, courier_comment,
        ${fmtTsSql("created_at")} AS created_at_local,
        ${fmtTsSql("closed_at")} AS closed_at_local,
        ${fmtTsSql("status_updated_at")} AS status_updated_at_local
       FROM msg_requests
       WHERE id=$1 LIMIT 1`,
      [id]
    );
    const before = beforeR.rows?.[0];
    if (!before) return safeJson(res, 404, { ok: false, message: "Solicitud no encontrada" });

    const oldStatus = String(before.status || "");
    const closedAtSql = status === "closed" ? "NOW()" : "NULL";

    await dbQuery(
      `UPDATE msg_requests
       SET status=$2, status_updated_at=NOW(), updated_at=NOW(), closed_at=${closedAtSql}
       WHERE id=$1`,
      [id, status]
    );

    // ✅ Notificación: cambio de estado (admin)
    if (oldStatus !== status) {
      try {
        // refrescar tiempos post-update
        const afterR = await dbQuery(
          `SELECT
             ${fmtTsSql("created_at")} AS created_at_local,
             ${fmtTsSql("closed_at")} AS closed_at_local,
             ${fmtTsSql("status_updated_at")} AS status_updated_at_local
           FROM msg_requests WHERE id=$1 LIMIT 1`,
          [id]
        );
        const after = afterR.rows?.[0] || {};

        const notifyTo = await buildNotifyRecipientsForCouriersAndSupervisors();
        void notifyViaGAS({
          event: "status_changed",
          requesterEmail: before.requester_email || "",
          notifyTo,
          data: {
            id,
            oldStatus,
            newStatus: status,
            status,
            requestType: before.request_type,
            priority: before.priority,
            department: before.requester_department,
            requesterName: before.requester_name,
            contactPersonPhone: before.contact_person_phone,
            addressDetails: before.address_details,
            description: before.description,
            assignedToName: before.assigned_to_name || "",
            courierComment: before.courier_comment || "",
            createdAt: after.created_at_local || before.created_at_local || null,
            closedAt: after.closed_at_local || null,
            statusUpdatedAt: after.status_updated_at_local || null,
          },
        });
      } catch (_) {}
    }

    return safeJson(res, 200, { ok: true });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================
   ADMIN: DASHBOARD
========================= */
app.get("/api/admin/messaging/dashboard", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const today = new Date();
    const defaultFrom = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, "0")}-01`;
    const defaultTo = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, "0")}-${String(today.getDate()).padStart(2, "0")}`;

    const from = isISODate(req.query?.from) ? String(req.query.from) : defaultFrom;
    const to = isISODate(req.query?.to) ? String(req.query.to) : defaultTo;
    const department = String(req.query?.department || "").trim();

    const where = [`r.created_at::date BETWEEN $1::date AND $2::date`];
    const params = [from, to];
    let p = 3;

    if (department && department !== "__ALL__") {
      where.push(`r.requester_department=$${p++}`);
      params.push(department);
    }

    const whereSql = `WHERE ${where.join(" AND ")}`;

    const totR = await dbQuery(`SELECT COUNT(*)::int AS total FROM msg_requests r ${whereSql}`, params);
    const totals = { total: Number(totR.rows?.[0]?.total || 0) };

    const byStatusR = await dbQuery(
      `SELECT r.status, COUNT(*)::int AS c
       FROM msg_requests r
       ${whereSql}
       GROUP BY r.status
       ORDER BY c DESC`,
      params
    );

    const statusLabel = (s) => {
      const t = String(s || "");
      if (t === "open") return "Abiertas";
      if (t === "in_progress") return "En progreso";
      if (t === "closed") return "Cerradas";
      if (t === "cancelled") return "Canceladas";
      return t;
    };

    const byStatus = (byStatusR.rows || []).map((x) => ({
      status: x.status,
      statusLabel: statusLabel(x.status),
      count: Number(x.c || 0),
    }));

    const byDeptR = await dbQuery(
      `SELECT COALESCE(NULLIF(r.requester_department,''),'(Sin depto)') AS d, COUNT(*)::int AS c
       FROM msg_requests r
       ${whereSql}
       GROUP BY 1
       ORDER BY c DESC`,
      params
    );
    const byDepartment = (byDeptR.rows || []).map((x) => ({
      department: x.d,
      count: Number(x.c || 0),
    }));

    const byDayR = await dbQuery(
      `SELECT to_char(r.created_at::date,'YYYY-MM-DD') AS day, COUNT(*)::int AS c
       FROM msg_requests r
       ${whereSql}
       GROUP BY r.created_at::date
       ORDER BY r.created_at::date ASC`,
      params
    );
    const byDay = (byDayR.rows || []).map((x) => ({
      day: x.day,
      count: Number(x.c || 0),
    }));

    return safeJson(res, 200, {
      ok: true,
      from,
      to,
      department: department || "__ALL__",
      totals,
      byStatus,
      byDepartment,
      byDay,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================
   START
========================= */
process.on("unhandledRejection", (e) => console.error("unhandledRejection:", e));
process.on("uncaughtException", (e) => console.error("uncaughtException:", e));

(async () => {
  try {
    await ensureDb();
    console.log(hasDb() ? "DB ready ✅" : "DB not configured ⚠️");
  } catch (e) {
    console.error("DB init error:", e.message);
  }

  app.listen(Number(PORT), () => console.log(`Server listening on :${PORT}`));
})();
