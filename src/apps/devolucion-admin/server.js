import express from "express";
import pg from "pg";
import jwt from "jsonwebtoken";
import bcrypt from "bcryptjs";

const { Pool } = pg;
const app = express();
app.use(express.json({ limit: "2mb" }));

/* =========================================================
   ENV
========================================================= */
const {
  PORT = 3000,
  DATABASE_URL = "",
  JWT_SECRET = "change_me",
  ADMIN_USER = "PRODIMA",
  ADMIN_PASS = "ADMINISTRADOR",
  CORS_ORIGIN = "",
} = process.env;

/* =========================================================
   CORS
========================================================= */
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

/* =========================================================
   DB
========================================================= */
const pool = new Pool({
  connectionString: DATABASE_URL || undefined,
  ssl:
    DATABASE_URL && DATABASE_URL.includes("sslmode")
      ? { rejectUnauthorized: false }
      : undefined,
});

function hasDb() {
  return Boolean(DATABASE_URL);
}

async function dbQuery(text, params = []) {
  return pool.query(text, params);
}

async function ensureDb() {
  if (!hasDb()) return;

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS app_users (
      id SERIAL PRIMARY KEY,
      username TEXT UNIQUE NOT NULL,
      full_name TEXT DEFAULT '',
      pin_hash TEXT NOT NULL,
      province TEXT DEFAULT '',
      warehouse_code TEXT DEFAULT '',
      is_active BOOLEAN DEFAULT TRUE,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS return_requests (
      id BIGSERIAL PRIMARY KEY,
      request_no TEXT UNIQUE NOT NULL,
      created_at TIMESTAMP DEFAULT NOW(),
      created_by_user_id INTEGER,
      created_by_username TEXT DEFAULT '',
      created_by_name TEXT DEFAULT '',
      province TEXT DEFAULT '',
      warehouse_code TEXT DEFAULT '',
      card_code TEXT NOT NULL,
      card_name TEXT DEFAULT '',
      causa TEXT NOT NULL,
      motivo TEXT NOT NULL,
      comments TEXT DEFAULT '',
      total_lines INTEGER DEFAULT 0,
      total_qty NUMERIC(19,6) DEFAULT 0,
      status TEXT DEFAULT 'CREATED',
      sap_entity TEXT DEFAULT '',
      sap_doc_entry BIGINT,
      sap_doc_num BIGINT,
      sap_payload JSONB,
      sap_response JSONB,
      raw_error TEXT DEFAULT ''
    );
  `);

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS return_request_lines (
      id BIGSERIAL PRIMARY KEY,
      request_id BIGINT NOT NULL REFERENCES return_requests(id) ON DELETE CASCADE,
      line_num INTEGER NOT NULL,
      item_code TEXT NOT NULL,
      item_name TEXT DEFAULT '',
      quantity NUMERIC(19,6) DEFAULT 0
    );
  `);

  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_rr_admin_created_at ON return_requests(created_at);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_rr_admin_user ON return_requests(created_by_username);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_rr_admin_card ON return_requests(card_code);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_rr_admin_ca ON return_requests(causa);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_rr_admin_mo ON return_requests(motivo);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_rr_admin_status ON return_requests(status);`);
}

/* =========================================================
   HELPERS
========================================================= */
const TZ_OFFSET_MIN = -300;

function safeJson(res, status, obj) {
  return res.status(status).json(obj);
}

function nowInOffsetMs(offsetMin = 0) {
  const now = new Date();
  return now.getTime() + now.getTimezoneOffset() * 60000 + Number(offsetMin) * 60000;
}

function isoDateInOffset(offsetMin = 0) {
  const d = new Date(nowInOffsetMs(offsetMin));
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

function addDaysISO(iso, days) {
  const d = new Date(String(iso || "").slice(0, 10) + "T00:00:00");
  if (Number.isNaN(d.getTime())) return "";
  d.setDate(d.getDate() + Number(days || 0));
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ");
}

function provinceToWarehouseServer(province) {
  const p = norm(province);
  if (p === "chiriqui" || p === "bocas del toro") return "200";
  if (p === "veraguas" || p === "cocle" || p === "los santos" || p === "herrera") return "500";
  if (p === "panama" || p === "panama oeste" || p === "colon") return "300";
  if (p === "rci") return "01";
  return "";
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

async function hashPin(pin) {
  return bcrypt.hash(String(pin), 10);
}

/* =========================================================
   HEALTH
========================================================= */
app.get("/api/health", async (req, res) => {
  return safeJson(res, 200, {
    ok: true,
    app: "devolucion-admin-api",
    db: hasDb() ? "on" : "off",
    nowPanama: new Date(nowInOffsetMs(TZ_OFFSET_MIN)).toISOString().replace("Z", ""),
  });
});

/* =========================================================
   ADMIN LOGIN
========================================================= */
app.post("/api/admin/login", async (req, res) => {
  const user = String(req.body?.user || "").trim();
  const pass = String(req.body?.pass || "").trim();
  if (user !== ADMIN_USER || pass !== ADMIN_PASS) {
    return safeJson(res, 401, { ok: false, message: "Credenciales inválidas" });
  }
  const token = signToken({ role: "admin", user }, "12h");
  return safeJson(res, 200, { ok: true, token });
});

/* =========================================================
   USERS
========================================================= */
app.get("/api/admin/users", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });
    const r = await dbQuery(
      `SELECT id, username, full_name, province, warehouse_code, is_active, created_at
       FROM app_users
       ORDER BY id DESC`
    );
    return safeJson(res, 200, { ok: true, users: r.rows });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.post("/api/admin/users", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const username = String(req.body?.username || "").trim().toLowerCase();
    const full_name = String(req.body?.full_name ?? req.body?.fullName ?? "").trim();
    const pin = String(req.body?.pin || "").trim();
    const province = String(req.body?.province || "").trim();

    if (!username) return safeJson(res, 400, { ok: false, message: "Username requerido" });
    if (!full_name) return safeJson(res, 400, { ok: false, message: "Nombre requerido" });
    if (!pin || pin.length < 4) return safeJson(res, 400, { ok: false, message: "PIN mínimo 4" });
    if (!province) return safeJson(res, 400, { ok: false, message: "Provincia requerida" });

    const warehouse_code = provinceToWarehouseServer(province);
    const pin_hash = await hashPin(pin);

    const r = await dbQuery(
      `INSERT INTO app_users(username, full_name, pin_hash, province, warehouse_code, is_active)
       VALUES ($1,$2,$3,$4,$5,TRUE)
       RETURNING id, username, full_name, province, warehouse_code, is_active, created_at`,
      [username, full_name, pin_hash, province, warehouse_code]
    );

    return safeJson(res, 200, { ok: true, user: r.rows[0] });
  } catch (e) {
    if (String(e?.code) === "23505") {
      return safeJson(res, 409, { ok: false, message: "Ese username ya existe" });
    }
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.patch("/api/admin/users/:id/toggle", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });
    const id = Number(req.params.id || 0);
    if (!Number.isFinite(id) || id <= 0) return safeJson(res, 400, { ok: false, message: "ID inválido" });

    const r = await dbQuery(
      `UPDATE app_users
       SET is_active = NOT is_active
       WHERE id = $1
       RETURNING id, username, full_name, province, warehouse_code, is_active, created_at`,
      [id]
    );

    if (!r.rowCount) return safeJson(res, 404, { ok: false, message: "Usuario no encontrado" });
    return safeJson(res, 200, { ok: true, user: r.rows[0] });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.delete("/api/admin/users/:id", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });
    const id = Number(req.params.id || 0);
    if (!Number.isFinite(id) || id <= 0) return safeJson(res, 400, { ok: false, message: "ID inválido" });

    const r = await dbQuery(`DELETE FROM app_users WHERE id=$1`, [id]);
    if (!r.rowCount) return safeJson(res, 404, { ok: false, message: "Usuario no encontrado" });
    return safeJson(res, 200, { ok: true });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.patch("/api/admin/users/:id/pin", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const id = Number(req.params.id || 0);
    const pin = String(req.body?.pin || "").trim();
    if (!Number.isFinite(id) || id <= 0) return safeJson(res, 400, { ok: false, message: "ID inválido" });
    if (!pin || pin.length < 4) return safeJson(res, 400, { ok: false, message: "PIN mínimo 4" });

    const pin_hash = await hashPin(pin);
    const r = await dbQuery(`UPDATE app_users SET pin_hash=$2 WHERE id=$1`, [id, pin_hash]);
    if (!r.rowCount) return safeJson(res, 404, { ok: false, message: "Usuario no encontrado" });
    return safeJson(res, 200, { ok: true });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================================================
   DASHBOARD
========================================================= */
app.get("/api/admin/requests/dashboard", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const fromQ = String(req.query?.from || "").slice(0, 10);
    const toQ = String(req.query?.to || "").slice(0, 10);
    const onlyCreated = String(req.query?.onlyCreated || "0") === "1";

    const today = isoDateInOffset(TZ_OFFSET_MIN);
    const from = /^\d{4}-\d{2}-\d{2}$/.test(fromQ) ? fromQ : addDaysISO(today, -30);
    const to = /^\d{4}-\d{2}-\d{2}$/.test(toQ) ? toQ : today;

    const createdJoin = onlyCreated
      ? `AND LOWER(COALESCE(r.created_by_username,'')) IN (SELECT LOWER(username) FROM app_users WHERE is_active=TRUE)`
      : ``;

    const totalsR = await dbQuery(
      `SELECT
         COUNT(*)::int AS requests,
         COALESCE(SUM(r.total_qty),0)::float AS total_qty,
         COALESCE(SUM(r.total_lines),0)::int AS total_lines,
         COALESCE(SUM(CASE WHEN COALESCE(r.sap_doc_num,0) > 0 THEN 1 ELSE 0 END),0)::int AS sent_to_sap,
         COALESCE(SUM(CASE WHEN UPPER(COALESCE(r.status,'')) LIKE 'CREATED%' THEN 1 ELSE 0 END),0)::int AS created_ok
       FROM return_requests r
       WHERE r.created_at::date BETWEEN $1 AND $2
       ${createdJoin}`,
      [from, to]
    );

    const byUserR = await dbQuery(
      `SELECT
         COALESCE(NULLIF(r.created_by_username,''),'sin_user') AS usuario,
         COUNT(*)::int AS cnt,
         COALESCE(SUM(r.total_qty),0)::float AS total_qty
       FROM return_requests r
       WHERE r.created_at::date BETWEEN $1 AND $2
       ${createdJoin}
       GROUP BY 1
       ORDER BY cnt DESC, total_qty DESC
       LIMIT 2000`,
      [from, to]
    );

    const byWhR = await dbQuery(
      `SELECT
         COALESCE(NULLIF(r.warehouse_code,''),'sin_wh') AS warehouse,
         COUNT(*)::int AS cnt,
         COALESCE(SUM(r.total_qty),0)::float AS total_qty
       FROM return_requests r
       WHERE r.created_at::date BETWEEN $1 AND $2
       ${createdJoin}
       GROUP BY 1
       ORDER BY cnt DESC, total_qty DESC
       LIMIT 2000`,
      [from, to]
    );

    const byCausaR = await dbQuery(
      `SELECT
         COALESCE(NULLIF(r.causa,''),'Sin causa') AS causa,
         COUNT(*)::int AS cnt,
         COALESCE(SUM(r.total_qty),0)::float AS total_qty
       FROM return_requests r
       WHERE r.created_at::date BETWEEN $1 AND $2
       ${createdJoin}
       GROUP BY 1
       ORDER BY cnt DESC, total_qty DESC
       LIMIT 2000`,
      [from, to]
    );

    const byMotivoR = await dbQuery(
      `SELECT
         COALESCE(NULLIF(r.motivo,''),'Sin motivo') AS motivo,
         COUNT(*)::int AS cnt,
         COALESCE(SUM(r.total_qty),0)::float AS total_qty
       FROM return_requests r
       WHERE r.created_at::date BETWEEN $1 AND $2
       ${createdJoin}
       GROUP BY 1
       ORDER BY cnt DESC, total_qty DESC
       LIMIT 2000`,
      [from, to]
    );

    const byClientR = await dbQuery(
      `SELECT
         COALESCE(NULLIF(r.card_name,''), r.card_code, 'sin_cliente') AS customer,
         COUNT(*)::int AS cnt,
         COALESCE(SUM(r.total_qty),0)::float AS total_qty
       FROM return_requests r
       WHERE r.created_at::date BETWEEN $1 AND $2
       ${createdJoin}
       GROUP BY 1
       ORDER BY cnt DESC, total_qty DESC
       LIMIT 2000`,
      [from, to]
    );

    const totals = totalsR.rows?.[0] || {};

    return safeJson(res, 200, {
      ok: true,
      from,
      to,
      totals: {
        requests: Number(totals.requests || 0),
        total_qty: Number(totals.total_qty || 0),
        total_lines: Number(totals.total_lines || 0),
        sent_to_sap: Number(totals.sent_to_sap || 0),
        created_ok: Number(totals.created_ok || 0),
      },
      byUser: byUserR.rows || [],
      byWh: byWhR.rows || [],
      byCausa: byCausaR.rows || [],
      byMotivo: byMotivoR.rows || [],
      byClient: byClientR.rows || [],
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================================================
   REQUESTS LIST
========================================================= */
app.get("/api/admin/requests", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const fromQ = String(req.query?.from || "").slice(0, 10);
    const toQ = String(req.query?.to || "").slice(0, 10);
    const user = String(req.query?.user || "").trim().toLowerCase();
    const client = String(req.query?.client || "").trim().toLowerCase();
    const causa = String(req.query?.causa || "").trim().toLowerCase();
    const motivo = String(req.query?.motivo || "").trim().toLowerCase();
    const status = String(req.query?.status || "").trim().toLowerCase();
    const onlyCreated = String(req.query?.onlyCreated || "0") === "1";

    const skipRaw = Number(req.query?.skip || 0);
    const limitRaw = Number(req.query?.limit || 20);
    const skip = Math.max(0, Number.isFinite(skipRaw) ? Math.trunc(skipRaw) : 0);
    const limit = Math.max(1, Math.min(200, Number.isFinite(limitRaw) ? Math.trunc(limitRaw) : 20));

    const today = isoDateInOffset(TZ_OFFSET_MIN);
    const from = /^\d{4}-\d{2}-\d{2}$/.test(fromQ) ? fromQ : addDaysISO(today, -30);
    const to = /^\d{4}-\d{2}-\d{2}$/.test(toQ) ? toQ : today;

    const where = [];
    const params = [];
    let p = 1;

    where.push(`r.created_at::date BETWEEN $${p++} AND $${p++}`);
    params.push(from, to);

    if (onlyCreated) {
      where.push(`LOWER(COALESCE(r.created_by_username,'')) IN (SELECT LOWER(username) FROM app_users WHERE is_active=TRUE)`);
    }

    if (user) {
      where.push(`LOWER(COALESCE(r.created_by_username,'')) LIKE $${p++}`);
      params.push(`%${user}%`);
    }

    if (client) {
      where.push(`(LOWER(COALESCE(r.card_code,'')) LIKE $${p++} OR LOWER(COALESCE(r.card_name,'')) LIKE $${p++})`);
      params.push(`%${client}%`, `%${client}%`);
    }

    if (causa) {
      where.push(`LOWER(COALESCE(r.causa,'')) LIKE $${p++}`);
      params.push(`%${causa}%`);
    }

    if (motivo) {
      where.push(`LOWER(COALESCE(r.motivo,'')) LIKE $${p++}`);
      params.push(`%${motivo}%`);
    }

    if (status) {
      where.push(`LOWER(COALESCE(r.status,'')) LIKE $${p++}`);
      params.push(`%${status}%`);
    }

    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

    const totalR = await dbQuery(`SELECT COUNT(*)::int AS total FROM return_requests r ${whereSql}`, params);
    const total = Number(totalR.rows?.[0]?.total || 0);

    const dataR = await dbQuery(
      `SELECT
         r.id,
         r.request_no AS "requestNo",
         r.created_at,
         r.created_by_username AS usuario,
         r.created_by_name AS "fullName",
         r.warehouse_code AS warehouse,
         r.card_code AS "cardCode",
         r.card_name AS "cardName",
         r.causa,
         r.motivo,
         r.total_lines AS "totalLines",
         r.total_qty::float AS "totalQty",
         r.status,
         r.sap_doc_entry AS "sapDocEntry",
         r.sap_doc_num AS "sapDocNum",
         r.comments
       FROM return_requests r
       ${whereSql}
       ORDER BY r.id DESC
       OFFSET $${p++} LIMIT $${p++}`,
      [...params, skip, limit]
    );

    return safeJson(res, 200, {
      ok: true,
      from,
      to,
      total,
      skip,
      limit,
      requests: dataR.rows || [],
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================================================
   REQUEST LINES
========================================================= */
app.get("/api/admin/requests/lines", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const requestId = Number(req.query?.requestId || 0);
    if (!Number.isFinite(requestId) || requestId <= 0) {
      return safeJson(res, 400, { ok: false, message: "requestId inválido" });
    }

    const head = await dbQuery(
      `SELECT id, request_no, created_at, card_code, card_name, causa, motivo, status, sap_doc_num
       FROM return_requests WHERE id=$1 LIMIT 1`,
      [requestId]
    );

    const h = head.rows?.[0];
    if (!h) return safeJson(res, 404, { ok: false, message: "Solicitud no encontrada" });

    const lines = await dbQuery(
      `SELECT
         line_num AS "lineNum",
         item_code AS "itemCode",
         item_name AS "itemName",
         quantity::float AS quantity
       FROM return_request_lines
       WHERE request_id=$1
       ORDER BY line_num ASC, id ASC`,
      [requestId]
    );

    return safeJson(res, 200, {
      ok: true,
      request: h,
      lines: lines.rows || [],
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================================================
   START
========================================================= */
process.on("unhandledRejection", (e) => console.error("unhandledRejection:", e));
process.on("uncaughtException", (e) => console.error("uncaughtException:", e));

(async () => {
  try {
    await ensureDb();
    console.log(hasDb() ? "DB ready ✅" : "DB not configured ⚠️");
  } catch (e) {
    console.error("DB init error:", e.message);
  }

  app.listen(Number(PORT), () => console.log(`DEVOLUCION ADMIN API listening on :${PORT}`));
})();
