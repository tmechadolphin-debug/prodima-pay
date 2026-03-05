// server.js (ADMIN DEVOLUCIONES) — COMPLETO
// ESM: usa "type":"module" en package.json

import express from "express";
import pg from "pg";
import jwt from "jsonwebtoken";
import bcrypt from "bcryptjs";

const { Pool } = pg;

const app = express();
app.use(express.json({ limit: "2mb" }));

/* =========================
   ENV (Render)
========================= */
const {
  PORT = 3000,
  DATABASE_URL = "",
  JWT_SECRET = "change_me",

  ADMIN_USER = "PRODIMA",
  ADMIN_PASS = "ADMINISTRADOR",

  // Render:
  // CORS_ORIGIN=https://tu-admin-web.com,https://www.tu-admin-web.com
  CORS_ORIGIN = "",

  // Listas para filtros en Admin
  RETURN_MOTIVOS = "Producto vencido,Cliente rechazó,Producto dañado,Error de facturación,Otro",
  RETURN_CAUSAS = "Empaque roto,Pedido incorrecto,Producto incorrecto,Faltante,Otro",
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

  // si no configuras CORS_ORIGIN -> allowAll
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

function parseCsvList(str) {
  return String(str || "")
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}
const MOTIVOS = parseCsvList(RETURN_MOTIVOS);
const CAUSAS = parseCsvList(RETURN_CAUSAS);

async function ensureDb() {
  if (!hasDb()) return;

  // Usuarios (mercaderistas)
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

  // Cabecera devoluciones
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS return_requests (
      id BIGSERIAL PRIMARY KEY,
      req_num BIGINT UNIQUE NOT NULL,
      req_entry BIGINT,
      doc_date DATE,
      doc_time INT,
      card_code TEXT,
      card_name TEXT,
      usuario TEXT,
      warehouse TEXT,
      motivo TEXT,
      causa TEXT,
      total_amount NUMERIC(19,6) DEFAULT 0,
      total_qty NUMERIC(19,6) DEFAULT 0,
      status TEXT DEFAULT 'Open',
      comments TEXT,
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  // Líneas devoluciones
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS return_lines (
      id BIGSERIAL PRIMARY KEY,
      req_num BIGINT NOT NULL,
      doc_date DATE,
      item_code TEXT NOT NULL,
      item_desc TEXT DEFAULT '',
      qty NUMERIC(19,6) DEFAULT 0,
      price NUMERIC(19,6) DEFAULT 0,
      line_total NUMERIC(19,6) DEFAULT 0,
      updated_at TIMESTAMP DEFAULT NOW(),
      UNIQUE(req_num, item_code)
    );
  `);

  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_returns_date ON return_requests(doc_date);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_returns_user ON return_requests(usuario);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_returns_wh ON return_requests(warehouse);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_returns_card ON return_requests(card_code);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_return_lines_req ON return_lines(req_num);`);
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
  const m = auth.match(/^Bearer\\s+(.+)$/i);
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

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\\u0300-\\u036f]/g, "")
    .replace(/\\s+/g, " ");
}

async function hashPin(pin) {
  const saltRounds = 10;
  return bcrypt.hash(String(pin), saltRounds);
}

function provinceToWarehouseServer(province) {
  const p = norm(province);
  if (p === "chiriqui" || p === "bocas del toro") return "200";
  if (p === "veraguas" || p === "cocle" || p === "los santos" || p === "herrera") return "500";
  if (p === "panama" || p === "panama oeste" || p === "colon") return "300";
  if (p === "rci") return "01";
  return "";
}

/* =========================
   HEALTH
========================= */
app.get("/api/health", async (req, res) => {
  safeJson(res, 200, {
    ok: true,
    message: "✅ PRODIMA DEVOLUCIONES ADMIN API activa",
    db: hasDb() ? "on" : "off",
    motivos: MOTIVOS.length,
    causas: CAUSAS.length,
  });
});

/* =========================
   META (motivos/causas)
========================= */
app.get("/api/admin/meta", verifyAdmin, async (req, res) => {
  return safeJson(res, 200, { ok: true, motivos: MOTIVOS, causas: CAUSAS });
});

/* =========================
   ADMIN LOGIN
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

/* =========================
   ADMIN USERS
========================= */
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

/* =========================
   DASHBOARD (DB)
   GET /api/admin/returns/dashboard-db?from&to&motivo&onlyCreated=1
========================= */
app.get("/api/admin/returns/dashboard-db", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const fromQ = String(req.query?.from || "").slice(0, 10);
    const toQ = String(req.query?.to || "").slice(0, 10);
    const motivo = String(req.query?.motivo || "").trim();
    const onlyCreated = String(req.query?.onlyCreated || "0") === "1";

    const now = new Date();
    const fromDef = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-01`;
    const toDef = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(
      new Date(now.getFullYear(), now.getMonth() + 1, 0).getDate()
    ).padStart(2, "0")}`;

    const from = /^\d{4}-\d{2}-\d{2}$/.test(fromQ) ? fromQ : fromDef;
    const to = /^\d{4}-\d{2}-\d{2}$/.test(toQ) ? toQ : toDef;

    const where = [];
    const params = [];
    let p = 1;

    where.push(`r.doc_date BETWEEN $${p++} AND $${p++}`);
    params.push(from, to);

    if (onlyCreated) {
      where.push(`LOWER(COALESCE(r.usuario,'')) IN (SELECT LOWER(username) FROM app_users WHERE is_active=TRUE)`);
    }
    if (motivo && motivo !== "__ALL__") {
      where.push(`r.motivo = $${p++}`);
      params.push(motivo);
    }

    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

    const totalsR = await dbQuery(
      `SELECT
        COUNT(*)::int AS requests,
        COALESCE(SUM(r.total_amount),0)::float AS total_amount,
        COALESCE(SUM(r.total_qty),0)::float AS total_qty,
        CASE WHEN COUNT(*)>0 THEN (COALESCE(SUM(r.total_amount),0) / COUNT(*))::float ELSE 0 END AS avg_amount
       FROM return_requests r
       ${whereSql}`,
      params
    );
    const totals = totalsR.rows?.[0] || { requests: 0, total_amount: 0, total_qty: 0, avg_amount: 0 };

    const byStatusR = await dbQuery(
      `SELECT COALESCE(NULLIF(r.status,''),'Open') AS status,
              COUNT(*)::int AS cnt,
              COALESCE(SUM(r.total_amount),0)::float AS amount
       FROM return_requests r
       ${whereSql}
       GROUP BY 1
       ORDER BY amount DESC`,
      params
    );

    const byMotivoR = await dbQuery(
      `SELECT COALESCE(NULLIF(r.motivo,''),'Sin motivo') AS motivo,
              COUNT(*)::int AS cnt,
              COALESCE(SUM(r.total_amount),0)::float AS amount
       FROM return_requests r
       ${whereSql}
       GROUP BY 1
       ORDER BY amount DESC
       LIMIT 2000`,
      params
    );

    const byCausaR = await dbQuery(
      `SELECT COALESCE(NULLIF(r.causa,''),'Sin causa') AS causa,
              COUNT(*)::int AS cnt,
              COALESCE(SUM(r.total_amount),0)::float AS amount
       FROM return_requests r
       ${whereSql}
       GROUP BY 1
       ORDER BY amount DESC
       LIMIT 2000`,
      params
    );

    const byUserR = await dbQuery(
      `SELECT COALESCE(NULLIF(r.usuario,''),'sin_user') AS usuario,
              COUNT(*)::int AS cnt,
              COALESCE(SUM(r.total_amount),0)::float AS amount
       FROM return_requests r
       ${whereSql}
       GROUP BY 1
       ORDER BY amount DESC
       LIMIT 2000`,
      params
    );

    const byWhR = await dbQuery(
      `SELECT COALESCE(NULLIF(r.warehouse,''),'sin_wh') AS warehouse,
              COUNT(*)::int AS cnt,
              COALESCE(SUM(r.total_amount),0)::float AS amount
       FROM return_requests r
       ${whereSql}
       GROUP BY 1
       ORDER BY amount DESC
       LIMIT 2000`,
      params
    );

    const byClientR = await dbQuery(
      `SELECT COALESCE(NULLIF(r.card_name,''), r.card_code, 'sin_cliente') AS customer,
              COUNT(*)::int AS cnt,
              COALESCE(SUM(r.total_amount),0)::float AS amount
       FROM return_requests r
       ${whereSql}
       GROUP BY 1
       ORDER BY amount DESC
       LIMIT 2000`,
      params
    );

    return safeJson(res, 200, {
      ok: true,
      from,
      to,
      totals: {
        requests: Number(totals.requests || 0),
        totalAmount: Number(totals.total_amount || 0),
        totalQty: Number(totals.total_qty || 0),
        avgAmount: Number(totals.avg_amount || 0),
      },
      byStatus: (byStatusR.rows || []).map((x) => ({
        status: x.status,
        cnt: Number(x.cnt || 0),
        amount: Number(x.amount || 0),
      })),
      byMotivo: (byMotivoR.rows || []).map((x) => ({
        motivo: x.motivo,
        cnt: Number(x.cnt || 0),
        amount: Number(x.amount || 0),
      })),
      byCausa: (byCausaR.rows || []).map((x) => ({
        causa: x.causa,
        cnt: Number(x.cnt || 0),
        amount: Number(x.amount || 0),
      })),
      byUser: (byUserR.rows || []).map((x) => ({
        usuario: x.usuario,
        cnt: Number(x.cnt || 0),
        amount: Number(x.amount || 0),
      })),
      byWh: (byWhR.rows || []).map((x) => ({
        warehouse: x.warehouse,
        cnt: Number(x.cnt || 0),
        amount: Number(x.amount || 0),
      })),
      byClient: (byClientR.rows || []).map((x) => ({
        customer: x.customer,
        cnt: Number(x.cnt || 0),
        amount: Number(x.amount || 0),
      })),
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================
   HISTÓRICO (DB)
   GET /api/admin/returns/db?...&skip&limit&openOnly=1
========================= */
app.get("/api/admin/returns/db", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const fromQ = String(req.query?.from || "").slice(0, 10);
    const toQ = String(req.query?.to || "").slice(0, 10);
    const user = String(req.query?.user || "").trim().toLowerCase();
    const client = String(req.query?.client || "").trim().toLowerCase();
    const motivo = String(req.query?.motivo || "").trim();
    const causa = String(req.query?.causa || "").trim();

    const onlyCreated = String(req.query?.onlyCreated || "0") === "1";
    const openOnly = String(req.query?.openOnly || "0") === "1";

    const skipRaw = Number(req.query?.skip || 0);
    const limitRaw = Number(req.query?.limit || 20);
    const skip = Math.max(0, Number.isFinite(skipRaw) ? Math.trunc(skipRaw) : 0);
    const limit = Math.max(1, Math.min(200, Number.isFinite(limitRaw) ? Math.trunc(limitRaw) : 20));

    const now = new Date();
    const fromDef = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-01`;
    const toDef = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(
      new Date(now.getFullYear(), now.getMonth() + 1, 0).getDate()
    ).padStart(2, "0")}`;

    const from = /^\d{4}-\d{2}-\d{2}$/.test(fromQ) ? fromQ : fromDef;
    const to = /^\d{4}-\d{2}-\d{2}$/.test(toQ) ? toQ : toDef;

    const where = [];
    const params = [];
    let p = 1;

    where.push(`r.doc_date BETWEEN $${p++} AND $${p++}`);
    params.push(from, to);

    if (onlyCreated) {
      where.push(`LOWER(COALESCE(r.usuario,'')) IN (SELECT LOWER(username) FROM app_users WHERE is_active=TRUE)`);
    }
    if (user) {
      where.push(`LOWER(COALESCE(r.usuario,'')) LIKE $${p++}`);
      params.push(`%${user}%`);
    }
    if (client) {
      where.push(`(LOWER(COALESCE(r.card_code,'')) LIKE $${p++} OR LOWER(COALESCE(r.card_name,'')) LIKE $${p++})`);
      params.push(`%${client}%`, `%${client}%`);
    }
    if (motivo && motivo !== "__ALL__") {
      where.push(`r.motivo = $${p++}`);
      params.push(motivo);
    }
    if (causa && causa !== "__ALL__") {
      where.push(`r.causa = $${p++}`);
      params.push(causa);
    }
    if (openOnly) {
      where.push(`LOWER(COALESCE(r.status,'')) LIKE '%open%'`);
    }

    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

    const totalR = await dbQuery(`SELECT COUNT(*)::int AS total FROM return_requests r ${whereSql}`, params);
    const total = Number(totalR.rows?.[0]?.total || 0);

    const dataR = await dbQuery(
      `SELECT
        r.req_num AS "reqNum",
        r.req_entry AS "reqEntry",
        r.doc_date AS "fecha",
        r.card_code AS "cardCode",
        r.card_name AS "cardName",
        r.usuario AS "usuario",
        r.warehouse AS "warehouse",
        r.motivo AS "motivo",
        r.causa AS "causa",
        r.status AS "status",
        r.total_amount::float AS "totalAmount",
        r.total_qty::float AS "totalQty",
        r.comments AS "comments"
       FROM return_requests r
       ${whereSql}
       ORDER BY r.doc_date DESC, r.req_num DESC
       OFFSET $${p++} LIMIT $${p++}`,
      [...params, skip, limit]
    );

    return safeJson(res, 200, { ok: true, from, to, total, skip, limit, rows: dataR.rows || [] });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================
   LÍNEAS (DB)
========================= */
app.get("/api/admin/returns/lines", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const reqNum = Number(req.query?.reqNum || 0);
    if (!Number.isFinite(reqNum) || reqNum <= 0) return safeJson(res, 400, { ok: false, message: "reqNum inválido" });

    const headR = await dbQuery(
      `SELECT req_num AS "reqNum", doc_date AS "docDate", status AS "status"
       FROM return_requests
       WHERE req_num=$1
       LIMIT 1`,
      [reqNum]
    );
    const head = headR.rows?.[0];
    if (!head) return safeJson(res, 404, { ok: false, message: "Solicitud no encontrada" });

    const linesR = await dbQuery(
      `SELECT
        req_num AS "reqNum",
        doc_date AS "docDate",
        item_code AS "itemCode",
        item_desc AS "itemDesc",
        qty::float AS "qty",
        price::float AS "price",
        line_total::float AS "lineTotal"
       FROM return_lines
       WHERE req_num=$1
       ORDER BY line_total DESC, item_code ASC`,
      [reqNum]
    );

    return safeJson(res, 200, {
      ok: true,
      reqNum: head.reqNum,
      docDate: head.docDate,
      status: head.status || "Open",
      lines: linesR.rows || [],
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================
   CAMBIAR ESTADO (DB)
========================= */
app.patch("/api/admin/returns/:reqNum/status", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const reqNum = Number(req.params.reqNum || 0);
    if (!Number.isFinite(reqNum) || reqNum <= 0) return safeJson(res, 400, { ok: false, message: "reqNum inválido" });

    const status = String(req.body?.status || "").trim().toLowerCase();
    const finalStatus = status.includes("open") ? "Open" : status.includes("close") ? "Closed" : "";
    if (!finalStatus) return safeJson(res, 400, { ok: false, message: "status inválido (usa Open/Closed)" });

    const r = await dbQuery(
      `UPDATE return_requests
       SET status=$2, updated_at=NOW()
       WHERE req_num=$1
       RETURNING req_num AS "reqNum", status`,
      [reqNum, finalStatus]
    );

    if (!r.rowCount) return safeJson(res, 404, { ok: false, message: "Solicitud no encontrada" });
    return safeJson(res, 200, { ok: true, reqNum, status: r.rows[0].status });
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

  app.listen(Number(PORT), () => console.log(`ADMIN DEVOLUCIONES API listening on :${PORT}`));
})();
