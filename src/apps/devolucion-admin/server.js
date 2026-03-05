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
  // Render: CORS_ORIGIN=https://tu-dominio.com,https://www.tu-dominio.com
  CORS_ORIGIN = "",
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

  // Users
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

  // Return requests header
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS return_requests (
      req_num BIGSERIAL PRIMARY KEY,
      req_date DATE,
      usuario TEXT,
      full_name TEXT DEFAULT '',
      province TEXT DEFAULT '',
      warehouse TEXT DEFAULT '',
      card_code TEXT DEFAULT '',
      card_name TEXT DEFAULT '',
      motivo TEXT NOT NULL,
      causa TEXT NOT NULL,
      comments TEXT DEFAULT '',
      status TEXT DEFAULT 'Open',
      lines_count INT DEFAULT 0,
      total_qty NUMERIC(19,6) DEFAULT 0,
      total_amount NUMERIC(19,6) DEFAULT 0,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  // Return request lines
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS return_request_lines (
      id BIGSERIAL PRIMARY KEY,
      req_num BIGINT NOT NULL REFERENCES return_requests(req_num) ON DELETE CASCADE,
      item_code TEXT NOT NULL,
      item_desc TEXT DEFAULT '',
      qty NUMERIC(19,6) DEFAULT 0,
      price NUMERIC(19,6) DEFAULT 0,
      amount NUMERIC(19,6) DEFAULT 0,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_ret_date ON return_requests(req_date);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_ret_user ON return_requests(usuario);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_ret_wh ON return_requests(warehouse);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_ret_card ON return_requests(card_code);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_ret_lines_req ON return_request_lines(req_num);`);
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

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ");
}

/* =========================
   HEALTH
========================= */
app.get("/api/health", async (req, res) => {
  safeJson(res, 200, {
    ok: true,
    message: "✅ PRODIMA DEVOLUCIONES ADMIN API activa",
    db: hasDb() ? "on" : "off",
  });
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
   USERS
========================= */
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
    if (!hasDb()) return safeJson(res, 500, { ok:false, message:"DB no configurada" });

    const username = String(req.body?.username || "").trim().toLowerCase();
    const full_name = String(req.body?.full_name ?? req.body?.fullName ?? "").trim();
    const pin = String(req.body?.pin || "").trim();
    const province = String(req.body?.province || "").trim();

    if(!username) return safeJson(res, 400, { ok:false, message:"Username requerido" });
    if(!full_name) return safeJson(res, 400, { ok:false, message:"Nombre requerido" });
    if(!pin || pin.length < 4) return safeJson(res, 400, { ok:false, message:"PIN mínimo 4" });
    if(!province) return safeJson(res, 400, { ok:false, message:"Provincia requerida" });

    const warehouse_code = provinceToWarehouseServer(province);
    const pin_hash = await hashPin(pin);

    const r = await dbQuery(
      `INSERT INTO app_users(username, full_name, pin_hash, province, warehouse_code, is_active)
       VALUES ($1,$2,$3,$4,$5,TRUE)
       RETURNING id, username, full_name, province, warehouse_code, is_active, created_at`,
      [username, full_name, pin_hash, province, warehouse_code]
    );

    return safeJson(res, 200, { ok:true, user: r.rows[0] });
  } catch (e) {
    if (String(e?.code) === "23505") return safeJson(res, 409, { ok:false, message:"Ese username ya existe" });
    return safeJson(res, 500, { ok:false, message: e.message || String(e) });
  }
});

app.patch("/api/admin/users/:id/toggle", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok:false, message:"DB no configurada" });

    const id = Number(req.params.id || 0);
    if(!Number.isFinite(id) || id <= 0) return safeJson(res, 400, { ok:false, message:"ID inválido" });

    const r = await dbQuery(
      `UPDATE app_users
       SET is_active = NOT is_active
       WHERE id = $1
       RETURNING id, username, full_name, province, warehouse_code, is_active, created_at`,
      [id]
    );

    if(!r.rowCount) return safeJson(res, 404, { ok:false, message:"Usuario no encontrado" });
    return safeJson(res, 200, { ok:true, user: r.rows[0] });
  } catch (e) {
    return safeJson(res, 500, { ok:false, message: e.message || String(e) });
  }
});

app.delete("/api/admin/users/:id", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok:false, message:"DB no configurada" });

    const id = Number(req.params.id || 0);
    if(!Number.isFinite(id) || id <= 0) return safeJson(res, 400, { ok:false, message:"ID inválido" });

    const r = await dbQuery(`DELETE FROM app_users WHERE id=$1`, [id]);
    if(!r.rowCount) return safeJson(res, 404, { ok:false, message:"Usuario no encontrado" });

    return safeJson(res, 200, { ok:true });
  } catch (e) {
    return safeJson(res, 500, { ok:false, message: e.message || String(e) });
  }
});

app.patch("/api/admin/users/:id/pin", verifyAdmin, async (req, res) => {
  try{
    if (!hasDb()) return safeJson(res, 500, { ok:false, message:"DB no configurada" });

    const id = Number(req.params.id || 0);
    const pin = String(req.body?.pin || "").trim();

    if(!Number.isFinite(id) || id <= 0) return safeJson(res, 400, { ok:false, message:"ID inválido" });
    if(!pin || pin.length < 4) return safeJson(res, 400, { ok:false, message:"PIN mínimo 4" });

    const pin_hash = await hashPin(pin);

    const r = await dbQuery(
      `UPDATE app_users SET pin_hash=$2 WHERE id=$1`,
      [id, pin_hash]
    );

    if(!r.rowCount) return safeJson(res, 404, { ok:false, message:"Usuario no encontrado" });
    return safeJson(res, 200, { ok:true });
  }catch(e){
    return safeJson(res, 500, { ok:false, message: e.message || String(e) });
  }
});

/* =========================
   DEVOLUCIONES: META
========================= */
app.get("/api/admin/returns/meta", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok:false, message:"DB no configurada" });

    const m = await dbQuery(`SELECT DISTINCT motivo FROM return_requests WHERE motivo IS NOT NULL AND motivo <> '' ORDER BY motivo ASC`);
    const c = await dbQuery(`SELECT DISTINCT causa  FROM return_requests WHERE causa  IS NOT NULL AND causa  <> '' ORDER BY causa  ASC`);

    return safeJson(res, 200, {
      ok: true,
      motivos: (m.rows||[]).map(x=>x.motivo),
      causas:  (c.rows||[]).map(x=>x.causa),
    });
  } catch (e) {
    return safeJson(res, 500, { ok:false, message: e.message || String(e) });
  }
});

/* =========================
   DEVOLUCIONES: DASHBOARD DB
   GET /api/admin/returns/dashboard-db?from&to&onlyCreated=1&motivo=...
========================= */
app.get("/api/admin/returns/dashboard-db", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok:false, message:"DB no configurada" });

    const fromQ = String(req.query?.from || "").slice(0, 10);
    const toQ = String(req.query?.to || "").slice(0, 10);
    const onlyCreated = String(req.query?.onlyCreated || "0") === "1";
    const motivo = String(req.query?.motivo || "").trim();

    const today = new Date();
    const yyyy = today.getFullYear();
    const mm = String(today.getMonth()+1).padStart(2,"0");
    const dd = String(today.getDate()).padStart(2,"0");
    const toDefault = `${yyyy}-${mm}-${dd}`;
    const fromDefault = `${yyyy}-${mm}-01`;

    const from = /^\d{4}-\d{2}-\d{2}$/.test(fromQ) ? fromQ : fromDefault;
    const to = /^\d{4}-\d{2}-\d{2}$/.test(toQ) ? toQ : toDefault;

    const where = [];
    const params = [];
    let p = 1;

    where.push(`r.req_date BETWEEN $${p++} AND $${p++}`);
    params.push(from, to);

    if (onlyCreated) {
      where.push(`LOWER(COALESCE(r.usuario,'')) IN (SELECT LOWER(username) FROM app_users WHERE is_active=TRUE)`);
    }
    if (motivo) {
      where.push(`r.motivo = $${p++}`);
      params.push(motivo);
    }

    const whereSql = `WHERE ${where.join(" AND ")}`;

    const totalsR = await dbQuery(
      `SELECT
        COUNT(*)::int AS requests,
        COALESCE(SUM(r.total_amount),0)::float AS amount,
        COALESCE(SUM(r.total_qty),0)::float AS qty
       FROM return_requests r
       ${whereSql}`,
      params
    );

    const byUserR = await dbQuery(
      `SELECT
        COALESCE(NULLIF(r.usuario,''),'sin_user') AS usuario,
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
      `SELECT
        COALESCE(NULLIF(r.warehouse,''),'sin_wh') AS warehouse,
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
      `SELECT
        COALESCE(NULLIF(r.card_name,''), r.card_code, 'sin_cliente') AS customer,
        COUNT(*)::int AS cnt,
        COALESCE(SUM(r.total_amount),0)::float AS amount
       FROM return_requests r
       ${whereSql}
       GROUP BY 1
       ORDER BY amount DESC
       LIMIT 2000`,
      params
    );

    const byMotivoR = await dbQuery(
      `SELECT
        COALESCE(NULLIF(r.motivo,''),'Sin motivo') AS motivo,
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
      `SELECT
        COALESCE(NULLIF(r.causa,''),'Sin causa') AS causa,
        COUNT(*)::int AS cnt,
        COALESCE(SUM(r.total_amount),0)::float AS amount
       FROM return_requests r
       ${whereSql}
       GROUP BY 1
       ORDER BY amount DESC
       LIMIT 2000`,
      params
    );

    const byStatusR = await dbQuery(
      `SELECT
        COALESCE(NULLIF(r.status,''),'Unknown') AS status,
        COUNT(*)::int AS cnt
       FROM return_requests r
       ${whereSql}
       GROUP BY 1
       ORDER BY cnt DESC`,
      params
    );

    const totals = totalsR.rows?.[0] || { requests:0, amount:0, qty:0 };

    return safeJson(res, 200, {
      ok: true,
      from, to,
      totals: {
        requests: Number(totals.requests||0),
        amount: Number(totals.amount||0),
        qty: Number(totals.qty||0),
      },
      byUser: (byUserR.rows||[]).map(r=>({ usuario:r.usuario, cnt:Number(r.cnt||0), amount:Number(r.amount||0) })),
      byWh: (byWhR.rows||[]).map(r=>({ warehouse:r.warehouse, cnt:Number(r.cnt||0), amount:Number(r.amount||0) })),
      byClient: (byClientR.rows||[]).map(r=>({ customer:r.customer, cnt:Number(r.cnt||0), amount:Number(r.amount||0) })),
      byMotivo: (byMotivoR.rows||[]).map(r=>({ motivo:r.motivo, cnt:Number(r.cnt||0), amount:Number(r.amount||0) })),
      byCausa: (byCausaR.rows||[]).map(r=>({ causa:r.causa, cnt:Number(r.cnt||0), amount:Number(r.amount||0) })),
      byStatus: (byStatusR.rows||[]).map(r=>({ status:r.status, cnt:Number(r.cnt||0) })),
    });
  } catch (e) {
    return safeJson(res, 500, { ok:false, message: e.message || String(e) });
  }
});

/* =========================
   DEVOLUCIONES: HISTÓRICO DB (PAGINADO)
   GET /api/admin/returns/db?from&to&user&client&motivo&causa&skip&limit&onlyCreated=1&openOnly=1
========================= */
app.get("/api/admin/returns/db", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok:false, message:"DB no configurada" });

    const fromQ = String(req.query?.from || "").slice(0, 10);
    const toQ = String(req.query?.to || "").slice(0, 10);
    const user = String(req.query?.user || "").trim().toLowerCase();
    const client = String(req.query?.client || "").trim().toLowerCase();
    const motivo = String(req.query?.motivo || "").trim();
    const causa = String(req.query?.causa || "").trim();

    const skipRaw = Number(req.query?.skip || 0);
    const limitRaw = Number(req.query?.limit || 20);
    const skip = Math.max(0, Number.isFinite(skipRaw) ? Math.trunc(skipRaw) : 0);
    const limit = Math.max(1, Math.min(200, Number.isFinite(limitRaw) ? Math.trunc(limitRaw) : 20));

    const onlyCreated = String(req.query?.onlyCreated || "0") === "1";
    const openOnly = String(req.query?.openOnly || "0") === "1";

    const today = new Date();
    const yyyy = today.getFullYear();
    const mm = String(today.getMonth()+1).padStart(2,"0");
    const dd = String(today.getDate()).padStart(2,"0");
    const toDefault = `${yyyy}-${mm}-${dd}`;
    const fromDefault = `${yyyy}-${mm}-01`;

    const from = /^\d{4}-\d{2}-\d{2}$/.test(fromQ) ? fromQ : fromDefault;
    const to = /^\d{4}-\d{2}-\d{2}$/.test(toQ) ? toQ : toDefault;

    const where = [];
    const params = [];
    let p = 1;

    where.push(`r.req_date BETWEEN $${p++} AND $${p++}`);
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
    if (motivo) { where.push(`r.motivo = $${p++}`); params.push(motivo); }
    if (causa)  { where.push(`r.causa  = $${p++}`); params.push(causa); }
    if (openOnly) where.push(`LOWER(COALESCE(r.status,'')) LIKE '%open%'`);

    const whereSql = `WHERE ${where.join(" AND ")}`;

    const totalR = await dbQuery(`SELECT COUNT(*)::int AS total FROM return_requests r ${whereSql}`, params);
    const total = Number(totalR.rows?.[0]?.total || 0);

    const dataR = await dbQuery(
      `SELECT
        r.req_num AS "reqNum",
        r.req_date AS "fecha",
        r.card_code AS "cardCode",
        r.card_name AS "cardName",
        r.usuario AS "usuario",
        r.warehouse AS "warehouse",
        r.motivo AS "motivo",
        r.causa AS "causa",
        r.status AS "status",
        r.comments AS "comments",
        r.total_qty::float AS "totalQty",
        r.total_amount::float AS "totalAmount"
       FROM return_requests r
       ${whereSql}
       ORDER BY r.req_date DESC, r.req_num DESC
       OFFSET $${p++} LIMIT $${p++}`,
      [...params, skip, limit]
    );

    return safeJson(res, 200, {
      ok: true,
      from, to,
      total,
      skip, limit,
      rows: dataR.rows || [],
    });
  } catch (e) {
    return safeJson(res, 500, { ok:false, message: e.message || String(e) });
  }
});

/* =========================
   DEVOLUCIONES: LÍNEAS
   GET /api/admin/returns/lines?reqNum=123
========================= */
app.get("/api/admin/returns/lines", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok:false, message:"DB no configurada" });

    const reqNum = Number(req.query?.reqNum || 0);
    if (!Number.isFinite(reqNum) || reqNum <= 0) return safeJson(res, 400, { ok:false, message:"reqNum inválido" });

    const headR = await dbQuery(
      `SELECT
        req_num AS "reqNum",
        req_date AS "fecha",
        card_code AS "cardCode",
        card_name AS "cardName",
        usuario AS "usuario",
        warehouse AS "warehouse",
        motivo AS "motivo",
        causa AS "causa",
        status AS "status",
        comments AS "comments",
        total_qty::float AS "totalQty",
        total_amount::float AS "totalAmount"
       FROM return_requests
       WHERE req_num=$1
       LIMIT 1`,
      [reqNum]
    );
    const header = headR.rows?.[0];
    if (!header) return safeJson(res, 404, { ok:false, message:"Solicitud no encontrada" });

    const linesR = await dbQuery(
      `SELECT
        item_code AS "itemCode",
        item_desc AS "itemDesc",
        qty::float AS "qty",
        price::float AS "price",
        amount::float AS "amount"
       FROM return_request_lines
       WHERE req_num=$1
       ORDER BY amount DESC, item_code ASC`,
      [reqNum]
    );

    return safeJson(res, 200, { ok:true, header, lines: linesR.rows || [] });
  } catch (e) {
    return safeJson(res, 500, { ok:false, message: e.message || String(e) });
  }
});

/* =========================
   DEVOLUCIONES: TOGGLE STATUS
   PATCH /api/admin/returns/:reqNum/toggle
========================= */
app.patch("/api/admin/returns/:reqNum/toggle", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok:false, message:"DB no configurada" });

    const reqNum = Number(req.params.reqNum || 0);
    if (!Number.isFinite(reqNum) || reqNum <= 0) return safeJson(res, 400, { ok:false, message:"reqNum inválido" });

    const r = await dbQuery(
      `UPDATE return_requests
       SET status = CASE
         WHEN LOWER(COALESCE(status,'')) LIKE '%open%' THEN 'Closed'
         ELSE 'Open'
       END,
       updated_at = NOW()
       WHERE req_num=$1
       RETURNING req_num, status`,
      [reqNum]
    );

    if (!r.rowCount) return safeJson(res, 404, { ok:false, message:"Solicitud no encontrada" });
    return safeJson(res, 200, { ok:true, reqNum: r.rows[0].req_num, status: r.rows[0].status });
  } catch (e) {
    return safeJson(res, 500, { ok:false, message: e.message || String(e) });
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

  app.listen(Number(PORT), () => console.log(`DEVOLUCIONES ADMIN API listening on :${PORT}`));
})();
