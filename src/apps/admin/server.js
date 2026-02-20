// src/apps/admin/server.js
import express from "express";
import cors from "cors";
import pg from "pg";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";

const { Pool } = pg;

const app = express();
app.use(express.json({ limit: "2mb" }));

/* =========================================================
   ✅ ENV
========================================================= */
const {
  PORT = 3000,
  CORS_ORIGIN = "*",

  DATABASE_URL = "",
  JWT_SECRET = "change_me",

  ADMIN_USER = "PRODIMA",
  ADMIN_PASS = "ADMINISTRADOR",

  SAP_BASE_URL = "",
  SAP_COMPANYDB = "",
  SAP_USER = "",
  SAP_PASS = "",

  SAP_WAREHOUSE = "300",
  SAP_PRICE_LIST = "Lista de Precios 99 2018",
} = process.env;

/* =========================================================
   ✅ CORS
========================================================= */
app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? true : CORS_ORIGIN,
    credentials: false,
  })
);

/* =========================================================
   ✅ DB (Postgres)
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

  await pool.query(`
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
}

/* =========================================================
   ✅ Helpers
========================================================= */
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

function missingSapEnv() {
  return !SAP_BASE_URL || !SAP_COMPANYDB || !SAP_USER || !SAP_PASS;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/* =========================================================
   ✅ Time helpers (Panamá UTC-5)
========================================================= */
const TZ_OFFSET_MIN = -300;

function getDateISOInOffset(offsetMin = 0) {
  const now = new Date();
  const ms =
    now.getTime() + now.getTimezoneOffset() * 60_000 + Number(offsetMin) * 60_000;
  const d = new Date(ms);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

function addDaysISO(iso, days) {
  const d = new Date(String(iso || "").slice(0, 10));
  if (Number.isNaN(d.getTime())) return "";
  d.setDate(d.getDate() + Number(days || 0));
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

/* =========================================================
   ✅ Parse helpers (Comments)
========================================================= */
function parseUserFromComments(comments) {
  const m = String(comments || "").match(/\[user:([^\]]+)\]/i);
  return m ? String(m[1]).trim() : "";
}

function parseWhFromComments(comments) {
  const m = String(comments || "").match(/\[wh:([^\]]+)\]/i);
  return m ? String(m[1]).trim() : "";
}

function isCancelledLike(q) {
  const cancelVal =
    q?.CancelStatus ?? q?.cancelStatus ?? q?.Cancelled ?? q?.cancelled ?? "";
  const cancelRaw = String(cancelVal).trim().toLowerCase();
  const commLower = String(q?.Comments || q?.comments || "").toLowerCase();
  return (
    cancelRaw === "csyes" ||
    cancelRaw === "yes" ||
    cancelRaw === "true" ||
    cancelRaw.includes("csyes") ||
    cancelRaw.includes("cancel") ||
    commLower.includes("[cancel") ||
    commLower.includes("cancelad")
  );
}

/* =========================================================
   ✅ SAP Service Layer (Session cookie)
========================================================= */
let SL_COOKIE = "";
let SL_COOKIE_AT = 0;

async function slLogin() {
  const url = `${SAP_BASE_URL.replace(/\/$/, "")}/Login`;
  const body = {
    CompanyDB: SAP_COMPANYDB,
    UserName: SAP_USER,
    Password: SAP_PASS,
  };

  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  const txt = await r.text();
  let data = {};
  try {
    data = JSON.parse(txt);
  } catch {}

  if (!r.ok) {
    throw new Error(`SAP login failed: HTTP ${r.status} ${data?.error?.message?.value || txt}`);
  }

  const setCookie = r.headers.get("set-cookie") || "";
  const cookies = [];
  for (const part of setCookie.split(",")) {
    const s = part.trim();
    if (s.startsWith("B1SESSION=") || s.startsWith("ROUTEID=")) cookies.push(s.split(";")[0]);
  }
  SL_COOKIE = cookies.join("; ");
  SL_COOKIE_AT = Date.now();
  return true;
}

async function slFetch(path, options = {}) {
  if (missingSapEnv()) throw new Error("Missing SAP env");

  if (!SL_COOKIE || Date.now() - SL_COOKIE_AT > 25 * 60 * 1000) {
    await slLogin();
  }

  const base = SAP_BASE_URL.replace(/\/$/, "");
  const url = `${base}${path.startsWith("/") ? path : `/${path}`}`;

  const method = String(options.method || "GET").toUpperCase();

  const r = await fetch(url, {
    method,
    headers: {
      "Content-Type": "application/json",
      Cookie: SL_COOKIE,
      ...(options.headers || {}),
    },
    body: options.body,
  });

  const txt = await r.text();
  let data = {};
  try {
    data = JSON.parse(txt);
  } catch {
    data = { raw: txt };
  }

  if (!r.ok) {
    if (r.status === 401 || r.status === 403) {
      SL_COOKIE = "";
      await slLogin();
      return slFetch(path, options);
    }
    throw new Error(`SAP error ${r.status}: ${data?.error?.message?.value || txt}`);
  }

  return data;
}

/* =========================================================
   ✅ HEALTH
========================================================= */
app.get("/api/health", async (req, res) => {
  safeJson(res, 200, {
    ok: true,
    app: "admin-api",
    message: "✅ PRODIMA ADMIN API activa",
    db: hasDb() ? "on" : "off",
    priceList: SAP_PRICE_LIST,
    warehouse_default: SAP_WAREHOUSE,
  });
});

/* =========================================================
   ✅ ADMIN LOGIN
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
   ✅ ADMIN USERS (NO TOCADO)
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

function normalizeUsername(u) {
  const s = String(u || "").trim().toLowerCase();
  if (!s) return "";
  if (!/^[a-z0-9._-]{2,50}$/.test(s)) return "__INVALID__";
  return s;
}

function toIntId(x) {
  const n = Number(x);
  return Number.isFinite(n) && n > 0 ? Math.trunc(n) : null;
}

app.post("/api/admin/users", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const username = normalizeUsername(req.body?.username);
    const full_name = String(req.body?.full_name || req.body?.fullName || "").trim();
    const province = String(req.body?.province || "").trim();
    const pin = String(req.body?.pin || "").trim();
    const warehouse_code = String(req.body?.warehouse_code || req.body?.warehouse || "").trim();

    if (!username || username === "__INVALID__") {
      return safeJson(res, 400, {
        ok: false,
        message: "Username inválido. Usa letras/números y . _ - (mín 2).",
      });
    }
    if (!pin || pin.length < 4) {
      return safeJson(res, 400, { ok: false, message: "PIN mínimo 4" });
    }

    const pin_hash = await bcrypt.hash(pin, 10);

    const q = await dbQuery(
      `INSERT INTO app_users (username, full_name, pin_hash, province, warehouse_code, is_active)
       VALUES ($1,$2,$3,$4,$5,TRUE)
       RETURNING id, username, full_name, province, warehouse_code, is_active, created_at`,
      [username, full_name, pin_hash, province, warehouse_code]
    );

    return safeJson(res, 200, { ok: true, user: q.rows[0] });
  } catch (e) {
    if (String(e?.code) === "23505") {
      return safeJson(res, 409, { ok: false, message: "Ese username ya existe" });
    }
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.patch("/api/admin/users/:id/pin", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const id = toIntId(req.params.id);
    if (!id) return safeJson(res, 400, { ok: false, message: "ID inválido" });

    const pin = String(req.body?.pin || "").trim();
    if (!pin || pin.length < 4) {
      return safeJson(res, 400, { ok: false, message: "PIN mínimo 4" });
    }

    const pin_hash = await bcrypt.hash(pin, 10);

    const r = await dbQuery(
      `UPDATE app_users SET pin_hash=$1 WHERE id=$2
       RETURNING id, username, full_name, province, warehouse_code, is_active, created_at`,
      [pin_hash, id]
    );

    if (!r.rows?.length) return safeJson(res, 404, { ok: false, message: "Usuario no existe" });
    return safeJson(res, 200, { ok: true, user: r.rows[0] });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.patch("/api/admin/users/:id/toggle", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const id = toIntId(req.params.id);
    if (!id) return safeJson(res, 400, { ok: false, message: "ID inválido" });

    const r = await dbQuery(
      `UPDATE app_users
       SET is_active = NOT is_active
       WHERE id=$1
       RETURNING id, username, full_name, province, warehouse_code, is_active, created_at`,
      [id]
    );

    if (!r.rows?.length) return safeJson(res, 404, { ok: false, message: "Usuario no existe" });
    return safeJson(res, 200, { ok: true, user: r.rows[0] });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.delete("/api/admin/users/:id", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const id = toIntId(req.params.id);
    if (!id) return safeJson(res, 400, { ok: false, message: "ID inválido" });

    const r = await dbQuery(`DELETE FROM app_users WHERE id=$1 RETURNING id`, [id]);

    if (!r.rows?.length) return safeJson(res, 404, { ok: false, message: "Usuario no existe" });
    return safeJson(res, 200, { ok: true });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ ADMIN QUOTES (CRAWLER)
   - Lee muchas páginas (maxPages) de SAP
   - Filtra en servidor
   - Pagina para el frontend
========================================================= */
async function crawlQuotesFromSap({
  from,
  to,
  maxPages,
  batchTop,
  userFilter,
  clientFilter,
}) {
  const f = String(from);
  const t = String(to);
  const toPlus1 = addDaysISO(t, 1);

  let skipSap = 0;
  const all = [];
  const seenDocEntry = new Set();

  const uFilter = String(userFilter || "").trim().toLowerCase();
  const cFilter = String(clientFilter || "").trim().toLowerCase();

  for (let page = 0; page < maxPages; page++) {
    const raw = await slFetch(
      `/Quotations?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
        `&$filter=${encodeURIComponent(`DocDate ge '${f}' and DocDate lt '${toPlus1}'`)}` +
        `&$orderby=DocDate desc,DocEntry desc&$top=${batchTop}&$skip=${skipSap}`
    );

    const rows = Array.isArray(raw?.value) ? raw.value : [];
    if (!rows.length) break;

    skipSap += rows.length;

    for (const q of rows) {
      const de = Number(q?.DocEntry);
      if (Number.isFinite(de)) {
        if (seenDocEntry.has(de)) continue;
        seenDocEntry.add(de);
      }

      if (isCancelledLike(q)) continue;

      const usuario = parseUserFromComments(q.Comments || "") || "sin_user";
      const wh = parseWhFromComments(q.Comments || "") || "sin_wh";

      if (uFilter && !String(usuario).toLowerCase().includes(uFilter)) continue;

      if (cFilter) {
        const cc = String(q.CardCode || "").toLowerCase();
        const cn = String(q.CardName || "").toLowerCase();
        if (!cc.includes(cFilter) && !cn.includes(cFilter)) continue;
      }

      all.push({
        docEntry: q.DocEntry,
        docNum: q.DocNum,
        cardCode: String(q.CardCode || "").trim(),
        cardName: String(q.CardName || "").trim(),
        montoCotizacion: Number(q.DocTotal || 0),
        fecha: String(q.DocDate || "").slice(0, 10),
        estado: q.DocumentStatus || "",
        cancelStatus: q.CancelStatus ?? "",
        comments: q.Comments || "",
        usuario,
        warehouse: wh,
      });
    }

    // pequeño respiro para no saturar SAP
    await sleep(25);
  }

  return all;
}

app.get("/api/admin/quotes", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) {
      return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });
    }

    // ✅ Paginación frontend
    const limit = Math.min(Math.max(Number(req.query?.limit || 50), 1), 200);
    const page = Math.max(1, Number(req.query?.page || 1));
    const wantSkip = (page - 1) * limit;

    // ✅ “Sin rango” práctico: por defecto desde 2016-01-01 hasta hoy
    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const fromIn = String(req.query?.from || "").trim();
    const toIn = String(req.query?.to || "").trim();

    const f = /^\d{4}-\d{2}-\d{2}$/.test(fromIn) ? fromIn : "2016-01-01";
    const t = /^\d{4}-\d{2}-\d{2}$/.test(toIn) ? toIn : today;

    // ✅ Control de “cuántas páginas de SAP leer”
    // Ej: maxPages=100 => hasta 100*200=20,000 registros
    const maxPages = Math.min(Math.max(Number(req.query?.maxPages || 100), 1), 300);
    const batchTop = 200;

    const userFilter = String(req.query?.user || "").trim().toLowerCase();
    const clientFilter = String(req.query?.client || "").trim().toLowerCase();

    // 1) Crawl grande
    const all = await crawlQuotesFromSap({
      from: f,
      to: t,
      maxPages,
      batchTop,
      userFilter,
      clientFilter,
    });

    // 2) Pagina aquí (ya filtrado)
    const total = all.length;
    const pageCount = Math.max(1, Math.ceil(total / limit));
    const quotes = all.slice(wantSkip, wantSkip + limit);

    return safeJson(res, 200, {
      ok: true,
      mode: "crawler",
      from: f,
      to: t,
      maxPages,
      page,
      limit,
      total,
      pageCount,
      quotes,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ START
========================================================= */
(async () => {
  try {
    await ensureDb();
    console.log(hasDb() ? "DB ready ✅" : "DB not configured (skipped init) ⚠️");
  } catch (e) {
    console.error("DB init error:", e.message);
  }

  app.listen(Number(PORT), () => {
    console.log(`Admin server listening on :${PORT}`);
  });
})();
