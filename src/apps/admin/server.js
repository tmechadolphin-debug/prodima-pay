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
   ✅ CORS (SIEMPRE)
========================================================= */
app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? true : CORS_ORIGIN,
    credentials: false,
    methods: ["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization", "X-Requested-With"],
  })
);
app.options("*", cors());

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

function addDaysISO(iso, days) {
  const d = new Date(String(iso || "").slice(0, 10));
  if (Number.isNaN(d.getTime())) return "";
  d.setDate(d.getDate() + Number(days || 0));
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

function parseUserFromComments(comments) {
  const m = String(comments || "").match(/\[user:([^\]]+)\]/i);
  return m ? String(m[1]).trim() : "";
}

function parseWhFromComments(comments) {
  const m = String(comments || "").match(/\[wh:([^\]]+)\]/i);
  return m ? String(m[1]).trim() : "";
}

function isCancelledLike(q) {
  const cancelVal = q?.CancelStatus ?? q?.cancelStatus ?? q?.Cancelled ?? q?.cancelled ?? "";
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
   ✅ ADMIN USERS (si tu HTML lo usa)
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
   ✅ Crawler: trae MUCHAS páginas de SAP y filtra por scope
========================================================= */
async function getCreatedUsersSet() {
  if (!hasDb()) return new Set();
  const r = await dbQuery(`SELECT username FROM app_users WHERE is_active=TRUE`);
  return new Set((r.rows || []).map((x) => String(x.username || "").trim().toLowerCase()).filter(Boolean));
}

// Escanea SAP por páginas y devuelve SOLO lo filtrado (y luego se pagina local)
async function crawlQuotes({
  from,
  to,
  maxPages = 100,
  sapPageSize = 200,
  scope = "all", // "created" o "all"
  userFilter = "",
  clientFilter = "",
}) {
  const toPlus1 = addDaysISO(to, 1);

  const uFilter = String(userFilter || "").trim().toLowerCase();
  const cFilter = String(clientFilter || "").trim().toLowerCase();

  const createdSet = scope === "created" ? await getCreatedUsersSet() : null;

  let skipSap = 0;
  const seenDocEntry = new Set();
  const out = [];

  for (let page = 0; page < maxPages; page++) {
    const raw = await slFetch(
      `/Quotations?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
        `&$filter=${encodeURIComponent(`DocDate ge '${from}' and DocDate lt '${toPlus1}'`)}` +
        `&$orderby=DocDate desc,DocEntry desc&$top=${sapPageSize}&$skip=${skipSap}`
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

      // scope=created => solo usuarios que existen en DB
      if (createdSet) {
        const u = String(usuario || "").trim().toLowerCase();
        if (!u || u === "sin_user" || !createdSet.has(u)) continue;
      }

      if (uFilter && !String(usuario).toLowerCase().includes(uFilter)) continue;

      if (cFilter) {
        const cc = String(q.CardCode || "").toLowerCase();
        const cn = String(q.CardName || "").toLowerCase();
        if (!cc.includes(cFilter) && !cn.includes(cFilter)) continue;
      }

      out.push({
        docEntry: q.DocEntry,
        docNum: q.DocNum,
        cardCode: String(q.CardCode || "").trim(),
        cardName: String(q.CardName || "").trim(),
        fecha: String(q.DocDate || "").slice(0, 10),
        estado: q.DocumentStatus || "",
        comments: q.Comments || "",
        usuario,
        warehouse: wh,
        montoCotizacion: Number(q.DocTotal || 0),
      });
    }

    // micro-sleep para no saturar
    await sleep(10);
  }

  return { quotes: out, createdUsersCount: createdSet ? createdSet.size : null };
}

/* =========================================================
   ✅ ADMIN QUOTES (crawler + paginación final)
   Params:
     - from, to (opcional)
     - page, limit (paginación final)
     - maxPages (páginas SAP a escanear)
     - scope=created | all
     - user, client (filtros)
========================================================= */
app.get("/api/admin/quotes", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    // RANGO por defecto: súper amplio, para no depender de fechas en UI
    const from = /^\d{4}-\d{2}-\d{2}$/.test(String(req.query?.from || "")) ? String(req.query.from) : "2016-01-01";
    const to = /^\d{4}-\d{2}-\d{2}$/.test(String(req.query?.to || "")) ? String(req.query.to) : String(new Date().toISOString()).slice(0, 10);

    const page = Math.max(1, Number(req.query?.page || 1));
    const limit = Math.min(Math.max(Number(req.query?.limit || 50), 1), 200);

    const maxPages = Math.min(Math.max(Number(req.query?.maxPages || 100), 1), 250);
    const sapPageSize = 200;

    const scope = String(req.query?.scope || "all").toLowerCase() === "created" ? "created" : "all";
    const userFilter = String(req.query?.user || "").trim();
    const clientFilter = String(req.query?.client || "").trim();

    const { quotes, createdUsersCount } = await crawlQuotes({
      from,
      to,
      maxPages,
      sapPageSize,
      scope,
      userFilter,
      clientFilter,
    });

    const total = quotes.length;
    const pageCount = Math.max(1, Math.ceil(total / limit));

    const start = (page - 1) * limit;
    const slice = quotes.slice(start, start + limit);

    return safeJson(res, 200, {
      ok: true,
      mode: "crawler",
      scope,
      from,
      to,
      maxPages,
      page,
      limit,
      total,
      pageCount,
      createdUsersCount,
      quotes: slice,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ DASHBOARD (para que tu HTML no tire 404)
   Usa el MISMO crawler y devuelve KPIs básicos.
   Params igual a /api/admin/quotes
========================================================= */
app.get("/api/admin/dashboard", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const from = /^\d{4}-\d{2}-\d{2}$/.test(String(req.query?.from || "")) ? String(req.query.from) : "2016-01-01";
    const to = /^\d{4}-\d{2}-\d{2}$/.test(String(req.query?.to || "")) ? String(req.query.to) : String(new Date().toISOString()).slice(0, 10);

    const maxPages = Math.min(Math.max(Number(req.query?.maxPages || 100), 1), 250);
    const scope = String(req.query?.scope || "all").toLowerCase() === "created" ? "created" : "all";

    const { quotes, createdUsersCount } = await crawlQuotes({
      from,
      to,
      maxPages,
      sapPageSize: 200,
      scope,
      userFilter: String(req.query?.user || "").trim(),
      clientFilter: String(req.query?.client || "").trim(),
    });

    const totalCot = quotes.reduce((a, q) => a + Number(q.montoCotizacion || 0), 0);

    return safeJson(res, 200, {
      ok: true,
      scope,
      from,
      to,
      maxPages,
      kpis: {
        totalCotizaciones: quotes.length,
        montoTotalCotizado: Number(totalCot.toFixed(2)),
      },
      createdUsersCount,
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
