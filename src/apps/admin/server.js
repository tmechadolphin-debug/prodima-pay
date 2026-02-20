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
  SAP_PRICE_LIST = "Lista 02 Res. Com. Ind. Analitic",
  YAPPY_ALIAS = "@prodimasansae",
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
   ✅ Warehouse map (para app_users)
========================================================= */
function provinceToWarehouse(province) {
  const p = String(province || "").trim().toLowerCase();
  if (p === "chiriquí" || p === "chiriqui" || p === "bocas del toro") return "200";
  if (
    p === "veraguas" ||
    p === "coclé" ||
    p === "cocle" ||
    p === "los santos" ||
    p === "herrera"
  )
    return "500";
  if (
    p === "panamá" ||
    p === "panama" ||
    p === "panamá oeste" ||
    p === "panama oeste" ||
    p === "colón" ||
    p === "colon"
  )
    return "300";
  if (p === "rci") return "01";
  return SAP_WAREHOUSE || "300";
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
function signUserToken(u, ttl = "30d") {
  return signToken(
    {
      role: "user",
      id: u.id,
      username: u.username,
      full_name: u.full_name || "",
      province: u.province || "",
      warehouse_code: u.warehouse_code || "",
    },
    ttl
  );
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
function missingSapEnv() {
  return !SAP_BASE_URL || !SAP_COMPANYDB || !SAP_USER || !SAP_PASS;
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
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
    message: "✅ PRODIMA API activa",
    yappy: YAPPY_ALIAS,
    warehouse_default: SAP_WAREHOUSE,
    priceList: SAP_PRICE_LIST,
    db: hasDb() ? "on" : "off",
  });
});

/* =========================================================
   ✅ Fetch wrapper (Node 16/18)
========================================================= */
let _fetch = globalThis.fetch || null;
async function httpFetch(url, options) {
  if (_fetch) return _fetch(url, options);
  const mod = await import("node-fetch");
  _fetch = mod.default;
  return _fetch(url, options);
}

/* =========================================================
   ✅ SAP Service Layer (cookie) + timeout
========================================================= */
let SL_COOKIE = "";
let SL_COOKIE_AT = 0;

async function slLogin() {
  const url = `${SAP_BASE_URL.replace(/\/$/, "")}/Login`;
  const body = { CompanyDB: SAP_COMPANYDB, UserName: SAP_USER, Password: SAP_PASS };

  const r = await httpFetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  const txt = await r.text();
  let data = {};
  try { data = JSON.parse(txt); } catch {}

  if (!r.ok) throw new Error(`SAP login failed: HTTP ${r.status} ${data?.error?.message?.value || txt}`);

  const setCookie = r.headers.get("set-cookie") || "";
  const cookies = [];
  for (const part of setCookie.split(",")) {
    const s = part.trim();
    if (s.startsWith("B1SESSION=") || s.startsWith("ROUTEID=")) cookies.push(s.split(";")[0]);
  }
  SL_COOKIE = cookies.join("; ");
  SL_COOKIE_AT = Date.now();
}

async function slFetch(path, options = {}) {
  if (missingSapEnv()) throw new Error("Missing SAP env");

  if (!SL_COOKIE || Date.now() - SL_COOKIE_AT > 25 * 60 * 1000) await slLogin();

  const base = SAP_BASE_URL.replace(/\/$/, "");
  const url = `${base}${path.startsWith("/") ? path : `/${path}`}`;
  const method = String(options.method || "GET").toUpperCase();

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30000);

  try {
    const r = await httpFetch(url, {
      method,
      signal: controller.signal,
      headers: {
        "Content-Type": "application/json",
        Cookie: SL_COOKIE,
        ...(options.headers || {}),
      },
      body: options.body,
    });

    const txt = await r.text();
    let data = {};
    try { data = JSON.parse(txt); } catch { data = { raw: txt }; }

    if (!r.ok) {
      if (r.status === 401 || r.status === 403) {
        SL_COOKIE = "";
        await slLogin();
        return slFetch(path, options);
      }
      throw new Error(`SAP error ${r.status}: ${data?.error?.message?.value || txt}`);
    }

    return data;
  } catch (e) {
    if (String(e?.name) === "AbortError") throw new Error("SAP timeout (30s) en slFetch");
    throw e;
  } finally {
    clearTimeout(timeout);
  }
}

/* =========================================================
   ✅ SAP helpers
========================================================= */
async function sapGetFirstByDocNum(entity, docNum, select) {
  const n = Number(docNum);
  if (!Number.isFinite(n) || n <= 0) throw new Error("DocNum inválido");

  const parts = [];
  if (select) parts.push(`$select=${encodeURIComponent(select)}`);
  parts.push(`$filter=${encodeURIComponent(`DocNum eq ${n}`)}`);
  parts.push(`$top=1`);

  const r = await slFetch(`/${entity}?${parts.join("&")}`);
  const arr = Array.isArray(r?.value) ? r.value : [];
  return arr[0] || null;
}
async function sapGetByDocEntry(entity, docEntry, select) {
  const n = Number(docEntry);
  if (!Number.isFinite(n) || n <= 0) throw new Error("DocEntry inválido");
  let path = `/${entity}(${n})`;
  if (select) path += `?$select=${encodeURIComponent(select)}`;
  return slFetch(path);
}

/* =========================================================
   ✅ CACHE: usuarios creados (para ONLY CREATED scope)
========================================================= */
let CREATED_USERS_CACHE = { ts: 0, set: new Set() };
const CREATED_USERS_TTL_MS = 5 * 60 * 1000;

async function getCreatedUsersSetCached() {
  if (!hasDb()) return new Set();
  const now = Date.now();
  if (CREATED_USERS_CACHE.ts && now - CREATED_USERS_CACHE.ts < CREATED_USERS_TTL_MS) {
    return CREATED_USERS_CACHE.set;
  }
  const r = await dbQuery(`SELECT username FROM app_users WHERE is_active=TRUE`);
  const set = new Set((r.rows || []).map(x => String(x.username || "").trim().toLowerCase()).filter(Boolean));
  CREATED_USERS_CACHE = { ts: now, set };
  return set;
}

/* =========================================================
   ✅ TRACE (ENTREGADO) + cache
========================================================= */
const TRACE_CACHE = new Map();
const TRACE_TTL_MS = 6 * 60 * 60 * 1000;

function cacheGet(key) {
  const it = TRACE_CACHE.get(key);
  if (!it) return null;
  if (Date.now() - it.at > TRACE_TTL_MS) {
    TRACE_CACHE.delete(key);
    return null;
  }
  return it.data;
}
function cacheSet(key, data) {
  TRACE_CACHE.set(key, { at: Date.now(), data });
}

async function traceQuote(quoteDocNum, fromOverride, toOverride) {
  const cacheKey = `QDOCNUM:${quoteDocNum}`;
  const cached = cacheGet(cacheKey);
  if (cached) return cached;

  const quoteHead = await sapGetFirstByDocNum(
    "Quotations",
    quoteDocNum,
    "DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments"
  );
  if (!quoteHead) {
    const out = { ok: false, message: "Cotización no encontrada" };
    cacheSet(cacheKey, out);
    return out;
  }

  const quote = await sapGetByDocEntry("Quotations", quoteHead.DocEntry);
  const quoteDocEntry = Number(quote.DocEntry);
  const cardCode = String(quote.CardCode || "").trim();
  const quoteDate = String(quote.DocDate || "").slice(0, 10);

  const from = /^\d{4}-\d{2}-\d{2}$/.test(String(fromOverride || "")) ? String(fromOverride) : addDaysISO(quoteDate, -7);
  const to = /^\d{4}-\d{2}-\d{2}$/.test(String(toOverride || "")) ? String(toOverride) : addDaysISO(quoteDate, 30);
  const toPlus1 = addDaysISO(to, 1);

  const ordersList = await slFetch(
    `/Orders?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
      `&$filter=${encodeURIComponent(
        `CardCode eq '${cardCode.replace(/'/g, "''")}' and DocDate ge '${from}' and DocDate lt '${toPlus1}'`
      )}` +
      `&$orderby=DocDate desc,DocEntry desc&$top=200`
  );
  const orderCandidates = Array.isArray(ordersList?.value) ? ordersList.value : [];

  const orders = [];
  for (const o of orderCandidates) {
    const od = await sapGetByDocEntry("Orders", o.DocEntry);
    const lines = Array.isArray(od?.DocumentLines) ? od.DocumentLines : [];
    const linked = lines.some((l) => Number(l?.BaseType) === 23 && Number(l?.BaseEntry) === quoteDocEntry);
    if (linked) orders.push(od);
    await sleep(15);
  }

  const deliveries = [];
  const orderDocEntrySet = new Set(orders.map((x) => Number(x.DocEntry)));

  if (orders.length) {
    const delList = await slFetch(
      `/DeliveryNotes?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
        `&$filter=${encodeURIComponent(
          `CardCode eq '${cardCode.replace(/'/g, "''")}' and DocDate ge '${from}' and DocDate lt '${toPlus1}'`
        )}` +
        `&$orderby=DocDate desc,DocEntry desc&$top=300`
    );
    const delCandidates = Array.isArray(delList?.value) ? delList.value : [];

    const seen = new Set();
    for (const d of delCandidates) {
      const dd = await sapGetByDocEntry("DeliveryNotes", d.DocEntry);
      const lines = Array.isArray(dd?.DocumentLines) ? dd.DocumentLines : [];
      const linked = lines.some((l) => Number(l?.BaseType) === 17 && orderDocEntrySet.has(Number(l?.BaseEntry)));
      if (linked) {
        const de = Number(dd.DocEntry);
        if (!seen.has(de)) {
          seen.add(de);
          deliveries.push(dd);
        }
      }
      await sleep(15);
    }
  }

  const totalCotizado = Number(quote.DocTotal || 0);
  const totalEntregado = deliveries.reduce((a, d) => a + Number(d?.DocTotal || 0), 0);
  const pendiente = Number((totalCotizado - totalEntregado).toFixed(2));

  const out = { ok: true, quote, orders, deliveries, totals: { totalCotizado, totalEntregado, pendiente } };
  cacheSet(cacheKey, out);
  cacheSet(`QDOCENTRY:${quoteDocEntry}`, out);
  return out;
}

/* =========================================================
   ✅ Groups cache
========================================================= */
const ITEM_GROUP_CODE_TO_NAME = new Map();
const ITEM_CODE_TO_GROUP_NAME = new Map();
const GROUP_TTL_MS = 24 * 60 * 60 * 1000;
const GROUP_CACHE_AT = new Map();

function cacheFresh(key, ttl) {
  const ts = GROUP_CACHE_AT.get(key);
  return ts && Date.now() - ts < ttl;
}
function cacheStamp(key) {
  GROUP_CACHE_AT.set(key, Date.now());
}

async function getGroupNameByGroupCode(groupCode) {
  const code = Number(groupCode);
  if (!Number.isFinite(code)) return "";

  const key = `G:${code}`;
  if (ITEM_GROUP_CODE_TO_NAME.has(code) && cacheFresh(key, GROUP_TTL_MS)) {
    return ITEM_GROUP_CODE_TO_NAME.get(code) || "";
  }

  const r = await slFetch(
    `/ItemGroups?$select=GroupCode,GroupName&$filter=${encodeURIComponent(`GroupCode eq ${code}`)}&$top=1`
  );
  const arr = Array.isArray(r?.value) ? r.value : [];
  const name = String(arr?.[0]?.GroupName || "").trim();

  ITEM_GROUP_CODE_TO_NAME.set(code, name);
  cacheStamp(key);
  return name;
}

async function getGroupNameByItemCode(itemCode) {
  const code = String(itemCode || "").trim();
  if (!code) return "";

  const key = `I:${code}`;
  if (ITEM_CODE_TO_GROUP_NAME.has(code) && cacheFresh(key, GROUP_TTL_MS)) {
    return ITEM_CODE_TO_GROUP_NAME.get(code) || "";
  }

  const it = await slFetch(`/Items('${encodeURIComponent(code)}')?$select=ItemCode,ItemsGroupCode`);
  const gcode = it?.ItemsGroupCode;
  const gname = await getGroupNameByGroupCode(gcode);

  ITEM_CODE_TO_GROUP_NAME.set(code, gname);
  cacheStamp(key);
  return gname;
}

function wantsGroups(req) {
  const q = req.query || {};
  const v = (x) => String(x ?? "").trim().toLowerCase();
  return (
    v(q.withGroups) === "1" ||
    v(q.groups) === "1" ||
    v(q.includeGroups) === "1" ||
    v(q.dashboard) === "1" ||
    v(q.mode) === "dashboard"
  );
}

async function resolveGroupsForItemCodes(itemCodes) {
  const unique = Array.from(new Set(itemCodes.map((x) => String(x || "").trim()).filter(Boolean)));
  if (!unique.length) return new Map();

  const out = new Map();
  const CONC = 8;
  let idx = 0;

  async function worker() {
    while (idx < unique.length) {
      const i = idx++;
      const code = unique[i];
      try {
        const g = await getGroupNameByItemCode(code);
        out.set(code, g || "");
      } catch {
        out.set(code, "");
      }
      await sleep(3);
    }
  }

  await Promise.all(Array.from({ length: CONC }, worker));
  return out;
}

/* =========================================================
   ✅ USER LOGIN
========================================================= */
async function handleUserLogin(req, res) {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const username = String(req.body?.username || req.body?.user || "").trim().toLowerCase();
    const pin = String(req.body?.pin || req.body?.pass || "").trim();

    if (!username || !pin) return safeJson(res, 400, { ok: false, message: "username y pin requeridos" });

    const r = await dbQuery(
      `SELECT id, username, full_name, pin_hash, province, warehouse_code, is_active
       FROM app_users
       WHERE username=$1
       LIMIT 1`,
      [username]
    );

    const u = r.rows?.[0];
    if (!u) return safeJson(res, 401, { ok: false, message: "Credenciales inválidas" });
    if (!u.is_active) return safeJson(res, 403, { ok: false, message: "Usuario inactivo" });

    const ok = await bcrypt.compare(pin, u.pin_hash);
    if (!ok) return safeJson(res, 401, { ok: false, message: "Credenciales inválidas" });

    let wh = String(u.warehouse_code || "").trim();
    if (!wh) {
      wh = provinceToWarehouse(u.province || "");
      try {
        await dbQuery(`UPDATE app_users SET warehouse_code=$1 WHERE id=$2`, [wh, u.id]);
        u.warehouse_code = wh;
      } catch {}
    }

    const token = signUserToken(u, "30d");
    return safeJson(res, 200, {
      ok: true,
      token,
      user: {
        id: u.id,
        username: u.username,
        full_name: u.full_name || "",
        province: u.province || "",
        warehouse_code: u.warehouse_code || "",
      },
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
}
app.post("/api/login", handleUserLogin);
app.post("/api/auth/login", handleUserLogin);
app.get("/api/me", verifyUser, async (req, res) => safeJson(res, 200, { ok: true, user: req.user }));

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
   ✅ ADMIN USERS
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
    const warehouse_code_in = String(req.body?.warehouse_code || req.body?.warehouse || "").trim();

    if (!username || username === "__INVALID__") {
      return safeJson(res, 400, { ok: false, message: "Username inválido." });
    }
    if (!pin || pin.length < 4) return safeJson(res, 400, { ok: false, message: "PIN mínimo 4" });

    const warehouse_code = warehouse_code_in || provinceToWarehouse(province || "");
    const pin_hash = await bcrypt.hash(pin, 10);

    const q = await dbQuery(
      `INSERT INTO app_users (username, full_name, pin_hash, province, warehouse_code, is_active)
       VALUES ($1,$2,$3,$4,$5,TRUE)
       RETURNING id, username, full_name, province, warehouse_code, is_active, created_at`,
      [username, full_name, pin_hash, province, warehouse_code]
    );

    // refrescar cache
    CREATED_USERS_CACHE.ts = 0;

    return safeJson(res, 200, { ok: true, user: q.rows[0] });
  } catch (e) {
    if (String(e?.code) === "23505") return safeJson(res, 409, { ok: false, message: "Ese username ya existe" });
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.patch("/api/admin/users/:id/pin", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });
    const id = toIntId(req.params.id);
    if (!id) return safeJson(res, 400, { ok: false, message: "ID inválido" });

    const pin = String(req.body?.pin || "").trim();
    if (!pin || pin.length < 4) return safeJson(res, 400, { ok: false, message: "PIN mínimo 4" });

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
      `UPDATE app_users SET is_active = NOT is_active WHERE id=$1
       RETURNING id, username, full_name, province, warehouse_code, is_active, created_at`,
      [id]
    );
    if (!r.rows?.length) return safeJson(res, 404, { ok: false, message: "Usuario no existe" });

    // refrescar cache
    CREATED_USERS_CACHE.ts = 0;

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

    // refrescar cache
    CREATED_USERS_CACHE.ts = 0;

    return safeJson(res, 200, { ok: true });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ ADMIN QUOTES (HISTÓRICO + DASHBOARD)
   ✅ FIX real: onlyCreated=1 filtra en backend (ANTES de entregado/grupos)
========================================================= */
const HARD_CAP_DELIVERED = 60;
const HARD_CAP_GROUPS = 120;

async function scanQuotes({
  f,
  t,
  wantSkip,
  wantLimit,
  userFilter,
  clientFilter,
  includeTotal,
  onlyCreated,
}) {
  const toPlus1 = addDaysISO(t, 1);
  const batchTop = 200;

  let skipSap = 0;
  let totalFiltered = 0;
  const pageRows = [];

  const uFilter = String(userFilter || "").trim().toLowerCase();
  const cFilter = String(clientFilter || "").trim().toLowerCase();

  const maxSapPages = includeTotal ? 200 : 60;

  const seenDocEntry = new Set();

  // ✅ traer set de usuarios creados SOLO si se pidió
  const createdSet = onlyCreated ? await getCreatedUsersSetCached() : null;

  for (let page = 0; page < maxSapPages; page++) {
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

      // ✅ filtro ONLY CREATED (backend)
      if (createdSet) {
        const u = String(usuario || "").trim().toLowerCase();
        if (!u || !createdSet.has(u)) continue;
      }

      if (uFilter && !String(usuario).toLowerCase().includes(uFilter)) continue;

      if (cFilter) {
        const cc = String(q.CardCode || "").toLowerCase();
        const cn = String(q.CardName || "").toLowerCase();
        if (!cc.includes(cFilter) && !cn.includes(cFilter)) continue;
      }

      const idx = totalFiltered;
      totalFiltered++;

      if (idx >= wantSkip && pageRows.length < wantLimit) {
        pageRows.push({
          docEntry: q.DocEntry,
          docNum: q.DocNum,
          cardCode: q.CardCode,
          cardName: q.CardName,
          fecha: String(q.DocDate || "").slice(0, 10),
          estado: q.DocumentStatus || "",
          cancelStatus: q.CancelStatus ?? "",
          comments: q.Comments || "",
          usuario,
          warehouse: wh,
          montoCotizacion: Number(q.DocTotal || 0),
          montoEntregado: 0,
          pendiente: Number(q.DocTotal || 0),
        });
      }
    }

    if (!includeTotal && pageRows.length >= wantLimit) break;
  }

  return { pageRows, totalFiltered };
}

app.get("/api/admin/quotes", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const from = String(req.query?.from || "");
    const to = String(req.query?.to || "");

    const withDelivered = String(req.query?.withDelivered || "0") === "1";
    const withGroups = wantsGroups(req);

    const onlyCreated = String(req.query?.onlyCreated || "0") === "1";

    const limitRaw =
      req.query?.limit != null
        ? Number(req.query.limit)
        : req.query?.top != null
        ? Number(req.query.top)
        : 20;

    let limit = Math.max(1, Math.min(200, Number.isFinite(limitRaw) ? limitRaw : 20));
    if (withDelivered) limit = Math.min(limit, HARD_CAP_DELIVERED);
    if (withGroups) limit = Math.min(limit, HARD_CAP_GROUPS);

    const pageRaw = req.query?.page != null ? Number(req.query.page) : null;
    const page = pageRaw != null ? Math.max(1, Number.isFinite(pageRaw) ? pageRaw : 1) : null;

    const skipExplicit = req.query?.skip != null ? Number(req.query.skip) : null;
    const skip =
      skipExplicit != null && Number.isFinite(skipExplicit)
        ? Math.max(0, Math.trunc(skipExplicit))
        : page != null
        ? (page - 1) * limit
        : 0;

    const includeTotal = String(req.query?.includeTotal || "0") === "1";
    const userFilter = String(req.query?.user || "");
    const clientFilter = String(req.query?.client || "");

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const defaultFrom = addDaysISO(today, -30);

    const f = /^\d{4}-\d{2}-\d{2}$/.test(from) ? from : defaultFrom;
    const t = /^\d{4}-\d{2}-\d{2}$/.test(to) ? to : today;

    const { pageRows, totalFiltered } = await scanQuotes({
      f,
      t,
      wantSkip: skip,
      wantLimit: limit,
      userFilter,
      clientFilter,
      includeTotal,
      onlyCreated,
    });

    // ✅ ENTREGADO (solo sobre lo que YA pasó el filtro)
    if (withDelivered && pageRows.length) {
      const CONC = 2;
      let idx = 0;

      async function worker() {
        while (idx < pageRows.length) {
          const i = idx++;
          const q = pageRows[i];
          try {
            const tr = await traceQuote(q.docNum, f, t);
            if (tr.ok) {
              q.montoEntregado = Number(tr.totals?.totalEntregado || 0);
              q.pendiente = Number(tr.totals?.pendiente || 0);
            }
          } catch {}
          await sleep(15);
        }
      }

      await Promise.all(Array.from({ length: CONC }, () => worker()));
    }

    // ✅ GROUPS (solo sobre lo que YA pasó el filtro)
    if (withGroups && pageRows.length) {
      const CONC = 4;
      let idx = 0;

      async function workerGroups() {
        while (idx < pageRows.length) {
          const i = idx++;
          const q = pageRows[i];

          try {
            const full = await slFetch(
              `/Quotations(${Number(q.docEntry)})?$select=DocEntry&$expand=DocumentLines($select=ItemCode,LineTotal)`
            );

            const lines = Array.isArray(full?.DocumentLines) ? full.DocumentLines : [];
            const codes = lines.map((ln) => String(ln?.ItemCode || "").trim()).filter(Boolean);

            const mapGroups = await resolveGroupsForItemCodes(codes);

            const outLines = [];
            for (const ln of lines) {
              const code = String(ln?.ItemCode || "").trim();
              if (!code) continue;

              outLines.push({
                ItemCode: code,
                LineTotal: Number(ln?.LineTotal || 0),
                ItmsGrpNam: mapGroups.get(code) || "",
              });
            }

            q.lines = outLines;

            const uniq = new Set(outLines.map((x) => x.ItmsGrpNam).filter(Boolean));
            if (uniq.size === 1) q.itemGroup = Array.from(uniq)[0];
          } catch {}
          await sleep(8);
        }
      }

      await Promise.all(Array.from({ length: CONC }, () => workerGroups()));
    }

    return safeJson(res, 200, {
      ok: true,
      quotes: pageRows,
      from: f,
      to: t,
      page: page ?? null,
      limit,
      skip,
      total: includeTotal ? totalFiltered : null,
      pageCount: includeTotal ? Math.max(1, Math.ceil(totalFiltered / limit)) : null,
      scope: { onlyCreated },
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ ADMIN DASHBOARD (tu front lo llama)
========================================================= */
app.get("/api/admin/dashboard", verifyAdmin, async (req, res) => {
  return safeJson(res, 200, {
    ok: true,
    message: "Dashboard se arma en frontend con /api/admin/quotes",
  });
});

/* =========================================================
   ✅ START
========================================================= */
process.on("unhandledRejection", (e) => console.error("unhandledRejection:", e));
process.on("uncaughtException", (e) => console.error("uncaughtException:", e));

(async () => {
  try {
    await ensureDb();
    console.log(hasDb() ? "DB ready ✅" : "DB not configured (skipped init) ⚠️");
  } catch (e) {
    console.error("DB init error:", e.message);
  }

  app.listen(Number(PORT), () => {
    console.log(`Server listening on :${PORT}`);
  });
})();
