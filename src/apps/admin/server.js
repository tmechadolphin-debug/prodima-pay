import express from "express";
import pg from "pg";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";

const { Pool } = pg;

const app = express();
app.use(express.json({ limit: "6mb" }));

/* =========================================================
   ✅ ENV
========================================================= */
const {
  PORT = 3000,
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

  // Render:
  // CORS_ORIGIN=https://prodima.com.pa,https://www.prodima.com.pa
  CORS_ORIGIN = "",
} = process.env;

/* =========================================================
   ✅ CORS ROBUSTO
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
   ✅ DB (Postgres)
========================================================= */
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

  // users
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

  // sync state
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS sync_state (
      k TEXT PRIMARY KEY,
      v TEXT NOT NULL,
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  // quote header cache (DB dashboard)
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS quote_head_cache (
      doc_entry INTEGER PRIMARY KEY,
      doc_num   INTEGER NOT NULL,
      doc_date  DATE NOT NULL,
      card_code TEXT NOT NULL,
      card_name TEXT NOT NULL,
      usuario   TEXT NOT NULL DEFAULT 'sin_user',
      warehouse TEXT NOT NULL DEFAULT 'sin_wh',
      doc_total NUMERIC(18,2) NOT NULL DEFAULT 0,
      delivered_total NUMERIC(18,2) NOT NULL DEFAULT 0,
      pending_total   NUMERIC(18,2) NOT NULL DEFAULT 0,
      document_status TEXT NOT NULL DEFAULT '',
      cancel_status   TEXT NOT NULL DEFAULT '',
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  // quote lines cache (para categorías)
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS quote_line_cache (
      doc_entry INTEGER NOT NULL,
      line_num  INTEGER NOT NULL,
      doc_num   INTEGER NOT NULL,
      doc_date  DATE NOT NULL,
      card_code TEXT NOT NULL,
      warehouse TEXT NOT NULL DEFAULT 'sin_wh',
      item_code TEXT NOT NULL DEFAULT '',
      item_desc TEXT NOT NULL DEFAULT '',
      item_group TEXT NOT NULL DEFAULT '',
      quantity NUMERIC(18,4) NOT NULL DEFAULT 0,
      line_total NUMERIC(18,2) NOT NULL DEFAULT 0,
      updated_at TIMESTAMP DEFAULT NOW(),
      PRIMARY KEY (doc_entry, line_num)
    );
  `);

  // item group cache (ItemCode -> GroupName)
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS item_group_cache (
      item_code TEXT PRIMARY KEY,
      group_name TEXT NOT NULL DEFAULT '',
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_qhc_date ON quote_head_cache(doc_date);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_qhc_user ON quote_head_cache(usuario);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_qhc_wh ON quote_head_cache(warehouse);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_qhc_card ON quote_head_cache(card_code);`);

  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_qlc_date ON quote_line_cache(doc_date);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_qlc_group ON quote_line_cache(item_group);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_qlc_item ON quote_line_cache(item_code);`);
}

async function setState(k, v) {
  if (!hasDb()) return;
  await dbQuery(
    `INSERT INTO sync_state(k,v,updated_at) VALUES($1,$2,NOW())
     ON CONFLICT(k) DO UPDATE SET v=EXCLUDED.v, updated_at=NOW()`,
    [String(k), String(v)]
  );
}
async function getState(k) {
  if (!hasDb()) return "";
  const r = await dbQuery(`SELECT v FROM sync_state WHERE k=$1 LIMIT 1`, [String(k)]);
  return r.rows?.[0]?.v || "";
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
const TZ_OFFSET_MIN = -300;
function getDateISOInOffset(offsetMin = 0) {
  const now = new Date();
  const ms = now.getTime() + now.getTimezoneOffset() * 60000 + Number(offsetMin) * 60000;
  const d = new Date(ms);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}
function isISO(s) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(s || ""));
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
  const stLower = String(q?.DocumentStatus || q?.documentStatus || "").toLowerCase();
  return (
    cancelRaw === "csyes" ||
    cancelRaw === "yes" ||
    cancelRaw === "true" ||
    cancelRaw.includes("csyes") ||
    cancelRaw.includes("cancel") ||
    stLower.includes("cancel") ||
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
    db: hasDb() ? "on" : "off",
    sap: missingSapEnv() ? "missing" : "ok",
    priceList: SAP_PRICE_LIST,
    whDefault: SAP_WAREHOUSE,
    quotes_last_sync_at: await getState("quotes_last_sync_at"),
  });
});

/* =========================================================
   ✅ fetch wrapper (Node16/18)
========================================================= */
let _fetch = globalThis.fetch || null;
async function httpFetch(url, options) {
  if (_fetch) return _fetch(url, options);
  const mod = await import("node-fetch");
  _fetch = mod.default;
  return _fetch(url, options);
}

/* =========================================================
   ✅ SAP Service Layer
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

  const controller = new AbortController();
  const timeoutMs = Number(options.timeoutMs || 20000);
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const r = await httpFetch(url, {
      method: String(options.method || "GET").toUpperCase(),
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
    if (String(e?.name) === "AbortError") throw new Error(`SAP timeout (${timeoutMs}ms) en slFetch`);
    throw e;
  } finally {
    clearTimeout(timeout);
  }
}

async function sapGetFirstByDocNum(entity, docNum, select) {
  const n = Number(docNum);
  if (!Number.isFinite(n) || n <= 0) throw new Error("DocNum inválido");

  const parts = [];
  if (select) parts.push(`$select=${encodeURIComponent(select)}`);
  parts.push(`$filter=${encodeURIComponent(`DocNum eq ${n}`)}`);
  parts.push(`$top=1`);

  const r = await slFetch(`/${entity}?${parts.join("&")}`, { timeoutMs: 20000 });
  const arr = Array.isArray(r?.value) ? r.value : [];
  return arr[0] || null;
}
async function sapGetByDocEntry(entity, docEntry, timeoutMs = 20000) {
  const n = Number(docEntry);
  if (!Number.isFinite(n) || n <= 0) throw new Error("DocEntry inválido");
  return slFetch(`/${entity}(${n})`, { timeoutMs });
}

/* =========================================================
   ✅ Cache usuarios creados (scope)
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
  const set = new Set(
    (r.rows || []).map((x) => String(x.username || "").trim().toLowerCase()).filter(Boolean)
  );
  CREATED_USERS_CACHE = { ts: now, set };
  return set;
}

/* =========================================================
   ✅ TRACE entregado (cotización -> pedido -> entrega)
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

async function traceQuoteTotals(quoteDocNum, fromOverride, toOverride) {
  const cacheKey = `Q:${quoteDocNum}:${fromOverride || ""}:${toOverride || ""}`;
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

  const quote = await sapGetByDocEntry("Quotations", quoteHead.DocEntry, 25000);
  const quoteDocEntry = Number(quote.DocEntry);
  const cardCode = String(quote.CardCode || "").trim();
  const quoteDate = String(quote.DocDate || "").slice(0, 10);

  const from = isISO(fromOverride) ? String(fromOverride) : addDaysISO(quoteDate, -7);
  const to = isISO(toOverride) ? String(toOverride) : addDaysISO(quoteDate, 30);
  const toPlus1 = addDaysISO(to, 1);

  const ordersList = await slFetch(
    `/Orders?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
      `&$filter=${encodeURIComponent(
        `CardCode eq '${cardCode.replace(/'/g, "''")}' and DocDate ge '${from}' and DocDate lt '${toPlus1}'`
      )}` +
      `&$orderby=DocDate desc,DocEntry desc&$top=200`,
    { timeoutMs: 30000 }
  );
  const orderCandidates = Array.isArray(ordersList?.value) ? ordersList.value : [];

  const orders = [];
  for (const o of orderCandidates) {
    const od = await sapGetByDocEntry("Orders", o.DocEntry, 30000);
    const lines = Array.isArray(od?.DocumentLines) ? od.DocumentLines : [];
    const linked = lines.some((l) => Number(l?.BaseType) === 23 && Number(l?.BaseEntry) === quoteDocEntry);
    if (linked) orders.push(od);
    await sleep(12);
  }

  let totalEntregado = 0;

  if (orders.length) {
    const orderDocEntrySet = new Set(orders.map((x) => Number(x.DocEntry)));

    const delList = await slFetch(
      `/DeliveryNotes?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
        `&$filter=${encodeURIComponent(
          `CardCode eq '${cardCode.replace(/'/g, "''")}' and DocDate ge '${from}' and DocDate lt '${toPlus1}'`
        )}` +
        `&$orderby=DocDate desc,DocEntry desc&$top=300`,
      { timeoutMs: 30000 }
    );
    const delCandidates = Array.isArray(delList?.value) ? delList.value : [];

    const seen = new Set();
    for (const d of delCandidates) {
      const dd = await sapGetByDocEntry("DeliveryNotes", d.DocEntry, 30000);
      const lines = Array.isArray(dd?.DocumentLines) ? dd.DocumentLines : [];
      const linked = lines.some((l) => Number(l?.BaseType) === 17 && orderDocEntrySet.has(Number(l?.BaseEntry)));
      if (linked) {
        const de = Number(dd.DocEntry);
        if (!seen.has(de)) {
          seen.add(de);
          totalEntregado += Number(dd?.DocTotal || 0);
        }
      }
      await sleep(12);
    }
  }

  const totalCotizado = Number(quote.DocTotal || 0);
  const pendiente = Number((totalCotizado - totalEntregado).toFixed(2));

  const out = { ok: true, totalCotizado, totalEntregado, pendiente };
  cacheSet(cacheKey, out);
  return out;
}

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
   ✅ ADMIN USERS (CRUD completo)
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

function provinceToWarehouse(province) {
  const p = String(province || "").trim().toLowerCase();
  if (p === "chiriquí" || p === "chiriqui" || p === "bocas del toro") return "200";
  if (p === "veraguas" || p === "coclé" || p === "cocle" || p === "los santos" || p === "herrera") return "500";
  if (p === "panamá" || p === "panama" || p === "panamá oeste" || p === "panama oeste" || p === "colón" || p === "colon") return "300";
  return "";
}

app.post("/api/admin/users", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const username = String(req.body?.username || "").trim().toLowerCase();
    const fullName = String(req.body?.fullName || req.body?.full_name || "").trim();
    const pin = String(req.body?.pin || "").trim();
    const province = String(req.body?.province || "").trim();

    if (!username) return safeJson(res, 400, { ok: false, message: "username requerido" });
    if (!pin || pin.length < 4) return safeJson(res, 400, { ok: false, message: "PIN mínimo 4" });

    const wh = provinceToWarehouse(province) || String(req.body?.warehouse_code || "").trim() || "";

    const pin_hash = await bcrypt.hash(pin, 10);

    const r = await dbQuery(
      `INSERT INTO app_users (username, full_name, pin_hash, province, warehouse_code, is_active)
       VALUES ($1,$2,$3,$4,$5,TRUE)
       RETURNING id, username, full_name, province, warehouse_code, is_active, created_at`,
      [username, fullName, pin_hash, province, wh]
    );

    CREATED_USERS_CACHE.ts = 0; // invalidate cache
    return safeJson(res, 200, { ok: true, user: r.rows[0] });
  } catch (e) {
    const msg = String(e?.message || e);
    if (msg.toLowerCase().includes("duplicate") || msg.toLowerCase().includes("unique")) {
      return safeJson(res, 400, { ok: false, message: "username ya existe" });
    }
    return safeJson(res, 500, { ok: false, message: msg });
  }
});

app.patch("/api/admin/users/:id/toggle", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });
    const id = Number(req.params.id);
    if (!Number.isFinite(id)) return safeJson(res, 400, { ok: false, message: "id inválido" });

    const r = await dbQuery(
      `UPDATE app_users
       SET is_active = NOT is_active
       WHERE id=$1
       RETURNING id, username, full_name, province, warehouse_code, is_active, created_at`,
      [id]
    );
    CREATED_USERS_CACHE.ts = 0;
    return safeJson(res, 200, { ok: true, user: r.rows[0] });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.delete("/api/admin/users/:id", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });
    const id = Number(req.params.id);
    if (!Number.isFinite(id)) return safeJson(res, 400, { ok: false, message: "id inválido" });

    await dbQuery(`DELETE FROM app_users WHERE id=$1`, [id]);
    CREATED_USERS_CACHE.ts = 0;
    return safeJson(res, 200, { ok: true });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.patch("/api/admin/users/:id/pin", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });
    const id = Number(req.params.id);
    const pin = String(req.body?.pin || "").trim();
    if (!Number.isFinite(id)) return safeJson(res, 400, { ok: false, message: "id inválido" });
    if (!pin || pin.length < 4) return safeJson(res, 400, { ok: false, message: "PIN mínimo 4" });

    const pin_hash = await bcrypt.hash(pin, 10);
    await dbQuery(`UPDATE app_users SET pin_hash=$1 WHERE id=$2`, [pin_hash, id]);
    return safeJson(res, 200, { ok: true });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ HISTÓRICO (Service Layer) - rápido
========================================================= */
async function scanQuotes({ f, t, wantSkip, wantLimit, userFilter, clientFilter, onlyCreated }) {
  const toPlus1 = addDaysISO(t, 1);
  const batchTop = 200;

  let skipSap = 0;
  let totalFiltered = 0;
  const pageRows = [];

  const uFilter = String(userFilter || "").trim().toLowerCase();
  const cFilter = String(clientFilter || "").trim().toLowerCase();

  const maxSapPages = 40;
  const seenDocEntry = new Set();

  const createdSet = onlyCreated ? await getCreatedUsersSetCached() : null;

  for (let page = 0; page < maxSapPages; page++) {
    const raw = await slFetch(
      `/Quotations?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
        `&$filter=${encodeURIComponent(`DocDate ge '${f}' and DocDate lt '${toPlus1}'`)}` +
        `&$orderby=DocDate desc,DocEntry desc&$top=${batchTop}&$skip=${skipSap}`,
      { timeoutMs: 20000 }
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

      // para histórico dejamos canceladas visibles (tú las quieres ver). No filtramos aquí.

      const usuario = parseUserFromComments(q.Comments || "") || "sin_user";
      const wh = parseWhFromComments(q.Comments || "") || "sin_wh";

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

      const idx = totalFiltered++;
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

          // entregado: se llena por endpoint batch (quotes/delivered)
          montoEntregado: 0,
          pendiente: Number(q.DocTotal || 0),
        });
      }
    }

    if (pageRows.length >= wantLimit) break;
  }

  return { pageRows, totalFiltered };
}

app.get("/api/admin/quotes", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const from = String(req.query?.from || "");
    const to = String(req.query?.to || "");

    const onlyCreated = String(req.query?.onlyCreated || "0") === "1";

    const limitRaw =
      req.query?.limit != null
        ? Number(req.query.limit)
        : req.query?.top != null
        ? Number(req.query.top)
        : 20;

    const limit = Math.max(1, Math.min(200, Number.isFinite(limitRaw) ? limitRaw : 20));
    const skip = req.query?.skip != null ? Math.max(0, Number(req.query.skip) || 0) : 0;

    const userFilter = String(req.query?.user || "");
    const clientFilter = String(req.query?.client || "");

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const defaultFrom = addDaysISO(today, -30);

    const f = isISO(from) ? from : defaultFrom;
    const t = isISO(to) ? to : today;

    const { pageRows, totalFiltered } = await scanQuotes({
      f,
      t,
      wantSkip: skip,
      wantLimit: limit,
      userFilter,
      clientFilter,
      onlyCreated,
    });

    return safeJson(res, 200, {
      ok: true,
      quotes: pageRows,
      from: f,
      to: t,
      limit,
      skip,
      total: totalFiltered,
      scope: { onlyCreated },
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.get("/api/admin/quotes/delivered", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const docNums = String(req.query?.docNums || "")
      .split(",")
      .map((x) => Number(String(x).trim()))
      .filter((n) => Number.isFinite(n) && n > 0)
      .slice(0, 20);

    if (!docNums.length) return safeJson(res, 400, { ok: false, message: "docNums vacío" });

    const from = String(req.query?.from || "");
    const to = String(req.query?.to || "");

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const defaultFrom = addDaysISO(today, -30);

    const f = isISO(from) ? from : defaultFrom;
    const tt = isISO(to) ? to : today;

    const out = {};
    for (const dn of docNums) {
      try {
        const r = await traceQuoteTotals(dn, f, tt);
        if (r.ok) out[String(dn)] = { ok: true, totalEntregado: r.totalEntregado, pendiente: r.pendiente };
        else out[String(dn)] = { ok: false, message: r.message || "no ok" };
      } catch (e) {
        out[String(dn)] = { ok: false, message: e.message || String(e) };
      }
      await sleep(60);
    }

    return safeJson(res, 200, { ok: true, from: f, to: tt, delivered: out });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ DASHBOARD DB: Sync (SAP -> Supabase)
========================================================= */

/** Item group resolver (cache DB + memory) */
const ITEM_GROUP_MEM = new Map(); // itemCode -> {name, at}
const ITEM_GROUP_TTL = 7 * 24 * 60 * 60 * 1000;

async function getGroupFromDb(itemCode) {
  if (!hasDb()) return "";
  const r = await dbQuery(`SELECT group_name FROM item_group_cache WHERE item_code=$1 LIMIT 1`, [itemCode]);
  return r.rows?.[0]?.group_name || "";
}
async function setGroupToDb(itemCode, groupName) {
  if (!hasDb()) return;
  await dbQuery(
    `INSERT INTO item_group_cache(item_code, group_name, updated_at)
     VALUES($1,$2,NOW())
     ON CONFLICT(item_code) DO UPDATE SET group_name=EXCLUDED.group_name, updated_at=NOW()`,
    [itemCode, groupName]
  );
}

async function resolveItemGroup(itemCode) {
  const code = String(itemCode || "").trim();
  if (!code) return "";

  const mem = ITEM_GROUP_MEM.get(code);
  if (mem && Date.now() - mem.at < ITEM_GROUP_TTL) return mem.name;

  const dbVal = await getGroupFromDb(code);
  if (dbVal) {
    ITEM_GROUP_MEM.set(code, { name: dbVal, at: Date.now() });
    return dbVal;
  }

  // SAP: Items('CODE') -> ItemsGroupCode -> ItemGroups(id) -> GroupName
  try {
    const it = await slFetch(`/Items('${encodeURIComponent(code)}')?$select=ItemCode,ItemsGroupCode`, { timeoutMs: 20000 });
    const grpCode = Number(it?.ItemsGroupCode);
    if (!Number.isFinite(grpCode)) {
      ITEM_GROUP_MEM.set(code, { name: "", at: Date.now() });
      return "";
    }
    const grp = await slFetch(`/ItemGroups(${grpCode})?$select=GroupName`, { timeoutMs: 20000 });
    const name = String(grp?.GroupName || "").trim();

    await setGroupToDb(code, name);
    ITEM_GROUP_MEM.set(code, { name, at: Date.now() });
    return name;
  } catch {
    ITEM_GROUP_MEM.set(code, { name: "", at: Date.now() });
    return "";
  }
}

async function scanQuotationsHeaders({ from, to, maxDocs = 5000, onlyCreated = false }) {
  const toPlus1 = addDaysISO(to, 1);
  const batchTop = 200;
  let skip = 0;
  const out = [];

  const createdSet = onlyCreated ? await getCreatedUsersSetCached() : null;

  for (let page = 0; page < 250; page++) {
    const raw = await slFetch(
      `/Quotations?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
        `&$filter=${encodeURIComponent(`DocDate ge '${from}' and DocDate lt '${toPlus1}'`)}` +
        `&$orderby=DocDate asc,DocEntry asc&$top=${batchTop}&$skip=${skip}`,
      { timeoutMs: 25000 }
    );

    const rows = Array.isArray(raw?.value) ? raw.value : [];
    if (!rows.length) break;
    skip += rows.length;

    for (const q of rows) {
      // Dashboard DB NO quiere canceladas
      if (isCancelledLike(q)) continue;

      const usuario = parseUserFromComments(q.Comments || "") || "sin_user";
      if (createdSet) {
        const u = String(usuario || "").trim().toLowerCase();
        if (!u || !createdSet.has(u)) continue;
      }

      const wh = parseWhFromComments(q.Comments || "") || "sin_wh";

      out.push({
        DocEntry: Number(q.DocEntry),
        DocNum: Number(q.DocNum),
        DocDate: String(q.DocDate || "").slice(0, 10),
        DocTotal: Number(q.DocTotal || 0),
        CardCode: String(q.CardCode || ""),
        CardName: String(q.CardName || ""),
        DocumentStatus: String(q.DocumentStatus || ""),
        CancelStatus: String(q.CancelStatus ?? ""),
        Comments: String(q.Comments || ""),
        usuario,
        warehouse: wh,
      });

      if (out.length >= maxDocs) return out;
    }
  }
  return out;
}

async function upsertQuoteHead(h, delivered) {
  const docEntry = Number(h.DocEntry);
  const docNum = Number(h.DocNum);
  const docDate = String(h.DocDate || "").slice(0, 10);
  const cardCode = String(h.CardCode || "");
  const cardName = String(h.CardName || "");
  const usuario = String(h.usuario || "sin_user");
  const wh = String(h.warehouse || "sin_wh");
  const docTotal = Number(h.DocTotal || 0);
  const deliveredTotal = Number(delivered?.totalEntregado || 0);
  const pendingTotal = Number((docTotal - deliveredTotal).toFixed(2));

  await dbQuery(
    `
    INSERT INTO quote_head_cache
      (doc_entry, doc_num, doc_date, card_code, card_name, usuario, warehouse, doc_total, delivered_total, pending_total, document_status, cancel_status)
    VALUES
      ($1,$2,$3::date,$4,$5,$6,$7,$8,$9,$10,$11,$12)
    ON CONFLICT(doc_entry) DO UPDATE SET
      doc_num=EXCLUDED.doc_num,
      doc_date=EXCLUDED.doc_date,
      card_code=EXCLUDED.card_code,
      card_name=EXCLUDED.card_name,
      usuario=EXCLUDED.usuario,
      warehouse=EXCLUDED.warehouse,
      doc_total=EXCLUDED.doc_total,
      delivered_total=EXCLUDED.delivered_total,
      pending_total=EXCLUDED.pending_total,
      document_status=EXCLUDED.document_status,
      cancel_status=EXCLUDED.cancel_status,
      updated_at=NOW()
    `,
    [
      docEntry,
      docNum,
      docDate,
      cardCode,
      cardName,
      usuario,
      wh,
      docTotal,
      deliveredTotal,
      pendingTotal,
      String(h.DocumentStatus || ""),
      String(h.CancelStatus || ""),
    ]
  );
}

async function upsertQuoteLines(h, doc) {
  const docEntry = Number(h.DocEntry);
  const docNum = Number(h.DocNum);
  const docDate = String(h.DocDate || "").slice(0, 10);
  const cardCode = String(h.CardCode || "");
  const whDefault = String(h.warehouse || "sin_wh");

  const lines = Array.isArray(doc?.DocumentLines) ? doc.DocumentLines : [];
  if (!lines.length) return 0;

  const values = [];
  const params = [];
  let p = 1;

  for (const ln of lines) {
    const lineNum = Number(ln.LineNum);
    if (!Number.isFinite(lineNum)) continue;

    const itemCode = String(ln.ItemCode || "").trim();
    const itemDesc = String(ln.ItemDescription || ln.ItemName || "").trim();
    const qty = Number(ln.Quantity || 0);
    const lt = Number(ln.LineTotal || 0);
    const wh = String(ln.WarehouseCode || whDefault || "sin_wh").trim() || "sin_wh";

    // resolve group (cached)
    const groupName = itemCode ? await resolveItemGroup(itemCode) : "";

    params.push(docEntry, lineNum, docNum, docDate, cardCode, wh, itemCode, itemDesc, groupName, qty, lt);
    values.push(`($${p++},$${p++},$${p++},$${p++}::date,$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);
  }

  if (!values.length) return 0;

  await dbQuery(
    `
    INSERT INTO quote_line_cache
      (doc_entry,line_num,doc_num,doc_date,card_code,warehouse,item_code,item_desc,item_group,quantity,line_total)
    VALUES ${values.join(",")}
    ON CONFLICT(doc_entry,line_num) DO UPDATE SET
      doc_num=EXCLUDED.doc_num,
      doc_date=EXCLUDED.doc_date,
      card_code=EXCLUDED.card_code,
      warehouse=EXCLUDED.warehouse,
      item_code=EXCLUDED.item_code,
      item_desc=EXCLUDED.item_desc,
      item_group=EXCLUDED.item_group,
      quantity=EXCLUDED.quantity,
      line_total=EXCLUDED.line_total,
      updated_at=NOW()
    `,
    params
  );

  return values.length;
}

async function syncQuotesRange({ from, to, maxDocs = 5000, onlyCreated = true }) {
  if (!hasDb()) throw new Error("DB no configurada (DATABASE_URL)");
  if (missingSapEnv()) throw new Error("Faltan variables SAP");

  const headers = await scanQuotationsHeaders({ from, to, maxDocs, onlyCreated });
  let linesCount = 0;

  // estable: 1-by-1 (evita timeouts)
  for (let i = 0; i < headers.length; i++) {
    const h = headers[i];
    try {
      const delivered = await traceQuoteTotals(h.DocNum, from, to); // fill-rate real
      await upsertQuoteHead(h, delivered.ok ? delivered : { totalEntregado: 0 });

      const doc = await sapGetByDocEntry("Quotations", h.DocEntry, 30000);
      linesCount += await upsertQuoteLines(h, doc);

    } catch {
      // sigue con el próximo
    }
    await sleep(35);
  }

  await setState("quotes_last_sync_from", from);
  await setState("quotes_last_sync_to", to);
  await setState("quotes_last_sync_at", new Date().toISOString());

  return { headers: headers.length, lines: linesCount };
}

/* Sync endpoints */
app.post("/api/admin/quotes/sync", verifyAdmin, async (req, res) => {
  try {
    const from = String(req.query?.from || "");
    const to = String(req.query?.to || "");
    if (!isISO(from) || !isISO(to)) return safeJson(res, 400, { ok: false, message: "Requiere from y to (YYYY-MM-DD)" });

    const maxDocsRaw = Number(req.query?.maxDocs || 5000);
    const maxDocs = Math.max(50, Math.min(20000, Number.isFinite(maxDocsRaw) ? Math.trunc(maxDocsRaw) : 5000));

    const onlyCreated = String(req.query?.onlyCreated || "1") === "1";

    const out = await syncQuotesRange({ from, to, maxDocs, onlyCreated });
    return safeJson(res, 200, { ok: true, ...out, from, to, maxDocs, onlyCreated });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.post("/api/admin/quotes/sync/recent", verifyAdmin, async (req, res) => {
  try {
    const daysRaw = Number(req.query?.days || 5);
    const days = Math.max(1, Math.min(60, Number.isFinite(daysRaw) ? Math.trunc(daysRaw) : 5));

    const maxDocsRaw = Number(req.query?.maxDocs || 2500);
    const maxDocs = Math.max(50, Math.min(20000, Number.isFinite(maxDocsRaw) ? Math.trunc(maxDocsRaw) : 2500));

    const onlyCreated = String(req.query?.onlyCreated || "1") === "1";

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = addDaysISO(today, -days);

    const out = await syncQuotesRange({ from, to: today, maxDocs, onlyCreated });
    return safeJson(res, 200, { ok: true, ...out, from, to: today, days, maxDocs, onlyCreated });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ DASHBOARD (desde DB) - rápido
========================================================= */
const NON_CONSUMABLE = new Set([
  "Cuidado de la Ropa",
  "Art. De limpieza",
  "M.P. Cuid. de la Rop",
  "Prod. De limpieza",
]);

function grossPct(num, den) {
  const n = Number(num || 0);
  const d = Number(den || 0);
  return d > 0 ? Number(((n / d) * 100).toFixed(2)) : 0;
}

app.get("/api/admin/quotes/dashboard-db", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const fromQ = String(req.query?.from || "");
    const toQ = String(req.query?.to || "");

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const defaultFrom = addDaysISO(today, -30);

    const from = isISO(fromQ) ? fromQ : defaultFrom;
    const to = isISO(toQ) ? toQ : today;

    const totals = await dbQuery(
      `
      SELECT
        COUNT(*)::int AS quotes,
        COALESCE(SUM(doc_total),0)::numeric(18,2) AS cotizado,
        COALESCE(SUM(delivered_total),0)::numeric(18,2) AS entregado
      FROM quote_head_cache
      WHERE doc_date >= $1::date AND doc_date <= $2::date
      `,
      [from, to]
    );

    const cot = Number(totals.rows?.[0]?.cotizado || 0);
    const ent = Number(totals.rows?.[0]?.entregado || 0);

    const byUser = await dbQuery(
      `
      SELECT usuario, COUNT(*)::int AS cnt,
             COALESCE(SUM(doc_total),0)::numeric(18,2) AS cotizado,
             COALESCE(SUM(delivered_total),0)::numeric(18,2) AS entregado
      FROM quote_head_cache
      WHERE doc_date >= $1::date AND doc_date <= $2::date
      GROUP BY 1
      ORDER BY cotizado DESC
      LIMIT 25
      `,
      [from, to]
    );

    const byWh = await dbQuery(
      `
      SELECT warehouse, COUNT(*)::int AS cnt,
             COALESCE(SUM(doc_total),0)::numeric(18,2) AS cotizado,
             COALESCE(SUM(delivered_total),0)::numeric(18,2) AS entregado
      FROM quote_head_cache
      WHERE doc_date >= $1::date AND doc_date <= $2::date
      GROUP BY 1
      ORDER BY cotizado DESC
      LIMIT 25
      `,
      [from, to]
    );

    const byClient = await dbQuery(
      `
      SELECT card_code, card_name,
             COALESCE(SUM(doc_total),0)::numeric(18,2) AS cotizado
      FROM quote_head_cache
      WHERE doc_date >= $1::date AND doc_date <= $2::date
      GROUP BY 1,2
      ORDER BY cotizado DESC
      LIMIT 25
      `,
      [from, to]
    );

    const byMonth = await dbQuery(
      `
      SELECT to_char(date_trunc('month', doc_date),'YYYY-MM') AS month,
             COUNT(*)::int AS cnt,
             COALESCE(SUM(doc_total),0)::numeric(18,2) AS cotizado,
             COALESCE(SUM(delivered_total),0)::numeric(18,2) AS entregado
      FROM quote_head_cache
      WHERE doc_date >= $1::date AND doc_date <= $2::date
      GROUP BY 1
      ORDER BY 1
      `,
      [from, to]
    );

    // categorías por grupo desde líneas
    const byGroup = await dbQuery(
      `
      SELECT item_group,
             COALESCE(SUM(line_total),0)::numeric(18,2) AS cotizado
      FROM quote_line_cache
      WHERE doc_date >= $1::date AND doc_date <= $2::date
        AND item_group <> ''
      GROUP BY 1
      ORDER BY cotizado DESC
      LIMIT 50
      `,
      [from, to]
    );

    let consumibles = 0;
    let noConsumibles = 0;
    for (const r of byGroup.rows || []) {
      const g = String(r.item_group || "").trim();
      const v = Number(r.cotizado || 0);
      if (NON_CONSUMABLE.has(g)) noConsumibles += v;
      else consumibles += v;
    }

    return safeJson(res, 200, {
      ok: true,
      from,
      to,
      lastSyncAt: await getState("quotes_last_sync_at"),
      totals: {
        quotes: Number(totals.rows?.[0]?.quotes || 0),
        cotizado: cot,
        entregado: ent,
        fillRatePct: grossPct(ent, cot),
      },
      byUser: (byUser.rows || []).map((r) => ({
        usuario: r.usuario,
        cnt: Number(r.cnt || 0),
        cotizado: Number(r.cotizado || 0),
        entregado: Number(r.entregado || 0),
        fillRatePct: grossPct(Number(r.entregado || 0), Number(r.cotizado || 0)),
      })),
      byWh: (byWh.rows || []).map((r) => ({
        warehouse: r.warehouse,
        cnt: Number(r.cnt || 0),
        cotizado: Number(r.cotizado || 0),
        entregado: Number(r.entregado || 0),
        fillRatePct: grossPct(Number(r.entregado || 0), Number(r.cotizado || 0)),
      })),
      byClient: (byClient.rows || []).map((r) => ({
        customer: `${r.card_code} · ${r.card_name}`,
        cotizado: Number(r.cotizado || 0),
      })),
      byMonth: (byMonth.rows || []).map((r) => ({
        month: r.month,
        cnt: Number(r.cnt || 0),
        cotizado: Number(r.cotizado || 0),
        entregado: Number(r.entregado || 0),
        fillRatePct: grossPct(Number(r.entregado || 0), Number(r.cotizado || 0)),
      })),
      byGroup: (byGroup.rows || []).map((r) => ({
        group: r.item_group,
        cotizado: Number(r.cotizado || 0),
      })),
      pie: {
        consumibles,
        noConsumibles,
      },
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ START
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

  app.listen(Number(PORT), () => console.log(`Server listening on :${PORT}`));
})();
