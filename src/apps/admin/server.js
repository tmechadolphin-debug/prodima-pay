import express from "express";
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

  // ⚠️ pon esto en Render:
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
    db: hasDb() ? "on" : "off",
    sap: missingSapEnv() ? "missing" : "ok",
    priceList: SAP_PRICE_LIST,
    whDefault: SAP_WAREHOUSE,
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
   - timeout corto (12s) para listados
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
  try {
    data = JSON.parse(txt);
  } catch {}

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
  const timeoutMs = Number(options.timeoutMs || 12000); // ✅ 12s por defecto
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

  const r = await slFetch(`/${entity}?${parts.join("&")}`, { timeoutMs: 12000 });
  const arr = Array.isArray(r?.value) ? r.value : [];
  return arr[0] || null;
}
async function sapGetByDocEntry(entity, docEntry) {
  const n = Number(docEntry);
  if (!Number.isFinite(n) || n <= 0) throw new Error("DocEntry inválido");
  return slFetch(`/${entity}(${n})`, { timeoutMs: 12000 });
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
   ✅ TRACE cache (entregado por docNum)
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

  // ⚠️ trazado es pesado => timeout mayor SOLO aquí
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

  // ✅ listados con timeout un poco mayor
  const ordersList = await slFetch(
    `/Orders?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
      `&$filter=${encodeURIComponent(
        `CardCode eq '${cardCode.replace(/'/g, "''")}' and DocDate ge '${from}' and DocDate lt '${toPlus1}'`
      )}` +
      `&$orderby=DocDate desc,DocEntry desc&$top=200`,
    { timeoutMs: 18000 }
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

  let totalEntregado = 0;

  if (orders.length) {
    const orderDocEntrySet = new Set(orders.map((x) => Number(x.DocEntry)));

    const delList = await slFetch(
      `/DeliveryNotes?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
        `&$filter=${encodeURIComponent(
          `CardCode eq '${cardCode.replace(/'/g, "''")}' and DocDate ge '${from}' and DocDate lt '${toPlus1}'`
        )}` +
        `&$orderby=DocDate desc,DocEntry desc&$top=300`,
      { timeoutMs: 18000 }
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
          totalEntregado += Number(dd?.DocTotal || 0);
        }
      }
      await sleep(15);
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
   ✅ ADMIN USERS (igual)
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
   ✅ ADMIN QUOTES (rápido)
   - NO calcula entregado aquí
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
      { timeoutMs: 12000 }
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
          // entregado se llena luego por endpoint batch:
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

    const f = /^\d{4}-\d{2}-\d{2}$/.test(from) ? from : defaultFrom;
    const t = /^\d{4}-\d{2}-\d{2}$/.test(to) ? to : today;

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

/* =========================================================
   ✅ CACHE Items -> Group y GroupCode -> GroupName
========================================================= */
const ITEM_GROUP_CACHE = new Map(); // itemCode -> { groupCode, at }
const GROUP_NAME_CACHE = new Map(); // groupCode -> { name, at }
const IG_TTL = 12 * 60 * 60 * 1000;

function igCacheGet(map, key) {
  const it = map.get(key);
  if (!it) return null;
  if (Date.now() - it.at > IG_TTL) {
    map.delete(key);
    return null;
  }
  return it;
}
function igCacheSet(map, key, value) {
  map.set(key, { ...value, at: Date.now() });
}

async function sapGetQuotationLines(docEntry) {
  const n = Number(docEntry);
  if (!Number.isFinite(n) || n <= 0) return [];

  // Trae SOLO líneas (ItemCode y LineTotal) para no cargar pesado
  const q = await slFetch(
    `/Quotations(${n})?$select=DocEntry,DocNum&$expand=` +
      `DocumentLines($select=ItemCode,LineTotal,Quantity)`,
    { timeoutMs: 18000 }
  );

  const lines = Array.isArray(q?.DocumentLines) ? q.DocumentLines : [];
  return lines.map((l) => ({
    itemCode: String(l?.ItemCode || "").trim(),
    lineTotal: Number(l?.LineTotal || 0),
    qty: Number(l?.Quantity || 0),
  })).filter(x => x.itemCode);
}

async function sapGetItemsGroupCodes(itemCodes) {
  // Devuelve map itemCode -> groupCode
  const out = new Map();

  // Primero, intenta desde cache
  const pending = [];
  for (const code of itemCodes) {
    const hit = igCacheGet(ITEM_GROUP_CACHE, code);
    if (hit?.groupCode != null) out.set(code, hit.groupCode);
    else pending.push(code);
  }
  if (!pending.length) return out;

  // SAP SL no siempre soporta "in (...)" fácil, así que usamos OR por chunks
  const chunkSize = 15;
  for (let i = 0; i < pending.length; i += chunkSize) {
    const chunk = pending.slice(i, i + chunkSize);

    const ors = chunk
      .map((c) => `ItemCode eq '${c.replace(/'/g, "''")}'`)
      .join(" or ");

    const r = await slFetch(
      `/Items?$select=ItemCode,ItemsGroupCode&$filter=${encodeURIComponent(ors)}&$top=${chunkSize}`,
      { timeoutMs: 18000 }
    );

    const rows = Array.isArray(r?.value) ? r.value : [];
    for (const it of rows) {
      const ic = String(it?.ItemCode || "").trim();
      const gc = Number(it?.ItemsGroupCode);
      if (ic) {
        out.set(ic, gc);
        igCacheSet(ITEM_GROUP_CACHE, ic, { groupCode: gc });
      }
    }

    await sleep(40);
  }

  return out;
}

async function sapGetGroupNames(groupCodes) {
  const out = new Map();

  const pending = [];
  for (const gc of groupCodes) {
    const k = String(gc);
    const hit = igCacheGet(GROUP_NAME_CACHE, k);
    if (hit?.name) out.set(gc, hit.name);
    else pending.push(gc);
  }
  if (!pending.length) return out;

  // ItemGroups: ItmsGrpCod, ItmsGrpNam
  const chunkSize = 20;
  for (let i = 0; i < pending.length; i += chunkSize) {
    const chunk = pending.slice(i, i + chunkSize);
    const ors = chunk.map((gc) => `ItmsGrpCod eq ${Number(gc)}`).join(" or ");

    const r = await slFetch(
      `/ItemGroups?$select=ItmsGrpCod,ItmsGrpNam&$filter=${encodeURIComponent(ors)}&$top=${chunkSize}`,
      { timeoutMs: 18000 }
    );

    const rows = Array.isArray(r?.value) ? r.value : [];
    for (const g of rows) {
      const code = Number(g?.ItmsGrpCod);
      const name = String(g?.ItmsGrpNam || "").trim();
      if (Number.isFinite(code) && name) {
        out.set(code, name);
        igCacheSet(GROUP_NAME_CACHE, String(code), { name });
      }
    }

    await sleep(40);
  }

  return out;
}

/* =========================================================
   ✅ DASHBOARD (agregados por grupo + consumibles)
   GET /api/admin/dashboard?from=YYYY-MM-DD&to=YYYY-MM-DD&limit=200&skip=0
========================================================= */
app.get("/api/admin/dashboard", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const from = String(req.query?.from || "");
    const to = String(req.query?.to || "");

    const limitRaw = req.query?.limit != null ? Number(req.query.limit) : 200;
    const limit = Math.max(1, Math.min(400, Number.isFinite(limitRaw) ? limitRaw : 200));
    const skip = req.query?.skip != null ? Math.max(0, Number(req.query.skip) || 0) : 0;

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const defaultFrom = addDaysISO(today, -30);

    const f = /^\d{4}-\d{2}-\d{2}$/.test(from) ? from : defaultFrom;
    const t = /^\d{4}-\d{2}-\d{2}$/.test(to) ? to : today;

    // 1) Trae quotes “rápido” (encabezado)
    const { pageRows, totalFiltered } = await scanQuotes({
      f, t,
      wantSkip: skip,
      wantLimit: limit,
      userFilter: "",
      clientFilter: "",
      onlyCreated: false,
    });

    // 2) Para esas quotes, trae líneas y resuelve grupos
    const allItemCodes = new Set();
    const quotesWithLines = [];

    for (const q of pageRows) {
      const lines = await sapGetQuotationLines(q.docEntry);
      for (const l of lines) allItemCodes.add(l.itemCode);
      quotesWithLines.push({ ...q, lines });
      await sleep(30);
    }

    const itemCodesArr = Array.from(allItemCodes);
    const itemToGroup = await sapGetItemsGroupCodes(itemCodesArr);

    const groupCodes = new Set();
    for (const gc of itemToGroup.values()) if (Number.isFinite(gc)) groupCodes.add(gc);
    const groupNameMap = await sapGetGroupNames(Array.from(groupCodes));

    // 3) Clasificación consumible vs no consumible (por nombre de grupo)
    const NO_CONSUMIBLES = new Set([
      "Cuidado de la Ropa",
      "Art. De Limpieza",
      "Prod. De Limpieza",
      "M.P. Cuid. de la Rop",
    ].map(s => s.toLowerCase()));

    // 4) Agregados
    const byGroup = new Map(); // groupName -> { cotizado, countLines }
    let totalConsumibles = 0;
    let totalNoConsumibles = 0;

    for (const q of quotesWithLines) {
      for (const l of q.lines) {
        const gc = itemToGroup.get(l.itemCode);
        const gname = groupNameMap.get(gc) || "Sin grupo";
        const key = String(gname);

        const cur = byGroup.get(key) || { cotizado: 0, countLines: 0 };
        cur.cotizado += Number(l.lineTotal || 0);
        cur.countLines += 1;
        byGroup.set(key, cur);

        const isNo = NO_CONSUMIBLES.has(String(gname).toLowerCase());
        if (isNo) totalNoConsumibles += Number(l.lineTotal || 0);
        else totalConsumibles += Number(l.lineTotal || 0);
      }
    }

    const groupsArr = Array.from(byGroup.entries())
      .map(([group, v]) => ({ group, cotizado: Number(v.cotizado.toFixed(2)), countLines: v.countLines }))
      .sort((a, b) => b.cotizado - a.cotizado);

    return safeJson(res, 200, {
      ok: true,
      from: f,
      to: t,
      total: totalFiltered,
      quotes: quotesWithLines, // si tu UI no lo necesita, puedes quitarlo
      meta: {
        groups: groupsArr,
        consumibles: Number(totalConsumibles.toFixed(2)),
        noConsumibles: Number(totalNoConsumibles.toFixed(2)),
      },
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ ENTREGADO BATCH (nuevo)
   GET /api/admin/quotes/delivered?docNums=123,456&from=YYYY-MM-DD&to=YYYY-MM-DD
========================================================= */
app.get("/api/admin/quotes/delivered", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const docNums = String(req.query?.docNums || "")
      .split(",")
      .map((x) => Number(String(x).trim()))
      .filter((n) => Number.isFinite(n) && n > 0)
      .slice(0, 20); // ✅ máximo 20 por batch

    if (!docNums.length) return safeJson(res, 400, { ok: false, message: "docNums vacío" });

    const from = String(req.query?.from || "");
    const to = String(req.query?.to || "");

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const defaultFrom = addDaysISO(today, -30);

    const f = /^\d{4}-\d{2}-\d{2}$/.test(from) ? from : defaultFrom;
    const tt = /^\d{4}-\d{2}-\d{2}$/.test(to) ? to : today;

    const out = {};
    // ✅ 1 por 1 para no morir por timeout
    for (const dn of docNums) {
      try {
        const r = await traceQuoteTotals(dn, f, tt);
        if (r.ok) out[String(dn)] = { ok: true, totalEntregado: r.totalEntregado, pendiente: r.pendiente };
        else out[String(dn)] = { ok: false, message: r.message || "no ok" };
      } catch (e) {
        out[String(dn)] = { ok: false, message: e.message || String(e) };
      }
      await sleep(80);
    }

    return safeJson(res, 200, { ok: true, from: f, to: tt, delivered: out });
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
