// server.js
import express from "express";
import pg from "pg";
import jwt from "jsonwebtoken";

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

  SAP_BASE_URL = "",
  SAP_COMPANYDB = "",
  SAP_USER = "",
  SAP_PASS = "",

  // Render:
  // CORS_ORIGIN=https://prodima.com.pa,https://www.prodima.com.pa
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
  // ✅ mantenemos igual (no cambiamos nada de CORS aquí)
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

  // Usuarios
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

  // Cache de cotizaciones (PowerBI-like)
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS quotes_cache (
      id BIGSERIAL PRIMARY KEY,
      doc_num BIGINT UNIQUE NOT NULL,
      doc_entry BIGINT,
      doc_date DATE,
      doc_time INT,
      card_code TEXT,
      card_name TEXT,
      usuario TEXT,
      warehouse TEXT,
      doc_total NUMERIC(19,6) DEFAULT 0,
      delivered_total NUMERIC(19,6) DEFAULT 0,
      status TEXT,
      cancel_status TEXT,
      comments TEXT,
      group_name TEXT,
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  // ✅ NUEVO: cache de líneas por cotización (para modal sin tocar SAP)
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS quote_lines_cache (
      id BIGSERIAL PRIMARY KEY,
      doc_num BIGINT NOT NULL,
      doc_date DATE,
      item_code TEXT NOT NULL,
      item_desc TEXT DEFAULT '',
      qty_quoted NUMERIC(19,6) DEFAULT 0,
      qty_delivered NUMERIC(19,6) DEFAULT 0,
      dollars_quoted NUMERIC(19,6) DEFAULT 0,
      dollars_delivered NUMERIC(19,6) DEFAULT 0,
      updated_at TIMESTAMP DEFAULT NOW(),
      UNIQUE(doc_num, item_code)
    );
  `);

  // Estado (last sync)
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS app_state (
      k TEXT PRIMARY KEY,
      v TEXT NOT NULL,
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_quotes_cache_date ON quotes_cache(doc_date);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_quotes_cache_user ON quotes_cache(usuario);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_quotes_cache_wh ON quotes_cache(warehouse);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_quotes_cache_card ON quotes_cache(card_code);`);

  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_quote_lines_doc ON quote_lines_cache(doc_num);`);
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

function missingSapEnv() {
  return !SAP_BASE_URL || !SAP_COMPANYDB || !SAP_USER || !SAP_PASS;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// Panamá = UTC-5
const TZ_OFFSET_MIN = -300;

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
function isoDateTimeInOffset(offsetMin = 0) {
  const d = new Date(nowInOffsetMs(offsetMin));
  return d.toISOString().replace("Z", "");
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
function toDocDateFilterRange(fromIso, toIso) {
  // Service Layer usa DocDate date; hacemos rango [from, to+1)
  const toPlus1 = addDaysISO(toIso, 1);
  return { from: fromIso, toPlus1 };
}

function parseUserFromComments(comments) {
  const m = String(comments || "").match(/\[user:([^\]]+)\]/i);
  return m ? String(m[1]).trim() : "";
}
function parseWhFromComments(comments) {
  const m = String(comments || "").match(/\[wh:([^\]]+)\]/i);
  return m ? String(m[1]).trim() : "";
}

/**
 * ✅ Ajuste mínimo:
 * Antes estabas descartando por "cancel" en comments y eso puede tumbar docs.
 * Aquí dejamos solo CancelStatus real.
 */
function isCancelledLike(q) {
  const cancelVal = q?.CancelStatus ?? q?.cancelStatus ?? q?.Cancelled ?? q?.cancelled ?? "";
  const cancelRaw = String(cancelVal).trim().toLowerCase();
  return cancelRaw === "csyes" || cancelRaw === "yes" || cancelRaw === "true" || cancelRaw.includes("csyes");
}

/* =========================
   HEALTH
========================= */
app.get("/api/health", async (req, res) => {
  safeJson(res, 200, {
    ok: true,
    message: "✅ PRODIMA ADMIN API activa",
    db: hasDb() ? "on" : "off",
    sap: missingSapEnv() ? "missing" : "ok",
    nowPanama: isoDateTimeInOffset(TZ_OFFSET_MIN),
  });
});

/* =========================
   fetch wrapper
========================= */
let _fetch = globalThis.fetch || null;
async function httpFetch(url, options) {
  if (_fetch) return _fetch(url, options);
  const mod = await import("node-fetch");
  _fetch = mod.default;
  return _fetch(url, options);
}

/* =========================
   SAP Service Layer
========================= */
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
  const timeoutMs = Number(options.timeoutMs || 12000);
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

/* =========================
   ✅ CATEGORÍAS / GROUP_NAME
   - Cachea ItemCode -> group
========================= */
const ITEM_GROUP_CACHE = new Map(); // key: itemCode => {at, group}
const ITEM_GROUP_TTL_MS = 6 * 60 * 60 * 1000;

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ");
}

function mapToSixCats(raw) {
  const t = norm(raw);
  if (!t) return "";

  if (t.includes("sazon")) return "Sazonadores";
  if (t.includes("vinagr")) return "Vinagres";
  if (t.includes("cuidado de la ropa") || (t.includes("cuidado") && t.includes("ropa"))) return "Cuidado de la Ropa";
  if (t.includes("prod") && t.includes("limp")) return "Prod. De limpieza";
  if (t.includes("art") && t.includes("limp")) return "Art. De limpieza";
  if (t.includes("especial") || t.includes("gmt")) return "Especialidades y GMT";

  if (t.includes("limpieza") && t.includes("producto")) return "Prod. De limpieza";
  if (t.includes("limpieza") && t.includes("articulo")) return "Art. De limpieza";

  return "";
}

function cacheItemGet(itemCode) {
  const it = ITEM_GROUP_CACHE.get(itemCode);
  if (!it) return null;
  if (Date.now() - it.at > ITEM_GROUP_TTL_MS) {
    ITEM_GROUP_CACHE.delete(itemCode);
    return null;
  }
  return it.group || null;
}
function cacheItemSet(itemCode, group) {
  ITEM_GROUP_CACHE.set(itemCode, { at: Date.now(), group: group || null });
}

async function sapGetItemGroupName(itemCode) {
  const code = String(itemCode || "").trim();
  if (!code) return "";

  const cached = cacheItemGet(code);
  if (cached !== null) return cached || "";

  const safe = code.replace(/'/g, "''");

  let item = null;
  try {
    item = await slFetch(`/Items('${safe}')`, { timeoutMs: 12000 });
  } catch {
    cacheItemSet(code, "");
    return "";
  }

  const candidates = [
    item?.U_group_name,
    item?.U_GroupName,
    item?.U_GROUP_NAME,
    item?.U_Categoria,
    item?.U_CATEGORIA,
    item?.U_CATEGORY,
    item?.U_Cat,
    item?.U_LINEA,
    item?.U_Grupo,
    item?.U_GRUPO,
  ]
    .map((x) => String(x || "").trim())
    .filter(Boolean);

  for (const c of candidates) {
    const mapped = mapToSixCats(c);
    if (mapped) {
      cacheItemSet(code, mapped);
      return mapped;
    }
  }

  const igc = item?.ItemsGroupCode;
  const igcNum = Number(igc);
  if (Number.isFinite(igcNum) && igcNum > 0) {
    try {
      const g = await slFetch(`/ItemGroups(${igcNum})`, { timeoutMs: 12000 });
      const gname = String(g?.GroupName || g?.groupName || "").trim();
      const mapped = mapToSixCats(gname);
      if (mapped) {
        cacheItemSet(code, mapped);
        return mapped;
      }
    } catch {}
  }

  cacheItemSet(code, "");
  return "";
}

async function inferQuoteGroupNameByDocEntry(docEntry) {
  try {
    const q = await sapGetByDocEntry("Quotations", docEntry);
    const lines = Array.isArray(q?.DocumentLines) ? q.DocumentLines : [];
    if (!lines.length) return "";

    const maxLines = Math.min(12, lines.length);
    const counts = new Map();

    for (let i = 0; i < maxLines; i++) {
      const itemCode = lines[i]?.ItemCode;
      const g = await sapGetItemGroupName(itemCode);
      if (g) counts.set(g, (counts.get(g) || 0) + 1);
      await sleep(10);
    }

    let best = "";
    let bestN = 0;
    for (const [k, v] of counts.entries()) {
      if (v > bestN) {
        best = k;
        bestN = v;
      }
    }
    return best || "";
  } catch {
    return "";
  }
}

/* =========================
   TRACE entregado (Quote->Orders->Delivery)
========================= */
const TRACE_CACHE = new Map();
const TRACE_TTL_MS = 2 * 60 * 60 * 1000;

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
    await sleep(10);
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
      await sleep(10);
    }
  }

  const totalCotizado = Number(quote.DocTotal || 0);
  const pendiente = Number((totalCotizado - totalEntregado).toFixed(2));

  const out = { ok: true, totalCotizado, totalEntregado, pendiente };
  cacheSet(cacheKey, out);
  return out;
}

/* =========================
   ✅ NUEVO: Trace por líneas (entregado por ItemCode)
   - Se usa SOLO en SYNC para poblar quote_lines_cache
========================= */
async function traceQuoteLinesByItem(quoteDocNum, fromOverride, toOverride) {
  const out = new Map(); // itemCode -> { qtyDelivered, dollarsDelivered }

  const quoteHead = await sapGetFirstByDocNum("Quotations", quoteDocNum, "DocEntry,DocNum,DocDate,CardCode");
  if (!quoteHead?.DocEntry) return out;

  const quote = await sapGetByDocEntry("Quotations", quoteHead.DocEntry);
  const quoteDocEntry = Number(quote.DocEntry);
  const cardCode = String(quote.CardCode || "").trim();
  const quoteDate = String(quote.DocDate || "").slice(0, 10);

  const from = /^\d{4}-\d{2}-\d{2}$/.test(String(fromOverride || "")) ? String(fromOverride) : addDaysISO(quoteDate, -30);
  const to = /^\d{4}-\d{2}-\d{2}$/.test(String(toOverride || "")) ? String(toOverride) : addDaysISO(quoteDate, 60);
  const toPlus1 = addDaysISO(to, 1);

  const ordersList = await slFetch(
    `/Orders?$select=DocEntry,DocNum,DocDate,CardCode` +
      `&$filter=${encodeURIComponent(
        `CardCode eq '${cardCode.replace(/'/g, "''")}' and DocDate ge '${from}' and DocDate lt '${toPlus1}'`
      )}` +
      `&$orderby=DocDate desc,DocEntry desc&$top=200`,
    { timeoutMs: 20000 }
  );

  const orderCandidates = Array.isArray(ordersList?.value) ? ordersList.value : [];
  const orderDocEntrySet = new Set();

  for (const o of orderCandidates) {
    const od = await sapGetByDocEntry("Orders", o.DocEntry);
    const lines = Array.isArray(od?.DocumentLines) ? od.DocumentLines : [];
    const linked = lines.some((l) => Number(l?.BaseType) === 23 && Number(l?.BaseEntry) === quoteDocEntry);
    if (linked) orderDocEntrySet.add(Number(od.DocEntry));
    await sleep(10);
  }

  if (!orderDocEntrySet.size) return out;

  const delList = await slFetch(
    `/DeliveryNotes?$select=DocEntry,DocNum,DocDate,CardCode` +
      `&$filter=${encodeURIComponent(
        `CardCode eq '${cardCode.replace(/'/g, "''")}' and DocDate ge '${from}' and DocDate lt '${toPlus1}'`
      )}` +
      `&$orderby=DocDate desc,DocEntry desc&$top=300`,
    { timeoutMs: 20000 }
  );

  const delCandidates = Array.isArray(delList?.value) ? delList.value : [];
  const seen = new Set();

  for (const d of delCandidates) {
    const dd = await sapGetByDocEntry("DeliveryNotes", d.DocEntry);
    const de = Number(dd?.DocEntry);
    if (!de || seen.has(de)) continue;
    seen.add(de);

    const lines = Array.isArray(dd?.DocumentLines) ? dd.DocumentLines : [];

    const linked = lines.some((l) => Number(l?.BaseType) === 17 && orderDocEntrySet.has(Number(l?.BaseEntry)));
    if (!linked) {
      await sleep(10);
      continue;
    }

    for (const ln of lines) {
      if (Number(ln?.BaseType) !== 17) continue;
      if (!orderDocEntrySet.has(Number(ln?.BaseEntry))) continue;

      const itemCode = String(ln?.ItemCode || "").trim();
      if (!itemCode) continue;

      const qty = Number(ln?.Quantity || 0);
      const dollars = Number(ln?.LineTotal ?? 0);

      const prev = out.get(itemCode) || { qtyDelivered: 0, dollarsDelivered: 0 };
      prev.qtyDelivered += Number.isFinite(qty) ? qty : 0;
      prev.dollarsDelivered += Number.isFinite(dollars) ? dollars : 0;
      out.set(itemCode, prev);
    }

    await sleep(10);
  }

  return out;
}

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
   ADMIN USERS (solo lectura aquí; tu backend completo ya lo tienes)
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

/* =========================
   SYNC (DB) - Ventanas cortas
   GET /api/admin/quotes/sync?mode=days|hours|minutes&n=5&maxDocs=500
========================= */
async function setState(k, v) {
  await dbQuery(
    `INSERT INTO app_state(k,v,updated_at)
     VALUES($1,$2,NOW())
     ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v, updated_at=NOW()`,
    [k, String(v)]
  );
}
async function getState(k) {
  const r = await dbQuery(`SELECT v FROM app_state WHERE k=$1`, [k]);
  return r.rows?.[0]?.v || "";
}

// parse DocDate + DocTime (HHMM) => timestamp ms (Panama offset)
function docDateTimeToMs(docDate, docTime) {
  const d = String(docDate || "").slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(d)) return 0;
  const t = Number(docTime || 0);
  const hh = Math.floor(t / 100);
  const mm = t % 100;
  const iso = `${d}T${String(hh).padStart(2, "0")}:${String(mm).padStart(2, "0")}:00`;
  const dt = new Date(iso + "Z");
  const msLocalAsUtc = dt.getTime();
  return msLocalAsUtc - (TZ_OFFSET_MIN * 60000);
}

async function upsertQuoteCache(row) {
  const q = row;
  await dbQuery(
    `INSERT INTO quotes_cache(
      doc_num, doc_entry, doc_date, doc_time, card_code, card_name,
      usuario, warehouse, doc_total, delivered_total,
      status, cancel_status, comments, group_name, updated_at
     ) VALUES(
      $1,$2,$3,$4,$5,$6,
      $7,$8,$9,$10,
      $11,$12,$13,$14,NOW()
     )
     ON CONFLICT (doc_num) DO UPDATE SET
      doc_entry=EXCLUDED.doc_entry,
      doc_date=EXCLUDED.doc_date,
      doc_time=EXCLUDED.doc_time,
      card_code=EXCLUDED.card_code,
      card_name=EXCLUDED.card_name,
      usuario=EXCLUDED.usuario,
      warehouse=EXCLUDED.warehouse,
      doc_total=EXCLUDED.doc_total,
      delivered_total=GREATEST(quotes_cache.delivered_total, EXCLUDED.delivered_total),
      status=EXCLUDED.status,
      cancel_status=EXCLUDED.cancel_status,
      comments=EXCLUDED.comments,
      group_name=COALESCE(EXCLUDED.group_name, quotes_cache.group_name),
      updated_at=NOW()`,
    [
      Number(q.docNum),
      Number(q.docEntry || 0) || null,
      String(q.docDate || "").slice(0, 10) || null,
      Number(q.docTime || 0) || 0,
      String(q.cardCode || ""),
      String(q.cardName || ""),
      String(q.usuario || ""),
      String(q.warehouse || ""),
      Number(q.docTotal || 0),
      Number(q.deliveredTotal || 0),
      String(q.status || ""),
      String(q.cancelStatus || ""),
      String(q.comments || ""),
      q.groupName ? String(q.groupName) : null,
    ]
  );
}

/* =========================
   ✅ NUEVO: quote_lines_cache helpers
========================= */
async function upsertQuoteLineCache(row) {
  const r = row || {};
  await dbQuery(
    `INSERT INTO quote_lines_cache(
      doc_num, doc_date, item_code, item_desc,
      qty_quoted, qty_delivered, dollars_quoted, dollars_delivered, updated_at
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())
     ON CONFLICT (doc_num, item_code) DO UPDATE SET
      doc_date=EXCLUDED.doc_date,
      item_desc=EXCLUDED.item_desc,
      qty_quoted=EXCLUDED.qty_quoted,
      qty_delivered=EXCLUDED.qty_delivered,
      dollars_quoted=EXCLUDED.dollars_quoted,
      dollars_delivered=EXCLUDED.dollars_delivered,
      updated_at=NOW()`,
    [
      Number(r.docNum),
      String(r.docDate || "").slice(0, 10) || null,
      String(r.itemCode || ""),
      String(r.itemDesc || ""),
      Number(r.qtyQuoted || 0),
      Number(r.qtyDelivered || 0),
      Number(r.dollarsQuoted || 0),
      Number(r.dollarsDelivered || 0),
    ]
  );
}

async function readQuoteLinesFromDb(docNum) {
  const r = await dbQuery(
    `SELECT
      doc_num AS "docNum",
      doc_date AS "docDate",
      item_code AS "itemCode",
      item_desc AS "itemDesc",
      qty_quoted::float AS "qtyQuoted",
      qty_delivered::float AS "qtyDelivered",
      dollars_quoted::float AS "dollarsQuoted",
      dollars_delivered::float AS "dollarsDelivered"
     FROM quote_lines_cache
     WHERE doc_num=$1
     ORDER BY dollars_quoted DESC, item_code ASC`,
    [Number(docNum)]
  );
  return r.rows || [];
}

app.get("/api/admin/quotes/sync", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const mode = String(req.query?.mode || "days").toLowerCase(); // days|hours|minutes
    const nRaw = Number(req.query?.n || 5);

    /**
     * ✅ FIX 1 (CRÍTICO):
     * Antes: days máximo 10 => Ene→Hoy jamás funcionaba.
     * Ahora: days permite hasta 400 días.
     */
    const nMax = mode === "days" ? 400 : mode === "hours" ? 48 : 720;
    const n = Math.max(1, Math.min(nMax, Number.isFinite(nRaw) ? Math.trunc(nRaw) : 5));

    const maxDocsRaw = Number(req.query?.maxDocs || 500);
    const maxDocs = Math.max(20, Math.min(2000, Number.isFinite(maxDocsRaw) ? Math.trunc(maxDocsRaw) : 500));

    // ventana en ms (Panamá)
    const nowMs = nowInOffsetMs(TZ_OFFSET_MIN);
    const winMs =
      mode === "hours"
        ? n * 60 * 60 * 1000
        : mode === "minutes"
        ? n * 60 * 1000
        : n * 24 * 60 * 60 * 1000;

    const fromMs = nowMs - winMs;

    // para SL usamos DocDate por rango de días (buffer 1-2 días)
    const today = isoDateInOffset(TZ_OFFSET_MIN);
    const fromDate = mode === "days" ? addDaysISO(today, -n) : addDaysISO(today, -2);

    const toDate = today;
    const { from, toPlus1 } = toDocDateFilterRange(fromDate, toDate);

    const batchTop = 200;
    let skipSap = 0;
    let saved = 0;
    let scanned = 0;

    // trazado entregado total: limitar para que sync sea rápido
    const maxTrace = Math.min(200, maxDocs);

    // categorías: limitar
    const maxGroupCalc = Math.min(800, maxDocs);

    // ✅ NUEVO: detalle de líneas: limitar fuerte para evitar sync eterno
    // (Sube/baja según tu necesidad)
    const maxLinesCalc = Math.min(120, maxDocs);

    for (let page = 0; page < 60; page++) {
      if (saved >= maxDocs) break;

      const raw = await slFetch(
        `/Quotations?$select=DocEntry,DocNum,DocDate,DocTime,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
          `&$filter=${encodeURIComponent(`DocDate ge '${from}' and DocDate lt '${toPlus1}'`)}` +
          `&$orderby=DocDate desc,DocEntry desc&$top=${batchTop}&$skip=${skipSap}`,
        { timeoutMs: 20000 }
      );

      const rows = Array.isArray(raw?.value) ? raw.value : [];
      if (!rows.length) break;

      skipSap += rows.length;

      for (const q of rows) {
        scanned++;
        if (saved >= maxDocs) break;

        if (isCancelledLike(q)) continue;

        const docNum = Number(q.DocNum);
        const docDate = String(q.DocDate || "").slice(0, 10);
        const docTime = Number(q.DocTime || 0);

        // filtro fino por minutos/horas (si DocTime existe)
        if (mode !== "days") {
          const ms = docDateTimeToMs(docDate, docTime);
          if (ms && ms < fromMs) continue;
        }

        const usuario = parseUserFromComments(q.Comments || "") || "sin_user";
        const warehouse = parseWhFromComments(q.Comments || "") || "sin_wh";

        // entregado total
        let deliveredTotal = 0;
        if (saved < maxTrace) {
          try {
            const tr = await traceQuoteTotals(docNum, addDaysISO(today, -30), today);
            if (tr?.ok) deliveredTotal = Number(tr.totalEntregado || 0);
          } catch {}
          await sleep(20);
        }

        // group_name
        let groupName = null;
        if (saved < maxGroupCalc) {
          const g = await inferQuoteGroupNameByDocEntry(q.DocEntry);
          groupName = g ? g : null;
          await sleep(15);
        }

        await upsertQuoteCache({
          docNum,
          docEntry: q.DocEntry,
          docDate,
          docTime,
          cardCode: q.CardCode,
          cardName: q.CardName,
          usuario,
          warehouse,
          docTotal: Number(q.DocTotal || 0),
          deliveredTotal,
          status: q.DocumentStatus || "",
          cancelStatus: q.CancelStatus ?? "",
          comments: q.Comments || "",
          groupName,
        });

        // ✅ NUEVO: guardar líneas (DB cache) para que el modal NO llame SAP
        if (saved < maxLinesCalc) {
          try {
            // traer cotización completa para DocumentLines
            const qFull = await sapGetByDocEntry("Quotations", q.DocEntry);
            const qLines = Array.isArray(qFull?.DocumentLines) ? qFull.DocumentLines : [];

            // entregado por itemCode (map)
            const deliveredMap = await traceQuoteLinesByItem(docNum, addDaysISO(today, -45), today);

            // agrupar cotizado por itemCode
            const quotedMap = new Map(); // itemCode -> {qtyQuoted, dollarsQuoted, desc}
            for (const ln of qLines) {
              const itemCode = String(ln?.ItemCode || "").trim();
              if (!itemCode) continue;

              const qtyQ = Number(ln?.Quantity || 0);
              const dollarsQ = Number(ln?.LineTotal ?? 0);
              const desc = String(ln?.ItemDescription || ln?.ItemName || "").trim();

              const prev = quotedMap.get(itemCode) || { qtyQuoted: 0, dollarsQuoted: 0, desc: desc || "" };
              prev.qtyQuoted += Number.isFinite(qtyQ) ? qtyQ : 0;
              prev.dollarsQuoted += Number.isFinite(dollarsQ) ? dollarsQ : 0;
              if (!prev.desc && desc) prev.desc = desc;
              quotedMap.set(itemCode, prev);
            }

            for (const [itemCode, qv] of quotedMap.entries()) {
              const dv = deliveredMap.get(itemCode) || { qtyDelivered: 0, dollarsDelivered: 0 };
              await upsertQuoteLineCache({
                docNum,
                docDate,
                itemCode,
                itemDesc: qv.desc || "",
                qtyQuoted: qv.qtyQuoted,
                qtyDelivered: dv.qtyDelivered,
                dollarsQuoted: qv.dollarsQuoted,
                dollarsDelivered: dv.dollarsDelivered,
              });
            }

            await sleep(25);
          } catch {
            // no rompemos el sync
          }
        }

        saved++;
      }

      if (saved >= maxDocs) break;
    }

    const stamp = isoDateTimeInOffset(TZ_OFFSET_MIN);
    await setState("quotes_cache_last_sync", stamp);

    return safeJson(res, 200, {
      ok: true,
      mode,
      n,
      maxDocs,
      scanned,
      saved,
      lastSyncAt: stamp,
      window: { fromDate, toDate },
      note: "Sync guardado en Supabase. Incluye group_name y cache de líneas (limitado) para el modal.",
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================
   DASHBOARD DB
   GET /api/admin/quotes/dashboard-db?from=YYYY-MM-DD&to=YYYY-MM-DD&onlyCreated=1
========================= */
app.get("/api/admin/quotes/dashboard-db", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const fromQ = String(req.query?.from || "").slice(0, 10);
    const toQ = String(req.query?.to || "").slice(0, 10);
    const onlyCreated = String(req.query?.onlyCreated || "0") === "1";

    const today = isoDateInOffset(TZ_OFFSET_MIN);
    const from = /^\d{4}-\d{2}-\d{2}$/.test(fromQ) ? fromQ : addDaysISO(today, -30);
    const to = /^\d{4}-\d{2}-\d{2}$/.test(toQ) ? toQ : today;

    const createdJoin = onlyCreated
      ? `AND LOWER(COALESCE(q.usuario,'')) IN (SELECT LOWER(username) FROM app_users WHERE is_active=TRUE)`
      : ``;

    // Totales
    const totalsR = await dbQuery(
      `SELECT
        COUNT(*)::int AS quotes,
        COALESCE(SUM(q.doc_total),0)::float AS cotizado,
        COALESCE(SUM(q.delivered_total),0)::float AS entregado
       FROM quotes_cache q
       WHERE q.doc_date BETWEEN $1 AND $2
       ${createdJoin}`,
      [from, to]
    );
    const totals = totalsR.rows[0] || { quotes: 0, cotizado: 0, entregado: 0 };
    const fillRatePct = Number(totals.cotizado) > 0 ? (Number(totals.entregado) / Number(totals.cotizado)) * 100 : 0;

    // byUser
    const byUserR = await dbQuery(
      `SELECT
        COALESCE(NULLIF(q.usuario,''),'sin_user') AS usuario,
        COUNT(*)::int AS cnt,
        COALESCE(SUM(q.doc_total),0)::float AS cotizado,
        COALESCE(SUM(q.delivered_total),0)::float AS entregado
       FROM quotes_cache q
       WHERE q.doc_date BETWEEN $1 AND $2
       ${createdJoin}
       GROUP BY 1
       ORDER BY cotizado DESC
       LIMIT 2000`,
      [from, to]
    );

    // byWh
    const byWhR = await dbQuery(
      `SELECT
        COALESCE(NULLIF(q.warehouse,''),'sin_wh') AS warehouse,
        COUNT(*)::int AS cnt,
        COALESCE(SUM(q.doc_total),0)::float AS cotizado,
        COALESCE(SUM(q.delivered_total),0)::float AS entregado
       FROM quotes_cache q
       WHERE q.doc_date BETWEEN $1 AND $2
       ${createdJoin}
       GROUP BY 1
       ORDER BY cotizado DESC
       LIMIT 2000`,
      [from, to]
    );

    // byClient
    const byClientR = await dbQuery(
      `SELECT
        COALESCE(NULLIF(q.card_name,''), q.card_code, 'sin_cliente') AS customer,
        COUNT(*)::int AS cnt,
        COALESCE(SUM(q.doc_total),0)::float AS cotizado
       FROM quotes_cache q
       WHERE q.doc_date BETWEEN $1 AND $2
       ${createdJoin}
       GROUP BY 1
       ORDER BY cotizado DESC
       LIMIT 2000`,
      [from, to]
    );

    // byGroup
    const byGroupR = await dbQuery(
      `SELECT
        COALESCE(NULLIF(q.group_name,''), 'Sin grupo') AS "group",
        COUNT(*)::int AS cnt,
        COALESCE(SUM(q.doc_total),0)::float AS cotizado
       FROM quotes_cache q
       WHERE q.doc_date BETWEEN $1 AND $2
       ${createdJoin}
       GROUP BY 1
       ORDER BY cotizado DESC
       LIMIT 2000`,
      [from, to]
    );

    const lastSyncAt = await getState("quotes_cache_last_sync");

    return safeJson(res, 200, {
      ok: true,
      from,
      to,
      lastSyncAt: lastSyncAt || null,
      totals: {
        quotes: Number(totals.quotes || 0),
        cotizado: Number(totals.cotizado || 0),
        entregado: Number(totals.entregado || 0),
        fillRatePct: Number(fillRatePct.toFixed(2)),
      },
      byUser: (byUserR.rows || []).map((r) => ({
        usuario: r.usuario,
        cnt: Number(r.cnt || 0),
        cotizado: Number(r.cotizado || 0),
        entregado: Number(r.entregado || 0),
        fillRatePct:
          Number(r.cotizado || 0) > 0
            ? Number(((Number(r.entregado || 0) / Number(r.cotizado || 0)) * 100).toFixed(2))
            : 0,
      })),
      byWh: (byWhR.rows || []).map((r) => ({
        warehouse: r.warehouse,
        cnt: Number(r.cnt || 0),
        cotizado: Number(r.cotizado || 0),
        entregado: Number(r.entregado || 0),
        fillRatePct:
          Number(r.cotizado || 0) > 0
            ? Number(((Number(r.entregado || 0) / Number(r.cotizado || 0)) * 100).toFixed(2))
            : 0,
      })),
      byClient: (byClientR.rows || []).map((r) => ({
        customer: r.customer,
        cnt: Number(r.cnt || 0),
        cotizado: Number(r.cotizado || 0),
      })),
      byGroup: (byGroupR.rows || []).map((r) => ({
        group: r.group,
        cnt: Number(r.cnt || 0),
        cotizado: Number(r.cotizado || 0),
      })),
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================
   HISTÓRICO DB (PAGINADO)
   GET /api/admin/quotes/db?from&to&user&client&skip&limit&onlyCreated=1
========================= */
app.get("/api/admin/quotes/db", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const fromQ = String(req.query?.from || "").slice(0, 10);
    const toQ = String(req.query?.to || "").slice(0, 10);
    const user = String(req.query?.user || "").trim().toLowerCase();
    const client = String(req.query?.client || "").trim().toLowerCase();

    const skipRaw = Number(req.query?.skip || 0);
    const limitRaw = Number(req.query?.limit || 20);

    const skip = Math.max(0, Number.isFinite(skipRaw) ? Math.trunc(skipRaw) : 0);
    const limit = Math.max(1, Math.min(200, Number.isFinite(limitRaw) ? Math.trunc(limitRaw) : 20));

    const onlyCreated = String(req.query?.onlyCreated || "0") === "1";

    const today = isoDateInOffset(TZ_OFFSET_MIN);
    const from = /^\d{4}-\d{2}-\d{2}$/.test(fromQ) ? fromQ : addDaysISO(today, -30);
    const to = /^\d{4}-\d{2}-\d{2}$/.test(toQ) ? toQ : today;

    const where = [];
    const params = [];
    let p = 1;

    where.push(`q.doc_date BETWEEN $${p++} AND $${p++}`);
    params.push(from, to);

    if (onlyCreated) {
      where.push(`LOWER(COALESCE(q.usuario,'')) IN (SELECT LOWER(username) FROM app_users WHERE is_active=TRUE)`);
    }

    if (user) {
      where.push(`LOWER(COALESCE(q.usuario,'')) LIKE $${p++}`);
      params.push(`%${user}%`);
    }

    if (client) {
      where.push(`(LOWER(COALESCE(q.card_code,'')) LIKE $${p++} OR LOWER(COALESCE(q.card_name,'')) LIKE $${p++})`);
      params.push(`%${client}%`, `%${client}%`);
    }

    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

    const totalR = await dbQuery(`SELECT COUNT(*)::int AS total FROM quotes_cache q ${whereSql}`, params);
    const total = Number(totalR.rows?.[0]?.total || 0);

    const dataR = await dbQuery(
      `SELECT
        q.doc_num AS "docNum",
        q.doc_entry AS "docEntry",
        q.doc_date AS "fecha",
        q.card_code AS "cardCode",
        q.card_name AS "cardName",
        q.usuario AS "usuario",
        q.warehouse AS "warehouse",
        q.status AS "estado",
        q.cancel_status AS "cancelStatus",
        q.comments AS "comments",
        q.doc_total::float AS "montoCotizacion",
        q.delivered_total::float AS "montoEntregado"
       FROM quotes_cache q
       ${whereSql}
       ORDER BY q.doc_date DESC, q.doc_num DESC
       OFFSET $${p++} LIMIT $${p++}`,
      [...params, skip, limit]
    );

    const lastSyncAt = await getState("quotes_cache_last_sync");

    return safeJson(res, 200, {
      ok: true,
      from,
      to,
      total,
      skip,
      limit,
      lastSyncAt: lastSyncAt || null,
      quotes: dataR.rows || [],
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================
   ✅ QUOTE LINES (DETALLE POR DOCNUM) — DB ONLY
   GET /api/admin/quotes/lines?docNum=123
   Lee de Supabase (quote_lines_cache). No llama SAP.
========================= */
app.get("/api/admin/quotes/lines", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const docNum = Number(req.query?.docNum || 0);
    if (!Number.isFinite(docNum) || docNum <= 0) {
      return safeJson(res, 400, { ok: false, message: "docNum inválido" });
    }

    const lines = await readQuoteLinesFromDb(docNum);

    if (!lines.length) {
      return safeJson(res, 404, {
        ok: false,
        message:
          "No hay líneas en cache (DB). Ejecuta Sync (Ene→Hoy o últimos días) para poblar el detalle.",
      });
    }

    const docDate = lines[0]?.docDate || null;

    return safeJson(res, 200, {
      ok: true,
      docNum,
      docDate,
      lines,
      source: "db",
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
