import express from "express";
import pg from "pg";
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

  // Render: CORS_ORIGIN=https://prodima.com.pa,https://www.prodima.com.pa
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
   ✅ DB (Postgres) - solo si lo usas para app_users (scope)
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
      pin_hash TEXT NOT NULL DEFAULT '',
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
   ✅ SAP Service Layer (cookie + timeout)
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
   ✅ Scope: usuarios creados (opcional)
========================================================= */
let CREATED_USERS_CACHE = { ts: 0, set: new Set() };
const CREATED_USERS_TTL_MS = 5 * 60 * 1000;

async function getCreatedUsersSetCached() {
  if (!hasDb()) return new Set();
  const now = Date.now();
  if (CREATED_USERS_CACHE.ts && now - CREATED_USERS_CACHE.ts < CREATED_USERS_TTL_MS) return CREATED_USERS_CACHE.set;

  const r = await dbQuery(`SELECT username FROM app_users WHERE is_active=TRUE`);
  const set = new Set((r.rows || []).map((x) => String(x.username || "").trim().toLowerCase()).filter(Boolean));
  CREATED_USERS_CACHE = { ts: now, set };
  return set;
}

/* =========================================================
   ✅ QUOTES scan (rápido)
========================================================= */
async function scanQuotes({ f, t, wantSkip, wantLimit, userFilter, clientFilter, onlyCreated }) {
  const toPlus1 = addDaysISO(t, 1);
  const batchTop = 200;

  let skipSap = 0;
  let totalFiltered = 0;
  const pageRows = [];

  const uFilter = String(userFilter || "").trim().toLowerCase();
  const cFilter = String(clientFilter || "").trim().toLowerCase();

  const maxSapPages = 60;
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
          docEntry: q.DocEntry,        // Quote DocEntry
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

    if (pageRows.length >= wantLimit) break;
  }

  return { pageRows, totalFiltered };
}

/* =========================================================
   ✅ ENTREGADO OPTIMIZADO POR LOTE (SIN “TRACE” POR COTIZACIÓN)
   Calcula entregado para LAS COTIZACIONES DE LA PÁGINA (limit cap)
========================================================= */

// Cache en memoria para no recalcular mismo rango
const DELIV_BATCH_CACHE = new Map();
const DELIV_BATCH_TTL_MS = 10 * 60 * 1000; // 10 min

function batchCacheGet(key) {
  const it = DELIV_BATCH_CACHE.get(key);
  if (!it) return null;
  if (Date.now() - it.at > DELIV_BATCH_TTL_MS) {
    DELIV_BATCH_CACHE.delete(key);
    return null;
  }
  return it.data;
}
function batchCacheSet(key, data) {
  DELIV_BATCH_CACHE.set(key, { at: Date.now(), data });
}

async function computeDeliveredForQuotesPage(pageRows, f, t) {
  // pageRows: [{docEntry(quote), docNum, cardCode, montoCotizacion...}]
  const quotes = pageRows || [];
  if (!quotes.length) return new Map();

  // agrupamos por CardCode
  const byCard = new Map();
  for (const q of quotes) {
    const cc = String(q.cardCode || "").trim();
    if (!cc) continue;
    if (!byCard.has(cc)) byCard.set(cc, []);
    byCard.get(cc).push(q);
  }

  const deliveredByQuoteDocEntry = new Map(); // quoteDocEntry -> deliveredAmount

  // para cada cardCode: buscamos Orders y Deliveries del rango
  for (const [cardCode, qlist] of byCard.entries()) {
    const quoteDocEntrySet = new Set(qlist.map(q => Number(q.docEntry)).filter(n=>Number.isFinite(n)&&n>0));
    if (!quoteDocEntrySet.size) continue;

    const cacheKey = `cc:${cardCode}|${f}|${t}|q:${Array.from(quoteDocEntrySet).sort((a,b)=>a-b).join(",")}`;
    const cached = batchCacheGet(cacheKey);
    if (cached) {
      for (const [k,v] of cached.entries()) deliveredByQuoteDocEntry.set(k,v);
      continue;
    }

    // 1) Orders list (limitado)
    const toPlus1 = addDaysISO(t, 1);
    const safeCard = cardCode.replace(/'/g, "''");

    const ordersList = await slFetch(
      `/Orders?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode` +
        `&$filter=${encodeURIComponent(`CardCode eq '${safeCard}' and DocDate ge '${f}' and DocDate lt '${toPlus1}'`)}` +
        `&$orderby=DocDate desc,DocEntry desc&$top=120`,
      { timeoutMs: 15000 }
    );

    const ordersHead = Array.isArray(ordersList?.value) ? ordersList.value : [];

    // 2) Construir mapa orderDocEntry -> { quoteDocEntry -> lineTotalSum }
    const orderToQuoteWeights = new Map(); // orderDocEntry -> Map(quoteDocEntry->sumLineTotal)

    let checkedOrders = 0;
    for (const oh of ordersHead) {
      checkedOrders++;
      if (checkedOrders > 60) break; // cap por cardCode (estabilidad)

      const ode = Number(oh?.DocEntry);
      if (!Number.isFinite(ode) || ode <= 0) continue;

      // traemos solo líneas (rápido)
      let od;
      try {
        od = await slFetch(
          `/Orders(${ode})?$select=DocEntry&$expand=DocumentLines($select=BaseType,BaseEntry,LineTotal)`,
          { timeoutMs: 15000 }
        );
      } catch {
        continue;
      }

      const lines = Array.isArray(od?.DocumentLines) ? od.DocumentLines : [];
      if (!lines.length) continue;

      const w = new Map();
      for (const ln of lines) {
        // Order line base to quote?
        const bt = Number(ln?.BaseType);
        const be = Number(ln?.BaseEntry);
        if (bt === 23 && quoteDocEntrySet.has(be)) {
          const lt = Number(ln?.LineTotal || 0);
          w.set(be, (w.get(be) || 0) + (Number.isFinite(lt) ? lt : 0));
        }
      }
      if (w.size) orderToQuoteWeights.set(ode, w);

      await sleep(10);
      // si ya encontramos por lo menos 1 order para cada quote, podríamos parar antes
      // (opcional) aquí lo dejamos simple y seguro
    }

    if (!orderToQuoteWeights.size) {
      batchCacheSet(cacheKey, new Map());
      continue;
    }

    // 3) DeliveryNotes list
    const delList = await slFetch(
      `/DeliveryNotes?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode` +
        `&$filter=${encodeURIComponent(`CardCode eq '${safeCard}' and DocDate ge '${f}' and DocDate lt '${toPlus1}'`)}` +
        `&$orderby=DocDate desc,DocEntry desc&$top=180`,
      { timeoutMs: 15000 }
    );
    const delHead = Array.isArray(delList?.value) ? delList.value : [];

    // 4) Para cada Delivery: ver líneas BaseEntry(OrderDocEntry) y asignar DocTotal proporcionalmente
    const localDelivered = new Map(); // quoteDocEntry->sum delivered

    let checkedDel = 0;
    for (const dh of delHead) {
      checkedDel++;
      if (checkedDel > 90) break;

      const dde = Number(dh?.DocEntry);
      if (!Number.isFinite(dde) || dde <= 0) continue;

      let dd;
      try {
        dd = await slFetch(
          `/DeliveryNotes(${dde})?$select=DocEntry,DocTotal&$expand=DocumentLines($select=BaseType,BaseEntry,LineTotal)`,
          { timeoutMs: 15000 }
        );
      } catch {
        continue;
      }

      const docTotal = Number(dd?.DocTotal || 0);
      if (!Number.isFinite(docTotal) || docTotal <= 0) continue;

      const dLines = Array.isArray(dd?.DocumentLines) ? dd.DocumentLines : [];
      if (!dLines.length) continue;

      // Map quoteDocEntry -> deliveryLineTotal (según order->quote weights)
      const qWeight = new Map();
      let sumWeight = 0;

      for (const ln of dLines) {
        const bt = Number(ln?.BaseType);
        const beOrder = Number(ln?.BaseEntry);
        if (bt !== 17) continue; // delivery line base to order type=17
        if (!orderToQuoteWeights.has(beOrder)) continue;

        const orderWeights = orderToQuoteWeights.get(beOrder); // Map(quote->sumLineTotal in order)
        // si orderWeights tiene varios quotes, repartimos por LineTotal proporcional a order line totals (aprox)
        const lt = Number(ln?.LineTotal || 0);
        const base = Number.isFinite(lt) && lt > 0 ? lt : 0;

        // repartir base entre quotes según peso relativo del order
        const owSum = Array.from(orderWeights.values()).reduce((a, v) => a + Number(v || 0), 0);
        if (owSum <= 0) continue;

        for (const [qde, wv] of orderWeights.entries()) {
          const part = (base * Number(wv || 0)) / owSum;
          if (part > 0) {
            qWeight.set(qde, (qWeight.get(qde) || 0) + part);
          }
        }
      }

      sumWeight = Array.from(qWeight.values()).reduce((a, v) => a + Number(v || 0), 0);

      if (qWeight.size && sumWeight > 0) {
        for (const [qde, wv] of qWeight.entries()) {
          const alloc = (Number(wv || 0) / sumWeight) * docTotal;
          localDelivered.set(qde, (localDelivered.get(qde) || 0) + alloc);
        }
      }

      await sleep(10);
    }

    // guardar local en cache y merge global
    batchCacheSet(cacheKey, localDelivered);
    for (const [k, v] of localDelivered.entries()) {
      deliveredByQuoteDocEntry.set(k, (deliveredByQuoteDocEntry.get(k) || 0) + Number(v || 0));
    }
  }

  return deliveredByQuoteDocEntry;
}

/* =========================================================
   ✅ /api/admin/quotes
   - Normal: rápido
   - withDelivered=1: calcula entregado SOLO para esa página (cap)
========================================================= */
app.get("/api/admin/quotes", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const from = String(req.query?.from || "");
    const to = String(req.query?.to || "");

    const withDelivered = String(req.query?.withDelivered || "0") === "1";
    const onlyCreated = String(req.query?.onlyCreated || "0") === "1";

    const limitRaw =
      req.query?.limit != null
        ? Number(req.query.limit)
        : req.query?.top != null
        ? Number(req.query.top)
        : 20;

    let limit = Math.max(1, Math.min(200, Number.isFinite(limitRaw) ? limitRaw : 20));
    const skip = req.query?.skip != null ? Math.max(0, Number(req.query.skip) || 0) : 0;

    // ✅ cap duro en modo entregado para NO trabar
    if (withDelivered) limit = Math.min(limit, 60);

    const userFilter = String(req.query?.user || "");
    const clientFilter = String(req.query?.client || "");

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const defaultFrom = addDaysISO(today, -30);

    const f = /^\d{4}-\d{2}-\d{2}$/.test(from) ? from : defaultFrom;
    const tt = /^\d{4}-\d{2}-\d{2}$/.test(to) ? to : today;

    const { pageRows, totalFiltered } = await scanQuotes({
      f,
      t: tt,
      wantSkip: skip,
      wantLimit: limit,
      userFilter,
      clientFilter,
      onlyCreated,
    });

    if (withDelivered && pageRows.length) {
      // ✅ Calcula entregado por lote (optimizado)
      let mapDelivered;
      try {
        mapDelivered = await computeDeliveredForQuotesPage(pageRows, f, tt);
      } catch (e) {
        mapDelivered = new Map(); // no rompe
      }

      for (const q of pageRows) {
        const qde = Number(q.docEntry);
        const del = mapDelivered.get(qde);
        if (del != null && Number.isFinite(Number(del))) {
          const totalEntregado = Number(del);
          q.montoEntregado = Number(totalEntregado.toFixed(2));
          q.pendiente = Number((Number(q.montoCotizacion || 0) - q.montoEntregado).toFixed(2));
        }
      }
    }

    return safeJson(res, 200, {
      ok: true,
      quotes: pageRows,
      from: f,
      to: tt,
      limit,
      skip,
      total: totalFiltered,
      withDelivered,
      scope: { onlyCreated },
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
