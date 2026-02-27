import express from "express";
import jwt from "jsonwebtoken";

/* =========================================================
   ✅ ENV
========================================================= */
const {
  PORT = 3000,
  JWT_SECRET = "change_me",
  ADMIN_USER = "PRODIMA",
  ADMIN_PASS = "ADMINISTRADOR",

  SAP_BASE_URL = "",
  SAP_COMPANYDB = "",
  SAP_USER = "",
  SAP_PASS = "",

  // ⚠️ Render:
  // CORS_ORIGIN=https://prodima.com.pa,https://www.prodima.com.pa
  CORS_ORIGIN = "",
} = process.env;

const app = express();
app.use(express.json({ limit: "2mb" }));

/* =========================================================
   ✅ CORS ROBUSTO (igual a tu quotes)
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

/* =========================================================
   ✅ HEALTH
========================================================= */
app.get("/api/health", async (req, res) => {
  safeJson(res, 200, {
    ok: true,
    message: "✅ PRODIMA INVOICES API activa",
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
   ✅ SAP Service Layer
   - timeout corto por defecto (12s)
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
  const timeoutMs = Number(options.timeoutMs || 30000);
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
   ✅ Admin login (igual)
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
   ✅ INVOICES: scan rápido (solo headers)
========================================================= */
async function scanInvoices({ f, t, wantSkip, wantLimit, clientFilter }) {
  const toPlus1 = addDaysISO(t, 1);
  const batchTop = 200;

  let skipSap = 0;
  let totalFiltered = 0;
  const pageRows = [];

  const cFilter = String(clientFilter || "").trim().toLowerCase();
  const maxSapPages = 80;

  const seenDocEntry = new Set();

  for (let page = 0; page < maxSapPages; page++) {
    const raw = await slFetch(
      `/Invoices?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName` +
        `&$filter=${encodeURIComponent(`DocDate ge '${f}' and DocDate lt '${toPlus1}'`)}` +
        `&$orderby=DocDate desc,DocEntry desc&$top=${batchTop}&$skip=${skipSap}`,
      { timeoutMs: 12000 }
    );

    const rows = Array.isArray(raw?.value) ? raw.value : [];
    if (!rows.length) break;

    skipSap += rows.length;

    for (const inv of rows) {
      const de = Number(inv?.DocEntry);
      if (Number.isFinite(de)) {
        if (seenDocEntry.has(de)) continue;
        seenDocEntry.add(de);
      }

      const idx = totalFiltered++;
      if (idx >= wantSkip && pageRows.length < wantLimit) {
        pageRows.push({
          docEntry: inv.DocEntry,
          docNum: inv.DocNum,
          fecha: String(inv.DocDate || "").slice(0, 10),
          cardCode: inv.CardCode,
          cardName: inv.CardName,
          docTotal: Number(inv.DocTotal || 0),
        });
      }
    }

    if (pageRows.length >= wantLimit) break;
  }

  return { pageRows, totalFiltered };
}

/* =========================================================
   ✅ CACHE: líneas de facturas por DocEntry
========================================================= */
const INV_LINES_CACHE = new Map();
const INV_LINES_TTL_MS = 6 * 60 * 60 * 1000;

function invCacheGet(key) {
  const it = INV_LINES_CACHE.get(key);
  if (!it) return null;
  if (Date.now() - it.at > INV_LINES_TTL_MS) {
    INV_LINES_CACHE.delete(key);
    return null;
  }
  return it.data;
}
function invCacheSet(key, data) {
  INV_LINES_CACHE.set(key, { at: Date.now(), data });
}

async function getInvoiceLinesCached(docEntry) {
  const de = Number(docEntry);
  if (!Number.isFinite(de) || de <= 0) return { ok: false, lines: [] };

  const key = `INV:${de}`;
  const cached = invCacheGet(key);
  if (cached) return cached;

  // ✅ Aquí SÍ viene DocumentLines (sin $expand)
  const full = await slFetch(`/Invoices(${de})`, { timeoutMs: 18000 });
  const lines = Array.isArray(full?.DocumentLines) ? full.DocumentLines : [];

  const outLines = lines.map((ln) => ({
    WarehouseCode: String(ln?.WarehouseCode || "SIN_WH"),
    LineTotal: Number(ln?.LineTotal || 0),
  }));

  const out = { ok: true, lines: outLines };
  invCacheSet(key, out);
  return out;
}

/* =========================================================
   ✅ INVOICES LIST (headers)
   GET /api/admin/invoices?from=&to=&skip=0&limit=50&client=
========================================================= */
app.get("/api/admin/invoices", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const from = String(req.query?.from || "");
    const to = String(req.query?.to || "");

    const limitRaw = req.query?.limit != null ? Number(req.query.limit) : 50;
    const limit = Math.max(1, Math.min(200, Number.isFinite(limitRaw) ? limitRaw : 50));
    const skip = req.query?.skip != null ? Math.max(0, Number(req.query.skip) || 0) : 0;

    const clientFilter = String(req.query?.client || "");

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const defaultFrom = addDaysISO(today, -30);

    const f = isISO(from) ? from : defaultFrom;
    const t = isISO(to) ? to : today;

    const { pageRows, totalFiltered } = await scanInvoices({
      f,
      t,
      wantSkip: skip,
      wantLimit: limit,
      clientFilter,
    });

    return safeJson(res, 200, {
      ok: true,
      invoices: pageRows,
      from: f,
      to: t,
      limit,
      skip,
      total: totalFiltered,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ DASHBOARD FACTURAS: Cliente x Bodega (rápido + batch)
   GET /api/admin/invoices/dashboard?from=&to=&maxDocs=600
========================================================= */
app.get("/api/admin/invoices/dashboard", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const from = String(req.query?.from || "");
    const to = String(req.query?.to || "");

    const maxDocsRaw = Number(req.query?.maxDocs || 1500);
    const maxDocs = Math.max(100, Math.min(2000, Number.isFinite(maxDocsRaw) ? Math.trunc(maxDocsRaw) : 1500));

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const defaultFrom = addDaysISO(today, -30);

    const f = isISO(from) ? from : defaultFrom;
    const t = isISO(to) ? to : today;

    // 1) headers rápido
    const { pageRows } = await scanInvoices({
      f,
      t,
      wantSkip: 0,
      wantLimit: maxDocs,
      clientFilter: "",
    });

    const invoiceSet = new Set();
    let totalDocTotal = 0;

    const byWh = new Map();     // wh -> $ (por líneas)
    const byCust = new Map();   // customer -> $ (DocTotal)
    const byCustWh = new Map(); // key -> { dollars, invoices:Set }

    const CONC = 5;
    let idx = 0;

    async function worker() {
      while (idx < pageRows.length) {
        const i = idx++;
        const inv = pageRows[i];

        const cust = `${inv.cardCode} · ${inv.cardName}`.trim() || "SIN_CLIENTE";

        // ✅ "facturas totales" por cliente (DocTotal)
        byCust.set(cust, (byCust.get(cust) || 0) + Number(inv.docTotal || 0));

        totalDocTotal += Number(inv.docTotal || 0);
        invoiceSet.add(String(inv.docNum || inv.docEntry));

        // ✅ líneas para saber bodegas (WarehouseCode) SIN $expand
        try {
          const r = await getInvoiceLinesCached(inv.docEntry);
          if (!r.ok) continue;

          const whSeen = new Set();
          for (const ln of r.lines) {
            const wh = String(ln.WarehouseCode || "SIN_WH").trim() || "SIN_WH";
            const lt = Number(ln.LineTotal || 0);

            byWh.set(wh, (byWh.get(wh) || 0) + lt);

            const key = `${cust}||${wh}`;
            if (!byCustWh.has(key)) byCustWh.set(key, { dollars: 0, invoices: new Set() });
            const cur = byCustWh.get(key);
            cur.dollars += lt;

            if (!whSeen.has(wh)) {
              whSeen.add(wh);
              cur.invoices.add(String(inv.docNum || inv.docEntry));
            }
          }
        } catch {}

        await sleep(10);
      }
    }

    await Promise.all(Array.from({ length: CONC }, () => worker()));

    const topSort = (m) =>
      Array.from(m.entries())
        .map(([k, v]) => ({ key: k, dollars: Number(Number(v || 0).toFixed(2)) }))
        .sort((a, b) => b.dollars - a.dollars);

    const table = Array.from(byCustWh.entries())
      .map(([k, v]) => {
        const [customer, warehouse] = k.split("||");
        return {
          customer,
          warehouse,
          dollars: Number(Number(v.dollars || 0).toFixed(2)),
          invoices: v.invoices.size,
        };
      })
      .sort((a, b) => b.dollars - a.dollars);

    return safeJson(res, 200, {
      ok: true,
      from: f,
      to: t,
      totals: {
        invoices: invoiceSet.size,
        dollars: Number(totalDocTotal.toFixed(2)), // suma DocTotal (facturas totales)
      },
      topWarehouses: topSort(byWh).slice(0, 20),  // por líneas
      topCustomers: topSort(byCust).slice(0, 20), // por DocTotal
      table,
      meta: { maxDocsUsed: pageRows.length },
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

app.listen(Number(PORT), () => console.log(`Invoices server listening on :${PORT}`));
