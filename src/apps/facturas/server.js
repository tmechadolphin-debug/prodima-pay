import express from "express";
import cors from "cors";
import jwt from "jsonwebtoken";

/* =========================================================
   ✅ ENV
========================================================= */
const {
  PORT = 3000,
  CORS_ORIGIN = "*",
  JWT_SECRET = "change_me",

  ADMIN_USER = "PRODIMA",
  ADMIN_PASS = "ADMINISTRADOR",

  SAP_BASE_URL = "",
  SAP_COMPANYDB = "",
  SAP_USER = "",
  SAP_PASS = "",
} = process.env;

const app = express();
app.use(express.json({ limit: "2mb" }));

app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? true : CORS_ORIGIN,
    credentials: false,
  })
);

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
function ymd(d) {
  const dt = d instanceof Date ? d : new Date(d);
  const y = dt.getFullYear();
  const m = String(dt.getMonth() + 1).padStart(2, "0");
  const dd = String(dt.getDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}
function addDaysISO(iso, days) {
  const d = new Date(String(iso || "").slice(0, 10));
  if (Number.isNaN(d.getTime())) return "";
  d.setDate(d.getDate() + Number(days || 0));
  return ymd(d);
}
function isISO(s) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(s || ""));
}

/* =========================================================
   ✅ HEALTH
========================================================= */
app.get("/api/health", async (req, res) => {
  safeJson(res, 200, { ok: true, message: "✅ SALES API activa" });
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
  const timeout = setTimeout(() => controller.abort(), 45000);

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
    if (String(e?.name) === "AbortError") throw new Error("SAP timeout (45s) en slFetch");
    throw e;
  } finally {
    clearTimeout(timeout);
  }
}

/* =========================================================
   ✅ Core: leer facturas por rango (Invoices)
   - POS = ShipToCode
   - Bodega = WarehouseCode por línea
========================================================= */
async function fetchInvoicesLines({ from, to, top = 2000 }) {
  const f = isISO(from) ? from : addDaysISO(ymd(new Date()), -30);
  const t = isISO(to) ? to : ymd(new Date());
  const toPlus1 = addDaysISO(t, 1);

  // OJO: expand de líneas puede crecer. Para MVP, limitamos docs.
  // Si el rango es enorme, se recomienda cache / jobs / ETL.
  const q =
    `/Invoices?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,ShipToCode` +
    `&$filter=${encodeURIComponent(`DocDate ge '${f}' and DocDate lt '${toPlus1}'`)}` +
    `&$orderby=DocDate desc,DocEntry desc&$top=${Math.min(200, Math.max(1, top))}`;

  const head = await slFetch(q);
  const invoices = Array.isArray(head?.value) ? head.value : [];

  const out = [];
  for (const inv of invoices) {
    const de = Number(inv.DocEntry);
    if (!Number.isFinite(de)) continue;

    const full = await slFetch(
      `/Invoices(${de})?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,ShipToCode` +
      `&$expand=DocumentLines($select=ItemCode,ItemDescription,Quantity,LineTotal,WarehouseCode)`
    );

    const lines = Array.isArray(full?.DocumentLines) ? full.DocumentLines : [];
    for (const ln of lines) {
      out.push({
        docEntry: full.DocEntry,
        docNum: full.DocNum,
        docDate: String(full.DocDate || "").slice(0, 10),
        cardCode: String(full.CardCode || ""),
        cardName: String(full.CardName || ""),
        shipTo: String(full.ShipToCode || "SIN_POS"),
        itemCode: String(ln.ItemCode || ""),
        itemName: String(ln.ItemDescription || ""),
        qty: Number(ln.Quantity || 0),
        lineTotal: Number(ln.LineTotal || 0),
        wh: String(ln.WarehouseCode || "SIN_WH"),
      });
    }
  }

  return { from: f, to: t, rows: out };
}

function sumMap(map, key, val) {
  map.set(key, (Number(map.get(key) || 0) + Number(val || 0)));
}

/* =========================================================
   ✅ API: summary
========================================================= */
app.get("/api/admin/sales/summary", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const from = String(req.query?.from || "");
    const to = String(req.query?.to || "");
    const top = Number(req.query?.top || 120);

    const data = await fetchInvoicesLines({ from, to, top });

    const total = { dollars: 0, docs: new Set() };
    const byWh = new Map();
    const byCustomer = new Map();
    const byMonth = new Map();

    for (const r of data.rows) {
      total.dollars += r.lineTotal;
      total.docs.add(r.docNum);

      sumMap(byWh, r.wh, r.lineTotal);
      sumMap(byCustomer, `${r.cardCode} · ${r.cardName}`, r.lineTotal);

      const m = /^\d{4}-\d{2}-\d{2}$/.test(r.docDate) ? r.docDate.slice(0, 7) : "SIN_MES";
      sumMap(byMonth, m, r.lineTotal);
    }

    const toSorted = (m) =>
      Array.from(m.entries())
        .map(([k, v]) => ({ key: k, dollars: Number(v || 0) }))
        .sort((a, b) => b.dollars - a.dollars);

    return safeJson(res, 200, {
      ok: true,
      from: data.from,
      to: data.to,
      totals: { dollars: Number(total.dollars.toFixed(2)), invoices: total.docs.size },
      byWarehouse: toSorted(byWh).slice(0, 20),
      byCustomer: toSorted(byCustomer).slice(0, 20),
      byMonth: toSorted(byMonth).sort((a, b) => String(a.key).localeCompare(String(b.key))),
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ API: clientes por bodega
========================================================= */
app.get("/api/admin/sales/warehouse/:wh/customers", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const wh = String(req.params.wh || "").trim();
    const from = String(req.query?.from || "");
    const to = String(req.query?.to || "");
    const top = Number(req.query?.top || 120);

    const data = await fetchInvoicesLines({ from, to, top });

    const byCustomer = new Map();
    const docs = new Map();

    for (const r of data.rows) {
      if (r.wh !== wh) continue;
      const key = `${r.cardCode} · ${r.cardName}`;
      sumMap(byCustomer, key, r.lineTotal);

      const dk = key;
      if (!docs.has(dk)) docs.set(dk, new Set());
      docs.get(dk).add(r.docNum);
    }

    const out = Array.from(byCustomer.entries())
      .map(([k, v]) => ({ customer: k, dollars: Number(v.toFixed(2)), invoices: docs.get(k)?.size || 0 }))
      .sort((a, b) => b.dollars - a.dollars);

    return safeJson(res, 200, { ok: true, from: data.from, to: data.to, wh, customers: out });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ API: POS por cliente (ShipToCode)
========================================================= */
app.get("/api/admin/sales/customer/:cardCode/pos", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const cardCode = String(req.params.cardCode || "").trim();
    const wh = String(req.query?.wh || "").trim(); // opcional
    const from = String(req.query?.from || "");
    const to = String(req.query?.to || "");
    const top = Number(req.query?.top || 120);

    const data = await fetchInvoicesLines({ from, to, top });

    const byPos = new Map();
    const docs = new Map();

    for (const r of data.rows) {
      if (String(r.cardCode) !== cardCode) continue;
      if (wh && r.wh !== wh) continue;

      const pos = r.shipTo || "SIN_POS";
      sumMap(byPos, pos, r.lineTotal);

      const dk = pos;
      if (!docs.has(dk)) docs.set(dk, new Set());
      docs.get(dk).add(r.docNum);
    }

    const out = Array.from(byPos.entries())
      .map(([k, v]) => ({ pos: k, dollars: Number(v.toFixed(2)), invoices: docs.get(k)?.size || 0 }))
      .sort((a, b) => b.dollars - a.dollars);

    return safeJson(res, 200, { ok: true, from: data.from, to: data.to, cardCode, wh: wh || null, pos: out });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ API: SKU por POS (dólares + unidades)
========================================================= */
app.get("/api/admin/sales/pos/skus", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const from = String(req.query?.from || "");
    const to = String(req.query?.to || "");
    const wh = String(req.query?.wh || "").trim(); // opcional
    const cardCode = String(req.query?.cardCode || "").trim(); // opcional
    const shipTo = String(req.query?.shipTo || "").trim(); // requerido para drill down
    const top = Number(req.query?.top || 120);

    if (!shipTo) return safeJson(res, 400, { ok: false, message: "shipTo (POS) requerido" });

    const data = await fetchInvoicesLines({ from, to, top });

    const bySkuDol = new Map();
    const bySkuQty = new Map();
    const nameMap = new Map();

    for (const r of data.rows) {
      if (shipTo && r.shipTo !== shipTo) continue;
      if (wh && r.wh !== wh) continue;
      if (cardCode && r.cardCode !== cardCode) continue;

      const sku = r.itemCode || "SIN_SKU";
      sumMap(bySkuDol, sku, r.lineTotal);
      sumMap(bySkuQty, sku, r.qty);
      if (r.itemName) nameMap.set(sku, r.itemName);
    }

    const out = Array.from(bySkuDol.entries())
      .map(([sku, dollars]) => ({
        sku,
        itemName: nameMap.get(sku) || "",
        dollars: Number(Number(dollars || 0).toFixed(2)),
        units: Number(Number(bySkuQty.get(sku) || 0).toFixed(3)),
      }))
      .sort((a, b) => b.dollars - a.dollars);

    return safeJson(res, 200, {
      ok: true,
      from: data.from,
      to: data.to,
      filters: { wh: wh || null, cardCode: cardCode || null, shipTo },
      skus: out,
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

app.listen(Number(PORT), () => console.log(`Sales server listening on :${PORT}`));
