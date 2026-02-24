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
function isISO(s) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(s || ""));
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
function sumMap(map, key, val) {
  map.set(key, (Number(map.get(key) || 0) + Number(val || 0)));
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
   ✅ Fetch wrapper
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
    if (String(e?.name) === "AbortError") throw new Error("SAP timeout (45s)");
    throw e;
  } finally {
    clearTimeout(timeout);
  }
}

/* =========================================================
   ✅ Leer Invoices + Lines (paginado) en UNA sola llamada por página
========================================================= */
async function fetchInvoicesExpanded({ from, to, maxDocs = 2000 }) {
  const f = isISO(from) ? from : addDaysISO(ymd(new Date()), -30);
  const t = isISO(to) ? to : ymd(new Date());
  const toPlus1 = addDaysISO(t, 1);

  const pageTop = 200;
  let skip = 0;
  let docs = [];

  while (docs.length < maxDocs) {
    const q =
      `/Invoices?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName` +
      `&$filter=${encodeURIComponent(`DocDate ge '${f}' and DocDate lt '${toPlus1}'`)}` +
      `&$orderby=DocDate desc,DocEntry desc&$top=${pageTop}&$skip=${skip}` +
      `&$expand=DocumentLines($select=WarehouseCode,LineTotal)`;

    const r = await slFetch(q);
    const arr = Array.isArray(r?.value) ? r.value : [];
    if (!arr.length) break;

    docs = docs.concat(arr);
    skip += arr.length;

    if (arr.length < pageTop) break;
  }

  return { from: f, to: t, invoices: docs.slice(0, maxDocs) };
}

/* =========================================================
   ✅ ENDPOINT PRINCIPAL: Totales por Cliente y Bodega
   - Para la bodega usamos LineTotal por WarehouseCode (lo correcto)
   - Total de factura = DocTotal (KPI)
========================================================= */
app.get("/api/admin/sales/byCustomerWarehouse", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const from = String(req.query?.from || "");
    const to = String(req.query?.to || "");
    const maxDocsRaw = Number(req.query?.maxDocs || 2000);
    const maxDocs = Math.max(100, Math.min(8000, Number.isFinite(maxDocsRaw) ? Math.trunc(maxDocsRaw) : 2000));

    const data = await fetchInvoicesExpanded({ from, to, maxDocs });

    const invoiceSet = new Set();
    let totalInvoiceDollars = 0;

    // agregaciones
    const byWarehouse = new Map();          // wh -> $
    const byCustomer = new Map();           // customerLabel -> $
    const byCustWh = new Map();             // "cust||wh" -> { $ , invoices:Set }

    for (const inv of data.invoices) {
      const docNum = inv.DocNum;
      if (docNum != null) invoiceSet.add(String(docNum));
      totalInvoiceDollars += Number(inv.DocTotal || 0);

      const customerLabel = `${String(inv.CardCode || "")} · ${String(inv.CardName || "")}`.trim() || "SIN_CLIENTE";

      // sumar total por cliente usando DocTotal (visión “factura total”)
      sumMap(byCustomer, customerLabel, Number(inv.DocTotal || 0));

      // por bodega y por cliente+bodega usando líneas
      const lines = Array.isArray(inv.DocumentLines) ? inv.DocumentLines : [];
      const whSeenInThisInvoice = new Set();

      for (const ln of lines) {
        const wh = String(ln?.WarehouseCode || "SIN_WH").trim() || "SIN_WH";
        const lt = Number(ln?.LineTotal || 0);

        sumMap(byWarehouse, wh, lt);

        const key = `${customerLabel}||${wh}`;
        if (!byCustWh.has(key)) byCustWh.set(key, { dollars: 0, invoices: new Set() });
        const cur = byCustWh.get(key);
        cur.dollars += lt;

        // contar factura una vez por customer+bodega
        if (!whSeenInThisInvoice.has(wh)) {
          whSeenInThisInvoice.add(wh);
          cur.invoices.add(String(docNum || inv.DocEntry || ""));
        }
      }
    }

    const topSort = (m) =>
      Array.from(m.entries())
        .map(([k, v]) => ({ key: k, dollars: Number(Number(v || 0).toFixed(2)) }))
        .sort((a, b) => b.dollars - a.dollars);

    const table = Array.from(byCustWh.entries())
      .map(([k, v]) => {
        const [customer, wh] = k.split("||");
        return {
          customer,
          warehouse: wh,
          dollars: Number(Number(v.dollars || 0).toFixed(2)),
          invoices: v.invoices.size,
        };
      })
      .sort((a, b) => b.dollars - a.dollars);

    return safeJson(res, 200, {
      ok: true,
      from: data.from,
      to: data.to,
      totals: {
        invoices: invoiceSet.size,
        dollars: Number(totalInvoiceDollars.toFixed(2)), // DocTotal sum
      },
      topWarehouses: topSort(byWarehouse).slice(0, 20), // líneas por wh
      topCustomers: topSort(byCustomer).slice(0, 20),   // DocTotal por cliente
      table, // cliente x bodega
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
