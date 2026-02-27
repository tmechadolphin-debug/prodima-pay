// server.js
import express from "express";
import jwt from "jsonwebtoken";
import { createClient } from "@supabase/supabase-js";

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

  // Render:
  // CORS_ORIGIN=https://prodima.com.pa,https://www.prodima.com.pa
  CORS_ORIGIN = "",

  // Supabase
  SUPABASE_URL = "",
  SUPABASE_SERVICE_ROLE = "",
} = process.env;

/* =========================================================
   ✅ APP
========================================================= */
const app = express();
app.use(express.json({ limit: "2mb" }));

/* =========================================================
   ✅ Supabase client
========================================================= */
const sb =
  SUPABASE_URL && SUPABASE_SERVICE_ROLE
    ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
        auth: { persistSession: false },
      })
    : null;

function missingSupabaseEnv() {
  return !sb;
}

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
  // ✅ añade headers extra si usas x-warehouse u otros
  res.setHeader(
    "Access-Control-Allow-Headers",
    "Content-Type, Authorization, x-warehouse"
  );
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
const TZ_OFFSET_MIN = -300; // Panamá
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
function norm(s) {
  return String(s || "").trim().toLowerCase();
}

/* =========================================================
   ✅ HEALTH
========================================================= */
app.get("/api/health", async (req, res) => {
  safeJson(res, 200, {
    ok: true,
    message: "✅ PRODIMA INVOICES API activa",
    sap: missingSapEnv() ? "missing" : "ok",
    supabase: missingSupabaseEnv() ? "missing" : "ok",
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

/* =========================================================
   ✅ Admin login
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
   ✅ Supabase: helpers (upsert + read)
========================================================= */
async function sbUpsertHeaders(rows) {
  if (!rows?.length) return;
  const payload = rows.map((r) => ({
    doc_entry: Number(r.docEntry),
    doc_num: Number(r.docNum || 0),
    doc_date: String(r.fecha),
    card_code: r.cardCode || "",
    card_name: r.cardName || "",
    doc_total: Number(r.docTotal || 0),
    updated_at: new Date().toISOString(),
  }));

  const { error } = await sb.from("inv_headers").upsert(payload, { onConflict: "doc_entry" });
  if (error) throw new Error(error.message);
}

async function sbUpsertLines(docEntry, lines) {
  if (!lines?.length) return;
  const payload = lines.map((ln, i) => ({
    doc_entry: Number(docEntry),
    line_num: Number(ln.LineNum ?? i),
    whs_code: String(ln.WarehouseCode || "SIN_WH"),
    line_total: Number(ln.LineTotal || 0),
  }));

  const { error } = await sb.from("inv_lines").upsert(payload, { onConflict: "doc_entry,line_num" });
  if (error) throw new Error(error.message);
}

async function sbGetInvoices({ f, t, skip, limit, clientFilter }) {
  let q = sb
    .from("inv_headers")
    .select("doc_entry,doc_num,doc_date,card_code,card_name,doc_total", { count: "exact" })
    .gte("doc_date", f)
    .lte("doc_date", t)
    .order("doc_date", { ascending: false })
    .order("doc_entry", { ascending: false });

  const cf = String(clientFilter || "").trim();
  if (cf) {
    // filtra por CardCode o CardName
    q = q.or(`card_code.ilike.%${cf}%,card_name.ilike.%${cf}%`);
  }

  const from = skip;
  const to = skip + limit - 1;

  const { data, count, error } = await q.range(from, to);
  if (error) throw new Error(error.message);

  const invoices = (data || []).map((r) => ({
    docEntry: r.doc_entry,
    docNum: r.doc_num,
    fecha: String(r.doc_date),
    cardCode: r.card_code,
    cardName: r.card_name,
    docTotal: Number(r.doc_total || 0),
  }));

  return { invoices, total: Number(count || 0) };
}

async function sbDashboard({ f, t, maxDocs }) {
  const { data: headers, error: e1 } = await sb
    .from("inv_headers")
    .select("doc_entry,doc_num,doc_date,card_code,card_name,doc_total")
    .gte("doc_date", f)
    .lte("doc_date", t)
    .order("doc_date", { ascending: false })
    .order("doc_entry", { ascending: false })
    .limit(maxDocs);

  if (e1) throw new Error(e1.message);

  const docEntries = (headers || []).map((x) => x.doc_entry);

  const invoiceSet = new Set();
  let totalDocTotal = 0;

  const byCust = new Map();
  for (const inv of headers || []) {
    const cust = `${inv.card_code} · ${inv.card_name}`.trim() || "SIN_CLIENTE";
    byCust.set(cust, (byCust.get(cust) || 0) + Number(inv.doc_total || 0));
    totalDocTotal += Number(inv.doc_total || 0);
    invoiceSet.add(String(inv.doc_num || inv.doc_entry));
  }

  let lines = [];
  if (docEntries.length) {
    const { data: ln, error: e2 } = await sb
      .from("inv_lines")
      .select("doc_entry,line_num,whs_code,line_total")
      .in("doc_entry", docEntries);

    if (e2) throw new Error(e2.message);
    lines = ln || [];
  }

  const byWh = new Map();
  const byCustWh = new Map();

  const headerByDe = new Map((headers || []).map((h) => [h.doc_entry, h]));

  for (const ln of lines) {
    const h = headerByDe.get(ln.doc_entry);
    if (!h) continue;

    const cust = `${h.card_code} · ${h.card_name}`.trim() || "SIN_CLIENTE";
    const wh = String(ln.whs_code || "SIN_WH").trim() || "SIN_WH";
    const lt = Number(ln.line_total || 0);

    byWh.set(wh, (byWh.get(wh) || 0) + lt);

    const key = `${cust}||${wh}`;
    if (!byCustWh.has(key)) byCustWh.set(key, { dollars: 0, invoices: new Set() });

    const cur = byCustWh.get(key);
    cur.dollars += lt;
    cur.invoices.add(String(h.doc_num || h.doc_entry));
  }

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

  return {
    totals: { invoices: invoiceSet.size, dollars: Number(totalDocTotal.toFixed(2)) },
    topWarehouses: topSort(byWh).slice(0, 20),
    topCustomers: topSort(byCust).slice(0, 20),
    table,
    meta: { maxDocsUsed: (headers || []).length },
  };
}

/* =========================================================
   ✅ SAP -> Supabase sync por día (estable)
========================================================= */
async function syncSapToSupabaseByDay({ f, t, includeLines = true }) {
  if (!sb) throw new Error("Missing Supabase env");
  if (missingSapEnv()) throw new Error("Missing SAP env");

  let cur = f;
  while (cur <= t) {
    const next = addDaysISO(cur, 1);

    // headers del día
    const raw = await slFetch(
      `/Invoices?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName` +
        `&$filter=${encodeURIComponent(`DocDate ge '${cur}' and DocDate lt '${next}'`)}` +
        `&$orderby=DocEntry desc&$top=200&$skip=0`,
      { timeoutMs: 25000 } // ✅ más alto SOLO para sync
    );

    const rows = Array.isArray(raw?.value) ? raw.value : [];
    const headers = rows.map((inv) => ({
      docEntry: inv.DocEntry,
      docNum: inv.DocNum,
      fecha: String(inv.DocDate || "").slice(0, 10),
      cardCode: inv.CardCode,
      cardName: inv.CardName,
      docTotal: Number(inv.DocTotal || 0),
    }));

    await sbUpsertHeaders(headers);

    if (includeLines && headers.length) {
      // líneas (para bodegas)
      const CONC = 3;
      let idx = 0;

      async function worker() {
        while (idx < headers.length) {
          const i = idx++;
          const h = headers[i];
          try {
            const full = await slFetch(`/Invoices(${Number(h.docEntry)})`, { timeoutMs: 25000 });
            const lines = Array.isArray(full?.DocumentLines) ? full.DocumentLines : [];
            const compact = lines.map((ln) => ({
              LineNum: ln.LineNum,
              WarehouseCode: ln.WarehouseCode,
              LineTotal: ln.LineTotal,
            }));
            await sbUpsertLines(h.docEntry, compact);
          } catch {
            // si una factura falla, seguimos
          }
          await sleep(10);
        }
      }

      await Promise.all(Array.from({ length: CONC }, () => worker()));
    }

    cur = next;
    await sleep(50);
  }

  // opcional: guardar estado
  await sb
    .from("inv_sync_state")
    .upsert(
      { id: 1, synced_from: f, synced_to: t, last_run_at: new Date().toISOString() },
      { onConflict: "id" }
    );
}

/* =========================================================
   ✅ ENDPOINTS
========================================================= */

/**
 * GET /api/admin/invoices?from=&to=&skip=0&limit=50&client=
 * ✅ Lee Supabase primero
 * ✅ Si no hay data en Supabase para ese rango: hace sync y vuelve a leer
 */
app.get("/api/admin/invoices", verifyAdmin, async (req, res) => {
  try {
    if (missingSupabaseEnv()) return safeJson(res, 500, { ok: false, message: "Faltan variables Supabase" });

    const from = String(req.query?.from || "");
    const to = String(req.query?.to || "");

    const limitRaw = req.query?.limit != null ? Number(req.query.limit) : 50;
    const limit = Math.max(1, Math.min(200, Number.isFinite(limitRaw) ? limitRaw : 50));
    const skip = req.query?.skip != null ? Math.max(0, Number(req.query.skip) || 0) : 0;

    const clientFilter = String(req.query?.client || "");

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const defaultFrom = addDaysISO(today, -30);

    // ✅ si el user manda uno, deben venir ambos
    const hasAnyDate = !!String(from).trim() || !!String(to).trim();
    let f = defaultFrom,
      t = today;

    if (hasAnyDate) {
      if (!isISO(from) || !isISO(to)) {
        return safeJson(res, 400, { ok: false, message: "Debes seleccionar DESDE y HASTA (YYYY-MM-DD) antes de cargar." });
      }
      f = from;
      t = to;
    }

    // 1) Supabase primero
    let r = await sbGetInvoices({ f, t, skip, limit, clientFilter });

    // 2) si no hay data y SAP está configurado, sync y reintenta
    if (r.total === 0 && !missingSapEnv()) {
      await syncSapToSupabaseByDay({ f, t, includeLines: false }); // headers solamente para listado
      r = await sbGetInvoices({ f, t, skip, limit, clientFilter });
    }

    return safeJson(res, 200, {
      ok: true,
      invoices: r.invoices,
      from: f,
      to: t,
      limit,
      skip,
      total: r.total,
      source: "supabase",
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/**
 * GET /api/admin/invoices/dashboard?from=&to=&maxDocs=600
 * ✅ Lee Supabase primero
 * ✅ Si no hay data: sync (headers+lines) y reintenta
 */
app.get("/api/admin/invoices/dashboard", verifyAdmin, async (req, res) => {
  try {
    if (missingSupabaseEnv()) return safeJson(res, 500, { ok: false, message: "Faltan variables Supabase" });

    const from = String(req.query?.from || "");
    const to = String(req.query?.to || "");

    const maxDocsRaw = Number(req.query?.maxDocs || 1500);
    const maxDocs = Math.max(100, Math.min(2000, Number.isFinite(maxDocsRaw) ? Math.trunc(maxDocsRaw) : 1500));

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const defaultFrom = addDaysISO(today, -30);

    const hasAnyDate = !!String(from).trim() || !!String(to).trim();
    let f = defaultFrom,
      t = today;

    if (hasAnyDate) {
      if (!isISO(from) || !isISO(to)) {
        return safeJson(res, 400, { ok: false, message: "Debes seleccionar DESDE y HASTA (YYYY-MM-DD) antes de cargar el dashboard." });
      }
      f = from;
      t = to;
    }

    // 1) Supabase dashboard
    let dash = await sbDashboard({ f, t, maxDocs });

    // 2) si está vacío y SAP existe, sync completo (headers + líneas) y reintenta
    if (dash?.totals?.invoices === 0 && !missingSapEnv()) {
      await syncSapToSupabaseByDay({ f, t, includeLines: true });
      dash = await sbDashboard({ f, t, maxDocs });
    }

    return safeJson(res, 200, {
      ok: true,
      from: f,
      to: t,
      ...dash,
      source: "supabase",
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/**
 * POST /api/admin/invoices/sync
 * body: { from:"YYYY-MM-DD", to:"YYYY-MM-DD", includeLines:true/false }
 * ✅ Forzar carga SAP -> Supabase
 */
app.post("/api/admin/invoices/sync", verifyAdmin, async (req, res) => {
  try {
    if (missingSupabaseEnv()) return safeJson(res, 500, { ok: false, message: "Faltan variables Supabase" });
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const from = String(req.body?.from || "").trim();
    const to = String(req.body?.to || "").trim();
    const includeLines = req.body?.includeLines !== false; // default true

    if (!isISO(from) || !isISO(to)) {
      return safeJson(res, 400, { ok: false, message: "from/to inválidos. Usa YYYY-MM-DD." });
    }

    await syncSapToSupabaseByDay({ f: from, t: to, includeLines });

    return safeJson(res, 200, { ok: true, message: "Sync completado", from, to, includeLines });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

// ✅ POST /api/admin/item-groups/force
// Body: { itemCodes: ["PL0242C", "PM0060C", ...] }
app.post("/api/admin/item-groups/force", verifyAdmin, async (req, res) => {
  try {
    if (!sb) return safeJson(res, 500, { ok: false, message: "Faltan variables Supabase" });

    const itemCodes = Array.isArray(req.body?.itemCodes) ? req.body.itemCodes : [];
    const clean = Array.from(
      new Set(itemCodes.map(x => String(x || "").trim()).filter(Boolean))
    );

    if (!clean.length) {
      return safeJson(res, 400, { ok: false, message: "Envía itemCodes: [] con al menos 1 código." });
    }

    // ✅ Ajusta aquí si tu columna de grupo en sales se llama "grupo" (o algo distinto):
    const SALES_GROUP_COL = "item_group"; // <-- cámbiala a "grupo" si aplica
    const SALES_DATE_COL  = "doc_date";   // <-- cámbiala si tu fecha se llama distinto

    // 1) Traer grupos desde sales (más reciente primero)
    const { data: rows, error } = await sb
      .from("sales")
      .select(`item_code,${SALES_GROUP_COL},${SALES_DATE_COL}`)
      .in("item_code", clean)
      .not(SALES_GROUP_COL, "is", null)
      .order(SALES_DATE_COL, { ascending: false });

    if (error) throw new Error(error.message);

    // 2) Elegir el grupo "más reciente" por item_code
    const best = new Map();
    for (const r of (rows || [])) {
      const code = String(r.item_code || "").trim();
      const grp = String(r[SALES_GROUP_COL] || "").trim();
      if (!code || !grp) continue;
      if (!best.has(code)) best.set(code, grp); // primera vez = más reciente
    }

    const upPayload = Array.from(best.entries()).map(([code, grp]) => ({
      item_code: code,
      group_name: grp,
      updated_at: new Date().toISOString(),
    }));

    if (!upPayload.length) {
      return safeJson(res, 200, {
        ok: true,
        requested: clean.length,
        updated: 0,
        message: "No encontré grupo en sales para esos códigos (o la columna de grupo no coincide).",
      });
    }

    // 3) Upsert al cache
    const { error: e2 } = await sb
      .from("item_group_cache")
      .upsert(upPayload, { onConflict: "item_code" });

    if (e2) throw new Error(e2.message);

    const missingInSales = clean.filter(c => !best.has(c));

    return safeJson(res, 200, {
      ok: true,
      requested: clean.length,
      updated: upPayload.length,
      missingInSalesCount: missingInSales.length,
      missingInSales: missingInSales.slice(0, 100),
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
