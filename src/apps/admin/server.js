/**
 * PRODIMA ADMIN API (OPTIMIZADO)
 * - /api/admin/quotes: paginado real con page/limit y crawler SAP por páginas
 * - /api/admin/dashboard: KPIs rápidos + entregado con "cap" (no tumba el server)
 * - scope=created: filtra solo cotizaciones de usuarios creados en app_users
 *
 * Requisitos:
 * - Node 18+ (Render usa Node 22 ok)
 * - Postgres con tabla app_users (id, username, full_name, pin_hash, is_active, province, warehouse_code, created_at)
 */

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
   ✅ CORS
========================================================= */
app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? true : CORS_ORIGIN.split(",").map((s) => s.trim()),
    credentials: true,
    methods: ["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  })
);

/* =========================================================
   ✅ DB
========================================================= */
const pool =
  DATABASE_URL && String(DATABASE_URL).trim()
    ? new Pool({ connectionString: DATABASE_URL, ssl: { rejectUnauthorized: false } })
    : null;

async function dbQuery(text, params = []) {
  if (!pool) throw new Error("DB apagada (DATABASE_URL vacío)");
  return pool.query(text, params);
}

/* =========================================================
   ✅ Utils
========================================================= */
function safeJson(res, code, obj) {
  return res.status(code).json(obj);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function nowMs() {
  return Date.now();
}

function pickInt(v, def, min, max) {
  const n = Number(v);
  if (!Number.isFinite(n)) return def;
  return Math.max(min, Math.min(max, Math.trunc(n)));
}

function toYmd(d) {
  try {
    const dt = new Date(d);
    if (Number.isNaN(dt.getTime())) return "";
    const y = dt.getFullYear();
    const m = String(dt.getMonth() + 1).padStart(2, "0");
    const dd = String(dt.getDate()).padStart(2, "0");
    return `${y}-${m}-${dd}`;
  } catch {
    return "";
  }
}

function parseUserFromComments(comments) {
  const m = String(comments || "").match(/\[user:([^\]]+)\]/i);
  return m ? String(m[1]).trim() : "";
}

function parseWhFromComments(comments) {
  const m = String(comments || "").match(/\[wh:([^\]]+)\]/i);
  return m ? String(m[1]).trim() : "";
}

/* =========================================================
   ✅ Auth (Admin)
========================================================= */
function signAdminToken() {
  return jwt.sign({ role: "admin" }, JWT_SECRET, { expiresIn: "30d" });
}

function verifyAdmin(req, res, next) {
  try {
    const h = String(req.headers.authorization || "");
    const m = h.match(/^Bearer\s+(.+)$/i);
    if (!m) return safeJson(res, 401, { ok: false, message: "Missing Bearer token" });

    const token = m[1];
    const decoded = jwt.verify(token, JWT_SECRET);
    if (!decoded || decoded.role !== "admin") return safeJson(res, 401, { ok: false, message: "Invalid token" });

    req.admin = decoded;
    next();
  } catch {
    return safeJson(res, 401, { ok: false, message: "Invalid token" });
  }
}

/* =========================================================
   ✅ SAP Service Layer helper (B1)
   - Login cachea cookies por un rato
========================================================= */
let sapSession = {
  cookies: "",
  exp: 0,
};

function missingSapEnv() {
  return !SAP_BASE_URL || !SAP_COMPANYDB || !SAP_USER || !SAP_PASS;
}

async function sapLogin() {
  if (sapSession.cookies && sapSession.exp > nowMs()) return sapSession.cookies;

  const url = `${SAP_BASE_URL.replace(/\/+$/, "")}/Login`;
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
  if (!r.ok) throw new Error(`SAP Login error ${r.status}: ${txt.slice(0, 200)}`);

  const setCookie = r.headers.get("set-cookie") || "";
  // Render/Node: a veces viene varios set-cookie en una sola string
  const cookies = setCookie
    .split(",")
    .map((x) => x.split(";")[0].trim())
    .filter((x) => x.includes("="))
    .join("; ");

  sapSession.cookies = cookies;
  sapSession.exp = nowMs() + 1000 * 60 * 25; // 25 min
  return cookies;
}

async function slFetch(path, { method = "GET", body = null } = {}) {
  if (missingSapEnv()) throw new Error("Faltan variables SAP");

  const base = SAP_BASE_URL.replace(/\/+$/, "");
  const url = path.startsWith("http") ? path : `${base}${path.startsWith("/") ? "" : "/"}${path}`;

  const cookies = await sapLogin();

  const r = await fetch(url, {
    method,
    headers: {
      "Content-Type": "application/json",
      Cookie: cookies,
    },
    body: body ? JSON.stringify(body) : null,
  });

  // si sesión expira, reintenta una vez
  if (r.status === 401 || r.status === 403) {
    sapSession = { cookies: "", exp: 0 };
    const cookies2 = await sapLogin();
    const r2 = await fetch(url, {
      method,
      headers: { "Content-Type": "application/json", Cookie: cookies2 },
      body: body ? JSON.stringify(body) : null,
    });

    const data2 = await r2.json().catch(async () => ({ _raw: await r2.text() }));
    if (!r2.ok) throw new Error(`SAP error ${r2.status}: ${JSON.stringify(data2).slice(0, 280)}`);
    return data2;
  }

  const data = await r.json().catch(async () => ({ _raw: await r.text() }));
  if (!r.ok) throw new Error(`SAP error ${r.status}: ${JSON.stringify(data).slice(0, 280)}`);
  return data;
}

/* =========================================================
   ✅ Cache (in-memory)
========================================================= */
const CACHE = new Map();
function cacheGet(key) {
  const it = CACHE.get(key);
  if (!it) return null;
  if (it.exp && it.exp < nowMs()) {
    CACHE.delete(key);
    return null;
  }
  return it.val;
}
function cacheSet(key, val, ttlMs = 1000 * 60 * 5) {
  CACHE.set(key, { val, exp: nowMs() + ttlMs });
}

/* =========================================================
   ✅ USERS (app_users)
========================================================= */
async function getCreatedUsernamesSet() {
  const key = "created_users_set";
  const cached = cacheGet(key);
  if (cached) return cached;

  if (!pool) {
    const empty = new Set();
    cacheSet(key, empty, 1000 * 30);
    return empty;
  }

  const r = await dbQuery(`SELECT username FROM app_users WHERE username IS NOT NULL`);
  const set = new Set(r.rows.map((x) => String(x.username || "").trim().toLowerCase()).filter(Boolean));
  cacheSet(key, set, 1000 * 60 * 2); // 2 min
  return set;
}

/* =========================================================
   ✅ TRACE: Quote -> Orders -> Deliveries
   - Cache por docNum para no recalcular
========================================================= */
async function traceQuote(quoteDocNum) {
  const cacheKey = `TRACE:${quoteDocNum}`;
  const cached = cacheGet(cacheKey);
  if (cached) return cached;

  // 1) Encontrar Orders referenciando a esa Quote
  // Nota: hay varias formas; aquí usamos DocumentLines.BaseEntry/BaseType si está disponible.
  // Si tu SAP no soporta este query, lo ajustamos luego, pero esta es la base.
  const orders = await slFetch(
    `/Orders?$select=DocEntry,DocNum,DocTotal,DocDate&$filter=DocumentLines/any(d:d/BaseType eq 23 and d/BaseRef eq '${quoteDocNum}')`
  ).then((x) => x.value || []);

  // 2) Por cada Order, buscar Deliveries que lo referencien
  let deliveries = [];
  for (const o of orders) {
    const oDocNum = o.DocNum;
    const dels = await slFetch(
      `/DeliveryNotes?$select=DocEntry,DocNum,DocTotal,DocDate&$filter=DocumentLines/any(d:d/BaseType eq 17 and d/BaseRef eq '${oDocNum}')`
    ).then((x) => x.value || []);
    deliveries = deliveries.concat(dels);
    await sleep(40);
  }

  const totalEntregado = deliveries.reduce((a, d) => a + Number(d?.DocTotal || 0), 0);

  const out = {
    ok: true,
    quoteDocNum,
    orders,
    deliveries,
    totals: { totalEntregado: Number(totalEntregado.toFixed(2)) },
  };

  cacheSet(cacheKey, out, 1000 * 60 * 15); // 15 min
  return out;
}

/* =========================================================
   ✅ QUOTES crawler (SAP Quotations)
   - Escanea páginas de SAP hasta llenar la página requerida (page/limit)
   - Soporta filtros: user/client, from/to opcionales
   - scope=created filtra por app_users
========================================================= */
async function scanQuotesPaged({
  from,
  to,
  page,
  limit,
  maxPages,
  userFilter,
  clientFilter,
  scope,
  withDelivered,
}) {
  const wantSkip = (page - 1) * limit;
  const wantLimit = limit;

  const uFilter = String(userFilter || "").trim().toLowerCase();
  const cFilter = String(clientFilter || "").trim().toLowerCase();

  const createdSet = scope === "created" ? await getCreatedUsernamesSet() : null;

  // ODATA: si mandas from/to, filtramos DocDate
  const filters = [];
  if (from) filters.push(`DocDate ge '${from}'`);
  if (to) filters.push(`DocDate le '${to}'`);

  const baseFilter = filters.length ? `&$filter=${encodeURIComponent(filters.join(" and "))}` : "";

  const pageRows = [];
  let totalMatched = 0;
  let scannedPages = 0;

  const TOP = 50; // recomendado para SAP SL
  for (let p = 0; p < maxPages; p++) {
    const skip = p * TOP;
    scannedPages++;

    const data = await slFetch(
      `/Quotations?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
        `&$orderby=DocDate desc, DocNum desc&$top=${TOP}&$skip=${skip}${baseFilter}`
    );

    const rows = data?.value || [];
    if (!rows.length) break;

    for (const q of rows) {
      // user/wh viene en Comments [user:xxx][wh:300]
      const comments = q.Comments || "";
      const usuario = parseUserFromComments(comments);
      const wh = parseWhFromComments(comments);

      // filtros
      if (uFilter) {
        const u = String(usuario || "").toLowerCase();
        if (!u.includes(uFilter)) continue;
      }
      if (cFilter) {
        const cc = String(q.CardCode || "").toLowerCase();
        const cn = String(q.CardName || "").toLowerCase();
        if (!cc.includes(cFilter) && !cn.includes(cFilter)) continue;
      }
      if (scope === "created") {
        const u = String(usuario || "").trim().toLowerCase();
        if (!u || !createdSet.has(u)) continue;
      }

      const idx = totalMatched;
      totalMatched++;

      // solo guardamos lo necesario para la página actual
      if (idx >= wantSkip && pageRows.length < wantLimit) {
        pageRows.push({
          docEntry: q.DocEntry,
          docNum: q.DocNum,
          cardCode: q.CardCode,
          cardName: q.CardName,
          fecha: String(q.DocDate || "").slice(0, 10),
          estado: q.DocumentStatus || "",
          cancelStatus: q.CancelStatus ?? "",
          comments: comments,
          usuario: usuario || "",
          warehouse: wh || "",
          montoCotizacion: Number(q.DocTotal || 0),
          montoEntregado: 0,
          pendiente: Number(q.DocTotal || 0),
        });
      }

      if (pageRows.length >= wantLimit) break;
    }

    if (pageRows.length >= wantLimit) break;
  }

  // ✅ Entregado SOLO para lo que vas a retornar (no para todo el universo)
  if (withDelivered && pageRows.length) {
    const CONC = 3; // concurrencia moderada
    let idx = 0;

    async function worker() {
      while (idx < pageRows.length) {
        const i = idx++;
        const r = pageRows[i];
        try {
          const tr = await traceQuote(r.docNum);
          if (tr?.ok) {
            const ent = Number(tr?.totals?.totalEntregado || 0);
            r.montoEntregado = ent;
            r.pendiente = Number((Number(r.montoCotizacion || 0) - ent).toFixed(2));
          }
        } catch {
          // no rompe
        }
        await sleep(25);
      }
    }

    await Promise.all(Array.from({ length: CONC }, () => worker()));
  }

  // hasMore: si todavía no llegamos al final del universo
  const hasMore = totalMatched > wantSkip + pageRows.length ? true : scannedPages >= maxPages ? false : true;

  return {
    ok: true,
    page,
    limit,
    maxPages,
    totalMatched, // total match encontrado durante scan (parcial; sirve como guía)
    hasMore,
    quotes: pageRows,
  };
}

/* =========================================================
   ✅ DASHBOARD
   - KPIs rápidos: escanea hasta needCount cotizaciones (scope + filtros básicos)
   - Entregado: opcional y limitado por deliveredCap para evitar 502/hangs
========================================================= */
async function buildDashboard({
  from,
  to,
  scope,
  maxPages,
  needCount,
  withDelivered,
  deliveredCap,
}) {
  const createdSet = scope === "created" ? await getCreatedUsernamesSet() : null;

  const filters = [];
  if (from) filters.push(`DocDate ge '${from}'`);
  if (to) filters.push(`DocDate le '${to}'`);
  const baseFilter = filters.length ? `&$filter=${encodeURIComponent(filters.join(" and "))}` : "";

  const TOP = 50;

  // agregaciones
  let count = 0;
  let totalCot = 0;
  let totalEnt = 0;

  // para breakdowns rápidos
  const byUserAmt = new Map();
  const byUserCnt = new Map();
  const byWhAmt = new Map();
  const byWhCnt = new Map();
  const byClientAmt = new Map();
  const byMonthAmt = new Map();
  const byMonthCnt = new Map();

  // guardamos docNum de una muestra para entregado (cap)
  const sampleForDelivered = [];

  function add(map, k, v) {
    map.set(k, Number(map.get(k) || 0) + Number(v || 0));
  }

  for (let p = 0; p < maxPages; p++) {
    const skip = p * TOP;

    const data = await slFetch(
      `/Quotations?$select=DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
        `&$orderby=DocDate desc, DocNum desc&$top=${TOP}&$skip=${skip}${baseFilter}`
    );

    const rows = data?.value || [];
    if (!rows.length) break;

    for (const q of rows) {
      const comments = q.Comments || "";
      const usuario = parseUserFromComments(comments);
      const wh = parseWhFromComments(comments);

      if (scope === "created") {
        const u = String(usuario || "").trim().toLowerCase();
        if (!u || !createdSet.has(u)) continue;
      }

      // Dashboard: omitimos canceladas (tal como tu UI dice)
      const cancel = String(q.CancelStatus ?? "").toLowerCase();
      const isCancelled =
        cancel.includes("csyes") ||
        String(q.DocumentStatus || "").toLowerCase().includes("cancel") ||
        String(comments || "").toLowerCase().includes("cancelad");

      if (isCancelled) continue;

      const docTotal = Number(q.DocTotal || 0);

      count += 1;
      totalCot += docTotal;

      const uKey = usuario || "sin_user";
      const wKey = wh || "sin_wh";
      const cKey = (q.CardName || q.CardCode || "sin_cliente").trim();

      add(byUserAmt, uKey, docTotal);
      add(byUserCnt, uKey, 1);
      add(byWhAmt, wKey, docTotal);
      add(byWhCnt, wKey, 1);
      add(byClientAmt, cKey, docTotal);

      const fecha = String(q.DocDate || "").slice(0, 10);
      const mes = /^\d{4}-\d{2}-\d{2}$/.test(fecha) ? fecha.slice(0, 7) : "sin_mes";
      add(byMonthAmt, mes, docTotal);
      add(byMonthCnt, mes, 1);

      if (withDelivered && sampleForDelivered.length < deliveredCap) {
        sampleForDelivered.push({ docNum: q.DocNum, cot: docTotal });
      }

      if (count >= needCount) break;
    }

    if (count >= needCount) break;
  }

  let deliveredPartial = false;

  if (withDelivered && sampleForDelivered.length) {
    // Entregado limitado (cap)
    deliveredPartial = count > sampleForDelivered.length;

    const CONC = 3;
    let idx = 0;

    async function worker() {
      while (idx < sampleForDelivered.length) {
        const i = idx++;
        const it = sampleForDelivered[i];
        try {
          const tr = await traceQuote(it.docNum);
          if (tr?.ok) totalEnt += Number(tr?.totals?.totalEntregado || 0);
        } catch {}
        await sleep(25);
      }
    }

    await Promise.all(Array.from({ length: CONC }, () => worker()));
  }

  const fill = totalCot > 0 ? (totalEnt / totalCot) * 100 : 0;

  function topMap(map, n = 12) {
    return Array.from(map.entries())
      .map(([k, v]) => ({ key: k, value: Number(v || 0) }))
      .sort((a, b) => b.value - a.value)
      .slice(0, n);
  }

  function topMapCount(mapAmt, mapCnt, n = 12) {
    const cnt = new Map(mapCnt);
    return Array.from(mapAmt.entries())
      .map(([k, v]) => ({ key: k, value: Number(v || 0), count: Number(cnt.get(k) || 0) }))
      .sort((a, b) => b.value - a.value)
      .slice(0, n);
  }

  const months = Array.from(byMonthAmt.entries())
    .map(([k, v]) => ({ month: k, amount: Number(v || 0), count: Number(byMonthCnt.get(k) || 0) }))
    .sort((a, b) => String(a.month).localeCompare(String(b.month)));

  return {
    ok: true,
    kpis: {
      totalCotizaciones: count,
      montoTotalCotizado: Number(totalCot.toFixed(2)),
      montoTotalEntregado: Number(totalEnt.toFixed(2)),
      fillRatePct: Number(fill.toFixed(1)),
    },
    deliveredPartial,
    tops: {
      users: topMapCount(byUserAmt, byUserCnt, 12),
      warehouses: topMapCount(byWhAmt, byWhCnt, 12),
      clients: topMap(byClientAmt, 12),
    },
    months,
  };
}

/* =========================================================
   ✅ ROUTES
========================================================= */
app.get("/api/health", async (req, res) => {
  let db = "off";
  try {
    if (pool) {
      await dbQuery("select 1 as ok");
      db = "on";
    }
  } catch {
    db = "off";
  }
  return res.json({
    ok: true,
    app: "admin-api",
    message: "✅ PRODIMA ADMIN API activa",
    db,
    priceList: SAP_PRICE_LIST,
    warehouse_default: SAP_WAREHOUSE,
  });
});

app.post("/api/admin/login", async (req, res) => {
  try {
    const user = String(req.body?.user || "");
    const pass = String(req.body?.pass || "");
    if (user !== ADMIN_USER || pass !== ADMIN_PASS)
      return safeJson(res, 401, { ok: false, message: "Credenciales inválidas" });

    const token = signAdminToken();
    return res.json({ ok: true, token });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: "Login error" });
  }
});

/* --------------------------
   USERS CRUD (app_users)
-------------------------- */
app.get("/api/admin/users", verifyAdmin, async (req, res) => {
  try {
    if (!pool) return res.json({ ok: true, users: [] });

    const r = await dbQuery(
      `SELECT id, username, full_name, is_active, province, warehouse_code, created_at
       FROM app_users
       ORDER BY id DESC`
    );
    return res.json({ ok: true, users: r.rows });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

function provinceToWarehouse(province) {
  const p = String(province || "").trim().toLowerCase();
  if (p === "chiriquí" || p === "chiriqui" || p === "bocas del toro") return "200";
  if (p === "veraguas" || p === "coclé" || p === "cocle" || p === "los santos" || p === "herrera") return "500";
  if (p === "panamá" || p === "panama" || p === "panamá oeste" || p === "panama oeste" || p === "colón" || p === "colon") return "300";
  if (p === "rci") return "01";
  return String(SAP_WAREHOUSE || "300");
}

app.post("/api/admin/users", verifyAdmin, async (req, res) => {
  try {
    if (!pool) return safeJson(res, 400, { ok: false, message: "DB apagada" });

    const username = String(req.body?.username || "").trim().toLowerCase();
    const fullName = String(req.body?.fullName || req.body?.full_name || "").trim();
    const pin = String(req.body?.pin || "").trim();
    const province = String(req.body?.province || "").trim();
    if (!username) return safeJson(res, 400, { ok: false, message: "Username requerido" });
    if (!pin || pin.length < 4) return safeJson(res, 400, { ok: false, message: "PIN mínimo 4" });

    const wh = provinceToWarehouse(province);
    const pinHash = await bcrypt.hash(pin, 10);

    const r = await dbQuery(
      `INSERT INTO app_users (username, full_name, pin_hash, is_active, province, warehouse_code)
       VALUES ($1,$2,$3,true,$4,$5)
       RETURNING id, username, full_name, is_active, province, warehouse_code, created_at`,
      [username, fullName, pinHash, province, wh]
    );

    CACHE.delete("created_users_set"); // refrescar scope
    return res.json({ ok: true, user: r.rows[0] });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.patch("/api/admin/users/:id/toggle", verifyAdmin, async (req, res) => {
  try {
    if (!pool) return safeJson(res, 400, { ok: false, message: "DB apagada" });

    const id = Number(req.params.id);
    const r0 = await dbQuery(`SELECT is_active FROM app_users WHERE id=$1`, [id]);
    if (!r0.rows.length) return safeJson(res, 404, { ok: false, message: "No existe" });

    const next = !r0.rows[0].is_active;
    const r = await dbQuery(
      `UPDATE app_users SET is_active=$2 WHERE id=$1
       RETURNING id, username, full_name, is_active, province, warehouse_code, created_at`,
      [id, next]
    );

    CACHE.delete("created_users_set");
    return res.json({ ok: true, user: r.rows[0] });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.patch("/api/admin/users/:id/pin", verifyAdmin, async (req, res) => {
  try {
    if (!pool) return safeJson(res, 400, { ok: false, message: "DB apagada" });

    const id = Number(req.params.id);
    const pin = String(req.body?.pin || "").trim();
    if (!pin || pin.length < 4) return safeJson(res, 400, { ok: false, message: "PIN mínimo 4" });

    const pinHash = await bcrypt.hash(pin, 10);
    await dbQuery(`UPDATE app_users SET pin_hash=$2 WHERE id=$1`, [id, pinHash]);

    return res.json({ ok: true });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.delete("/api/admin/users/:id", verifyAdmin, async (req, res) => {
  try {
    if (!pool) return safeJson(res, 400, { ok: false, message: "DB apagada" });

    const id = Number(req.params.id);
    await dbQuery(`DELETE FROM app_users WHERE id=$1`, [id]);

    CACHE.delete("created_users_set");
    return res.json({ ok: true });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

/* --------------------------
   QUOTES (Histórico)
   GET /api/admin/quotes?page=1&limit=50&maxPages=100&scope=created|all&withDelivered=1
-------------------------- */
app.get("/api/admin/quotes", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv())
      return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const limit = Math.min(Math.max(Number(req.query?.limit || 100), 1), 1000);
    const page = Math.max(1, Number(req.query?.page || 1));
    const skip = (page - 1) * limit;

    const maxPages = Math.min(Number(req.query?.maxPages || 100), 200);
    const batchTop = 200;

    const userFilter = String(req.query?.user || "").trim().toLowerCase();
    const clientFilter = String(req.query?.client || "").trim().toLowerCase();

    let skipSap = 0;
    let totalFiltered = 0;
    const allRows = [];
    const seenDocEntry = new Set();

    for (let i = 0; i < maxPages; i++) {

      const raw = await slFetch(
        `/Quotations?$select=DocEntry,DocNum,CardCode,CardName,DocTotal,DocDate,DocumentStatus,CancelStatus,Comments` +
        `&$orderby=DocDate desc,DocEntry desc` +
        `&$top=${batchTop}&$skip=${skipSap}`
      );

      const rows = Array.isArray(raw?.value) ? raw.value : [];
      if (!rows.length) break;

      skipSap += rows.length;

      for (const q of rows) {

        const de = Number(q.DocEntry);
        if (seenDocEntry.has(de)) continue;
        seenDocEntry.add(de);

        if (isCancelledLike(q)) continue;

        const usuario = parseUserFromComments(q.Comments || "") || "sin_user";
        const wh = parseWhFromComments(q.Comments || "") || "sin_wh";

        if (userFilter && !usuario.toLowerCase().includes(userFilter)) continue;

        if (clientFilter) {
          const cc = String(q.CardCode || "").toLowerCase();
          const cn = String(q.CardName || "").toLowerCase();
          if (!cc.includes(clientFilter) && !cn.includes(clientFilter)) continue;
        }

        totalFiltered++;

        allRows.push({
          docEntry: q.DocEntry,
          docNum: q.DocNum,
          cardCode: q.CardCode,
          cardName: q.CardName,
          montoCotizacion: Number(q.DocTotal || 0),
          fecha: String(q.DocDate || "").slice(0, 10),
          estado: q.DocumentStatus || "",
          comments: q.Comments || "",
          usuario,
          warehouse: wh,
        });
      }

      if (rows.length < batchTop) break;
    }

    const paginated = allRows.slice(skip, skip + limit);

    return safeJson(res, 200, {
      ok: true,
      mode: "crawler",
      maxPages,
      total: totalFiltered,
      page,
      pageCount: Math.ceil(totalFiltered / limit),
      quotes: paginated,
    });

  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});
/* --------------------------
   DASHBOARD
   GET /api/admin/dashboard?needCount=800&maxPages=100&scope=created|all&withDelivered=1&deliveredCap=200
-------------------------- */
app.get("/api/admin/dashboard", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const from = String(req.query?.from || "").slice(0, 10);
    const to = String(req.query?.to || "").slice(0, 10);

    const maxPages = pickInt(req.query?.maxPages, 100, 1, 500);
    const needCount = pickInt(req.query?.needCount, 800, 50, 5000);

    const withDelivered = String(req.query?.withDelivered || "0") === "1";
    const deliveredCap = pickInt(req.query?.deliveredCap, 150, 0, 600);

    const scopeRaw = String(req.query?.scope || "all").toLowerCase();
    const scope = scopeRaw === "created" ? "created" : "all";

    const out = await buildDashboard({
      from: from || "",
      to: to || "",
      scope,
      maxPages,
      needCount,
      withDelivered,
      deliveredCap,
    });

    return res.json(out);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

/* =========================================================
   ✅ START
========================================================= */
app.listen(PORT, () => {
  console.log(`✅ PRODIMA admin-api listening on :${PORT}`);
});
