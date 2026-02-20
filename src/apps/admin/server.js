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
   ✅ CORS (importante: Authorization)
========================================================= */
app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? true : CORS_ORIGIN,
    credentials: false,
    methods: ["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization", "X-Requested-With"],
  })
);
app.options("*", cors());

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

/* =========================================================
   ✅ Time helpers (Panamá UTC-5)
========================================================= */
const TZ_OFFSET_MIN = -300;

function getDateISOInOffset(offsetMin = 0) {
  const now = new Date();
  const ms = now.getTime() + now.getTimezoneOffset() * 60_000 + Number(offsetMin) * 60_000;
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
   ✅ Parse helpers
========================================================= */
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
    app: "admin-api",
    message: "✅ PRODIMA ADMIN API activa",
    db: hasDb() ? "on" : "off",
    priceList: SAP_PRICE_LIST,
    warehouse_default: SAP_WAREHOUSE,
  });
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

/* =========================================================
   ✅ SAP Service Layer (cookie)
========================================================= */
let SL_COOKIE = "";
let SL_COOKIE_AT = 0;

async function slLogin() {
  const url = `${SAP_BASE_URL.replace(/\/$/, "")}/Login`;
  const body = { CompanyDB: SAP_COMPANYDB, UserName: SAP_USER, Password: SAP_PASS };

  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  const txt = await r.text();
  let data = {};
  try { data = JSON.parse(txt); } catch {}

  if (!r.ok) {
    throw new Error(`SAP login failed: HTTP ${r.status} ${data?.error?.message?.value || txt}`);
  }

  const setCookie = r.headers.get("set-cookie") || "";
  const cookies = [];
  for (const part of setCookie.split(",")) {
    const s = part.trim();
    if (s.startsWith("B1SESSION=") || s.startsWith("ROUTEID=")) cookies.push(s.split(";")[0]);
  }
  SL_COOKIE = cookies.join("; ");
  SL_COOKIE_AT = Date.now();
  return true;
}

async function slFetch(path, options = {}) {
  if (missingSapEnv()) throw new Error("Missing SAP env");

  if (!SL_COOKIE || Date.now() - SL_COOKIE_AT > 25 * 60 * 1000) {
    await slLogin();
  }

  const base = SAP_BASE_URL.replace(/\/$/, "");
  const url = `${base}${path.startsWith("/") ? path : `/${path}`}`;

  const method = String(options.method || "GET").toUpperCase();

  const r = await fetch(url, {
    method,
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
}

/* =========================================================
   ✅ Scope: users creados
========================================================= */
async function getCreatedUsersSet() {
  if (!hasDb()) return new Set();
  const r = await dbQuery(`SELECT username FROM app_users WHERE is_active=TRUE`);
  return new Set(
    (r.rows || []).map((x) => String(x.username || "").trim().toLowerCase()).filter(Boolean)
  );
}

/* =========================================================
   ✅ TRACE OPTIMIZADO (2 llamadas grandes con $expand)
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

async function sapGetFirstByDocNum(entity, docNum, select) {
  const n = Number(docNum);
  if (!Number.isFinite(n) || n <= 0) throw new Error("DocNum inválido");
  const parts = [];
  if (select) parts.push(`$select=${encodeURIComponent(select)}`);
  parts.push(`$filter=${encodeURIComponent(`DocNum eq ${n}`)}`);
  parts.push(`$top=1`);
  const path = `/${entity}?${parts.join("&")}`;
  const r = await slFetch(path);
  const arr = Array.isArray(r?.value) ? r.value : [];
  return arr[0] || null;
}

async function traceQuoteFast(quoteDocNum, fromOverride, toOverride) {
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

  const quoteDocEntry = Number(quoteHead.DocEntry);
  const cardCode = String(quoteHead.CardCode || "").trim();
  const quoteDate = String(quoteHead.DocDate || "").slice(0, 10);

  // 🔥 rango para trace (más ancho pero no infinito)
  const from =
    /^\d{4}-\d{2}-\d{2}$/.test(String(fromOverride || ""))
      ? String(fromOverride)
      : addDaysISO(quoteDate, -90);

  const to =
    /^\d{4}-\d{2}-\d{2}$/.test(String(toOverride || ""))
      ? String(toOverride)
      : addDaysISO(quoteDate, 120);

  const toPlus1 = addDaysISO(to, 1);

  // 1) Orders con DocumentLines expandidas (1 llamada)
  const ordersResp = await slFetch(
    `/Orders?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode` +
      `&$expand=DocumentLines($select=BaseType,BaseEntry)` +
      `&$filter=${encodeURIComponent(
        `CardCode eq '${cardCode.replace(/'/g, "''")}' and DocDate ge '${from}' and DocDate lt '${toPlus1}'`
      )}` +
      `&$orderby=DocDate desc,DocEntry desc&$top=200`
  );
  const orders = Array.isArray(ordersResp?.value) ? ordersResp.value : [];

  const linkedOrderDocEntries = new Set();
  for (const o of orders) {
    const lines = Array.isArray(o?.DocumentLines) ? o.DocumentLines : [];
    const linked = lines.some(
      (l) => Number(l?.BaseType) === 23 && Number(l?.BaseEntry) === quoteDocEntry
    );
    if (linked) linkedOrderDocEntries.add(Number(o.DocEntry));
  }

  // Si no hay orders linkeadas, no hay entregas
  const totalCotizado = Number(quoteHead.DocTotal || 0);
  if (linkedOrderDocEntries.size === 0) {
    const out = {
      ok: true,
      totals: { totalCotizado, totalEntregado: 0, pendiente: Number(totalCotizado.toFixed(2)) },
    };
    cacheSet(cacheKey, out);
    return out;
  }

  // 2) DeliveryNotes con DocumentLines expandidas (1 llamada)
  const delResp = await slFetch(
    `/DeliveryNotes?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode` +
      `&$expand=DocumentLines($select=BaseType,BaseEntry)` +
      `&$filter=${encodeURIComponent(
        `CardCode eq '${cardCode.replace(/'/g, "''")}' and DocDate ge '${from}' and DocDate lt '${toPlus1}'`
      )}` +
      `&$orderby=DocDate desc,DocEntry desc&$top=300`
  );
  const dels = Array.isArray(delResp?.value) ? delResp.value : [];

  let totalEntregado = 0;
  for (const d of dels) {
    const lines = Array.isArray(d?.DocumentLines) ? d.DocumentLines : [];
    const linked = lines.some(
      (l) => Number(l?.BaseType) === 17 && linkedOrderDocEntries.has(Number(l?.BaseEntry))
    );
    if (linked) totalEntregado += Number(d?.DocTotal || 0);
  }

  const pendiente = Number((totalCotizado - totalEntregado).toFixed(2));
  const out = { ok: true, totals: { totalCotizado, totalEntregado, pendiente } };
  cacheSet(cacheKey, out);
  return out;
}

/* =========================================================
   ✅ CRAWLER OPTIMIZADO:
   - deja de leer SAP cuando ya tiene lo necesario
========================================================= */
async function crawlQuotesOptimized({
  from,
  to,
  maxPages = 100,
  sapPageSize = 200,
  scope = "all", // all | created
  userFilter = "",
  clientFilter = "",
  needCount = 200, // ✅ cuántas filas necesitamos juntar para responder
  hardCap = 5000,  // ✅ por seguridad, no acumular infinito
}) {
  const toPlus1 = addDaysISO(to, 1);

  const uFilter = String(userFilter || "").trim().toLowerCase();
  const cFilter = String(clientFilter || "").trim().toLowerCase();

  const createdSet = scope === "created" ? await getCreatedUsersSet() : null;

  let skipSap = 0;
  const seenDocEntry = new Set();
  const out = [];
  let excluded = 0;

  for (let page = 0; page < maxPages; page++) {
    const raw = await slFetch(
      `/Quotations?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
        `&$filter=${encodeURIComponent(`DocDate ge '${from}' and DocDate lt '${toPlus1}'`)}` +
        `&$orderby=DocDate desc,DocEntry desc&$top=${sapPageSize}&$skip=${skipSap}`
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

      if (isCancelledLike(q)) {
        excluded++;
        continue;
      }

      const usuario = parseUserFromComments(q.Comments || "") || "sin_user";
      const wh = parseWhFromComments(q.Comments || "") || "sin_wh";

      if (createdSet) {
        const u = String(usuario || "").trim().toLowerCase();
        if (!u || u === "sin_user" || !createdSet.has(u)) {
          excluded++;
          continue;
        }
      }

      if (uFilter && !String(usuario).toLowerCase().includes(uFilter)) {
        excluded++;
        continue;
      }

      if (cFilter) {
        const cc = String(q.CardCode || "").toLowerCase();
        const cn = String(q.CardName || "").toLowerCase();
        if (!cc.includes(cFilter) && !cn.includes(cFilter)) {
          excluded++;
          continue;
        }
      }

      out.push({
        docEntry: q.DocEntry,
        docNum: q.DocNum,
        cardCode: String(q.CardCode || "").trim(),
        cardName: String(q.CardName || "").trim(),
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

      if (out.length >= needCount) break;
      if (out.length >= hardCap) break;
    }

    if (out.length >= needCount) break;
    if (out.length >= hardCap) break;

    await sleep(10);
  }

  return { quotes: out, excluded, createdUsersCount: createdSet ? createdSet.size : null };
}

/* =========================================================
   ✅ /api/admin/quotes (Histórico)
   - rápido: solo junta lo necesario para la página solicitada
   - entregado: solo para la página visible
========================================================= */
app.get("/api/admin/quotes", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const withDelivered = String(req.query?.withDelivered || "0") === "1";

    // Nota: no obligo rango, pero pongo defaults razonables.
    // Si tu UI manda vacío, igual trae bastante.
    const from =
      /^\d{4}-\d{2}-\d{2}$/.test(String(req.query?.from || ""))
        ? String(req.query.from)
        : "2016-01-01";

    const to =
      /^\d{4}-\d{2}-\d{2}$/.test(String(req.query?.to || ""))
        ? String(req.query.to)
        : getDateISOInOffset(TZ_OFFSET_MIN);

    const page = Math.max(1, Number(req.query?.page || 1));
    const limit = Math.min(Math.max(Number(req.query?.limit || 50), 1), 200);

    const maxPages = Math.min(Math.max(Number(req.query?.maxPages || 100), 1), 250);
    const scope = String(req.query?.scope || "all").toLowerCase() === "created" ? "created" : "all";
    const userFilter = String(req.query?.user || "").trim();
    const clientFilter = String(req.query?.client || "").trim();

    // ✅ SOLO juntamos lo que necesitamos para esa página + buffer
    const needCount = page * limit + 50;

    const { quotes, excluded, createdUsersCount } = await crawlQuotesOptimized({
      from,
      to,
      maxPages,
      sapPageSize: 200,
      scope,
      userFilter,
      clientFilter,
      needCount,
      hardCap: 8000,
    });

    const totalKnown = quotes.length; // total “conocido” (hasta donde leímos)
    const start = (page - 1) * limit;
    const slice = quotes.slice(start, start + limit);

    // ✅ Entregado solo para la página (rápido)
    if (withDelivered && slice.length) {
      const CONC = 3;
      let idx = 0;

      async function worker() {
        while (idx < slice.length) {
          const i = idx++;
          const q = slice[i];
          try {
            const tr = await traceQuoteFast(q.docNum, from, to);
            if (tr.ok) {
              q.montoEntregado = Number(tr.totals?.totalEntregado || 0);
              q.pendiente = Number(tr.totals?.pendiente || 0);
            }
          } catch {}
          await sleep(10);
        }
      }

      await Promise.all(Array.from({ length: CONC }, worker));
    }

    return safeJson(res, 200, {
      ok: true,
      mode: "crawler_pro",
      scope,
      from,
      to,
      maxPages,
      withDelivered,
      page,
      limit,
      totalKnown,
      excluded,
      createdUsersCount,
      quotes: slice,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ /api/admin/dashboard
   - rápido: usa una muestra suficientemente grande
   - entregado: por defecto solo calcula para N cotizaciones
========================================================= */
app.get("/api/admin/dashboard", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const withDelivered = String(req.query?.withDelivered || "0") === "1";

    const from =
      /^\d{4}-\d{2}-\d{2}$/.test(String(req.query?.from || ""))
        ? String(req.query.from)
        : "2016-01-01";

    const to =
      /^\d{4}-\d{2}-\d{2}$/.test(String(req.query?.to || ""))
        ? String(req.query.to)
        : getDateISOInOffset(TZ_OFFSET_MIN);

    const maxPages = Math.min(Math.max(Number(req.query?.maxPages || 100), 1), 250);
    const scope = String(req.query?.scope || "all").toLowerCase() === "created" ? "created" : "all";

    const userFilter = String(req.query?.user || "").trim();
    const clientFilter = String(req.query?.client || "").trim();

    // ✅ Dashboard no necesita 10,000 filas para renderizar: tomamos una muestra grande
    // Puedes subir needCount si quieres (ej: 2000) pero 800 es buen balance
    const needCount = Math.min(Math.max(Number(req.query?.needCount || 800), 200), 3000);

    const { quotes, excluded, createdUsersCount } = await crawlQuotesOptimized({
      from,
      to,
      maxPages,
      sapPageSize: 200,
      scope,
      userFilter,
      clientFilter,
      needCount,
      hardCap: 12000,
    });

    const totalCotizado = quotes.reduce((a, q) => a + Number(q.montoCotizacion || 0), 0);

    let totalEntregado = 0;

    // ✅ PRO: limitar entregado para que no se muera
    const deliveredCap = Math.min(Math.max(Number(req.query?.deliveredCap || 150), 0), 1000);

    let deliveredComputed = 0;
    let deliveredPartial = false;

    if (withDelivered && quotes.length && deliveredCap > 0) {
      const work = quotes.slice(0, Math.min(deliveredCap, quotes.length));
      deliveredPartial = work.length < quotes.length;

      const CONC = 3;
      let idx = 0;

      async function worker() {
        while (idx < work.length) {
          const i = idx++;
          const q = work[i];
          try {
            const tr = await traceQuoteFast(q.docNum, from, to);
            if (tr.ok) totalEntregado += Number(tr.totals?.totalEntregado || 0);
          } catch {}
          deliveredComputed++;
          await sleep(10);
        }
      }

      await Promise.all(Array.from({ length: CONC }, worker));
    }

    const pendiente = Number((Number(totalCotizado) - Number(totalEntregado)).toFixed(2));
    const fillRate = totalCotizado > 0 ? Number(((totalEntregado / totalCotizado) * 100).toFixed(2)) : 0;

    return safeJson(res, 200, {
      ok: true,
      mode: "dashboard_pro",
      scope,
      from,
      to,
      maxPages,
      needCount,
      withDelivered,
      deliveredCap,
      deliveredComputed,
      deliveredPartial,
      excluded,
      createdUsersCount,
      kpis: {
        totalCotizaciones: quotes.length,
        montoTotalCotizado: Number(totalCotizado.toFixed(2)),
        montoTotalEntregado: Number(totalEntregado.toFixed(2)),
        pendiente,
        fillRatePct: fillRate,
      },
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ START
========================================================= */
(async () => {
  try {
    await ensureDb();
    console.log(hasDb() ? "DB ready ✅" : "DB not configured (skipped init) ⚠️");
  } catch (e) {
    console.error("DB init error:", e.message);
  }

  app.listen(Number(PORT), () => {
    console.log(`Admin server listening on :${PORT}`);
  });
})();
