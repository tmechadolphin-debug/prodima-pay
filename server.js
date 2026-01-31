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
  SAP_PRICE_LIST = "Lista 02 Res. Com. Ind. Analitic",
  YAPPY_ALIAS = "@prodimasansae",
} = process.env;

/* =========================================================
   ✅ CORS
========================================================= */
app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? true : CORS_ORIGIN,
    credentials: false,
  })
);

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
   ✅ Time helpers (Panamá UTC-5)
========================================================= */
const TZ_OFFSET_MIN = -300;

function getDateISOInOffset(offsetMin = 0) {
  const now = new Date();
  const ms =
    now.getTime() + now.getTimezoneOffset() * 60_000 + Number(offsetMin) * 60_000;
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
   ✅ Warehouse map
========================================================= */
function provinceToWarehouse(province) {
  const p = String(province || "").trim().toLowerCase();
  if (p === "chiriquí" || p === "chiriqui" || p === "bocas del toro") return "200";
  if (
    p === "veraguas" ||
    p === "coclé" ||
    p === "cocle" ||
    p === "los santos" ||
    p === "herrera"
  )
    return "500";
  if (
    p === "panamá" ||
    p === "panama" ||
    p === "panamá oeste" ||
    p === "panama oeste" ||
    p === "colón" ||
    p === "colon"
  )
    return "300";
  if (p === "rci") return "01";
  return SAP_WAREHOUSE || "300";
}

function getWarehouseFromReq(req) {
  const whToken = String(req.user?.warehouse_code || "").trim();
  if (whToken) return whToken;

  const whQuery = String(req.query?.warehouse || req.query?.wh || "").trim();
  if (whQuery) return whQuery;

  const whHeader = String(req.headers["x-warehouse"] || "").trim();
  if (whHeader) return whHeader;

  const prov = String(req.user?.province || "").trim();
  if (prov) return provinceToWarehouse(prov);

  return SAP_WAREHOUSE || "300";
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

function signUserToken(u, ttl = "30d") {
  return signToken(
    {
      role: "user",
      id: u.id,
      username: u.username,
      full_name: u.full_name || "",
      province: u.province || "",
      warehouse_code: u.warehouse_code || "",
    },
    ttl
  );
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
    if (decoded?.role !== "admin")
      return safeJson(res, 403, { ok: false, message: "Forbidden" });
    req.admin = decoded;
    next();
  } catch {
    return safeJson(res, 401, { ok: false, message: "Invalid token" });
  }
}

function verifyUser(req, res, next) {
  const token = readBearer(req);
  if (!token) return safeJson(res, 401, { ok: false, message: "Missing Bearer token" });

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    if (decoded?.role !== "user")
      return safeJson(res, 403, { ok: false, message: "Forbidden" });
    req.user = decoded;
    next();
  } catch {
    return safeJson(res, 401, { ok: false, message: "Invalid token" });
  }
}

async function audit(eventName, req, actor, extra = {}) {
  try {
    console.log(
      `[AUDIT] ${new Date().toISOString()} ${eventName} actor=${actor} ip=${req.ip}`,
      extra
    );
  } catch {}
}

function missingSapEnv() {
  return !SAP_BASE_URL || !SAP_COMPANYDB || !SAP_USER || !SAP_PASS;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
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
  const cancelVal =
    q?.CancelStatus ?? q?.cancelStatus ?? q?.Cancelled ?? q?.cancelled ?? "";
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
   ✅ SAP Service Layer (Session cookie)
========================================================= */
let SL_COOKIE = "";
let SL_COOKIE_AT = 0;

async function slLogin() {
  const url = `${SAP_BASE_URL.replace(/\/$/, "")}/Login`;
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
  let data = {};
  try {
    data = JSON.parse(txt);
  } catch {}

  if (!r.ok) {
    throw new Error(
      `SAP login failed: HTTP ${r.status} ${data?.error?.message?.value || txt}`
    );
  }

  const setCookie = r.headers.get("set-cookie") || "";
  const cookies = [];
  for (const part of setCookie.split(",")) {
    const s = part.trim();
    if (s.startsWith("B1SESSION=") || s.startsWith("ROUTEID=")) {
      cookies.push(s.split(";")[0]);
    }
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
  const headers = {
    "Content-Type": "application/json",
    Cookie: SL_COOKIE,
    ...(options.headers || {}),
  };

  const r = await fetch(url, {
    method,
    headers,
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
}

/* =========================================================
   ✅ SAP helpers
========================================================= */
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

async function sapGetByDocEntry(entity, docEntry, select) {
  const n = Number(docEntry);
  if (!Number.isFinite(n) || n <= 0) throw new Error("DocEntry inválido");

  let path = `/${entity}(${n})`;
  if (select) path += `?$select=${encodeURIComponent(select)}`;
  return slFetch(path);
}

/* =========================================================
   ✅ TRACE logic + cache
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

async function traceQuote(quoteDocNum, fromOverride, toOverride) {
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

  const quote = await sapGetByDocEntry("Quotations", quoteHead.DocEntry);
  const quoteDocEntry = Number(quote.DocEntry);
  const cardCode = String(quote.CardCode || "").trim();
  const quoteDate = String(quote.DocDate || "").slice(0, 10);

  const from = /^\d{4}-\d{2}-\d{2}$/.test(String(fromOverride || ""))
    ? String(fromOverride)
    : addDaysISO(quoteDate, -7);

  const to = /^\d{4}-\d{2}-\d{2}$/.test(String(toOverride || ""))
    ? String(toOverride)
    : addDaysISO(quoteDate, 30);

  // ✅ half-open: [from, to+1)
  const toPlus1 = addDaysISO(to, 1);

  const ordersList = await slFetch(
    `/Orders?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
      `&$filter=${encodeURIComponent(
        `CardCode eq '${cardCode.replace(/'/g, "''")}' and DocDate ge '${from}' and DocDate lt '${toPlus1}'`
      )}` +
      `&$orderby=DocDate asc&$top=200`
  );
  const orderCandidates = Array.isArray(ordersList?.value) ? ordersList.value : [];

  const orders = [];
  for (const o of orderCandidates) {
    const od = await sapGetByDocEntry("Orders", o.DocEntry);
    const lines = Array.isArray(od?.DocumentLines) ? od.DocumentLines : [];
    const linked = lines.some(
      (l) => Number(l?.BaseType) === 23 && Number(l?.BaseEntry) === quoteDocEntry
    );
    if (linked) orders.push(od);
    await sleep(30);
  }

  const deliveries = [];
  const orderDocEntrySet = new Set(orders.map((x) => Number(x.DocEntry)));

  if (orders.length) {
    const delList = await slFetch(
      `/DeliveryNotes?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
        `&$filter=${encodeURIComponent(
          `CardCode eq '${cardCode.replace(/'/g, "''")}' and DocDate ge '${from}' and DocDate lt '${toPlus1}'`
        )}` +
        `&$orderby=DocDate asc&$top=300`
    );
    const delCandidates = Array.isArray(delList?.value) ? delList.value : [];

    const seen = new Set();
    for (const d of delCandidates) {
      const dd = await sapGetByDocEntry("DeliveryNotes", d.DocEntry);
      const lines = Array.isArray(dd?.DocumentLines) ? dd.DocumentLines : [];
      const linked = lines.some(
        (l) => Number(l?.BaseType) === 17 && orderDocEntrySet.has(Number(l?.BaseEntry))
      );
      if (linked) {
        const de = Number(dd.DocEntry);
        if (!seen.has(de)) {
          seen.add(de);
          deliveries.push(dd);
        }
      }
      await sleep(30);
    }
  }

  const totalCotizado = Number(quote.DocTotal || 0);
  const totalPedido = orders.reduce((a, o) => a + Number(o?.DocTotal || 0), 0);
  const totalEntregado = deliveries.reduce((a, d) => a + Number(d?.DocTotal || 0), 0);
  const pendiente = Number((totalCotizado - totalEntregado).toFixed(2));

  const out = {
    ok: true,
    quote,
    orders,
    deliveries,
    totals: { totalCotizado, totalPedido, totalEntregado, pendiente },
    debug: { from, to, cardCode, quoteDocEntry },
  };

  cacheSet(cacheKey, out);
  cacheSet(`QDOCENTRY:${quoteDocEntry}`, out);
  return out;
}

/* =========================================================
   ✅ HEALTH
========================================================= */
app.get("/api/health", async (req, res) => {
  safeJson(res, 200, {
    ok: true,
    message: "✅ PRODIMA API activa",
    yappy: YAPPY_ALIAS,
    warehouse_default: SAP_WAREHOUSE,
    priceList: SAP_PRICE_LIST,
    db: hasDb() ? "on" : "off",
  });
});

/* =========================================================
   ✅ USER LOGIN (unificado)
========================================================= */
async function handleUserLogin(req, res) {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const username = String(req.body?.username || req.body?.user || "").trim().toLowerCase();
    const pin = String(req.body?.pin || req.body?.pass || "").trim();

    if (!username || !pin) {
      return safeJson(res, 400, { ok: false, message: "username y pin requeridos" });
    }

    const r = await dbQuery(
      `SELECT id, username, full_name, pin_hash, province, warehouse_code, is_active
       FROM app_users
       WHERE username=$1
       LIMIT 1`,
      [username]
    );

    const u = r.rows?.[0];
    if (!u) {
      await audit("USER_LOGIN_FAIL", req, username, { reason: "not_found" });
      return safeJson(res, 401, { ok: false, message: "Credenciales inválidas" });
    }
    if (!u.is_active) {
      await audit("USER_LOGIN_FAIL", req, username, { reason: "inactive" });
      return safeJson(res, 403, { ok: false, message: "Usuario inactivo" });
    }

    const ok = await bcrypt.compare(pin, u.pin_hash);
    if (!ok) {
      await audit("USER_LOGIN_FAIL", req, username, { reason: "bad_pin" });
      return safeJson(res, 401, { ok: false, message: "Credenciales inválidas" });
    }

    let wh = String(u.warehouse_code || "").trim();
    if (!wh) {
      wh = provinceToWarehouse(u.province || "");
      try {
        await dbQuery(`UPDATE app_users SET warehouse_code=$1 WHERE id=$2`, [wh, u.id]);
        u.warehouse_code = wh;
      } catch {}
    }

    const token = signUserToken(u, "30d");

    await audit("USER_LOGIN_OK", req, username, {
      province: u.province,
      warehouse_code: u.warehouse_code,
    });

    return safeJson(res, 200, {
      ok: true,
      token,
      user: {
        id: u.id,
        username: u.username,
        full_name: u.full_name || "",
        province: u.province || "",
        warehouse_code: u.warehouse_code || "",
      },
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
}

app.post("/api/login", handleUserLogin);
app.post("/api/auth/login", handleUserLogin);

app.get("/api/me", verifyUser, async (req, res) => {
  safeJson(res, 200, { ok: true, user: req.user });
});
app.get("/api/auth/me", verifyUser, async (req, res) => {
  safeJson(res, 200, { ok: true, user: req.user });
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
   ✅ ADMIN USERS CRUD
========================================================= */
app.get("/api/admin/users", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const r = await dbQuery(
      `SELECT id, username, full_name, province, warehouse_code, is_active, created_at
       FROM app_users
       ORDER BY id DESC`
    );
    safeJson(res, 200, { ok: true, users: r.rows });
  } catch (e) {
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ ADMIN QUOTES LIST (HISTÓRICO + DASHBOARD)
   FIXES:
   - Total real para paginar
   - No se rompe por $top antes de filtrar
   - Fecha half-open: [from, to+1)
========================================================= */
async function scanQuotesAndPaginate({
  f,
  t,
  userFilter,
  clientFilter,
  wantSkip,
  wantLimit,
  includeTotal = true,
}) {
  const toPlus1 = addDaysISO(t, 1);

  const batchTop = 200;
  const maxPages = 200; // 200*200 = 40k registros máx (para mes suele sobrar)
  let skipSap = 0;

  let totalFiltered = 0;
  const pageRows = [];

  const safeUser = String(userFilter || "").trim().toLowerCase();
  const safeClient = String(clientFilter || "").trim().toLowerCase();

  for (let page = 0; page < maxPages; page++) {
    const raw = await slFetch(
      `/Quotations?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
        `&$filter=${encodeURIComponent(
          `DocDate ge '${f}' and DocDate lt '${toPlus1}'`
        )}` +
        `&$orderby=DocDate desc&$top=${batchTop}&$skip=${skipSap}`
    );

    const rows = Array.isArray(raw?.value) ? raw.value : [];
    if (!rows.length) break;

    skipSap += rows.length;

    for (const q of rows) {
      // ✅ omitimos canceladas aquí (no en OData)
      if (isCancelledLike(q)) continue;

      const usuario = parseUserFromComments(q.Comments || "") || "sin_user";
      const wh = parseWhFromComments(q.Comments || "") || "sin_wh";

      // filtros opcionales
      if (safeUser && !String(usuario).toLowerCase().includes(safeUser)) continue;

      if (safeClient) {
        const cc = String(q.CardCode || "").toLowerCase();
        const cn = String(q.CardName || "").toLowerCase();
        if (!cc.includes(safeClient) && !cn.includes(safeClient)) continue;
      }

      // este ya cuenta para total filtrado
      const idx = totalFiltered;
      totalFiltered++;

      // si cae dentro de la ventana de paginación, lo guardamos
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
          montoEntregado: 0,
          pendiente: Number(q.DocTotal || 0),
        });
      }
    }

    // Si no queremos total, podemos cortar cuando llenamos la página
    if (!includeTotal && pageRows.length >= wantLimit) break;
  }

  return { pageRows, totalFiltered };
}

app.get("/api/admin/quotes", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const from = String(req.query?.from || "");
    const to = String(req.query?.to || "");

    const userFilter = String(req.query?.user || "").trim();
    const clientFilter = String(req.query?.client || "").trim();

    const withDelivered = String(req.query?.withDelivered || "0") === "1";

    // ✅ soporta page/pageSize y también skip/limit
    const pageSizeRaw =
      req.query?.limit != null ? Number(req.query.limit) :
      req.query?.top != null ? Number(req.query.top) : 20;

    const safeTop = Math.max(1, Math.min(200, Number.isFinite(pageSizeRaw) ? pageSizeRaw : 20));

    const pageRaw = req.query?.page != null ? Number(req.query.page) : null;
    const skipRaw = req.query?.skip != null ? Number(req.query.skip) : null;

    const safeSkip = Number.isFinite(pageRaw) && pageRaw > 0
      ? (pageRaw - 1) * safeTop
      : Math.max(0, Number.isFinite(skipRaw) ? skipRaw : 0);

    // default fechas
    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const defaultFrom = addDaysISO(today, -30);

    const f = /^\d{4}-\d{2}-\d{2}$/.test(from) ? from : defaultFrom;
    const t = /^\d{4}-\d{2}-\d{2}$/.test(to) ? to : today;

    // ✅ importante: incluir total para paginar
    const includeTotal = String(req.query?.includeTotal ?? "1") === "1";

    const { pageRows, totalFiltered } = await scanQuotesAndPaginate({
      f,
      t,
      userFilter,
      clientFilter,
      wantSkip: safeSkip,
      wantLimit: safeTop,
      includeTotal,
    });

    // opcional: calcular entregado (trace) en la página actual
    if (withDelivered && pageRows.length) {
      const CONC = 2;
      let idx = 0;

      async function worker() {
        while (idx < pageRows.length) {
          const i = idx++;
          const q = pageRows[i];

          try {
            const tr = await traceQuote(q.docNum, f, t);
            if (tr.ok) {
              q.montoEntregado = Number(tr.totals?.totalEntregado || 0);
              q.pendiente = Number(tr.totals?.pendiente || 0);
              q.pedidoDocNum = tr.orders?.[0]?.DocNum ?? null;
              q.entregasDocNums = Array.isArray(tr.deliveries)
                ? tr.deliveries.map((d) => d.DocNum)
                : [];
            }
          } catch {}

          await sleep(25);
        }
      }

      await Promise.all(Array.from({ length: CONC }, () => worker()));
    }

    const pageCount = includeTotal ? Math.max(1, Math.ceil(totalFiltered / safeTop)) : null;
    const currentPage = Math.floor(safeSkip / safeTop) + 1;

    return safeJson(res, 200, {
      ok: true,
      quotes: pageRows,
      from: f,
      to: t,
      limit: safeTop,
      skip: safeSkip,
      page: currentPage,
      pageCount,
      total: includeTotal ? totalFiltered : null,
    });
  } catch (e) {
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ ADMIN TRACE
========================================================= */
app.get("/api/admin/trace/quote/:docNum", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const quoteDocNum = Number(req.params.docNum || 0);
    if (!quoteDocNum) return safeJson(res, 400, { ok: false, message: "docNum inválido" });

    const from = String(req.query?.from || "");
    const to = String(req.query?.to || "");

    const out = await traceQuote(quoteDocNum, from, to);
    if (!out.ok) return safeJson(res, 404, out);
    safeJson(res, 200, out);
  } catch (e) {
    safeJson(res, 500, { ok: false, message: e.message });
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
    console.log(`Server listening on :${PORT}`);
  });
})();
