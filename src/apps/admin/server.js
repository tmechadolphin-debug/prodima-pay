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
   ✅ Helpers
========================================================= */
function safeJson(res, status, obj) {
  res.status(status).json(obj);
}

function readBearer(req) {
  const auth = String(req.headers.authorization || "");
  const m = auth.match(/^Bearer\s+(.+)$/i);
  return m ? m[1] : "";
}

function signToken(payload, ttl = "12h") {
  return jwt.sign(payload, JWT_SECRET, { expiresIn: ttl });
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

function normalizeUsername(u) {
  const s = String(u || "").trim().toLowerCase();
  if (!s) return "";
  if (!/^[a-z0-9._-]{2,50}$/.test(s)) return "__INVALID__";
  return s;
}
function toIntId(x) {
  const n = Number(x);
  return Number.isFinite(n) && n > 0 ? Math.trunc(n) : null;
}

function missingSapEnv() {
  return !SAP_BASE_URL || !SAP_COMPANYDB || !SAP_USER || !SAP_PASS;
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
    app: "admin-api",
    message: "✅ PRODIMA ADMIN API activa",
    db: hasDb() ? "on" : "off",
    priceList: SAP_PRICE_LIST,
    warehouse_default: SAP_WAREHOUSE,
  });
});

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
   ✅ SAP helpers para documentos
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
   ✅ TRACE (para delivered) + cache
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
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
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

  const toPlus1 = addDaysISO(to, 1);

  const ordersList = await slFetch(
    `/Orders?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
      `&$filter=${encodeURIComponent(
        `CardCode eq '${cardCode.replace(/'/g, "''")}' and DocDate ge '${from}' and DocDate lt '${toPlus1}'`
      )}` +
      `&$orderby=DocDate desc,DocEntry desc&$top=200`
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
    await sleep(25);
  }

  const deliveries = [];
  const orderDocEntrySet = new Set(orders.map((x) => Number(x.DocEntry)));

  if (orders.length) {
    const delList = await slFetch(
      `/DeliveryNotes?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
        `&$filter=${encodeURIComponent(
          `CardCode eq '${cardCode.replace(/'/g, "''")}' and DocDate ge '${from}' and DocDate lt '${toPlus1}'`
        )}` +
        `&$orderby=DocDate desc,DocEntry desc&$top=300`
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
      await sleep(25);
    }
  }

  const totalCotizado = Number(quote.DocTotal || 0);
  const totalEntregado = deliveries.reduce((a, d) => a + Number(d?.DocTotal || 0), 0);
  const pendiente = Number((totalCotizado - totalEntregado).toFixed(2));

  const out = {
    ok: true,
    quote,
    orders,
    deliveries,
    totals: { totalCotizado, totalEntregado, pendiente },
  };

  cacheSet(cacheKey, out);
  cacheSet(`QDOCENTRY:${quoteDocEntry}`, out);
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
   ✅ ADMIN USERS (CRUD)
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

app.post("/api/admin/users", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const username = normalizeUsername(req.body?.username);
    const full_name = String(req.body?.full_name || req.body?.fullName || "").trim();
    const province = String(req.body?.province || "").trim();
    const pin = String(req.body?.pin || "").trim();
    const warehouse_code = String(req.body?.warehouse_code || req.body?.warehouse || "").trim();

    if (!username || username === "__INVALID__") {
      return safeJson(res, 400, {
        ok: false,
        message: "Username inválido. Usa letras/números y . _ - (mín 2).",
      });
    }
    if (!pin || pin.length < 4) {
      return safeJson(res, 400, { ok: false, message: "PIN mínimo 4" });
    }

    const pin_hash = await bcrypt.hash(pin, 10);

    const q = await dbQuery(
      `INSERT INTO app_users (username, full_name, pin_hash, province, warehouse_code, is_active)
       VALUES ($1,$2,$3,$4,$5,TRUE)
       RETURNING id, username, full_name, province, warehouse_code, is_active, created_at`,
      [username, full_name, pin_hash, province, warehouse_code]
    );

    return safeJson(res, 200, { ok: true, user: q.rows[0] });
  } catch (e) {
    if (String(e?.code) === "23505") {
      return safeJson(res, 409, { ok: false, message: "Ese username ya existe" });
    }
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.patch("/api/admin/users/:id/pin", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const id = toIntId(req.params.id);
    if (!id) return safeJson(res, 400, { ok: false, message: "ID inválido" });

    const pin = String(req.body?.pin || "").trim();
    if (!pin || pin.length < 4) {
      return safeJson(res, 400, { ok: false, message: "PIN mínimo 4" });
    }

    const pin_hash = await bcrypt.hash(pin, 10);

    const r = await dbQuery(
      `UPDATE app_users SET pin_hash=$1 WHERE id=$2
       RETURNING id, username, full_name, province, warehouse_code, is_active, created_at`,
      [pin_hash, id]
    );

    if (!r.rows?.length) return safeJson(res, 404, { ok: false, message: "Usuario no existe" });
    return safeJson(res, 200, { ok: true, user: r.rows[0] });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.patch("/api/admin/users/:id/toggle", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const id = toIntId(req.params.id);
    if (!id) return safeJson(res, 400, { ok: false, message: "ID inválido" });

    const r = await dbQuery(
      `UPDATE app_users
       SET is_active = NOT is_active
       WHERE id=$1
       RETURNING id, username, full_name, province, warehouse_code, is_active, created_at`,
      [id]
    );

    if (!r.rows?.length) return safeJson(res, 404, { ok: false, message: "Usuario no existe" });
    return safeJson(res, 200, { ok: true, user: r.rows[0] });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.delete("/api/admin/users/:id", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const id = toIntId(req.params.id);
    if (!id) return safeJson(res, 400, { ok: false, message: "ID inválido" });

    const r = await dbQuery(
      `DELETE FROM app_users WHERE id=$1
       RETURNING id`,
      [id]
    );

    if (!r.rows?.length) return safeJson(res, 404, { ok: false, message: "Usuario no existe" });
    return safeJson(res, 200, { ok: true });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ ADMIN QUOTES (paginado) + (opcional) delivered
========================================================= */
app.get("/api/admin/quotes", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv())
      return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const limit = Math.min(Math.max(Number(req.query?.limit || 20), 1), 200);
    const page = Math.max(1, Number(req.query?.page || 1));
    const skip = (page - 1) * limit;

    const today = getDateISOInOffset(TZ_OFFSET_MIN);

    const from = String(req.query?.from || "").trim();
    const to = String(req.query?.to || "").trim();

    function firstDayOfMonth(dateStr) {
      return dateStr.substring(0, 7) + "-01";
    }

    const defaultFrom = firstDayOfMonth(today);

    const f = /^\d{4}-\d{2}-\d{2}$/.test(from) ? from : defaultFrom;
    const t = /^\d{4}-\d{2}-\d{2}$/.test(to) ? to : today;

    const userFilter = String(req.query?.user || "").trim().toLowerCase();
    const clientFilter = String(req.query?.client || "").trim().toLowerCase();

    const withDelivered = String(req.query?.withDelivered || "0") === "1";

    let sapFilter = `DocDate ge '${f}' and DocDate le '${t}' and CancelStatus eq 'csNo'`;

    if (clientFilter) {
      const safe = clientFilter.replace(/'/g, "''");
      sapFilter += ` and (contains(CardCode,'${safe}') or contains(CardName,'${safe}'))`;
    }

    if (userFilter) {
      const safe = userFilter.replace(/'/g, "''");
      sapFilter += ` and contains(Comments,'${safe}')`;
    }

    const SELECT =
      `DocEntry,DocNum,CardCode,CardName,DocTotal,DocDate,DocumentStatus,CancelStatus,Comments`;

    const sap = await slFetch(
      `/Quotations?$select=${SELECT}` +
        `&$filter=${encodeURIComponent(sapFilter)}` +
        `&$orderby=DocDate desc,DocEntry desc` +
        `&$top=${limit}&$skip=${skip}`
    );

    const values = Array.isArray(sap?.value) ? sap.value : [];

    const quotes = [];
    for (const q of values) {
      const usuario = parseUserFromComments(q.Comments || "") || "sin_user";
      const wh = parseWhFromComments(q.Comments || "") || "sin_wh";

      const base = {
        docEntry: q.DocEntry,
        docNum: q.DocNum,
        cardCode: String(q.CardCode || "").trim(),
        cardName: String(q.CardName || "").trim(),
        montoCotizacion: Number(q.DocTotal || 0),
        fecha: String(q.DocDate || "").slice(0, 10),
        estado: q.DocumentStatus || "",
        comments: q.Comments || "",
        usuario,
        warehouse: wh,
        // defaults:
        montoEntregado: 0,
        pendiente: Number(q.DocTotal || 0),
      };

      if (withDelivered && Number(q?.DocNum) > 0) {
        try {
          // ⚠️ esto es costoso; está cacheado. Si se pone lento, baja limit o quítalo.
          const tr = await traceQuote(q.DocNum, f, t);
          if (tr?.ok) {
            base.montoEntregado = Number(tr?.totals?.totalEntregado || 0);
            base.pendiente = Number(tr?.totals?.pendiente || 0);
          }
        } catch {}
      }

      quotes.push(base);
    }

    let total = null;
    let pageCount = null;

    try {
      const countRes = await slFetch(
        `/Quotations/$count?$filter=${encodeURIComponent(sapFilter)}`
      );
      total = Number(countRes || 0);
      pageCount = Math.max(1, Math.ceil(total / limit));
    } catch {
      total = null;
      pageCount = null;
    }

    return safeJson(res, 200, {
      ok: true,
      quotes,
      from: f,
      to: t,
      page,
      limit,
      total,
      pageCount,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ ADMIN DASHBOARD (para tu HTML)
   - agrega totales y agrupaciones por usuario/bodega
========================================================= */
app.get("/api/admin/dashboard", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv())
      return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const today = getDateISOInOffset(TZ_OFFSET_MIN);

    const from = String(req.query?.from || "").trim();
    const to = String(req.query?.to || "").trim();

    function firstDayOfMonth(dateStr) {
      return dateStr.substring(0, 7) + "-01";
    }

    const defaultFrom = firstDayOfMonth(today);

    const f = /^\d{4}-\d{2}-\d{2}$/.test(from) ? from : defaultFrom;
    const t = /^\d{4}-\d{2}-\d{2}$/.test(to) ? to : today;

    const userFilter = String(req.query?.user || "").trim().toLowerCase();
    const clientFilter = String(req.query?.client || "").trim().toLowerCase();

    let sapFilter = `DocDate ge '${f}' and DocDate le '${t}' and CancelStatus eq 'csNo'`;

    if (clientFilter) {
      const safe = clientFilter.replace(/'/g, "''");
      sapFilter += ` and (contains(CardCode,'${safe}') or contains(CardName,'${safe}'))`;
    }
    if (userFilter) {
      const safe = userFilter.replace(/'/g, "''");
      sapFilter += ` and contains(Comments,'${safe}')`;
    }

    // Traemos un "batch" grande (dashboard es resumen, no paginado).
    // Si tu SAP es grande y se pone lento, baja este tope.
    const TOP = 1000;

    const SELECT =
      `DocEntry,DocNum,CardCode,CardName,DocTotal,DocDate,DocumentStatus,CancelStatus,Comments`;

    const sap = await slFetch(
      `/Quotations?$select=${SELECT}` +
        `&$filter=${encodeURIComponent(sapFilter)}` +
        `&$orderby=DocDate desc,DocEntry desc` +
        `&$top=${TOP}`
    );

    const values = Array.isArray(sap?.value) ? sap.value : [];

    const totals = {
      count: 0,
      totalCotizado: 0,
    };

    const byUser = new Map();
    const byWarehouse = new Map();

    for (const q of values) {
      if (isCancelledLike(q)) continue;

      const usuario = parseUserFromComments(q.Comments || "") || "sin_user";
      const wh = parseWhFromComments(q.Comments || "") || "sin_wh";
      const amt = Number(q.DocTotal || 0);

      totals.count += 1;
      totals.totalCotizado += amt;

      byUser.set(usuario, (byUser.get(usuario) || 0) + amt);
      byWarehouse.set(wh, (byWarehouse.get(wh) || 0) + amt);
    }

    function mapToSortedArr(m) {
      return Array.from(m.entries())
        .map(([k, v]) => ({ key: k, total: Number(v.toFixed(2)) }))
        .sort((a, b) => b.total - a.total);
    }

    return safeJson(res, 200, {
      ok: true,
      from: f,
      to: t,
      totals: {
        ...totals,
        totalCotizado: Number(totals.totalCotizado.toFixed(2)),
      },
      groups: {
        byUser: mapToSortedArr(byUser),
        byWarehouse: mapToSortedArr(byWarehouse),
      },
      note: "Dashboard basado en Quotations (entregas opcionales están en /api/admin/quotes?withDelivered=1)",
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
    console.log(`ADMIN API listening on :${PORT}`);
  });
})();
