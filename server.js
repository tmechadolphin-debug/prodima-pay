import express from "express";
import cors from "cors";
import pg from "pg";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";

const { Pool } = pg;

const app = express();
app.use(express.json({ limit: "4mb" }));

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

  // Opcionales
  SAP_TIMEOUT_MS = "12000",
} = process.env;

const SAP_TIMEOUT = Math.max(2000, Number(SAP_TIMEOUT_MS || 12000));

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
   Supabase/Render: normalmente requiere SSL con rejectUnauthorized:false
========================================================= */
const pool =
  DATABASE_URL
    ? new Pool({
        connectionString: DATABASE_URL,
        ssl: { rejectUnauthorized: false },
        max: 6,
        idleTimeoutMillis: 30_000,
        connectionTimeoutMillis: 8_000,
      })
    : null;

async function ensureDb() {
  if (!pool) return;

  await pool.query(`
    CREATE TABLE IF NOT EXISTS app_users (
      id SERIAL PRIMARY KEY,
      username TEXT UNIQUE NOT NULL,
      full_name TEXT DEFAULT '',
      pin_hash TEXT NOT NULL,
      province TEXT DEFAULT '',
      warehouse_code TEXT DEFAULT '',
      is_active BOOLEAN DEFAULT TRUE,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  // por si vienes de versiones anteriores
  await pool.query(`ALTER TABLE app_users ADD COLUMN IF NOT EXISTS province TEXT DEFAULT '';`).catch(()=>{});
  await pool.query(`ALTER TABLE app_users ADD COLUMN IF NOT EXISTS warehouse_code TEXT DEFAULT '';`).catch(()=>{});
  await pool.query(`ALTER TABLE app_users ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE;`).catch(()=>{});
}

function hasDb() {
  return !!pool;
}

/* =========================================================
   ✅ Provincias -> Bodegas
========================================================= */
function provinceToWarehouse(province) {
  const p = String(province || "").trim().toLowerCase();

  if (p === "chiriquí" || p === "chiriqui" || p === "bocas del toro") return "200";
  if (p === "veraguas" || p === "coclé" || p === "cocle" || p === "los santos" || p === "herrera") return "500";
  if (
    p === "panamá" || p === "panama" ||
    p === "panamá oeste" || p === "panama oeste" ||
    p === "colón" || p === "colon"
  ) return "300";

  if (p === "rci") return "01";

  return SAP_WAREHOUSE || "300";
}

/* =========================================================
   ✅ Helpers
========================================================= */
function safeJson(res, status, obj) {
  res.status(status).json(obj);
}

function missingSapEnv() {
  return !SAP_BASE_URL || !SAP_COMPANYDB || !SAP_USER || !SAP_PASS;
}

function normUserKey(s) {
  // normaliza para que filtros no "pierdan" usuarios por espacios raros / mayúsculas
  return String(s || "")
    .normalize("NFKC")
    .replace(/[\u200B-\u200D\uFEFF]/g, "") // zero width
    .trim()
    .toLowerCase();
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
  // SAP suele traer CancelStatus (csYes/csNo) y/o Cancelled (tYES/tNO)
  const cancelVal = q?.CancelStatus ?? q?.cancelStatus ?? "";
  const cancelledVal = q?.Cancelled ?? q?.cancelled ?? "";

  const cancelRaw = String(cancelVal).trim().toLowerCase();
  const cancelledRaw = String(cancelledVal).trim().toLowerCase();

  const commLower = String(q?.Comments || q?.comments || "").toLowerCase();

  return (
    cancelRaw === "csyes" ||
    cancelledRaw === "tyes" ||
    cancelledRaw === "true" ||
    cancelRaw.includes("csyes") ||
    cancelRaw.includes("cancel") ||
    commLower.includes("[cancel") ||
    commLower.includes("cancelad")
  );
}

function signAdminToken(payload) {
  return jwt.sign(payload, JWT_SECRET, { expiresIn: "12h" });
}

function signUserToken(userRow) {
  return jwt.sign(
    {
      typ: "user",
      id: userRow.id,
      username: userRow.username,
      full_name: userRow.full_name || "",
      province: userRow.province || "",
      warehouse_code: userRow.warehouse_code || "",
    },
    JWT_SECRET,
    { expiresIn: "30d" }
  );
}

function verifyAdmin(req, res, next) {
  const auth = String(req.headers.authorization || "");
  const m = auth.match(/^Bearer\s+(.+)$/i);
  if (!m) return safeJson(res, 401, { ok: false, message: "Missing Bearer token" });

  try {
    const decoded = jwt.verify(m[1], JWT_SECRET);
    if (!decoded?.role || decoded.role !== "admin") {
      return safeJson(res, 403, { ok: false, message: "Forbidden" });
    }
    req.admin = decoded;
    next();
  } catch {
    return safeJson(res, 401, { ok: false, message: "Invalid token" });
  }
}

function verifyUser(req, res, next) {
  const auth = String(req.headers.authorization || "");
  const m = auth.match(/^Bearer\s+(.+)$/i);
  if (!m) return safeJson(res, 401, { ok: false, message: "Missing Bearer token" });

  try {
    const decoded = jwt.verify(m[1], JWT_SECRET);
    if (!decoded || decoded.typ !== "user") {
      return safeJson(res, 403, { ok: false, message: "Forbidden" });
    }
    req.user = decoded;
    next();
  } catch {
    return safeJson(res, 401, { ok: false, message: "Invalid token" });
  }
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

/* =========================================================
   ✅ SAP Service Layer (Session cookie + retry + timeout)
========================================================= */
let SL_COOKIE = "";
let SL_COOKIE_AT = 0;

function baseSap() {
  return SAP_BASE_URL.replace(/\/$/, "");
}

async function slLogin() {
  const url = `${baseSap()}/Login`;
  const body = {
    CompanyDB: SAP_COMPANYDB,
    UserName: SAP_USER,
    Password: SAP_PASS,
  };

  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), SAP_TIMEOUT);

  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal: ctrl.signal,
  }).finally(() => clearTimeout(t));

  const txt = await r.text().catch(() => "");
  let data = {};
  try { data = JSON.parse(txt); } catch {}

  if (!r.ok) {
    throw new Error(`SAP login failed: HTTP ${r.status} ${data?.error?.message?.value || txt}`);
  }

  const setCookie = r.headers.get("set-cookie") || "";

  // Tomar B1SESSION + ROUTEID (robusto)
  const cookies = [];
  // split por coma pero cuidando que SAP no meta comas dentro (lo usual es ok)
  for (const chunk of setCookie.split(",")) {
    const s = chunk.trim();
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

  // refrescar sesión cada ~25min o si no hay cookie
  if (!SL_COOKIE || (Date.now() - SL_COOKIE_AT) > 25 * 60 * 1000) {
    await slLogin();
  }

  const url = `${baseSap()}${path.startsWith("/") ? path : `/${path}`}`;

  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), SAP_TIMEOUT);

  const method = (options.method || "GET").toUpperCase();
  const body = options.body ?? undefined;

  const r = await fetch(url, {
    method,
    headers: {
      "Content-Type": "application/json",
      Cookie: SL_COOKIE,
      ...(options.headers || {}),
    },
    body,
    signal: ctrl.signal,
  }).finally(() => clearTimeout(t));

  const txt = await r.text().catch(() => "");
  let data = {};
  try { data = txt ? JSON.parse(txt) : {}; } catch { data = { raw: txt }; }

  if (!r.ok) {
    // sesión expirada → reintento 1 vez
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
   ✅ PRICE LIST NO (cache)
========================================================= */
let PRICE_LIST_CACHE = { name: "", no: null, at: 0 };
const PRICE_LIST_TTL = 6 * 60 * 60 * 1000;

async function getPriceListNoByNameCached(name) {
  const now = Date.now();
  if (PRICE_LIST_CACHE.name === name && PRICE_LIST_CACHE.no != null && (now - PRICE_LIST_CACHE.at) < PRICE_LIST_TTL) {
    return PRICE_LIST_CACHE.no;
  }

  const safe = String(name || "").replace(/'/g, "''");
  let no = null;

  // SAP B1 a veces usa PriceListName o ListName
  try {
    const r1 = await slFetch(`/PriceLists?$select=PriceListNo,PriceListName&$filter=PriceListName eq '${safe}'&$top=1`);
    if (Array.isArray(r1?.value) && r1.value.length) no = r1.value[0].PriceListNo;
  } catch {}

  if (no == null) {
    try {
      const r2 = await slFetch(`/PriceLists?$select=PriceListNo,ListName&$filter=ListName eq '${safe}'&$top=1`);
      if (Array.isArray(r2?.value) && r2.value.length) no = r2.value[0].PriceListNo;
    } catch {}
  }

  PRICE_LIST_CACHE = { name, no, at: now };
  return no;
}

function getPriceFromPriceList(itemFull, priceListNo) {
  const listNo = Number(priceListNo);
  const row = Array.isArray(itemFull?.ItemPrices)
    ? itemFull.ItemPrices.find((p) => Number(p?.PriceList) === listNo)
    : null;
  const price = row && row.Price != null ? Number(row.Price) : null;
  return Number.isFinite(price) ? price : null;
}

/* =========================================================
   ✅ FACTOR CAJA (UoM de ventas) → precio por caja
========================================================= */
function getSalesUomFactor(itemFull) {
  const directFields = [
    itemFull?.SalesItemsPerUnit,
    itemFull?.SalesQtyPerPackUnit,
    itemFull?.SalesQtyPerPackage,
  ];
  for (const v of directFields) {
    const n = Number(v);
    if (Number.isFinite(n) && n > 0) return n;
  }

  const coll = itemFull?.ItemUnitOfMeasurementCollection;
  if (!Array.isArray(coll) || !coll.length) return null;

  let row =
    coll.find((x) => String(x?.UoMType || "").toLowerCase().includes("sales")) ||
    coll.find((x) => Number(x?.BaseQuantity) > 1) ||
    null;

  if (!row) return null;

  const b = Number(row?.BaseQuantity ?? row?.BaseQty ?? null);
  const a = Number(row?.AlternateQuantity ?? row?.AltQty ?? row?.AlternativeQuantity ?? null);

  if (Number.isFinite(b) && b > 0 && Number.isFinite(a) && a > 0) {
    const f = b / a;
    return Number.isFinite(f) && f > 0 ? f : null;
  }
  if (Number.isFinite(b) && b > 0) return b;
  return null;
}

/* =========================================================
   ✅ TRACE logic + cache
   (tu lógica actual se mantiene, solo queda “encapsulada”)
========================================================= */
const TRACE_CACHE = new Map();
const TRACE_TTL_MS = 6 * 60 * 60 * 1000;

function traceCacheGet(key) {
  const it = TRACE_CACHE.get(key);
  if (!it) return null;
  if ((Date.now() - it.at) > TRACE_TTL_MS) {
    TRACE_CACHE.delete(key);
    return null;
  }
  return it.data;
}
function traceCacheSet(key, data) {
  TRACE_CACHE.set(key, { at: Date.now(), data });
}

async function traceQuote(quoteDocNum, fromOverride, toOverride) {
  const cacheKey = `QDOCNUM:${quoteDocNum}`;
  const cached = traceCacheGet(cacheKey);
  if (cached) return cached;

  const quoteHead = await sapGetFirstByDocNum(
    "Quotations",
    quoteDocNum,
    "DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Cancelled,Comments"
  );
  if (!quoteHead) {
    const out = { ok: false, message: "Cotización no encontrada" };
    traceCacheSet(cacheKey, out);
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

  const ordersList = await slFetch(
    `/Orders?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Cancelled,Comments` +
      `&$filter=${encodeURIComponent(
        `CardCode eq '${cardCode.replace(/'/g, "''")}' and DocDate ge '${from}' and DocDate le '${to}'`
      )}` +
      `&$orderby=DocDate asc&$top=80`
  );
  const orderCandidates = Array.isArray(ordersList?.value) ? ordersList.value : [];

  const orders = [];
  for (const o of orderCandidates) {
    const od = await sapGetByDocEntry("Orders", o.DocEntry);
    const lines = Array.isArray(od?.DocumentLines) ? od.DocumentLines : [];
    const linked = lines.some(
      (l) => Number(l?.BaseType) === 23 && Number(l?.BaseEntry) === quoteDocEntry
    );
    if (linked && !isCancelledLike(od)) orders.push(od);
    await sleep(30);
  }

  const deliveries = [];
  const orderDocEntrySet = new Set(orders.map((x) => Number(x.DocEntry)));

  if (orders.length) {
    const delList = await slFetch(
      `/DeliveryNotes?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Cancelled,Comments` +
        `&$filter=${encodeURIComponent(
          `CardCode eq '${cardCode.replace(/'/g, "''")}' and DocDate ge '${from}' and DocDate le '${to}'`
        )}` +
        `&$orderby=DocDate asc&$top=120`
    );
    const delCandidates = Array.isArray(delList?.value) ? delList.value : [];

    const seen = new Set();
    for (const d of delCandidates) {
      const dd = await sapGetByDocEntry("DeliveryNotes", d.DocEntry);
      if (isCancelledLike(dd)) { await sleep(30); continue; }

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

  traceCacheSet(cacheKey, out);
  traceCacheSet(`QDOCENTRY:${quoteDocEntry}`, out);
  return out;
}

/* =========================================================
   ✅ Routes
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
   ✅ LOGIN ADMIN
========================================================= */
app.post("/api/admin/login", async (req, res) => {
  const user = String(req.body?.user || "").trim();
  const pass = String(req.body?.pass || "").trim();

  if (user !== ADMIN_USER || pass !== ADMIN_PASS) {
    return safeJson(res, 401, { ok: false, message: "Credenciales inválidas" });
  }

  const token = signAdminToken({ role: "admin", user });
  return safeJson(res, 200, { ok: true, token });
});

/* =========================================================
   ✅ LOGIN PEDIDOS (MERCADERISTAS)  ← ESTE ERA EL PROBLEMA
   Endpoint esperado por tu App de Pedidos:
   POST /api/auth/login { username, pin }
========================================================= */
app.post("/api/auth/login", async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const username = normUserKey(req.body?.username || "");
    const pin = String(req.body?.pin || "").trim();

    if (!username || !pin) return safeJson(res, 400, { ok: false, message: "username y pin requeridos" });

    const r = await pool.query(
      `SELECT id, username, full_name, pin_hash, province, warehouse_code, is_active
       FROM app_users WHERE lower(username)= $1 LIMIT 1`,
      [username]
    );

    if (!r.rows[0]) return safeJson(res, 401, { ok: false, message: "Credenciales inválidas" });

    const u = r.rows[0];
    if (!u.is_active) return safeJson(res, 401, { ok: false, message: "Usuario desactivado" });

    const ok = await bcrypt.compare(pin, u.pin_hash);
    if (!ok) return safeJson(res, 401, { ok: false, message: "Credenciales inválidas" });

    // asegurar warehouse_code
    let wh = String(u.warehouse_code || "").trim();
    if (!wh) {
      wh = provinceToWarehouse(u.province || "");
      try {
        await pool.query(`UPDATE app_users SET warehouse_code=$1 WHERE id=$2`, [wh, u.id]);
        u.warehouse_code = wh;
      } catch {}
    }

    const token = signUserToken(u);

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
});

app.get("/api/auth/me", verifyUser, async (req, res) => {
  safeJson(res, 200, { ok: true, user: req.user });
});

/* =========================================================
   ✅ USERS (ADMIN CRUD)
========================================================= */
app.get("/api/admin/users", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const r = await pool.query(
      `SELECT id, username, full_name, province, warehouse_code, is_active, created_at
       FROM app_users
       ORDER BY id DESC`
    );
    safeJson(res, 200, { ok: true, users: r.rows });
  } catch (e) {
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.post("/api/admin/users", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const username = normUserKey(req.body?.username || "");
    const fullName = String(req.body?.fullName || req.body?.full_name || "").trim();
    const pin = String(req.body?.pin || "").trim();
    const province = String(req.body?.province || "").trim();

    if (!username) return safeJson(res, 400, { ok: false, message: "username requerido" });
    if (!pin || pin.length < 4) return safeJson(res, 400, { ok: false, message: "PIN mínimo 4" });

    const wh = provinceToWarehouse(province);
    const pin_hash = await bcrypt.hash(pin, 10);

    const r = await pool.query(
      `INSERT INTO app_users (username, full_name, pin_hash, province, warehouse_code, is_active)
       VALUES ($1,$2,$3,$4,$5,TRUE)
       RETURNING id, username, full_name, province, warehouse_code, is_active, created_at`,
      [username, fullName, pin_hash, province, wh]
    );

    safeJson(res, 200, { ok: true, user: r.rows[0] });
  } catch (e) {
    const msg = String(e.message || "");
    if (msg.includes("duplicate key")) {
      return safeJson(res, 409, { ok: false, message: "username ya existe" });
    }
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.patch("/api/admin/users/:id/toggle", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const id = Number(req.params.id || 0);
    const r = await pool.query(
      `UPDATE app_users
       SET is_active = NOT is_active
       WHERE id=$1
       RETURNING id, username, full_name, province, warehouse_code, is_active, created_at`,
      [id]
    );
    if (!r.rows[0]) return safeJson(res, 404, { ok: false, message: "No encontrado" });
    safeJson(res, 200, { ok: true, user: r.rows[0] });
  } catch (e) {
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.patch("/api/admin/users/:id/pin", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const id = Number(req.params.id || 0);
    const pin = String(req.body?.pin || "").trim();
    if (!pin || pin.length < 4) return safeJson(res, 400, { ok: false, message: "PIN mínimo 4" });

    const pin_hash = await bcrypt.hash(pin, 10);
    const r = await pool.query(
      `UPDATE app_users SET pin_hash=$2 WHERE id=$1 RETURNING id`,
      [id, pin_hash]
    );
    if (!r.rows[0]) return safeJson(res, 404, { ok: false, message: "No encontrado" });
    safeJson(res, 200, { ok: true });
  } catch (e) {
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.delete("/api/admin/users/:id", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const id = Number(req.params.id || 0);
    const r = await pool.query(`DELETE FROM app_users WHERE id=$1 RETURNING id`, [id]);
    if (!r.rows[0]) return safeJson(res, 404, { ok: false, message: "No encontrado" });
    safeJson(res, 200, { ok: true });
  } catch (e) {
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ EXPORT EXCEL USERS (SERVER)
   GET /api/admin/users.xlsx
========================================================= */
app.get("/api/admin/users.xlsx", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const r = await pool.query(
      `SELECT id, username, full_name, is_active, province, warehouse_code, created_at
       FROM app_users
       ORDER BY id DESC`
    );

    const data = (r.rows || []).map(u => ({
      ID: u.id,
      Username: u.username,
      Nombre: u.full_name,
      Activo: u.is_active ? "Sí" : "No",
      Provincia: u.province,
      Bodega: u.warehouse_code,
      Creado: String(u.created_at || "").replace("T"," ").slice(0,19),
    }));

    const ws = XLSX.utils.json_to_sheet(data);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "Usuarios");

    const buf = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });

    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.setHeader("Content-Disposition", `attachment; filename="usuarios_prodima_${Date.now()}.xlsx"`);
    return res.status(200).send(buf);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ SAP: ITEM (precio en CAJA) + stock bodega
   GET /api/sap/item/:code   (USER TOKEN)
========================================================= */
app.get("/api/sap/item/:code", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const code = String(req.params.code || "").trim();
    if (!code) return safeJson(res, 400, { ok: false, message: "ItemCode vacío" });

    const warehouseCode = String(req.user?.warehouse_code || SAP_WAREHOUSE || "300").trim();
    const priceListNo = await getPriceListNoByNameCached(SAP_PRICE_LIST);

    const itemFull = await slFetch(
      `/Items('${encodeURIComponent(code)}')` +
        `?$select=ItemCode,ItemName,SalesUnit,InventoryItem,ItemPrices,SalesItemsPerUnit,SalesQtyPerPackUnit,SalesQtyPerPackage,ItemWarehouseInfoCollection` +
        `&$expand=ItemUnitOfMeasurementCollection($select=UoMType,UoMCode,UoMEntry,BaseQuantity,AlternateQuantity),` +
        `ItemWarehouseInfoCollection($select=WarehouseCode,InStock,Committed,Ordered)`
    );

    const priceUnit = getPriceFromPriceList(itemFull, priceListNo);
    const factorCaja = getSalesUomFactor(itemFull);
    const priceCaja = (priceUnit != null && factorCaja != null) ? (priceUnit * factorCaja) : priceUnit;

    const whRow = Array.isArray(itemFull?.ItemWarehouseInfoCollection)
      ? itemFull.ItemWarehouseInfoCollection.find(w => String(w?.WarehouseCode||"").trim() === warehouseCode)
      : null;

    const onHand = whRow?.InStock != null ? Number(whRow.InStock) : null;
    const committed = whRow?.Committed != null ? Number(whRow.Committed) : null;
    const ordered = whRow?.Ordered != null ? Number(whRow.Ordered) : null;
    const available = (Number.isFinite(onHand) && Number.isFinite(committed)) ? (onHand - committed) : null;

    return safeJson(res, 200, {
      ok: true,
      item: {
        ItemCode: itemFull.ItemCode,
        ItemName: itemFull.ItemName,
        SalesUnit: itemFull.SalesUnit || "Caja",
        InventoryItem: itemFull.InventoryItem ?? null,
      },
      warehouse: warehouseCode,
      priceList: SAP_PRICE_LIST,
      priceListNo,
      priceUnit,
      factorCaja,
      priceCaja,
      price: priceCaja, // compat
      stock: {
        onHand,
        committed,
        ordered,
        available,
        hasStock: available != null ? available > 0 : null,
      }
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ SAP: SEARCH ITEMS
   GET /api/sap/items/search?q=xxx   (USER TOKEN)
========================================================= */
app.get("/api/sap/items/search", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const q = String(req.query?.q || "").trim();
    const top = Math.min(Math.max(Number(req.query?.top || 20), 5), 50);
    if (q.length < 2) return safeJson(res, 200, { ok: true, q, results: [] });

    const safe = q.replace(/'/g, "''");

    let r;
    try {
      r = await slFetch(
        `/Items?$select=ItemCode,ItemName,SalesUnit,InventoryItem` +
          `&$filter=(contains(ItemCode,'${safe}') or contains(ItemName,'${safe}'))` +
          `&$orderby=ItemName asc&$top=${top}`
      );
    } catch {
      r = await slFetch(
        `/Items?$select=ItemCode,ItemName,SalesUnit,InventoryItem` +
          `&$filter=substringof('${safe}',ItemCode) or substringof('${safe}',ItemName)` +
          `&$orderby=ItemName asc&$top=${top}`
      );
    }

    const values = Array.isArray(r?.value) ? r.value : [];
    const results = values.map(x => ({
      ItemCode: x.ItemCode,
      ItemName: x.ItemName,
      SalesUnit: x.SalesUnit || "",
      InventoryItem: x.InventoryItem ?? null
    }));

    return safeJson(res, 200, { ok: true, q, results });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ SAP: CREAR COTIZACIÓN (USER TOKEN)
   POST /api/sap/quote
   { cardCode, comments, lines:[{itemCode, qty}] }
========================================================= */
app.post("/api/sap/quote", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const cardCode = String(req.body?.cardCode || "").trim();
    const comments = String(req.body?.comments || "").trim();
    const lines = Array.isArray(req.body?.lines) ? req.body.lines : [];

    if (!cardCode) return safeJson(res, 400, { ok: false, message: "cardCode requerido" });
    if (!lines.length) return safeJson(res, 400, { ok: false, message: "lines requerido" });

    const warehouseCode = String(req.user?.warehouse_code || SAP_WAREHOUSE || "300").trim();

    const DocumentLines = lines
      .map(l => ({
        ItemCode: String(l.itemCode || "").trim(),
        Quantity: Number(l.qty || 0),
        WarehouseCode: warehouseCode,
      }))
      .filter(x => x.ItemCode && x.Quantity > 0);

    if (!DocumentLines.length) return safeJson(res, 400, { ok: false, message: "No hay líneas válidas" });

    const now = new Date();
    const docDate = now.toISOString().slice(0,10);

    const creator = String(req.user?.username || "").trim();
    const province = String(req.user?.province || "").trim();

    const sapComments = [
      `[WEB PEDIDOS]`,
      creator ? `[user:${creator}]` : "",
      province ? `[prov:${province}]` : "",
      warehouseCode ? `[wh:${warehouseCode}]` : "",
      comments || "Cotización mercaderista",
    ].filter(Boolean).join(" ");

    const created = await slFetch(`/Quotations`, {
      method: "POST",
      body: JSON.stringify({
        CardCode: cardCode,
        DocDate: docDate,
        DocDueDate: docDate,
        Comments: sapComments,
        JournalMemo: "Cotización web mercaderistas",
        DocumentLines,
      }),
    });

    return safeJson(res, 200, {
      ok: true,
      message: "Cotización creada",
      docEntry: created.DocEntry,
      docNum: created.DocNum,
      warehouse: warehouseCode,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ SAP basic (ADMIN debug)
========================================================= */
app.get("/api/admin/sap/order/:docNum", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const docNum = Number(req.params.docNum || 0);
    const head = await sapGetFirstByDocNum(
      "Orders",
      docNum,
      "DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Cancelled,Comments"
    );
    if (!head) return safeJson(res, 404, { ok: false, message: "Pedido no encontrado" });

    const order = await sapGetByDocEntry("Orders", head.DocEntry);
    safeJson(res, 200, { ok: true, order });
  } catch (e) {
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.get("/api/admin/sap/delivery/:docNum", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const docNum = Number(req.params.docNum || 0);
    const head = await sapGetFirstByDocNum(
      "DeliveryNotes",
      docNum,
      "DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Cancelled,Comments"
    );
    if (!head) return safeJson(res, 404, { ok: false, message: "Entrega no encontrada" });

    const delivery = await sapGetByDocEntry("DeliveryNotes", head.DocEntry);
    safeJson(res, 200, { ok: true, delivery });
  } catch (e) {
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ TRACE quote (ADMIN)
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
   ✅ Quotes list (Histórico)  — FIX IMPORTANTE
   Problema original: aplicabas $top/$skip ANTES del filtro user/client
   → si la página SAP no traía coincidencias, parecía “no existen”.
   Solución: paginar desde SAP hasta completar (skip+top) YA FILTRADO.
========================================================= */
app.get("/api/admin/quotes", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const fromQ = String(req.query?.from || "");
    const toQ = String(req.query?.to || "");

    const userFilterRaw = String(req.query?.user || "").trim();
    const clientFilterRaw = String(req.query?.client || "").trim();

    const userFilter = normUserKey(userFilterRaw);
    const clientFilter = String(clientFilterRaw || "").trim().toLowerCase();

    const withDelivered = String(req.query?.withDelivered || "0") === "1";

    // paginación solicitada por tu HTML
    const topReq = req.query?.top != null ? Number(req.query.top) : (req.query?.limit != null ? Number(req.query.limit) : 20);
    const skipReq = req.query?.skip != null ? Number(req.query.skip) : 0;

    const take = Math.max(1, Math.min(500, Number.isFinite(topReq) ? topReq : 20));
    const skip = Math.max(0, Number.isFinite(skipReq) ? skipReq : 0);

    // Si no pasan fechas, tomamos últimos 30 días (igual que tú)
    const now = new Date();
    const today = now.toISOString().slice(0,10);
    const defaultFrom = addDaysISO(today, -30);

    const f = /^\d{4}-\d{2}-\d{2}$/.test(fromQ) ? fromQ : defaultFrom;
    const t = /^\d{4}-\d{2}-\d{2}$/.test(toQ) ? toQ : today;

    const SAP_PAGE = 200;     // tamaño por página SAP
    const MAX_PAGES = 80;     // límite de seguridad

    const matched = [];

    // buscamos (skip+take) tras aplicar filtros
    const need = skip + take;

    for (let page = 0; page < MAX_PAGES && matched.length < need; page++) {
      const sapSkip = page * SAP_PAGE;

      const raw = await slFetch(
        `/Quotations?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Cancelled,Comments` +
          `&$filter=${encodeURIComponent(`DocDate ge '${f}' and DocDate le '${t}'`)}` +
          `&$orderby=DocDate desc&$top=${SAP_PAGE}&$skip=${sapSkip}`
      );

      const values = Array.isArray(raw?.value) ? raw.value : [];
      if (!values.length) break;

      for (const q of values) {
        const usuario = parseUserFromComments(q.Comments || "");
        const usuarioKey = normUserKey(usuario);

        const wh = parseWhFromComments(q.Comments || "") || "";

        const item = {
          docEntry: q.DocEntry,
          docNum: q.DocNum,
          cardCode: q.CardCode,
          cardName: q.CardName,
          fecha: String(q.DocDate || "").slice(0, 10),
          estado: q.DocumentStatus || "",
          cancelStatus: q.CancelStatus ?? "",
          cancelled: q.Cancelled ?? "",
          comments: q.Comments || "",
          usuario,
          warehouse: wh,
          montoCotizacion: Number(q.DocTotal || 0),
          montoEntregado: 0,
          pendiente: Number(q.DocTotal || 0),
          isCancelled: isCancelledLike(q),
        };

        // filtro usuario
        if (userFilter) {
          if (!usuarioKey.includes(userFilter)) continue;
        }
        // filtro cliente
        if (clientFilter) {
          const cc = String(item.cardCode || "").toLowerCase();
          const cn = String(item.cardName || "").toLowerCase();
          if (!cc.includes(clientFilter) && !cn.includes(clientFilter)) continue;
        }

        matched.push(item);
        if (matched.length >= need) break;
      }
    }

    const out = matched.slice(skip, skip + take);

    // entregado solo para lo devuelto
    if (withDelivered && out.length) {
      const CONC = 2;
      let idx = 0;

      async function worker() {
        while (idx < out.length) {
          const i = idx++;
          const q = out[i];

          if (q.isCancelled) {
            q.montoEntregado = 0;
            q.pendiente = Number(q.montoCotizacion || 0);
            continue;
          }

          try {
            const tr = await traceQuote(q.docNum, f, t);
            if (tr.ok) {
              q.montoEntregado = Number(tr.totals?.totalEntregado || 0);
              q.pendiente = Number(tr.totals?.pendiente || 0);
            }
          } catch {
            // dejamos 0
          }
          await sleep(25);
        }
      }

      await Promise.all(Array.from({ length: CONC }, worker));
    }

    return safeJson(res, 200, {
      ok: true,
      quotes: out,
      from: f,
      to: t,
      limit: take,
      skip,
    });
  } catch (e) {
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ Dashboard
   (tu HTML lo recalcula, entonces solo “ok”)
========================================================= */
app.get("/api/admin/dashboard", verifyAdmin, async (req, res) => {
  safeJson(res, 200, { ok: true });
});

/* =========================================================
   ✅ Start
========================================================= */
(async () => {
  try {
    await ensureDb();
    console.log("DB ready ✅");
  } catch (e) {
    console.error("DB init error:", e.message);
  }

  app.listen(Number(PORT), () => {
    console.log(`Server listening on :${PORT}`);
  });
})();
