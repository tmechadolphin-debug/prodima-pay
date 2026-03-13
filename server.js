
const DOCS_MAIL_BUILD = "DOCS_MAIL_V6_2026-03-12";
console.log("BOOT", DOCS_MAIL_BUILD);
import express from "express";
import pg from "pg";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";
import crypto from "crypto";
import XLSX from "xlsx";

const { Pool } = pg;
const app = express();
app.use(express.json({ limit: "20mb" }));
const __extraBootTasks = [];

/* =========================================================
   ENV
========================================================= */
const {
  PORT = 3000,
  DATABASE_URL = "",
  JWT_SECRET = "change_me",

  // CORS
  CORS_ORIGIN = "",

  // Admin
  ADMIN_USER = "PRODIMA",
  ADMIN_PASS = "ADMINISTRADOR",

  // SAP
  SAP_BASE_URL = "",
  SAP_COMPANYDB = "",
  SAP_USER = "",
  SAP_PASS = "",
  SAP_WAREHOUSE = "300",
  SAP_PRICE_LIST = "Lista de Precios 99 2018",
  SAP_RETURN_ENTITY = "Returns",
  SAP_RETURN_HEADER_MOTIVO_FIELD = "",
  SAP_RETURN_HEADER_CAUSA_FIELD = "",
  SAP_RETURN_LINE_MOTIVO_FIELD = "",
  SAP_RETURN_LINE_CAUSA_FIELD = "",

  // Devoluciones / dimensiones
  SAP_DIM1_DEFAULT = "",
  SAP_DIM1_200 = "",
  SAP_DIM1_300 = "",
  SAP_DIM1_500 = "",
  SAP_DIM1_01 = "",

  // Negocio
  YAPPY_ALIAS = "@prodimasansae",
  ACTIVE_CODES_200 = "",
  ACTIVE_CODES_300 = "",
  ACTIVE_CODES_500 = "",
  ADMIN_FREE_WHS_USERS = "soto,liliana,daniel11,respinosa,test",
  WAREHOUSE_FALLBACK = "200,300,500,01",

  // Devoluciones
  RETURN_MOTIVOS = "Producto vencido,Cliente rechazó,Producto dañado,Error de facturación,Otro",
  RETURN_CAUSAS = "Empaque roto,Pedido incorrecto,Producto incorrecto,Faltante,Otro",
} = process.env;

/* =========================================================
   CORS robusto
========================================================= */
const ALLOWED_ORIGINS = new Set(
  String(CORS_ORIGIN || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)
);
const allowAllOrigins = !String(CORS_ORIGIN || "").trim() || String(CORS_ORIGIN).trim() === "*";

app.use((req, res, next) => {
  const origin = req.headers.origin;

  if (allowAllOrigins && origin) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
  } else if (origin && ALLOWED_ORIGINS.has(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
  }

  res.setHeader("Access-Control-Allow-Methods", "GET,POST,PATCH,DELETE,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, x-warehouse");
  res.setHeader("Access-Control-Max-Age", "86400");

  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

/* =========================================================
   DB
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

  await dbQuery(`
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

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS return_requests (
      id BIGSERIAL PRIMARY KEY,
      req_num BIGINT UNIQUE NOT NULL,
      req_entry BIGINT,
      doc_date DATE,
      doc_time INT,
      card_code TEXT,
      card_name TEXT,
      usuario TEXT,
      warehouse TEXT,
      motivo TEXT,
      causa TEXT,
      total_amount NUMERIC(19,6) DEFAULT 0,
      total_qty NUMERIC(19,6) DEFAULT 0,
      status TEXT DEFAULT 'Open',
      comments TEXT,
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS return_lines (
      id BIGSERIAL PRIMARY KEY,
      req_num BIGINT NOT NULL,
      doc_date DATE,
      item_code TEXT NOT NULL,
      item_desc TEXT DEFAULT '',
      qty NUMERIC(19,6) DEFAULT 0,
      price NUMERIC(19,6) DEFAULT 0,
      line_total NUMERIC(19,6) DEFAULT 0,
      updated_at TIMESTAMP DEFAULT NOW(),
      UNIQUE(req_num, item_code)
    );
  `);

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS quotes_cache (
      id BIGSERIAL PRIMARY KEY,
      doc_num BIGINT UNIQUE NOT NULL,
      doc_entry BIGINT,
      doc_date DATE,
      doc_time INT,
      card_code TEXT,
      card_name TEXT,
      usuario TEXT,
      warehouse TEXT,
      doc_total NUMERIC(19,6) DEFAULT 0,
      delivered_total NUMERIC(19,6) DEFAULT 0,
      status TEXT,
      cancel_status TEXT,
      comments TEXT,
      group_name TEXT,
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS quote_lines_cache (
      id BIGSERIAL PRIMARY KEY,
      doc_num BIGINT NOT NULL,
      doc_date DATE,
      item_code TEXT NOT NULL,
      item_desc TEXT DEFAULT '',
      qty_quoted NUMERIC(19,6) DEFAULT 0,
      qty_delivered NUMERIC(19,6) DEFAULT 0,
      dollars_quoted NUMERIC(19,6) DEFAULT 0,
      dollars_delivered NUMERIC(19,6) DEFAULT 0,
      updated_at TIMESTAMP DEFAULT NOW(),
      UNIQUE(doc_num, item_code)
    );
  `);

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS app_state (
      k TEXT PRIMARY KEY,
      v TEXT NOT NULL,
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  const alters = [
    `ALTER TABLE return_requests ADD COLUMN IF NOT EXISTS req_num BIGINT`,
    `ALTER TABLE return_requests ADD COLUMN IF NOT EXISTS req_entry BIGINT`,
    `ALTER TABLE return_requests ADD COLUMN IF NOT EXISTS doc_date DATE`,
    `ALTER TABLE return_requests ADD COLUMN IF NOT EXISTS doc_time INT`,
    `ALTER TABLE return_requests ADD COLUMN IF NOT EXISTS card_code TEXT`,
    `ALTER TABLE return_requests ADD COLUMN IF NOT EXISTS card_name TEXT`,
    `ALTER TABLE return_requests ADD COLUMN IF NOT EXISTS usuario TEXT`,
    `ALTER TABLE return_requests ADD COLUMN IF NOT EXISTS warehouse TEXT`,
    `ALTER TABLE return_requests ADD COLUMN IF NOT EXISTS motivo TEXT`,
    `ALTER TABLE return_requests ADD COLUMN IF NOT EXISTS causa TEXT`,
    `ALTER TABLE return_requests ADD COLUMN IF NOT EXISTS total_amount NUMERIC(19,6) DEFAULT 0`,
    `ALTER TABLE return_requests ADD COLUMN IF NOT EXISTS total_qty NUMERIC(19,6) DEFAULT 0`,
    `ALTER TABLE return_requests ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'Open'`,
    `ALTER TABLE return_requests ADD COLUMN IF NOT EXISTS comments TEXT`,
    `ALTER TABLE return_requests ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()`,

    `ALTER TABLE return_lines ADD COLUMN IF NOT EXISTS req_num BIGINT`,
    `ALTER TABLE return_lines ADD COLUMN IF NOT EXISTS doc_date DATE`,
    `ALTER TABLE return_lines ADD COLUMN IF NOT EXISTS item_code TEXT`,
    `ALTER TABLE return_lines ADD COLUMN IF NOT EXISTS item_desc TEXT DEFAULT ''`,
    `ALTER TABLE return_lines ADD COLUMN IF NOT EXISTS qty NUMERIC(19,6) DEFAULT 0`,
    `ALTER TABLE return_lines ADD COLUMN IF NOT EXISTS price NUMERIC(19,6) DEFAULT 0`,
    `ALTER TABLE return_lines ADD COLUMN IF NOT EXISTS line_total NUMERIC(19,6) DEFAULT 0`,
    `ALTER TABLE return_lines ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()`,
  ];

  for (const q of alters) {
    try {
      await dbQuery(q);
    } catch {}
  }

  const indexes = [
    `CREATE UNIQUE INDEX IF NOT EXISTS idx_return_requests_req_num ON return_requests(req_num)`,
    `CREATE INDEX IF NOT EXISTS idx_returns_date ON return_requests(doc_date)`,
    `CREATE INDEX IF NOT EXISTS idx_returns_user ON return_requests(usuario)`,
    `CREATE INDEX IF NOT EXISTS idx_returns_wh ON return_requests(warehouse)`,
    `CREATE INDEX IF NOT EXISTS idx_returns_card ON return_requests(card_code)`,
    `CREATE INDEX IF NOT EXISTS idx_return_lines_req ON return_lines(req_num)`,
    `CREATE INDEX IF NOT EXISTS idx_quotes_cache_date ON quotes_cache(doc_date)`,
    `CREATE INDEX IF NOT EXISTS idx_quotes_cache_user ON quotes_cache(usuario)`,
    `CREATE INDEX IF NOT EXISTS idx_quotes_cache_wh ON quotes_cache(warehouse)`,
    `CREATE INDEX IF NOT EXISTS idx_quotes_cache_card ON quotes_cache(card_code)`,
    `CREATE INDEX IF NOT EXISTS idx_quote_lines_doc ON quote_lines_cache(doc_num)`,
  ];

  for (const q of indexes) {
    try {
      await dbQuery(q);
    } catch {}
  }
}

/* =========================================================
   Helpers generales
========================================================= */
const TZ_OFFSET_MIN = -300; // Panamá UTC-5

function safeJson(res, status, obj) {
  res.status(status).json(obj);
}

function parseCsvList(str) {
  return String(str || "")
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}

function parseCsvSet(str) {
  return new Set(parseCsvList(str).map((x) => x.toLowerCase()));
}

function parseCodesEnv(str) {
  return parseCsvList(str);
}

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function nowInOffsetMs(offsetMin = 0) {
  const now = new Date();
  return now.getTime() + now.getTimezoneOffset() * 60000 + Number(offsetMin) * 60000;
}

function getDateISOInOffset(offsetMin = 0) {
  const d = new Date(nowInOffsetMs(offsetMin));
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

function isoDateTimeInOffset(offsetMin = 0) {
  const d = new Date(nowInOffsetMs(offsetMin));
  return d.toISOString().replace("Z", "");
}

function addDaysISO(iso, days) {
  const d = new Date(String(iso || "").slice(0, 10) + "T00:00:00");
  if (Number.isNaN(d.getTime())) return "";
  d.setDate(d.getDate() + Number(days || 0));
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

function isISO(s) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(s || ""));
}

function truncate(s, max = 240) {
  const x = String(s || "").trim();
  return x.length > max ? x.slice(0, max) : x;
}

function quoteODataString(v) {
  return `'${String(v || "").replace(/'/g, "''")}'`;
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

function verifyUser(req, res, next) {
  const token = readBearer(req);
  if (!token) return safeJson(res, 401, { ok: false, message: "Missing Bearer token" });

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    if (decoded?.role !== "user") return safeJson(res, 403, { ok: false, message: "Forbidden" });
    req.user = decoded;
    next();
  } catch {
    return safeJson(res, 401, { ok: false, message: "Invalid token" });
  }
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

async function hashPin(pin) {
  return bcrypt.hash(String(pin), 10);
}

/* =========================================================
   Warehouse helpers
========================================================= */
function provinceToWarehouse(province) {
  const p = norm(province);
  if (p === "chiriqui" || p === "bocas del toro") return "200";
  if (p === "veraguas" || p === "cocle" || p === "los santos" || p === "herrera") return "500";
  if (p === "panama" || p === "panama oeste" || p === "colon") return "300";
  if (p === "rci") return "01";
  return SAP_WAREHOUSE || "300";
}

const ADMIN_FREE_WHS_SET = parseCsvSet(ADMIN_FREE_WHS_USERS);
const ALLOWED_BY_WH = {
  "200": parseCodesEnv(ACTIVE_CODES_200),
  "300": parseCodesEnv(ACTIVE_CODES_300),
  "500": parseCodesEnv(ACTIVE_CODES_500),
};

function canOverrideWarehouse(req) {
  const u = String(req.user?.username || "").trim().toLowerCase();
  return u && ADMIN_FREE_WHS_SET.has(u);
}

function getWarehouseFromUserToken(req) {
  const whToken = String(req.user?.warehouse_code || "").trim();
  if (whToken) return whToken;

  const prov = String(req.user?.province || "").trim();
  if (prov) return provinceToWarehouse(prov);

  return SAP_WAREHOUSE || "300";
}

function getRequestedWarehouse(req) {
  const fromBody = String(req.body?.whsCode || req.body?.WhsCode || req.body?.warehouse || "").trim();
  if (fromBody) return fromBody;

  const whQuery = String(req.query?.warehouse || req.query?.wh || "").trim();
  if (whQuery) return whQuery;

  const whHeader = String(req.headers["x-warehouse"] || "").trim();
  if (whHeader) return whHeader;

  return "";
}

function getWarehouseFromReq(req) {
  if (req.user && canOverrideWarehouse(req)) {
    const requested = getRequestedWarehouse(req);
    if (requested) return requested;
  }
  return getWarehouseFromUserToken(req);
}

function isRestrictedWarehouse(wh) {
  return wh === "200" || wh === "300" || wh === "500";
}

function getAllowedSetForWh(wh) {
  if (!isRestrictedWarehouse(wh)) return null;
  const arr = Array.isArray(ALLOWED_BY_WH[wh]) ? ALLOWED_BY_WH[wh] : [];
  return new Set(arr.map((x) => String(x).trim()));
}

function filterItemsByWarehouse(warehouseCode, rows) {
  const set = getAllowedSetForWh(warehouseCode);
  if (!set || set.size === 0) return rows;
  return (rows || []).filter((x) => set.has(String(x.ItemCode || "").trim()));
}

function assertItemAllowedOrThrow(wh, itemCode) {
  const code = String(itemCode || "").trim();
  if (!code) throw new Error("ItemCode vacío");

  if (!isRestrictedWarehouse(wh)) return true;

  const set = getAllowedSetForWh(wh);
  if (!set || set.size === 0) return true;

  if (!set.has(code)) {
    throw new Error(`ItemCode no permitido en bodega ${wh}: ${code}`);
  }
  return true;
}

function getDim1ForWh(warehouse) {
  const wh = String(warehouse || "").trim();
  const byWh = {
    "200": SAP_DIM1_200,
    "300": SAP_DIM1_300,
    "500": SAP_DIM1_500,
    "01": SAP_DIM1_01,
  };
  return String(byWh[wh] || SAP_DIM1_DEFAULT || "").trim();
}

function applyIfPresent(obj, fieldName, value) {
  const key = String(fieldName || "").trim();
  if (!key) return obj;
  obj[key] = value;
  return obj;
}

/* =========================================================
   SAP helpers
========================================================= */
function getSapServiceRoot() {
  let base = String(SAP_BASE_URL || "").trim().replace(/\/$/, "");
  if (!base) return "";
  if (!/\/b1s\/v[12]$/i.test(base)) base += "/b1s/v2";
  return base;
}

function missingSapEnv() {
  return !SAP_BASE_URL || !SAP_COMPANYDB || !SAP_USER || !SAP_PASS;
}

let SL_COOKIE = "";
let SL_COOKIE_AT = 0;
let SL_LOGIN_PROMISE = null;

function clearSlSession() {
  SL_COOKIE = "";
  SL_COOKIE_AT = 0;
}

function extractSapCookies(headers) {
  let cookieHeaders = [];

  if (typeof headers.getSetCookie === "function") {
    cookieHeaders = headers.getSetCookie();
  } else {
    const single = headers.get("set-cookie") || "";
    if (single) cookieHeaders = single.split(/,(?=\s*[A-Za-z0-9_\-]+=)/);
  }

  const wanted = [];
  for (const raw of cookieHeaders) {
    const firstPart = String(raw || "").split(";")[0].trim();
    if (/^(B1SESSION|ROUTEID)=/i.test(firstPart)) wanted.push(firstPart);
  }

  return wanted.join("; ");
}

async function slLogin(force = false) {
  if (SL_LOGIN_PROMISE && !force) return SL_LOGIN_PROMISE;

  SL_LOGIN_PROMISE = (async () => {
    const url = `${getSapServiceRoot()}/Login`;
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

    const cookie = extractSapCookies(r.headers);
    if (!cookie || !/B1SESSION=/i.test(cookie)) {
      throw new Error("SAP login failed: no se obtuvo cookie B1SESSION");
    }

    SL_COOKIE = cookie;
    SL_COOKIE_AT = Date.now();
    return true;
  })();

  try {
    return await SL_LOGIN_PROMISE;
  } finally {
    SL_LOGIN_PROMISE = null;
  }
}

function isSapNoMatchError(err) {
  const msg = String(err?.message || err || "").toLowerCase();
  return msg.includes("odbc -2028") || msg.includes("no matching records found");
}

function isSapAuthLikeError(err) {
  const msg = String(err?.message || err || "").toLowerCase();
  return (
    msg.includes("sap error 401") ||
    msg.includes("sap error 403") ||
    msg.includes("invalid session") ||
    msg.includes("session") ||
    isSapNoMatchError(err)
  );
}

async function slFetch(path, options = {}, allowAuthRetry = true) {
  if (missingSapEnv()) throw new Error("Missing SAP env");

  if (!SL_COOKIE || Date.now() - SL_COOKIE_AT > 25 * 60 * 1000) {
    await slLogin();
  }

  const base = getSapServiceRoot();
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
    const err = new Error(`SAP error ${r.status}: ${data?.error?.message?.value || txt}`);

    if (allowAuthRetry && (r.status === 401 || r.status === 403)) {
      clearSlSession();
      await slLogin(true);
      return slFetch(path, options, false);
    }

    throw err;
  }

  return data;
}

async function slFetchFreshSession(path, options = {}) {
  try {
    return await slFetch(path, options, true);
  } catch (err) {
    if (!isSapAuthLikeError(err)) throw err;
    clearSlSession();
    await slLogin(true);
    return slFetch(path, options, false);
  }
}

async function sapGetByDocEntry(entity, docEntry) {
  return slFetchFreshSession(`/${entity}(${encodeURIComponent(docEntry)})`);
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
   State helpers
========================================================= */
async function getState(k) {
  if (!hasDb()) return "";
  const r = await dbQuery(`SELECT v FROM app_state WHERE k=$1 LIMIT 1`, [k]);
  return r.rows?.[0]?.v || "";
}

async function setState(k, v) {
  if (!hasDb()) return;
  await dbQuery(
    `INSERT INTO app_state(k, v, updated_at)
     VALUES ($1,$2,NOW())
     ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v, updated_at=NOW()`,
    [k, String(v ?? "")]
  );
}

/* =========================================================
   Pedidos / cotizaciones helpers
========================================================= */
let PRICE_LIST_CACHE = { name: "", no: null, ts: 0 };
const PRICE_LIST_TTL_MS = 6 * 60 * 60 * 1000;
const ITEM_CACHE = new Map();
const ITEM_TTL_MS = 10 * 60 * 1000;

async function getPriceListNoByNameCached(name) {
  const now = Date.now();
  if (
    PRICE_LIST_CACHE.name === name &&
    PRICE_LIST_CACHE.no !== null &&
    now - PRICE_LIST_CACHE.ts < PRICE_LIST_TTL_MS
  ) {
    return PRICE_LIST_CACHE.no;
  }

  const safe = String(name || "").replace(/'/g, "''");
  let no = null;

  try {
    const r1 = await slFetch(`/PriceLists?$select=PriceListNo,PriceListName&$filter=PriceListName eq '${safe}'`);
    if (r1?.value?.length) no = r1.value[0].PriceListNo;
  } catch {}

  if (no === null) {
    try {
      const r2 = await slFetch(`/PriceLists?$select=PriceListNo,ListName&$filter=ListName eq '${safe}'`);
      if (r2?.value?.length) no = r2.value[0].PriceListNo;
    } catch {}
  }

  PRICE_LIST_CACHE = { name, no, ts: now };
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

function getSalesUomFactor(itemFull) {
  const directFields = [
    itemFull?.SalesItemsPerUnit,
    itemFull?.SalesQtyPerPackUnit,
    itemFull?.SalesQtyPerPackage,
    itemFull?.SalesPackagingUnit,
  ];

  for (const v of directFields) {
    const n = Number(v);
    if (Number.isFinite(n) && n > 0) return n;
  }

  const coll = itemFull?.ItemUnitOfMeasurementCollection;
  if (!Array.isArray(coll) || !coll.length) return null;

  let row =
    coll.find((x) => String(x?.UoMType || "").toLowerCase().includes("sales")) ||
    coll.find((x) => String(x?.UoMType || "").toLowerCase().includes("iut_sales")) ||
    null;

  if (!row) row = coll.find((x) => Number(x?.BaseQuantity) > 1) || null;
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

function buildItemResponse(itemFull, code, priceListNo, warehouseCode) {
  const item = {
    ItemCode: itemFull.ItemCode ?? code,
    ItemName: itemFull.ItemName ?? `Producto ${code}`,
    SalesUnit: itemFull.SalesUnit ?? "",
    InventoryItem: itemFull.InventoryItem ?? null,
    Valid: itemFull.Valid ?? null,
    FrozenFor: itemFull.FrozenFor ?? null,
  };

  const priceUnit = getPriceFromPriceList(itemFull, priceListNo);
  const factorCaja = getSalesUomFactor(itemFull);
  const priceCaja = priceUnit != null && factorCaja != null ? priceUnit * factorCaja : priceUnit;

  let warehouseRow = null;
  if (Array.isArray(itemFull?.ItemWarehouseInfoCollection)) {
    warehouseRow =
      itemFull.ItemWarehouseInfoCollection.find(
        (w) => String(w?.WarehouseCode || "").trim() === String(warehouseCode || "").trim()
      ) || null;
  }

  const onHand = warehouseRow?.InStock != null ? Number(warehouseRow.InStock) : null;
  const committed = warehouseRow?.Committed != null ? Number(warehouseRow.Committed) : null;
  const ordered = warehouseRow?.Ordered != null ? Number(warehouseRow.Ordered) : null;
  let available = null;
  if (Number.isFinite(onHand) && Number.isFinite(committed)) available = onHand - committed;

  return {
    item,
    price: priceCaja,
    priceUnit,
    factorCaja,
    stock: {
      warehouse: warehouseCode,
      onHand: Number.isFinite(onHand) ? onHand : null,
      committed: Number.isFinite(committed) ? committed : null,
      ordered: Number.isFinite(ordered) ? ordered : null,
      available: Number.isFinite(available) ? available : null,
      hasStock: available != null ? available > 0 : null,
    },
  };
}

async function getOneItem(code, priceListNo, warehouseCode) {
  const now = Date.now();
  const key = `${code}::${warehouseCode}::${priceListNo}`;
  const cached = ITEM_CACHE.get(key);
  if (cached && now - cached.ts < ITEM_TTL_MS) return cached.data;

  let itemFull;
  try {
    itemFull = await slFetch(
      `/Items('${encodeURIComponent(code)}')` +
        `?$select=ItemCode,ItemName,SalesUnit,InventoryItem,Valid,FrozenFor,ItemPrices,ItemWarehouseInfoCollection` +
        `&$expand=ItemUnitOfMeasurementCollection($select=UoMType,UoMCode,UoMEntry,BaseQuantity,AlternateQuantity)`
    );
  } catch {
    try {
      itemFull = await slFetch(
        `/Items('${encodeURIComponent(code)}')` +
          `?$select=ItemCode,ItemName,SalesUnit,InventoryItem,Valid,FrozenFor,ItemPrices,ItemWarehouseInfoCollection`
      );
    } catch {
      itemFull = await slFetch(`/Items('${encodeURIComponent(code)}')`);
    }
  }

  if (!Array.isArray(itemFull?.ItemWarehouseInfoCollection)) {
    try {
      const whInfo = await slFetch(
        `/Items('${encodeURIComponent(code)}')/ItemWarehouseInfoCollection?$select=WarehouseCode,InStock,Committed,Ordered`
      );
      if (Array.isArray(whInfo?.value)) itemFull.ItemWarehouseInfoCollection = whInfo.value;
    } catch {}
  }

  const data = buildItemResponse(itemFull, code, priceListNo, warehouseCode);
  ITEM_CACHE.set(key, { ts: now, data });
  return data;
}

/* =========================================================
   Quotes cache helpers
========================================================= */
function docDateTimeToMs(docDate, docTime) {
  const d = String(docDate || "").slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(d)) return 0;
  const t = Number(docTime || 0);
  const hh = Math.floor(t / 100);
  const mm = t % 100;
  const iso = `${d}T${String(hh).padStart(2, "0")}:${String(mm).padStart(2, "0")}:00`;
  const dt = new Date(iso + "Z");
  const msLocalAsUtc = dt.getTime();
  return msLocalAsUtc - TZ_OFFSET_MIN * 60000;
}

async function upsertQuoteCache(row) {
  const q = row || {};
  await dbQuery(
    `INSERT INTO quotes_cache(
      doc_num, doc_entry, doc_date, doc_time, card_code, card_name,
      usuario, warehouse, doc_total, delivered_total,
      status, cancel_status, comments, group_name, updated_at
     ) VALUES(
      $1,$2,$3,$4,$5,$6,
      $7,$8,$9,$10,
      $11,$12,$13,$14,NOW()
     )
     ON CONFLICT (doc_num) DO UPDATE SET
      doc_entry=EXCLUDED.doc_entry,
      doc_date=EXCLUDED.doc_date,
      doc_time=EXCLUDED.doc_time,
      card_code=EXCLUDED.card_code,
      card_name=EXCLUDED.card_name,
      usuario=EXCLUDED.usuario,
      warehouse=EXCLUDED.warehouse,
      doc_total=EXCLUDED.doc_total,
      delivered_total=EXCLUDED.delivered_total,
      status=EXCLUDED.status,
      cancel_status=EXCLUDED.cancel_status,
      comments=EXCLUDED.comments,
      group_name=COALESCE(EXCLUDED.group_name, quotes_cache.group_name),
      updated_at=NOW()`,
    [
      Number(q.docNum),
      Number(q.docEntry || 0) || null,
      String(q.docDate || "").slice(0, 10) || null,
      Number(q.docTime || 0) || 0,
      String(q.cardCode || ""),
      String(q.cardName || ""),
      String(q.usuario || ""),
      String(q.warehouse || ""),
      Number(q.docTotal || 0),
      Number(q.deliveredTotal || 0),
      String(q.status || ""),
      String(q.cancelStatus || ""),
      String(q.comments || ""),
      q.groupName ? String(q.groupName) : null,
    ]
  );
}

async function upsertQuoteLineCache(row) {
  const r = row || {};
  await dbQuery(
    `INSERT INTO quote_lines_cache(
      doc_num, doc_date, item_code, item_desc,
      qty_quoted, qty_delivered, dollars_quoted, dollars_delivered, updated_at
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())
     ON CONFLICT (doc_num, item_code) DO UPDATE SET
      doc_date=EXCLUDED.doc_date,
      item_desc=EXCLUDED.item_desc,
      qty_quoted=EXCLUDED.qty_quoted,
      qty_delivered=EXCLUDED.qty_delivered,
      dollars_quoted=EXCLUDED.dollars_quoted,
      dollars_delivered=EXCLUDED.dollars_delivered,
      updated_at=NOW()`,
    [
      Number(r.docNum || 0),
      r.docDate ? String(r.docDate).slice(0, 10) : null,
      String(r.itemCode || ""),
      String(r.itemDesc || ""),
      Number(r.qtyQuoted || 0),
      Number(r.qtyDelivered || 0),
      Number(r.dollarsQuoted || 0),
      Number(r.dollarsDelivered || 0),
    ]
  );
}

/* =========================================================
   Returns cache helpers
========================================================= */
async function upsertReturnRequestCache(row) {
  const r = row || {};
  await dbQuery(
    `INSERT INTO return_requests(
      req_num, req_entry, doc_date, doc_time, card_code, card_name,
      usuario, warehouse, motivo, causa, total_amount, total_qty,
      status, comments, updated_at
     ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,NOW())
     ON CONFLICT (req_num) DO UPDATE SET
      req_entry=EXCLUDED.req_entry,
      doc_date=EXCLUDED.doc_date,
      doc_time=EXCLUDED.doc_time,
      card_code=EXCLUDED.card_code,
      card_name=EXCLUDED.card_name,
      usuario=EXCLUDED.usuario,
      warehouse=EXCLUDED.warehouse,
      motivo=EXCLUDED.motivo,
      causa=EXCLUDED.causa,
      total_amount=EXCLUDED.total_amount,
      total_qty=EXCLUDED.total_qty,
      status=EXCLUDED.status,
      comments=EXCLUDED.comments,
      updated_at=NOW()`,
    [
      Number(r.reqNum || 0),
      Number(r.reqEntry || 0) || null,
      r.docDate ? String(r.docDate).slice(0, 10) : null,
      Number(r.docTime || 0) || 0,
      String(r.cardCode || ""),
      String(r.cardName || ""),
      String(r.usuario || ""),
      String(r.warehouse || ""),
      String(r.motivo || ""),
      String(r.causa || ""),
      Number(r.totalAmount || 0),
      Number(r.totalQty || 0),
      String(r.status || "Open"),
      String(r.comments || ""),
    ]
  );
}

async function upsertReturnLineCache(row) {
  const r = row || {};
  await dbQuery(
    `INSERT INTO return_lines(
      req_num, doc_date, item_code, item_desc, qty, price, line_total, updated_at
     ) VALUES($1,$2,$3,$4,$5,$6,$7,NOW())
     ON CONFLICT (req_num, item_code) DO UPDATE SET
      doc_date=EXCLUDED.doc_date,
      item_desc=EXCLUDED.item_desc,
      qty=EXCLUDED.qty,
      price=EXCLUDED.price,
      line_total=EXCLUDED.line_total,
      updated_at=NOW()`,
    [
      Number(r.reqNum || 0),
      r.docDate ? String(r.docDate).slice(0, 10) : null,
      String(r.itemCode || ""),
      String(r.itemDesc || ""),
      Number(r.qty || 0),
      Number(r.price || 0),
      Number(r.lineTotal || 0),
    ]
  );
}

/* =========================================================
   Auth routes usuario
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
    if (!u) return safeJson(res, 401, { ok: false, message: "Credenciales inválidas" });
    if (!u.is_active) return safeJson(res, 403, { ok: false, message: "Usuario inactivo" });

    const ok = await bcrypt.compare(pin, u.pin_hash);
    if (!ok) return safeJson(res, 401, { ok: false, message: "Credenciales inválidas" });

    let wh = String(u.warehouse_code || "").trim();
    if (!wh) {
      wh = provinceToWarehouse(u.province || "");
      try {
        await dbQuery(`UPDATE app_users SET warehouse_code=$1 WHERE id=$2`, [wh, u.id]);
        u.warehouse_code = wh;
      } catch {}
    }

    const token = signUserToken(u, "30d");
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
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
}

app.post("/api/login", handleUserLogin);
app.post("/api/auth/login", handleUserLogin);
app.get("/api/me", verifyUser, async (req, res) => safeJson(res, 200, { ok: true, user: req.user }));
app.get("/api/auth/me", verifyUser, async (req, res) => safeJson(res, 200, { ok: true, user: req.user }));

/* =========================================================
   Meta / health
========================================================= */
app.get("/api/health", async (req, res) => {
  safeJson(res, 200, {
    ok: true,
    app: "prodima-unificado-api",
    message: "✅ PRODIMA API unificada activa",
    db: hasDb() ? "on" : "off",
    sap: missingSapEnv() ? "missing" : "ok",
    yappy: YAPPY_ALIAS,
    warehouse_default: SAP_WAREHOUSE,
    priceList: SAP_PRICE_LIST,
    return_entity: SAP_RETURN_ENTITY,
    dim1_default_set: !!String(SAP_DIM1_DEFAULT || "").trim(),
    modules: {
      pedidos: true,
      cotizaciones: true,
      devoluciones: true,
      admin: true,
      quotes_cache: true,
    },
  });
});

app.get("/api/meta", verifyUser, async (req, res) => {
  return safeJson(res, 200, {
    ok: true,
    motivos: parseCsvList(RETURN_MOTIVOS),
    causas: parseCsvList(RETURN_CAUSAS),
  });
});

app.get("/api/returns/options", verifyUser, async (req, res) => {
  return safeJson(res, 200, {
    ok: true,
    motivos: parseCsvList(RETURN_MOTIVOS),
    causas: parseCsvList(RETURN_CAUSAS),
  });
});

/* =========================================================
   SAP endpoints usuario: items / clientes / cotización
========================================================= */
app.get("/api/sap/allowed-items", verifyUser, async (req, res) => {
  const wh = getWarehouseFromReq(req);
  const set = getAllowedSetForWh(wh);
  const list = set ? Array.from(set) : [];
  return res.json({ ok: true, warehouse: wh, allowedCount: list.length, allowed: list });
});

app.get("/api/sap/warehouses", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) {
      const fb = parseCsvList(WAREHOUSE_FALLBACK || "300");
      return res.json({ ok: true, warehouses: fb.map((w) => ({ WarehouseCode: w })) });
    }

    let raw;
    try {
      raw = await slFetch(`/Warehouses?$select=WarehouseCode,WarehouseName&$orderby=WarehouseCode asc&$top=200`);
    } catch {
      raw = await slFetch(`/Warehouses?$select=WhsCode,WhsName&$orderby=WhsCode asc&$top=200`);
    }

    const values = Array.isArray(raw?.value) ? raw.value : [];
    const warehouses = values
      .map((w) => ({
        WarehouseCode: w?.WarehouseCode || w?.WhsCode || w?.whsCode || w?.Code || "",
        WarehouseName: w?.WarehouseName || w?.WhsName || w?.whsName || w?.Name || "",
      }))
      .filter((w) => String(w.WarehouseCode || "").trim());

    if (!warehouses.length) {
      const fb = parseCsvList(WAREHOUSE_FALLBACK || "300");
      return res.json({ ok: true, warehouses: fb.map((w) => ({ WarehouseCode: w, WarehouseName: "" })) });
    }

    return res.json({ ok: true, warehouses });
  } catch (err) {
    return res.status(500).json({ ok: false, message: String(err.message || err) });
  }
});

app.get("/api/sap/item/:code", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const code = String(req.params.code || "").trim();
    if (!code) return res.status(400).json({ ok: false, message: "ItemCode vacío." });

    const warehouseCode = getWarehouseFromReq(req);
    assertItemAllowedOrThrow(warehouseCode, code);

    const priceListNo = await getPriceListNoByNameCached(SAP_PRICE_LIST);
    const r = await getOneItem(code, priceListNo, warehouseCode);

    return res.json({
      ok: true,
      item: r.item,
      warehouse: warehouseCode,
      bodega: warehouseCode,
      priceList: SAP_PRICE_LIST,
      priceListNo,
      price: Number(r.price ?? 0),
      priceUnit: r.priceUnit,
      factorCaja: r.factorCaja,
      uom: "Caja",
      stock: r.stock,
      disponible: r?.stock?.available ?? null,
      enStock: r?.stock?.hasStock ?? null,
    });
  } catch (err) {
    return res.status(400).json({ ok: false, message: err.message || String(err) });
  }
});

app.get("/api/sap/items", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const codes = String(req.query.codes || "")
      .split(",")
      .map((x) => x.trim())
      .filter(Boolean);

    if (!codes.length) return res.status(400).json({ ok: false, message: "codes vacío" });

    const warehouseCode = getWarehouseFromReq(req);
    for (const c of codes) assertItemAllowedOrThrow(warehouseCode, c);

    const priceListNo = await getPriceListNoByNameCached(SAP_PRICE_LIST);
    const items = {};
    let i = 0;
    const CONCURRENCY = 5;

    async function worker() {
      while (i < codes.length) {
        const idx = i++;
        const code = codes[idx];
        try {
          const r = await getOneItem(code, priceListNo, warehouseCode);
          items[code] = {
            ok: true,
            name: r.item.ItemName,
            unit: "Caja",
            price: r.price,
            priceUnit: r.priceUnit,
            factorCaja: r.factorCaja,
            stock: r.stock,
            disponible: r?.stock?.available ?? null,
            enStock: r?.stock?.hasStock ?? null,
          };
        } catch (e) {
          items[code] = { ok: false, message: String(e.message || e) };
        }
      }
    }

    await Promise.all(Array.from({ length: CONCURRENCY }, worker));

    return res.json({
      ok: true,
      warehouse: warehouseCode,
      bodega: warehouseCode,
      priceList: SAP_PRICE_LIST,
      priceListNo,
      items,
    });
  } catch (err) {
    return res.status(500).json({ ok: false, message: err.message || String(err) });
  }
});

app.get("/api/sap/items/search", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const q = String(req.query?.q || "").trim();
    const top = Math.min(Math.max(Number(req.query?.top || 15), 5), 50);
    if (q.length < 2) return res.json({ ok: true, q, results: [] });

    const warehouseCode = getWarehouseFromReq(req);
    const safe = q.replace(/'/g, "''");
    const preTop = Math.min(100, top * 5);

    let raw;
    try {
      raw = await slFetch(
        `/Items?$select=ItemCode,ItemName,SalesUnit,Valid,FrozenFor` +
          `&$filter=${encodeURIComponent(`(contains(ItemCode,'${safe}') or contains(ItemName,'${safe}'))`)}` +
          `&$orderby=ItemName asc&$top=${preTop}`
      );
    } catch {
      raw = await slFetch(
        `/Items?$select=ItemCode,ItemName,SalesUnit,Valid,FrozenFor` +
          `&$filter=${encodeURIComponent(`(substringof('${safe}',ItemCode) or substringof('${safe}',ItemName))`)}` +
          `&$orderby=ItemName asc&$top=${preTop}`
      );
    }

    const values = Array.isArray(raw?.value) ? raw.value : [];
    let filtered = values.filter((it) => it?.ItemCode);

    filtered = filtered.filter((it) => {
      const v = String(it?.Valid ?? "").toLowerCase();
      const f = String(it?.FrozenFor ?? "").toLowerCase();
      const validOk = !v || v.includes("tyes") || v === "yes" || v === "true";
      const frozenOk = !f || f.includes("tno") || f === "no" || f === "false";
      return validOk && frozenOk;
    });

    filtered = filterItemsByWarehouse(warehouseCode, filtered);

    const results = filtered.slice(0, top).map((it) => ({
      ItemCode: it.ItemCode,
      ItemName: it.ItemName,
      SalesUnit: "Caja",
    }));

    return res.json({ ok: true, q, warehouse: warehouseCode, results });
  } catch (err) {
    return res.status(500).json({ ok: false, message: err.message || String(err) });
  }
});

app.get("/api/sap/customers/search", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const q = String(req.query?.q || "").trim();
    const top = Math.min(Math.max(Number(req.query?.top || 15), 5), 50);
    if (q.length < 2) return res.json({ ok: true, results: [] });

    const safe = q.replace(/'/g, "''");
    let r;
    try {
      r = await slFetch(
        `/BusinessPartners?$select=CardCode,CardName,Phone1,EmailAddress&$filter=contains(CardName,'${safe}') or contains(CardCode,'${safe}')&$orderby=CardName asc&$top=${top}`
      );
    } catch {
      r = await slFetch(
        `/BusinessPartners?$select=CardCode,CardName,Phone1,EmailAddress&$filter=substringof('${safe}',CardName) or substringof('${safe}',CardCode)&$orderby=CardName asc&$top=${top}`
      );
    }

    const values = Array.isArray(r?.value) ? r.value : [];
    const results = values.map((x) => ({
      CardCode: x.CardCode,
      CardName: x.CardName,
      Phone1: x.Phone1 || "",
      EmailAddress: x.EmailAddress || "",
    }));

    return res.json({ ok: true, q, results });
  } catch (err) {
    return res.status(500).json({ ok: false, message: err.message || String(err) });
  }
});

app.get("/api/sap/customer/:code", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const code = String(req.params.code || "").trim();
    if (!code) return res.status(400).json({ ok: false, message: "CardCode vacío." });

    const bp = await slFetch(
      `/BusinessPartners('${encodeURIComponent(code)}')?$select=CardCode,CardName,Phone1,Phone2,EmailAddress,Address,City,Country,ZipCode`
    );

    const addrParts = [bp.Address, bp.City, bp.ZipCode, bp.Country].filter(Boolean).join(", ");

    return res.json({
      ok: true,
      customer: {
        CardCode: bp.CardCode,
        CardName: bp.CardName,
        Phone1: bp.Phone1,
        Phone2: bp.Phone2,
        EmailAddress: bp.EmailAddress,
        Address: addrParts || bp.Address || "",
      },
    });
  } catch (err) {
    return res.status(500).json({ ok: false, message: err.message || String(err) });
  }
});

app.post("/api/sap/quote", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const cardCode = String(req.body?.cardCode || "").trim();
    const lines = Array.isArray(req.body?.lines) ? req.body.lines : [];

    if (!cardCode) return res.status(400).json({ ok: false, message: "cardCode requerido." });
    if (!lines.length) return res.status(400).json({ ok: false, message: "lines requerido." });

    const warehouseCode = getWarehouseFromReq(req);
    const cleanLines = lines
      .map((l) => ({ ItemCode: String(l.itemCode || "").trim(), Quantity: Number(l.qty || 0) }))
      .filter((x) => x.ItemCode && x.Quantity > 0);

    if (!cleanLines.length) {
      return res.status(400).json({ ok: false, message: "No hay líneas válidas (qty>0)." });
    }

    for (const ln of cleanLines) assertItemAllowedOrThrow(warehouseCode, ln.ItemCode);

    const bp = await slFetchFreshSession(`/BusinessPartners('${encodeURIComponent(cardCode)}')?$select=CardCode,CardName`);

    const docDate = getDateISOInOffset(TZ_OFFSET_MIN);
    const creator = req.user?.username || "unknown";
    const baseComments = [`[user:${creator}]`, `[wh:${warehouseCode}]`].join(" ");

    const payload = {
      CardCode: cardCode,
      DocDate: docDate,
      DocDueDate: docDate,
      Comments: baseComments,
      JournalMemo: "Cotización web mercaderistas",
      DocumentLines: cleanLines.map((ln) => ({
        ItemCode: ln.ItemCode,
        Quantity: ln.Quantity,
        WarehouseCode: warehouseCode,
      })),
    };

    async function createQuotation(body) {
      return slFetchFreshSession(`/Quotations`, {
        method: "POST",
        body: JSON.stringify(body),
      });
    }

    try {
      const created = await createQuotation(payload);

      let mailResult = { ok: false, skipped: true, message: "" };
      try {
        mailResult = await sendDocumentEmailViaGAS({
          event: "quote_created",
          notifyTo: DOCS_NOTIFY_TO,
          attachments: req.body?.attachments,
          data: {
            kind: "quote",
            docNum: created.DocNum,
            docEntry: created.DocEntry,
            docDate,
            warehouse: warehouseCode,
            createdBy: creator,
            cardCode,
            cardName: String(bp?.CardName || ""),
            comments: String(req.body?.comments || "").trim(),
            lines: cleanLines.map((ln) => ({ itemCode: ln.ItemCode, qty: ln.Quantity })),
          },
        });
      } catch (mailErr) {
        mailResult = { ok: false, skipped: false, message: String(mailErr?.message || mailErr) };
        console.error("quote email error:", mailResult.message);
      }

      return res.json({
        ok: true,
        message: "Cotización creada",
        docEntry: created.DocEntry,
        docNum: created.DocNum,
        warehouse: warehouseCode,
        bodega: warehouseCode,
        fallback: false,
        mailSent: !!mailResult?.ok,
        mailMessage: mailResult?.message || "",
      });
    } catch (err1) {
      if (!isSapNoMatchError(err1)) throw err1;

      const payloadFallback = {
        ...payload,
        Comments: `${baseComments} [wh_fallback:1]`,
        DocumentLines: cleanLines.map((ln) => ({
          ItemCode: ln.ItemCode,
          Quantity: ln.Quantity,
        })),
      };

      const created2 = await createQuotation(payloadFallback);

      let mailResult = { ok: false, skipped: true, message: "" };
      try {
        mailResult = await sendDocumentEmailViaGAS({
          event: "quote_created",
          notifyTo: DOCS_NOTIFY_TO,
          attachments: req.body?.attachments,
          data: {
            kind: "quote",
            docNum: created2.DocNum,
            docEntry: created2.DocEntry,
            docDate,
            warehouse: warehouseCode,
            createdBy: creator,
            cardCode,
            cardName: String(bp?.CardName || ""),
            comments: String(req.body?.comments || "").trim(),
            lines: cleanLines.map((ln) => ({ itemCode: ln.ItemCode, qty: ln.Quantity })),
          },
        });
      } catch (mailErr) {
        mailResult = { ok: false, skipped: false, message: String(mailErr?.message || mailErr) };
        console.error("quote fallback email error:", mailResult.message);
      }

      return res.json({
        ok: true,
        message: "Cotización creada (fallback sin WarehouseCode por -2028)",
        docEntry: created2.DocEntry,
        docNum: created2.DocNum,
        warehouse: warehouseCode,
        bodega: warehouseCode,
        fallback: true,
        mailSent: !!mailResult?.ok,
        mailMessage: mailResult?.message || "",
      });
    }
  } catch (err) {
    const msg = String(err?.message || err);
    const isAllow = msg.toLowerCase().includes("no permitido");
    return res.status(isAllow ? 400 : 500).json({ ok: false, message: msg });
  }
});

/* =========================================================
   Devoluciones usuario
========================================================= */
async function createReturnRequestHandler(req, res) {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const cardCode = String(req.body?.cardCode || "").trim();
    const cardName = String(req.body?.cardName || "").trim();
    const motivo = String(req.body?.motivo || "").trim();
    const causa = String(req.body?.causa || "").trim();
    const extraComments = String(req.body?.comments || "").trim();
    const lines = Array.isArray(req.body?.lines) ? req.body.lines : [];

    if (!cardCode) return res.status(400).json({ ok: false, message: "cardCode requerido." });
    if (!motivo) return res.status(400).json({ ok: false, message: "Motivo requerido." });
    if (!causa) return res.status(400).json({ ok: false, message: "Causa requerida." });
    if (!lines.length) return res.status(400).json({ ok: false, message: "lines requerido." });

    const warehouseCode = getWarehouseFromReq(req);
    const cleanLines = lines
      .map((l) => ({
        ItemCode: String(l.itemCode || "").trim(),
        ItemDescription: String(l.itemDesc || l.itemDescription || "").trim(),
        Quantity: Number(l.qty || 0),
        Price: Number(l.price || 0),
      }))
      .filter((x) => x.ItemCode && x.Quantity > 0);

    if (!cleanLines.length) {
      return res.status(400).json({ ok: false, message: "No hay líneas válidas (qty>0)." });
    }

    for (const ln of cleanLines) assertItemAllowedOrThrow(warehouseCode, ln.ItemCode);

    const dim1 = getDim1ForWh(warehouseCode);
    if (!dim1) {
      return res.status(400).json({
        ok: false,
        message:
          "SAP requiere Dimensión 1 (Distribution Rule) para esa cuenta. Configura SAP_DIM1_DEFAULT o SAP_DIM1_200/300/500/01 con un código válido.",
      });
    }

    const bp = await slFetchFreshSession(`/BusinessPartners('${encodeURIComponent(cardCode)}')?$select=CardCode,CardName`);

    const docDate = getDateISOInOffset(TZ_OFFSET_MIN);
    const creator = req.user?.username || "unknown";
    const comments = truncate(
      [`[user:${creator}]`, `[wh:${warehouseCode}]`, `[motivo:${motivo}]`, `[causa:${causa}]`, extraComments].filter(Boolean).join(" "),
      240
    );

    const payload = {
      CardCode: cardCode,
      DocDate: docDate,
      Comments: comments,
      JournalMemo: `Devolución web ${motivo}`,
      DocumentLines: cleanLines.map((ln) => {
        const line = {
          ItemCode: ln.ItemCode,
          ItemDescription: ln.ItemDescription,
          Quantity: ln.Quantity,
          UnitPrice: ln.Price,
          Price: ln.Price,
          WarehouseCode: warehouseCode,
          CostingCode: dim1,
          COGSCostingCode: dim1,
        };

        applyIfPresent(line, SAP_RETURN_LINE_MOTIVO_FIELD, motivo);
        applyIfPresent(line, SAP_RETURN_LINE_CAUSA_FIELD, causa);
        return line;
      }),
    };

    applyIfPresent(payload, SAP_RETURN_HEADER_MOTIVO_FIELD, motivo);
    applyIfPresent(payload, SAP_RETURN_HEADER_CAUSA_FIELD, causa);

    const entity = `/${String(SAP_RETURN_ENTITY || "Returns").replace(/^\/+/, "")}`;
    const created = await slFetchFreshSession(entity, {
      method: "POST",
      body: JSON.stringify(payload),
    });

    const reqNum = Number(created.DocNum || created.ReqNum || created.RequestNo || created.DocEntry || 0);
    const reqEntry = Number(created.DocEntry || created.DocNum || 0);
    const totalQty = cleanLines.reduce((acc, x) => acc + Number(x.Quantity || 0), 0);
    const totalAmount = cleanLines.reduce((acc, x) => acc + Number(x.Quantity || 0) * Number(x.Price || 0), 0);

    if (hasDb() && reqNum > 0) {
      await upsertReturnRequestCache({
        reqNum,
        reqEntry,
        docDate,
        docTime: 0,
        cardCode,
        cardName,
        usuario: creator,
        warehouse: warehouseCode,
        motivo,
        causa,
        totalAmount,
        totalQty,
        status: String(created.DocumentStatus || created.Status || "Open"),
        comments,
      });

      for (const ln of cleanLines) {
        await upsertReturnLineCache({
          reqNum,
          docDate,
          itemCode: ln.ItemCode,
          itemDesc: ln.ItemDescription,
          qty: ln.Quantity,
          price: ln.Price,
          lineTotal: Number(ln.Quantity || 0) * Number(ln.Price || 0),
        });
      }
    }

    let mailResult = { ok: false, skipped: true, message: "" };
    try {
      mailResult = await sendDocumentEmailViaGAS({
        event: "return_created",
        notifyTo: DOCS_NOTIFY_TO,
        attachments: req.body?.attachments,
        data: {
          kind: "return",
          reqNum,
          reqEntry,
          docDate,
          warehouse: warehouseCode,
          createdBy: creator,
          cardCode,
          cardName: String(bp?.CardName || cardName || ""),
          motivo,
          causa,
          comments: extraComments,
          totalQty,
          totalAmount,
          lines: cleanLines.map((ln) => ({
            itemCode: ln.ItemCode,
            itemDesc: ln.ItemDescription,
            qty: ln.Quantity,
            price: ln.Price,
          })),
        },
      });
    } catch (mailErr) {
      mailResult = { ok: false, skipped: false, message: String(mailErr?.message || mailErr) };
      console.error("return email error:", mailResult.message);
    }

    return res.json({
      ok: true,
      message: "Solicitud de devolución creada",
      reqNum,
      reqEntry,
      warehouse: warehouseCode,
      bodega: warehouseCode,
      entity: String(SAP_RETURN_ENTITY || "Returns"),
      mailSent: !!mailResult?.ok,
      mailMessage: mailResult?.message || "",
    });
  } catch (err) {
    return res.status(500).json({ ok: false, message: String(err?.message || err) + " | " + DOCS_MAIL_BUILD });
  }
}

app.post("/api/returns", verifyUser, createReturnRequestHandler);
app.post("/api/returns/create", verifyUser, createReturnRequestHandler);
app.post("/api/sap/returns", verifyUser, createReturnRequestHandler);
app.post("/api/sap/return-request", verifyUser, createReturnRequestHandler);

/* =========================================================
   Admin auth y usuarios
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

app.get("/api/admin/meta", verifyAdmin, async (req, res) => {
  return safeJson(res, 200, {
    ok: true,
    motivos: parseCsvList(RETURN_MOTIVOS),
    causas: parseCsvList(RETURN_CAUSAS),
  });
});

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
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.post("/api/admin/users", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const username = String(req.body?.username || "").trim().toLowerCase();
    const full_name = String(req.body?.full_name ?? req.body?.fullName ?? "").trim();
    const pin = String(req.body?.pin || "").trim();
    const province = String(req.body?.province || "").trim();

    if (!username) return safeJson(res, 400, { ok: false, message: "Username requerido" });
    if (!full_name) return safeJson(res, 400, { ok: false, message: "Nombre requerido" });
    if (!pin || pin.length < 4) return safeJson(res, 400, { ok: false, message: "PIN mínimo 4" });
    if (!province) return safeJson(res, 400, { ok: false, message: "Provincia requerida" });

    const warehouse_code = provinceToWarehouse(province);
    const pin_hash = await hashPin(pin);

    const r = await dbQuery(
      `INSERT INTO app_users(username, full_name, pin_hash, province, warehouse_code, is_active)
       VALUES ($1,$2,$3,$4,$5,TRUE)
       RETURNING id, username, full_name, province, warehouse_code, is_active, created_at`,
      [username, full_name, pin_hash, province, warehouse_code]
    );

    return safeJson(res, 200, { ok: true, user: r.rows[0] });
  } catch (e) {
    if (String(e?.code) === "23505") {
      return safeJson(res, 409, { ok: false, message: "Ese username ya existe" });
    }
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.patch("/api/admin/users/:id/toggle", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });
    const id = Number(req.params.id || 0);
    if (!Number.isFinite(id) || id <= 0) return safeJson(res, 400, { ok: false, message: "ID inválido" });

    const r = await dbQuery(
      `UPDATE app_users
       SET is_active = NOT is_active
       WHERE id = $1
       RETURNING id, username, full_name, province, warehouse_code, is_active, created_at`,
      [id]
    );

    if (!r.rowCount) return safeJson(res, 404, { ok: false, message: "Usuario no encontrado" });
    return safeJson(res, 200, { ok: true, user: r.rows[0] });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.delete("/api/admin/users/:id", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });
    const id = Number(req.params.id || 0);
    if (!Number.isFinite(id) || id <= 0) return safeJson(res, 400, { ok: false, message: "ID inválido" });
    const r = await dbQuery(`DELETE FROM app_users WHERE id=$1`, [id]);
    if (!r.rowCount) return safeJson(res, 404, { ok: false, message: "Usuario no encontrado" });
    return safeJson(res, 200, { ok: true });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.patch("/api/admin/users/:id/pin", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });
    const id = Number(req.params.id || 0);
    const pin = String(req.body?.pin || "").trim();
    if (!Number.isFinite(id) || id <= 0) return safeJson(res, 400, { ok: false, message: "ID inválido" });
    if (!pin || pin.length < 4) return safeJson(res, 400, { ok: false, message: "PIN mínimo 4" });
    const pin_hash = await hashPin(pin);
    const r = await dbQuery(`UPDATE app_users SET pin_hash=$2 WHERE id=$1`, [id, pin_hash]);
    if (!r.rowCount) return safeJson(res, 404, { ok: false, message: "Usuario no encontrado" });
    return safeJson(res, 200, { ok: true });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================================================
   Admin devoluciones DB
========================================================= */
app.get("/api/admin/returns/dashboard-db", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const fromQ = String(req.query?.from || "").slice(0, 10);
    const toQ = String(req.query?.to || "").slice(0, 10);
    const motivo = String(req.query?.motivo || "").trim();
    const onlyCreated = String(req.query?.onlyCreated || "0") === "1";

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const fromDef = `${today.slice(0, 7)}-01`;
    const toDef = today;

    const from = isISO(fromQ) ? fromQ : fromDef;
    const to = isISO(toQ) ? toQ : toDef;

    const where = [];
    const params = [];
    let p = 1;

    where.push(`r.doc_date BETWEEN $${p++} AND $${p++}`);
    params.push(from, to);

    if (onlyCreated) {
      where.push(`LOWER(COALESCE(r.usuario,'')) IN (SELECT LOWER(username) FROM app_users WHERE is_active=TRUE)`);
    }
    if (motivo && motivo !== "__ALL__") {
      where.push(`r.motivo = $${p++}`);
      params.push(motivo);
    }

    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

    const totalsR = await dbQuery(
      `SELECT
        COUNT(*)::int AS requests,
        COALESCE(SUM(r.total_amount),0)::float AS total_amount,
        COALESCE(SUM(r.total_qty),0)::float AS total_qty,
        CASE WHEN COUNT(*)>0 THEN (COALESCE(SUM(r.total_amount),0) / COUNT(*))::float ELSE 0 END AS avg_amount
       FROM return_requests r
       ${whereSql}`,
      params
    );
    const totals = totalsR.rows?.[0] || { requests: 0, total_amount: 0, total_qty: 0, avg_amount: 0 };

    const byStatusR = await dbQuery(
      `SELECT COALESCE(NULLIF(r.status,''),'Open') AS status,
              COUNT(*)::int AS cnt,
              COALESCE(SUM(r.total_amount),0)::float AS amount
       FROM return_requests r
       ${whereSql}
       GROUP BY 1
       ORDER BY amount DESC`,
      params
    );

    const byMotivoR = await dbQuery(
      `SELECT COALESCE(NULLIF(r.motivo,''),'Sin motivo') AS motivo,
              COUNT(*)::int AS cnt,
              COALESCE(SUM(r.total_amount),0)::float AS amount
       FROM return_requests r
       ${whereSql}
       GROUP BY 1
       ORDER BY amount DESC
       LIMIT 2000`,
      params
    );

    const byCausaR = await dbQuery(
      `SELECT COALESCE(NULLIF(r.causa,''),'Sin causa') AS causa,
              COUNT(*)::int AS cnt,
              COALESCE(SUM(r.total_amount),0)::float AS amount
       FROM return_requests r
       ${whereSql}
       GROUP BY 1
       ORDER BY amount DESC
       LIMIT 2000`,
      params
    );

    const byUserR = await dbQuery(
      `SELECT COALESCE(NULLIF(r.usuario,''),'sin_user') AS usuario,
              COUNT(*)::int AS cnt,
              COALESCE(SUM(r.total_amount),0)::float AS amount
       FROM return_requests r
       ${whereSql}
       GROUP BY 1
       ORDER BY amount DESC
       LIMIT 2000`,
      params
    );

    const byWhR = await dbQuery(
      `SELECT COALESCE(NULLIF(r.warehouse,''),'sin_wh') AS warehouse,
              COUNT(*)::int AS cnt,
              COALESCE(SUM(r.total_amount),0)::float AS amount
       FROM return_requests r
       ${whereSql}
       GROUP BY 1
       ORDER BY amount DESC
       LIMIT 2000`,
      params
    );

    const byClientR = await dbQuery(
      `SELECT COALESCE(NULLIF(r.card_name,''), r.card_code, 'sin_cliente') AS customer,
              COUNT(*)::int AS cnt,
              COALESCE(SUM(r.total_amount),0)::float AS amount
       FROM return_requests r
       ${whereSql}
       GROUP BY 1
       ORDER BY amount DESC
       LIMIT 2000`,
      params
    );

    return safeJson(res, 200, {
      ok: true,
      from,
      to,
      totals: {
        requests: Number(totals.requests || 0),
        totalAmount: Number(totals.total_amount || 0),
        totalQty: Number(totals.total_qty || 0),
        avgAmount: Number(totals.avg_amount || 0),
      },
      byStatus: (byStatusR.rows || []).map((x) => ({ status: x.status, cnt: Number(x.cnt || 0), amount: Number(x.amount || 0) })),
      byMotivo: (byMotivoR.rows || []).map((x) => ({ motivo: x.motivo, cnt: Number(x.cnt || 0), amount: Number(x.amount || 0) })),
      byCausa: (byCausaR.rows || []).map((x) => ({ causa: x.causa, cnt: Number(x.cnt || 0), amount: Number(x.amount || 0) })),
      byUser: (byUserR.rows || []).map((x) => ({ usuario: x.usuario, cnt: Number(x.cnt || 0), amount: Number(x.amount || 0) })),
      byWh: (byWhR.rows || []).map((x) => ({ warehouse: x.warehouse, cnt: Number(x.cnt || 0), amount: Number(x.amount || 0) })),
      byClient: (byClientR.rows || []).map((x) => ({ customer: x.customer, cnt: Number(x.cnt || 0), amount: Number(x.amount || 0) })),
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get("/api/admin/returns/db", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const fromQ = String(req.query?.from || "").slice(0, 10);
    const toQ = String(req.query?.to || "").slice(0, 10);
    const user = String(req.query?.user || "").trim().toLowerCase();
    const client = String(req.query?.client || "").trim().toLowerCase();
    const motivo = String(req.query?.motivo || "").trim();
    const causa = String(req.query?.causa || "").trim();
    const onlyCreated = String(req.query?.onlyCreated || "0") === "1";
    const openOnly = String(req.query?.openOnly || "0") === "1";
    const skip = Math.max(0, Math.trunc(Number(req.query?.skip || 0) || 0));
    const limit = Math.max(1, Math.min(200, Math.trunc(Number(req.query?.limit || 20) || 20)));

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = isISO(fromQ) ? fromQ : `${today.slice(0, 7)}-01`;
    const to = isISO(toQ) ? toQ : today;

    const where = [];
    const params = [];
    let p = 1;

    where.push(`r.doc_date BETWEEN $${p++} AND $${p++}`);
    params.push(from, to);

    if (onlyCreated) where.push(`LOWER(COALESCE(r.usuario,'')) IN (SELECT LOWER(username) FROM app_users WHERE is_active=TRUE)`);
    if (user) {
      where.push(`LOWER(COALESCE(r.usuario,'')) LIKE $${p++}`);
      params.push(`%${user}%`);
    }
    if (client) {
      where.push(`(LOWER(COALESCE(r.card_code,'')) LIKE $${p++} OR LOWER(COALESCE(r.card_name,'')) LIKE $${p++})`);
      params.push(`%${client}%`, `%${client}%`);
    }
    if (motivo && motivo !== "__ALL__") {
      where.push(`r.motivo = $${p++}`);
      params.push(motivo);
    }
    if (causa && causa !== "__ALL__") {
      where.push(`r.causa = $${p++}`);
      params.push(causa);
    }
    if (openOnly) where.push(`LOWER(COALESCE(r.status,'')) LIKE '%open%'`);

    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
    const totalR = await dbQuery(`SELECT COUNT(*)::int AS total FROM return_requests r ${whereSql}`, params);
    const total = Number(totalR.rows?.[0]?.total || 0);

    const dataR = await dbQuery(
      `SELECT
        r.req_num AS "reqNum",
        r.req_entry AS "reqEntry",
        r.doc_date AS "fecha",
        r.card_code AS "cardCode",
        r.card_name AS "cardName",
        r.usuario AS "usuario",
        r.warehouse AS "warehouse",
        r.motivo AS "motivo",
        r.causa AS "causa",
        r.status AS "status",
        r.total_amount::float AS "totalAmount",
        r.total_qty::float AS "totalQty",
        r.comments AS "comments"
       FROM return_requests r
       ${whereSql}
       ORDER BY r.doc_date DESC, r.req_num DESC
       OFFSET $${p++} LIMIT $${p++}`,
      [...params, skip, limit]
    );

    return safeJson(res, 200, { ok: true, from, to, total, skip, limit, rows: dataR.rows || [] });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get("/api/admin/returns/lines", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });
    const reqNum = Number(req.query?.reqNum || 0);
    if (!Number.isFinite(reqNum) || reqNum <= 0) return safeJson(res, 400, { ok: false, message: "reqNum inválido" });

    const headR = await dbQuery(
      `SELECT req_num AS "reqNum", doc_date AS "docDate", status AS "status"
       FROM return_requests
       WHERE req_num=$1
       LIMIT 1`,
      [reqNum]
    );
    const head = headR.rows?.[0];
    if (!head) return safeJson(res, 404, { ok: false, message: "Solicitud no encontrada" });

    const linesR = await dbQuery(
      `SELECT
        req_num AS "reqNum",
        doc_date AS "docDate",
        item_code AS "itemCode",
        item_desc AS "itemDesc",
        qty::float AS "qty",
        price::float AS "price",
        line_total::float AS "lineTotal"
       FROM return_lines
       WHERE req_num=$1
       ORDER BY line_total DESC, item_code ASC`,
      [reqNum]
    );

    return safeJson(res, 200, {
      ok: true,
      reqNum: head.reqNum,
      docDate: head.docDate,
      status: head.status || "Open",
      lines: linesR.rows || [],
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.patch("/api/admin/returns/:reqNum/status", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });
    const reqNum = Number(req.params.reqNum || 0);
    if (!Number.isFinite(reqNum) || reqNum <= 0) return safeJson(res, 400, { ok: false, message: "reqNum inválido" });
    const status = String(req.body?.status || "").trim().toLowerCase();
    const finalStatus = status.includes("open") ? "Open" : status.includes("close") ? "Closed" : "";
    if (!finalStatus) return safeJson(res, 400, { ok: false, message: "status inválido (usa Open/Closed)" });

    const r = await dbQuery(
      `UPDATE return_requests
       SET status=$2, updated_at=NOW()
       WHERE req_num=$1
       RETURNING req_num AS "reqNum", status`,
      [reqNum, finalStatus]
    );

    if (!r.rowCount) return safeJson(res, 404, { ok: false, message: "Solicitud no encontrada" });
    return safeJson(res, 200, { ok: true, reqNum, status: r.rows[0].status });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================================================
   Admin cotizaciones DB / sync
========================================================= */
function qcIsCancelledLike(row) {
  const cancelStr = String(row?.CancelStatus ?? row?.cancelStatus ?? "").toLowerCase();
  const statusStr = String(row?.DocumentStatus ?? row?.status ?? "").toLowerCase();
  const commentsStr = String(row?.Comments ?? row?.comments ?? "").toLowerCase();
  return (
    cancelStr.includes("csyes") ||
    cancelStr.includes("cancel") ||
    statusStr.includes("cancel") ||
    commentsStr.includes("cancel")
  );
}

function qcNormalizeStatus(doc, lines = []) {
  if (qcIsCancelledLike(doc)) return "Cancelled";

  const header = String(doc?.DocumentStatus ?? doc?.status ?? "").toLowerCase();
  const lineStates = (Array.isArray(lines) ? lines : [])
    .map((ln) => String(ln?.LineStatus ?? "").toLowerCase())
    .filter(Boolean);

  const allClosed = lineStates.length > 0 && lineStates.every((s) => s.includes("close"));
  const anyOpen = lineStates.some((s) => s.includes("open"));

  if (header.includes("close")) return "Close";
  if (header.includes("open")) return allClosed && !anyOpen ? "Close" : "Open";
  if (allClosed) return "Close";
  if (anyOpen) return "Open";

  const raw = String(doc?.DocumentStatus ?? doc?.status ?? "").trim();
  return raw || "Open";
}

async function qcDeleteQuoteCacheDoc(docNum) {
  const n = Number(docNum || 0);
  if (!n) return;
  await dbQuery(`DELETE FROM quote_lines_cache WHERE doc_num=$1`, [n]);
  await dbQuery(`DELETE FROM quotes_cache WHERE doc_num=$1`, [n]);
}

async function qcDeleteQuoteLinesOnly(docNum) {
  const n = Number(docNum || 0);
  if (!n) return;
  await dbQuery(`DELETE FROM quote_lines_cache WHERE doc_num=$1`, [n]);
}

async function qcSapGetFirstByDocNum(entity, docNum, select = "DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments") {
  const n = Number(docNum || 0);
  if (!n) return null;
  const raw = await slFetchFreshSession(
    `/${entity}?$select=${select}&$filter=${encodeURIComponent(`DocNum eq ${n}`)}&$orderby=DocEntry desc&$top=1`
  );
  return Array.isArray(raw?.value) && raw.value.length ? raw.value[0] : null;
}

async function qcFindOrdersLinkedToQuote(quoteDocEntry, cardCode, from, toPlus1) {
  const cc = String(cardCode || "").trim();
  if (!quoteDocEntry || !cc) return [];
  const raw = await slFetchFreshSession(
    `/Orders?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
      `&$filter=${encodeURIComponent(`CardCode eq '${cc.replace(/'/g, "''")}' and DocDate ge '${from}' and DocDate lt '${toPlus1}'`)}` +
      `&$orderby=DocDate desc,DocEntry desc&$top=200`
  );
  const candidates = Array.isArray(raw?.value) ? raw.value : [];
  const out = [];
  for (const o of candidates) {
    const od = await sapGetByDocEntry("Orders", o.DocEntry);
    const lines = Array.isArray(od?.DocumentLines) ? od.DocumentLines : [];
    const linked = lines.some((l) => Number(l?.BaseType) === 23 && Number(l?.BaseEntry) === Number(quoteDocEntry));
    if (linked) out.push(od);
    await sleep(8);
  }
  return out;
}

async function qcTraceQuoteTotals(quoteDocNum, fromOverride, toOverride) {
  const quoteHead = await qcSapGetFirstByDocNum(
    "Quotations",
    quoteDocNum,
    "DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments"
  );
  if (!quoteHead?.DocEntry) return { ok: false, totalEntregado: 0, totalCotizado: 0, pendiente: 0 };

  const quote = await sapGetByDocEntry("Quotations", quoteHead.DocEntry);
  const quoteDocEntry = Number(quote?.DocEntry || quoteHead.DocEntry || 0);
  const cardCode = String(quote?.CardCode || quoteHead?.CardCode || "").trim();
  const quoteDate = String(quote?.DocDate || quoteHead?.DocDate || "").slice(0, 10);
  const totalCotizado = Number(quote?.DocTotal || quoteHead?.DocTotal || 0);

  const from = /^\d{4}-\d{2}-\d{2}$/.test(String(fromOverride || "")) ? String(fromOverride) : addDaysISO(quoteDate, -30);
  const to = /^\d{4}-\d{2}-\d{2}$/.test(String(toOverride || "")) ? String(toOverride) : addDaysISO(quoteDate, 60);
  const toPlus1 = addDaysISO(to, 1);

  const orders = await qcFindOrdersLinkedToQuote(quoteDocEntry, cardCode, from, toPlus1);
  if (!orders.length) {
    return { ok: true, totalCotizado, totalEntregado: 0, pendiente: Number(totalCotizado.toFixed(2)) };
  }

  const orderDocEntrySet = new Set(orders.map((x) => Number(x?.DocEntry || 0)).filter(Boolean));
  let totalEntregado = 0;
  const seen = new Set();

  const delRaw = await slFetchFreshSession(
    `/DeliveryNotes?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
      `&$filter=${encodeURIComponent(`CardCode eq '${cardCode.replace(/'/g, "''")}' and DocDate ge '${from}' and DocDate lt '${toPlus1}'`)}` +
      `&$orderby=DocDate desc,DocEntry desc&$top=300`
  );
  const delCandidates = Array.isArray(delRaw?.value) ? delRaw.value : [];

  for (const d of delCandidates) {
    const dd = await sapGetByDocEntry("DeliveryNotes", d.DocEntry);
    const de = Number(dd?.DocEntry || 0);
    if (!de || seen.has(de)) continue;
    const lines = Array.isArray(dd?.DocumentLines) ? dd.DocumentLines : [];
    const linked = lines.some((l) => Number(l?.BaseType) === 17 && orderDocEntrySet.has(Number(l?.BaseEntry)));
    if (linked) {
      seen.add(de);
      totalEntregado += Number(dd?.DocTotal || 0);
    }
    await sleep(8);
  }

  return {
    ok: true,
    totalCotizado,
    totalEntregado: Number(totalEntregado.toFixed(2)),
    pendiente: Number((totalCotizado - totalEntregado).toFixed(2)),
  };
}

async function qcTraceQuoteLinesByItem(quoteDocNum, fromOverride, toOverride) {
  const out = new Map();
  const quoteHead = await qcSapGetFirstByDocNum("Quotations", quoteDocNum, "DocEntry,DocNum,DocDate,CardCode");
  if (!quoteHead?.DocEntry) return out;

  const quote = await sapGetByDocEntry("Quotations", quoteHead.DocEntry);
  const quoteDocEntry = Number(quote?.DocEntry || quoteHead.DocEntry || 0);
  const cardCode = String(quote?.CardCode || quoteHead?.CardCode || "").trim();
  const quoteDate = String(quote?.DocDate || quoteHead?.DocDate || "").slice(0, 10);

  const from = /^\d{4}-\d{2}-\d{2}$/.test(String(fromOverride || "")) ? String(fromOverride) : addDaysISO(quoteDate, -30);
  const to = /^\d{4}-\d{2}-\d{2}$/.test(String(toOverride || "")) ? String(toOverride) : addDaysISO(quoteDate, 60);
  const toPlus1 = addDaysISO(to, 1);

  const orders = await qcFindOrdersLinkedToQuote(quoteDocEntry, cardCode, from, toPlus1);
  const orderDocEntrySet = new Set(orders.map((x) => Number(x?.DocEntry || 0)).filter(Boolean));
  if (!orderDocEntrySet.size) return out;

  const delRaw = await slFetchFreshSession(
    `/DeliveryNotes?$select=DocEntry,DocNum,DocDate,CardCode` +
      `&$filter=${encodeURIComponent(`CardCode eq '${cardCode.replace(/'/g, "''")}' and DocDate ge '${from}' and DocDate lt '${toPlus1}'`)}` +
      `&$orderby=DocDate desc,DocEntry desc&$top=300`
  );
  const delCandidates = Array.isArray(delRaw?.value) ? delRaw.value : [];
  const seen = new Set();

  for (const d of delCandidates) {
    const dd = await sapGetByDocEntry("DeliveryNotes", d.DocEntry);
    const de = Number(dd?.DocEntry || 0);
    if (!de || seen.has(de)) continue;
    seen.add(de);

    const lines = Array.isArray(dd?.DocumentLines) ? dd.DocumentLines : [];
    const linked = lines.some((l) => Number(l?.BaseType) === 17 && orderDocEntrySet.has(Number(l?.BaseEntry)));
    if (!linked) {
      await sleep(8);
      continue;
    }

    for (const ln of lines) {
      if (Number(ln?.BaseType) !== 17) continue;
      if (!orderDocEntrySet.has(Number(ln?.BaseEntry))) continue;

      const itemCode = String(ln?.ItemCode || "").trim();
      if (!itemCode) continue;

      const qty = Number(ln?.Quantity || 0);
      const dollars = Number(ln?.LineTotal ?? 0);

      const prev = out.get(itemCode) || { qtyDelivered: 0, dollarsDelivered: 0 };
      prev.qtyDelivered += Number.isFinite(qty) ? qty : 0;
      prev.dollarsDelivered += Number.isFinite(dollars) ? dollars : 0;
      out.set(itemCode, prev);
    }

    await sleep(8);
  }

  return out;
}

async function fetchQuotationHeadersPage(fromDate, toDate, top = 200, skip = 0) {
  const fromExpr = quoteODataString(fromDate);
  const toPlus1 = addDaysISO(toDate, 1);
  const toExpr = quoteODataString(toPlus1);

  const path =
    `/Quotations?$select=DocEntry,DocNum,DocDate,DocTime,CardCode,CardName,DocTotal,Comments,DocumentStatus,CancelStatus` +
    `&$filter=DocDate ge ${fromExpr} and DocDate lt ${toExpr}` +
    `&$orderby=DocDate desc,DocNum desc&$top=${top}&$skip=${skip}`;

  const r = await slFetchFreshSession(path);
  return Array.isArray(r?.value) ? r.value : [];
}


async function handleAdminQuotesSync(req, res) {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const mode = String(req.body?.mode || req.query?.mode || "days").trim().toLowerCase();
    const nRaw = Number(req.body?.n || req.query?.n || 30);
    const n = Math.max(1, Math.min(mode === "days" ? 400 : mode === "hours" ? 72 : 1440, Number.isFinite(nRaw) ? Math.trunc(nRaw) : 30));
    const maxDocsRaw = Number(req.body?.maxDocs || req.query?.maxDocs || 600);
    const maxDocs = Math.max(1, Math.min(2000, Number.isFinite(maxDocsRaw) ? Math.trunc(maxDocsRaw) : 600));

    const nowMs = nowInOffsetMs(TZ_OFFSET_MIN);
    const fromMs =
      mode === "hours"
        ? nowMs - n * 60 * 60 * 1000
        : mode === "minutes"
        ? nowMs - n * 60 * 1000
        : 0;

    const monthStart = `${today.slice(0, 8)}01`;
    const fromDate = isISO(req.body?.fromDate || req.query?.fromDate)
      ? String(req.body?.fromDate || req.query?.fromDate)
      : mode === "today"
      ? today
      : mode === "month"
      ? monthStart
      : mode === "hours" || mode === "minutes"
      ? addDaysISO(today, -2)
      : addDaysISO(today, -n);

    const toDate = isISO(req.body?.toDate || req.query?.toDate)
      ? String(req.body?.toDate || req.query?.toDate)
      : today;

    let scanned = 0;
    let saved = 0;
    let removed = 0;
    let skipSap = 0;
    const batchTop = Math.min(200, maxDocs);
    const maxTrace = Math.min(500, maxDocs);
    const maxLinesCalc = Math.min(250, maxDocs);

    for (let page = 0; page < 80 && saved < maxDocs; page++) {
      const headers = await fetchQuotationHeadersPage(fromDate, toDate, batchTop, skipSap);
      if (!headers.length) break;
      skipSap += headers.length;

      for (const q of headers) {
        if (saved >= maxDocs) break;
        scanned++;

        const docNum = Number(q?.DocNum || 0);
        const docTime = Number(q?.DocTime || 0);
        const docDate = String(q?.DocDate || "").slice(0, 10);

        if ((mode === "hours" || mode === "minutes") && docDate) {
          const ms = docDateTimeToMs(docDate, docTime);
          if (ms && ms < fromMs) continue;
        }

        const qFull = await sapGetByDocEntry("Quotations", q.DocEntry);
        const qLines = Array.isArray(qFull?.DocumentLines) ? qFull.DocumentLines : [];
        const comments = String(qFull?.Comments || q?.Comments || "");
        const cancelStatus = String(qFull?.CancelStatus || q?.CancelStatus || "");
        const isCancelled = qcIsCancelledLike({
          CancelStatus: cancelStatus,
          DocumentStatus: qFull?.DocumentStatus || q?.DocumentStatus || "",
          Comments: comments,
        });

        await qcDeleteQuoteLinesOnly(docNum);

        if (isCancelled) {
          await qcDeleteQuoteCacheDoc(docNum);
          removed++;
          continue;
        }

        const usuario = parseUserFromComments(comments) || "sin_user";
        const warehouse = parseWhFromComments(comments) || "sin_wh";
        const cardCode = String(qFull?.CardCode || q?.CardCode || "");
        const cardName = String(qFull?.CardName || q?.CardName || "");
        const docTotal = Number(qFull?.DocTotal || q?.DocTotal || 0);
        const status = qcNormalizeStatus(qFull || q, qLines);

        let deliveredTotal = 0;
        let deliveredMap = new Map();

        if (saved < maxTrace) {
          try {
            const tr = await qcTraceQuoteTotals(docNum, addDaysISO(today, -45), today);
            if (tr?.ok) deliveredTotal = Number(tr.totalEntregado || 0);
          } catch {}
        }

        if (saved < maxLinesCalc) {
          try {
            deliveredMap = await qcTraceQuoteLinesByItem(docNum, addDaysISO(today, -45), today);
          } catch {
            deliveredMap = new Map();
          }
        }

        await upsertQuoteCache({
          docNum,
          docEntry: qFull?.DocEntry || q?.DocEntry || null,
          docDate,
          docTime,
          cardCode,
          cardName,
          usuario,
          warehouse,
          docTotal,
          deliveredTotal,
          status,
          cancelStatus,
          comments,
          groupName: "",
        });

        const quotedMap = new Map();
        for (const ln of qLines) {
          const itemCode = String(ln?.ItemCode || "").trim();
          if (!itemCode) continue;
          const qtyQuoted = Number(ln?.Quantity || 0);
          const dollarsQuoted = Number(ln?.LineTotal ?? 0);
          const itemDesc = String(ln?.ItemDescription || ln?.ItemName || "").trim();
          const prev = quotedMap.get(itemCode) || { qtyQuoted: 0, dollarsQuoted: 0, itemDesc: itemDesc || "" };
          prev.qtyQuoted += Number.isFinite(qtyQuoted) ? qtyQuoted : 0;
          prev.dollarsQuoted += Number.isFinite(dollarsQuoted) ? dollarsQuoted : 0;
          if (!prev.itemDesc && itemDesc) prev.itemDesc = itemDesc;
          quotedMap.set(itemCode, prev);
        }

        for (const [itemCode, qv] of quotedMap.entries()) {
          const dv = deliveredMap.get(itemCode) || { qtyDelivered: 0, dollarsDelivered: 0 };
          await upsertQuoteLineCache({
            docNum,
            docDate,
            itemCode,
            itemDesc: qv.itemDesc || "",
            qtyQuoted: qv.qtyQuoted,
            qtyDelivered: Number(dv.qtyDelivered || 0),
            dollarsQuoted: qv.dollarsQuoted,
            dollarsDelivered: Number(dv.dollarsDelivered || 0),
          });
        }

        saved++;
        await sleep(10);
      }
    }

    const stamp = isoDateTimeInOffset(TZ_OFFSET_MIN);
    await setState("quotes_cache_last_sync", stamp);

    return safeJson(res, 200, {
      ok: true,
      mode,
      n,
      maxDocs,
      scanned,
      saved,
      removed,
      lastSyncAt: stamp,
      window: { fromDate, toDate },
      note: "Historico lee quotes_cache. Este sync reemplaza las lineas cacheadas del documento, elimina canceladas y recalcula entregado desde Quote -> Order -> Delivery.",
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
}

app.post("/api/admin/quotes/sync", verifyAdmin, async (req, res) => {
  return handleAdminQuotesSync(req, res);
});
app.get("/api/admin/quotes/sync", verifyAdmin, async (req, res) => {
  return handleAdminQuotesSync(req, res);
});

app.get("/api/admin/quotes/db", verifyAdmin, async (req, res) => {

  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const fromQ = String(req.query?.from || "").slice(0, 10);
    const toQ = String(req.query?.to || "").slice(0, 10);
    const user = String(req.query?.user || "").trim().toLowerCase();
    const client = String(req.query?.client || "").trim().toLowerCase();
    const skip = Math.max(0, Math.trunc(Number(req.query?.skip || 0) || 0));
    const limit = Math.max(1, Math.min(200, Math.trunc(Number(req.query?.limit || 20) || 20)));
    const onlyCreated = String(req.query?.onlyCreated || "0") === "1";
    const openOnly = String(req.query?.openOnly || "0") === "1";

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = isISO(fromQ) ? fromQ : addDaysISO(today, -30);
    const to = isISO(toQ) ? toQ : today;

    const where = [];
    const params = [];
    let p = 1;

    where.push(`q.doc_date BETWEEN $${p++} AND $${p++}`);
    params.push(from, to);
    where.push(`NOT (LOWER(COALESCE(q.cancel_status,'')) LIKE '%csyes%' OR LOWER(COALESCE(q.cancel_status,'')) LIKE '%cancel%' OR LOWER(COALESCE(q.status,'')) LIKE '%cancel%')`);

    if (onlyCreated) where.push(`LOWER(COALESCE(q.usuario,'')) IN (SELECT LOWER(username) FROM app_users WHERE is_active=TRUE)`);
    if (user) {
      where.push(`LOWER(COALESCE(q.usuario,'')) LIKE $${p++}`);
      params.push(`%${user}%`);
    }
    if (client) {
      where.push(`(LOWER(COALESCE(q.card_code,'')) LIKE $${p++} OR LOWER(COALESCE(q.card_name,'')) LIKE $${p++})`);
      params.push(`%${client}%`, `%${client}%`);
    }
    if (openOnly) {
      where.push(`LOWER(COALESCE(q.status,'')) LIKE '%open%'`);
      where.push(`NOT (
        LOWER(COALESCE(q.cancel_status,'')) LIKE '%csyes%' OR
        LOWER(COALESCE(q.cancel_status,'')) LIKE '%cancel%' OR
        LOWER(COALESCE(q.status,'')) LIKE '%cancel%'
      )`);
    }

    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
    const totalR = await dbQuery(`SELECT COUNT(*)::int AS total FROM quotes_cache q ${whereSql}`, params);
    const total = Number(totalR.rows?.[0]?.total || 0);

    const dataR = await dbQuery(
      `SELECT
        q.doc_num AS "docNum",
        q.doc_entry AS "docEntry",
        q.doc_date AS "fecha",
        q.card_code AS "cardCode",
        q.card_name AS "cardName",
        q.usuario AS "usuario",
        q.warehouse AS "warehouse",
        q.status AS "estado",
        q.cancel_status AS "cancelStatus",
        q.comments AS "comments",
        q.doc_total::float AS "montoCotizacion",
        q.delivered_total::float AS "montoEntregado"
       FROM quotes_cache q
       ${whereSql}
       ORDER BY q.doc_date DESC, q.doc_num DESC
       OFFSET $${p++} LIMIT $${p++}`,
      [...params, skip, limit]
    );

    const lastSyncAt = await getState("quotes_cache_last_sync");
    return safeJson(res, 200, {
      ok: true,
      from,
      to,
      total,
      skip,
      limit,
      lastSyncAt: lastSyncAt || null,
      quotes: dataR.rows || [],
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get("/api/admin/quotes/dashboard-db", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const fromQ = String(req.query?.from || "").slice(0, 10);
    const toQ = String(req.query?.to || "").slice(0, 10);
    const onlyCreated = String(req.query?.onlyCreated || "0") === "1";

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = isISO(fromQ) ? fromQ : addDaysISO(today, -30);
    const to = isISO(toQ) ? toQ : today;

    const createdJoin = onlyCreated
      ? `AND LOWER(COALESCE(q.usuario,'')) IN (SELECT LOWER(username) FROM app_users WHERE is_active=TRUE)`
      : ``;
    const notCancelledJoin = `AND NOT (LOWER(COALESCE(q.cancel_status,'')) LIKE '%csyes%' OR LOWER(COALESCE(q.cancel_status,'')) LIKE '%cancel%' OR LOWER(COALESCE(q.status,'')) LIKE '%cancel%')`;

    const totalsR = await dbQuery(
      `SELECT
        COUNT(*)::int AS quotes,
        COALESCE(SUM(q.doc_total),0)::float AS cotizado,
        COALESCE(SUM(q.delivered_total),0)::float AS entregado
       FROM quotes_cache q
       WHERE q.doc_date BETWEEN $1 AND $2
       ${createdJoin}
       ${notCancelledJoin}`,
      [from, to]
    );
    const totals = totalsR.rows?.[0] || { quotes: 0, cotizado: 0, entregado: 0 };
    const fillRatePct = Number(totals.cotizado) > 0 ? (Number(totals.entregado) / Number(totals.cotizado)) * 100 : 0;

    const byUserR = await dbQuery(
      `SELECT
        COALESCE(NULLIF(q.usuario,''),'sin_user') AS usuario,
        COUNT(*)::int AS cnt,
        COALESCE(SUM(q.doc_total),0)::float AS cotizado,
        COALESCE(SUM(q.delivered_total),0)::float AS entregado
       FROM quotes_cache q
       WHERE q.doc_date BETWEEN $1 AND $2
       ${createdJoin}
       ${notCancelledJoin}
       GROUP BY 1
       ORDER BY cotizado DESC
       LIMIT 2000`,
      [from, to]
    );

    const byWhR = await dbQuery(
      `SELECT
        COALESCE(NULLIF(q.warehouse,''),'sin_wh') AS warehouse,
        COUNT(*)::int AS cnt,
        COALESCE(SUM(q.doc_total),0)::float AS cotizado,
        COALESCE(SUM(q.delivered_total),0)::float AS entregado
       FROM quotes_cache q
       WHERE q.doc_date BETWEEN $1 AND $2
       ${createdJoin}
       ${notCancelledJoin}
       GROUP BY 1
       ORDER BY cotizado DESC
       LIMIT 2000`,
      [from, to]
    );

    const byClientR = await dbQuery(
      `SELECT
        COALESCE(NULLIF(q.card_name,''), q.card_code, 'sin_cliente') AS customer,
        COUNT(*)::int AS cnt,
        COALESCE(SUM(q.doc_total),0)::float AS cotizado
       FROM quotes_cache q
       WHERE q.doc_date BETWEEN $1 AND $2
       ${createdJoin}
       ${notCancelledJoin}
       GROUP BY 1
       ORDER BY cotizado DESC
       LIMIT 2000`,
      [from, to]
    );

    const byGroupR = await dbQuery(
      `SELECT
        COALESCE(NULLIF(q.group_name,''), 'Sin grupo') AS "group",
        COUNT(*)::int AS cnt,
        COALESCE(SUM(q.doc_total),0)::float AS cotizado
       FROM quotes_cache q
       WHERE q.doc_date BETWEEN $1 AND $2
       ${createdJoin}
       ${notCancelledJoin}
       GROUP BY 1
       ORDER BY cotizado DESC
       LIMIT 2000`,
      [from, to]
    );

    const lastSyncAt = await getState("quotes_cache_last_sync");

    return safeJson(res, 200, {
      ok: true,
      from,
      to,
      lastSyncAt: lastSyncAt || null,
      totals: {
        quotes: Number(totals.quotes || 0),
        cotizado: Number(totals.cotizado || 0),
        entregado: Number(totals.entregado || 0),
        fillRatePct: Number(fillRatePct.toFixed(2)),
      },
      byUser: (byUserR.rows || []).map((r) => ({
        usuario: r.usuario,
        cnt: Number(r.cnt || 0),
        cotizado: Number(r.cotizado || 0),
        entregado: Number(r.entregado || 0),
        fillRatePct: Number(r.cotizado || 0) > 0 ? Number(((Number(r.entregado || 0) / Number(r.cotizado || 0)) * 100).toFixed(2)) : 0,
      })),
      byWh: (byWhR.rows || []).map((r) => ({
        warehouse: r.warehouse,
        cnt: Number(r.cnt || 0),
        cotizado: Number(r.cotizado || 0),
        entregado: Number(r.entregado || 0),
        fillRatePct: Number(r.cotizado || 0) > 0 ? Number(((Number(r.entregado || 0) / Number(r.cotizado || 0)) * 100).toFixed(2)) : 0,
      })),
      byClient: (byClientR.rows || []).map((r) => ({
        customer: r.customer,
        cnt: Number(r.cnt || 0),
        cotizado: Number(r.cotizado || 0),
      })),
      byGroup: (byGroupR.rows || []).map((r) => ({
        group: r.group,
        cnt: Number(r.cnt || 0),
        cotizado: Number(r.cotizado || 0),
      })),
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get("/api/admin/quotes/lines", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });
    const docNum = Number(req.query?.docNum || 0);
    if (!Number.isFinite(docNum) || docNum <= 0) return safeJson(res, 400, { ok: false, message: "docNum inválido" });

    const linesR = await dbQuery(
      `SELECT
        doc_num AS "docNum",
        doc_date AS "docDate",
        item_code AS "itemCode",
        item_desc AS "itemDesc",
        qty_quoted::float AS "qtyQuoted",
        qty_delivered::float AS "qtyDelivered",
        dollars_quoted::float AS "dollarsQuoted",
        dollars_delivered::float AS "dollarsDelivered"
       FROM quote_lines_cache
       WHERE doc_num=$1
       ORDER BY dollars_quoted DESC, item_code ASC`,
      [docNum]
    );

    return safeJson(res, 200, { ok: true, docNum, lines: linesR.rows || [] });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});



/* =========================================================
   Módulo integrado: Mensajería
========================================================= */
{
// server.js (Mensajería Interna PRODIMA)
// ✅ Notificaciones por Google Apps Script (solicitante + mensajería + supervisores + couriers)
// ✅ Fechas sin desfase (strings en hora Panamá)
// ✅ Auto-asignación: todas las solicitudes nuevas se asignan automáticamente a "victor"


const { Pool } = pg;

app.use(express.json({ limit: "2mb" }));

/* =========================
   ENV
========================= */
const {
  PORT = 3000,
  DATABASE_URL = "",
  JWT_SECRET = "change_me",

  ADMIN_USER = "PRODIMA",
  ADMIN_PASS = "ADMINISTRADOR",

  // CORS_ORIGIN=https://prodima.com.pa,https://www.prodima.com.pa
  CORS_ORIGIN = "",

  // ✅ Google Apps Script Webhook
  GAS_WEBHOOK_URL = "",
  GAS_WEBHOOK_SECRET = "",

  // ✅ Buzón/grupo mensajería
  COURIER_MAILBOX = "mensajeria@prodima.com.pa",

  // ✅ Supervisores
  SUPERVISOR_NOTIFY_TO = "logistica2@prodima.com.pa,melanie.choy@prodima.com.pa,malena.torrero@prodima.com.pa",

  // ✅ Auto-asignación a Victor
  AUTO_ASSIGN_ENABLED = "1",              // "1" = activo
  AUTO_ASSIGN_COURIER_USERNAME = "victor",// username del mensajero Victor en msg_users
  AUTO_ASSIGN_STRICT = "0",               // "1" = si no existe Victor, falla la creación
} = process.env;

/* =========================
   CORS ROBUSTO
========================= */
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

/* =========================
   DB (Supabase Postgres)
========================= */
const pool = new Pool({
  connectionString: DATABASE_URL || undefined,
  ssl: DATABASE_URL ? { rejectUnauthorized: false } : undefined,
});

function hasDb() {
  return Boolean(DATABASE_URL);
}
async function dbQuery(text, params = []) {
  return pool.query(text, params);
}

// Asegura TZ de sesión en Panamá
if (hasDb()) {
  pool.on("connect", (client) => {
    client.query("SET TIME ZONE 'America/Panama'").catch(() => {});
  });
}

async function ensureDb() {
  if (!hasDb()) return;

  // Usuarios de mensajería (solicitantes + mensajeros)
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS msg_users (
      id BIGSERIAL PRIMARY KEY,
      username TEXT UNIQUE NOT NULL,
      full_name TEXT NOT NULL DEFAULT '',
      department TEXT NOT NULL DEFAULT '',
      role TEXT NOT NULL DEFAULT 'requester', -- requester | courier
      email TEXT DEFAULT '',
      phone TEXT DEFAULT '',
      pin_hash TEXT NOT NULL,
      is_active BOOLEAN DEFAULT TRUE,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_msg_users_dept ON msg_users(department);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_msg_users_role ON msg_users(role);`);

  // Solicitudes de mensajería
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS msg_requests (
      id BIGSERIAL PRIMARY KEY,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW(),

      created_by_user_id BIGINT NOT NULL,
      created_by_username TEXT NOT NULL DEFAULT '',
      requester_name TEXT NOT NULL DEFAULT '',
      requester_department TEXT NOT NULL DEFAULT '',
      requester_email TEXT DEFAULT '',
      requester_phone TEXT DEFAULT '',

      request_type TEXT NOT NULL DEFAULT '',
      contact_person_phone TEXT NOT NULL DEFAULT '',
      address_details TEXT NOT NULL DEFAULT '',
      description TEXT NOT NULL DEFAULT '',
      priority TEXT NOT NULL DEFAULT 'Media', -- Alta|Media|Baja

      status TEXT NOT NULL DEFAULT 'open', -- open|in_progress|closed|cancelled
      status_updated_at TIMESTAMP DEFAULT NOW(),

      assigned_to_user_id BIGINT,
      assigned_to_name TEXT DEFAULT '',
      assigned_at TIMESTAMP
    );
  `);

  // columnas comentario del mensajero + fecha cierre
  await dbQuery(`ALTER TABLE msg_requests ADD COLUMN IF NOT EXISTS courier_comment TEXT DEFAULT '';`);
  await dbQuery(`ALTER TABLE msg_requests ADD COLUMN IF NOT EXISTS closed_at TIMESTAMP;`);

  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_msg_requests_created_at ON msg_requests(created_at);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_msg_requests_status ON msg_requests(status);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_msg_requests_dept ON msg_requests(requester_department);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_msg_requests_created_by ON msg_requests(created_by_user_id);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_msg_requests_assigned_to ON msg_requests(assigned_to_user_id);`);
}

/* =========================
   Helpers
========================= */
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
function verifyUser(req, res, next) {
  const token = readBearer(req);
  if (!token) return safeJson(res, 401, { ok: false, message: "Missing Bearer token" });
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    if (decoded?.role !== "user") return safeJson(res, 403, { ok: false, message: "Forbidden" });
    req.user = decoded;
    next();
  } catch {
    return safeJson(res, 401, { ok: false, message: "Invalid token" });
  }
}
function isCourierRole(role) {
  const r = String(role || "").trim().toLowerCase();
  return r === "courier" || r === "mercaderista";
}

/* =========================
   PIN hashing (crypto pbkdf2)
========================= */
const PIN_ITER = 120_000;

function hashPin(pin) {
  const p = String(pin || "").trim();
  if (p.length < 4) throw new Error("PIN muy corto (mín 4).");
  const salt = crypto.randomBytes(16).toString("hex");
  const dk = crypto.pbkdf2Sync(p, salt, PIN_ITER, 32, "sha256").toString("hex");
  return `pbkdf2$${PIN_ITER}$${salt}$${dk}`;
}
function verifyPin(pin, stored) {
  const p = String(pin || "").trim();
  const s = String(stored || "");
  const parts = s.split("$");
  if (parts.length !== 4 || parts[0] !== "pbkdf2") return false;
  const iter = Number(parts[1] || 0);
  const salt = String(parts[2] || "");
  const hash = String(parts[3] || "");
  if (!iter || !salt || !hash) return false;

  const dk = crypto.pbkdf2Sync(p, salt, iter, 32, "sha256").toString("hex");
  return crypto.timingSafeEqual(Buffer.from(dk, "hex"), Buffer.from(hash, "hex"));
}

/* =========================
   Normalizadores
========================= */
function normStatus(s) {
  const t = String(s || "").trim().toLowerCase();
  if (["open", "abierta", "abierto"].includes(t)) return "open";
  if (["in_progress", "en progreso", "progreso", "in progress"].includes(t)) return "in_progress";
  if (["closed", "cerrada", "cerrado"].includes(t)) return "closed";
  if (["cancelled", "canceled", "cancelada", "cancelado"].includes(t)) return "cancelled";
  return t;
}
function isISODate(s) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(s || ""));
}

/* =========================
   FECHAS “LOCAL PANAMÁ” (evita desfase 5 horas)
   Retornamos strings sin "Z" para que el frontend no las interprete como UTC.
========================= */
const SQL_DT_FMT = `YYYY-MM-DD"T"HH24:MI:SS`;
function fmtTsSql(col) {
  return `to_char(${col}, '${SQL_DT_FMT}')`;
}

/* =========================
   EMAIL NOTIFICATIONS via Google Apps Script (Webhook)
========================= */
function isEmail(s) {
  return /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i.test(String(s || "").trim());
}
function parseEmailList(csv) {
  return String(csv || "")
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter((x) => isEmail(x));
}


const DOCS_NOTIFY_TO = parseEmailList(
  "facturacion@prodima.com.pa,adm-red@prodima.com.pa,ventasconsumidor@prodima.com.pa,liliana.vergara@prodima.com.pa"
).join(",");

function sanitizeAttachmentName(name, fallback = "archivo") {
  const raw = String(name || fallback || "archivo")
    .replace(/[\\/\r\n\t]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  const clean = raw.replace(/[^A-Za-z0-9._()\- áéíóúÁÉÍÓÚñÑ]/g, "_");
  return (clean || fallback || "archivo").slice(0, 120);
}

function normalizeIncomingAttachments(list) {
  const allowed = new Set([
    "application/pdf",
    "image/jpeg",
    "image/jpg",
    "image/png",
    "image/webp",
    "image/heic",
    "image/heif",
  ]);

  const incoming = Array.isArray(list) ? list : [];
  const out = [];
  let totalBytes = 0;

  for (const file of incoming.slice(0, 5)) {
    const filename = sanitizeAttachmentName(file?.filename || file?.name || "archivo");
    const mimeType = String(file?.mimeType || file?.type || "application/octet-stream").trim().toLowerCase();
    const contentBase64 = String(file?.contentBase64 || file?.base64 || file?.content || "").trim();

    if (!filename || !contentBase64) continue;
    if (!allowed.has(mimeType)) continue;

    let bytes = 0;
    try {
      bytes = Buffer.byteLength(contentBase64, "base64");
    } catch {
      continue;
    }

    if (!bytes || bytes > 8 * 1024 * 1024) continue;
    if (totalBytes + bytes > 18 * 1024 * 1024) break;
    totalBytes += bytes;

    out.push({ filename, mimeType, contentBase64, size: bytes });
  }

  return out;
}

async function sendDocumentEmailViaGAS({ event, notifyTo, data, attachments }) {
  if (!GAS_WEBHOOK_URL || !GAS_WEBHOOK_SECRET) {
    return { ok: false, skipped: true, message: "GAS no configurado" };
  }

  const payload = {
    secret: GAS_WEBHOOK_SECRET,
    event,
    requesterEmail: "",
    notifyTo: notifyTo || DOCS_NOTIFY_TO,
    data: data || {},
    attachments: normalizeIncomingAttachments(attachments),
  };

  try {
    const f = await _getFetch();
    const resp = await f(GAS_WEBHOOK_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    const text = await resp.text().catch(() => "");
    if (!resp.ok) {
      return { ok: false, skipped: false, message: text || `HTTP ${resp.status}` };
    }
    return { ok: true, skipped: false, message: (text || "ok") + " | " + DOCS_MAIL_BUILD };
  } catch (err) {
    return { ok: false, skipped: false, message: String(err?.message || err) + " | " + DOCS_MAIL_BUILD };
  }
}

// Fetch helper: Node 18+ tiene fetch; Node <18 usa node-fetch si lo instalas.
async function _getFetch() {
  if (typeof fetch !== "undefined") return fetch;
  const mod = await import("node-fetch"); // npm i node-fetch
  return mod.default;
}

async function getActiveCourierEmails() {
  const r = await dbQuery(
    `SELECT email
     FROM msg_users
     WHERE is_active=TRUE
       AND role='courier'
       AND COALESCE(email,'') <> ''`
  );
  return (r.rows || [])
    .map((x) => String(x.email || "").trim().toLowerCase())
    .filter((x) => isEmail(x));
}

async function buildNotifyRecipientsForCouriersAndSupervisors() {
  const base = [];
  if (COURIER_MAILBOX && isEmail(COURIER_MAILBOX)) base.push(String(COURIER_MAILBOX).trim().toLowerCase());

  const supervisors = parseEmailList(SUPERVISOR_NOTIFY_TO);
  const couriers = await getActiveCourierEmails();

  const all = [...base, ...supervisors, ...couriers];
  const uniq = [...new Set(all.map((x) => x.trim().toLowerCase()).filter(Boolean))];

  return uniq.join(",");
}

async function notifyViaGAS({ event, requesterEmail, notifyTo, data }) {
  if (!GAS_WEBHOOK_URL || !GAS_WEBHOOK_SECRET) return;

  const payload = {
    secret: GAS_WEBHOOK_SECRET,
    event,
    requesterEmail: requesterEmail || "",
    notifyTo: notifyTo || "",
    data: data || {},
  };

  try {
    const f = await _getFetch();
    const resp = await f(GAS_WEBHOOK_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!resp.ok) {
      const t = await resp.text().catch(() => "");
      console.error("GAS notify failed:", resp.status, t);
    }
  } catch (err) {
    console.error("GAS notify error:", err?.message || err);
  }
}

/* =========================
   AUTO-ASIGNACIÓN: buscar mensajero "victor"
========================= */
async function getAutoAssignCourier() {
  if (String(AUTO_ASSIGN_ENABLED) !== "1") return null;

  const username = String(AUTO_ASSIGN_COURIER_USERNAME || "").trim().toLowerCase();
  if (!username) return null;

  const r = await dbQuery(
    `SELECT id, full_name
     FROM msg_users
     WHERE is_active=TRUE
       AND role='courier'
       AND LOWER(username)=LOWER($1)
     LIMIT 1`,
    [username]
  );
  const u = r.rows?.[0];
  return u ? { id: Number(u.id), name: String(u.full_name || "") } : null;
}

/* =========================
   HEALTH
========================= */
app.get("/api/health", async (req, res) => {
  safeJson(res, 200, {
    ok: true,
    message: "✅ PRODIMA MENSAJERÍA API activa",
    db: hasDb() ? "on" : "off",
  });
});

/* =========================
   AUTH
========================= */
app.post("/api/admin/login", async (req, res) => {
  const user = String(req.body?.user || "").trim();
  const pass = String(req.body?.pass || "").trim();
  if (user !== ADMIN_USER || pass !== ADMIN_PASS) {
    return safeJson(res, 401, { ok: false, message: "Credenciales inválidas" });
  }
  const token = signToken({ role: "admin", user }, "12h");
  return safeJson(res, 200, { ok: true, token });
});

app.post("/api/user/login", async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const username = String(req.body?.username || "").trim().toLowerCase();
    const pin = String(req.body?.pin || "").trim();
    if (!username || !pin) return safeJson(res, 400, { ok: false, message: "Falta username/pin" });

    const r = await dbQuery(
      `SELECT id, username, full_name, department, role, email, phone, pin_hash, is_active
       FROM msg_users
       WHERE LOWER(username)=LOWER($1)
       LIMIT 1`,
      [username]
    );
    const u = r.rows?.[0];
    if (!u || !u.is_active) return safeJson(res, 401, { ok: false, message: "Usuario no válido/inactivo" });
    if (!verifyPin(pin, u.pin_hash)) return safeJson(res, 401, { ok: false, message: "PIN incorrecto" });

    const token = signToken({ role: "user", userId: u.id, username: u.username }, "12h");
    return safeJson(res, 200, {
      ok: true,
      token,
      profile: {
        id: u.id,
        username: u.username,
        fullName: u.full_name,
        department: u.department,
        role: u.role,
        email: u.email || "",
        phone: u.phone || "",
      },
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get("/api/user/me", verifyUser, async (req, res) => {
  try {
    const r = await dbQuery(
      `SELECT id, username, full_name, department, role, email, phone, is_active
       FROM msg_users WHERE id=$1 LIMIT 1`,
      [Number(req.user.userId)]
    );
    const u = r.rows?.[0];
    if (!u || !u.is_active) return safeJson(res, 401, { ok: false, message: "Usuario no válido/inactivo" });

    return safeJson(res, 200, {
      ok: true,
      profile: {
        id: u.id,
        username: u.username,
        fullName: u.full_name,
        department: u.department,
        role: u.role,
        email: u.email || "",
        phone: u.phone || "",
      },
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================
   ADMIN: USERS
========================= */
app.get("/api/admin/messaging/users", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const r = await dbQuery(
      `SELECT
         id, username, full_name, department, role, email, phone, is_active,
         ${fmtTsSql("created_at")} AS created_at_local
       FROM msg_users
       ORDER BY id DESC
       LIMIT 2000`
    );

    const users = (r.rows || []).map((u) => ({
      id: Number(u.id),
      username: u.username,
      fullName: u.full_name,
      department: u.department,
      role: u.role,
      email: u.email || "",
      phone: u.phone || "",
      isActive: Boolean(u.is_active),
      createdAt: u.created_at_local || null,
    }));

    return safeJson(res, 200, { ok: true, users });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.post("/api/admin/messaging/users", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const username = String(req.body?.username || "").trim().toLowerCase();
    const fullName = String(req.body?.fullName || "").trim();
    const department = String(req.body?.department || "").trim();
    const roleRaw = String(req.body?.role || "requester").trim().toLowerCase();
    const role = roleRaw === "courier" || roleRaw === "mercaderista" ? "courier" : "requester";
    const email = String(req.body?.email || "").trim();
    const phone = String(req.body?.phone || "").trim();
    const pin = String(req.body?.pin || "").trim();

    if (!username) return safeJson(res, 400, { ok: false, message: "Falta username" });
    if (!fullName) return safeJson(res, 400, { ok: false, message: "Falta fullName" });
    if (!department) return safeJson(res, 400, { ok: false, message: "Falta department" });
    if (!pin) return safeJson(res, 400, { ok: false, message: "Falta pin" });

    const pinHash = hashPin(pin);

    const r = await dbQuery(
      `INSERT INTO msg_users(username, full_name, department, role, email, phone, pin_hash, is_active, created_at, updated_at)
       VALUES($1,$2,$3,$4,$5,$6,$7,TRUE,NOW(),NOW())
       RETURNING id`,
      [username, fullName, department, role, email, phone, pinHash]
    );

    return safeJson(res, 200, { ok: true, id: Number(r.rows?.[0]?.id || 0) });
  } catch (e) {
    const msg = String(e.message || e);
    if (msg.toLowerCase().includes("duplicate") || msg.toLowerCase().includes("unique")) {
      return safeJson(res, 400, { ok: false, message: "Username ya existe" });
    }
    return safeJson(res, 500, { ok: false, message: msg });
  }
});

app.patch("/api/admin/messaging/users/:id", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });
    const id = Number(req.params.id || 0);
    if (!id) return safeJson(res, 400, { ok: false, message: "id inválido" });

    const isActive = req.body?.isActive;
    if (typeof isActive !== "boolean") return safeJson(res, 400, { ok: false, message: "isActive requerido (boolean)" });

    await dbQuery(`UPDATE msg_users SET is_active=$2, updated_at=NOW() WHERE id=$1`, [id, isActive]);
    return safeJson(res, 200, { ok: true });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.patch("/api/admin/messaging/users/:id/pin", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });
    const id = Number(req.params.id || 0);
    const pin = String(req.body?.pin || "").trim();
    if (!id) return safeJson(res, 400, { ok: false, message: "id inválido" });
    if (!pin) return safeJson(res, 400, { ok: false, message: "pin requerido" });

    const pinHash = hashPin(pin);
    await dbQuery(`UPDATE msg_users SET pin_hash=$2, updated_at=NOW() WHERE id=$1`, [id, pinHash]);
    return safeJson(res, 200, { ok: true });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================
   USER (SOLICITANTE): CREATE REQUEST + LIST MY REQUESTS
========================= */
app.post("/api/user/requests", verifyUser, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const userId = Number(req.user.userId);
    const uR = await dbQuery(
      `SELECT id, username, full_name, department, email, phone, is_active, role
       FROM msg_users WHERE id=$1 LIMIT 1`,
      [userId]
    );
    const u = uR.rows?.[0];
    if (!u || !u.is_active) return safeJson(res, 401, { ok: false, message: "Usuario inactivo" });

    if (isCourierRole(u.role)) {
      return safeJson(res, 403, { ok: false, message: "Mensajero no crea solicitudes desde este módulo." });
    }

    const requestType = String(req.body?.requestType || "").trim();
    const contactPersonPhone = String(req.body?.contactPersonPhone || "").trim();
    const addressDetails = String(req.body?.addressDetails || "").trim();
    const description = String(req.body?.description || "").trim();
    const priority = String(req.body?.priority || "Media").trim();

    if (!requestType) return safeJson(res, 400, { ok: false, message: "Falta requestType" });
    if (!contactPersonPhone) return safeJson(res, 400, { ok: false, message: "Falta contactPersonPhone" });
    if (!addressDetails) return safeJson(res, 400, { ok: false, message: "Falta addressDetails" });
    if (!description) return safeJson(res, 400, { ok: false, message: "Falta description" });

    // ✅ Auto-asignación a Victor
    const autoCourier = await getAutoAssignCourier();
    if (!autoCourier && String(AUTO_ASSIGN_STRICT) === "1") {
      return safeJson(res, 500, { ok: false, message: "Auto-asignación activa pero no existe mensajero 'victor' (activo)." });
    }
    const assignedToUserId = autoCourier ? autoCourier.id : null;
    const assignedToName = autoCourier ? autoCourier.name : "";

    const r = await dbQuery(
      `INSERT INTO msg_requests(
        created_by_user_id, created_by_username,
        requester_name, requester_department, requester_email, requester_phone,
        request_type, contact_person_phone, address_details, description, priority,
        status, status_updated_at, updated_at,
        assigned_to_user_id, assigned_to_name, assigned_at,
        courier_comment, closed_at
      )
      VALUES(
        $1,$2,
        $3,$4,$5,$6,
        $7,$8,$9,$10,$11,
        'open', NOW(), NOW(),
        $12,$13, CASE WHEN $12::bigint IS NULL THEN NULL ELSE NOW() END,
        '', NULL
      )
      RETURNING
        id,
        ${fmtTsSql("created_at")} AS created_at_local,
        ${fmtTsSql("status_updated_at")} AS status_updated_at_local`,
      [
        Number(u.id),
        String(u.username || ""),
        String(u.full_name || ""),
        String(u.department || ""),
        String(u.email || ""),
        String(u.phone || ""),
        requestType,
        contactPersonPhone,
        addressDetails,
        description,
        priority,
        assignedToUserId,
        assignedToName,
      ]
    );

    const newId = Number(r.rows?.[0]?.id || 0);
    const createdAtLocal = r.rows?.[0]?.created_at_local || null;
    const statusUpdatedAtLocal = r.rows?.[0]?.status_updated_at_local || null;

    // ✅ Notificación: creada (solicitante + mensajeros + supervisores + buzón)
    try {
      const notifyTo = await buildNotifyRecipientsForCouriersAndSupervisors();
      void notifyViaGAS({
        event: "created",
        requesterEmail: String(u.email || ""),
        notifyTo,
        data: {
          id: newId,
          status: "open",
          requestType,
          priority,
          department: String(u.department || ""),
          requesterName: String(u.full_name || ""),
          contactPersonPhone,
          addressDetails,
          description,
          assignedToName: assignedToName || "",
          courierComment: "",
          createdAt: createdAtLocal,
          closedAt: null,
          statusUpdatedAt: statusUpdatedAtLocal,
        },
      });
    } catch (_) {}

    return safeJson(res, 200, { ok: true, id: newId });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get("/api/user/requests", verifyUser, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const userId = Number(req.user.userId);
    const status = normStatus(req.query?.status || "");

    const where = [`created_by_user_id=$1`];
    const params = [userId];
    let p = 2;

    if (status && status !== "__all__") {
      where.push(`status=$${p++}`);
      params.push(status);
    }

    const r = await dbQuery(
      `SELECT
        id,
        ${fmtTsSql("created_at")} AS created_at_local,
        request_type, priority, status,
        assigned_to_name, assigned_to_user_id, address_details,
        courier_comment,
        ${fmtTsSql("closed_at")} AS closed_at_local
       FROM msg_requests
       WHERE ${where.join(" AND ")}
       ORDER BY created_at DESC
       LIMIT 500`,
      params
    );

    const requests = (r.rows || []).map((x) => ({
      id: Number(x.id),
      createdAt: x.created_at_local || null,
      requestType: x.request_type,
      priority: x.priority,
      status: x.status,
      assignedToName: x.assigned_to_name || "",
      assignedToUserId: x.assigned_to_user_id != null ? Number(x.assigned_to_user_id) : null,
      addressDetails: x.address_details || "",
      courierComment: x.courier_comment || "",
      closedAt: x.closed_at_local || null,
    }));

    return safeJson(res, 200, { ok: true, requests });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================
   USER (MENSAJERO): LIST + UPDATE
========================= */

// GET /api/user/courier/requests?scope=assigned|all&status=open|...
app.get("/api/user/courier/requests", verifyUser, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const userId = Number(req.user.userId);

    const uR = await dbQuery(
      `SELECT id, role, is_active FROM msg_users WHERE id=$1 LIMIT 1`,
      [userId]
    );
    const u = uR.rows?.[0];
    if (!u || !u.is_active) return safeJson(res, 401, { ok: false, message: "Usuario inactivo" });
    if (!isCourierRole(u.role)) return safeJson(res, 403, { ok: false, message: "Solo mensajeros." });

    const scope = String(req.query?.scope || "assigned").trim().toLowerCase(); // assigned | all
    const status = normStatus(req.query?.status || "");

    const where = [];
    const params = [];
    let p = 1;

    if (scope !== "all") {
      where.push(`r.assigned_to_user_id=$${p++}`);
      params.push(userId);
    }

    if (status && status !== "__all__") {
      where.push(`r.status=$${p++}`);
      params.push(status);
    }

    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

    const r = await dbQuery(
      `SELECT
        r.id,
        ${fmtTsSql("r.created_at")} AS created_at_local,
        ${fmtTsSql("r.updated_at")} AS updated_at_local,
        r.created_by_username,
        r.requester_name, r.requester_department,
        r.request_type, r.priority, r.status,
        r.contact_person_phone, r.address_details, r.description,
        r.assigned_to_user_id, r.assigned_to_name,
        r.courier_comment,
        ${fmtTsSql("r.closed_at")} AS closed_at_local
       FROM msg_requests r
       ${whereSql}
       ORDER BY r.created_at DESC
       LIMIT 500`,
      params
    );

    const requests = (r.rows || []).map((x) => ({
      id: Number(x.id),
      createdAt: x.created_at_local || null,
      updatedAt: x.updated_at_local || null,
      createdByUsername: x.created_by_username || "",
      requesterName: x.requester_name || "",
      department: x.requester_department || "",
      requestType: x.request_type || "",
      priority: x.priority || "",
      status: x.status || "open",
      contactPersonPhone: x.contact_person_phone || "",
      addressDetails: x.address_details || "",
      description: x.description || "",
      assignedToUserId: x.assigned_to_user_id != null ? Number(x.assigned_to_user_id) : null,
      assignedToName: x.assigned_to_name || "",
      courierComment: x.courier_comment || "",
      closedAt: x.closed_at_local || null,
    }));

    return safeJson(res, 200, { ok: true, scope, status: status || "__ALL__", requests });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

// PATCH /api/user/courier/requests/:id  { status, comment }
app.patch("/api/user/courier/requests/:id", verifyUser, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const userId = Number(req.user.userId);
    const id = Number(req.params.id || 0);
    if (!id) return safeJson(res, 400, { ok: false, message: "id inválido" });

    // validar usuario courier
    const uR = await dbQuery(`SELECT id, role, is_active FROM msg_users WHERE id=$1 LIMIT 1`, [userId]);
    const u = uR.rows?.[0];
    if (!u || !u.is_active) return safeJson(res, 401, { ok: false, message: "Usuario inactivo" });
    if (!isCourierRole(u.role)) return safeJson(res, 403, { ok: false, message: "Solo mensajeros." });

    // validar que la solicitud esté asignada a este mensajero (seguridad)
    const curR = await dbQuery(
      `SELECT id, assigned_to_user_id, status, courier_comment
       FROM msg_requests WHERE id=$1 LIMIT 1`,
      [id]
    );
    const cur = curR.rows?.[0];
    if (!cur) return safeJson(res, 404, { ok: false, message: "Solicitud no encontrada" });

    const assignedTo = cur.assigned_to_user_id != null ? Number(cur.assigned_to_user_id) : null;
    if (!assignedTo || assignedTo !== userId) {
      return safeJson(res, 403, { ok: false, message: "Solo puedes actualizar solicitudes asignadas a ti." });
    }

    const oldStatus = String(cur.status || "");
    const oldComment = String(cur.courier_comment || "");

    const status = normStatus(req.body?.status || "");
    const comment = String(req.body?.comment || "").trim();

    if (!["open", "in_progress", "closed", "cancelled"].includes(status)) {
      return safeJson(res, 400, { ok: false, message: "status inválido" });
    }

    const closedAtSql = status === "closed" ? "NOW()" : "NULL";

    await dbQuery(
      `UPDATE msg_requests
       SET status=$2,
           courier_comment=$3,
           closed_at=${closedAtSql},
           status_updated_at=NOW(),
           updated_at=NOW()
       WHERE id=$1`,
      [id, status, comment]
    );

    // Notificación si cambió estado o comentario
    const changed = (oldStatus !== status) || (oldComment !== comment);
    if (changed) {
      const reqR = await dbQuery(
        `SELECT
          id, requester_email, requester_name, requester_department,
          request_type, priority, contact_person_phone, address_details, description,
          assigned_to_name, courier_comment,
          ${fmtTsSql("created_at")} AS created_at_local,
          ${fmtTsSql("closed_at")} AS closed_at_local,
          ${fmtTsSql("status_updated_at")} AS status_updated_at_local
         FROM msg_requests
         WHERE id=$1 LIMIT 1`,
        [id]
      );
      const reqRow = reqR.rows?.[0];
      if (reqRow) {
        const notifyTo = await buildNotifyRecipientsForCouriersAndSupervisors();
        void notifyViaGAS({
          event: "status_changed",
          requesterEmail: reqRow.requester_email || "",
          notifyTo,
          data: {
            id,
            oldStatus,
            newStatus: status,
            status,
            requestType: reqRow.request_type,
            priority: reqRow.priority,
            department: reqRow.requester_department,
            requesterName: reqRow.requester_name,
            contactPersonPhone: reqRow.contact_person_phone,
            addressDetails: reqRow.address_details,
            description: reqRow.description,
            assignedToName: reqRow.assigned_to_name || "",
            courierComment: comment || reqRow.courier_comment || "",
            createdAt: reqRow.created_at_local || null,
            closedAt: reqRow.closed_at_local || null,
            statusUpdatedAt: reqRow.status_updated_at_local || null,
          },
        });
      }
    }

    return safeJson(res, 200, { ok: true });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================
   ADMIN: REQUESTS LIST + ASSIGN + STATUS
========================= */
app.get("/api/admin/messaging/requests", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const pageRaw = Number(req.query?.page || 1);
    const limitRaw = Number(req.query?.limit || 20);
    const page = Math.max(1, Number.isFinite(pageRaw) ? Math.trunc(pageRaw) : 1);
    const limit = Math.max(1, Math.min(200, Number.isFinite(limitRaw) ? Math.trunc(limitRaw) : 20));
    const skip = (page - 1) * limit;

    const status = normStatus(req.query?.status || "");
    const department = String(req.query?.department || "").trim();
    const q = String(req.query?.q || "").trim().toLowerCase();

    const where = [];
    const params = [];
    let p = 1;

    if (status && status !== "__all__") {
      where.push(`r.status=$${p++}`);
      params.push(status);
    }
    if (department && department !== "__ALL__") {
      where.push(`r.requester_department=$${p++}`);
      params.push(department);
    }
    if (q) {
      where.push(
        `(CAST(r.id AS TEXT) ILIKE $${p++} OR LOWER(COALESCE(r.created_by_username,'')) ILIKE $${p++} OR LOWER(COALESCE(r.description,'')) ILIKE $${p++})`
      );
      params.push(`%${q}%`, `%${q}%`, `%${q}%`);
    }

    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

    const totalR = await dbQuery(`SELECT COUNT(*)::int AS total FROM msg_requests r ${whereSql}`, params);
    const total = Number(totalR.rows?.[0]?.total || 0);

    const dataR = await dbQuery(
      `SELECT
        r.id,
        ${fmtTsSql("r.created_at")} AS created_at_local,
        ${fmtTsSql("r.updated_at")} AS updated_at_local,
        r.created_by_username,
        r.requester_name, r.requester_department,
        r.request_type, r.priority, r.status,
        r.contact_person_phone, r.address_details,
        r.assigned_to_user_id, r.assigned_to_name,
        r.courier_comment,
        ${fmtTsSql("r.closed_at")} AS closed_at_local
       FROM msg_requests r
       ${whereSql}
       ORDER BY r.created_at DESC
       OFFSET $${p++} LIMIT $${p++}`,
      [...params, skip, limit]
    );

    const requests = (dataR.rows || []).map((r) => ({
      id: Number(r.id),
      createdAt: r.created_at_local || null,
      updatedAt: r.updated_at_local || null,
      createdByUsername: r.created_by_username,
      requesterName: r.requester_name,
      department: r.requester_department,
      requestType: r.request_type,
      priority: r.priority,
      status: r.status,
      contactPersonPhone: r.contact_person_phone || "",
      addressDetails: r.address_details || "",
      assignedToUserId: r.assigned_to_user_id != null ? Number(r.assigned_to_user_id) : null,
      assignedToName: r.assigned_to_name || "",
      courierComment: r.courier_comment || "",
      closedAt: r.closed_at_local || null,
    }));

    return safeJson(res, 200, { ok: true, total, page, limit, requests });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.patch("/api/admin/messaging/requests/:id/assign", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const id = Number(req.params.id || 0);
    if (!id) return safeJson(res, 400, { ok: false, message: "id inválido" });

    const courierUserIdRaw = req.body?.courierUserId;
    const courierUserId = courierUserIdRaw == null ? null : Number(courierUserIdRaw);

    let assignedName = "";
    if (courierUserId) {
      const uR = await dbQuery(
        `SELECT id, full_name, role, is_active FROM msg_users WHERE id=$1 LIMIT 1`,
        [courierUserId]
      );
      const u = uR.rows?.[0];
      if (!u || !u.is_active || !isCourierRole(u.role)) {
        return safeJson(res, 400, { ok: false, message: "Mensajero inválido/inactivo" });
      }
      assignedName = String(u.full_name || "");
    }

    await dbQuery(
      `UPDATE msg_requests
       SET assigned_to_user_id=$2,
           assigned_to_name=$3,
           assigned_at=CASE WHEN $2::bigint IS NULL THEN NULL ELSE NOW() END,
           updated_at=NOW()
       WHERE id=$1`,
      [id, courierUserId, assignedName]
    );

    // Notificación: asignación
    try {
      const reqR = await dbQuery(
        `SELECT
           id, requester_email, requester_name, requester_department,
           request_type, priority, contact_person_phone, address_details, description,
           assigned_to_name,
           ${fmtTsSql("created_at")} AS created_at_local,
           ${fmtTsSql("closed_at")} AS closed_at_local,
           ${fmtTsSql("status_updated_at")} AS status_updated_at_local
         FROM msg_requests WHERE id=$1 LIMIT 1`,
        [id]
      );
      const reqRow = reqR.rows?.[0];
      if (reqRow) {
        const notifyTo = await buildNotifyRecipientsForCouriersAndSupervisors();
        void notifyViaGAS({
          event: "assigned",
          requesterEmail: reqRow.requester_email || "",
          notifyTo,
          data: {
            id,
            requestType: reqRow.request_type,
            priority: reqRow.priority,
            department: reqRow.requester_department,
            requesterName: reqRow.requester_name,
            contactPersonPhone: reqRow.contact_person_phone,
            addressDetails: reqRow.address_details,
            description: reqRow.description,
            assignedToName: reqRow.assigned_to_name || "",
            createdAt: reqRow.created_at_local || null,
            closedAt: reqRow.closed_at_local || null,
            statusUpdatedAt: reqRow.status_updated_at_local || null,
          },
        });
      }
    } catch (_) {}

    return safeJson(res, 200, { ok: true });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.patch("/api/admin/messaging/requests/:id/status", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const id = Number(req.params.id || 0);
    if (!id) return safeJson(res, 400, { ok: false, message: "id inválido" });

    const status = normStatus(req.body?.status || "");
    if (!["open", "in_progress", "closed", "cancelled"].includes(status)) {
      return safeJson(res, 400, { ok: false, message: "status inválido" });
    }

    const beforeR = await dbQuery(
      `SELECT
        id, status, requester_email, requester_name, requester_department,
        request_type, priority, contact_person_phone, address_details, description,
        assigned_to_name, courier_comment,
        ${fmtTsSql("created_at")} AS created_at_local,
        ${fmtTsSql("closed_at")} AS closed_at_local,
        ${fmtTsSql("status_updated_at")} AS status_updated_at_local
       FROM msg_requests
       WHERE id=$1 LIMIT 1`,
      [id]
    );
    const before = beforeR.rows?.[0];
    if (!before) return safeJson(res, 404, { ok: false, message: "Solicitud no encontrada" });

    const oldStatus = String(before.status || "");
    const closedAtSql = status === "closed" ? "NOW()" : "NULL";

    await dbQuery(
      `UPDATE msg_requests
       SET status=$2, status_updated_at=NOW(), updated_at=NOW(), closed_at=${closedAtSql}
       WHERE id=$1`,
      [id, status]
    );

    // Notificación: cambio de estado (admin)
    if (oldStatus !== status) {
      try {
        const afterR = await dbQuery(
          `SELECT
             ${fmtTsSql("created_at")} AS created_at_local,
             ${fmtTsSql("closed_at")} AS closed_at_local,
             ${fmtTsSql("status_updated_at")} AS status_updated_at_local
           FROM msg_requests WHERE id=$1 LIMIT 1`,
          [id]
        );
        const after = afterR.rows?.[0] || {};

        const notifyTo = await buildNotifyRecipientsForCouriersAndSupervisors();
        void notifyViaGAS({
          event: "status_changed",
          requesterEmail: before.requester_email || "",
          notifyTo,
          data: {
            id,
            oldStatus,
            newStatus: status,
            status,
            requestType: before.request_type,
            priority: before.priority,
            department: before.requester_department,
            requesterName: before.requester_name,
            contactPersonPhone: before.contact_person_phone,
            addressDetails: before.address_details,
            description: before.description,
            assignedToName: before.assigned_to_name || "",
            courierComment: before.courier_comment || "",
            createdAt: after.created_at_local || before.created_at_local || null,
            closedAt: after.closed_at_local || null,
            statusUpdatedAt: after.status_updated_at_local || null,
          },
        });
      } catch (_) {}
    }

    return safeJson(res, 200, { ok: true });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================
   ADMIN: DASHBOARD
========================= */
app.get("/api/admin/messaging/dashboard", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const today = new Date();
    const defaultFrom = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, "0")}-01`;
    const defaultTo = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, "0")}-${String(today.getDate()).padStart(2, "0")}`;

    const from = isISODate(req.query?.from) ? String(req.query.from) : defaultFrom;
    const to = isISODate(req.query?.to) ? String(req.query.to) : defaultTo;
    const department = String(req.query?.department || "").trim();

    const where = [`r.created_at::date BETWEEN $1::date AND $2::date`];
    const params = [from, to];
    let p = 3;

    if (department && department !== "__ALL__") {
      where.push(`r.requester_department=$${p++}`);
      params.push(department);
    }

    const whereSql = `WHERE ${where.join(" AND ")}`;

    const totR = await dbQuery(`SELECT COUNT(*)::int AS total FROM msg_requests r ${whereSql}`, params);
    const totals = { total: Number(totR.rows?.[0]?.total || 0) };

    const byStatusR = await dbQuery(
      `SELECT r.status, COUNT(*)::int AS c
       FROM msg_requests r
       ${whereSql}
       GROUP BY r.status
       ORDER BY c DESC`,
      params
    );

    const statusLabel = (s) => {
      const t = String(s || "");
      if (t === "open") return "Abiertas";
      if (t === "in_progress") return "En progreso";
      if (t === "closed") return "Cerradas";
      if (t === "cancelled") return "Canceladas";
      return t;
    };

    const byStatus = (byStatusR.rows || []).map((x) => ({
      status: x.status,
      statusLabel: statusLabel(x.status),
      count: Number(x.c || 0),
    }));

    const byDeptR = await dbQuery(
      `SELECT COALESCE(NULLIF(r.requester_department,''),'(Sin depto)') AS d, COUNT(*)::int AS c
       FROM msg_requests r
       ${whereSql}
       GROUP BY 1
       ORDER BY c DESC`,
      params
    );
    const byDepartment = (byDeptR.rows || []).map((x) => ({
      department: x.d,
      count: Number(x.c || 0),
    }));

    const byDayR = await dbQuery(
      `SELECT to_char(r.created_at::date,'YYYY-MM-DD') AS day, COUNT(*)::int AS c
       FROM msg_requests r
       ${whereSql}
       GROUP BY r.created_at::date
       ORDER BY r.created_at::date ASC`,
      params
    );
    const byDay = (byDayR.rows || []).map((x) => ({
      day: x.day,
      count: Number(x.c || 0),
    }));

    return safeJson(res, 200, {
      ok: true,
      from,
      to,
      department: department || "__ALL__",
      totals,
      byStatus,
      byDepartment,
      byDay,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

__extraBootTasks.push(async () => {
  try {
    await ensureDb();
  } catch (e) {
    console.error("Messaging DB init error:", e.message || String(e));
  }
});
}


/* =========================================================
   Módulo integrado: Estratificación
========================================================= */
{
// prodima-pay/src/apps/estratificacion/server.js

const { Pool } = pg;

app.use(express.json({ limit: "6mb" }));

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

  // CORS_ORIGIN=https://prodima.com.pa,https://www.prodima.com.pa
  CORS_ORIGIN = "",
} = process.env;

/* =========================================================
   ✅ GRUPOS (TU REGLA DE NEGOCIO)
========================================================= */
const GROUPS_CONS = new Set([
  "Prod. De Limpieza",
  "Cuidado De La Ropa",
  "Sazonadores",
  "Art. De Limpieza",
  "Vinagres",
  "Especialidades y GMT",
]);

const GROUPS_RCI = new Set([
  "Equip. Y Acces. Agua",
  "Químicos Piscina",
  "Servicios",
  "Químicos Trat. Agua",
  "Equip. Y Acces. Pisc",
  "M.P.Res.Comer.Ind.",
]);

/* =========================================================
   ✅ NORMALIZACIÓN + CANONICALIZACIÓN (+ HEURÍSTICA)
========================================================= */
function normGroupName(s) {
  return String(s || "")
    .trim()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "") // quita acentos
    .replace(/\s+/g, " ")
    .toUpperCase();
}

const GROUPS_CONS_N = new Set(Array.from(GROUPS_CONS).map(normGroupName));
const GROUPS_RCI_N = new Set(Array.from(GROUPS_RCI).map(normGroupName));

const CANON_GROUP = new Map(
  [...Array.from(GROUPS_CONS), ...Array.from(GROUPS_RCI)].map((g) => [normGroupName(g), g])
);

function canonicalGroupName(groupName) {
  const n = normGroupName(groupName);
  return CANON_GROUP.get(n) || String(groupName || "").trim();
}

/**
 * ✅ HEURÍSTICA: fuerza variantes del SAP a tus 12 grupos
 * (resuelve la mayoría de "Sin grupo" por abreviaturas/puntos/mayúsculas)
 */
function guessCanonicalGroupName(groupNameRaw) {
  const n = normGroupName(groupNameRaw);
  if (!n) return "";

  // ---- RCI ----
  if (n.includes("EQUIP") && n.includes("AGUA")) return "Equip. Y Acces. Agua";
  if (n.includes("EQUIP") && (n.includes("PISC") || n.includes("PISCINA"))) return "Equip. Y Acces. Pisc";

  if ((n.includes("QUIM") || n.includes("QUIMIC")) && (n.includes("PISC") || n.includes("PISCINA")))
    return "Químicos Piscina";

  if ((n.includes("QUIM") || n.includes("QUIMIC")) && n.includes("TRAT") && n.includes("AGUA"))
    return "Químicos Trat. Agua";

  if (n.includes("SERV")) return "Servicios";

  if (n.includes("M.P") || (n.includes("MP") && (n.includes("COMER") || n.includes("IND"))))
    return "M.P.Res.Comer.Ind.";

  // ---- CONS ----
  if (n.includes("SAZON")) return "Sazonadores";
  if (n.includes("VINAGR")) return "Vinagres";
  if (n.includes("ROPA")) return "Cuidado De La Ropa";

  if (n.includes("LIMPIEZ") || n.includes("LIMPIEZA") || n.includes("CLEAN")) {
    if (n.includes("ART") || n.includes("ESPON") || n.includes("PANO") || n.includes("PAÑO")) return "Art. De Limpieza";
    return "Prod. De Limpieza";
  }

  if (n.includes("ESPECIAL") || n.includes("GMT")) return "Especialidades y GMT";

  return "";
}

function normalizeGrupoFinal(grupoRaw) {
  const raw = String(grupoRaw || "").trim();
  if (!raw) return "Sin grupo";

  const canon = canonicalGroupName(raw);
  const canonN = normGroupName(canon);

  // si ya está en tus listas, perfecto
  if (GROUPS_CONS_N.has(canonN) || GROUPS_RCI_N.has(canonN)) return canon;

  // si no, prueba heurística
  const guessed = guessCanonicalGroupName(raw);
  if (guessed) return guessed;

  return canon || "Sin grupo";
}

function inferAreaFromGroup(groupName) {
  const g = normGroupName(groupName);
  if (!g) return "";
  if (GROUPS_CONS_N.has(g)) return "CONS";
  if (GROUPS_RCI_N.has(g)) return "RCI";
  return "";
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
function isISO(s) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(s || ""));
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

/* =========================================================
   ✅ Postgres (Supabase)
========================================================= */
const pool = new Pool({
  connectionString: DATABASE_URL || undefined,
  ssl: DATABASE_URL ? { rejectUnauthorized: false } : undefined,
});

function hasDb() {
  return Boolean(DATABASE_URL);
}
async function dbQuery(text, params = []) {
  return pool.query(text, params);
}

/**
 * ✅ Tablas usadas por Estratificación
 */
async function ensureDb() {
  if (!hasDb()) return;

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS sales_item_lines (
      doc_entry INTEGER NOT NULL,
      line_num  INTEGER NOT NULL,
      doc_type  TEXT NOT NULL, -- 'INV' o 'CRN'
      doc_date  DATE NOT NULL,
      item_code TEXT NOT NULL DEFAULT '',
      item_desc TEXT NOT NULL DEFAULT '',
      quantity  NUMERIC(18,4) NOT NULL DEFAULT 0,
      revenue   NUMERIC(18,2) NOT NULL DEFAULT 0,
      gross_profit NUMERIC(18,2) NOT NULL DEFAULT 0,
      item_group TEXT DEFAULT '',
      area TEXT DEFAULT '',
      warehouse TEXT DEFAULT '',
      updated_at TIMESTAMP DEFAULT NOW(),
      PRIMARY KEY (doc_entry, line_num, doc_type)
    );
  `);

  await dbQuery(`ALTER TABLE sales_item_lines ADD COLUMN IF NOT EXISTS doc_num INTEGER;`);

  /* ✅ NUEVO: cliente en líneas (para el modal) */
  await dbQuery(`ALTER TABLE sales_item_lines ADD COLUMN IF NOT EXISTS card_code TEXT DEFAULT '';`);
  await dbQuery(`ALTER TABLE sales_item_lines ADD COLUMN IF NOT EXISTS card_name TEXT DEFAULT '';`);

  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_sales_item_date ON sales_item_lines(doc_date);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_sales_item_code ON sales_item_lines(item_code);`);

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS inv_item_cache (
      item_code TEXT PRIMARY KEY,
      item_desc TEXT NOT NULL DEFAULT '',
      stock NUMERIC(18,4) NOT NULL DEFAULT 0,
      stock_min NUMERIC(18,4) NOT NULL DEFAULT 0,
      stock_max NUMERIC(18,4) NOT NULL DEFAULT 0,
      committed NUMERIC(18,4) NOT NULL DEFAULT 0,
      ordered NUMERIC(18,4) NOT NULL DEFAULT 0,
      available NUMERIC(18,4) NOT NULL DEFAULT 0,
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS item_group_cache (
      item_code TEXT PRIMARY KEY,
      group_name TEXT NOT NULL DEFAULT '',
      area TEXT NOT NULL DEFAULT '',
      grupo TEXT NOT NULL DEFAULT '',
      item_desc TEXT NOT NULL DEFAULT '',
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await dbQuery(`ALTER TABLE item_group_cache ADD COLUMN IF NOT EXISTS group_name TEXT NOT NULL DEFAULT '';`);
  await dbQuery(`ALTER TABLE item_group_cache ADD COLUMN IF NOT EXISTS area TEXT NOT NULL DEFAULT '';`);
  await dbQuery(`ALTER TABLE item_group_cache ADD COLUMN IF NOT EXISTS grupo TEXT NOT NULL DEFAULT '';`);
  await dbQuery(`ALTER TABLE item_group_cache ADD COLUMN IF NOT EXISTS item_desc TEXT NOT NULL DEFAULT '';`);
  await dbQuery(`ALTER TABLE item_group_cache ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();`);

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS sync_state (
      k TEXT PRIMARY KEY,
      v TEXT NOT NULL,
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);
}

async function setState(k, v) {
  if (!hasDb()) return;
  await dbQuery(
    `INSERT INTO sync_state(k,v,updated_at) VALUES($1,$2,NOW())
     ON CONFLICT(k) DO UPDATE SET v=EXCLUDED.v, updated_at=NOW()`,
    [k, String(v)]
  );
}
async function getState(k) {
  if (!hasDb()) return "";
  const r = await dbQuery(`SELECT v FROM sync_state WHERE k=$1 LIMIT 1`, [k]);
  return r.rows?.[0]?.v || "";
}

/* =========================================================
   ✅ SAP Service Layer (cookie + timeout)
========================================================= */
let _fetch = globalThis.fetch || null;
async function httpFetch(url, options) {
  if (_fetch) return _fetch(url, options);
  const mod = await import("node-fetch");
  _fetch = mod.default;
  return _fetch(url, options);
}

let SL_COOKIE = "";
let SL_COOKIE_AT = 0;

async function slLogin() {
  const base = SAP_BASE_URL.replace(/\/$/, "");
  const url = `${base}/Login`;
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
  const timeoutMs = Number(options.timeoutMs || 60000);
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
   ✅ Sync: Ventas netas por item (INV - CRN)
========================================================= */
function pickGrossProfit(ln) {
  const candidates = [ln?.GrossProfit, ln?.GrossProfitTotal, ln?.GrossProfitFC, ln?.GrossProfitSC];
  for (const c of candidates) {
    const n = Number(c);
    if (Number.isFinite(n)) return n;
  }
  return 0;
}

async function scanDocHeaders(entity, { from, to, maxDocs = 2500 }) {
  const toPlus1 = addDaysISO(to, 1);
  const batchTop = 200;
  let skipSap = 0;
  const out = [];

  for (let page = 0; page < 300; page++) {
    const raw = await slFetch(
      `/${entity}?$select=DocEntry,DocNum,DocDate&` +
        `$filter=${encodeURIComponent(`DocDate ge '${from}' and DocDate lt '${toPlus1}'`)}&` +
        `$orderby=DocDate asc,DocEntry asc&$top=${batchTop}&$skip=${skipSap}`,
      { timeoutMs: 120000 }
    );

    const rows = Array.isArray(raw?.value) ? raw.value : [];
    if (!rows.length) break;
    skipSap += rows.length;

    for (const r of rows) {
      out.push({
        DocEntry: Number(r.DocEntry),
        DocNum: r.DocNum != null ? Number(r.DocNum) : null,
        DocDate: String(r.DocDate || "").slice(0, 10),
      });
      if (out.length >= maxDocs) return out;
    }
  }
  return out;
}

async function getDoc(entity, docEntry) {
  const de = Number(docEntry);
  if (!Number.isFinite(de) || de <= 0) return null;
  return slFetch(`/${entity}(${de})`, { timeoutMs: 180000 });
}

async function upsertSalesLines(docType, docDate, docFull, sign) {
  const docEntry = Number(docFull?.DocEntry || 0);
  const docNum = docFull?.DocNum != null ? Number(docFull.DocNum) : null;

  /* ✅ NUEVO: cliente en líneas */
  const cardCode = String(docFull?.CardCode || "").trim();
  const cardName = String(docFull?.CardName || "").trim();

  const lines = Array.isArray(docFull?.DocumentLines) ? docFull.DocumentLines : [];
  if (!docEntry || !lines.length) return 0;

  let inserted = 0;

  for (const ln of lines) {
    const lineNum = Number(ln?.LineNum);
    if (!Number.isFinite(lineNum)) continue;

    const itemCode = String(ln?.ItemCode || "").trim();
    if (!itemCode) continue;

    const itemDesc = String(ln?.ItemDescription || ln?.ItemName || "").trim();

    // ✅ FIX NC: ABS + signo para que CRN siempre reste
    const qtyRaw = Number(ln?.Quantity || 0);
    const revRaw = Number(ln?.LineTotal || 0);
    const gpRaw = Number(pickGrossProfit(ln) || 0);

    const qty = Math.abs(qtyRaw) * sign;
    const rev = Math.abs(revRaw) * sign;
    const gp = Math.abs(gpRaw) * sign;

    await dbQuery(
      `
      INSERT INTO sales_item_lines(
        doc_entry,line_num,doc_type,doc_date,doc_num,card_code,card_name,
        item_code,item_desc,quantity,revenue,gross_profit,updated_at
      )
      VALUES($1,$2,$3,$4::date,$5,$6,$7,$8,$9,$10,$11,$12,NOW())
      ON CONFLICT(doc_entry,line_num,doc_type) DO UPDATE SET
        doc_date=EXCLUDED.doc_date,
        doc_num=EXCLUDED.doc_num,
        card_code=EXCLUDED.card_code,
        card_name=EXCLUDED.card_name,
        item_code=EXCLUDED.item_code,
        item_desc=EXCLUDED.item_desc,
        quantity=EXCLUDED.quantity,
        revenue=EXCLUDED.revenue,
        gross_profit=EXCLUDED.gross_profit,
        updated_at=NOW()
      `,
      [docEntry, lineNum, docType, docDate, docNum, cardCode, cardName, itemCode, itemDesc, qty, rev, gp]
    );

    inserted++;
    if (inserted % 200 === 0) await sleep(10);
  }

  return inserted;
}

async function syncSales({ from, to, maxDocs = 2500 }) {
  let saved = 0;

  const invHeaders = await scanDocHeaders("Invoices", { from, to, maxDocs });
  for (const h of invHeaders) {
    try {
      const full = await getDoc("Invoices", h.DocEntry);
      saved += await upsertSalesLines("INV", h.DocDate, full, +1);
    } catch {}
    await sleep(10);
  }

  const crnHeaders = await scanDocHeaders("CreditNotes", { from, to, maxDocs });
  for (const h of crnHeaders) {
    try {
      const full = await getDoc("CreditNotes", h.DocEntry);
      saved += await upsertSalesLines("CRN", h.DocDate, full, -1);
    } catch {}
    await sleep(10);
  }

  return saved;
}

/* =========================================================
   ✅ Inventario (solo bodegas 300,200,500,01)
========================================================= */
const INV_WH_ALLOWED = new Set(["300", "200", "500", "01"]);

function sumInvFromWarehouseInfo(infoArr) {
  const rowsAll = Array.isArray(infoArr) ? infoArr : [];
  const rows = rowsAll.filter((w) => INV_WH_ALLOWED.has(String(w?.WarehouseCode ?? w?.WhsCode ?? "").trim()));

  let stock = 0;
  let committed = 0;
  let ordered = 0;

  let stockMin = 0;
  let stockMax = 0;

  for (const w of rows) {
    stock += Number(w?.InStock ?? w?.OnHand ?? 0);
    committed += Number(w?.Committed ?? w?.IsCommited ?? 0);
    ordered += Number(w?.Ordered ?? w?.OnOrder ?? 0);

    const mn = Number(w?.MinimalStock ?? w?.MinStock ?? 0);
    const mx = Number(w?.MaximalStock ?? w?.MaxStock ?? 0);
    if (Number.isFinite(mn) && mn > stockMin) stockMin = mn;
    if (Number.isFinite(mx) && mx > stockMax) stockMax = mx;
  }

  const available = stock - committed + ordered;

  return { stock, committed, ordered, available, stockMin, stockMax };
}

async function getInventoryForItemCode(code) {
  const itemCode = String(code || "").trim();
  if (!itemCode) return null;

  const safe = itemCode.replace(/'/g, "''");
  const a = await slFetch(`/Items('${safe}')?$select=ItemCode,ItemName,ItemWarehouseInfoCollection`, { timeoutMs: 120000 });

  const itemName = String(a?.ItemName || "").trim();
  const inv = sumInvFromWarehouseInfo(a?.ItemWarehouseInfoCollection);

  return { itemCode, itemDesc: itemName, ...inv };
}

async function syncInventoryForSalesItems({ from, to, maxItems = 1200 }) {
  const r = await dbQuery(
    `
    SELECT DISTINCT item_code
    FROM sales_item_lines
    WHERE doc_date >= $1::date AND doc_date <= $2::date
      AND item_code <> ''
    LIMIT $3
    `,
    [from, to, Math.max(100, Math.min(5000, Number(maxItems || 1200)))]
  );

  const codes = (r.rows || []).map((x) => String(x.item_code || "").trim()).filter(Boolean);
  let saved = 0;

  for (let i = 0; i < codes.length; i++) {
    const code = codes[i];
    try {
      const inv = await getInventoryForItemCode(code);
      if (!inv) continue;

      await dbQuery(
        `
        INSERT INTO inv_item_cache(item_code,item_desc,stock,stock_min,stock_max,committed,ordered,available,updated_at)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,NOW())
        ON CONFLICT(item_code) DO UPDATE SET
          item_desc=EXCLUDED.item_desc,
          stock=EXCLUDED.stock,
          stock_min=EXCLUDED.stock_min,
          stock_max=EXCLUDED.stock_max,
          committed=EXCLUDED.committed,
          ordered=EXCLUDED.ordered,
          available=EXCLUDED.available,
          updated_at=NOW()
        `,
        [
          inv.itemCode,
          inv.itemDesc || "",
          Number(inv.stock || 0),
          Number(inv.stockMin || 0),
          Number(inv.stockMax || 0),
          Number(inv.committed || 0),
          Number(inv.ordered || 0),
          Number(inv.available || 0),
        ]
      );

      saved++;
    } catch {}
    if ((i + 1) % 25 === 0) await sleep(15);
  }

  return saved;
}

/* =========================================================
   ✅ Sync: Grupos por Item (ItemGroups)
========================================================= */
async function getItemGroupNameFromSap(itemCode) {
  const code = String(itemCode || "").trim();
  if (!code) return { groupName: "", itemName: "", itmsGrpCod: null };

  const safe = code.replace(/'/g, "''");
  const item = await slFetch(`/Items('${safe}')?$select=ItemCode,ItemName,ItemsGroupCode`, { timeoutMs: 90000 });

  const itmsGrpCod = item?.ItemsGroupCode != null ? Number(item.ItemsGroupCode) : null;
  const itemName = String(item?.ItemName || "").trim();

  if (!Number.isFinite(itmsGrpCod) || itmsGrpCod == null) {
    return { groupName: "", itemName, itmsGrpCod: null };
  }

  try {
    const g = await slFetch(`/ItemGroups(${itmsGrpCod})?$select=GroupName`, { timeoutMs: 90000 });
    const groupName = String(g?.GroupName || "").trim();
    return { groupName, itemName, itmsGrpCod };
  } catch {
    return { groupName: "", itemName, itmsGrpCod };
  }
}

async function syncItemGroupsForSalesItems({ from, to, maxItems = 1500 }) {
  const r = await dbQuery(
    `
    SELECT DISTINCT item_code, MAX(NULLIF(item_desc,'')) AS item_desc
    FROM sales_item_lines
    WHERE doc_date >= $1::date AND doc_date <= $2::date
      AND item_code <> ''
    GROUP BY item_code
    LIMIT $3
    `,
    [from, to, Math.max(100, Math.min(8000, Number(maxItems || 1500)))]
  );

  const rows = r.rows || [];
  let saved = 0;

  for (let i = 0; i < rows.length; i++) {
    const code = String(rows[i].item_code || "").trim();
    const descFromSales = String(rows[i].item_desc || "").trim();
    if (!code) continue;

    try {
      const sap = await getItemGroupNameFromSap(code);
      const groupNameRaw = String(sap.groupName || "").trim();

      // ✅ FIX: normaliza (canon + heurística) para evitar "Sin grupo"
      const grupo = normalizeGrupoFinal(groupNameRaw || "");
      const area = inferAreaFromGroup(grupo) || inferAreaFromGroup(groupNameRaw) || "";

      const itemDesc = (String(sap.itemName || "").trim() || descFromSales || "");

      await dbQuery(
        `
        INSERT INTO item_group_cache(item_code,group_name,area,grupo,item_desc,updated_at)
        VALUES($1,$2,$3,$4,$5,NOW())
        ON CONFLICT(item_code) DO UPDATE SET
          group_name=EXCLUDED.group_name,
          area=CASE WHEN EXCLUDED.area <> '' THEN EXCLUDED.area ELSE item_group_cache.area END,
          grupo=CASE WHEN EXCLUDED.grupo <> '' THEN EXCLUDED.grupo ELSE item_group_cache.grupo END,
          item_desc=CASE WHEN EXCLUDED.item_desc <> '' THEN EXCLUDED.item_desc ELSE item_group_cache.item_desc END,
          updated_at=NOW()
        `,
        [code, groupNameRaw, area, grupo, itemDesc]
      );

      saved++;
    } catch {}
    if ((i + 1) % 25 === 0) await sleep(15);
  }

  return saved;
}

/* =========================================================
   ✅ ABC helpers (AHORA A/B/C/D como Excel)
========================================================= */
function abcByMetric(rows, metricKey) {
  const arr = rows
    .map((r) => ({ key: r.itemCode, v: Math.max(0, Number(r[metricKey] || 0)) }))
    .sort((a, b) => b.v - a.v);

  const total = arr.reduce((a, x) => a + x.v, 0) || 0;
  let acc = 0;

  const out = new Map();
  for (const x of arr) {
    acc += x.v;
    const share = total > 0 ? acc / total : 1;
    const letter = share <= 0.8 ? "A" : share <= 0.95 ? "B" : share <= 0.99 ? "C" : "D";
    out.set(x.key, letter);
  }
  return out;
}

function letterScore(l) {
  const L = String(l || "").toUpperCase();
  if (L === "A") return 4;
  if (L === "B") return 3;
  if (L === "C") return 2;
  return 1; // D o vacío
}

function totalFromLetters(a1, a2, a3) {
  const r = letterScore(a1);
  const g = letterScore(a2);
  const p = letterScore(a3);
  const avg = (r + g + p) / 3;
  const t = Math.round(avg * 10) / 10; // 1 decimal como Excel

  if (t >= 3.5) return { label: "AB Crítico", cls: "bad", r, g, p, t };
  if (t >= 2.0) return { label: "C Importante", cls: "warn", r, g, p, t };
  return { label: "D", cls: "ok", r, g, p, t };
}

/* =========================================================
   ✅ Dashboard (DB) con universo ABC como Excel + orden
   ✅ FIX: normaliza grupos (canon + heurística) en lectura también
========================================================= */
async function dashboardFromDb({ from, to, area, grupo, q }) {
  const salesAgg = await dbQuery(
    `
    WITH s AS (
      SELECT
        item_code,
        MAX(NULLIF(item_desc,'')) AS item_desc,
        COALESCE(SUM(revenue),0)::numeric(18,2) AS revenue,
        COALESCE(SUM(gross_profit),0)::numeric(18,2) AS gp,
        MAX(NULLIF(area,'')) AS area_s,
        MAX(NULLIF(item_group,'')) AS grupo_s
      FROM sales_item_lines
      WHERE doc_date >= $1::date AND doc_date <= $2::date
      GROUP BY item_code
    )
    SELECT
      s.item_code AS item_code,
      COALESCE(NULLIF(s.item_desc,''), NULLIF(g.item_desc,''), '') AS item_desc,
      COALESCE(NULLIF(s.grupo_s,''), NULLIF(g.grupo,''), NULLIF(g.group_name,''), 'Sin grupo') AS grupo,
      COALESCE(NULLIF(s.area_s,''), NULLIF(g.area,''), '') AS area,
      s.revenue AS revenue,
      s.gp AS gp,
      COALESCE(i.stock,0)::float AS stock,
      COALESCE(i.stock_min,0)::float AS stock_min,
      COALESCE(i.stock_max,0)::float AS stock_max,
      COALESCE(i.committed,0)::float AS committed,
      COALESCE(i.ordered,0)::float AS ordered,
      COALESCE(i.available,0)::float AS available
    FROM s
    LEFT JOIN item_group_cache g ON g.item_code = s.item_code
    LEFT JOIN inv_item_cache i   ON i.item_code = s.item_code
    ORDER BY s.revenue DESC
    `,
    [from, to]
  );

  let items = (salesAgg.rows || []).map((r) => {
    const rev = Number(r.revenue || 0);
    const gp = Number(r.gp || 0);
    const pct = rev > 0 ? (gp / rev) * 100 : 0;

    const grupoTxtRaw = String(r.grupo || "Sin grupo");
    const grupoTxt = normalizeGrupoFinal(grupoTxtRaw);

    const areaDb = String(r.area || "");
    const areaFinal = areaDb || inferAreaFromGroup(grupoTxt) || inferAreaFromGroup(grupoTxtRaw) || "CONS";

    return {
      itemCode: String(r.item_code || ""),
      itemDesc: String(r.item_desc || ""),
      area: areaFinal,
      grupo: grupoTxt,
      revenue: rev,
      gp: gp,
      gpPct: Number(pct.toFixed(2)),
      stock: Number(r.stock || 0),
      stockMin: Number(r.stock_min || 0),
      stockMax: Number(r.stock_max || 0),
      committed: Number(r.committed || 0),
      ordered: Number(r.ordered || 0),
      available: Number(r.available || 0),
    };
  });

  const areaSel = String(area || "__ALL__");
  const grupoSel = String(grupo || "__ALL__");
  const qq = String(q || "").trim().toLowerCase();

  // availableGroups (según filtro área)
  let availableGroups = [];
  if (areaSel === "CONS") availableGroups = Array.from(GROUPS_CONS);
  else if (areaSel === "RCI") availableGroups = Array.from(GROUPS_RCI);
  else availableGroups = Array.from(new Set([...GROUPS_CONS, ...GROUPS_RCI]));
  availableGroups.sort((a, b) => a.localeCompare(b));

  /* =========================
     ✅ NUEVO: Rank Total por Revenue (NO depende de filtros)
  ========================= */
  const allByRev = items.slice().sort((a, b) => (b.revenue || 0) - (a.revenue || 0));
  const rankTotalMap = new Map();
  allByRev.forEach((it, idx) => rankTotalMap.set(it.itemCode, idx + 1));

  /* =========================
     ✅ UNIVERSO ABC (NO depende de q)
  ========================= */
  let universe = items.slice();

  if (areaSel !== "__ALL__") {
    universe = universe.filter((x) => String(x.area || "") === areaSel);
  }
  if (grupoSel !== "__ALL__") {
    const gSelN = normGroupName(grupoSel);
    universe = universe.filter((x) => normGroupName(x.grupo) === gSelN);
  }

  const abcRev = abcByMetric(universe, "revenue");
  const abcGP = abcByMetric(universe, "gp");
  const abcPct = abcByMetric(universe, "gpPct");

  let outItems = items.map((it) => {
    const a1 = abcRev.get(it.itemCode) || "D";
    const a2 = abcGP.get(it.itemCode) || "D";
    const a3 = abcPct.get(it.itemCode) || "D";
    const total = totalFromLetters(a1, a2, a3);

    return {
      ...it,
      rankTotal: rankTotalMap.get(it.itemCode) || 999999, // ✅ NUEVO
      abcRevenue: a1,
      abcGP: a2,
      abcGPPct: a3,
      totalLabel: total.label,
      totalTagClass: total.cls,
      R: total.r,
      G: total.g,
      "%": total.p,
      T: total.t,
    };
  });

  // filtros de vista (incluye q)
  if (areaSel !== "__ALL__") outItems = outItems.filter((x) => String(x.area || "") === areaSel);
  if (grupoSel !== "__ALL__") {
    const gSelN = normGroupName(grupoSel);
    outItems = outItems.filter((x) => normGroupName(x.grupo) === gSelN);
  }
  if (qq) {
    outItems = outItems.filter(
      (x) => x.itemCode.toLowerCase().includes(qq) || x.itemDesc.toLowerCase().includes(qq)
    );
  }

  // groupAgg / rank (sobre lo mostrado)
  const groupAggMap = new Map();
  for (const it of outItems) {
    const g = it.grupo || "Sin grupo";
    const cur = groupAggMap.get(g) || { grupo: g, revenue: 0, gp: 0 };
    cur.revenue += it.revenue;
    cur.gp += it.gp;
    groupAggMap.set(g, cur);
  }
  const groupAgg = Array.from(groupAggMap.values())
    .map((g) => ({ ...g, gpPct: g.revenue > 0 ? Number(((g.gp / g.revenue) * 100).toFixed(2)) : 0 }))
    .sort((a, b) => b.revenue - a.revenue);

  const groupRank = new Map();
  groupAgg.forEach((g, idx) => groupRank.set(g.grupo, idx + 1));
  outItems = outItems.map((x) => ({ ...x, rankArea: groupRank.get(x.grupo) || 9999 }));

  // ✅ orden solicitado: Revenue -> GM$ -> GM% -> TOTAL
  outItems.sort((a, b) => {
    const dr = (b.revenue || 0) - (a.revenue || 0);
    if (dr) return dr;

    const dg = (b.gp || 0) - (a.gp || 0);
    if (dg) return dg;

    const dp = (b.gpPct || 0) - (a.gpPct || 0);
    if (dp) return dp;

    const dt = (b.T || 0) - (a.T || 0);
    if (dt) return dt;

    const rankLabel = (lab) => (lab === "AB Crítico" ? 3 : lab === "C Importante" ? 2 : 1);
    return rankLabel(b.totalLabel) - rankLabel(a.totalLabel);
  });

  const totals = outItems.reduce(
    (acc, x) => {
      acc.revenue += Number(x.revenue || 0);
      acc.gp += Number(x.gp || 0);
      return acc;
    },
    { revenue: 0, gp: 0 }
  );
  const gpPctTotal = totals.revenue > 0 ? Number(((totals.gp / totals.revenue) * 100).toFixed(2)) : 0;

  return {
    ok: true,
    from,
    to,
    area: areaSel,
    grupo: grupoSel,
    q: qq,
    lastSyncAt: await getState("last_sync_at"),
    totals: { revenue: totals.revenue, gp: totals.gp, gpPct: gpPctTotal },
    availableGroups,
    groupAgg,
    items: outItems,
  };
}

/* =========================================================
   ✅ Item docs endpoint (modal)
========================================================= */
app.get("/api/admin/estratificacion/item-docs", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const itemCode = String(req.query?.itemCode || "").trim();
    if (!itemCode) return safeJson(res, 400, { ok: false, message: "Falta itemCode" });

    const fromQ = String(req.query?.from || "");
    const toQ = String(req.query?.to || "");
    const areaSel = String(req.query?.area || "__ALL__");
    const grupoSel = String(req.query?.grupo || "__ALL__");

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = isISO(fromQ) ? fromQ : "2024-01-01";
    const to = isISO(toQ) ? toQ : today;

    const q1 = await dbQuery(
      `
      SELECT
        s.doc_type,
        s.doc_date,
        s.doc_entry,
        s.doc_num,
        s.card_code,
        s.card_name,
        s.item_code,
        s.item_desc,
        s.quantity,
        s.revenue,
        s.gross_profit,
        COALESCE(NULLIF(s.area,''), NULLIF(g.area,''), '') AS area,
        COALESCE(NULLIF(s.item_group,''), NULLIF(g.grupo,''), NULLIF(g.group_name,''), 'Sin grupo') AS grupo
      FROM sales_item_lines s
      LEFT JOIN item_group_cache g ON g.item_code = s.item_code
      WHERE s.item_code = $1
        AND s.doc_date >= $2::date
        AND s.doc_date <= $3::date
      ORDER BY s.doc_date DESC, s.doc_entry DESC, s.line_num ASC
      LIMIT 500
      `,
      [itemCode, from, to]
    );

    let rows = (q1.rows || []).map((r) => {
      const grupoTxtRaw = String(r.grupo || "Sin grupo");
      const grupoTxt = normalizeGrupoFinal(grupoTxtRaw);

      const areaDb = String(r.area || "");
      const areaFinal = areaDb || inferAreaFromGroup(grupoTxt) || inferAreaFromGroup(grupoTxtRaw) || "CONS";

      return {
        docType: String(r.doc_type || ""),
        docDate: String(r.doc_date || "").slice(0, 10),
        docEntry: Number(r.doc_entry || 0),
        docNum: r.doc_num != null ? Number(r.doc_num) : null,
        cardCode: String(r.card_code || ""),
        cardName: String(r.card_name || ""),
        itemCode: String(r.item_code || ""),
        itemDesc: String(r.item_desc || ""),
        quantity: Number(r.quantity || 0),
        total: Number(r.revenue || 0),
        gp: Number(r.gross_profit || 0),
        area: areaFinal,
        grupo: grupoTxt,
      };
    });

    if (areaSel !== "__ALL__") rows = rows.filter((x) => String(x.area || "") === areaSel);
    if (grupoSel !== "__ALL__") {
      const gSelN = normGroupName(grupoSel);
      rows = rows.filter((x) => normGroupName(x.grupo) === gSelN);
    }

    return safeJson(res, 200, { ok: true, itemCode, from, to, rows });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================================================
   ✅ Health + Auth
========================================================= */
app.get("/api/health", async (req, res) => {
  return safeJson(res, 200, {
    ok: true,
    message: "✅ PRODIMA ESTRATIFICACION API activa",
    sap: missingSapEnv() ? "missing" : "ok",
    db: hasDb() ? "on" : "off",
    last_sync_at: await getState("last_sync_at"),
  });
});

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
   ✅ Dashboard endpoint
========================================================= */
app.get("/api/admin/estratificacion/dashboard", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const fromQ = String(req.query?.from || "");
    const toQ = String(req.query?.to || "");
    const area = String(req.query?.area || "__ALL__");
    const grupo = String(req.query?.grupo || "__ALL__");
    const q = String(req.query?.q || "");

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = isISO(fromQ) ? fromQ : "2024-01-01";
    const to = isISO(toQ) ? toQ : today;

    const data = await dashboardFromDb({ from, to, area, grupo, q });
    return safeJson(res, 200, data);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================================================
   ✅ Sync endpoint (SAP -> DB)
========================================================= */
app.get("/api/admin/estratificacion/sync", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const mode = String(req.query?.mode || "days").toLowerCase();

    const maxDocsRaw = Number(req.query?.maxDocs || 2500);
    const maxDocs = Math.max(50, Math.min(20000, Number.isFinite(maxDocsRaw) ? Math.trunc(maxDocsRaw) : 2500));

    const today = getDateISOInOffset(TZ_OFFSET_MIN);

    let from = "";
    let to = today;

    if (mode === "range") {
      const fromQ = String(req.query?.from || "");
      const toQ = String(req.query?.to || "");
      from = isISO(fromQ) ? fromQ : "2024-01-01";
      to = isISO(toQ) ? toQ : today;
    } else {
      const nRaw = Number(req.query?.n || 5);
      const n =
        mode === "days"
          ? Math.max(1, Math.min(120, Number.isFinite(nRaw) ? Math.trunc(nRaw) : 5))
          : Math.max(1, Math.min(30, Number.isFinite(nRaw) ? Math.trunc(nRaw) : 5));
      from = addDaysISO(today, -n);
      to = today;
    }

    // ventas netas
    const salesSaved = await syncSales({ from, to, maxDocs });

    // grupos + inventario para items del rango
    const groupsSaved = await syncItemGroupsForSalesItems({ from, to, maxItems: 2500 });
    const invSaved = await syncInventoryForSalesItems({ from, to, maxItems: 2500 });

    await setState("last_sync_at", new Date().toISOString());
    await setState("last_sync_from", from);
    await setState("last_sync_to", to);

    return safeJson(res, 200, {
      ok: true,
      mode,
      maxDocs,
      from,
      to,
      salesSaved,
      groupsSaved,
      invSaved,
      lastSyncAt: await getState("last_sync_at"),
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================================================
   ✅ DEBUG: counts
========================================================= */
app.get("/api/admin/estratificacion/debug-counts", verifyAdmin, async (req, res) => {
  try {
    const r1 = await dbQuery(`SELECT COUNT(*)::int AS c FROM sales_item_lines`);
    const r2 = await dbQuery(`SELECT COUNT(*)::int AS c FROM inv_item_cache`);
    const r3 = await dbQuery(`SELECT COUNT(*)::int AS c FROM item_group_cache`);
    return safeJson(res, 200, {
      ok: true,
      sales_item_lines: r1.rows?.[0]?.c || 0,
      inv_item_cache: r2.rows?.[0]?.c || 0,
      item_group_cache: r3.rows?.[0]?.c || 0,
      last_sync_at: await getState("last_sync_at"),
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});


/* =========================================================
   Portal central (usa portal_users, no app_users)
========================================================= */
async function ensurePortalUsersDb() {
  if (!hasDb()) return;

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS portal_users (
      id SERIAL PRIMARY KEY,
      username TEXT UNIQUE NOT NULL,
      full_name TEXT DEFAULT '',
      pin_hash TEXT NOT NULL,
      role TEXT NOT NULL DEFAULT 'user',
      is_active BOOLEAN DEFAULT TRUE,
      permissions_json JSONB NOT NULL DEFAULT '[]'::jsonb,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await dbQuery(`
    CREATE INDEX IF NOT EXISTS idx_portal_users_username
    ON portal_users(username);
  `);
}

async function hashPortalPin(pin) {
  return bcrypt.hash(String(pin), 10);
}

async function comparePortalPin(pin, pinHash) {
  return bcrypt.compare(String(pin), String(pinHash || ""));
}

function verifyPortalAuth(req, res, next) {
  const token = readBearer(req);
  if (!token) return safeJson(res, 401, { ok: false, message: "Missing Bearer token" });

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    req.user = decoded;
    return next();
  } catch {
    return safeJson(res, 401, { ok: false, message: "Invalid token" });
  }
}

function verifyPortalAdmin(req, res, next) {
  const token = readBearer(req);
  if (!token) return safeJson(res, 401, { ok: false, message: "Missing Bearer token" });

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    if (decoded?.role !== "admin") {
      return safeJson(res, 403, { ok: false, message: "Forbidden" });
    }
    req.user = decoded;
    return next();
  } catch {
    return safeJson(res, 401, { ok: false, message: "Invalid token" });
  }
}

app.post("/api/portal/auth/login", async (req, res) => {
  try {
    const username = String(req.body?.username || "").trim();
    const pass = String(req.body?.pass || "").trim();

    if (!username || !pass) {
      return safeJson(res, 400, { ok: false, message: "Usuario y contraseña requeridos" });
    }

    if (username === ADMIN_USER && pass === ADMIN_PASS) {
      const user = {
        id: 0,
        username: ADMIN_USER,
        full_name: "Administrador PRODIMA",
        role: "admin",
        permissions: ["*"],
      };
      const token = signToken(user, "12h");
      return safeJson(res, 200, { ok: true, token, user });
    }

    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const r = await dbQuery(
      `SELECT id, username, full_name, pin_hash, role, is_active, permissions_json
       FROM portal_users
       WHERE LOWER(username)=LOWER($1)
       LIMIT 1`,
      [username]
    );

    const u = r.rows?.[0];
    if (!u) return safeJson(res, 401, { ok: false, message: "Credenciales inválidas" });
    if (!u.is_active) return safeJson(res, 403, { ok: false, message: "Usuario inactivo" });

    const okPass = await comparePortalPin(pass, u.pin_hash);
    if (!okPass) return safeJson(res, 401, { ok: false, message: "Credenciales inválidas" });

    const user = {
      id: u.id,
      username: u.username,
      full_name: u.full_name || "",
      role: u.role || "user",
      permissions: Array.isArray(u.permissions_json) ? u.permissions_json : [],
    };

    const token = signToken(user, "12h");
    return safeJson(res, 200, { ok: true, token, user });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get("/api/portal/auth/me", verifyPortalAuth, async (req, res) => {
  return safeJson(res, 200, { ok: true, user: req.user });
});

app.get("/api/portal/admin/users", verifyPortalAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const r = await dbQuery(
      `SELECT id, username, full_name, role, is_active, permissions_json, created_at
       FROM portal_users
       ORDER BY id DESC`
    );

    const users = (r.rows || []).map((x) => ({
      id: x.id,
      username: x.username,
      full_name: x.full_name,
      role: x.role,
      is_active: x.is_active,
      permissions: Array.isArray(x.permissions_json) ? x.permissions_json : [],
      created_at: x.created_at,
    }));

    return safeJson(res, 200, { ok: true, users });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.post("/api/portal/admin/users", verifyPortalAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const username = String(req.body?.username || "").trim().toLowerCase();
    const full_name = String(req.body?.full_name || "").trim();
    const pin = String(req.body?.pin || "").trim();
    const role = String(req.body?.role || "user").trim().toLowerCase();
    const permissions = Array.isArray(req.body?.permissions)
      ? req.body.permissions.map((x) => String(x).trim()).filter(Boolean)
      : [];

    if (!username) return safeJson(res, 400, { ok: false, message: "Username requerido" });
    if (!full_name) return safeJson(res, 400, { ok: false, message: "Nombre requerido" });
    if (!pin || pin.length < 4) return safeJson(res, 400, { ok: false, message: "PIN mínimo 4" });
    if (!["user", "admin"].includes(role)) return safeJson(res, 400, { ok: false, message: "Rol inválido" });

    const pin_hash = await hashPortalPin(pin);
    const finalPermissions = role === "admin" ? ["*"] : permissions;

    const r = await dbQuery(
      `INSERT INTO portal_users(username, full_name, pin_hash, role, is_active, permissions_json)
       VALUES ($1,$2,$3,$4,TRUE,$5::jsonb)
       RETURNING id, username, full_name, role, is_active, permissions_json, created_at`,
      [username, full_name, pin_hash, role, JSON.stringify(finalPermissions)]
    );

    const user = r.rows?.[0];
    return safeJson(res, 200, {
      ok: true,
      user: {
        id: user.id,
        username: user.username,
        full_name: user.full_name,
        role: user.role,
        is_active: user.is_active,
        permissions: Array.isArray(user.permissions_json) ? user.permissions_json : [],
        created_at: user.created_at,
      },
    });
  } catch (e) {
    if (String(e?.code) === "23505") {
      return safeJson(res, 409, { ok: false, message: "Ese username ya existe" });
    }
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.patch("/api/portal/admin/users/:id", verifyPortalAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const id = Number(req.params.id || 0);
    const full_name = String(req.body?.full_name || "").trim();
    const role = String(req.body?.role || "user").trim().toLowerCase();
    const permissions = Array.isArray(req.body?.permissions)
      ? req.body.permissions.map((x) => String(x).trim()).filter(Boolean)
      : [];

    if (!Number.isFinite(id) || id <= 0) return safeJson(res, 400, { ok: false, message: "ID inválido" });
    if (!full_name) return safeJson(res, 400, { ok: false, message: "Nombre requerido" });
    if (!["user", "admin"].includes(role)) return safeJson(res, 400, { ok: false, message: "Rol inválido" });

    const finalPermissions = role === "admin" ? ["*"] : permissions;

    const r = await dbQuery(
      `UPDATE portal_users
       SET full_name=$2,
           role=$3,
           permissions_json=$4::jsonb
       WHERE id=$1
       RETURNING id, username, full_name, role, is_active, permissions_json, created_at`,
      [id, full_name, role, JSON.stringify(finalPermissions)]
    );

    if (!r.rowCount) return safeJson(res, 404, { ok: false, message: "Usuario no encontrado" });

    return safeJson(res, 200, {
      ok: true,
      user: {
        ...r.rows[0],
        permissions: Array.isArray(r.rows[0].permissions_json) ? r.rows[0].permissions_json : [],
      },
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.patch("/api/portal/admin/users/:id/toggle", verifyPortalAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const id = Number(req.params.id || 0);
    if (!Number.isFinite(id) || id <= 0) return safeJson(res, 400, { ok: false, message: "ID inválido" });

    const r = await dbQuery(
      `UPDATE portal_users
       SET is_active = NOT is_active
       WHERE id = $1
       RETURNING id, username, full_name, role, is_active, permissions_json, created_at`,
      [id]
    );

    if (!r.rowCount) return safeJson(res, 404, { ok: false, message: "Usuario no encontrado" });

    const user = r.rows[0];
    return safeJson(res, 200, {
      ok: true,
      user: {
        id: user.id,
        username: user.username,
        full_name: user.full_name,
        role: user.role,
        is_active: user.is_active,
        permissions: Array.isArray(user.permissions_json) ? user.permissions_json : [],
        created_at: user.created_at,
      },
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.delete("/api/portal/admin/users/:id", verifyPortalAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const id = Number(req.params.id || 0);
    if (!Number.isFinite(id) || id <= 0) return safeJson(res, 400, { ok: false, message: "ID inválido" });

    const r = await dbQuery(`DELETE FROM portal_users WHERE id=$1`, [id]);
    if (!r.rowCount) return safeJson(res, 404, { ok: false, message: "Usuario no encontrado" });

    return safeJson(res, 200, { ok: true });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.patch("/api/portal/admin/users/:id/pin", verifyPortalAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const id = Number(req.params.id || 0);
    const pin = String(req.body?.pin || "").trim();

    if (!Number.isFinite(id) || id <= 0) return safeJson(res, 400, { ok: false, message: "ID inválido" });
    if (!pin || pin.length < 4) return safeJson(res, 400, { ok: false, message: "PIN mínimo 4" });

    const pin_hash = await hashPortalPin(pin);
    const r = await dbQuery(`UPDATE portal_users SET pin_hash=$2 WHERE id=$1`, [id, pin_hash]);

    if (!r.rowCount) return safeJson(res, 404, { ok: false, message: "Usuario no encontrado" });
    return safeJson(res, 200, { ok: true });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

__extraBootTasks.push(async () => {
  try {
    await ensurePortalUsersDb();
  } catch (e) {
    console.error("Portal DB init error:", e.message || String(e));
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

__extraBootTasks.push(async () => {
  try {
    await ensureDb();
  } catch (e) {
    console.error("Estratificación DB init error:", e.message || String(e));
  }
});
}


/* =========================================================
   Módulo integrado: Clientes / Facturación
========================================================= */
{
const { Pool } = pg;

app.use(express.json({ limit: "6mb" }));

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

  // CORS_ORIGIN=https://prodima.com.pa,https://www.prodima.com.pa
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
function isISO(s) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(s || ""));
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

/* =========================================================
   ✅ Postgres (Supabase)
========================================================= */
const pool = new Pool({
  connectionString: DATABASE_URL || undefined,
  ssl: DATABASE_URL ? { rejectUnauthorized: false } : undefined,
});

function hasDb() {
  return Boolean(DATABASE_URL);
}
async function dbQuery(text, params = []) {
  return pool.query(text, params);
}

async function ensureDb() {
  if (!hasDb()) return;

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS fact_invoice_lines (
      doc_entry      INTEGER NOT NULL,
      line_num       INTEGER NOT NULL,
      doc_num        INTEGER NOT NULL,
      doc_date       DATE    NOT NULL,
      card_code      TEXT    NOT NULL,
      card_name      TEXT    NOT NULL,
      warehouse_code TEXT    NOT NULL,
      item_code      TEXT    NOT NULL DEFAULT '',
      item_desc      TEXT    NOT NULL DEFAULT '',
      quantity       NUMERIC(18,4) NOT NULL DEFAULT 0,
      line_total     NUMERIC(18,2) NOT NULL DEFAULT 0,
      gross_profit   NUMERIC(18,2) NOT NULL DEFAULT 0,
      updated_at     TIMESTAMP DEFAULT NOW(),
      PRIMARY KEY (doc_entry, line_num)
    );
  `);

  // migrations safe
  await dbQuery(`ALTER TABLE fact_invoice_lines ADD COLUMN IF NOT EXISTS item_code TEXT NOT NULL DEFAULT '';`);
  await dbQuery(`ALTER TABLE fact_invoice_lines ADD COLUMN IF NOT EXISTS item_desc TEXT NOT NULL DEFAULT '';`);
  await dbQuery(`ALTER TABLE fact_invoice_lines ADD COLUMN IF NOT EXISTS quantity NUMERIC(18,4) NOT NULL DEFAULT 0;`);
  await dbQuery(`ALTER TABLE fact_invoice_lines ADD COLUMN IF NOT EXISTS gross_profit NUMERIC(18,2) NOT NULL DEFAULT 0;`);

  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_fact_doc_date ON fact_invoice_lines(doc_date);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_fact_wh ON fact_invoice_lines(warehouse_code);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_fact_card ON fact_invoice_lines(card_code);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_fact_item ON fact_invoice_lines(item_code);`);

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS sync_state (
      k TEXT PRIMARY KEY,
      v TEXT NOT NULL,
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);
}

async function setState(k, v) {
  if (!hasDb()) return;
  await dbQuery(
    `INSERT INTO sync_state(k,v,updated_at) VALUES($1,$2,NOW())
     ON CONFLICT(k) DO UPDATE SET v=EXCLUDED.v, updated_at=NOW()`,
    [k, String(v)]
  );
}
async function getState(k) {
  if (!hasDb()) return "";
  const r = await dbQuery(`SELECT v FROM sync_state WHERE k=$1 LIMIT 1`, [k]);
  return r.rows?.[0]?.v || "";
}

/* =========================================================
   ✅ SAP Service Layer (cookie + timeout)
========================================================= */
let _fetch = globalThis.fetch || null;
async function httpFetch(url, options) {
  if (_fetch) return _fetch(url, options);
  const mod = await import("node-fetch");
  _fetch = mod.default;
  return _fetch(url, options);
}

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
  const timeoutMs = Number(options.timeoutMs || 60000);
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
   ✅ SAP: scan invoice headers
========================================================= */
async function scanInvoicesHeaders({ f, t, maxDocs = 3000 }) {
  const toPlus1 = addDaysISO(t, 1);
  const batchTop = 200;
  let skipSap = 0;
  const out = [];

  for (let page = 0; page < 300; page++) {
    const raw = await slFetch(
      `/Invoices?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName` +
        `&$filter=${encodeURIComponent(`DocDate ge '${f}' and DocDate lt '${toPlus1}'`)}` +
        `&$orderby=DocDate asc,DocEntry asc&$top=${batchTop}&$skip=${skipSap}`,
      { timeoutMs: 60000 }
    );

    const rows = Array.isArray(raw?.value) ? raw.value : [];
    if (!rows.length) break;
    skipSap += rows.length;

    for (const r of rows) {
      out.push({
        DocEntry: Number(r.DocEntry),
        DocNum: Number(r.DocNum),
        DocDate: String(r.DocDate || "").slice(0, 10),
        DocTotal: Number(r.DocTotal || 0),
        CardCode: String(r.CardCode || ""),
        CardName: String(r.CardName || ""),
      });
      if (out.length >= maxDocs) return out;
    }
  }
  return out;
}

async function getInvoiceDoc(docEntry) {
  const de = Number(docEntry);
  if (!Number.isFinite(de) || de <= 0) return null;
  return slFetch(`/Invoices(${de})`, { timeoutMs: 90000 });
}

/* =========================================================
   ✅ Sync: upsert invoice lines
========================================================= */
function pickGrossProfit(ln) {
  const candidates = [ln?.GrossProfit, ln?.GrossProfitTotal, ln?.GrossProfitFC, ln?.GrossProfitSC];
  for (const c of candidates) {
    const n = Number(c);
    if (Number.isFinite(n)) return n;
  }
  return 0;
}

async function upsertLinesToDb(invHeader, docFull) {
  const lines = Array.isArray(docFull?.DocumentLines) ? docFull.DocumentLines : [];
  if (!lines.length) return 0;

  const docEntry = Number(invHeader.DocEntry);
  const docNum = Number(invHeader.DocNum);
  const docDate = String(invHeader.DocDate || "").slice(0, 10);
  const cardCode = String(invHeader.CardCode || "");
  const cardName = String(invHeader.CardName || "");

  const values = [];
  const params = [];
  let p = 1;

  for (const ln of lines) {
    const lineNum = Number(ln.LineNum);
    if (!Number.isFinite(lineNum)) continue;

    const wh = String(ln.WarehouseCode || "SIN_WH").trim() || "SIN_WH";
    const lt = Number(ln.LineTotal || 0);
    const qty = Number(ln.Quantity || 0);
    const itemCode = String(ln.ItemCode || "").trim();
    const itemDesc = String(ln.ItemDescription || ln.ItemName || "").trim();
    const gp = pickGrossProfit(ln);

    params.push(docEntry, lineNum, docNum, docDate, cardCode, cardName, wh, itemCode, itemDesc, qty, lt, gp);
    values.push(
      `($${p++},$${p++},$${p++},$${p++}::date,$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`
    );
  }

  if (!values.length) return 0;

  await dbQuery(
    `
    INSERT INTO fact_invoice_lines
      (doc_entry,line_num,doc_num,doc_date,card_code,card_name,warehouse_code,item_code,item_desc,quantity,line_total,gross_profit)
    VALUES ${values.join(",")}
    ON CONFLICT (doc_entry,line_num)
    DO UPDATE SET
      doc_num=EXCLUDED.doc_num,
      doc_date=EXCLUDED.doc_date,
      card_code=EXCLUDED.card_code,
      card_name=EXCLUDED.card_name,
      warehouse_code=EXCLUDED.warehouse_code,
      item_code=EXCLUDED.item_code,
      item_desc=EXCLUDED.item_desc,
      quantity=EXCLUDED.quantity,
      line_total=EXCLUDED.line_total,
      gross_profit=EXCLUDED.gross_profit,
      updated_at=NOW()
    `,
    params
  );

  return values.length;
}

async function syncRangeToDb({ from, to, maxDocs = 6000 }) {
  if (!hasDb()) throw new Error("DB no configurada (DATABASE_URL)");
  const headers = await scanInvoicesHeaders({ f: from, t: to, maxDocs });

  if (!headers.length) {
    await setState("last_sync_at", new Date().toISOString());
    return { headers: 0, lines: 0 };
  }

  const CONC = 1; // estable
  let idx = 0;
  let totalLines = 0;

  async function worker() {
    while (idx < headers.length) {
      const i = idx++;
      const h = headers[i];
      try {
        const full = await getInvoiceDoc(h.DocEntry);
        const inserted = await upsertLinesToDb(h, full);
        totalLines += inserted;
      } catch {}
      await sleep(20);
    }
  }

  await Promise.all(Array.from({ length: CONC }, () => worker()));

  await setState("last_sync_from", from);
  await setState("last_sync_to", to);
  await setState("last_sync_at", new Date().toISOString());

  return { headers: headers.length, lines: totalLines };
}

/* =========================================================
   ✅ Dashboard from DB
========================================================= */
async function dashboardFromDb(from, to) {
  const totalsQ = await dbQuery(
    `
    SELECT
      COUNT(DISTINCT doc_entry) AS invoices,
      COALESCE(SUM(line_total),0)::numeric(18,2) AS dollars,
      COALESCE(SUM(gross_profit),0)::numeric(18,2) AS gross_profit
    FROM fact_invoice_lines
    WHERE doc_date >= $1::date AND doc_date <= $2::date
    `,
    [from, to]
  );

  const tableQ = await dbQuery(
    `
    SELECT
      card_code AS card_code,
      card_name AS card_name,
      warehouse_code AS warehouse,
      SUM(line_total)::numeric(18,2) AS dollars,
      SUM(gross_profit)::numeric(18,2) AS gross_profit,
      COUNT(DISTINCT doc_entry) AS invoices
    FROM fact_invoice_lines
    WHERE doc_date >= $1::date AND doc_date <= $2::date
    GROUP BY 1,2,3
    ORDER BY dollars DESC
    `,
    [from, to]
  );

  const byMonthQ = await dbQuery(
    `
    SELECT
      to_char(date_trunc('month', doc_date), 'YYYY-MM') AS month,
      COUNT(DISTINCT doc_entry) AS invoices,
      COALESCE(SUM(line_total),0)::numeric(18,2) AS dollars,
      COALESCE(SUM(gross_profit),0)::numeric(18,2) AS gross_profit
    FROM fact_invoice_lines
    WHERE doc_date >= $1::date AND doc_date <= $2::date
    GROUP BY 1
    ORDER BY 1
    `,
    [from, to]
  );

  const topWhQ = await dbQuery(
    `
    SELECT warehouse_code AS warehouse,
           COALESCE(SUM(line_total),0)::numeric(18,2) AS dollars
    FROM fact_invoice_lines
    WHERE doc_date >= $1::date AND doc_date <= $2::date
    GROUP BY 1
    ORDER BY dollars DESC
    LIMIT 50
    `,
    [from, to]
  );

  const topCustQ = await dbQuery(
    `
    SELECT card_code, card_name,
           COALESCE(SUM(line_total),0)::numeric(18,2) AS dollars
    FROM fact_invoice_lines
    WHERE doc_date >= $1::date AND doc_date <= $2::date
    GROUP BY 1,2
    ORDER BY dollars DESC
    LIMIT 50
    `,
    [from, to]
  );

  const toNum = (x) => Number(x || 0);
  const totalDol = toNum(totalsQ.rows?.[0]?.dollars);
  const totalGP = toNum(totalsQ.rows?.[0]?.gross_profit);

  return {
    ok: true,
    from,
    to,
    totals: {
      invoices: toNum(totalsQ.rows?.[0]?.invoices),
      dollars: totalDol,
      grossProfit: totalGP,
      grossPct: totalDol > 0 ? Number(((totalGP / totalDol) * 100).toFixed(2)) : 0,
    },
    byMonth: (byMonthQ.rows || []).map((r) => {
      const dol = toNum(r.dollars);
      const gp = toNum(r.gross_profit);
      return {
        month: r.month,
        invoices: toNum(r.invoices),
        dollars: dol,
        grossProfit: gp,
        grossPct: dol > 0 ? Number(((gp / dol) * 100).toFixed(2)) : 0,
      };
    }),
    topWarehouses: (topWhQ.rows || []).map((r) => ({
      warehouse: r.warehouse,
      dollars: toNum(r.dollars),
    })),
    topCustomers: (topCustQ.rows || []).map((r) => ({
      cardCode: r.card_code,
      cardName: r.card_name,
      customer: `${r.card_code} · ${r.card_name}`,
      dollars: toNum(r.dollars),
    })),
    table: (tableQ.rows || []).map((r) => {
      const dol = toNum(r.dollars);
      const gp = toNum(r.gross_profit);
      return {
        cardCode: r.card_code,
        cardName: r.card_name,
        customer: `${r.card_code} · ${r.card_name}`,
        warehouse: r.warehouse,
        dollars: dol,
        grossProfit: gp,
        grossPct: dol > 0 ? Number(((gp / dol) * 100).toFixed(2)) : 0,
        invoices: toNum(r.invoices),
      };
    }),
  };
}

/* =========================================================
   ✅ Details: invoices + lines
========================================================= */
async function detailsFromDb({ from, to, cardCode, warehouse }) {
  const q = await dbQuery(
    `
    SELECT
      doc_entry, doc_num, doc_date,
      item_code, item_desc, quantity, line_total, gross_profit
    FROM fact_invoice_lines
    WHERE doc_date >= $1::date AND doc_date <= $2::date
      AND card_code = $3
      AND warehouse_code = $4
    ORDER BY doc_date DESC, doc_num DESC, line_num ASC
    `,
    [from, to, cardCode, warehouse]
  );

  const map = new Map(); // doc_num -> invoice
  for (const r of q.rows || []) {
    const dn = Number(r.doc_num);
    if (!map.has(dn)) {
      map.set(dn, {
        docNum: dn,
        docDate: String(r.doc_date).slice(0, 10),
        docEntry: Number(r.doc_entry),
        lines: [],
        totals: { qty: 0, dollars: 0, grossProfit: 0 },
      });
    }
    const it = map.get(dn);
    const qty = Number(r.quantity || 0);
    const dol = Number(r.line_total || 0);
    const gp = Number(r.gross_profit || 0);
    it.lines.push({
      itemCode: String(r.item_code || ""),
      itemDesc: String(r.item_desc || ""),
      quantity: qty,
      dollars: dol,
      grossProfit: gp,
      grossPct: dol > 0 ? Number(((gp / dol) * 100).toFixed(2)) : 0,
    });
    it.totals.qty += qty;
    it.totals.dollars += dol;
    it.totals.grossProfit += gp;
  }

  const invoices = Array.from(map.values()).map((inv) => {
    inv.totals.qty = Number(inv.totals.qty.toFixed(4));
    inv.totals.dollars = Number(inv.totals.dollars.toFixed(2));
    inv.totals.grossProfit = Number(inv.totals.grossProfit.toFixed(2));
    inv.totals.grossPct =
      inv.totals.dollars > 0 ? Number(((inv.totals.grossProfit / inv.totals.dollars) * 100).toFixed(2)) : 0;
    return inv;
  });

  const totals = invoices.reduce(
    (a, x) => {
      a.invoices += 1;
      a.qty += Number(x.totals.qty || 0);
      a.dollars += Number(x.totals.dollars || 0);
      a.grossProfit += Number(x.totals.grossProfit || 0);
      return a;
    },
    { invoices: 0, qty: 0, dollars: 0, grossProfit: 0 }
  );

  totals.qty = Number(totals.qty.toFixed(4));
  totals.dollars = Number(totals.dollars.toFixed(2));
  totals.grossProfit = Number(totals.grossProfit.toFixed(2));
  totals.grossPct = totals.dollars > 0 ? Number(((totals.grossProfit / totals.dollars) * 100).toFixed(2)) : 0;

  return { ok: true, from, to, cardCode, warehouse, totals, invoices };
}

/* =========================================================
   ✅ Top products
========================================================= */
async function topProductsFromDb({ from, to, warehouse = "", cardCode = "", limit = 10 }) {
  const params = [from, to];
  let where = `doc_date >= $1::date AND doc_date <= $2::date`;

  if (warehouse) {
    params.push(warehouse);
    where += ` AND warehouse_code = $${params.length}`;
  }
  if (cardCode) {
    params.push(cardCode);
    where += ` AND card_code = $${params.length}`;
  }

  const q = await dbQuery(
    `
    SELECT
      item_code,
      item_desc,
      COALESCE(SUM(quantity),0)::numeric(18,4) AS qty,
      COALESCE(SUM(line_total),0)::numeric(18,2) AS dollars,
      COALESCE(SUM(gross_profit),0)::numeric(18,2) AS gross_profit,
      COUNT(DISTINCT doc_entry) AS invoices
    FROM fact_invoice_lines
    WHERE ${where}
      AND item_code <> ''
    GROUP BY 1,2
    ORDER BY dollars DESC
    LIMIT ${Math.max(1, Math.min(200, limit))}
    `,
    params
  );

  return {
    ok: true,
    from,
    to,
    warehouse: warehouse || null,
    cardCode: cardCode || null,
    top: (q.rows || []).map((r) => {
      const dol = Number(r.dollars || 0);
      const gp = Number(r.gross_profit || 0);
      return {
        itemCode: r.item_code,
        itemDesc: r.item_desc,
        qty: Number(r.qty || 0),
        dollars: dol,
        grossProfit: gp,
        grossPct: dol > 0 ? Number(((gp / dol) * 100).toFixed(2)) : 0,
        invoices: Number(r.invoices || 0),
      };
    }),
  };
}

/* =========================================================
   ✅ Health + Auth
========================================================= */
app.get("/api/health", async (req, res) => {
  return safeJson(res, 200, {
    ok: true,
    message: "✅ PRODIMA INVOICES API activa",
    sap: missingSapEnv() ? "missing" : "ok",
    db: hasDb() ? "on" : "off",
    last_sync_at: await getState("last_sync_at"),
  });
});

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
   ✅ API DB-FIRST
========================================================= */
app.get("/api/admin/invoices/dashboard", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const fromQ = String(req.query?.from || "");
    const toQ = String(req.query?.to || "");

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const defaultFrom = addDaysISO(today, -30);

    const from = isISO(fromQ) ? fromQ : defaultFrom;
    const to = isISO(toQ) ? toQ : today;

    const data = await dashboardFromDb(from, to);
    return safeJson(res, 200, data);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.get("/api/admin/invoices/details", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const fromQ = String(req.query?.from || "");
    const toQ = String(req.query?.to || "");
    const cardCode = String(req.query?.cardCode || "").trim();
    const warehouse = String(req.query?.warehouse || "").trim();

    if (!cardCode || !warehouse) return safeJson(res, 400, { ok: false, message: "cardCode y warehouse requeridos" });

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const defaultFrom = addDaysISO(today, -30);

    const from = isISO(fromQ) ? fromQ : defaultFrom;
    const to = isISO(toQ) ? toQ : today;

    const data = await detailsFromDb({ from, to, cardCode, warehouse });
    return safeJson(res, 200, data);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.get("/api/admin/invoices/top-products", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const fromQ = String(req.query?.from || "");
    const toQ = String(req.query?.to || "");
    const warehouse = String(req.query?.warehouse || "").trim();
    const cardCode = String(req.query?.cardCode || "").trim();
    const limitRaw = Number(req.query?.limit || 10);
    const limit = Math.max(1, Math.min(200, Number.isFinite(limitRaw) ? Math.trunc(limitRaw) : 10));

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const defaultFrom = addDaysISO(today, -30);

    const from = isISO(fromQ) ? fromQ : defaultFrom;
    const to = isISO(toQ) ? toQ : today;

    const data = await topProductsFromDb({ from, to, warehouse, cardCode, limit });
    return safeJson(res, 200, data);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ EXPORT EXCEL (SERVER-SIDE) — SOLO SE AGREGA ESTO
========================================================= */
app.get("/api/admin/invoices/export", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const fromQ = String(req.query?.from || "");
    const toQ = String(req.query?.to || "");

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const defaultFrom = addDaysISO(today, -30);

    const from = isISO(fromQ) ? fromQ : defaultFrom;
    const to = isISO(toQ) ? toQ : today;

    const data = await dashboardFromDb(from, to);

    const wb = XLSX.utils.book_new();

    const main = (data.table || []).map((r) => ({
      CardCode: r.cardCode,
      CardName: r.cardName,
      Cliente: r.customer,
      Bodega: r.warehouse,
      Dolares: Number(r.dollars || 0),
      GananciaBruta: Number(r.grossProfit || 0),
      PorcentajeGP: Number(r.grossPct || 0),
      Facturas: Number(r.invoices || 0),
    }));

    XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(main), "Cliente_x_Bodega");
    XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(data.byMonth || []), "PorMes");

    const buf = XLSX.write(wb, { bookType: "xlsx", type: "buffer" });

    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.setHeader("Content-Disposition", `attachment; filename="facturacion_${Date.now()}.xlsx"`);
    return res.status(200).send(buf);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get("/api/admin/invoices/details/export", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const fromQ = String(req.query?.from || "");
    const toQ = String(req.query?.to || "");
    const cardCode = String(req.query?.cardCode || "").trim();
    const warehouse = String(req.query?.warehouse || "").trim();

    if (!cardCode || !warehouse) return safeJson(res, 400, { ok: false, message: "cardCode y warehouse requeridos" });

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const defaultFrom = addDaysISO(today, -30);

    const from = isISO(fromQ) ? fromQ : defaultFrom;
    const to = isISO(toQ) ? toQ : today;

    const detail = await detailsFromDb({ from, to, cardCode, warehouse });

    const rows = [];
    for (const inv of detail.invoices || []) {
      for (const ln of inv.lines || []) {
        rows.push({
          DocNum: inv.docNum,
          Fecha: inv.docDate,
          ItemCode: ln.itemCode,
          Descripcion: ln.itemDesc,
          Cantidad: Number(ln.quantity || 0),
          Total: Number(ln.dollars || 0),
          GananciaBruta: Number(ln.grossProfit || 0),
          PorcentajeGP: Number(ln.grossPct || 0),
        });
      }
    }

    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(rows), "Detalle");

    const buf = XLSX.write(wb, { bookType: "xlsx", type: "buffer" });

    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="detalle_${cardCode}_${warehouse}_${Date.now()}.xlsx"`
    );
    return res.status(200).send(buf);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================================================
   ✅ SYNC endpoints (SAP -> DB)
========================================================= */
app.post("/api/admin/invoices/sync", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const fromQ = String(req.query?.from || "");
    const toQ = String(req.query?.to || "");
    if (!isISO(fromQ) || !isISO(toQ)) return safeJson(res, 400, { ok: false, message: "Requiere from y to (YYYY-MM-DD)" });

    const maxDocsRaw = Number(req.query?.maxDocs || 6000);
    const maxDocs = Math.max(50, Math.min(20000, Number.isFinite(maxDocsRaw) ? Math.trunc(maxDocsRaw) : 6000));

    const out = await syncRangeToDb({ from: fromQ, to: toQ, maxDocs });
    return safeJson(res, 200, { ok: true, ...out, from: fromQ, to: toQ, maxDocs });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.post("/api/admin/invoices/sync/recent", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const daysRaw = Number(req.query?.days || 10);
    const days = Math.max(1, Math.min(120, Number.isFinite(daysRaw) ? Math.trunc(daysRaw) : 10));

    const maxDocsRaw = Number(req.query?.maxDocs || 2500);
    const maxDocs = Math.max(50, Math.min(20000, Number.isFinite(maxDocsRaw) ? Math.trunc(maxDocsRaw) : 2500));

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = addDaysISO(today, -days);

    const out = await syncRangeToDb({ from, to: today, maxDocs });
    return safeJson(res, 200, { ok: true, ...out, from, to: today, days, maxDocs });
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

__extraBootTasks.push(async () => {
  try {
    await ensureDb();
  } catch (e) {
    console.error("Clientes DB init error:", e.message || String(e));
  }
});
}

/* =========================================================
   Start
========================================================= */
process.on("unhandledRejection", (e) => console.error("unhandledRejection:", e));
process.on("uncaughtException", (e) => console.error("uncaughtException:", e));

(async () => {
  try {
    await ensureDb();
    for (const task of __extraBootTasks) {
      await task();
    }
    console.log(hasDb() ? "DB ready ✅" : "DB not configured ⚠️");
  } catch (e) {
    console.error("DB init error:", e.message || String(e));
  }

  app.listen(Number(PORT), () => {
    console.log(`PRODIMA API UNIFICADA listening on :${PORT}`);
  });
})();
