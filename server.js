import express from "express";
import pg from "pg";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";
import crypto from "crypto";
import XLSX from "xlsx";
import fs from "fs";
import path from "path";

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
      .map((l) => ({
        ItemCode: String(l.itemCode || "").trim(),
        ItemDescription: String(l.itemDesc || l.itemDescription || l.description || l.name || "").trim(),
        Quantity: Number(l.qty || 0),
        Price: Number(l.price || l.unitPrice || 0),
      }))
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
        mailResult = await globalThis.sendDocumentEmailViaGAS({
          event: "quote_created",
          notifyTo: globalThis.DOCS_NOTIFY_TO,
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
        mailResult = await globalThis.sendDocumentEmailViaGAS({
          event: "quote_created",
          notifyTo: globalThis.DOCS_NOTIFY_TO,
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
      mailResult = await globalThis.sendDocumentEmailViaGAS({
        event: "return_created",
        notifyTo: globalThis.DOCS_NOTIFY_TO,
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
    return res.status(500).json({ ok: false, message: String(err?.message || err) });
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


globalThis.DOCS_NOTIFY_TO = parseEmailList(
  "pe-impa@prodima.com.pa"
).join(",");
const DOCS_NOTIFY_TO = globalThis.DOCS_NOTIFY_TO;
console.log("BOOT", "DOCS_MAIL_V11_BASE41_FAST_SEARCH");


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

globalThis.sendDocumentEmailViaGAS = async function sendDocumentEmailViaGAS({ event, notifyTo, data, attachments }) {
  if (!GAS_WEBHOOK_URL || !GAS_WEBHOOK_SECRET) {
    return { ok: false, skipped: true, message: "GAS no configurado" };
  }

  const payload = {
    secret: GAS_WEBHOOK_SECRET,
    event,
    requesterEmail: "",
    notifyTo: notifyTo || globalThis.DOCS_NOTIFY_TO,
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
    return { ok: true, skipped: false, message: text || "ok" };
  } catch (err) {
    return { ok: false, skipped: false, message: String(err?.message || err) };
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
globalThis.GROUPS_CONS = new Set([
  "Prod. De Limpieza",
  "Cuidado De La Ropa",
  "Sazonadores",
  "Art. De Limpieza",
  "Vinagres",
  "Especialidades y GMT",
]);

globalThis.GROUPS_RCI = new Set([
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

globalThis.GROUPS_CONS_N = new Set(Array.from(globalThis.GROUPS_CONS).map(normGroupName));
globalThis.GROUPS_RCI_N = new Set(Array.from(globalThis.GROUPS_RCI).map(normGroupName));

const CANON_GROUP = new Map(
  [...Array.from(globalThis.GROUPS_CONS), ...Array.from(globalThis.GROUPS_RCI)].map((g) => [normGroupName(g), g])
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

globalThis.normalizeGrupoFinal = function normalizeGrupoFinal(grupoRaw) {
  const raw = String(grupoRaw || "").trim();
  if (!raw) return "Sin grupo";

  const canon = canonicalGroupName(raw);
  const canonN = normGroupName(canon);

  // si ya está en tus listas, perfecto
  if (globalThis.GROUPS_CONS_N.has(canonN) || globalThis.GROUPS_RCI_N.has(canonN)) return canon;

  // si no, prueba heurística
  const guessed = guessCanonicalGroupName(raw);
  if (guessed) return guessed;

  return canon || "Sin grupo";
}

globalThis.inferAreaFromGroup = function inferAreaFromGroup(groupName) {
  const g = normGroupName(groupName);
  if (!g) return "";
  if (globalThis.GROUPS_CONS_N.has(g)) return "CONS";
  if (globalThis.GROUPS_RCI_N.has(g)) return "RCI";
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

globalThis.syncSales = async function syncSales({ from, to, maxDocs = 2500 }) {
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
      const grupo = globalThis.normalizeGrupoFinal(groupNameRaw || "");
      const area = globalThis.inferAreaFromGroup(grupo) || globalThis.inferAreaFromGroup(groupNameRaw) || "";

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
async function dashboardFromDbEstratificacion({ from, to, area, grupo, q }) {
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
    const grupoTxt = globalThis.normalizeGrupoFinal(grupoTxtRaw);

    const areaDb = String(r.area || "");
    const areaFinal = areaDb || globalThis.inferAreaFromGroup(grupoTxt) || globalThis.inferAreaFromGroup(grupoTxtRaw) || "CONS";

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
  if (areaSel === "CONS") availableGroups = Array.from(globalThis.GROUPS_CONS);
  else if (areaSel === "RCI") availableGroups = Array.from(globalThis.GROUPS_RCI);
  else availableGroups = Array.from(new Set([...globalThis.GROUPS_CONS, ...globalThis.GROUPS_RCI]));
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
      const grupoTxt = globalThis.normalizeGrupoFinal(grupoTxtRaw);

      const areaDb = String(r.area || "");
      const areaFinal = areaDb || globalThis.inferAreaFromGroup(grupoTxt) || globalThis.inferAreaFromGroup(grupoTxtRaw) || "CONS";

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
globalThis.dashboardFromDbEstratificacion = dashboardFromDbEstratificacion;

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

    const data = await dashboardFromDbEstratificacion({ from, to, area, grupo, q });
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
    const salesSaved = await globalThis.syncSales({ from, to, maxDocs });

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
   Portal central compartido · permisos por módulo/área
   Usa el mismo username + PIN de la intranet (portal_users)
========================================================= */
const PORTAL_ALL_AREAS = ["CONS", "RCI"];
const PORTAL_MODULE_KEYS = new Set(["admin-clientes", "estratificacion", "produccion"]);

function normalizePortalModuleKey(v) {
  const s = String(v || "").trim().toLowerCase();
  return s;
}

function normalizePortalAreaCode(v) {
  const s = String(v || "").trim().toUpperCase();
  if (!s || s === "__ALL__" || s === "ALL" || s === "*") return "__ALL__";
  if (s === "CONS" || s === "CONSUMIDOR") return "CONS";
  if (s === "RCI") return "RCI";
  return "";
}

function normalizePortalAreaList(arr) {
  const out = [];
  const seen = new Set();
  for (const raw of Array.isArray(arr) ? arr : []) {
    const code = normalizePortalAreaCode(raw);
    if (!code || code === "__ALL__") continue;
    if (!PORTAL_ALL_AREAS.includes(code)) continue;
    if (seen.has(code)) continue;
    seen.add(code);
    out.push(code);
  }
  return out;
}

async function ensurePortalModuleScopeDb() {
  if (!hasDb()) return;

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS portal_user_module_areas (
      id BIGSERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL REFERENCES portal_users(id) ON DELETE CASCADE,
      module_key TEXT NOT NULL,
      area_code TEXT NOT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT NOW(),
      UNIQUE(user_id, module_key, area_code)
    );
  `);

  await dbQuery(`
    CREATE INDEX IF NOT EXISTS idx_portal_user_module_areas_user_module
    ON portal_user_module_areas(user_id, module_key);
  `);

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS app_audit_log (
      id BIGSERIAL PRIMARY KEY,
      user_id INTEGER,
      module_key TEXT NOT NULL,
      action_type TEXT NOT NULL,
      entity_type TEXT NOT NULL DEFAULT '',
      entity_code TEXT NOT NULL DEFAULT '',
      entity_name TEXT NOT NULL DEFAULT '',
      area_code TEXT NOT NULL DEFAULT '',
      request_id TEXT NOT NULL DEFAULT '',
      status TEXT NOT NULL DEFAULT 'ok',
      message TEXT NOT NULL DEFAULT '',
      filters_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
  `);

  await dbQuery(`
    CREATE INDEX IF NOT EXISTS idx_app_audit_log_user_module_created
    ON app_audit_log(user_id, module_key, created_at DESC);
  `);

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS app_user_memory (
      user_id INTEGER NOT NULL REFERENCES portal_users(id) ON DELETE CASCADE,
      module_key TEXT NOT NULL,
      memory_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
      PRIMARY KEY (user_id, module_key)
    );
  `);
}

async function portalLoadUserById(userId) {
  if (!hasDb()) return null;
  const id = Number(userId || 0);
  if (!Number.isFinite(id) || id <= 0) return null;

  const r = await dbQuery(
    `SELECT id, username, full_name, role, is_active, permissions_json
     FROM portal_users
     WHERE id=$1
     LIMIT 1`,
    [id]
  );
  return r.rows?.[0] || null;
}

function portalPermissionsFromRow(row) {
  return Array.isArray(row?.permissions_json)
    ? row.permissions_json.map((x) => String(x || "").trim()).filter(Boolean)
    : [];
}

function portalUserIsAdminByRow(row) {
  return String(row?.role || "").trim().toLowerCase() === "admin";
}

function portalUserHasModulePermission(row, moduleKey) {
  if (portalUserIsAdminByRow(row)) return true;
  const perms = portalPermissionsFromRow(row);
  return perms.includes("*") || perms.includes(moduleKey);
}

async function portalGetModuleAreasForUser(userId, moduleKey) {
  if (!hasDb()) return PORTAL_ALL_AREAS.slice();
  const id = Number(userId || 0);
  if (!Number.isFinite(id) || id <= 0) return PORTAL_ALL_AREAS.slice();

  const mk = normalizePortalModuleKey(moduleKey);
  const r = await dbQuery(
    `SELECT area_code
     FROM portal_user_module_areas
     WHERE user_id=$1 AND module_key=$2
     ORDER BY area_code`,
    [id, mk]
  );

  const areas = normalizePortalAreaList((r.rows || []).map((x) => x.area_code));
  return areas.length ? areas : PORTAL_ALL_AREAS.slice();
}

function portalResolveEffectiveArea(access, requestedArea = "__ALL__") {
  const allowed = normalizePortalAreaList(access?.areas || []);
  const req = normalizePortalAreaCode(requestedArea);
  if (!allowed.length || allowed.length >= 2) {
    return req || "__ALL__";
  }
  return allowed[0];
}

function portalAvailableAreasForAccess(access) {
  const allowed = normalizePortalAreaList(access?.areas || []);
  if (!allowed.length || allowed.length >= 2) return ["__ALL__", ...PORTAL_ALL_AREAS];
  return allowed.slice();
}

async function portalGetModuleAccessContext(authUser, moduleKey) {
  const mk = normalizePortalModuleKey(moduleKey);
  if (!PORTAL_MODULE_KEYS.has(mk)) {
    return {
      ok: false,
      canAccess: false,
      isAdmin: false,
      moduleKey: mk,
      areas: [],
      availableAreas: [],
      permissions: [],
      user: authUser || null,
    };
  }

  if (Number(authUser?.id || 0) === 0 && String(authUser?.role || "").toLowerCase() === "admin") {
    return {
      ok: true,
      canAccess: true,
      isAdmin: true,
      moduleKey: mk,
      areas: PORTAL_ALL_AREAS.slice(),
      availableAreas: ["__ALL__", ...PORTAL_ALL_AREAS],
      permissions: ["*"],
      user: authUser,
    };
  }

  const row = await portalLoadUserById(authUser?.id);
  if (!row || !row.is_active) {
    return {
      ok: false,
      canAccess: false,
      isAdmin: false,
      moduleKey: mk,
      areas: [],
      availableAreas: [],
      permissions: [],
      user: authUser || null,
    };
  }

  const permissions = portalPermissionsFromRow(row);
  const isAdmin = portalUserIsAdminByRow(row) || permissions.includes("*");
  const canAccess = isAdmin || permissions.includes(mk);
  const areas = canAccess ? await portalGetModuleAreasForUser(row.id, mk) : [];
  return {
    ok: true,
    canAccess,
    isAdmin,
    moduleKey: mk,
    areas,
    availableAreas: canAccess ? portalAvailableAreasForAccess({ areas }) : [],
    permissions,
    user: {
      id: row.id,
      username: row.username,
      full_name: row.full_name || "",
      role: row.role || "user",
    },
  };
}

function verifyPortalModule(moduleKey, opts = {}) {
  return async (req, res, next) => {
    try {
      const access = await portalGetModuleAccessContext(req.user, moduleKey);
      if (!access?.canAccess) {
        return safeJson(res, 403, { ok: false, message: `Sin acceso al módulo ${moduleKey}` });
      }
      if (opts.adminOnly && !access.isAdmin) {
        return safeJson(res, 403, { ok: false, message: "Permiso de administrador requerido" });
      }
      req.moduleAccess = access;
      return next();
    } catch (e) {
      return safeJson(res, 500, { ok: false, message: e.message || String(e) });
    }
  };
}

async function writePortalAudit({
  userId = null,
  moduleKey = "",
  actionType = "",
  entityType = "",
  entityCode = "",
  entityName = "",
  areaCode = "",
  requestId = "",
  status = "ok",
  message = "",
  filters = {},
  payload = {},
} = {}) {
  try {
    if (!hasDb()) return;
    await dbQuery(
      `INSERT INTO app_audit_log(
        user_id, module_key, action_type, entity_type, entity_code, entity_name,
        area_code, request_id, status, message, filters_json, payload_json
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12::jsonb)`,
      [
        Number.isFinite(Number(userId)) ? Number(userId) : null,
        String(moduleKey || ""),
        String(actionType || ""),
        String(entityType || ""),
        String(entityCode || ""),
        String(entityName || ""),
        String(areaCode || ""),
        String(requestId || ""),
        String(status || "ok"),
        truncate(message || "", 240),
        JSON.stringify(filters || {}),
        JSON.stringify(payload || {}),
      ]
    );
  } catch {}
}

async function savePortalUserMemory(userId, moduleKey, memory = {}) {
  try {
    if (!hasDb()) return;
    const id = Number(userId || 0);
    if (!Number.isFinite(id) || id <= 0) return;

    await dbQuery(
      `INSERT INTO app_user_memory(user_id, module_key, memory_json, updated_at)
       VALUES ($1,$2,$3::jsonb,NOW())
       ON CONFLICT (user_id, module_key)
       DO UPDATE SET memory_json = EXCLUDED.memory_json, updated_at = NOW()`,
      [id, String(moduleKey || ""), JSON.stringify(memory || {})]
    );
  } catch {}
}

async function loadPortalUserMemory(userId, moduleKey) {
  try {
    if (!hasDb()) return {};
    const id = Number(userId || 0);
    if (!Number.isFinite(id) || id <= 0) return {};
    const r = await dbQuery(
      `SELECT memory_json
       FROM app_user_memory
       WHERE user_id=$1 AND module_key=$2
       LIMIT 1`,
      [id, String(moduleKey || "")]
    );
    const raw = r.rows?.[0]?.memory_json;
    return raw && typeof raw === "object" ? raw : {};
  } catch {
    return {};
  }
}

app.get("/api/portal/modules/:moduleKey/access", verifyPortalAuth, async (req, res) => {
  try {
    const moduleKey = normalizePortalModuleKey(req.params.moduleKey);
    if (!PORTAL_MODULE_KEYS.has(moduleKey)) {
      return safeJson(res, 400, { ok: false, message: "moduleKey inválido" });
    }
    const access = await portalGetModuleAccessContext(req.user, moduleKey);
    const memory = access?.canAccess ? await loadPortalUserMemory(req.user?.id, moduleKey) : {};
    return safeJson(res, 200, {
      ok: true,
      moduleKey,
      canAccess: !!access?.canAccess,
      isAdmin: !!access?.isAdmin,
      areas: access?.areas || [],
      availableAreas: access?.availableAreas || [],
      permissions: access?.permissions || [],
      memory,
      user: access?.user || req.user || null,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get("/api/portal/admin/modules/:moduleKey/users", verifyPortalAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });
    const moduleKey = normalizePortalModuleKey(req.params.moduleKey);
    if (!PORTAL_MODULE_KEYS.has(moduleKey)) {
      return safeJson(res, 400, { ok: false, message: "moduleKey inválido" });
    }

    const usersR = await dbQuery(
      `SELECT id, username, full_name, role, is_active, permissions_json, created_at
       FROM portal_users
       ORDER BY id DESC`
    );

    const areaR = await dbQuery(
      `SELECT user_id, module_key, area_code
       FROM portal_user_module_areas
       WHERE module_key=$1`,
      [moduleKey]
    );

    const areaMap = new Map();
    for (const row of areaR.rows || []) {
      const key = `${row.user_id}::${row.module_key}`;
      const cur = areaMap.get(key) || [];
      cur.push(row.area_code);
      areaMap.set(key, cur);
    }

    const users = (usersR.rows || []).map((u) => {
      const perms = Array.isArray(u.permissions_json) ? u.permissions_json : [];
      const isAdmin = String(u.role || "").toLowerCase() === "admin" || perms.includes("*");
      const moduleEnabled = isAdmin || perms.includes(moduleKey);
      const areas = moduleEnabled
        ? normalizePortalAreaList(areaMap.get(`${u.id}::${moduleKey}`) || []).concat()
        : [];
      return {
        id: u.id,
        username: u.username,
        full_name: u.full_name || "",
        role: u.role || "user",
        is_active: !!u.is_active,
        created_at: u.created_at,
        module_enabled: moduleEnabled,
        areas: moduleEnabled ? (areas.length ? areas : PORTAL_ALL_AREAS.slice()) : [],
      };
    });

    return safeJson(res, 200, { ok: true, moduleKey, users });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.patch("/api/portal/admin/modules/:moduleKey/users/:id/access", verifyPortalAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const moduleKey = normalizePortalModuleKey(req.params.moduleKey);
    const userId = Number(req.params.id || 0);
    if (!PORTAL_MODULE_KEYS.has(moduleKey)) {
      return safeJson(res, 400, { ok: false, message: "moduleKey inválido" });
    }
    if (!Number.isFinite(userId) || userId <= 0) {
      return safeJson(res, 400, { ok: false, message: "ID inválido" });
    }

    const moduleEnabled = !!req.body?.module_enabled;
    const desiredAreas = normalizePortalAreaList(req.body?.areas || []);

    const u = await portalLoadUserById(userId);
    if (!u) return safeJson(res, 404, { ok: false, message: "Usuario no encontrado" });

    const isTargetAdmin = portalUserIsAdminByRow(u) || portalPermissionsFromRow(u).includes("*");
    let perms = portalPermissionsFromRow(u);

    if (!isTargetAdmin) {
      if (moduleEnabled && !perms.includes(moduleKey)) perms.push(moduleKey);
      if (!moduleEnabled) perms = perms.filter((x) => x !== moduleKey);
      await dbQuery(
        `UPDATE portal_users
         SET permissions_json=$2::jsonb
         WHERE id=$1`,
        [userId, JSON.stringify(perms)]
      );
    }

    await dbQuery(
      `DELETE FROM portal_user_module_areas
       WHERE user_id=$1 AND module_key=$2`,
      [userId, moduleKey]
    );

    if (moduleEnabled) {
      const areasToStore = desiredAreas.length ? desiredAreas : PORTAL_ALL_AREAS.slice();
      for (const areaCode of areasToStore) {
        await dbQuery(
          `INSERT INTO portal_user_module_areas(user_id, module_key, area_code)
           VALUES ($1,$2,$3)
           ON CONFLICT (user_id, module_key, area_code) DO NOTHING`,
          [userId, moduleKey, areaCode]
        );
      }
    }

    const access = await portalGetModuleAccessContext({ id: userId }, moduleKey);
    return safeJson(res, 200, {
      ok: true,
      moduleKey,
      user: {
        id: userId,
        username: u.username,
        full_name: u.full_name || "",
        role: u.role || "user",
        module_enabled: !!access?.canAccess,
        areas: access?.areas || [],
      },
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get("/api/portal/admin-clientes/dashboard", verifyPortalAuth, verifyPortalModule("admin-clientes"), async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = isISO(req.query?.from) ? String(req.query.from) : addDaysISO(today, -30);
    const to = isISO(req.query?.to) ? String(req.query.to) : today;
    const area = portalResolveEffectiveArea(req.moduleAccess, req.query?.area || "__ALL__");
    const grupo = String(req.query?.grupo || "__ALL__");
    const q = String(req.query?.q || "");

    const data = await dashboardFromDbAdminClientes({ from, to, area, grupo, q });
    data.lastSyncAt = await getState("last_sync_at");
    data.access = {
      moduleKey: "admin-clientes",
      isAdmin: !!req.moduleAccess?.isAdmin,
      areas: req.moduleAccess?.areas || [],
      availableAreas: portalAvailableAreasForAccess(req.moduleAccess),
      effectiveArea: area,
    };

    await savePortalUserMemory(req.user?.id, "admin-clientes", {
      lastRange: { from, to },
      lastFilters: { area, grupo, q },
      updatedAt: new Date().toISOString(),
    });
    await writePortalAudit({
      userId: req.user?.id,
      moduleKey: "admin-clientes",
      actionType: "dashboard_load",
      areaCode: area === "__ALL__" ? "" : area,
      filters: { from, to, area, grupo, q },
    });

    return safeJson(res, 200, data);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get("/api/portal/admin-clientes/details", verifyPortalAuth, verifyPortalModule("admin-clientes"), async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = isISO(req.query?.from) ? String(req.query.from) : addDaysISO(today, -30);
    const to = isISO(req.query?.to) ? String(req.query.to) : today;
    const cardCode = String(req.query?.cardCode || "").trim();
    const warehouse = String(req.query?.warehouse || "").trim();
    const area = portalResolveEffectiveArea(req.moduleAccess, req.query?.area || "__ALL__");
    const grupo = String(req.query?.grupo || "__ALL__");

    if (!cardCode || !warehouse) {
      return safeJson(res, 400, { ok: false, message: "cardCode y warehouse requeridos" });
    }

    const data = await detailsFromDb({ from, to, cardCode, warehouse, area, grupo });
    data.access = {
      moduleKey: "admin-clientes",
      isAdmin: !!req.moduleAccess?.isAdmin,
      areas: req.moduleAccess?.areas || [],
      availableAreas: portalAvailableAreasForAccess(req.moduleAccess),
      effectiveArea: area,
    };

    await savePortalUserMemory(req.user?.id, "admin-clientes", {
      lastRange: { from, to },
      lastFilters: { area, grupo },
      lastFocus: { cardCode, warehouse },
      updatedAt: new Date().toISOString(),
    });
    await writePortalAudit({
      userId: req.user?.id,
      moduleKey: "admin-clientes",
      actionType: "open_detail",
      entityType: "customer_warehouse",
      entityCode: `${cardCode}|${warehouse}`,
      entityName: `${cardCode} @ ${warehouse}`,
      areaCode: area === "__ALL__" ? "" : area,
      filters: { from, to, area, grupo },
    });

    return safeJson(res, 200, data);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get("/api/portal/admin-clientes/top-products", verifyPortalAuth, verifyPortalModule("admin-clientes"), async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = isISO(req.query?.from) ? String(req.query.from) : addDaysISO(today, -30);
    const to = isISO(req.query?.to) ? String(req.query.to) : today;
    const warehouse = String(req.query?.warehouse || "").trim();
    const cardCode = String(req.query?.cardCode || "").trim();
    const area = portalResolveEffectiveArea(req.moduleAccess, req.query?.area || "__ALL__");
    const grupo = String(req.query?.grupo || "__ALL__");
    const limit = Math.max(1, Math.min(200, Number(req.query?.limit || 10)));

    const data = await topProductsFromDb({ from, to, warehouse, cardCode, area, grupo, limit });
    data.access = {
      moduleKey: "admin-clientes",
      isAdmin: !!req.moduleAccess?.isAdmin,
      areas: req.moduleAccess?.areas || [],
      availableAreas: portalAvailableAreasForAccess(req.moduleAccess),
      effectiveArea: area,
    };
    return safeJson(res, 200, data);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get("/api/portal/admin-clientes/export", verifyPortalAuth, verifyPortalModule("admin-clientes"), async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = isISO(req.query?.from) ? String(req.query.from) : addDaysISO(today, -30);
    const to = isISO(req.query?.to) ? String(req.query.to) : today;
    const area = portalResolveEffectiveArea(req.moduleAccess, req.query?.area || "__ALL__");
    const grupo = String(req.query?.grupo || "__ALL__");
    const q = String(req.query?.q || "");

    const data = await dashboardFromDbAdminClientes({ from, to, area, grupo, q });

    const wb = XLSX.utils.book_new();
    const rows = (data.table || []).map((r) => ({
      "Código cliente": r.cardCode,
      "Cliente": r.cardName,
      "Cliente label": r.customer,
      "Bodega": r.warehouse,
      "Ventas netas $": r.dollars,
      "Ganancia bruta $": r.grossProfit,
      "% GP": r.grossPct,
      "Facturas": r.invoices,
      "Notas de crédito": r.creditNotes,
    }));
    const ws = XLSX.utils.json_to_sheet(rows);
    XLSX.utils.book_append_sheet(wb, ws, "Resumen");

    const buf = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });
    await writePortalAudit({
      userId: req.user?.id,
      moduleKey: "admin-clientes",
      actionType: "export_excel",
      areaCode: area === "__ALL__" ? "" : area,
      filters: { from, to, area, grupo, q },
      payload: { rows: rows.length },
    });
    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.setHeader("Content-Disposition", `attachment; filename="admin-clientes_${from}_a_${to}.xlsx"`);
    return res.end(buf);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get("/api/portal/admin-clientes/details/export", verifyPortalAuth, verifyPortalModule("admin-clientes"), async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = isISO(req.query?.from) ? String(req.query.from) : addDaysISO(today, -30);
    const to = isISO(req.query?.to) ? String(req.query.to) : today;
    const cardCode = String(req.query?.cardCode || "").trim();
    const warehouse = String(req.query?.warehouse || "").trim();
    const area = portalResolveEffectiveArea(req.moduleAccess, req.query?.area || "__ALL__");
    const grupo = String(req.query?.grupo || "__ALL__");

    if (!cardCode || !warehouse) {
      return safeJson(res, 400, { ok: false, message: "cardCode y warehouse requeridos" });
    }

    const data = await detailsFromDb({ from, to, cardCode, warehouse, area, grupo });
    const wb = XLSX.utils.book_new();

    const rows = [];
    for (const doc of (data.invoices || [])) {
      for (const ln of (doc.lines || [])) {
        rows.push({
          "Tipo": doc.docTypeLabel,
          "DocNum": doc.docNum,
          "Fecha": doc.docDate,
          "Código cliente": data.cardCode,
          "Bodega": data.warehouse,
          "Área": ln.area,
          "Grupo": ln.grupo,
          "ItemCode": ln.itemCode,
          "Descripción": ln.itemDesc,
          "Cantidad": ln.quantity,
          "Ventas netas $": ln.dollars,
          "Ganancia bruta $": ln.grossProfit,
          "% GP": ln.grossPct,
        });
      }
    }

    const ws = XLSX.utils.json_to_sheet(rows);
    XLSX.utils.book_append_sheet(wb, ws, "Detalle");
    const buf = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });

    await writePortalAudit({
      userId: req.user?.id,
      moduleKey: "admin-clientes",
      actionType: "export_detail_excel",
      entityType: "customer_warehouse",
      entityCode: `${cardCode}|${warehouse}`,
      areaCode: area === "__ALL__" ? "" : area,
      filters: { from, to, area, grupo },
      payload: { rows: rows.length },
    });

    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.setHeader("Content-Disposition", `attachment; filename="detalle_${cardCode}_${warehouse}_${from}_a_${to}.xlsx"`);
    return res.end(buf);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.post("/api/portal/admin-clientes/ai-chat", verifyPortalAuth, verifyPortalModule("admin-clientes"), async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const question = String(req.body?.question || "").trim();
    if (!question) return safeJson(res, 400, { ok: false, message: "question requerida" });

    const fromQ = String(req.body?.from || req.query?.from || "");
    const toQ = String(req.body?.to || req.query?.to || "");
    const cardCode = String(req.body?.cardCode || "").trim();
    const warehouse = String(req.body?.warehouse || "").trim();
    const customerLabel = String(req.body?.customerLabel || "").trim();
    const area = portalResolveEffectiveArea(req.moduleAccess, req.body?.area || req.query?.area || "__ALL__");
    const grupo = String(req.body?.grupo || req.query?.grupo || "__ALL__");
    const q = String(req.body?.q || req.query?.q || "").trim();

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const defaultFrom = addDaysISO(today, -30);
    const from = isISO(fromQ) ? fromQ : defaultFrom;
    const to = isISO(toQ) ? toQ : today;

    const dashboard = await dashboardFromDbAdminClientes({ from, to, area, grupo, q });
    const analytics = await buildAdminClientesAiAnalytics({ from, to, area, grupo, q });

    const resolvedFocus = adminResolveDashboardFocus({
      dashboard,
      question,
      q,
      cardCode,
      warehouse,
      customerLabel,
    });

    const focusCardCode = String(resolvedFocus?.cardCode || cardCode || "").trim();
    const focusWarehouse = String(resolvedFocus?.warehouse || warehouse || "").trim();
    const focusLabel = String(resolvedFocus?.label || customerLabel || "").trim();

    let detail = null;
    if (focusCardCode && focusWarehouse) {
      detail = await detailsFromDb({ from, to, cardCode: focusCardCode, warehouse: focusWarehouse, area, grupo });
    }

    const recommendationContext = await buildAdminClientesRecommendationAnalytics({
      from,
      to,
      area,
      grupo,
      targetCardCode: focusCardCode,
      customerLabel: focusLabel,
      question,
    });

    const out = await openaiDbAnalystChat({
      question,
      dashboard,
      analytics,
      detail,
      customerLabel: focusLabel,
      recommendationContext,
    });

    await savePortalUserMemory(req.user?.id, "admin-clientes", {
      lastRange: { from, to },
      lastFilters: { area, grupo, q },
      lastFocus: focusCardCode ? { cardCode: focusCardCode, warehouse: focusWarehouse, label: focusLabel } : null,
      lastQuestion: question,
      lastAnswerPreview: truncate(out.answer || "", 300),
      updatedAt: new Date().toISOString(),
    });

    await writePortalAudit({
      userId: req.user?.id,
      moduleKey: "admin-clientes",
      actionType: "ai_question",
      entityType: focusCardCode ? "customer_warehouse" : "",
      entityCode: focusCardCode ? `${focusCardCode}|${focusWarehouse}` : "",
      entityName: focusLabel || "",
      areaCode: area === "__ALL__" ? "" : area,
      filters: { from, to, area, grupo, q },
      payload: {
        inferred_focus: !!resolvedFocus?.cardCode,
        question: truncate(question, 400),
      },
    });

    return safeJson(res, 200, {
      ok: true,
      answer: out.answer,
      model: out.model,
      source: "db",
      range: { from, to },
      filters: { area, grupo, q },
      access: {
        moduleKey: "admin-clientes",
        isAdmin: !!req.moduleAccess?.isAdmin,
        areas: req.moduleAccess?.areas || [],
        availableAreas: portalAvailableAreasForAccess(req.moduleAccess),
        effectiveArea: area,
      },
      focus: focusCardCode ? {
        cardCode: focusCardCode,
        warehouse: focusWarehouse,
        customerLabel: focusLabel || `${focusCardCode}`,
        label: focusLabel || `${focusCardCode}`,
        inferred: !(cardCode && warehouse),
        source: resolvedFocus?.source || (cardCode && warehouse ? "explicit" : "none"),
      } : null,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.post("/api/portal/admin-clientes/sync", verifyPortalAuth, verifyPortalModule("admin-clientes", { adminOnly: true }), async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });
    if (missingSapEnv()) return safeJson(res, 500, { ok: false, message: "SAP env incompleto" });

    const fromQ = String(req.query?.from || req.body?.from || "");
    const toQ = String(req.query?.to || req.body?.to || "");
    const maxDocs = Math.max(500, Math.min(50000, Number(req.query?.maxDocs || req.body?.maxDocs || 12000)));

    if (!isISO(fromQ) || !isISO(toQ)) {
      return safeJson(res, 400, { ok: false, message: "from y to deben ser YYYY-MM-DD" });
    }

    const out = await syncRangeToDb({ from: fromQ, to: toQ, maxDocs });
    await writePortalAudit({
      userId: req.user?.id,
      moduleKey: "admin-clientes",
      actionType: "sync_range",
      filters: { from: fromQ, to: toQ, maxDocs },
      payload: out,
    });
    return safeJson(res, 200, { ok: true, ...out, from: fromQ, to: toQ, maxDocs });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.post("/api/portal/admin-clientes/sync/recent", verifyPortalAuth, verifyPortalModule("admin-clientes", { adminOnly: true }), async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });
    if (missingSapEnv()) return safeJson(res, 500, { ok: false, message: "SAP env incompleto" });

    const days = Math.max(1, Math.min(90, Number(req.query?.days || req.body?.days || 10)));
    const maxDocs = Math.max(500, Math.min(50000, Number(req.query?.maxDocs || req.body?.maxDocs || 12000)));

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = addDaysISO(today, -(days - 1));

    const out = await syncRangeToDb({ from, to: today, maxDocs });
    await writePortalAudit({
      userId: req.user?.id,
      moduleKey: "admin-clientes",
      actionType: "sync_recent",
      filters: { from, to: today, days, maxDocs },
      payload: out,
    });
    return safeJson(res, 200, { ok: true, ...out, from, to: today, days, maxDocs });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

__extraBootTasks.push(async () => {
  try {
    await ensurePortalModuleScopeDb();
  } catch (e) {
    console.error("Portal module scope DB init error:", e.message || String(e));
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
  console.log(`SKIP duplicate listen on :${PORT}`);
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
function num(x, d = 0) {
  const n = Number(x || 0);
  return Number.isFinite(n) ? Number(n.toFixed(d)) : 0;
}
function money2(x) {
  return num(x, 2);
}
function canonicalInvoiceGroup(raw) {
  if (typeof globalThis.normalizeGrupoFinal === "function") {
    return globalThis.normalizeGrupoFinal(raw);
  }
  return String(raw || "").trim() || "Sin grupo";
}
function inferInvoiceArea(grupo, areaDb = "") {
  const a = String(areaDb || "").trim().toUpperCase();
  if (a === "CONS" || a === "RCI") return a;
  if (typeof globalThis.inferAreaFromGroup === "function") {
    return globalThis.inferAreaFromGroup(grupo) || "";
  }
  return "";
}
const GROUPS_CONS_LIST = Array.from(globalThis.GROUPS_CONS || []);
const GROUPS_RCI_LIST = Array.from(globalThis.GROUPS_RCI || []);
function getAllowedGroupsByArea(area) {
  const a = String(area || "__ALL__").trim().toUpperCase();
  if (a === "CONS") return GROUPS_CONS_LIST.slice();
  if (a === "RCI") return GROUPS_RCI_LIST.slice();
  return Array.from(new Set([...GROUPS_CONS_LIST, ...GROUPS_RCI_LIST]));
}
function signedDocEntry(docType, docEntry) {
  const de = Math.abs(Number(docEntry || 0));
  return String(docType || "INV").toUpperCase() === "CRN" ? -de : de;
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
      doc_type       TEXT    NOT NULL DEFAULT 'INV',
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

  await dbQuery(`ALTER TABLE fact_invoice_lines ADD COLUMN IF NOT EXISTS doc_type TEXT NOT NULL DEFAULT 'INV';`);
  await dbQuery(`ALTER TABLE fact_invoice_lines ADD COLUMN IF NOT EXISTS item_code TEXT NOT NULL DEFAULT '';`);
  await dbQuery(`ALTER TABLE fact_invoice_lines ADD COLUMN IF NOT EXISTS item_desc TEXT NOT NULL DEFAULT '';`);
  await dbQuery(`ALTER TABLE fact_invoice_lines ADD COLUMN IF NOT EXISTS quantity NUMERIC(18,4) NOT NULL DEFAULT 0;`);
  await dbQuery(`ALTER TABLE fact_invoice_lines ADD COLUMN IF NOT EXISTS gross_profit NUMERIC(18,2) NOT NULL DEFAULT 0;`);

  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_fact_doc_date ON fact_invoice_lines(doc_date);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_fact_wh ON fact_invoice_lines(warehouse_code);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_fact_card ON fact_invoice_lines(card_code);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_fact_item ON fact_invoice_lines(item_code);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_fact_type ON fact_invoice_lines(doc_type);`);

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
   ✅ SAP: scan invoices + credit notes
========================================================= */
async function scanDocHeaders(entity, { f, t, maxDocs = 3000 }) {
  const toPlus1 = addDaysISO(t, 1);
  const batchTop = 200;
  let skipSap = 0;
  const out = [];

  for (let page = 0; page < 500; page++) {
    const raw = await slFetch(
      `/${entity}?$select=DocEntry,DocNum,DocDate,CardCode,CardName` +
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
        CardCode: String(r.CardCode || ""),
        CardName: String(r.CardName || ""),
      });
      if (out.length >= maxDocs) return out;
    }
  }

  return out;
}
async function getSapDocument(entity, docEntry) {
  const de = Number(docEntry);
  if (!Number.isFinite(de) || de <= 0) return null;
  return slFetch(`/${entity}(${de})`, { timeoutMs: 90000 });
}

/* =========================================================
   ✅ Sync: upsert net invoice lines
========================================================= */
function pickGrossProfit(ln) {
  const candidates = [ln?.GrossProfit, ln?.GrossProfitTotal, ln?.GrossProfitFC, ln?.GrossProfitSC];
  for (const c of candidates) {
    const n = Number(c);
    if (Number.isFinite(n)) return n;
  }
  return 0;
}

async function upsertLinesToDb(docType, sign, header, docFull) {
  const lines = Array.isArray(docFull?.DocumentLines) ? docFull.DocumentLines : [];
  if (!lines.length) return 0;

  const storedDocEntry = signedDocEntry(docType, header.DocEntry);
  const docNum = Number(header.DocNum);
  const docDate = String(header.DocDate || "").slice(0, 10);
  const cardCode = String(header.CardCode || "");
  const cardName = String(header.CardName || "");

  const values = [];
  const params = [];
  let p = 1;

  for (const ln of lines) {
    const lineNum = Number(ln.LineNum);
    if (!Number.isFinite(lineNum)) continue;

    const wh = String(ln.WarehouseCode || "SIN_WH").trim() || "SIN_WH";
    const lt = Math.abs(Number(ln.LineTotal || 0)) * sign;
    const qty = Math.abs(Number(ln.Quantity || 0)) * sign;
    const itemCode = String(ln.ItemCode || "").trim();
    const itemDesc = String(ln.ItemDescription || ln.ItemName || "").trim();
    const gp = Math.abs(Number(pickGrossProfit(ln) || 0)) * sign;

    params.push(
      storedDocEntry,
      lineNum,
      String(docType || "INV").toUpperCase(),
      docNum,
      docDate,
      cardCode,
      cardName,
      wh,
      itemCode,
      itemDesc,
      qty,
      lt,
      gp
    );
    values.push(
      `($${p++},$${p++},$${p++},$${p++},$${p++}::date,$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`
    );
  }

  if (!values.length) return 0;

  await dbQuery(
    `
    INSERT INTO fact_invoice_lines
      (doc_entry,line_num,doc_type,doc_num,doc_date,card_code,card_name,warehouse_code,item_code,item_desc,quantity,line_total,gross_profit)
    VALUES ${values.join(",")}
    ON CONFLICT (doc_entry,line_num)
    DO UPDATE SET
      doc_type=EXCLUDED.doc_type,
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

  // limpia el rango para evitar residuos de syncs anteriores
  await dbQuery(`DELETE FROM fact_invoice_lines WHERE doc_date >= $1::date AND doc_date <= $2::date`, [from, to]);

  const invHeaders = await scanDocHeaders("Invoices", { f: from, t: to, maxDocs });
  const crnHeaders = await scanDocHeaders("CreditNotes", { f: from, t: to, maxDocs });

  let totalLines = 0;

  for (const h of invHeaders) {
    try {
      const full = await getSapDocument("Invoices", h.DocEntry);
      totalLines += await upsertLinesToDb("INV", +1, h, full);
    } catch (e) {
      console.error("Invoice sync error", h?.DocEntry, e?.message || String(e));
    }
    await sleep(20);
  }

  for (const h of crnHeaders) {
    try {
      const full = await getSapDocument("CreditNotes", h.DocEntry);
      totalLines += await upsertLinesToDb("CRN", -1, h, full);
    } catch (e) {
      console.error("Credit note sync error", h?.DocEntry, e?.message || String(e));
    }
    await sleep(20);
  }

  await setState("last_sync_from", from);
  await setState("last_sync_to", to);
  await setState("last_sync_at", new Date().toISOString());

  return { invoices: invHeaders.length, creditNotes: crnHeaders.length, lines: totalLines };
}

/* =========================================================
   ✅ Base rows + filters
========================================================= */
async function fetchInvoiceRows({ from, to, cardCode = "", warehouse = "", q = "" }) {
  const params = [from, to];
  const where = [`l.doc_date >= $1::date`, `l.doc_date <= $2::date`];

  if (cardCode) {
    params.push(cardCode);
    where.push(`l.card_code = $${params.length}`);
  }
  if (warehouse) {
    params.push(warehouse);
    where.push(`l.warehouse_code = $${params.length}`);
  }
  if (q) {
    params.push(`%${String(q).trim()}%`);
    const idx = params.length;
    where.push(`(l.card_code ILIKE $${idx} OR l.card_name ILIKE $${idx} OR l.item_code ILIKE $${idx} OR l.item_desc ILIKE $${idx})`);
  }

  const sql = `
    SELECT
      l.doc_type,
      l.doc_entry,
      l.line_num,
      l.doc_num,
      l.doc_date::text AS doc_date,
      l.card_code,
      l.card_name,
      l.warehouse_code,
      l.item_code,
      l.item_desc,
      l.quantity,
      l.line_total,
      l.gross_profit,
      COALESCE(NULLIF(g.area,''), '') AS area_db,
      COALESCE(NULLIF(g.grupo,''), NULLIF(g.group_name,''), 'Sin grupo') AS grupo_raw
    FROM fact_invoice_lines l
    LEFT JOIN item_group_cache g
      ON g.item_code = l.item_code
    WHERE ${where.join(" AND ")}
    ORDER BY l.doc_date DESC, l.doc_num DESC, l.line_num ASC
  `;
  const r = await dbQuery(sql, params);
  return (r.rows || []).map((x) => {
    const grupo = canonicalInvoiceGroup(x.grupo_raw || "Sin grupo");
    const area = inferInvoiceArea(grupo, x.area_db || "");
    return {
      docType: String(x.doc_type || "INV").toUpperCase(),
      docEntry: Number(x.doc_entry || 0),
      lineNum: Number(x.line_num || 0),
      docNum: Number(x.doc_num || 0),
      docDate: isoDateOnly(x.doc_date),
      cardCode: String(x.card_code || ""),
      cardName: String(x.card_name || ""),
      warehouse: String(x.warehouse_code || ""),
      itemCode: String(x.item_code || ""),
      itemDesc: String(x.item_desc || ""),
      quantity: estratAiNum(x.quantity, 4),
      dollars: money2(x.line_total),
      grossProfit: money2(x.gross_profit),
      grupo,
      area,
    };
  });
}


function isoDateOnly(v) {
  if (!v) return "";
  if (typeof v === "string") return v.slice(0, 10);
  if (v instanceof Date && !Number.isNaN(v.getTime())) return v.toISOString().slice(0, 10);
  return String(v).slice(0, 10);
}

function applyAreaGroupFilters(rows, { area = "__ALL__", grupo = "__ALL__" } = {}) {
  const areaSel = String(area || "__ALL__").trim().toUpperCase();
  const grupoSel = String(grupo || "__ALL__").trim();
  let out = Array.isArray(rows) ? rows.slice() : [];

  if (areaSel !== "__ALL__") {
    out = out.filter((r) => String(r.area || "").toUpperCase() === areaSel);
  }
  if (grupoSel !== "__ALL__") {
    const gSel = canonicalInvoiceGroup(grupoSel);
    out = out.filter((r) => canonicalInvoiceGroup(r.grupo) === gSel);
  }
  return out;
}

function availableGroupsForArea(area) {
  return getAllowedGroupsByArea(area).slice().sort((a, b) => a.localeCompare(b));
}

/* =========================================================
   ✅ Dashboard from DB (neto)
========================================================= */
async function dashboardFromDbAdminClientes({ from, to, area = "__ALL__", grupo = "__ALL__", q = "" }) {
  const baseRows = await fetchInvoiceRows({ from, to, q });
  const rows = applyAreaGroupFilters(baseRows, { area, grupo });

  const docInvSet = new Set();
  const docCrnSet = new Set();
  const monthMap = new Map();
  const tableMap = new Map();
  const whMap = new Map();
  const custMap = new Map();

  let totalDol = 0;
  let totalGP = 0;

  for (const r of rows) {
    totalDol += Number(r.dollars || 0);
    totalGP += Number(r.grossProfit || 0);

    const docKey = `${r.docType}:${r.docEntry}`;
    if (r.docType === "CRN") docCrnSet.add(docKey);
    else docInvSet.add(docKey);

    const month = String(r.docDate || "").slice(0, 7);
    if (month) {
      const cur = monthMap.get(month) || { month, invoicesSet: new Set(), creditNotesSet: new Set(), dollars: 0, grossProfit: 0 };
      if (r.docType === "CRN") cur.creditNotesSet.add(docKey);
      else cur.invoicesSet.add(docKey);
      cur.dollars += Number(r.dollars || 0);
      cur.grossProfit += Number(r.grossProfit || 0);
      monthMap.set(month, cur);
    }

    const rowKey = `${r.cardCode}||${r.cardName}||${r.warehouse}`;
    const rowCur = tableMap.get(rowKey) || {
      cardCode: r.cardCode,
      cardName: r.cardName,
      customer: `${r.cardCode} · ${r.cardName}`,
      warehouse: r.warehouse,
      dollars: 0,
      grossProfit: 0,
      invSet: new Set(),
      crnSet: new Set(),
    };
    rowCur.dollars += Number(r.dollars || 0);
    rowCur.grossProfit += Number(r.grossProfit || 0);
    if (r.docType === "CRN") rowCur.crnSet.add(docKey);
    else rowCur.invSet.add(docKey);
    tableMap.set(rowKey, rowCur);

    const whCur = whMap.get(r.warehouse) || { warehouse: r.warehouse, dollars: 0 };
    whCur.dollars += Number(r.dollars || 0);
    whMap.set(r.warehouse, whCur);

    const custKey = `${r.cardCode}||${r.cardName}`;
    const custCur = custMap.get(custKey) || {
      cardCode: r.cardCode,
      cardName: r.cardName,
      customer: `${r.cardCode} · ${r.cardName}`,
      dollars: 0,
    };
    custCur.dollars += Number(r.dollars || 0);
    custMap.set(custKey, custCur);
  }

  const byMonth = Array.from(monthMap.values())
    .sort((a, b) => String(a.month).localeCompare(String(b.month)))
    .map((x) => {
      const dol = money2(x.dollars);
      const gp = money2(x.grossProfit);
      return {
        month: x.month,
        invoices: x.invoicesSet.size,
        creditNotes: x.creditNotesSet.size,
        dollars: dol,
        grossProfit: gp,
        grossPct: dol !== 0 ? num((gp / dol) * 100, 2) : 0,
      };
    });

  const table = Array.from(tableMap.values())
    .map((x) => {
      const dol = money2(x.dollars);
      const gp = money2(x.grossProfit);
      return {
        cardCode: x.cardCode,
        cardName: x.cardName,
        customer: x.customer,
        warehouse: x.warehouse,
        dollars: dol,
        grossProfit: gp,
        grossPct: dol !== 0 ? num((gp / dol) * 100, 2) : 0,
        invoices: x.invSet.size,
        creditNotes: x.crnSet.size,
      };
    })
    .sort((a, b) => Number(b.dollars || 0) - Number(a.dollars || 0));

  const topWarehouses = Array.from(whMap.values())
    .map((x) => ({ warehouse: x.warehouse, dollars: money2(x.dollars) }))
    .sort((a, b) => Number(b.dollars || 0) - Number(a.dollars || 0))
    .slice(0, 50);

  const topCustomers = Array.from(custMap.values())
    .map((x) => ({ cardCode: x.cardCode, cardName: x.cardName, customer: x.customer, dollars: money2(x.dollars) }))
    .sort((a, b) => Number(b.dollars || 0) - Number(a.dollars || 0))
    .slice(0, 50);

  const totalDol2 = money2(totalDol);
  const totalGP2 = money2(totalGP);

  return {
    ok: true,
    from,
    to,
    area,
    grupo,
    availableAreas: ["__ALL__", "CONS", "RCI"],
    availableGroups: availableGroupsForArea(area),
    totals: {
      invoices: docInvSet.size,
      creditNotes: docCrnSet.size,
      dollars: totalDol2,
      grossProfit: totalGP2,
      grossPct: totalDol2 !== 0 ? num((totalGP2 / totalDol2) * 100, 2) : 0,
    },
    byMonth,
    topWarehouses,
    topCustomers,
    table,
  };
}

/* =========================================================
   ✅ Details + Top products
========================================================= */
async function detailsFromDb({ from, to, cardCode, warehouse, area = "__ALL__", grupo = "__ALL__" }) {
  let rows = await fetchInvoiceRows({ from, to, cardCode, warehouse });
  rows = applyAreaGroupFilters(rows, { area, grupo });

  const map = new Map();
  for (const r of rows) {
    const docKey = `${r.docType}:${r.docEntry}`;
    if (!map.has(docKey)) {
      map.set(docKey, {
        docType: r.docType,
        docTypeLabel: r.docType === "CRN" ? "Nota de crédito" : "Factura",
        docNum: r.docNum,
        docEntry: r.docEntry,
        docDate: r.docDate,
        warehouse: r.warehouse,
        cardCode: r.cardCode,
        lines: [],
        totals: { qty: 0, dollars: 0, grossProfit: 0 },
      });
    }
    const it = map.get(docKey);
    it.lines.push({
      itemCode: r.itemCode,
      itemDesc: r.itemDesc,
      quantity: r.quantity,
      dollars: r.dollars,
      grossProfit: r.grossProfit,
      grossPct: r.dollars !== 0 ? num((r.grossProfit / r.dollars) * 100, 2) : 0,
      area: r.area,
      grupo: r.grupo,
    });
    it.totals.qty += Number(r.quantity || 0);
    it.totals.dollars += Number(r.dollars || 0);
    it.totals.grossProfit += Number(r.grossProfit || 0);
  }

  const invoices = Array.from(map.values())
    .map((inv) => {
      inv.totals.qty = num(inv.totals.qty, 4);
      inv.totals.dollars = money2(inv.totals.dollars);
      inv.totals.grossProfit = money2(inv.totals.grossProfit);
      inv.totals.grossPct = inv.totals.dollars !== 0 ? num((inv.totals.grossProfit / inv.totals.dollars) * 100, 2) : 0;
      return inv;
    })
    .sort((a, b) => {
      if (String(b.docDate) !== String(a.docDate)) return String(b.docDate).localeCompare(String(a.docDate));
      if (b.docNum !== a.docNum) return Number(b.docNum) - Number(a.docNum);
      return String(a.docType).localeCompare(String(b.docType));
    });

  const totals = invoices.reduce(
    (a, x) => {
      if (x.docType === "INV") a.invoices += 1;
      else a.creditNotes += 1;
      a.qty += Number(x.totals.qty || 0);
      a.dollars += Number(x.totals.dollars || 0);
      a.grossProfit += Number(x.totals.grossProfit || 0);
      return a;
    },
    { invoices: 0, creditNotes: 0, qty: 0, dollars: 0, grossProfit: 0 }
  );

  totals.qty = num(totals.qty, 4);
  totals.dollars = money2(totals.dollars);
  totals.grossProfit = money2(totals.grossProfit);
  totals.grossPct = totals.dollars !== 0 ? num((totals.grossProfit / totals.dollars) * 100, 2) : 0;

  return { ok: true, from, to, area, grupo, cardCode, warehouse, totals, invoices };
}

async function topProductsFromDb({ from, to, warehouse = "", cardCode = "", area = "__ALL__", grupo = "__ALL__", limit = 10 }) {
  let rows = await fetchInvoiceRows({ from, to, cardCode, warehouse });
  rows = applyAreaGroupFilters(rows, { area, grupo });

  const map = new Map();
  for (const r of rows) {
    if (!r.itemCode) continue;
    const cur = map.get(r.itemCode) || {
      itemCode: r.itemCode,
      itemDesc: r.itemDesc,
      qty: 0,
      dollars: 0,
      grossProfit: 0,
      invSet: new Set(),
      area: r.area,
      grupo: r.grupo,
    };
    cur.qty += Number(r.quantity || 0);
    cur.dollars += Number(r.dollars || 0);
    cur.grossProfit += Number(r.grossProfit || 0);
    if (r.docType === "INV") cur.invSet.add(`${r.docType}:${r.docEntry}`);
    map.set(r.itemCode, cur);
  }

  return {
    ok: true,
    from,
    to,
    warehouse: warehouse || null,
    cardCode: cardCode || null,
    area,
    grupo,
    top: Array.from(map.values())
      .map((x) => {
        const dol = money2(x.dollars);
        const gp = money2(x.grossProfit);
        return {
          itemCode: x.itemCode,
          itemDesc: x.itemDesc,
          qty: num(x.qty, 4),
          dollars: dol,
          grossProfit: gp,
          grossPct: dol !== 0 ? num((gp / dol) * 100, 2) : 0,
          invoices: x.invSet.size,
          area: x.area,
          grupo: x.grupo,
        };
      })
      .sort((a, b) => Number(b.dollars || 0) - Number(a.dollars || 0))
      .slice(0, Math.max(1, Math.min(200, Number(limit || 10)))),
  };
}


/* =========================================================
   ✅ IA (admin-clientes) — contexto rico para análisis mensual
========================================================= */
function aiPctDelta(base, next) {
  const a = Number(base || 0);
  const b = Number(next || 0);
  if (a === 0 && b === 0) return 0;
  if (a === 0 && b !== 0) return 100;
  return num(((b - a) / a) * 100, 2);
}

function aiStatusFirstVsLast(firstSales, lastSales) {
  const a = Number(firstSales || 0);
  const b = Number(lastSales || 0);

  if (a > 0 && b === 0) return "compró en el primer mes y no compró en el último";
  if (a === 0 && b > 0) return "no compró en el primer mes y sí compró en el último";
  if (a > 0 && b > a) return "creció entre el primer y el último mes";
  if (a > 0 && b < a) return "cayó entre el primer y el último mes";
  if (a === 0 && b === 0) return "sin venta en ambos extremos del rango";
  return "comportamiento estable";
}

function aiBucketTopItems(bucketMap, limit = 8) {
  return Array.from(bucketMap.values())
    .map((x) => ({
      itemCode: x.itemCode,
      itemDesc: x.itemDesc,
      qty: num(x.qty, 4),
      dollars: money2(x.dollars),
      grossProfit: money2(x.grossProfit),
      grossPct: x.dollars !== 0 ? num((x.grossProfit / x.dollars) * 100, 2) : 0,
      area: x.area,
      grupo: x.grupo,
    }))
    .sort((a, b) => Math.abs(Number(b.dollars || 0)) - Math.abs(Number(a.dollars || 0)))
    .slice(0, Math.max(1, Math.min(50, Number(limit || 8))));
}

function aiTopChangedItems(sourceBucket, targetBucket, mode = "lost", limit = 8) {
  const src = sourceBucket instanceof Map ? sourceBucket : new Map();
  const trg = targetBucket instanceof Map ? targetBucket : new Map();
  const out = [];

  if (mode === "lost") {
    for (const item of src.values()) {
      const trgItem = trg.get(item.itemCode);
      const trgDol = Number(trgItem?.dollars || 0);
      if (Math.abs(Number(item.dollars || 0)) > 0.0001 && Math.abs(trgDol) <= 0.0001) {
        out.push({
          itemCode: item.itemCode,
          itemDesc: item.itemDesc,
          qty: num(item.qty, 4),
          dollars: money2(item.dollars),
          grossProfit: money2(item.grossProfit),
          area: item.area,
          grupo: item.grupo,
        });
      }
    }
  } else {
    for (const item of trg.values()) {
      const srcItem = src.get(item.itemCode);
      const srcDol = Number(srcItem?.dollars || 0);
      if (Math.abs(Number(item.dollars || 0)) > 0.0001 && Math.abs(srcDol) <= 0.0001) {
        out.push({
          itemCode: item.itemCode,
          itemDesc: item.itemDesc,
          qty: num(item.qty, 4),
          dollars: money2(item.dollars),
          grossProfit: money2(item.grossProfit),
          area: item.area,
          grupo: item.grupo,
        });
      }
    }
  }

  return out
    .sort((a, b) => Math.abs(Number(b.dollars || 0)) - Math.abs(Number(a.dollars || 0)))
    .slice(0, Math.max(1, Math.min(50, Number(limit || 8))));
}

async function buildAdminClientesAiAnalytics({ from, to, area = "__ALL__", grupo = "__ALL__", q = "" }) {
  let rows = await fetchInvoiceRows({ from, to, q });
  rows = applyAreaGroupFilters(rows, { area, grupo });

  const months = Array.from(
    new Set(
      rows
        .map((r) => String(r.docDate || "").slice(0, 7))
        .filter(Boolean)
    )
  ).sort((a, b) => a.localeCompare(b));

  const firstMonth = months[0] || "";
  const lastMonth = months[months.length - 1] || "";

  const customerMonthMap = new Map();
  const customerProfileMap = new Map();
  const customerMonthItemBuckets = new Map();
  const monthTotalsMap = new Map();

  for (const r of rows) {
    const month = String(r.docDate || "").slice(0, 7);
    if (!month) continue;

    const docKey = `${r.docType}:${r.docEntry}`;
    const customerKey = `${r.cardCode}||${r.cardName}`;
    const cmKey = `${month}||${r.cardCode}||${r.cardName}`;

    {
      const cur = monthTotalsMap.get(month) || {
        month,
        dollars: 0,
        grossProfit: 0,
        invoicesSet: new Set(),
        creditNotesSet: new Set(),
      };
      cur.dollars += Number(r.dollars || 0);
      cur.grossProfit += Number(r.grossProfit || 0);
      if (r.docType === "CRN") cur.creditNotesSet.add(docKey);
      else cur.invoicesSet.add(docKey);
      monthTotalsMap.set(month, cur);
    }

    {
      const cur = customerMonthMap.get(cmKey) || {
        month,
        cardCode: r.cardCode,
        cardName: r.cardName,
        customer: `${r.cardCode} · ${r.cardName}`,
        dollars: 0,
        grossProfit: 0,
        qty: 0,
        invoicesSet: new Set(),
        creditNotesSet: new Set(),
        warehousesMap: new Map(),
      };

      cur.dollars += Number(r.dollars || 0);
      cur.grossProfit += Number(r.grossProfit || 0);
      cur.qty += Number(r.quantity || 0);

      if (r.docType === "CRN") cur.creditNotesSet.add(docKey);
      else cur.invoicesSet.add(docKey);

      const wh = String(r.warehouse || "SIN_WH");
      const whCur = cur.warehousesMap.get(wh) || { warehouse: wh, dollars: 0 };
      whCur.dollars += Number(r.dollars || 0);
      cur.warehousesMap.set(wh, whCur);

      customerMonthMap.set(cmKey, cur);
    }

    {
      const cur = customerProfileMap.get(customerKey) || {
        cardCode: r.cardCode,
        cardName: r.cardName,
        customer: `${r.cardCode} · ${r.cardName}`,
        totalSales: 0,
        totalGP: 0,
        monthsActive: new Set(),
        salesByMonth: {},
        gpByMonth: {},
        topWarehousesMap: new Map(),
      };

      cur.totalSales += Number(r.dollars || 0);
      cur.totalGP += Number(r.grossProfit || 0);
      cur.monthsActive.add(month);
      cur.salesByMonth[month] = Number(cur.salesByMonth[month] || 0) + Number(r.dollars || 0);
      cur.gpByMonth[month] = Number(cur.gpByMonth[month] || 0) + Number(r.grossProfit || 0);

      const wh = String(r.warehouse || "SIN_WH");
      const whCur = cur.topWarehousesMap.get(wh) || { warehouse: wh, dollars: 0 };
      whCur.dollars += Number(r.dollars || 0);
      cur.topWarehousesMap.set(wh, whCur);

      customerProfileMap.set(customerKey, cur);
    }

    {
      const bucket = customerMonthItemBuckets.get(cmKey) || new Map();
      const itemKey = String(r.itemCode || "").trim() || `SIN_ITEM__${String(r.itemDesc || "").trim()}`;

      const cur = bucket.get(itemKey) || {
        itemCode: String(r.itemCode || ""),
        itemDesc: String(r.itemDesc || ""),
        qty: 0,
        dollars: 0,
        grossProfit: 0,
        area: r.area,
        grupo: r.grupo,
      };

      cur.qty += Number(r.quantity || 0);
      cur.dollars += Number(r.dollars || 0);
      cur.grossProfit += Number(r.grossProfit || 0);

      bucket.set(itemKey, cur);
      customerMonthItemBuckets.set(cmKey, bucket);
    }
  }

  const rangeMonthSummary = Array.from(monthTotalsMap.values())
    .map((x) => {
      const dol = money2(x.dollars);
      const gp = money2(x.grossProfit);
      return {
        month: x.month,
        invoices: x.invoicesSet.size,
        creditNotes: x.creditNotesSet.size,
        dollars: dol,
        grossProfit: gp,
        grossPct: dol !== 0 ? num((gp / dol) * 100, 2) : 0,
      };
    })
    .sort((a, b) => String(a.month).localeCompare(String(b.month)));

  const customerMonthly = Array.from(customerMonthMap.values())
    .map((x) => {
      const dol = money2(x.dollars);
      const gp = money2(x.grossProfit);
      const topWarehouses = Array.from(x.warehousesMap.values())
        .map((w) => ({ warehouse: w.warehouse, dollars: money2(w.dollars) }))
        .sort((a, b) => Number(b.dollars || 0) - Number(a.dollars || 0))
        .slice(0, 5);

      return {
        month: x.month,
        cardCode: x.cardCode,
        cardName: x.cardName,
        customer: x.customer,
        qty: num(x.qty, 4),
        dollars: dol,
        grossProfit: gp,
        grossPct: dol !== 0 ? num((gp / dol) * 100, 2) : 0,
        invoices: x.invoicesSet.size,
        creditNotes: x.creditNotesSet.size,
        topWarehouses,
      };
    })
    .sort((a, b) => {
      if (String(a.month) !== String(b.month)) return String(a.month).localeCompare(String(b.month));
      return Number(b.dollars || 0) - Number(a.dollars || 0);
    })
    .slice(0, 300);

  const customerMonthTopItems = Array.from(customerMonthItemBuckets.entries())
    .map(([cmKey, bucket]) => {
      const [month, cardCode, cardName] = cmKey.split("||");
      return {
        month,
        cardCode,
        cardName,
        customer: `${cardCode} · ${cardName}`,
        topItems: aiBucketTopItems(bucket, 10),
      };
    })
    .sort((a, b) => {
      if (String(a.month) !== String(b.month)) return String(a.month).localeCompare(String(b.month));
      const aTop = Number(a.topItems?.[0]?.dollars || 0);
      const bTop = Number(b.topItems?.[0]?.dollars || 0);
      return Math.abs(bTop) - Math.abs(aTop);
    })
    .slice(0, 180);

  const customerComparisons = Array.from(customerProfileMap.values())
    .map((x) => {
      const salesByMonth = {};
      const gpByMonth = {};

      for (const m of months) {
        salesByMonth[m] = money2(x.salesByMonth[m] || 0);
        gpByMonth[m] = money2(x.gpByMonth[m] || 0);
      }

      const salesFirst = money2(x.salesByMonth[firstMonth] || 0);
      const salesLast = money2(x.salesByMonth[lastMonth] || 0);

      const firstBucket = customerMonthItemBuckets.get(`${firstMonth}||${x.cardCode}||${x.cardName}`) || new Map();
      const lastBucket = customerMonthItemBuckets.get(`${lastMonth}||${x.cardCode}||${x.cardName}`) || new Map();

      const topWarehouses = Array.from(x.topWarehousesMap.values())
        .map((w) => ({ warehouse: w.warehouse, dollars: money2(w.dollars) }))
        .sort((a, b) => Number(b.dollars || 0) - Number(a.dollars || 0))
        .slice(0, 5);

      return {
        cardCode: x.cardCode,
        cardName: x.cardName,
        customer: x.customer,
        totalSales: money2(x.totalSales),
        totalGrossProfit: money2(x.totalGP),
        totalGrossPct: x.totalSales !== 0 ? num((x.totalGP / x.totalSales) * 100, 2) : 0,
        monthsActive: Array.from(x.monthsActive.values()).sort((a, b) => a.localeCompare(b)),
        missingMonthsWithinRange: months.filter((m) => Math.abs(Number(x.salesByMonth[m] || 0)) <= 0.0001),
        salesByMonth,
        gpByMonth,
        firstMonth,
        lastMonth,
        salesFirstMonth: salesFirst,
        salesLastMonth: salesLast,
        deltaFirstVsLast: money2(salesLast - salesFirst),
        deltaPctFirstVsLast: aiPctDelta(salesFirst, salesLast),
        statusFirstVsLast: aiStatusFirstVsLast(salesFirst, salesLast),
        topLostItemsFirstVsLast: aiTopChangedItems(firstBucket, lastBucket, "lost", 10),
        topGainedItemsFirstVsLast: aiTopChangedItems(firstBucket, lastBucket, "gained", 10),
        topWarehouses,
      };
    })
    .sort((a, b) => {
      const absA = Math.abs(Number(a.deltaFirstVsLast || 0));
      const absB = Math.abs(Number(b.deltaFirstVsLast || 0));
      if (absB !== absA) return absB - absA;
      return Number(b.totalSales || 0) - Number(a.totalSales || 0);
    })
    .slice(0, 150);

  return {
    range: { from, to },
    filters: { area, grupo, q },
    months,
    firstMonth,
    lastMonth,
    rangeMonthSummary,
    customerMonthly,
    customerMonthTopItems,
    customerComparisons,
  };
}

function aiCompactDashboard(dashboard, analytics = {}, focus = {}) {
  const table = Array.isArray(dashboard?.table) ? dashboard.table.slice(0, 25) : [];
  const byMonth = Array.isArray(dashboard?.byMonth) ? dashboard.byMonth : [];
  const topWh = Array.isArray(dashboard?.topWarehouses) ? dashboard.topWarehouses.slice(0, 10) : [];
  const topCust = Array.isArray(dashboard?.topCustomers) ? dashboard.topCustomers.slice(0, 15) : [];

  const customerMonthly = Array.isArray(analytics?.customerMonthly) ? analytics.customerMonthly.slice(0, 220) : [];
  const customerMonthTopItems = Array.isArray(analytics?.customerMonthTopItems) ? analytics.customerMonthTopItems.slice(0, 120) : [];
  const customerComparisons = Array.isArray(analytics?.customerComparisons) ? analytics.customerComparisons.slice(0, 120) : [];
  const rangeMonthSummary = Array.isArray(analytics?.rangeMonthSummary) ? analytics.rangeMonthSummary : [];

  return {
    range: { from: dashboard?.from || "", to: dashboard?.to || "" },
    filters: {
      area: dashboard?.area || "__ALL__",
      grupo: dashboard?.grupo || "__ALL__",
      q: analytics?.filters?.q || "",
    },
    totals: dashboard?.totals || {},
    byMonth,
    topWarehouses: topWh,
    topCustomers: topCust,
    table,
    analytics: {
      months: Array.isArray(analytics?.months) ? analytics.months : [],
      firstMonth: analytics?.firstMonth || "",
      lastMonth: analytics?.lastMonth || "",
      rangeMonthSummary,
      customerMonthly,
      customerComparisons,
      customerMonthTopItems,
    },
    focus,
  };
}

function aiCompactDetail(detail, customerLabel = "") {
  const docs = Array.isArray(detail?.invoices) ? detail.invoices.slice(0, 15) : [];
  return {
    label: customerLabel,
    cardCode: detail?.cardCode || "",
    warehouse: detail?.warehouse || "",
    filters: { area: detail?.area || "__ALL__", grupo: detail?.grupo || "__ALL__" },
    totals: detail?.totals || {},
    documents: docs.map((d) => ({
      docType: d.docType,
      docTypeLabel: d.docTypeLabel,
      docNum: d.docNum,
      docDate: d.docDate,
      totals: d.totals,
      lines: Array.isArray(d.lines) ? d.lines.slice(0, 25) : [],
    })),
  };
}


function aiCompactRecommendationContext(reco) {
  if (!reco) return null;
  return {
    targetCustomer: reco.targetCustomer || null,
    modeHint: reco.modeHint || "",
    range: reco.range || null,
    lastMonth: reco.lastMonth || "",
    peerKeywords: Array.isArray(reco.peerKeywords) ? reco.peerKeywords.slice(0, 12) : [],
    peerCustomersCount: Number(reco.peerCustomersCount || 0),
    peerCustomers: Array.isArray(reco.peerCustomers) ? reco.peerCustomers.slice(0, 30) : [],
    targetTopItemsInRange: Array.isArray(reco.targetTopItemsInRange) ? reco.targetTopItemsInRange.slice(0, 20) : [],
    targetTopItemsLastMonth: Array.isArray(reco.targetTopItemsLastMonth) ? reco.targetTopItemsLastMonth.slice(0, 20) : [],
    recommendationsNotBoughtInRange: Array.isArray(reco.recommendationsNotBoughtInRange) ? reco.recommendationsNotBoughtInRange.slice(0, 30) : [],
    recommendationsNotBoughtInLastMonth: Array.isArray(reco.recommendationsNotBoughtInLastMonth) ? reco.recommendationsNotBoughtInLastMonth.slice(0, 30) : [],
    recommendationsBoughtBeforeButNotLastMonth: Array.isArray(reco.recommendationsBoughtBeforeButNotLastMonth) ? reco.recommendationsBoughtBeforeButNotLastMonth.slice(0, 30) : [],
  };
}

function adminClientesExtractTargetFromQuestion(rows, question, explicit = {}) {
  const list = Array.isArray(rows) ? rows : [];
  const customerMap = new Map();
  for (const r of list) {
    const code = String(r.cardCode || "").trim();
    const name = String(r.cardName || "").trim();
    if (!code) continue;
    if (!customerMap.has(code)) {
      customerMap.set(code, {
        cardCode: code,
        cardName: name,
        label: `${code} · ${name}`,
      });
    }
  }

  const explicitCode = String(explicit.cardCode || "").trim();
  if (explicitCode && customerMap.has(explicitCode)) return customerMap.get(explicitCode);

  const qn = norm(question || "");
  const re = /\b([A-Z]\d{3,6})\b/g;
  let m;
  while ((m = re.exec(String(question || ""))) !== null) {
    const code = String(m[1] || "").toUpperCase();
    if (customerMap.has(code)) return customerMap.get(code);
  }

  const explicitLabel = norm(explicit.customerLabel || "");
  if (explicitLabel) {
    for (const c of customerMap.values()) {
      const cn = norm(c.label);
      if (cn && (cn.includes(explicitLabel) || explicitLabel.includes(cn))) return c;
    }
  }

  let best = null;
  let bestScore = 0;
  for (const c of customerMap.values()) {
    const nameN = norm(c.cardName);
    const labelN = norm(c.label);
    let score = 0;
    if (nameN && qn.includes(nameN)) score += nameN.length + 50;
    if (labelN && qn.includes(labelN)) score += labelN.length + 80;
    const tokens = nameN.split(/\s+/).filter(t => t && t.length >= 4 && !['super','compania','compañia','sa','s','el','la','de','del'].includes(t));
    for (const t of tokens) {
      if (qn.includes(t)) score += Math.min(10, t.length);
    }
    if (score > bestScore) {
      bestScore = score;
      best = c;
    }
  }
  return bestScore >= 12 ? best : null;
}

function adminClientesPeerKeywords(target, question) {
  const combined = `${target?.cardName || ''} ${target?.label || ''} ${question || ''}`;
  const text = norm(combined);
  const ordered = [
    'goly', 'machetazo', 'xtra', 'jumbo', 'mega depot', 'mega', '99', 'super 99', 'el machetazo', 'casa de la carne'
  ];
  const hits = [];
  for (const k of ordered) {
    if (text.includes(norm(k)) && !hits.includes(norm(k))) hits.push(norm(k));
  }
  if (!hits.length) {
    const nameN = norm(target?.cardName || '');
    if (nameN.includes('machetazo')) hits.push('machetazo');
    if (nameN.includes('goly')) hits.push('goly');
    if (nameN.includes('xtra')) hits.push('xtra');
    if (nameN.includes('99')) hits.push('99');
  }
  return hits;
}

function adminClientesPeerMatch(cardName, keywords = []) {
  const nameN = norm(cardName || '');
  if (!nameN) return false;
  return (Array.isArray(keywords) ? keywords : []).some(k => nameN.includes(norm(k)));
}


function adminNormalizeCardCodeLoose(v) {
  const raw = String(v || '').trim().toUpperCase();
  if (!raw) return '';
  const m = raw.match(/^([A-Z]{0,3})0*([0-9]+)$/i);
  if (m) return `${m[1] || ''}${m[2] || ''}`;
  return raw.replace(/\s+/g, '');
}

function adminExtractCustomerCodesFromText(text) {
  const src = String(text || '').toUpperCase();
  const matches = src.match(/[A-Z]{0,3}\d{2,7}/g) || [];
  const out = [];
  const seen = new Set();
  for (const code of matches) {
    const k = adminNormalizeCardCodeLoose(code);
    if (!k || seen.has(k)) continue;
    seen.add(k);
    out.push(String(code || '').trim());
  }
  return out;
}

function adminFindDashboardRowsByCardCode(rows, code) {
  const wanted = String(code || '').trim();
  if (!wanted) return [];
  const wantedLoose = adminNormalizeCardCodeLoose(wanted);
  return (Array.isArray(rows) ? rows : []).filter((r) => {
    const cardCode = String(r?.cardCode || '').trim();
    return cardCode && (
      cardCode.toUpperCase() === wanted.toUpperCase() ||
      adminNormalizeCardCodeLoose(cardCode) === wantedLoose
    );
  });
}

function adminChooseBestRow(rows, warehouse = '') {
  const list = Array.isArray(rows) ? rows.slice() : [];
  if (!list.length) return null;
  const wh = String(warehouse || '').trim().toUpperCase();
  const sorted = list.sort((a, b) => Number(b?.dollars || 0) - Number(a?.dollars || 0));
  if (wh) {
    const exact = sorted.find((r) => String(r?.warehouse || '').trim().toUpperCase() === wh);
    if (exact) return exact;
  }
  return sorted[0] || null;
}

function adminResolveDashboardFocus({ dashboard = null, question = '', q = '', cardCode = '', warehouse = '', customerLabel = '' }) {
  const rows = Array.isArray(dashboard?.table) ? dashboard.table : [];
  if (!rows.length) return null;

  const buildFocus = (row, source = 'inferred') => row ? ({
    cardCode: String(row.cardCode || '').trim(),
    warehouse: String(row.warehouse || '').trim(),
    label: String(row.customer || `${row.cardCode || ''} · ${row.cardName || ''}`).trim(),
    customerLabel: String(row.customer || `${row.cardCode || ''} · ${row.cardName || ''}`).trim(),
    inferred: source !== 'explicit',
    source,
  }) : null;

  if (cardCode) {
    const chosen = adminChooseBestRow(adminFindDashboardRowsByCardCode(rows, cardCode), warehouse);
    if (chosen) return buildFocus(chosen, warehouse ? 'explicit' : 'explicit_code');
  }

  const combined = `${String(question || '').trim()} ${String(q || '').trim()} ${String(customerLabel || '').trim()}`.trim();
  for (const code of adminExtractCustomerCodesFromText(combined)) {
    const chosen = adminChooseBestRow(adminFindDashboardRowsByCardCode(rows, code), warehouse);
    if (chosen) return buildFocus(chosen, 'question_code');
  }

  const qn = norm(`${customerLabel || ''} ${q || ''}`);
  if (qn) {
    const directMatches = rows.filter((r) => {
      const cardN = norm(r?.cardCode || '');
      const nameN = norm(r?.cardName || '');
      const custN = norm(r?.customer || '');
      return cardN === qn || custN.includes(qn) || nameN.includes(qn) || qn.includes(cardN);
    });
    const uniqCodes = Array.from(new Set(directMatches.map((r) => String(r.cardCode || '').trim()).filter(Boolean)));
    if (uniqCodes.length === 1) {
      const chosen = adminChooseBestRow(directMatches, warehouse);
      if (chosen) return buildFocus(chosen, 'search');
    }

    let bestRow = null;
    let bestScore = 0;
    for (const r of rows) {
      const codeN = norm(r?.cardCode || '');
      const nameN = norm(r?.cardName || '');
      const custN = norm(r?.customer || '');
      let score = 0;
      if (codeN && qn.includes(codeN)) score += 100 + codeN.length;
      if (custN && qn.includes(custN)) score += 90 + custN.length;
      if (nameN && qn.includes(nameN)) score += 80 + nameN.length;
      const tokens = nameN.split(/\s+/).filter((t) => t && t.length >= 4 && !['super','compania','compañia','sa','s','el','la','de','del'].includes(t));
      for (const t of tokens) {
        if (qn.includes(t)) score += Math.min(12, t.length);
      }
      if (score > bestScore) {
        bestScore = score;
        bestRow = r;
      }
    }
    if (bestRow && bestScore >= 14) return buildFocus(bestRow, 'search_scored');
  }

  return null;
}

async function buildAdminClientesRecommendationAnalytics({ from, to, area = "__ALL__", grupo = "__ALL__", targetCardCode = "", customerLabel = "", question = "" }) {
  let rows = await fetchInvoiceRows({ from, to, q: "" });
  rows = applyAreaGroupFilters(rows, { area, grupo });
  if (!rows.length) return null;

  const target = adminClientesExtractTargetFromQuestion(rows, question, { cardCode: targetCardCode, customerLabel });
  if (!target?.cardCode) return null;

  const peerKeywords = adminClientesPeerKeywords(target, question);
  const peerCustomerMap = new Map();
  for (const r of rows) {
    const code = String(r.cardCode || '').trim();
    if (!code || code === target.cardCode) continue;
    if (!adminClientesPeerMatch(r.cardName, peerKeywords)) continue;
    if (!peerCustomerMap.has(code)) {
      peerCustomerMap.set(code, { cardCode: code, cardName: String(r.cardName || ''), label: `${code} · ${String(r.cardName || '')}` });
    }
  }
  const peerCodes = new Set(Array.from(peerCustomerMap.keys()));
  if (!peerCodes.size) return {
    targetCustomer: target,
    modeHint: 'no_peers',
    range: { from, to },
    lastMonth: String(to || '').slice(0, 7),
    peerKeywords,
    peerCustomersCount: 0,
    peerCustomers: [],
    targetTopItemsInRange: [],
    targetTopItemsLastMonth: [],
    recommendationsNotBoughtInRange: [],
    recommendationsNotBoughtInLastMonth: [],
    recommendationsBoughtBeforeButNotLastMonth: [],
  };

  const lastMonth = String(to || '').slice(0, 7);
  const targetItemsRange = new Map();
  const targetItemsLastMonth = new Map();
  const peerItems = new Map();

  for (const r of rows) {
    const code = String(r.cardCode || '').trim();
    const itemCode = String(r.itemCode || '').trim() || 'SIN_ITEM';
    const itemDesc = String(r.itemDesc || '').trim();
    const month = String(r.docDate || '').slice(0, 7);
    const itemKey = `${itemCode}||${itemDesc}`;

    if (code === target.cardCode) {
      const cur = targetItemsRange.get(itemKey) || {
        itemCode, itemDesc, dollars: 0, grossProfit: 0, qty: 0,
        firstDate: '', lastDate: '', months: new Set(), warehouses: new Set()
      };
      cur.dollars += Number(r.dollars || 0);
      cur.grossProfit += Number(r.grossProfit || 0);
      cur.qty += Number(r.quantity || 0);
      if (!cur.firstDate || String(r.docDate) < cur.firstDate) cur.firstDate = String(r.docDate || '');
      if (!cur.lastDate || String(r.docDate) > cur.lastDate) cur.lastDate = String(r.docDate || '');
      if (month) cur.months.add(month);
      if (r.warehouse) cur.warehouses.add(String(r.warehouse));
      targetItemsRange.set(itemKey, cur);

      if (month === lastMonth) {
        const curM = targetItemsLastMonth.get(itemKey) || {
          itemCode, itemDesc, dollars: 0, grossProfit: 0, qty: 0,
          firstDate: '', lastDate: '', months: new Set(), warehouses: new Set()
        };
        curM.dollars += Number(r.dollars || 0);
        curM.grossProfit += Number(r.grossProfit || 0);
        curM.qty += Number(r.quantity || 0);
        if (!curM.firstDate || String(r.docDate) < curM.firstDate) curM.firstDate = String(r.docDate || '');
        if (!curM.lastDate || String(r.docDate) > curM.lastDate) curM.lastDate = String(r.docDate || '');
        if (month) curM.months.add(month);
        if (r.warehouse) curM.warehouses.add(String(r.warehouse));
        targetItemsLastMonth.set(itemKey, curM);
      }
      continue;
    }

    if (!peerCodes.has(code)) continue;
    const cur = peerItems.get(itemKey) || {
      itemCode, itemDesc, grupo: r.grupo, area: r.area,
      dollars: 0, grossProfit: 0, qty: 0,
      firstDate: '', lastDate: '',
      months: new Set(), customers: new Set(), customerNames: new Set(), warehouses: new Set()
    };
    cur.dollars += Number(r.dollars || 0);
    cur.grossProfit += Number(r.grossProfit || 0);
    cur.qty += Number(r.quantity || 0);
    if (!cur.firstDate || String(r.docDate) < cur.firstDate) cur.firstDate = String(r.docDate || '');
    if (!cur.lastDate || String(r.docDate) > cur.lastDate) cur.lastDate = String(r.docDate || '');
    if (month) cur.months.add(month);
    cur.customers.add(code);
    if (r.cardName) cur.customerNames.add(String(r.cardName));
    if (r.warehouse) cur.warehouses.add(String(r.warehouse));
    peerItems.set(itemKey, cur);
  }

  const toArray = (mp, enrichMode='target') => Array.from(mp.values()).map(x => {
    const dollars = money2(x.dollars);
    const gp = money2(x.grossProfit);
    return {
      itemCode: x.itemCode,
      itemDesc: x.itemDesc,
      grupo: x.grupo || '',
      area: x.area || '',
      dollars,
      grossProfit: gp,
      grossPct: dollars !== 0 ? num((gp / dollars) * 100, 2) : 0,
      qty: num(x.qty, 4),
      firstDate: x.firstDate || '',
      lastDate: x.lastDate || '',
      months: Array.from(x.months || []).sort((a,b)=>String(a).localeCompare(String(b))),
      warehouses: Array.from(x.warehouses || []).sort((a,b)=>String(a).localeCompare(String(b))),
      customersCount: enrichMode === 'peer' ? (x.customers ? x.customers.size : 0) : undefined,
      customerNames: enrichMode === 'peer' ? Array.from(x.customerNames || []).sort((a,b)=>String(a).localeCompare(String(b))).slice(0, 12) : undefined,
    };
  });

  const targetTopItemsInRange = toArray(targetItemsRange, 'target')
    .sort((a,b)=> Number(b.dollars||0)-Number(a.dollars||0))
    .slice(0,20);
  const targetTopItemsLastMonth = toArray(targetItemsLastMonth, 'target')
    .sort((a,b)=> Number(b.dollars||0)-Number(a.dollars||0))
    .slice(0,20);

  const recommendationBase = toArray(peerItems, 'peer').map(x => ({
    ...x,
    targetBoughtInRange: targetItemsRange.has(`${x.itemCode}||${x.itemDesc}`),
    targetBoughtInLastMonth: targetItemsLastMonth.has(`${x.itemCode}||${x.itemDesc}`),
    targetRangeDollars: targetItemsRange.has(`${x.itemCode}||${x.itemDesc}`) ? Number(targetItemsRange.get(`${x.itemCode}||${x.itemDesc}`).dollars || 0) : 0,
    targetLastMonthDollars: targetItemsLastMonth.has(`${x.itemCode}||${x.itemDesc}`) ? Number(targetItemsLastMonth.get(`${x.itemCode}||${x.itemDesc}`).dollars || 0) : 0,
  }));

  const sorter = (a,b) => {
    if (Number(b.dollars||0) !== Number(a.dollars||0)) return Number(b.dollars||0) - Number(a.dollars||0);
    if (Number(b.customersCount||0) !== Number(a.customersCount||0)) return Number(b.customersCount||0) - Number(a.customersCount||0);
    return Number(b.grossProfit||0) - Number(a.grossProfit||0);
  };

  const recommendationsNotBoughtInRange = recommendationBase.filter(x => !x.targetBoughtInRange).sort(sorter).slice(0, 30);
  const recommendationsNotBoughtInLastMonth = recommendationBase.filter(x => !x.targetBoughtInLastMonth).sort(sorter).slice(0, 30);
  const recommendationsBoughtBeforeButNotLastMonth = recommendationBase.filter(x => x.targetBoughtInRange && !x.targetBoughtInLastMonth).sort(sorter).slice(0, 30);

  let modeHint = 'cross_sell';
  const qn = norm(question || '');
  if (qn.includes('marzo') || qn.includes('mes de') || qn.includes('ultimo mes') || qn.includes('último mes')) modeHint = 'not_bought_last_month';
  if (qn.includes('nunca') || qn.includes('jamas') || qn.includes('jamás') || qn.includes('no ha comprado')) modeHint = modeHint === 'not_bought_last_month' ? 'not_bought_last_month' : 'not_bought_in_range';

  return {
    targetCustomer: target,
    modeHint,
    range: { from, to },
    lastMonth,
    peerKeywords,
    peerCustomersCount: peerCodes.size,
    peerCustomers: Array.from(peerCustomerMap.values()).slice(0, 40),
    targetTopItemsInRange,
    targetTopItemsLastMonth,
    recommendationsNotBoughtInRange,
    recommendationsNotBoughtInLastMonth,
    recommendationsBoughtBeforeButNotLastMonth,
  };
}

function extractResponseText(obj) {
  if (!obj) return "";
  if (typeof obj.output_text === "string" && obj.output_text.trim()) return obj.output_text.trim();

  const parts = [];
  for (const item of (obj.output || [])) {
    if (item?.type && item.type !== "message") continue;
    for (const c of (item.content || [])) {
      if ((c?.type === "output_text" || c?.type === "text") && c?.text) {
        parts.push(String(c.text));
      }
      if (c?.type === "refusal" && c?.refusal) {
        parts.push(`El modelo rechazó responder: ${String(c.refusal)}`);
      }
    }
  }
  return parts.join("\n").trim();
}

async function openaiDbAnalystChat({ question, dashboard, analytics = null, detail = null, customerLabel = "", recommendationContext = null }) {
  const apiKey = String(process.env.OPENAI_API_KEY || "").trim();
  const model = String(process.env.OPENAI_MODEL || "gpt-5-mini").trim();
  if (!apiKey) throw new Error("OPENAI_API_KEY no configurada");

  const compact = {
    dashboard: aiCompactDashboard(
      dashboard,
      analytics,
      {
        cardCode: detail?.cardCode || "",
        warehouse: detail?.warehouse || "",
      }
    ),
    detail: detail ? aiCompactDetail(detail, customerLabel) : null,
    recommendationContext: aiCompactRecommendationContext(recommendationContext),
  };

  const system = [
    "Eres un analista comercial interno senior de PRODIMA, experto en ventas, clientes, portafolio, variaciones mensuales, rentabilidad y hallazgos accionables.",
    "Usa exclusivamente la información entregada por el sistema como fuente de verdad. No menciones formatos internos ni digas que estás usando JSON.",
    "La fuente es la base de datos sincronizada del sistema, no SAP en vivo.",
    "Las ventas del módulo están neteadas: facturas menos notas de crédito.",
    "Respeta estrictamente los filtros activos de fecha, área, grupo, búsqueda, cliente, bodega y cualquier otro filtro entregado en el contexto.",
    "Responde siempre en español, con tono profesional, claro, ejecutivo y útil.",
    "Prioriza cifras concretas, comparaciones, tendencias, hallazgos accionables y posibles causas basadas en datos.",
    "Cuando existan datos por cliente y mes, identifica clientes que cayeron, crecieron, dejaron de comprar, compraron en un mes y en otro no, clientes nuevos y clientes intermitentes.",
    "Si existe recommendationContext, úsalo como la fuente prioritaria para responder recomendaciones de surtido, productos no comprados por un cliente objetivo, productos comprados en otras tiendas de la cadena y TOP N de artículos sugeridos.",
    "Cuando el usuario pida tabla o Excel, devuelve primero una tabla en markdown con datos reales del sistema. No uses aproximaciones, rangos, ni texto como ~ o aprox si el contexto ya trae cifras exactas.",
    "Para recomendaciones entre tiendas, diferencia claramente entre: no comprado en todo el rango, no comprado en el último mes del rango, y comprado antes pero no en el último mes. Usa la variante que mejor coincida con la pregunta del usuario.",
    "En tablas de recomendación usa columnas claras como Cliente objetivo, Código, Artículo, Ventas otras tiendas USD, Unidades otras tiendas, Tiendas de la cadena, Primera compra, Última compra, Comprado por cliente objetivo en rango, Comprado por cliente objetivo en último mes.",
    "Cuando existan datos por cliente, artículo y mes, explica qué artículos provocaron la caída, crecimiento o ausencia de compra de cada cliente.",
    "Si el usuario pregunta qué cliente cayó, responde con ranking de mayores caídas, monto, porcentaje, meses comparados y artículos asociados cuando existan.",
    "Si el usuario pregunta qué cliente compró en un mes sí y en otro no, usa el detalle cliente-mes y responde con una lista directa de clientes ausentes o intermitentes entre esos meses.",
    "Si el usuario pregunta qué artículos fueron, menciona códigos, descripciones, montos, cantidades y variaciones cuando existan.",
    "Si la información permite comparar el primer y el último mes del rango, usa también esa comparación para detectar clientes perdidos, recuperados, artículos perdidos y artículos ganados.",
    "Cuando compares meses, indica cliente, mes inicial, mes final, venta inicial, venta final, variación en dólares, variación porcentual y los artículos relacionados cuando existan.",
    "No des respuestas genéricas. Siempre que sea posible menciona nombres de clientes, códigos, artículos, montos, porcentajes, bodegas y meses concretos.",
    "Si no existe base suficiente para afirmar una causa, preséntala como hipótesis inferida de los datos observados.",
    "Si el usuario hace una pregunta puntual, responde directo primero. Luego agrega hallazgos adicionales útiles.",
    "Si el usuario pide un análisis amplio, usa esta estructura: resumen ejecutivo, clientes que cayeron, clientes que crecieron, clientes que dejaron de comprar, artículos relacionados, posibles causas y acciones sugeridas.",
    "Evita repetir cifras innecesariamente. Ordena de mayor impacto a menor impacto.",
    "Tu objetivo es ayudar a gerencia comercial y ventas a entender qué pasó, quién cayó, quién dejó de comprar, qué artículos explican el cambio y qué acciones conviene tomar."
  ].join(" ");

  const reasoning = model.startsWith("gpt-5.1") || model.startsWith("gpt-5.2")
    ? { effort: "none" }
    : model.startsWith("gpt-5")
      ? { effort: "minimal" }
      : undefined;

  const payload = {
    model,
    input: [
      { role: "system", content: [{ type: "input_text", text: system }] },
      {
        role: "user",
        content: [{
          type: "input_text",
          text:
            `Pregunta del usuario:
${String(question || "").trim()}

` +
            `Contexto del sistema:
${JSON.stringify(compact)}`
        }]
      }
    ],
    text: { format: { type: "text" } },
    max_output_tokens: 2200,
    ...(reasoning ? { reasoning } : {}),
  };

  const resp = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify(payload),
  });

  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    throw new Error(data?.error?.message || data?.message || `OpenAI HTTP ${resp.status}`);
  }

  const answer = extractResponseText(data);
  if (!answer) {
    console.error("OpenAI empty output", {
      model,
      status: data?.status || null,
      incomplete_details: data?.incomplete_details || null,
      output_types: Array.isArray(data?.output) ? data.output.map((x) => x?.type) : [],
    });
    throw new Error("OpenAI devolvió salida vacía");
  }

  return { answer, model };
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
   ✅ Routes
========================================================= */
app.get("/api/admin/invoices/dashboard", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = isISO(req.query?.from) ? String(req.query.from) : addDaysISO(today, -30);
    const to = isISO(req.query?.to) ? String(req.query.to) : today;
    const area = String(req.query?.area || "__ALL__");
    const grupo = String(req.query?.grupo || "__ALL__");
    const q = String(req.query?.q || "");

    const data = await dashboardFromDbAdminClientes({ from, to, area, grupo, q });
    data.lastSyncAt = await getState("last_sync_at");
    return safeJson(res, 200, data);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.get("/api/admin/invoices/details", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = isISO(req.query?.from) ? String(req.query.from) : addDaysISO(today, -30);
    const to = isISO(req.query?.to) ? String(req.query.to) : today;
    const cardCode = String(req.query?.cardCode || "").trim();
    const warehouse = String(req.query?.warehouse || "").trim();
    const area = String(req.query?.area || "__ALL__");
    const grupo = String(req.query?.grupo || "__ALL__");

    if (!cardCode || !warehouse) {
      return safeJson(res, 400, { ok: false, message: "cardCode y warehouse requeridos" });
    }

    const data = await detailsFromDb({ from, to, cardCode, warehouse, area, grupo });
    return safeJson(res, 200, data);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.get("/api/admin/invoices/top-products", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = isISO(req.query?.from) ? String(req.query.from) : addDaysISO(today, -30);
    const to = isISO(req.query?.to) ? String(req.query.to) : today;
    const warehouse = String(req.query?.warehouse || "").trim();
    const cardCode = String(req.query?.cardCode || "").trim();
    const area = String(req.query?.area || "__ALL__");
    const grupo = String(req.query?.grupo || "__ALL__");
    const limit = Math.max(1, Math.min(200, Number(req.query?.limit || 10)));

    const data = await topProductsFromDb({ from, to, warehouse, cardCode, area, grupo, limit });
    return safeJson(res, 200, data);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.get("/api/admin/invoices/export", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = isISO(req.query?.from) ? String(req.query.from) : addDaysISO(today, -30);
    const to = isISO(req.query?.to) ? String(req.query.to) : today;
    const area = String(req.query?.area || "__ALL__");
    const grupo = String(req.query?.grupo || "__ALL__");
    const q = String(req.query?.q || "");

    const data = await dashboardFromDbAdminClientes({ from, to, area, grupo, q });

    const wb = XLSX.utils.book_new();
    const rows = (data.table || []).map((r) => ({
      "Código cliente": r.cardCode,
      "Cliente": r.cardName,
      "Cliente label": r.customer,
      "Bodega": r.warehouse,
      "Ventas netas $": r.dollars,
      "Ganancia bruta $": r.grossProfit,
      "% GP": r.grossPct,
      "Facturas": r.invoices,
      "Notas de crédito": r.creditNotes,
    }));
    const ws = XLSX.utils.json_to_sheet(rows);
    XLSX.utils.book_append_sheet(wb, ws, "Resumen");

    const buf = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });
    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.setHeader("Content-Disposition", `attachment; filename="admin-clientes_${from}_a_${to}.xlsx"`);
    return res.end(buf);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.get("/api/admin/invoices/details/export", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = isISO(req.query?.from) ? String(req.query.from) : addDaysISO(today, -30);
    const to = isISO(req.query?.to) ? String(req.query.to) : today;
    const cardCode = String(req.query?.cardCode || "").trim();
    const warehouse = String(req.query?.warehouse || "").trim();
    const area = String(req.query?.area || "__ALL__");
    const grupo = String(req.query?.grupo || "__ALL__");

    if (!cardCode || !warehouse) {
      return safeJson(res, 400, { ok: false, message: "cardCode y warehouse requeridos" });
    }

    const data = await detailsFromDb({ from, to, cardCode, warehouse, area, grupo });
    const wb = XLSX.utils.book_new();

    const rows = [];
    for (const doc of (data.invoices || [])) {
      for (const ln of (doc.lines || [])) {
        rows.push({
          "Tipo": doc.docTypeLabel,
          "DocNum": doc.docNum,
          "Fecha": doc.docDate,
          "Código cliente": data.cardCode,
          "Bodega": data.warehouse,
          "Área": ln.area,
          "Grupo": ln.grupo,
          "ItemCode": ln.itemCode,
          "Descripción": ln.itemDesc,
          "Cantidad": ln.quantity,
          "Ventas netas $": ln.dollars,
          "Ganancia bruta $": ln.grossProfit,
          "% GP": ln.grossPct,
        });
      }
    }

    const ws = XLSX.utils.json_to_sheet(rows);
    XLSX.utils.book_append_sheet(wb, ws, "Detalle");
    const buf = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });

    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.setHeader("Content-Disposition", `attachment; filename="detalle_${cardCode}_${warehouse}_${from}_a_${to}.xlsx"`);
    return res.end(buf);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});


app.post("/api/admin/invoices/ai-chat", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const question = String(req.body?.question || "").trim();
    if (!question) return safeJson(res, 400, { ok: false, message: "question requerida" });

    const fromQ = String(req.body?.from || req.query?.from || "");
    const toQ = String(req.body?.to || req.query?.to || "");
    const cardCode = String(req.body?.cardCode || "").trim();
    const warehouse = String(req.body?.warehouse || "").trim();
    const customerLabel = String(req.body?.customerLabel || "").trim();
    const area = String(req.body?.area || req.query?.area || "__ALL__");
    const grupo = String(req.body?.grupo || req.query?.grupo || "__ALL__");
    const q = String(req.body?.q || req.query?.q || "").trim();

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const defaultFrom = addDaysISO(today, -30);
    const from = isISO(fromQ) ? fromQ : defaultFrom;
    const to = isISO(toQ) ? toQ : today;

    const dashboard = await dashboardFromDbAdminClientes({ from, to, area, grupo, q });
    const analytics = await buildAdminClientesAiAnalytics({ from, to, area, grupo, q });

    const resolvedFocus = adminResolveDashboardFocus({
      dashboard,
      question,
      q,
      cardCode,
      warehouse,
      customerLabel,
    });

    const focusCardCode = String(resolvedFocus?.cardCode || cardCode || "").trim();
    const focusWarehouse = String(resolvedFocus?.warehouse || warehouse || "").trim();
    const focusLabel = String(resolvedFocus?.label || customerLabel || "").trim();

    let detail = null;
    if (focusCardCode && focusWarehouse) {
      detail = await detailsFromDb({ from, to, cardCode: focusCardCode, warehouse: focusWarehouse, area, grupo });
    }

    const recommendationContext = await buildAdminClientesRecommendationAnalytics({
      from,
      to,
      area,
      grupo,
      targetCardCode: focusCardCode,
      customerLabel: focusLabel,
      question,
    });

    const out = await openaiDbAnalystChat({
      question,
      dashboard,
      analytics,
      detail,
      customerLabel: focusLabel,
      recommendationContext,
    });

    return safeJson(res, 200, {
      ok: true,
      answer: out.answer,
      model: out.model,
      source: "db",
      range: { from, to },
      filters: { area, grupo, q },
      focus: focusCardCode ? {
        cardCode: focusCardCode,
        warehouse: focusWarehouse,
        customerLabel: focusLabel || `${focusCardCode}`,
        label: focusLabel || `${focusCardCode}`,
        inferred: !(cardCode && warehouse),
        source: resolvedFocus?.source || (cardCode && warehouse ? 'explicit' : 'none'),
      } : null,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});


app.post("/api/admin/invoices/sync", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });
    if (missingSapEnv()) return safeJson(res, 500, { ok: false, message: "SAP env incompleto" });

    const fromQ = String(req.query?.from || req.body?.from || "");
    const toQ = String(req.query?.to || req.body?.to || "");
    const maxDocs = Math.max(500, Math.min(50000, Number(req.query?.maxDocs || req.body?.maxDocs || 12000)));

    if (!isISO(fromQ) || !isISO(toQ)) {
      return safeJson(res, 400, { ok: false, message: "from y to deben ser YYYY-MM-DD" });
    }

    const out = await syncRangeToDb({ from: fromQ, to: toQ, maxDocs });
    return safeJson(res, 200, { ok: true, ...out, from: fromQ, to: toQ, maxDocs });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.post("/api/admin/invoices/sync/recent", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });
    if (missingSapEnv()) return safeJson(res, 500, { ok: false, message: "SAP env incompleto" });

    const days = Math.max(1, Math.min(90, Number(req.query?.days || req.body?.days || 10)));
    const maxDocs = Math.max(500, Math.min(50000, Number(req.query?.maxDocs || req.body?.maxDocs || 12000)));

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = addDaysISO(today, -(days - 1));

    const out = await syncRangeToDb({ from, to: today, maxDocs });
    return safeJson(res, 200, { ok: true, ...out, from, to: today, days, maxDocs });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});


/* =========================================================
   Portal central compartido · permisos por módulo/área
   Usa el mismo username + PIN de la intranet (portal_users)
========================================================= */
const PORTAL_ALL_AREAS = ["CONS", "RCI"];
const PORTAL_MODULE_KEYS = new Set(["admin-clientes", "estratificacion", "produccion"]);

function normalizePortalModuleKey(v) {
  const s = String(v || "").trim().toLowerCase();
  return s;
}

function normalizePortalAreaCode(v) {
  const s = String(v || "").trim().toUpperCase();
  if (!s || s === "__ALL__" || s === "ALL" || s === "*") return "__ALL__";
  if (s === "CONS" || s === "CONSUMIDOR") return "CONS";
  if (s === "RCI") return "RCI";
  return "";
}

function normalizePortalAreaList(arr) {
  const out = [];
  const seen = new Set();
  for (const raw of Array.isArray(arr) ? arr : []) {
    const code = normalizePortalAreaCode(raw);
    if (!code || code === "__ALL__") continue;
    if (!PORTAL_ALL_AREAS.includes(code)) continue;
    if (seen.has(code)) continue;
    seen.add(code);
    out.push(code);
  }
  return out;
}

async function ensurePortalModuleScopeDb() {
  if (!hasDb()) return;

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS portal_user_module_areas (
      id BIGSERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL REFERENCES portal_users(id) ON DELETE CASCADE,
      module_key TEXT NOT NULL,
      area_code TEXT NOT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT NOW(),
      UNIQUE(user_id, module_key, area_code)
    );
  `);

  await dbQuery(`
    CREATE INDEX IF NOT EXISTS idx_portal_user_module_areas_user_module
    ON portal_user_module_areas(user_id, module_key);
  `);

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS app_audit_log (
      id BIGSERIAL PRIMARY KEY,
      user_id INTEGER,
      module_key TEXT NOT NULL,
      action_type TEXT NOT NULL,
      entity_type TEXT NOT NULL DEFAULT '',
      entity_code TEXT NOT NULL DEFAULT '',
      entity_name TEXT NOT NULL DEFAULT '',
      area_code TEXT NOT NULL DEFAULT '',
      request_id TEXT NOT NULL DEFAULT '',
      status TEXT NOT NULL DEFAULT 'ok',
      message TEXT NOT NULL DEFAULT '',
      filters_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
  `);

  await dbQuery(`
    CREATE INDEX IF NOT EXISTS idx_app_audit_log_user_module_created
    ON app_audit_log(user_id, module_key, created_at DESC);
  `);

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS app_user_memory (
      user_id INTEGER NOT NULL REFERENCES portal_users(id) ON DELETE CASCADE,
      module_key TEXT NOT NULL,
      memory_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
      PRIMARY KEY (user_id, module_key)
    );
  `);
}

async function portalLoadUserById(userId) {
  if (!hasDb()) return null;
  const id = Number(userId || 0);
  if (!Number.isFinite(id) || id <= 0) return null;

  const r = await dbQuery(
    `SELECT id, username, full_name, role, is_active, permissions_json
     FROM portal_users
     WHERE id=$1
     LIMIT 1`,
    [id]
  );
  return r.rows?.[0] || null;
}

function portalPermissionsFromRow(row) {
  return Array.isArray(row?.permissions_json)
    ? row.permissions_json.map((x) => String(x || "").trim()).filter(Boolean)
    : [];
}

function portalUserIsAdminByRow(row) {
  return String(row?.role || "").trim().toLowerCase() === "admin";
}

function portalUserHasModulePermission(row, moduleKey) {
  if (portalUserIsAdminByRow(row)) return true;
  const perms = portalPermissionsFromRow(row);
  return perms.includes("*") || perms.includes(moduleKey);
}

async function portalGetModuleAreasForUser(userId, moduleKey) {
  if (!hasDb()) return PORTAL_ALL_AREAS.slice();
  const id = Number(userId || 0);
  if (!Number.isFinite(id) || id <= 0) return PORTAL_ALL_AREAS.slice();

  const mk = normalizePortalModuleKey(moduleKey);
  const r = await dbQuery(
    `SELECT area_code
     FROM portal_user_module_areas
     WHERE user_id=$1 AND module_key=$2
     ORDER BY area_code`,
    [id, mk]
  );

  const areas = normalizePortalAreaList((r.rows || []).map((x) => x.area_code));
  return areas.length ? areas : PORTAL_ALL_AREAS.slice();
}

function portalResolveEffectiveArea(access, requestedArea = "__ALL__") {
  const allowed = normalizePortalAreaList(access?.areas || []);
  const req = normalizePortalAreaCode(requestedArea);
  if (!allowed.length || allowed.length >= 2) {
    return req || "__ALL__";
  }
  return allowed[0];
}

function portalAvailableAreasForAccess(access) {
  const allowed = normalizePortalAreaList(access?.areas || []);
  if (!allowed.length || allowed.length >= 2) return ["__ALL__", ...PORTAL_ALL_AREAS];
  return allowed.slice();
}

async function portalGetModuleAccessContext(authUser, moduleKey) {
  const mk = normalizePortalModuleKey(moduleKey);
  if (!PORTAL_MODULE_KEYS.has(mk)) {
    return {
      ok: false,
      canAccess: false,
      isAdmin: false,
      moduleKey: mk,
      areas: [],
      availableAreas: [],
      permissions: [],
      user: authUser || null,
    };
  }

  if (Number(authUser?.id || 0) === 0 && String(authUser?.role || "").toLowerCase() === "admin") {
    return {
      ok: true,
      canAccess: true,
      isAdmin: true,
      moduleKey: mk,
      areas: PORTAL_ALL_AREAS.slice(),
      availableAreas: ["__ALL__", ...PORTAL_ALL_AREAS],
      permissions: ["*"],
      user: authUser,
    };
  }

  const row = await portalLoadUserById(authUser?.id);
  if (!row || !row.is_active) {
    return {
      ok: false,
      canAccess: false,
      isAdmin: false,
      moduleKey: mk,
      areas: [],
      availableAreas: [],
      permissions: [],
      user: authUser || null,
    };
  }

  const permissions = portalPermissionsFromRow(row);
  const isAdmin = portalUserIsAdminByRow(row) || permissions.includes("*");
  const canAccess = isAdmin || permissions.includes(mk);
  const areas = canAccess ? await portalGetModuleAreasForUser(row.id, mk) : [];
  return {
    ok: true,
    canAccess,
    isAdmin,
    moduleKey: mk,
    areas,
    availableAreas: canAccess ? portalAvailableAreasForAccess({ areas }) : [],
    permissions,
    user: {
      id: row.id,
      username: row.username,
      full_name: row.full_name || "",
      role: row.role || "user",
    },
  };
}

function verifyPortalModule(moduleKey, opts = {}) {
  return async (req, res, next) => {
    try {
      const access = await portalGetModuleAccessContext(req.user, moduleKey);
      if (!access?.canAccess) {
        return safeJson(res, 403, { ok: false, message: `Sin acceso al módulo ${moduleKey}` });
      }
      if (opts.adminOnly && !access.isAdmin) {
        return safeJson(res, 403, { ok: false, message: "Permiso de administrador requerido" });
      }
      req.moduleAccess = access;
      return next();
    } catch (e) {
      return safeJson(res, 500, { ok: false, message: e.message || String(e) });
    }
  };
}

async function writePortalAudit({
  userId = null,
  moduleKey = "",
  actionType = "",
  entityType = "",
  entityCode = "",
  entityName = "",
  areaCode = "",
  requestId = "",
  status = "ok",
  message = "",
  filters = {},
  payload = {},
} = {}) {
  try {
    if (!hasDb()) return;
    await dbQuery(
      `INSERT INTO app_audit_log(
        user_id, module_key, action_type, entity_type, entity_code, entity_name,
        area_code, request_id, status, message, filters_json, payload_json
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12::jsonb)`,
      [
        Number.isFinite(Number(userId)) ? Number(userId) : null,
        String(moduleKey || ""),
        String(actionType || ""),
        String(entityType || ""),
        String(entityCode || ""),
        String(entityName || ""),
        String(areaCode || ""),
        String(requestId || ""),
        String(status || "ok"),
        truncate(message || "", 240),
        JSON.stringify(filters || {}),
        JSON.stringify(payload || {}),
      ]
    );
  } catch {}
}

async function savePortalUserMemory(userId, moduleKey, memory = {}) {
  try {
    if (!hasDb()) return;
    const id = Number(userId || 0);
    if (!Number.isFinite(id) || id <= 0) return;

    await dbQuery(
      `INSERT INTO app_user_memory(user_id, module_key, memory_json, updated_at)
       VALUES ($1,$2,$3::jsonb,NOW())
       ON CONFLICT (user_id, module_key)
       DO UPDATE SET memory_json = EXCLUDED.memory_json, updated_at = NOW()`,
      [id, String(moduleKey || ""), JSON.stringify(memory || {})]
    );
  } catch {}
}

async function loadPortalUserMemory(userId, moduleKey) {
  try {
    if (!hasDb()) return {};
    const id = Number(userId || 0);
    if (!Number.isFinite(id) || id <= 0) return {};
    const r = await dbQuery(
      `SELECT memory_json
       FROM app_user_memory
       WHERE user_id=$1 AND module_key=$2
       LIMIT 1`,
      [id, String(moduleKey || "")]
    );
    const raw = r.rows?.[0]?.memory_json;
    return raw && typeof raw === "object" ? raw : {};
  } catch {
    return {};
  }
}

app.get("/api/portal/modules/:moduleKey/access", verifyPortalAuth, async (req, res) => {
  try {
    const moduleKey = normalizePortalModuleKey(req.params.moduleKey);
    if (!PORTAL_MODULE_KEYS.has(moduleKey)) {
      return safeJson(res, 400, { ok: false, message: "moduleKey inválido" });
    }
    const access = await portalGetModuleAccessContext(req.user, moduleKey);
    const memory = access?.canAccess ? await loadPortalUserMemory(req.user?.id, moduleKey) : {};
    return safeJson(res, 200, {
      ok: true,
      moduleKey,
      canAccess: !!access?.canAccess,
      isAdmin: !!access?.isAdmin,
      areas: access?.areas || [],
      availableAreas: access?.availableAreas || [],
      permissions: access?.permissions || [],
      memory,
      user: access?.user || req.user || null,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get("/api/portal/admin/modules/:moduleKey/users", verifyPortalAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });
    const moduleKey = normalizePortalModuleKey(req.params.moduleKey);
    if (!PORTAL_MODULE_KEYS.has(moduleKey)) {
      return safeJson(res, 400, { ok: false, message: "moduleKey inválido" });
    }

    const usersR = await dbQuery(
      `SELECT id, username, full_name, role, is_active, permissions_json, created_at
       FROM portal_users
       ORDER BY id DESC`
    );

    const areaR = await dbQuery(
      `SELECT user_id, module_key, area_code
       FROM portal_user_module_areas
       WHERE module_key=$1`,
      [moduleKey]
    );

    const areaMap = new Map();
    for (const row of areaR.rows || []) {
      const key = `${row.user_id}::${row.module_key}`;
      const cur = areaMap.get(key) || [];
      cur.push(row.area_code);
      areaMap.set(key, cur);
    }

    const users = (usersR.rows || []).map((u) => {
      const perms = Array.isArray(u.permissions_json) ? u.permissions_json : [];
      const isAdmin = String(u.role || "").toLowerCase() === "admin" || perms.includes("*");
      const moduleEnabled = isAdmin || perms.includes(moduleKey);
      const areas = moduleEnabled
        ? normalizePortalAreaList(areaMap.get(`${u.id}::${moduleKey}`) || []).concat()
        : [];
      return {
        id: u.id,
        username: u.username,
        full_name: u.full_name || "",
        role: u.role || "user",
        is_active: !!u.is_active,
        created_at: u.created_at,
        module_enabled: moduleEnabled,
        areas: moduleEnabled ? (areas.length ? areas : PORTAL_ALL_AREAS.slice()) : [],
      };
    });

    return safeJson(res, 200, { ok: true, moduleKey, users });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.patch("/api/portal/admin/modules/:moduleKey/users/:id/access", verifyPortalAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const moduleKey = normalizePortalModuleKey(req.params.moduleKey);
    const userId = Number(req.params.id || 0);
    if (!PORTAL_MODULE_KEYS.has(moduleKey)) {
      return safeJson(res, 400, { ok: false, message: "moduleKey inválido" });
    }
    if (!Number.isFinite(userId) || userId <= 0) {
      return safeJson(res, 400, { ok: false, message: "ID inválido" });
    }

    const moduleEnabled = !!req.body?.module_enabled;
    const desiredAreas = normalizePortalAreaList(req.body?.areas || []);

    const u = await portalLoadUserById(userId);
    if (!u) return safeJson(res, 404, { ok: false, message: "Usuario no encontrado" });

    const isTargetAdmin = portalUserIsAdminByRow(u) || portalPermissionsFromRow(u).includes("*");
    let perms = portalPermissionsFromRow(u);

    if (!isTargetAdmin) {
      if (moduleEnabled && !perms.includes(moduleKey)) perms.push(moduleKey);
      if (!moduleEnabled) perms = perms.filter((x) => x !== moduleKey);
      await dbQuery(
        `UPDATE portal_users
         SET permissions_json=$2::jsonb
         WHERE id=$1`,
        [userId, JSON.stringify(perms)]
      );
    }

    await dbQuery(
      `DELETE FROM portal_user_module_areas
       WHERE user_id=$1 AND module_key=$2`,
      [userId, moduleKey]
    );

    if (moduleEnabled) {
      const areasToStore = desiredAreas.length ? desiredAreas : PORTAL_ALL_AREAS.slice();
      for (const areaCode of areasToStore) {
        await dbQuery(
          `INSERT INTO portal_user_module_areas(user_id, module_key, area_code)
           VALUES ($1,$2,$3)
           ON CONFLICT (user_id, module_key, area_code) DO NOTHING`,
          [userId, moduleKey, areaCode]
        );
      }
    }

    const access = await portalGetModuleAccessContext({ id: userId }, moduleKey);
    return safeJson(res, 200, {
      ok: true,
      moduleKey,
      user: {
        id: userId,
        username: u.username,
        full_name: u.full_name || "",
        role: u.role || "user",
        module_enabled: !!access?.canAccess,
        areas: access?.areas || [],
      },
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get("/api/portal/admin-clientes/dashboard", verifyPortalAuth, verifyPortalModule("admin-clientes"), async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = isISO(req.query?.from) ? String(req.query.from) : addDaysISO(today, -30);
    const to = isISO(req.query?.to) ? String(req.query.to) : today;
    const area = portalResolveEffectiveArea(req.moduleAccess, req.query?.area || "__ALL__");
    const grupo = String(req.query?.grupo || "__ALL__");
    const q = String(req.query?.q || "");

    const data = await dashboardFromDbAdminClientes({ from, to, area, grupo, q });
    data.lastSyncAt = await getState("last_sync_at");
    data.access = {
      moduleKey: "admin-clientes",
      isAdmin: !!req.moduleAccess?.isAdmin,
      areas: req.moduleAccess?.areas || [],
      availableAreas: portalAvailableAreasForAccess(req.moduleAccess),
      effectiveArea: area,
    };

    await savePortalUserMemory(req.user?.id, "admin-clientes", {
      lastRange: { from, to },
      lastFilters: { area, grupo, q },
      updatedAt: new Date().toISOString(),
    });
    await writePortalAudit({
      userId: req.user?.id,
      moduleKey: "admin-clientes",
      actionType: "dashboard_load",
      areaCode: area === "__ALL__" ? "" : area,
      filters: { from, to, area, grupo, q },
    });

    return safeJson(res, 200, data);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get("/api/portal/admin-clientes/details", verifyPortalAuth, verifyPortalModule("admin-clientes"), async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = isISO(req.query?.from) ? String(req.query.from) : addDaysISO(today, -30);
    const to = isISO(req.query?.to) ? String(req.query.to) : today;
    const cardCode = String(req.query?.cardCode || "").trim();
    const warehouse = String(req.query?.warehouse || "").trim();
    const area = portalResolveEffectiveArea(req.moduleAccess, req.query?.area || "__ALL__");
    const grupo = String(req.query?.grupo || "__ALL__");

    if (!cardCode || !warehouse) {
      return safeJson(res, 400, { ok: false, message: "cardCode y warehouse requeridos" });
    }

    const data = await detailsFromDb({ from, to, cardCode, warehouse, area, grupo });
    data.access = {
      moduleKey: "admin-clientes",
      isAdmin: !!req.moduleAccess?.isAdmin,
      areas: req.moduleAccess?.areas || [],
      availableAreas: portalAvailableAreasForAccess(req.moduleAccess),
      effectiveArea: area,
    };

    await savePortalUserMemory(req.user?.id, "admin-clientes", {
      lastRange: { from, to },
      lastFilters: { area, grupo },
      lastFocus: { cardCode, warehouse },
      updatedAt: new Date().toISOString(),
    });
    await writePortalAudit({
      userId: req.user?.id,
      moduleKey: "admin-clientes",
      actionType: "open_detail",
      entityType: "customer_warehouse",
      entityCode: `${cardCode}|${warehouse}`,
      entityName: `${cardCode} @ ${warehouse}`,
      areaCode: area === "__ALL__" ? "" : area,
      filters: { from, to, area, grupo },
    });

    return safeJson(res, 200, data);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get("/api/portal/admin-clientes/top-products", verifyPortalAuth, verifyPortalModule("admin-clientes"), async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = isISO(req.query?.from) ? String(req.query.from) : addDaysISO(today, -30);
    const to = isISO(req.query?.to) ? String(req.query.to) : today;
    const warehouse = String(req.query?.warehouse || "").trim();
    const cardCode = String(req.query?.cardCode || "").trim();
    const area = portalResolveEffectiveArea(req.moduleAccess, req.query?.area || "__ALL__");
    const grupo = String(req.query?.grupo || "__ALL__");
    const limit = Math.max(1, Math.min(200, Number(req.query?.limit || 10)));

    const data = await topProductsFromDb({ from, to, warehouse, cardCode, area, grupo, limit });
    data.access = {
      moduleKey: "admin-clientes",
      isAdmin: !!req.moduleAccess?.isAdmin,
      areas: req.moduleAccess?.areas || [],
      availableAreas: portalAvailableAreasForAccess(req.moduleAccess),
      effectiveArea: area,
    };
    return safeJson(res, 200, data);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get("/api/portal/admin-clientes/export", verifyPortalAuth, verifyPortalModule("admin-clientes"), async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = isISO(req.query?.from) ? String(req.query.from) : addDaysISO(today, -30);
    const to = isISO(req.query?.to) ? String(req.query.to) : today;
    const area = portalResolveEffectiveArea(req.moduleAccess, req.query?.area || "__ALL__");
    const grupo = String(req.query?.grupo || "__ALL__");
    const q = String(req.query?.q || "");

    const data = await dashboardFromDbAdminClientes({ from, to, area, grupo, q });

    const wb = XLSX.utils.book_new();
    const rows = (data.table || []).map((r) => ({
      "Código cliente": r.cardCode,
      "Cliente": r.cardName,
      "Cliente label": r.customer,
      "Bodega": r.warehouse,
      "Ventas netas $": r.dollars,
      "Ganancia bruta $": r.grossProfit,
      "% GP": r.grossPct,
      "Facturas": r.invoices,
      "Notas de crédito": r.creditNotes,
    }));
    const ws = XLSX.utils.json_to_sheet(rows);
    XLSX.utils.book_append_sheet(wb, ws, "Resumen");

    const buf = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });
    await writePortalAudit({
      userId: req.user?.id,
      moduleKey: "admin-clientes",
      actionType: "export_excel",
      areaCode: area === "__ALL__" ? "" : area,
      filters: { from, to, area, grupo, q },
      payload: { rows: rows.length },
    });
    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.setHeader("Content-Disposition", `attachment; filename="admin-clientes_${from}_a_${to}.xlsx"`);
    return res.end(buf);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get("/api/portal/admin-clientes/details/export", verifyPortalAuth, verifyPortalModule("admin-clientes"), async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = isISO(req.query?.from) ? String(req.query.from) : addDaysISO(today, -30);
    const to = isISO(req.query?.to) ? String(req.query.to) : today;
    const cardCode = String(req.query?.cardCode || "").trim();
    const warehouse = String(req.query?.warehouse || "").trim();
    const area = portalResolveEffectiveArea(req.moduleAccess, req.query?.area || "__ALL__");
    const grupo = String(req.query?.grupo || "__ALL__");

    if (!cardCode || !warehouse) {
      return safeJson(res, 400, { ok: false, message: "cardCode y warehouse requeridos" });
    }

    const data = await detailsFromDb({ from, to, cardCode, warehouse, area, grupo });
    const wb = XLSX.utils.book_new();

    const rows = [];
    for (const doc of (data.invoices || [])) {
      for (const ln of (doc.lines || [])) {
        rows.push({
          "Tipo": doc.docTypeLabel,
          "DocNum": doc.docNum,
          "Fecha": doc.docDate,
          "Código cliente": data.cardCode,
          "Bodega": data.warehouse,
          "Área": ln.area,
          "Grupo": ln.grupo,
          "ItemCode": ln.itemCode,
          "Descripción": ln.itemDesc,
          "Cantidad": ln.quantity,
          "Ventas netas $": ln.dollars,
          "Ganancia bruta $": ln.grossProfit,
          "% GP": ln.grossPct,
        });
      }
    }

    const ws = XLSX.utils.json_to_sheet(rows);
    XLSX.utils.book_append_sheet(wb, ws, "Detalle");
    const buf = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });

    await writePortalAudit({
      userId: req.user?.id,
      moduleKey: "admin-clientes",
      actionType: "export_detail_excel",
      entityType: "customer_warehouse",
      entityCode: `${cardCode}|${warehouse}`,
      areaCode: area === "__ALL__" ? "" : area,
      filters: { from, to, area, grupo },
      payload: { rows: rows.length },
    });

    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.setHeader("Content-Disposition", `attachment; filename="detalle_${cardCode}_${warehouse}_${from}_a_${to}.xlsx"`);
    return res.end(buf);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.post("/api/portal/admin-clientes/ai-chat", verifyPortalAuth, verifyPortalModule("admin-clientes"), async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const question = String(req.body?.question || "").trim();
    if (!question) return safeJson(res, 400, { ok: false, message: "question requerida" });

    const fromQ = String(req.body?.from || req.query?.from || "");
    const toQ = String(req.body?.to || req.query?.to || "");
    const cardCode = String(req.body?.cardCode || "").trim();
    const warehouse = String(req.body?.warehouse || "").trim();
    const customerLabel = String(req.body?.customerLabel || "").trim();
    const area = portalResolveEffectiveArea(req.moduleAccess, req.body?.area || req.query?.area || "__ALL__");
    const grupo = String(req.body?.grupo || req.query?.grupo || "__ALL__");
    const q = String(req.body?.q || req.query?.q || "").trim();

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const defaultFrom = addDaysISO(today, -30);
    const from = isISO(fromQ) ? fromQ : defaultFrom;
    const to = isISO(toQ) ? toQ : today;

    const dashboard = await dashboardFromDbAdminClientes({ from, to, area, grupo, q });
    const analytics = await buildAdminClientesAiAnalytics({ from, to, area, grupo, q });

    const resolvedFocus = adminResolveDashboardFocus({
      dashboard,
      question,
      q,
      cardCode,
      warehouse,
      customerLabel,
    });

    const focusCardCode = String(resolvedFocus?.cardCode || cardCode || "").trim();
    const focusWarehouse = String(resolvedFocus?.warehouse || warehouse || "").trim();
    const focusLabel = String(resolvedFocus?.label || customerLabel || "").trim();

    let detail = null;
    if (focusCardCode && focusWarehouse) {
      detail = await detailsFromDb({ from, to, cardCode: focusCardCode, warehouse: focusWarehouse, area, grupo });
    }

    const recommendationContext = await buildAdminClientesRecommendationAnalytics({
      from,
      to,
      area,
      grupo,
      targetCardCode: focusCardCode,
      customerLabel: focusLabel,
      question,
    });

    const out = await openaiDbAnalystChat({
      question,
      dashboard,
      analytics,
      detail,
      customerLabel: focusLabel,
      recommendationContext,
    });

    await savePortalUserMemory(req.user?.id, "admin-clientes", {
      lastRange: { from, to },
      lastFilters: { area, grupo, q },
      lastFocus: focusCardCode ? { cardCode: focusCardCode, warehouse: focusWarehouse, label: focusLabel } : null,
      lastQuestion: question,
      lastAnswerPreview: truncate(out.answer || "", 300),
      updatedAt: new Date().toISOString(),
    });

    await writePortalAudit({
      userId: req.user?.id,
      moduleKey: "admin-clientes",
      actionType: "ai_question",
      entityType: focusCardCode ? "customer_warehouse" : "",
      entityCode: focusCardCode ? `${focusCardCode}|${focusWarehouse}` : "",
      entityName: focusLabel || "",
      areaCode: area === "__ALL__" ? "" : area,
      filters: { from, to, area, grupo, q },
      payload: {
        inferred_focus: !!resolvedFocus?.cardCode,
        question: truncate(question, 400),
      },
    });

    return safeJson(res, 200, {
      ok: true,
      answer: out.answer,
      model: out.model,
      source: "db",
      range: { from, to },
      filters: { area, grupo, q },
      access: {
        moduleKey: "admin-clientes",
        isAdmin: !!req.moduleAccess?.isAdmin,
        areas: req.moduleAccess?.areas || [],
        availableAreas: portalAvailableAreasForAccess(req.moduleAccess),
        effectiveArea: area,
      },
      focus: focusCardCode ? {
        cardCode: focusCardCode,
        warehouse: focusWarehouse,
        customerLabel: focusLabel || `${focusCardCode}`,
        label: focusLabel || `${focusCardCode}`,
        inferred: !(cardCode && warehouse),
        source: resolvedFocus?.source || (cardCode && warehouse ? "explicit" : "none"),
      } : null,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.post("/api/portal/admin-clientes/sync", verifyPortalAuth, verifyPortalModule("admin-clientes", { adminOnly: true }), async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });
    if (missingSapEnv()) return safeJson(res, 500, { ok: false, message: "SAP env incompleto" });

    const fromQ = String(req.query?.from || req.body?.from || "");
    const toQ = String(req.query?.to || req.body?.to || "");
    const maxDocs = Math.max(500, Math.min(50000, Number(req.query?.maxDocs || req.body?.maxDocs || 12000)));

    if (!isISO(fromQ) || !isISO(toQ)) {
      return safeJson(res, 400, { ok: false, message: "from y to deben ser YYYY-MM-DD" });
    }

    const out = await syncRangeToDb({ from: fromQ, to: toQ, maxDocs });
    await writePortalAudit({
      userId: req.user?.id,
      moduleKey: "admin-clientes",
      actionType: "sync_range",
      filters: { from: fromQ, to: toQ, maxDocs },
      payload: out,
    });
    return safeJson(res, 200, { ok: true, ...out, from: fromQ, to: toQ, maxDocs });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.post("/api/portal/admin-clientes/sync/recent", verifyPortalAuth, verifyPortalModule("admin-clientes", { adminOnly: true }), async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });
    if (missingSapEnv()) return safeJson(res, 500, { ok: false, message: "SAP env incompleto" });

    const days = Math.max(1, Math.min(90, Number(req.query?.days || req.body?.days || 10)));
    const maxDocs = Math.max(500, Math.min(50000, Number(req.query?.maxDocs || req.body?.maxDocs || 12000)));

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = addDaysISO(today, -(days - 1));

    const out = await syncRangeToDb({ from, to: today, maxDocs });
    await writePortalAudit({
      userId: req.user?.id,
      moduleKey: "admin-clientes",
      actionType: "sync_recent",
      filters: { from, to: today, days, maxDocs },
      payload: out,
    });
    return safeJson(res, 200, { ok: true, ...out, from, to: today, days, maxDocs });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

__extraBootTasks.push(async () => {
  try {
    await ensurePortalModuleScopeDb();
  } catch (e) {
    console.error("Portal module scope DB init error:", e.message || String(e));
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
  console.log(`SKIP duplicate listen on :${PORT}`);
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
   ✅ PRODUCCIÓN (DB + JSON local + IA)
   Requiere:
   /data/production/production_formula_catalog.json
   /data/production/production_material_stock_catalog.json
   /data/production/production_capacity_config.json
========================================================= */

const PROD_FINISHED_WHS = ["01", "200", "300", "500"];

// Producción: helpers autosuficientes para no depender del scope de otros módulos
const PROD_GROUPS_CONS = (globalThis.GROUPS_CONS && globalThis.GROUPS_CONS.size ? globalThis.GROUPS_CONS : new Set([
  "Ambientador", "Cuidado de la ropa", "Lejías", "Desinfectante", "Limpieza cocina", "Limpieza Piso",
  "Limpieza Vidrios", "Limpieza General", "Limpieza de baño", "Quita grasa", "Lustramuebles",
  "Insecticida", "Detergente", "Desengrasante", "Suavizante", "Limpiador de baños"
]));
const PROD_GROUPS_RCI = (globalThis.GROUPS_RCI && globalThis.GROUPS_RCI.size ? globalThis.GROUPS_RCI : new Set([
  "Salsas", "Vinagres", "Consumidor", "Institucional", "Mayonesas", "Aderezos", "Bebidas", "Condimentos"
]));
const PROD_GROUPS_CONS_N = new Set(Array.from(PROD_GROUPS_CONS).map((s) => String(s||"").normalize("NFD").replace(/[̀-ͯ]/g, "").trim().toLowerCase()));
const PROD_GROUPS_RCI_N = new Set(Array.from(PROD_GROUPS_RCI).map((s) => String(s||"").normalize("NFD").replace(/[̀-ͯ]/g, "").trim().toLowerCase()));

function prodNormGroupName(v) {
  return String(v || "")
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function prodNormalizeGrupoFinal(grupoRaw) {
  const canon = String(grupoRaw || "").trim() || "Sin grupo";
  const canonN = prodNormGroupName(canon);
  if (PROD_GROUPS_CONS_N.has(canonN) || PROD_GROUPS_RCI_N.has(canonN)) return canon;
  if (!canonN) return "Sin grupo";
  if (/(ambient|ropa|lej|desinfect|cocina|piso|vidrio|bano|grasa|mueble|insect|deterg|suav|limpiador)/i.test(canonN)) return "Limpieza General";
  if (/(salsa|vinagre|mayonesa|aderezo|condiment|consumidor|institucional)/i.test(canonN)) return "Salsas";
  return canon;
}

function prodInferAreaFromGroup(groupName) {
  const g = prodNormGroupName(groupName);
  if (PROD_GROUPS_RCI_N.has(g)) return "RCI";
  if (PROD_GROUPS_CONS_N.has(g)) return "CONS";
  if (/(salsa|vinagre|mayonesa|aderezo|condiment|consumidor|institucional)/i.test(g)) return "RCI";
  if (/(ambient|ropa|lej|desinfect|cocina|piso|vidrio|bano|grasa|mueble|insect|deterg|suav|limpiador)/i.test(g)) return "CONS";
  return "";
}

function prodAbcByMetric(rows, metricKey) {
  const arr = (Array.isArray(rows) ? rows : [])
    .map((r) => ({ key: r.itemCode, v: Math.max(0, Number((r && r[metricKey]) || 0)) }))
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

function prodLetterScore(l) {
  const L = String(l || "").toUpperCase();
  if (L === "A") return 4;
  if (L === "B") return 3;
  if (L === "C") return 2;
  return 1;
}

function prodTotalFromLetters(a1, a2, a3) {
  const avg = (prodLetterScore(a1) + prodLetterScore(a2) + prodLetterScore(a3)) / 3;
  const t = Math.round(avg * 10) / 10;
  if (t >= 3.5) return { label: "AB Crítico", cls: "bad", t };
  if (t >= 2.0) return { label: "C Importante", cls: "warn", t };
  return { label: "D", cls: "ok", t };
}

const PROD_DATA_DIR = process.env.PROD_DATA_DIR || path.join(process.cwd(), "data", "production");
const PROD_FORMULA_JSON = process.env.PROD_FORMULA_JSON || path.join(PROD_DATA_DIR, "production_formula_catalog.json");
const PROD_MATERIAL_JSON = process.env.PROD_MATERIAL_JSON || path.join(PROD_DATA_DIR, "production_material_stock_catalog.json");
const PROD_CAPACITY_JSON = process.env.PROD_CAPACITY_JSON || path.join(PROD_DATA_DIR, "production_capacity_config.json");

let __prodLocalCache = { loadedAt: 0, formulas: { products: {}, liquids: {} }, materials: { materials: {} }, capacity: {} };

function prodNum(v, def = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
}
function prodRound(n, d = 2) {
  const p = 10 ** d;
  return Math.round(prodNum(n) * p) / p;
}
function prodNorm(s) {
  return String(s || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}
function prodReadJsonSafe(fp, fallback) {
  try {
    if (!fs.existsSync(fp)) return fallback;
    return JSON.parse(fs.readFileSync(fp, "utf8"));
  } catch {
    return fallback;
  }
}
function loadProductionLocalData(force = false) {
  const mtimes = [PROD_FORMULA_JSON, PROD_MATERIAL_JSON, PROD_CAPACITY_JSON]
    .map((fp) => (fs.existsSync(fp) ? fs.statSync(fp).mtimeMs : 0))
    .join("|");

  if (!force && __prodLocalCache.loadedAt === mtimes) return __prodLocalCache;

  const formulas = prodReadJsonSafe(PROD_FORMULA_JSON, { products: {}, liquids: {} });
  const materials = prodReadJsonSafe(PROD_MATERIAL_JSON, { materials: {} });
  const capacity = prodReadJsonSafe(PROD_CAPACITY_JSON, {
    shiftHours: 8,
    workdays: [1, 2, 3, 4, 5],
    allowSaturday: true,
    defaultRates: { SAUCES: 700, CLEANING: 650 },
    itemRates: { "68328": 666.67 },
    machineNames: { SAUCES: "Máquina de salsas", CLEANING: "Máquina de limpieza" },
  });

  __prodLocalCache = { loadedAt: mtimes, formulas, materials, capacity };
  return __prodLocalCache;
}

function prodMachineFromAreaOrGroup(area, grupo, itemMeta = null) {
  if (itemMeta?.machine) return itemMeta.machine;
  const g = prodNorm(grupo);
  const a = String(area || "").toUpperCase();
  if (a === "RCI") return "CLEANING";
  if (g.includes("sazon") || g.includes("vinagr") || g.includes("especial") || g.includes("gmt")) return "SAUCES";
  return "CLEANING";
}
function prodApplyMrp(need, minOrder, multiple) {
  let out = Math.max(0, prodNum(need));
  const mn = Math.max(0, prodNum(minOrder));
  const mul = Math.max(0, prodNum(multiple));

  if (out <= 0) return 0;
  if (mn > 0 && out < mn) out = mn;
  if (mul > 1) out = Math.ceil(out / mul) * mul;
  return Math.round(out);
}
function prodYm(d) {
  const dt = d instanceof Date ? d : new Date(d);
  return `${dt.getFullYear()}-${String(dt.getMonth() + 1).padStart(2, "0")}`;
}
function prodAddMonthsISO(iso, months) {
  const [y, m, d] = String(iso || "").split("-").map(Number);
  const dt = new Date(y || 2000, (m || 1) - 1, d || 1);
  dt.setMonth(dt.getMonth() + Number(months || 0));
  return `${dt.getFullYear()}-${String(dt.getMonth() + 1).padStart(2, "0")}-${String(dt.getDate()).padStart(2, "0")}`;
}
function prodCountDays(startISO, endISO, dayNums = [1, 2, 3, 4, 5]) {
  const set = new Set(dayNums.map(Number));
  const a = new Date(startISO);
  const b = new Date(endISO);
  let n = 0;
  for (let d = new Date(a); d <= b; d.setDate(d.getDate() + 1)) {
    const wd = d.getDay(); // 0 Sun, 6 Sat
    const map = wd === 0 ? 7 : wd; // 1 Mon ... 7 Sun
    if (set.has(map)) n++;
  }
  return n;
}
function prodCountSaturdays(startISO, endISO) {
  return prodCountDays(startISO, endISO, [6]);
}
function prodFormatMonthName(ym) {
  const [y, m] = String(ym || "").split("-").map(Number);
  const names = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"];
  return `${names[(m || 1) - 1]} ${y || ""}`.trim();
}
function prodInferLitersPerUnit(desc) {
  const d = prodNorm(desc);
  const mm = d.match(/(\d+(?:\.\d+)?)\s*ml/);
  if (mm) return prodRound(Number(mm[1]) / 1000, 6);
  if (/5\s*5\s*oz|5\s*5\s*onz/.test(d)) return 0.165833;
  if (/10\s*5\s*oz|10\s*5\s*onz/.test(d)) return 0.315417;
  if (/20\s*oz|20\s*onz/.test(d)) return 0.591667;
  if (/24\s*oz|24\s*onz/.test(d)) return 0.709765;
  if (/29\s*oz|29\s*onz/.test(d)) return 0.87;
  if (/32\s*oz|32\s*onz/.test(d)) return 0.94625;
  if (/1\s*gal/.test(d)) return 3.785;
  return 0;
}

async function ensureProductionDb() {
  if (!hasDb()) return;

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS production_inv_wh_cache (
      item_code TEXT NOT NULL,
      item_desc TEXT NOT NULL DEFAULT '',
      warehouse TEXT NOT NULL,
      stock NUMERIC(18,4) NOT NULL DEFAULT 0,
      stock_min NUMERIC(18,4) NOT NULL DEFAULT 0,
      stock_max NUMERIC(18,4) NOT NULL DEFAULT 0,
      committed NUMERIC(18,4) NOT NULL DEFAULT 0,
      ordered NUMERIC(18,4) NOT NULL DEFAULT 0,
      available NUMERIC(18,4) NOT NULL DEFAULT 0,
      updated_at TIMESTAMP DEFAULT NOW(),
      PRIMARY KEY(item_code, warehouse)
    );
  `);

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS production_mrp_cache (
      item_code TEXT PRIMARY KEY,
      item_desc TEXT NOT NULL DEFAULT '',
      procurement_method TEXT NOT NULL DEFAULT '',
      lead_time_days NUMERIC(18,4) NOT NULL DEFAULT 0,
      min_order_qty NUMERIC(18,4) NOT NULL DEFAULT 0,
      multiple_qty NUMERIC(18,4) NOT NULL DEFAULT 0,
      planning_system TEXT NOT NULL DEFAULT '',
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_prod_inv_wh_item ON production_inv_wh_cache(item_code);`);
}
__extraBootTasks.push(async () => {
  try {
    await ensureProductionDb();
    loadProductionLocalData(true);
  } catch (e) {
    console.error("Producción init error:", e.message || String(e));
  }
});

function prodProcurementMethodLabel(value) {
  const raw = String(value || "").trim();
  const norm = raw.toLowerCase();
  if (["bom_make", "make", "m", "tmake"].includes(norm)) return "Se fabrica en planta";
  if (["bom_buy", "buy", "b", "tbuy"].includes(norm)) return "No se fabrica en planta";
  return raw || "—";
}

function prodExtractMrpFromItem(it) {
  const procurementMethod = String(it?.ProcurementMethod || it?.IssueMethod || "").trim();
  return {
    itemCode: String(it?.ItemCode || "").trim(),
    itemDesc: String(it?.ItemName || "").trim(),
    procurementMethod,
    procurementMethodLabel: prodProcurementMethodLabel(procurementMethod),
    planningSystem: String(it?.PlanningSystem || "").trim(),
    leadTimeDays: prodNum(it?.LeadTime ?? it?.LeadTimeDays ?? it?.LeadTm ?? 0),
    minOrderQty: prodNum(it?.MinimumOrderQuantity ?? it?.MinOrderQty ?? it?.MinOrdrQty ?? it?.MinInventory ?? 0),
    multipleQty: prodNum(it?.OrderMultiple ?? it?.OrderMultipleQty ?? it?.OrdrMulti ?? it?.OrderIntervals ?? 0),
  };
}


async function prodGetFullItem(code) {
  const itemCode = String(code || "").trim();
  if (!itemCode) return null;
  const safe = itemCode.replace(/'/g, "''");

  let item = null;
  try {
    item = await slFetch(
      `/Items('${safe}')?$select=` +
      [
        "ItemCode","ItemName","ItemsGroupCode","SalesUnit","InventoryItem","Valid","FrozenFor",
        "ProcurementMethod","PlanningSystem","LeadTime","LeadTimeDays","MinimumOrderQuantity",
        "MinOrderQty","OrderMultiple","OrderMultipleQty","MinInventory","IssueMethod","TreeType",
        "AvgPrice","AveragePrice","AvgStdPrice","AvgStdPrc","LastPurPrc","LastPurchasePrice",
        "MainSupplier","SupplierCatalogNo","ForeignName"
      ].join(","),
      { timeoutMs: 120000 }
    );
  } catch (e1) {
    try {
      item = await slFetch(`/Items('${safe}')`, { timeoutMs: 120000 });
    } catch (e2) {
      item = { ItemCode: itemCode, ItemName: "", ItemWarehouseInfoCollection: [] };
    }
  }

  if (!Array.isArray(item?.ItemWarehouseInfoCollection) || !item.ItemWarehouseInfoCollection.length) {
    try {
      const whInfo = await slFetch(
        `/Items('${safe}')/ItemWarehouseInfoCollection?$select=` +
        [
          "WarehouseCode","WhsCode","InStock","OnHand","Committed","IsCommited","Ordered","OnOrder",
          "MinimalStock","MinStock","MaximalStock","MaxStock","AvgPrice","AveragePrice","AvgStdPrc","AvgStdPrice","Price"
        ].join(","),
        { timeoutMs: 120000 }
      );
      if (Array.isArray(whInfo?.value)) item.ItemWarehouseInfoCollection = whInfo.value;
      else if (Array.isArray(whInfo)) item.ItemWarehouseInfoCollection = whInfo;
    } catch (e3) {
      item.ItemWarehouseInfoCollection = Array.isArray(item?.ItemWarehouseInfoCollection)
        ? item.ItemWarehouseInfoCollection
        : [];
    }
  }

  return item;
}

function prodNormalizeSlCollection(obj) {
  if (Array.isArray(obj)) return obj;
  if (Array.isArray(obj?.value)) return obj.value;
  return [];
}

function prodExtractWeightedCostFromItem(item) {
  if (!item || typeof item !== "object") return 0;

  const whRows = Array.isArray(item.ItemWarehouseInfoCollection) ? item.ItemWarehouseInfoCollection : [];
  const priceKeys = ["AvgPrice", "AveragePrice", "AvgStdPrice", "AvgStdPrc", "Price"];
  const stockKeys = ["InStock", "OnHand", "Stock"];
  let nume = 0;
  let den = 0;

  for (const r of whRows) {
    const wh = String(r?.WarehouseCode ?? r?.WhsCode ?? "").trim();
    if (wh && !PROD_FINISHED_WHS.includes(wh)) continue;

    let price = 0;
    for (const k of priceKeys) {
      if (r && r[k] != null && r[k] !== "") {
        price = prodNum(r[k]);
        break;
      }
    }

    let stock = 0;
    for (const k of stockKeys) {
      if (r && r[k] != null && r[k] !== "") {
        stock = prodNum(r[k]);
        break;
      }
    }

    if (price > 0 && stock > 0) {
      nume += price * stock;
      den += stock;
    }
  }

  if (den > 0) return prodRound(nume / den, 6);

  const itemKeys = ["AvgPrice", "AveragePrice", "AvgStdPrice", "AvgStdPrc", "LastPurPrc", "LastPurchasePrice"];
  for (const k of itemKeys) {
    if (item[k] != null && item[k] !== "") {
      const v = prodNum(item[k]);
      if (v > 0) return prodRound(v, 6);
    }
  }
  return 0;
}

function prodExtractInventorySnapshotFromItem(item) {
  const byWh = { "01": 0, "200": 0, "300": 0, "500": 0 };
  const whRows = Array.isArray(item?.ItemWarehouseInfoCollection) ? item.ItemWarehouseInfoCollection : [];
  let total = 0;
  let stockMin = 0;
  let stockMax = 0;

  for (const r of whRows) {
    const wh = String(r?.WarehouseCode ?? r?.WhsCode ?? "").trim();
    const stock = prodNum(r?.InStock ?? r?.OnHand ?? 0);
    const minStock = prodNum(r?.MinimalStock ?? r?.MinStock ?? 0);
    const maxStock = prodNum(r?.MaximalStock ?? r?.MaxStock ?? 0);
    total += stock;
    stockMin = Math.max(stockMin, minStock);
    stockMax = Math.max(stockMax, maxStock);
    if (Object.prototype.hasOwnProperty.call(byWh, wh)) byWh[wh] = prodRound(stock, 3);
  }

  return {
    byWarehouse: byWh,
    total: prodRound(total, 3),
    stockMin: prodRound(stockMin, 3),
    stockMax: prodRound(stockMax, 3),
  };
}

function prodNormalizeProductionOrderRow(r) {
  const postDateRaw = r?.PostingDate ?? r?.PostDate ?? r?.StartDate ?? r?.DueDate ?? "";
  const postDate = String(postDateRaw || "").slice(0, 10);
  return {
    docNum: Number(r?.DocumentNumber ?? r?.DocNum ?? r?.AbsoluteEntry ?? r?.Absoluteentry ?? 0) || null,
    absoluteEntry: Number(r?.AbsoluteEntry ?? r?.Absoluteentry ?? r?.DocEntry ?? 0) || null,
    itemCode: String(r?.ItemNo ?? r?.ItemCode ?? "").trim(),
    prodName: String(r?.ProductDescription ?? r?.ProdName ?? r?.ItemName ?? "").trim(),
    plannedQty: prodRound(r?.PlannedQuantity ?? r?.PlannedQty ?? r?.PlannedQtty ?? 0, 3),
    completedQty: prodRound(r?.CompletedQuantity ?? r?.CmpltQty ?? r?.CompletedQty ?? 0, 3),
    rejectedQty: prodRound(r?.RejectedQuantity ?? r?.RejectedQty ?? 0, 3),
    postDate,
    status: String(r?.ProductionOrderStatus ?? r?.Status ?? "").trim(),
    warehouse: String(r?.Warehouse ?? r?.WarehouseCode ?? r?.WhsCode ?? "").trim(),
    origin: String(r?.ProductionOrderOrigin ?? r?.Origin ?? "").trim(),
  };
}

async function prodFetchProductionOrders(itemCode, top = 80) {
  const code = String(itemCode || "").trim();
  if (!code || missingSapEnv()) return { orders: [], monthly: new Map() };

  const safeCode = code.replace(/'/g, "''");
  const topSafe = Math.max(20, Math.min(200, Number(top || 80)));
  const paths = [
    `/ProductionOrders?$filter=ItemNo eq '${safeCode}'&$orderby=PostingDate desc&$top=${topSafe}`,
    `/ProductionOrders?$filter=ItemNo eq '${safeCode}'&$orderby=PostDate desc&$top=${topSafe}`,
    `/ProductionOrders?$filter=ItemCode eq '${safeCode}'&$orderby=PostingDate desc&$top=${topSafe}`,
    `/ProductionOrders?$filter=ItemCode eq '${safeCode}'&$orderby=PostDate desc&$top=${topSafe}`,
  ];

  let raw = [];
  let lastErr = null;
  for (const path of paths) {
    try {
      const res = await slFetch(path, { timeoutMs: 120000 });
      raw = prodNormalizeSlCollection(res);
      if (raw.length || Array.isArray(res)) break;
    } catch (e) {
      lastErr = e;
    }
  }
  if (!raw.length && lastErr) {
    return { orders: [], monthly: new Map(), warning: lastErr.message || String(lastErr) };
  }

  const orders = raw
    .map(prodNormalizeProductionOrderRow)
    .filter((x) => String(x.itemCode || "") === code || !x.itemCode)
    .sort((a, b) => String(b.postDate || "").localeCompare(String(a.postDate || "")) || Number(b.docNum || 0) - Number(a.docNum || 0));

  const monthly = new Map();
  for (const o of orders) {
    const ym = prodYm(o.postDate || new Date());
    const prev = monthly.get(ym) || 0;
    monthly.set(ym, prodRound(prev + prodNum(o.completedQty), 3));
  }
  return { orders, monthly };
}

function prodLooksLikeResource({ code, description = "", item = null, line = null }) {
  const txt = `${code || ""} ${description || ""} ${item?.ItemName || ""} ${item?.ForeignName || ""} ${line?.IssueMethod || ""} ${line?.ItemType || ""} ${line?.Type || ""}`.toLowerCase();
  const codeTxt = String(code || "").trim().toLowerCase();
  const inventoryFlag = String(item?.InventoryItem || "").trim().toLowerCase();
  if (/(operari|supervisor|linea de produccion|línea de producción|mano de obra|recurso|resource|labor|overhead|gastos? de fabricaci|horas? hombre|maquina de limpieza|maquina de salsas|servicio interno)/i.test(txt)) return true;
  if (/^(ogf|mli|mlim|mosup|m00\d|mo\d{2,}|res)/i.test(codeTxt)) return true;
  if (["tno", "no", "n", "f"].includes(inventoryFlag) && /(operari|supervisor|linea|línea|gasto|recurso|labor|overhead|servicio)/i.test(txt)) return true;
  return false;
}

function prodClassifyComponentType({ code, description = "", item = null, line = null }) {
  if (prodLooksLikeResource({ code, description, item, line })) return "RESOURCE";
  const txt = `${code || ""} ${description || ""} ${item?.ItemName || ""} ${item?.ForeignName || ""} ${line?.IssueMethod || ""}`.toLowerCase();
  if (/(botella|tapa|tap[aá]|liner|etiq|label|caja|cajeta|empaque|envase|shrink|sticker|frente|repuesto|atomizador|spray|doypack|valvula|válvula|dispensador|manga|sello|carton|cartón|bandeja|bolsa|bottle|cap)/i.test(txt)) {
    return "PACKAGING";
  }
  return "RAW_MATERIAL";
}

function prodExtractSupplierFromItem(item) {
  const candidates = [
    item?.MainSupplier,
    item?.PreferredVendor,
    item?.SupplierCatalogNo,
    item?.ForeignName,
    item?.Manufacturer,
  ];
  for (const c of candidates) {
    const v = String(c || "").trim();
    if (v) return v;
  }
  return "";
}

function prodExtractComponentStockInfo(item, preferredWh = "") {
  const whRows = Array.isArray(item?.ItemWarehouseInfoCollection) ? item.ItemWarehouseInfoCollection : [];
  const preferred = String(preferredWh || "").trim();
  let stockSpecific = 0;
  let availableSpecific = 0;
  let hasSpecific = false;
  let stockTotal = 0;
  let availableTotal = 0;

  for (const r of whRows) {
    const wh = String(r?.WarehouseCode ?? r?.WhsCode ?? "").trim();
    const stock = prodNum(r?.InStock ?? r?.OnHand ?? 0);
    const committed = prodNum(r?.Committed ?? r?.IsCommited ?? 0);
    const ordered = prodNum(r?.Ordered ?? r?.OnOrder ?? 0);
    const available = stock - committed + ordered;

    stockTotal += stock;
    availableTotal += available;

    if (preferred && wh === preferred) {
      stockSpecific += stock;
      availableSpecific += available;
      hasSpecific = true;
    }
  }

  return {
    stockQty: prodRound(hasSpecific ? stockSpecific : stockTotal, 3),
    availableQty: prodRound(hasSpecific ? availableSpecific : availableTotal, 3),
    stockTotal: prodRound(stockTotal, 3),
  };
}

function prodNormalizeBomLine(line) {
  return {
    code: String(line?.ItemCode ?? line?.Code ?? line?.ChildCode ?? "").trim(),
    description: String(line?.ItemName ?? line?.ItemDescription ?? line?.ProductDescription ?? line?.Description ?? "").trim(),
    quantity: prodNum(line?.Quantity ?? line?.PlannedQuantity ?? line?.Qty ?? line?.BaseQuantity ?? 0),
    unit: String(line?.InventoryUOM ?? line?.UoMCode ?? line?.UomCode ?? line?.UoMName ?? line?.Unit ?? "").trim(),
    warehouse: String(line?.Warehouse ?? line?.WarehouseCode ?? line?.WhsCode ?? "").trim(),
    issueMethod: String(line?.IssueMethod ?? "").trim(),
    raw: line || {},
  };
}

async function prodFetchSapBom(itemCode) {
  const code = String(itemCode || "").trim();
  if (!code || missingSapEnv()) return { source: "SAP ProductTrees", tree: null, headerQty: 1, lines: [] };

  const safe = code.replace(/'/g, "''");
  const tryPaths = [
    `/ProductTrees('${safe}')?$expand=ProductTreeLines`,
    `/ProductTrees('${safe}')`,
    `/ProductTrees?$filter=TreeCode eq '${safe}'&$top=1`,
    `/ProductTrees?$filter=Code eq '${safe}'&$top=1`,
  ];

  let tree = null;
  let lastErr = null;
  for (const path of tryPaths) {
    try {
      const res = await slFetch(path, { timeoutMs: 120000 });
      tree = Array.isArray(res?.value) ? (res.value[0] || null) : (res || null);
      if (tree) break;
    } catch (e) {
      lastErr = e;
    }
  }
  if (!tree) return { source: "SAP ProductTrees", tree: null, headerQty: 1, lines: [], warning: lastErr?.message || "" };

  let lines = [];
  const lineKeys = ["ProductTreeLines", "Items", "BOMLines", "BillOfMaterialsLines"];
  for (const key of lineKeys) {
    if (Array.isArray(tree?.[key]) && tree[key].length) {
      lines = tree[key];
      break;
    }
  }

  if (!lines.length) {
    const linePaths = [
      `/ProductTrees('${safe}')/ProductTreeLines`,
      `/ProductTrees('${safe}')/Items`,
    ];
    for (const path of linePaths) {
      try {
        const res = await slFetch(path, { timeoutMs: 120000 });
        const arr = prodNormalizeSlCollection(res);
        if (arr.length) {
          lines = arr;
          break;
        }
      } catch {}
    }
  }

  const normalized = lines
    .map(prodNormalizeBomLine)
    .filter((x) => String(x.code || "").trim() && prodNum(x.quantity) > 0);

  const headerQty = Math.max(1, prodNum(tree?.Quantity ?? tree?.TreeQuantity ?? tree?.PlannedQuantity ?? 1));
  return {
    source: "SAP ProductTrees",
    tree,
    headerQty,
    lines: normalized,
  };
}

async function prodBuildRequirementsFromSapBom({ itemCode, adjustedQty, sapBom }) {
  const headerQty = Math.max(1, prodNum(sapBom?.headerQty || 1));
  const bomLines = Array.isArray(sapBom?.lines) ? sapBom.lines : [];
  if (!bomLines.length) {
    return {
      source: sapBom?.source || "SAP ProductTrees",
      all: [],
      rawMaterials: [],
      packaging: [],
      bottlenecks: [],
      bomHeaderQty: headerQty,
      bomLines: [],
    };
  }

  const results = [];
  let idx = 0;
  const workers = Array.from({ length: Math.min(6, Math.max(1, bomLines.length)) }, async () => {
    while (idx < bomLines.length) {
      const current = bomLines[idx++];
      const item = await prodGetFullItem(current.code).catch(() => null);
      const perUnit = prodNum(current.quantity) / headerQty;
      const requiredQty = prodRound(perUnit * prodNum(adjustedQty), 3);
      const componentType = prodClassifyComponentType({ code: current.code, description: current.description, item, line: current.raw });
      const isResource = componentType === "RESOURCE";
      const stockInfo = isResource ? { stockQty: 0, availableQty: 0 } : prodExtractComponentStockInfo(item, current.warehouse);
      const stockQty = isResource ? 0 : (stockInfo.availableQty > 0 ? stockInfo.availableQty : stockInfo.stockQty);
      const shortage = isResource ? 0 : Math.max(0, requiredQty - stockQty);
      const coverage = isResource ? 1 : (requiredQty > 0 ? stockQty / requiredQty : 0);
      results.push({
        code: current.code,
        description: current.description || String(item?.ItemName || ""),
        requiredQty,
        unit: current.unit || String(item?.SalesUnit || ""),
        stockQty: prodRound(stockQty, 3),
        shortageQty: prodRound(shortage, 3),
        coveragePct: prodRound(coverage * 100, 1),
        supplier: isResource ? "Recurso interno" : prodExtractSupplierFromItem(item),
        cost: prodRound(prodExtractWeightedCostFromItem(item), 4),
        status: isResource ? "OK" : (shortage > 0 ? "FALTANTE" : "OK"),
        componentType,
        warehouse: current.warehouse || "",
        bomQtyBase: prodRound(current.quantity, 6),
        perUnitQty: prodRound(perUnit, 6),
        issueMethod: current.issueMethod || "",
        isResource,
        inventoryTracked: !isResource,
        subPlanQty: prodRound(shortage > 0 ? shortage : requiredQty, 3),
        resourceNote: isResource ? "Recurso de producción; no consume inventario y no debe tratarse como faltante." : "",
      });
    }
  });
  await Promise.all(workers);

  results.sort((a, b) => {
    const aRes = a.componentType === "RESOURCE" ? 1 : 0;
    const bRes = b.componentType === "RESOURCE" ? 1 : 0;
    if (aRes !== bRes) return aRes - bRes;
    return b.shortageQty - a.shortageQty || String(a.code).localeCompare(String(b.code));
  });
  const rawMaterials = results.filter((x) => x.componentType === "RAW_MATERIAL");
  const packaging = results.filter((x) => x.componentType === "PACKAGING");
  const resources = results.filter((x) => x.componentType === "RESOURCE");
  const bottlenecks = results.filter((x) => x.shortageQty > 0 && x.componentType !== "RESOURCE").slice(0, 10);

  return {
    source: sapBom?.source || "SAP ProductTrees",
    all: results,
    rawMaterials,
    packaging,
    resources,
    bottlenecks,
    bomHeaderQty: headerQty,
    bomLines,
  };
}

function prodRecommendPracticalQty({ neededQty = 0, avgMonthlyQty = 0, minOrderQty = 0, multipleQty = 0 }) {
  const need = Math.max(0, prodNum(neededQty));
  if (need <= 0) return 0;

  const avg = Math.max(0, prodNum(avgMonthlyQty));
  let out = prodApplyMrp(need, minOrderQty, multipleQty);

  let practicalMultiple = Math.max(0, prodNum(multipleQty));
  if (!(practicalMultiple > 1)) {
    if (avg >= 1000) practicalMultiple = 200;
    else if (avg >= 500) practicalMultiple = 100;
    else if (avg >= 100) practicalMultiple = 50;
    else practicalMultiple = 10;
  }

  const practicalFloor = avg > 0 ? avg * 1.5 : out;
  if (out < practicalFloor) {
    out = Math.ceil(practicalFloor / practicalMultiple) * practicalMultiple;
  }

  if (minOrderQty > 0 && out < prodNum(minOrderQty)) out = prodNum(minOrderQty);
  if (practicalMultiple > 1) out = Math.ceil(out / practicalMultiple) * practicalMultiple;
  return Math.round(out);
}

async function syncProductionInventoryWh({ from, to, maxItems = 2500 }) {
  const r = await dbQuery(
    `
    SELECT DISTINCT item_code
    FROM sales_item_lines
    WHERE doc_date >= $1::date AND doc_date <= $2::date
      AND item_code <> ''
    LIMIT $3
    `,
    [from, to, Math.max(100, Math.min(8000, Number(maxItems || 2500)))]
  );
  const codes = (r.rows || []).map((x) => String(x.item_code || "").trim()).filter(Boolean);
  let saved = 0;
  const errors = [];

  for (let i = 0; i < codes.length; i++) {
    try {
      const itemCode = codes[i];
      const it = await prodGetFullItem(itemCode);
      const itemDesc = String(it?.ItemName || "").trim();
      const whRows = Array.isArray(it?.ItemWarehouseInfoCollection) ? it.ItemWarehouseInfoCollection : [];

      for (const wh of PROD_FINISHED_WHS) {
        const w = whRows.find((x) => String(x?.WarehouseCode ?? x?.WhsCode ?? "").trim() === wh) || {};
        const stock = prodNum(w?.InStock ?? w?.OnHand ?? 0);
        const committed = prodNum(w?.Committed ?? w?.IsCommited ?? 0);
        const ordered = prodNum(w?.Ordered ?? w?.OnOrder ?? 0);
        const stockMin = prodNum(w?.MinimalStock ?? w?.MinStock ?? 0);
        const stockMax = prodNum(w?.MaximalStock ?? w?.MaxStock ?? 0);
        const available = stock - committed + ordered;

        await dbQuery(
          `
          INSERT INTO production_inv_wh_cache(item_code,item_desc,warehouse,stock,stock_min,stock_max,committed,ordered,available,updated_at)
          VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW())
          ON CONFLICT(item_code,warehouse) DO UPDATE SET
            item_desc=EXCLUDED.item_desc,
            stock=EXCLUDED.stock,
            stock_min=EXCLUDED.stock_min,
            stock_max=EXCLUDED.stock_max,
            committed=EXCLUDED.committed,
            ordered=EXCLUDED.ordered,
            available=EXCLUDED.available,
            updated_at=NOW()
          `,
          [itemCode, itemDesc, wh, stock, stockMin, stockMax, committed, ordered, available]
        );
        saved++;
      }
    } catch (e) {
      if (errors.length < 10) errors.push({ itemCode: codes[i], message: e.message || String(e) });
    }
    if ((i + 1) % 20 === 0) await sleep(15);
  }
  return { saved, errors, items: codes.length };
}

async function syncProductionMrp({ from, to, maxItems = 2500 }) {
  const r = await dbQuery(
    `
    SELECT DISTINCT item_code
    FROM sales_item_lines
    WHERE doc_date >= $1::date AND doc_date <= $2::date
      AND item_code <> ''
    LIMIT $3
    `,
    [from, to, Math.max(100, Math.min(8000, Number(maxItems || 2500)))]
  );
  const codes = (r.rows || []).map((x) => String(x.item_code || "").trim()).filter(Boolean);
  let saved = 0;

  for (let i = 0; i < codes.length; i++) {
    try {
      const it = await prodGetFullItem(codes[i]);
      const mrp = prodExtractMrpFromItem(it || {});
      await dbQuery(
        `
        INSERT INTO production_mrp_cache(item_code,item_desc,procurement_method,lead_time_days,min_order_qty,multiple_qty,planning_system,updated_at)
        VALUES($1,$2,$3,$4,$5,$6,$7,NOW())
        ON CONFLICT(item_code) DO UPDATE SET
          item_desc=EXCLUDED.item_desc,
          procurement_method=EXCLUDED.procurement_method,
          lead_time_days=EXCLUDED.lead_time_days,
          min_order_qty=EXCLUDED.min_order_qty,
          multiple_qty=EXCLUDED.multiple_qty,
          planning_system=EXCLUDED.planning_system,
          updated_at=NOW()
        `,
        [mrp.itemCode || codes[i], mrp.itemDesc || "", mrp.procurementMethod, mrp.leadTimeDays, mrp.minOrderQty, mrp.multipleQty, mrp.planningSystem]
      );
      saved++;
    } catch {}
    if ((i + 1) % 20 === 0) await sleep(15);
  }
  return saved;
}

async function productionDashboardFromDb({ from, to, area, grupo, q, avgMonths = 5, horizonMonths = 3 }) {
  const rows = await dbQuery(
    `
    WITH sales AS (
      SELECT
        s.item_code,
        MAX(NULLIF(s.item_desc,'')) AS item_desc,
        COALESCE(SUM(s.revenue),0)::numeric(18,2) AS revenue,
        COALESCE(SUM(s.gross_profit),0)::numeric(18,2) AS gp,
        COALESCE(SUM(s.quantity),0)::numeric(18,4) AS qty,
        MAX(NULLIF(s.area,'')) AS area_s,
        MAX(NULLIF(s.item_group,'')) AS grupo_s
      FROM sales_item_lines s
      WHERE s.doc_date >= $1::date AND s.doc_date <= $2::date
      GROUP BY s.item_code
    ),
    inv AS (
      SELECT
        item_code,
        SUM(CASE WHEN warehouse='01'  THEN stock ELSE 0 END)::float AS wh_01,
        SUM(CASE WHEN warehouse='200' THEN stock ELSE 0 END)::float AS wh_200,
        SUM(CASE WHEN warehouse='300' THEN stock ELSE 0 END)::float AS wh_300,
        SUM(CASE WHEN warehouse='500' THEN stock ELSE 0 END)::float AS wh_500,
        SUM(stock)::float AS stock_total,
        MAX(stock_min)::float AS stock_min,
        MAX(stock_max)::float AS stock_max
      FROM production_inv_wh_cache
      GROUP BY item_code
    )
    SELECT
      sales.item_code,
      COALESCE(NULLIF(sales.item_desc,''), NULLIF(g.item_desc,''), '') AS item_desc,
      COALESCE(NULLIF(sales.grupo_s,''), NULLIF(g.grupo,''), NULLIF(g.group_name,''), 'Sin grupo') AS grupo,
      COALESCE(NULLIF(sales.area_s,''), NULLIF(g.area,''), '') AS area,
      sales.revenue,
      sales.gp,
      sales.qty,
      COALESCE(inv.wh_01,0) AS wh_01,
      COALESCE(inv.wh_200,0) AS wh_200,
      COALESCE(inv.wh_300,0) AS wh_300,
      COALESCE(inv.wh_500,0) AS wh_500,
      COALESCE(inv.stock_total,0) AS stock_total,
      COALESCE(inv.stock_min,0) AS stock_min,
      COALESCE(inv.stock_max,0) AS stock_max,
      COALESCE(m.procurement_method,'') AS procurement_method,
      COALESCE(m.lead_time_days,0) AS lead_time_days,
      COALESCE(m.min_order_qty,0) AS min_order_qty,
      COALESCE(m.multiple_qty,0) AS multiple_qty
    FROM sales
    LEFT JOIN item_group_cache g ON g.item_code = sales.item_code
    LEFT JOIN inv ON inv.item_code = sales.item_code
    LEFT JOIN production_mrp_cache m ON m.item_code = sales.item_code
    ORDER BY sales.revenue DESC
    `,
    [from, to]
  );

  const monthTo = String(to || getDateISOInOffset(TZ_OFFSET_MIN));
  const monthFrom = prodAddMonthsISO(`${String(monthTo).slice(0,7)}-01`, -(Math.max(1, Number(avgMonths || 5)) - 1));
  const monthRows = await dbQuery(
    `
    SELECT
      item_code,
      to_char(date_trunc('month', doc_date), 'YYYY-MM') AS ym,
      COALESCE(SUM(quantity),0)::numeric(18,4) AS qty
    FROM sales_item_lines
    WHERE doc_date >= $1::date AND doc_date <= $2::date
    GROUP BY item_code, to_char(date_trunc('month', doc_date), 'YYYY-MM')
    `,
    [monthFrom, monthTo]
  );
  const monthly = new Map();
  for (const r of monthRows.rows || []) {
    const key = String(r.item_code || "");
    if (!monthly.has(key)) monthly.set(key, new Map());
    monthly.get(key).set(String(r.ym || ""), prodNum(r.qty));
  }

  let items = (rows.rows || []).map((r) => {
    const rev = prodNum(r.revenue);
    const gp = prodNum(r.gp);
    const gpPct = rev > 0 ? (gp / rev) * 100 : 0;
    const grupoTxt = prodNormalizeGrupoFinal(String(r.grupo || "Sin grupo"));
    const areaFinal = String(r.area || "") || prodInferAreaFromGroup(grupoTxt) || "CONS";
    const monthsMap = monthly.get(String(r.item_code || "")) || new Map();

    const avgMonthsSafe = Math.max(1, Number(avgMonths || 5));
    const monthLabels = [];
    let sumMonths = 0;
    for (let i = avgMonthsSafe - 1; i >= 0; i--) {
      const ym = prodYm(prodAddMonthsISO(`${String(monthTo).slice(0,7)}-01`, -i));
      monthLabels.push(ym);
      sumMonths += prodNum(monthsMap.get(ym));
    }
    const avgQty = sumMonths / avgMonthsSafe;
    const projectedQty = avgQty * Math.max(1, Number(horizonMonths || 3));
    const effectiveProjectedQty = projectedQty;
    const stockTotal = prodNum(r.stock_total);
    const needed = Math.max(0, effectiveProjectedQty - stockTotal);
    const adjusted = prodRecommendPracticalQty({ neededQty: needed, avgMonthlyQty: avgQty, minOrderQty: r.min_order_qty, multipleQty: r.multiple_qty });

    return {
      itemCode: String(r.item_code || ""),
      itemDesc: String(r.item_desc || ""),
      grupo: grupoTxt,
      area: areaFinal,
      revenue: rev,
      gp,
      gpPct: prodRound(gpPct, 2),
      soldQty: prodNum(r.qty),
      avgMonthlyQty: prodRound(avgQty, 2),
      projectedQty: prodRound(effectiveProjectedQty, 2),
      stockTotal,
      wh01: prodNum(r.wh_01),
      wh200: prodNum(r.wh_200),
      wh300: prodNum(r.wh_300),
      wh500: prodNum(r.wh_500),
      stockMin: prodNum(r.stock_min),
      stockMax: prodNum(r.stock_max),
      procurementMethod: String(r.procurement_method || ""),
      procurementMethodLabel: prodProcurementMethodLabel(String(r.procurement_method || "")),
      leadTimeDays: prodNum(r.lead_time_days),
      minOrderQty: prodNum(r.min_order_qty),
      multipleQty: prodNum(r.multiple_qty),
      productionNeeded: prodRound(needed, 2),
      productionAdjusted: adjusted,
    };
  });

  const areaSel = String(area || "__ALL__");
  const grupoSel = String(grupo || "__ALL__");
  const qq = String(q || "").trim().toLowerCase();

  let availableGroups = [];
  if (areaSel === "CONS") availableGroups = Array.from(PROD_GROUPS_CONS);
  else if (areaSel === "RCI") availableGroups = Array.from(PROD_GROUPS_RCI);
  else availableGroups = Array.from(new Set([...PROD_GROUPS_CONS, ...PROD_GROUPS_RCI]));
  availableGroups.sort((a, b) => a.localeCompare(b));

  let universe = items.slice();
  if (areaSel !== "__ALL__") universe = universe.filter((x) => x.area === areaSel);
  if (grupoSel !== "__ALL__") universe = universe.filter((x) => prodNormGroupName(x.grupo) === prodNormGroupName(grupoSel));

  const abcRev = prodAbcByMetric(universe, "revenue");
  const abcGP = prodAbcByMetric(universe, "gp");
  const abcQty = prodAbcByMetric(universe, "avgMonthlyQty");

  items = items.map((it, idx) => {
    const a1 = abcRev.get(it.itemCode) || "D";
    const a2 = abcGP.get(it.itemCode) || "D";
    const a3 = abcQty.get(it.itemCode) || "D";
    const total = prodTotalFromLetters(a1, a2, a3);
    const local = loadProductionLocalData();
    const meta = local.formulas.products?.[it.itemCode] || null;
    const machine = prodMachineFromAreaOrGroup(it.area, it.grupo, meta);
    const rate = prodNum(local.capacity?.itemRates?.[it.itemCode] || local.capacity?.defaultRates?.[machine] || 0);
    const hoursNeeded = rate > 0 ? it.productionAdjusted / rate : 0;

    return {
      ...it,
      rankTotal: idx + 1,
      abcRevenue: a1,
      abcGP: a2,
      abcDemand: a3,
      totalLabel: total.label,
      totalTagClass: total.cls,
      totalScore: total.t,
      machine,
      unitsPerHour: rate,
      hoursNeeded: prodRound(hoursNeeded, 2),
      hasFormula: !!meta,
      baseLiquidCode: meta?.baseLiquidCode || "",
    };
  });

  if (areaSel !== "__ALL__") items = items.filter((x) => x.area === areaSel);
  if (grupoSel !== "__ALL__") items = items.filter((x) => prodNormGroupName(x.grupo) === prodNormGroupName(grupoSel));
  if (qq) items = items.filter((x) => x.itemCode.toLowerCase().includes(qq) || x.itemDesc.toLowerCase().includes(qq));

  items.sort((a, b) => {
    const d1 = (b.projectedQty || 0) - (a.projectedQty || 0);
    if (d1) return d1;
    const d2 = (b.productionNeeded || 0) - (a.productionNeeded || 0);
    if (d2) return d2;
    return (b.revenue || 0) - (a.revenue || 0);
  });

  const totals = items.reduce(
    (acc, x) => {
      acc.revenue += prodNum(x.revenue);
      acc.projectedQty += prodNum(x.projectedQty);
      acc.stockTotal += prodNum(x.stockTotal);
      acc.productionNeeded += prodNum(x.productionNeeded);
      acc.productionAdjusted += prodNum(x.productionAdjusted);
      acc.hoursNeeded += prodNum(x.hoursNeeded);
      if (x.productionNeeded > 0) acc.riskCount += 1;
      if (x.totalLabel === "AB Crítico") acc.abCount += 1;
      return acc;
    },
    { revenue: 0, projectedQty: 0, stockTotal: 0, productionNeeded: 0, productionAdjusted: 0, hoursNeeded: 0, riskCount: 0, abCount: 0 }
  );

  const machineAggMap = new Map();
  for (const it of items) {
    const cur = machineAggMap.get(it.machine) || { machine: it.machine, items: 0, productionAdjusted: 0, hoursNeeded: 0 };
    cur.items += 1;
    cur.productionAdjusted += prodNum(it.productionAdjusted);
    cur.hoursNeeded += prodNum(it.hoursNeeded);
    machineAggMap.set(it.machine, cur);
  }

  return {
    ok: true,
    from, to, area: areaSel, grupo: grupoSel, q: qq,
    avgMonths: Math.max(1, Number(avgMonths || 5)),
    horizonMonths: Math.max(1, Number(horizonMonths || 3)),
    lastSyncAt: await getState("production_last_sync_at"),
    availableGroups,
    totals: {
      revenue: prodRound(totals.revenue, 2),
      projectedQty: prodRound(totals.projectedQty, 2),
      stockTotal: prodRound(totals.stockTotal, 2),
      productionNeeded: prodRound(totals.productionNeeded, 2),
      productionAdjusted: prodRound(totals.productionAdjusted, 2),
      hoursNeeded: prodRound(totals.hoursNeeded, 2),
      riskCount: totals.riskCount,
      abCount: totals.abCount,
    },
    machineAgg: Array.from(machineAggMap.values()).map((x) => ({ ...x, hoursNeeded: prodRound(x.hoursNeeded, 2) })),
    items,
    capacityConfig: loadProductionLocalData().capacity || {},
  };
}

function prodMergeMaterial(map, code, description, qty, unit, type) {
  if (!code) return;
  const key = String(code).trim();
  const cur = map.get(key) || { code: key, description: description || "", requiredQty: 0, unit: unit || "", componentType: type || "" };
  cur.requiredQty += prodNum(qty);
  if (!cur.description) cur.description = description || "";
  if (!cur.unit) cur.unit = unit || "";
  if (!cur.componentType) cur.componentType = type || "";
  map.set(key, cur);
}


async function productionBuildItemPlan({ itemCode, toDate, avgMonths = 5, horizonMonths = 3, shiftHours = 8, plannedQtyOverride = 0 }) {
  const code = String(itemCode || "").trim();
  if (!code) throw new Error("Falta itemCode");

  const local = loadProductionLocalData();
  const meta = local.formulas.products?.[code] || local.formulas.products?.[String(code).replace(/^0+/, "")] || null;

  const itemMaster = await dbQuery(
    `
    SELECT
      COALESCE(MAX(NULLIF(s.item_desc,'')), MAX(NULLIF(g.item_desc,'')), '') AS item_desc,
      COALESCE(MAX(NULLIF(g.grupo,'')), MAX(NULLIF(g.group_name,'')), MAX(NULLIF(s.item_group,'')), 'Sin grupo') AS grupo,
      COALESCE(MAX(NULLIF(g.area,'')), MAX(NULLIF(s.area,'')), '') AS area
    FROM sales_item_lines s
    LEFT JOIN item_group_cache g ON g.item_code = s.item_code
    WHERE s.item_code = $1
    `,
    [code]
  );
  const row0 = itemMaster.rows?.[0] || {};

  const sapItem = await prodGetFullItem(code).catch(() => null);
  const itemDesc = String(row0.item_desc || sapItem?.ItemName || meta?.description || "");
  const grupo = prodNormalizeGrupoFinal(String(row0.grupo || ""));
  const area = String(row0.area || "") || prodInferAreaFromGroup(grupo) || "";
  const machine = prodMachineFromAreaOrGroup(area, grupo, meta);

  const end = String(toDate || getDateISOInOffset(TZ_OFFSET_MIN));
  const monthStart = `${String(end).slice(0, 7)}-01`;
  const histFrom = prodAddMonthsISO(monthStart, -11);

  const monthlyRows = await dbQuery(
    `
    SELECT to_char(date_trunc('month', doc_date), 'YYYY-MM') AS ym, COALESCE(SUM(quantity),0)::numeric(18,4) AS qty
    FROM sales_item_lines
    WHERE item_code = $1 AND doc_date >= $2::date AND doc_date <= $3::date
    GROUP BY 1
    ORDER BY 1
    `,
    [code, histFrom, end]
  );
  const monthMap = new Map((monthlyRows.rows || []).map((r) => [String(r.ym || ""), prodNum(r.qty)]));

  const weightedCost = prodExtractWeightedCostFromItem(sapItem);
  const prodOrders = await prodFetchProductionOrders(code, 120).catch(() => ({ orders: [], monthly: new Map() }));
  const prodMonthMap = prodOrders?.monthly instanceof Map ? prodOrders.monthly : new Map();

  const salesHistory = [];
  for (let i = 11; i >= 0; i--) {
    const ym = prodYm(prodAddMonthsISO(monthStart, -i));
    salesHistory.push({
      ym,
      label: prodFormatMonthName(ym),
      qty: prodRound(monthMap.get(ym) || 0, 2),
      producedQty: prodRound(prodMonthMap.get(ym) || 0, 2),
      weightedCost: prodRound(weightedCost || 0, 4),
    });
  }

  let avgQty = 0;
  for (let i = Math.max(1, Number(avgMonths || 5)) - 1; i >= 0; i--) {
    const ym = prodYm(prodAddMonthsISO(monthStart, -i));
    avgQty += prodNum(monthMap.get(ym) || 0);
  }
  avgQty = avgQty / Math.max(1, Number(avgMonths || 5));
  const projectedQty = avgQty * Math.max(1, Number(horizonMonths || 3));

  const invRows = await dbQuery(
    `SELECT warehouse, stock, stock_min, stock_max, committed, ordered, available
     FROM production_inv_wh_cache
     WHERE item_code = $1`,
    [code]
  );
  const byWh = { "01": 0, "200": 0, "300": 0, "500": 0 };
  let stockTotal = 0;
  let stockMin = 0;
  let stockMax = 0;
  for (const r of invRows.rows || []) {
    const wh = String(r.warehouse || "").trim();
    if (Object.prototype.hasOwnProperty.call(byWh, wh)) byWh[wh] = prodNum(r.stock);
    stockTotal += prodNum(r.stock);
    stockMin = Math.max(stockMin, prodNum(r.stock_min));
    stockMax = Math.max(stockMax, prodNum(r.stock_max));
  }
  if ((!invRows.rows || !invRows.rows.length) && sapItem) {
    const sapInv = prodExtractInventorySnapshotFromItem(sapItem);
    stockTotal = sapInv.total;
    stockMin = sapInv.stockMin;
    stockMax = sapInv.stockMax;
    Object.assign(byWh, sapInv.byWarehouse || {});
  }

  const mrpRows = await dbQuery(
    `SELECT procurement_method, lead_time_days, min_order_qty, multiple_qty, planning_system
     FROM production_mrp_cache WHERE item_code = $1 LIMIT 1`,
    [code]
  );
  const mrpFromDb = mrpRows.rows?.[0] || {};
  const mrpFromSap = prodExtractMrpFromItem(sapItem || {});
  const mrp = {
    procurementMethod: String(mrpFromDb.procurement_method || mrpFromSap.procurementMethod || ""),
    planningSystem: String(mrpFromDb.planning_system || mrpFromSap.planningSystem || ""),
    leadTimeDays: prodNum(mrpFromDb.lead_time_days || mrpFromSap.leadTimeDays),
    minOrderQty: prodNum(mrpFromDb.min_order_qty || mrpFromSap.minOrderQty),
    multipleQty: prodNum(mrpFromDb.multiple_qty || mrpFromSap.multipleQty),
  };

  const manualPlanQty = Math.max(0, prodNum(plannedQtyOverride));
  const effectiveProjectedQty = manualPlanQty > 0 ? manualPlanQty : projectedQty;
  const productionNeeded = manualPlanQty > 0 ? manualPlanQty : Math.max(0, projectedQty - stockTotal);
  const mrpAdjustedQty = prodApplyMrp(productionNeeded, mrp.minOrderQty, mrp.multipleQty);
  const productionAdjusted = prodRecommendPracticalQty({
    neededQty: productionNeeded,
    avgMonthlyQty: avgQty > 0 ? avgQty : (manualPlanQty > 0 ? manualPlanQty : 0),
    minOrderQty: mrp.minOrderQty,
    multipleQty: mrp.multipleQty,
  });

  const sapBom = await prodFetchSapBom(code).catch(() => ({ source: "SAP ProductTrees", tree: null, headerQty: 1, lines: [] }));
  let requirementPack = await prodBuildRequirementsFromSapBom({ itemCode: code, adjustedQty: productionAdjusted, sapBom }).catch(() => null);

  let litersPerUnit = 0;
  let baseLiquidCode = "";
  let litersRequired = 0;
  let materialSource = "SAP producción · ProductTrees";
  let usedLocalFallback = false;

  if (!requirementPack || !Array.isArray(requirementPack.all) || !requirementPack.all.length) {
    const fallbackReqMap = new Map();
    const fallbackMeta = meta || null;
    litersPerUnit = prodNum(fallbackMeta?.litersPerUnit || prodInferLitersPerUnit(itemDesc));
    baseLiquidCode = String(fallbackMeta?.baseLiquidCode || "");
    const baseLiquidFormula = fallbackMeta?.baseLiquidFormula || (baseLiquidCode ? local.formulas.liquids?.[baseLiquidCode] : null);
    litersRequired = productionAdjusted * litersPerUnit;

    const topLevel = Array.isArray(fallbackMeta?.components) ? fallbackMeta.components : [];
    for (const c of topLevel) {
      const qty = prodNum(c.qtyPerUnit) * productionAdjusted;
      if (String(c.componentType || "") === "LIQUID_BASE") continue;
      prodMergeMaterial(fallbackReqMap, c.code, c.description, qty, c.unit, c.componentType);
    }
    if (baseLiquidFormula && Array.isArray(baseLiquidFormula.components)) {
      for (const c of baseLiquidFormula.components) {
        const qty = prodNum(c.qtyPerLiter) * litersRequired;
        prodMergeMaterial(fallbackReqMap, c.code, c.description, qty, c.unit, "RAW_MATERIAL");
      }
    }

    const requirements = Array.from(fallbackReqMap.values()).map((x) => {
      const stockRec = local.materials?.materials?.[x.code] || null;
      const stockQty = prodNum(stockRec?.stockQty);
      const shortage = Math.max(0, x.requiredQty - stockQty);
      const coverage = x.requiredQty > 0 ? stockQty / x.requiredQty : 0;
      return {
        ...x,
        requiredQty: prodRound(x.requiredQty, 3),
        stockQty: prodRound(stockQty, 3),
        shortageQty: prodRound(shortage, 3),
        coveragePct: prodRound(coverage * 100, 1),
        supplier: String(stockRec?.supplier || ""),
        cost: prodRound(stockRec?.cost, 4),
        status: shortage > 0 ? "FALTANTE" : "OK",
      };
    });

    requirementPack = {
      source: "Catálogo local de respaldo",
      all: requirements,
      rawMaterials: requirements.filter((x) => x.componentType === "RAW_MATERIAL"),
      packaging: requirements.filter((x) => x.componentType === "PACKAGING"),
      resources: requirements.filter((x) => x.componentType === "RESOURCE"),
      bottlenecks: requirements.filter((x) => x.shortageQty > 0 && x.componentType !== "RESOURCE").sort((a, b) => b.shortageQty - a.shortageQty).slice(0, 10),
      bomHeaderQty: 1,
      bomLines: [],
    };
    materialSource = "Catálogo local de respaldo";
    usedLocalFallback = true;
  } else {
    materialSource = requirementPack.source || "SAP producción · ProductTrees";
    litersPerUnit = prodNum(meta?.litersPerUnit || 0);
    baseLiquidCode = String(meta?.baseLiquidCode || "");
    litersRequired = prodRound(litersPerUnit * productionAdjusted, 3);
  }

  const requirements = Array.isArray(requirementPack?.all) ? requirementPack.all : [];
  const rawMaterials = Array.isArray(requirementPack?.rawMaterials) ? requirementPack.rawMaterials : [];
  const packaging = Array.isArray(requirementPack?.packaging) ? requirementPack.packaging : [];
  const resources = Array.isArray(requirementPack?.resources) ? requirementPack.resources : [];
  const bottlenecks = Array.isArray(requirementPack?.bottlenecks) ? requirementPack.bottlenecks : [];

  const rate = prodNum(local.capacity?.itemRates?.[code] || local.capacity?.defaultRates?.[machine] || 0);
  const hoursNeeded = rate > 0 ? productionAdjusted / rate : 0;
  const planStart = addDaysISO(end, 1);
  const planEnd = addDaysISO(prodAddMonthsISO(planStart, Math.max(1, Number(horizonMonths || 3))), -1);
  const businessDays = prodCountDays(planStart, planEnd, local.capacity?.workdays || [1, 2, 3, 4, 5]);
  const saturdays = prodCountSaturdays(planStart, planEnd);
  const shiftHoursSafe = prodNum(shiftHours || local.capacity?.shiftHours || 8, 8);
  const singleCapHours = businessDays * shiftHoursSafe;
  const saturdayCapHours = (businessDays + saturdays) * shiftHoursSafe;
  const doubleShiftHours = businessDays * shiftHoursSafe * 2;

  let laborRecommendation = "Capacidad normal";
  if (hoursNeeded <= singleCapHours) laborRecommendation = "Con turno normal lunes a viernes alcanza";
  else if (hoursNeeded <= saturdayCapHours) laborRecommendation = "Se recomienda agregar sábados";
  else if (hoursNeeded <= doubleShiftHours) laborRecommendation = "Se recomienda doble turno";
  else laborRecommendation = "Se recomienda doble turno + sábado o reprogramar";

  const maxUnitsByMaterial = requirements
    .filter((x) => x.requiredQty > 0 && x.componentType !== "RESOURCE")
    .map((x) => {
      const perUnit = productionAdjusted > 0 ? x.requiredQty / productionAdjusted : 0;
      return perUnit > 0 ? prodNum(x.stockQty) / perUnit : Number.POSITIVE_INFINITY;
    })
    .filter((n) => Number.isFinite(n));
  const maxUnitsToday = maxUnitsByMaterial.length ? Math.floor(Math.min(...maxUnitsByMaterial)) : 0;

  const costing = {
    weightedCost: prodRound(weightedCost || 0, 4),
    projectedDemandCost: prodRound((weightedCost || 0) * effectiveProjectedQty, 2),
    adjustedProductionCost: prodRound((weightedCost || 0) * productionAdjusted, 2),
    stockValue: prodRound((weightedCost || 0) * stockTotal, 2),
  };

  return {
    ok: true,
    itemCode: code,
    itemDesc,
    grupo,
    area,
    machine,
    abcHint: "",
    period: {
      endDate: end,
      avgMonths: Math.max(1, Number(avgMonths || 5)),
      horizonMonths: Math.max(1, Number(horizonMonths || 3)),
      planStart,
      planEnd
    },
    salesHistory,
    recentProductionOrders: (prodOrders?.orders || []).slice(0, 20),
    costing,
    avgMonthlyQty: prodRound(avgQty, 2),
    projectedQty: prodRound(projectedQty, 2),
    inventory: {
      total: prodRound(stockTotal, 2),
      byWarehouse: byWh,
      stockMin: prodRound(stockMin, 2),
      stockMax: prodRound(stockMax, 2),
    },
    mrp: {
      procurementMethod: mrp.procurementMethod,
      procurementMethodLabel: prodProcurementMethodLabel(mrp.procurementMethod),
      planningSystem: mrp.planningSystem,
      leadTimeDays: prodNum(mrp.leadTimeDays),
      minOrderQty: prodNum(mrp.minOrderQty),
      multipleQty: prodNum(mrp.multipleQty),
    },
    production: {
      litersPerUnit: prodRound(litersPerUnit, 6),
      baseLiquidCode,
      litersRequired: prodRound(litersRequired, 3),
      neededQty: prodRound(productionNeeded, 2),
      mrpAdjustedQty: prodRound(mrpAdjustedQty, 2),
      adjustedQty: prodRound(productionAdjusted, 2),
      manualPlanQty: prodRound(manualPlanQty, 3),
      planBasis: manualPlanQty > 0 ? "SUBPLAN_COMPONENTE" : "DEMANDA_PROYECTADA",
      maxUnitsToday,
      practicalRule: "Se recomienda un lote práctico mínimo de 1.5 meses promedio cuando el MRP quede demasiado corto.",
    },
    capacity: {
      unitsPerHour: prodRound(rate, 2),
      hoursNeeded: prodRound(hoursNeeded, 2),
      shiftHours: shiftHoursSafe,
      businessDays,
      saturdays,
      singleCapHours: prodRound(singleCapHours, 2),
      saturdayCapHours: prodRound(saturdayCapHours, 2),
      doubleShiftHours: prodRound(doubleShiftHours, 2),
      laborRecommendation,
      machineLabel: local.capacity?.machineNames?.[machine] || machine,
    },
    requirements: {
      all: requirements,
      rawMaterials,
      packaging,
      resources,
      bottlenecks,
    },
    sapProduction: {
      source: materialSource,
      bomHeaderQty: prodRound(requirementPack?.bomHeaderQty || 1, 3),
      bomLinesCount: Array.isArray(requirementPack?.bomLines) ? requirementPack.bomLines.length : 0,
      usingLocalFallback: usedLocalFallback,
    },
    meta: meta || null,
    source: usedLocalFallback
      ? "base de datos sincronizada + catálogo local de respaldo"
      : "base de datos sincronizada + SAP producción (ProductTrees / órdenes de fabricación)",
  };
}

function prodNormalizeItemCodeLoose(v) {
  const raw = String(v || "").trim().toUpperCase();
  if (!raw) return "";
  const m = raw.match(/^0*([0-9]+)(-[A-Z0-9]+)?$/i);
  if (m) return `${m[1]}${m[2] || ""}`;
  return raw.replace(/^0+(\d)/, "$1");
}

function prodExtractCodesFromText(text) {
  const src = String(text || "").toUpperCase();
  const matches = src.match(/\b\d{3,6}(?:-[A-Z0-9]+)?\b/g) || [];
  const out = [];
  const seen = new Set();
  for (const code of matches) {
    const normalized = prodNormalizeItemCodeLoose(code);
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    out.push(String(code || "").trim());
  }
  return out;
}

function prodFindDashboardItemByCode(items, code) {
  const wanted = String(code || "").trim();
  if (!wanted) return null;
  const wantedLoose = prodNormalizeItemCodeLoose(wanted);
  for (const item of Array.isArray(items) ? items : []) {
    const itemCode = String(item?.itemCode || "").trim();
    if (!itemCode) continue;
    if (itemCode.toUpperCase() === wanted.toUpperCase()) return item;
    if (prodNormalizeItemCodeLoose(itemCode) === wantedLoose) return item;
  }
  return null;
}

function prodResolveRequestedCodes({ question = "", q = "", itemCode = "", dashboard = null }) {
  const items = Array.isArray(dashboard?.items) ? dashboard.items : [];
  const out = [];
  const seen = new Set();
  const pushCode = (code) => {
    const found = prodFindDashboardItemByCode(items, code);
    const finalCode = String(found?.itemCode || code || "").trim();
    const key = prodNormalizeItemCodeLoose(finalCode);
    if (!finalCode || !key || seen.has(key)) return;
    seen.add(key);
    out.push(finalCode);
  };

  if (itemCode) pushCode(itemCode);

  const qTrim = String(q || "").trim();
  if (qTrim) {
    const direct = prodFindDashboardItemByCode(items, qTrim);
    if (direct) pushCode(direct.itemCode);
  }

  for (const code of prodExtractCodesFromText(`${String(question || "")} ${String(q || "")}`)) {
    pushCode(code);
  }

  return out;
}

function prodQuestionNeedsUrgentAbList(question) {
  const q = String(question || "").toLowerCase();
  return /(productos?|art[ií]culos?|tabla|lista|ranking)/.test(q) && /ab/.test(q) && /(urgente|cr[ií]tic|riesgo)/.test(q) && /stock/.test(q);
}

function prodQuestionNeedsPlanDetails(question) {
  const q = String(question || "").toLowerCase();
  return /(plan de producci[oó]n|materia prima|materias primas|empaque|empaques|cuello|botella|orden(?:es)? de producci[oó]n|costo ponderado|fabricar)/.test(q);
}

function prodCompactItemForAi(x) {
  const stockTotal = prodNum(x?.stockTotal);
  const stockMin = prodNum(x?.stockMin);
  const stockMax = prodNum(x?.stockMax);
  const productionNeeded = prodNum(x?.productionNeeded);
  const gapToMin = stockMin > stockTotal ? prodRound(stockMin - stockTotal, 2) : 0;
  return {
    itemCode: x?.itemCode || "",
    itemDesc: x?.itemDesc || "",
    grupo: x?.grupo || "",
    area: x?.area || "",
    totalLabel: x?.totalLabel || "",
    avgMonthlyQty: prodNum(x?.avgMonthlyQty),
    projectedQty: prodNum(x?.projectedQty),
    stockTotal,
    stockMin,
    stockMax,
    gapToMin,
    productionNeeded,
    productionAdjusted: prodNum(x?.productionAdjusted),
    machine: x?.machine || "",
    hoursNeeded: prodNum(x?.hoursNeeded),
    leadTimeDays: prodNum(x?.leadTimeDays),
    belowMin: stockMin > 0 ? stockTotal < stockMin : productionNeeded > 0,
    aboveMax: stockMax > 0 ? stockTotal > stockMax : false,
  };
}

function prodBuildUrgentAbStockItems(items) {
  return (Array.isArray(items) ? items : [])
    .filter((x) => String(x?.totalLabel || "").startsWith("AB") && prodNum(x?.stockMin) > 0 && prodNum(x?.stockTotal) < prodNum(x?.stockMin))
    .map((x) => ({
      ...prodCompactItemForAi(x),
      urgencyPctVsMin: prodRound((prodNum(x.stockMin) - prodNum(x.stockTotal)) / Math.max(prodNum(x.stockMin), 1) * 100, 1),
    }))
    .sort((a, b) => {
      const d1 = prodNum(b.urgencyPctVsMin) - prodNum(a.urgencyPctVsMin);
      if (d1) return d1;
      const d2 = prodNum(b.gapToMin) - prodNum(a.gapToMin);
      if (d2) return d2;
      return prodNum(b.productionNeeded) - prodNum(a.productionNeeded);
    });
}

function prodAiCompactDashboard(data) {
  const items = Array.isArray(data?.items) ? data.items : [];
  const compactItems = items.slice(0, 160).map(prodCompactItemForAi);
  const urgentAbStockRiskItems = prodBuildUrgentAbStockItems(items).slice(0, 80);
  return {
    filters: {
      from: data?.from || "",
      to: data?.to || "",
      area: data?.area || "__ALL__",
      grupo: data?.grupo || "__ALL__",
      q: data?.q || "",
      avgMonths: data?.avgMonths || 5,
      horizonMonths: data?.horizonMonths || 3,
    },
    totals: data?.totals || {},
    machineAgg: data?.machineAgg || [],
    filteredItemsCount: items.length,
    filteredItems: compactItems,
    urgentAbStockRiskItems,
  };
}
function prodAiCompactPlan(plan) {
  if (!plan) return null;

  const rawMaterials = Array.isArray(plan.requirements?.rawMaterials)
    ? plan.requirements.rawMaterials
    : [];

  const packaging = Array.isArray(plan.requirements?.packaging)
    ? plan.requirements.packaging
    : [];

  const bottlenecks = Array.isArray(plan.requirements?.bottlenecks)
    ? plan.requirements.bottlenecks
    : [];

  const resources = Array.isArray(plan.requirements?.resources)
    ? plan.requirements.resources
    : [];

  return {
    itemCode: plan.itemCode,
    itemDesc: plan.itemDesc,
    grupo: plan.grupo,
    area: plan.area,
    machine: plan.machine,
    period: plan.period,

    avgMonthlyQty: plan.avgMonthlyQty,
    projectedQty: plan.projectedQty,
    salesHistory: (plan.salesHistory || []).map((x) => ({
      ym: x.ym,
      label: x.label,
      qty: x.qty,
      producedQty: x.producedQty || 0,
      weightedCost: x.weightedCost || 0,
    })),
    costing: plan.costing || {},
    recentProductionOrders: (plan.recentProductionOrders || []).slice(0, 20).map((x) => ({
      docNum: x.docNum,
      postDate: x.postDate,
      plannedQty: x.plannedQty,
      completedQty: x.completedQty,
      status: x.status,
      warehouse: x.warehouse,
    })),

    inventory: plan.inventory,
    mrp: plan.mrp,
    production: plan.production,
    capacity: plan.capacity,
    sapProduction: plan.sapProduction || {},

    formula: {
      litersPerUnit: plan.production?.litersPerUnit || 0,
      baseLiquidCode: plan.production?.baseLiquidCode || "",
      litersRequired: plan.production?.litersRequired || 0,
      neededQty: plan.production?.neededQty || 0,
      mrpAdjustedQty: plan.production?.mrpAdjustedQty || 0,
      adjustedQty: plan.production?.adjustedQty || 0
    },

    requirements: {
      hasRawMaterials: rawMaterials.length > 0,
      hasPackaging: packaging.length > 0,
      hasBottlenecks: bottlenecks.length > 0,
      hasResources: resources.length > 0,

      rawMaterialsCount: rawMaterials.length,
      packagingCount: packaging.length,
      bottlenecksCount: bottlenecks.length,
      resourcesCount: resources.length,

      rawMaterials,
      packaging,
      bottlenecks,
      resources
    }
  };
}
function prodExtractResponseText(obj) {
  if (!obj || typeof obj !== "object") return "";
  if (typeof obj.output_text === "string" && obj.output_text.trim()) return obj.output_text.trim();

  const out = [];
  for (const item of Array.isArray(obj.output) ? obj.output : []) {
    if (Array.isArray(item.content)) {
      for (const c of item.content) {
        if (typeof c?.text === "string" && c.text.trim()) out.push(c.text.trim());
        else if (typeof c?.text?.value === "string" && c.text.value.trim()) out.push(c.text.value.trim());
      }
    }
  }
  return out.join("\n\n").trim();
}
async function prodOpenAiChat({ question, dashboard, plan, plans = [], requestedCodes = [], questionMatches = [] }) {
  const apiKey = String(process.env.OPENAI_API_KEY || "").trim();
  const model = String(process.env.OPENAI_MODEL || "gpt-5-mini").trim();
  if (!apiKey) throw new Error("OPENAI_API_KEY no configurada");

  const input = {
    dashboard: prodAiCompactDashboard(dashboard),
    requestedCodes: Array.isArray(requestedCodes) ? requestedCodes : [],
    questionMatches: (Array.isArray(questionMatches) ? questionMatches : []).map(prodCompactItemForAi),
    selectedPlan: prodAiCompactPlan(plan),
    requestedPlans: (Array.isArray(plans) ? plans : []).map((x) => prodAiCompactPlan(x)).filter(Boolean).slice(0, 8),
  };

  const system = [
    "Eres un planificador de producción interno de PRODIMA.",
    "Usa exclusivamente el JSON entregado como fuente de verdad.",
    "La fuente combina base de datos sincronizada (ventas, inventario terminado, MRP) con SAP producción (ProductTrees y órdenes de fabricación).",
    "Solo si el JSON lo indica explícitamente, puedes mencionar que hubo respaldo desde catálogo local.",
    "No inventes datos que no estén en el JSON.",
    "Responde en español.",
    "IMPORTANTE: las cantidades de producto terminado siempre se expresan en UNIDADES, nunca en cajas.",
    "IMPORTANTE: cuando exista sapProduction.source, menciónalo para dejar claro si los materiales vienen directo de SAP producción.",
    "IMPORTANTE: cuando existan salesHistory.producedQty o recentProductionOrders, esos son los datos válidos para responder cuánto se produjo el artículo por mes o en órdenes recientes.",
    "IMPORTANTE: cuando exista costing.weightedCost, úsalo como costo ponderado unitario del artículo y analiza también stockValue, projectedDemandCost y adjustedProductionCost.",
    "IMPORTANTE: si production.mrpAdjustedQty y production.adjustedQty son distintos, explica que el sistema elevó la recomendación a un lote práctico para evitar producir cantidades demasiado cortas y poco eficientes.",
    "IMPORTANTE: si requirements.rawMaterials, requirements.packaging, requirements.resources o requirements.bottlenecks existen, debes analizarlos y mencionarlos explícitamente.",
    "IMPORTANTE: los registros en requirements.resources son RECURSOS internos (operarios, supervisoras, líneas, gastos de fabricación) y no deben tratarse como faltantes de inventario.",
    "IMPORTANTE: usa procurementMethodLabel para explicar el método con lenguaje humano: 'Se fabrica en planta' o 'No se fabrica en planta'.",
    "IMPORTANTE: si el usuario compara varios planes, identifica si los mismos resources aparecen en más de un lote y advierte posibles choques de capacidad.",
    "IMPORTANTE: para riesgo de stock urgente usa SOLO dashboard.urgentAbStockRiskItems o el criterio estricto stockTotal < stockMin. No incluyas ítems en o por encima del stock mínimo, y nunca marques como urgentes artículos por encima del máximo.",
    "IMPORTANTE: aunque no haya artículo seleccionado, si la pregunta menciona códigos o el buscador coincide, debes usar requestedCodes, questionMatches y requestedPlans. No digas que un código no aparece en el JSON si existe en requestedPlans, questionMatches o filteredItems.",
    "IMPORTANTE: selectedPlan es el artículo seleccionado. requestedPlans puede traer varios planes completos sin selección manual; úsalo cuando el usuario pida varios productos o un código escrito en la pregunta.",
    "Si el usuario pide tabla, excel, ranking, lista, columnas o detalle por mes/material/orden, responde con una tabla markdown completa con encabezados claros y datos exactos.",
    "Evita responder con slash (/), listas planas o pseudo-tablas.",
    "Cuando el usuario pregunte por un plan de producción, responde como dashboard ejecutivo con: 1) Demanda y proyección 2) Inventario y cobertura 3) Producción necesaria, MRP y lote práctico 4) Materias primas 5) Empaques 6) Cuellos de botella 7) Capacidad y turnos 8) Conclusión con acciones.",
    "Si realmente falta información en el JSON, dilo claramente.",
  ].join(" ");

  const payload = {
    model,
    input: [
      { role: "system", content: [{ type: "input_text", text: system }] },
      {
        role: "user",
        content: [{ type: "input_text", text: `Pregunta:
${String(question || "").trim()}

Contexto JSON:
${JSON.stringify(input)}` }],
      },
    ],
    text: { format: { type: "text" } },
    max_output_tokens: 1200,
  };

  const resp = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${apiKey}` },
    body: JSON.stringify(payload),
  });
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    throw new Error(data?.error?.message || data?.message || `OpenAI HTTP ${resp.status}`);
  }
  const answer = prodExtractResponseText(data);
  if (!answer) throw new Error("OpenAI respondió sin texto utilizable.");
  return { answer, model };
}

app.get("/api/admin/production/dashboard", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = String(req.query?.from || "2025-01-01");
    const to = String(req.query?.to || today);
    const area = String(req.query?.area || "__ALL__");
    const grupo = String(req.query?.grupo || "__ALL__");
    const q = String(req.query?.q || "");
    const avgMonths = Math.max(1, Math.min(12, prodNum(req.query?.avgMonths, 5)));
    const horizonMonths = Math.max(1, Math.min(12, prodNum(req.query?.horizonMonths, 3)));

    const out = await productionDashboardFromDb({ from, to, area, grupo, q, avgMonths, horizonMonths });
    return safeJson(res, 200, out);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get("/api/admin/production/item-plan", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });
    const itemCode = String(req.query?.itemCode || "").trim();
    if (!itemCode) return safeJson(res, 400, { ok: false, message: "Falta itemCode" });

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const toDate = String(req.query?.toDate || today);
    const avgMonths = Math.max(1, Math.min(12, prodNum(req.query?.avgMonths, 5)));
    const horizonMonths = Math.max(1, Math.min(12, prodNum(req.query?.horizonMonths, 3)));
    const shiftHours = Math.max(1, Math.min(24, prodNum(req.query?.shiftHours, 8)));
    const plannedQty = Math.max(0, prodNum(req.query?.plannedQty, 0));

    const plan = await productionBuildItemPlan({ itemCode, toDate, avgMonths, horizonMonths, shiftHours, plannedQtyOverride: plannedQty });

    const dash = await productionDashboardFromDb({
      from: "2025-01-01",
      to: today,
      area: "__ALL__",
      grupo: "__ALL__",
      q: itemCode,
      avgMonths,
      horizonMonths,
    });
    const row = (dash.items || []).find((x) => String(x.itemCode || "") === itemCode);
    if (row) plan.abcHint = row.totalLabel || "";

    return safeJson(res, 200, plan);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.post("/api/admin/production/ai-chat", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });
    const question = String(req.body?.question || "").trim();
    if (!question) return safeJson(res, 400, { ok: false, message: "Falta question" });

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = String(req.body?.from || "2025-01-01");
    const to = String(req.body?.to || today);
    const area = String(req.body?.area || "__ALL__");
    const grupo = String(req.body?.grupo || "__ALL__");
    const q = String(req.body?.q || "");
    const avgMonths = Math.max(1, Math.min(12, prodNum(req.body?.avgMonths, 5)));
    const horizonMonths = Math.max(1, Math.min(12, prodNum(req.body?.horizonMonths, 3)));
    const shiftHours = Math.max(1, Math.min(24, prodNum(req.body?.shiftHours, 8)));
    const itemCode = String(req.body?.itemCode || "").trim();

    const dashboard = await productionDashboardFromDb({ from, to, area, grupo, q, avgMonths, horizonMonths });
    const requestedCodes = prodResolveRequestedCodes({ question, q, itemCode, dashboard });
    const questionMatches = requestedCodes.map((code) => prodFindDashboardItemByCode(dashboard?.items || [], code)).filter(Boolean);

    const planCodes = requestedCodes.slice(0, 8);
    const plans = [];
    for (const code of planCodes) {
      try {
        const built = await productionBuildItemPlan({ itemCode: code, toDate: to, avgMonths, horizonMonths, shiftHours });
        const row = prodFindDashboardItemByCode(dashboard?.items || [], code);
        if (row) built.abcHint = row.totalLabel || "";
        plans.push(built);
      } catch {}
    }

    if (!plans.length && prodQuestionNeedsPlanDetails(question) && prodQuestionNeedsUrgentAbList(question)) {
      const fallbackCodes = prodBuildUrgentAbStockItems(dashboard?.items || []).slice(0, 5).map((x) => x.itemCode);
      for (const code of fallbackCodes) {
        try {
          const built = await productionBuildItemPlan({ itemCode: code, toDate: to, avgMonths, horizonMonths, shiftHours });
          const row = prodFindDashboardItemByCode(dashboard?.items || [], code);
          if (row) built.abcHint = row.totalLabel || "";
          plans.push(built);
        } catch {}
      }
    }

    const plan = plans[0] || null;
    const out = await prodOpenAiChat({ question, dashboard, plan, plans, requestedCodes, questionMatches });
    const source = plans[0]?.source || plan?.source || "base de datos sincronizada + SAP producción";
    return safeJson(res, 200, { ok: true, answer: out.answer, model: out.model, source, requestedCodes, matchedPlans: plans.map((x) => x.itemCode) });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

async function handleProductionSync(req, res) {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const mode = String(req.body?.mode || req.query?.mode || "range");
    let from = String(req.body?.from || req.query?.from || "2025-01-01");
    let to = String(req.body?.to || req.query?.to || today);

    if (mode === "recent") {
      const days = Math.max(1, Math.min(120, prodNum(req.body?.days || req.query?.days, 30)));
      from = addDaysISO(today, -days);
      to = today;
    }

    const maxDocs = Math.max(50, Math.min(20000, prodNum(req.body?.maxDocs || req.query?.maxDocs, 4000)));

    const syncErrors = [];

    const salesSaved = await globalThis.syncSales({ from, to, maxDocs });

    let groupsSaved = 0;
    try {
      groupsSaved = await syncItemGroupsForSalesItems({ from, to, maxItems: 3000 });
    } catch (e) {
      syncErrors.push({ step: "item_groups", message: e.message || String(e) });
    }

    let invSaved = 0;
    try {
      invSaved = await syncInventoryForSalesItems({ from, to, maxItems: 3000 });
    } catch (e) {
      syncErrors.push({ step: "inventory_total", message: e.message || String(e) });
    }

    let invWhSaved = 0;
    let invWhErrors = [];
    try {
      const invWh = await syncProductionInventoryWh({ from, to, maxItems: 3000 });
      invWhSaved = Number(invWh?.saved || 0);
      invWhErrors = Array.isArray(invWh?.errors) ? invWh.errors : [];
      if (invWhErrors.length) syncErrors.push({ step: "inventory_wh", message: `items con error: ${invWhErrors.length}`, sample: invWhErrors.slice(0, 5) });
    } catch (e) {
      syncErrors.push({ step: "inventory_wh", message: e.message || String(e) });
    }

    let mrpSaved = 0;
    try {
      mrpSaved = await syncProductionMrp({ from, to, maxItems: 3000 });
    } catch (e) {
      syncErrors.push({ step: "mrp", message: e.message || String(e) });
    }

    await setState("production_last_sync_at", new Date().toISOString());

    return safeJson(res, 200, {
      ok: true,
      from, to, maxDocs,
      salesSaved, groupsSaved, invSaved, invWhSaved, mrpSaved,
      syncErrors,
      formulasLoaded: Object.keys(loadProductionLocalData().formulas?.products || {}).length,
      materialsLoaded: Object.keys(loadProductionLocalData().materials?.materials || {}).length,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
}
app.get("/api/admin/production/sync", verifyAdmin, handleProductionSync);
app.post("/api/admin/production/sync", verifyAdmin, handleProductionSync);

app.get("/api/admin/production/health", verifyAdmin, async (_req, res) => {
  try {
    const local = loadProductionLocalData();
    return safeJson(res, 200, {
      ok: true,
      formulasLoaded: Object.keys(local.formulas?.products || {}).length,
      materialsLoaded: Object.keys(local.materials?.materials || {}).length,
      capacityLoaded: Object.keys(local.capacity || {}).length,
      lastSyncAt: await getState("production_last_sync_at"),
      dataDir: PROD_DATA_DIR,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================================================
   IA — Estratificación (restaurada + super prompt)
========================================================= */

function estratCompactDashboard(data) {
  const items = Array.isArray(data?.items) ? data.items : [];
  const groupAgg = Array.isArray(data?.groupAgg) ? data.groupAgg : [];

  const counts = items.reduce(
    (acc, x) => {
      const lab = String(x.totalLabel || "Sin clasificar");
      acc.totalItems += 1;
      acc.byClass[lab] = (acc.byClass[lab] || 0) + 1;
      if (Number(x.stock || 0) < Number(x.stockMin || 0)) acc.stockBelowMin += 1;
      if (Number(x.stockMax || 0) > 0 && Number(x.stock || 0) >= Number(x.stockMax || 0)) acc.stockAtOrOverMax += 1;
      if (Number(x.stock || 0) <= 0) acc.stockZeroOrNegative += 1;
      return acc;
    },
    { totalItems: 0, stockBelowMin: 0, stockAtOrOverMax: 0, stockZeroOrNegative: 0, byClass: {} }
  );

  const topByRevenue = items.slice(0, 25).map((x) => ({
    rankTotal: x.rankTotal,
    rankArea: x.rankArea,
    itemCode: x.itemCode,
    itemDesc: x.itemDesc,
    area: x.area,
    grupo: x.grupo,
    revenue: x.revenue,
    gp: x.gp,
    gpPct: x.gpPct,
    totalLabel: x.totalLabel,
    abcRevenue: x.abcRevenue,
    abcGP: x.abcGP,
    abcGPPct: x.abcGPPct,
    stock: x.stock,
    stockMin: x.stockMin,
    stockMax: x.stockMax,
    available: x.available,
    committed: x.committed,
    ordered: x.ordered,
  }));

  const belowMin = items
    .filter((x) => Number(x.stock || 0) < Number(x.stockMin || 0))
    .slice(0, 25)
    .map((x) => ({
      itemCode: x.itemCode,
      itemDesc: x.itemDesc,
      area: x.area,
      grupo: x.grupo,
      totalLabel: x.totalLabel,
      revenue: x.revenue,
      gp: x.gp,
      gpPct: x.gpPct,
      stock: x.stock,
      stockMin: x.stockMin,
      stockMax: x.stockMax,
      available: x.available,
      committed: x.committed,
      ordered: x.ordered,
      faltanteVsMin: estratAiNum(Number(x.stockMin || 0) - Number(x.stock || 0), 2),
    }));

  const atOrOverMax = items
    .filter((x) => Number(x.stockMax || 0) > 0 && Number(x.stock || 0) >= Number(x.stockMax || 0))
    .slice(0, 25)
    .map((x) => ({
      itemCode: x.itemCode,
      itemDesc: x.itemDesc,
      area: x.area,
      grupo: x.grupo,
      totalLabel: x.totalLabel,
      revenue: x.revenue,
      gp: x.gp,
      gpPct: x.gpPct,
      stock: x.stock,
      stockMin: x.stockMin,
      stockMax: x.stockMax,
      available: x.available,
      committed: x.committed,
      ordered: x.ordered,
      excesoVsMax: estratAiNum(Number(x.stock || 0) - Number(x.stockMax || 0), 2),
    }));

  const highestMargin = [...items]
    .filter((x) => Number(x.revenue || 0) > 0)
    .sort((a, b) => Number(b.gpPct || 0) - Number(a.gpPct || 0))
    .slice(0, 20)
    .map((x) => ({
      itemCode: x.itemCode,
      itemDesc: x.itemDesc,
      area: x.area,
      grupo: x.grupo,
      revenue: x.revenue,
      gp: x.gp,
      gpPct: x.gpPct,
      totalLabel: x.totalLabel,
    }));

  const lowestMargin = [...items]
    .filter((x) => Number(x.revenue || 0) > 0)
    .sort((a, b) => Number(a.gpPct || 0) - Number(b.gpPct || 0))
    .slice(0, 20)
    .map((x) => ({
      itemCode: x.itemCode,
      itemDesc: x.itemDesc,
      area: x.area,
      grupo: x.grupo,
      revenue: x.revenue,
      gp: x.gp,
      gpPct: x.gpPct,
      totalLabel: x.totalLabel,
    }));

  return {
    range: { from: data?.from, to: data?.to },
    filters: {
      area: data?.area,
      grupo: data?.grupo,
      q: data?.q || "",
    },
    totals: data?.totals || {},
    counts,
    topGroupsByRevenue: groupAgg.slice(0, 15).map((g) => ({
      grupo: g.grupo,
      revenue: g.revenue,
      gp: g.gp,
      gpPct: g.gpPct,
    })),
    topItemsByRevenue: topByRevenue,
    itemsBelowMin: belowMin,
    itemsAtOrOverMax: atOrOverMax,
    topItemsByMarginPct: highestMargin,
    lowestItemsByMarginPct: lowestMargin,
  };
}

async function estratLoadItemDocsForAi({ itemCode, from, to, area = "__ALL__", grupo = "__ALL__" }) {
  if (!itemCode) return [];
  const q1 = await dbQuery(
    `
    SELECT
      s.doc_type,
      s.doc_date::text AS doc_date,
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
    LIMIT 400
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

  if (area !== "__ALL__") rows = rows.filter((x) => String(x.area || "") === area);
  if (grupo !== "__ALL__") {
    const gSelN = normGroupName(grupo);
    rows = rows.filter((x) => normGroupName(x.grupo) === gSelN);
  }
  return rows;
}

function estratAiNum(x, d = 0) {
  const n = Number(x || 0);
  return Number.isFinite(n) ? Number(n.toFixed(d)) : 0;
}
function estratAiMoney2(x) {
  return estratAiNum(x, 2);
}

function estratCompactItemDetail(rows, itemLabel = "") {
  const safeRows = Array.isArray(rows) ? rows : [];
  const totals = safeRows.reduce(
    (acc, x) => {
      acc.docs += 1;
      acc.quantity += Number(x.quantity || 0);
      acc.revenue += Number(x.total || 0);
      acc.gp += Number(x.gp || 0);
      return acc;
    },
    { docs: 0, quantity: 0, revenue: 0, gp: 0 }
  );
  const gpPct = totals.revenue > 0 ? Number(((totals.gp / totals.revenue) * 100).toFixed(2)) : 0;

  const customerAgg = new Map();
  const monthAgg = new Map();
  for (const x of safeRows) {
    const custKey = `${x.cardCode}||${x.cardName}`;
    const c = customerAgg.get(custKey) || {
      cardCode: x.cardCode,
      cardName: x.cardName,
      customer: `${x.cardCode} · ${x.cardName}`,
      quantity: 0,
      revenue: 0,
      gp: 0,
      docs: 0,
    };
    c.quantity += Number(x.quantity || 0);
    c.revenue += Number(x.total || 0);
    c.gp += Number(x.gp || 0);
    c.docs += 1;
    customerAgg.set(custKey, c);

    const m = String(x.docDate || "").slice(0, 7);
    const mo = monthAgg.get(m) || { month: m, quantity: 0, revenue: 0, gp: 0, docs: 0 };
    mo.quantity += Number(x.quantity || 0);
    mo.revenue += Number(x.total || 0);
    mo.gp += Number(x.gp || 0);
    mo.docs += 1;
    monthAgg.set(m, mo);
  }

  return {
    itemLabel,
    totals: { ...totals, gpPct },
    byMonth: Array.from(monthAgg.values())
      .sort((a, b) => String(a.month).localeCompare(String(b.month)))
      .map((x) => ({
        month: x.month,
        quantity: estratAiNum(x.quantity, 4),
        revenue: estratAiMoney2(x.revenue),
        gp: estratAiMoney2(x.gp),
        gpPct: x.revenue ? estratAiNum((x.gp / x.revenue) * 100, 2) : 0,
        docs: x.docs,
      })),
    topCustomers: Array.from(customerAgg.values())
      .sort((a, b) => Number(b.revenue || 0) - Number(a.revenue || 0))
      .slice(0, 20)
      .map((x) => ({
        cardCode: x.cardCode,
        cardName: x.cardName,
        customer: x.customer,
        quantity: estratAiNum(x.quantity, 4),
        revenue: estratAiMoney2(x.revenue),
        gp: estratAiMoney2(x.gp),
        gpPct: x.revenue ? estratAiNum((x.gp / x.revenue) * 100, 2) : 0,
        docs: x.docs,
      })),
    recentDocs: safeRows.slice(0, 80).map((x) => ({
      docType: x.docType,
      docDate: x.docDate,
      docNum: x.docNum,
      cardCode: x.cardCode,
      cardName: x.cardName,
      quantity: x.quantity,
      total: x.total,
      gp: x.gp,
      gpPct: x.total ? Number(((Number(x.gp || 0) / Number(x.total || 0)) * 100).toFixed(2)) : 0,
      grupo: x.grupo,
      area: x.area,
    })),
  };
}

function estratExtractResponseText(obj) {
  if (!obj || typeof obj !== "object") return "";
  if (typeof obj.output_text === "string" && obj.output_text.trim()) return obj.output_text.trim();

  const out = [];
  for (const item of Array.isArray(obj.output) ? obj.output : []) {
    if (item?.type && item.type !== "message") continue;
    if (!Array.isArray(item.content)) continue;

    for (const c of item.content) {
      if (typeof c?.text === "string" && c.text.trim()) {
        out.push(c.text.trim());
        continue;
      }
      if (typeof c?.text?.value === "string" && c.text.value.trim()) {
        out.push(c.text.value.trim());
        continue;
      }
      if (typeof c?.output_text === "string" && c.output_text.trim()) {
        out.push(c.output_text.trim());
        continue;
      }
      if (c?.type === "refusal" && c?.refusal) {
        out.push(`El modelo rechazó responder: ${String(c.refusal).trim()}`);
      }
    }
  }

  return out.join("\n\n").trim();
}


function estratNormalizeItemCodeLoose(v) {
  const raw = String(v || '').trim().toUpperCase();
  if (!raw) return '';
  const m = raw.match(/^0*([0-9]+)([.-][A-Z0-9]+)?$/i);
  if (m) return `${m[1] || ''}${m[2] || ''}`;
  return raw.replace(/^0+(\d)/, '$1');
}

function estratExtractCodesFromText(text) {
  const src = String(text || '').toUpperCase();
  const matches = src.match(/\d{3,6}(?:[.-][A-Z0-9]+)?/g) || [];
  const out = [];
  const seen = new Set();
  for (const code of matches) {
    const key = estratNormalizeItemCodeLoose(code);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(String(code || '').trim());
  }
  return out;
}

function estratFindDashboardItemByCode(items, code) {
  const wanted = String(code || '').trim();
  if (!wanted) return null;
  const wantedLoose = estratNormalizeItemCodeLoose(wanted);
  for (const item of Array.isArray(items) ? items : []) {
    const itemCode = String(item?.itemCode || '').trim();
    if (!itemCode) continue;
    if (itemCode.toUpperCase() === wanted.toUpperCase()) return item;
    if (estratNormalizeItemCodeLoose(itemCode) === wantedLoose) return item;
  }
  return null;
}

function estratFindDashboardItemsByText(items, text) {
  const qn = norm(text || '');
  if (!qn) return [];
  return (Array.isArray(items) ? items : []).filter((item) => {
    const codeN = norm(item?.itemCode || '');
    const descN = norm(item?.itemDesc || '');
    return codeN === qn || descN.includes(qn) || qn.includes(codeN);
  });
}

function estratResolveRequestedCodes({ question = '', q = '', itemCode = '', dashboard = null }) {
  const items = Array.isArray(dashboard?.items) ? dashboard.items : [];
  const out = [];
  const seen = new Set();
  const pushCode = (code) => {
    const found = estratFindDashboardItemByCode(items, code);
    const finalCode = String(found?.itemCode || code || '').trim();
    const key = estratNormalizeItemCodeLoose(finalCode);
    if (!finalCode || !key || seen.has(key)) return;
    seen.add(key);
    out.push(finalCode);
  };

  if (itemCode) pushCode(itemCode);

  const qTrim = String(q || '').trim();
  if (qTrim) {
    const direct = estratFindDashboardItemByCode(items, qTrim);
    if (direct) pushCode(direct.itemCode);
    const byText = estratFindDashboardItemsByText(items, qTrim);
    if (byText.length === 1) pushCode(byText[0].itemCode);
    if (!out.length && Array.isArray(items) && items.length === 1) pushCode(items[0].itemCode);
  }

  for (const code of estratExtractCodesFromText(`${String(question || '')} ${String(q || '')}`)) {
    pushCode(code);
  }

  return out;
}

function estratCompactSelectedItem(rows, itemLabel = '') {
  const safeNum = (x, d = 0) => {
    const n = Number(x || 0);
    return Number.isFinite(n) ? Number(n.toFixed(d)) : 0;
  };
  const safeMoney = (x) => safeNum(x, 2);
  const list = Array.isArray(rows) ? rows : [];
  if (!list.length) return null;

  const byCustomer = new Map();
  const byMonth = new Map();
  let totQty = 0, totRev = 0, totGp = 0;
  let area = '', grupo = '', itemCode = '', itemDesc = '';

  for (const r of list) {
    const qty = Number(r.quantity || 0);
    const rev = Number(r.revenue || 0);
    const gp = Number(r.gp || 0);
    const month = String(r.docDate || '').slice(0, 7);
    const ckey = `${r.cardCode || ''}||${r.cardName || ''}`;

    itemCode = itemCode || String(r.itemCode || '');
    itemDesc = itemDesc || String(r.itemDesc || '');
    area = area || String(r.area || '');
    grupo = grupo || String(r.grupo || '');

    totQty += qty; totRev += rev; totGp += gp;

    const c = byCustomer.get(ckey) || { cardCode: String(r.cardCode || ''), cardName: String(r.cardName || ''), quantity: 0, revenue: 0, gp: 0 };
    c.quantity += qty; c.revenue += rev; c.gp += gp;
    byCustomer.set(ckey, c);

    if (month) {
      const m = byMonth.get(month) || { month, quantity: 0, revenue: 0, gp: 0 };
      m.quantity += qty; m.revenue += rev; m.gp += gp;
      byMonth.set(month, m);
    }
  }

  return {
    label: itemLabel || itemCode || itemDesc || '',
    itemCode,
    itemDesc,
    area,
    grupo,
    totals: {
      quantity: safeNum(totQty, 4),
      revenue: safeMoney(totRev),
      gp: safeMoney(totGp),
      gpPct: totRev ? safeNum((totGp / totRev) * 100, 2) : 0
    },
    topCustomers: Array.from(byCustomer.values()).map((x) => ({
      cardCode: x.cardCode,
      cardName: x.cardName,
      quantity: safeNum(x.quantity, 4),
      revenue: safeMoney(x.revenue),
      gp: safeMoney(x.gp),
      gpPct: x.revenue ? safeNum((x.gp / x.revenue) * 100, 2) : 0
    })).sort((a, b) => Number(b.revenue || 0) - Number(a.revenue || 0)).slice(0, 20),
    byMonth: Array.from(byMonth.values()).map((x) => ({
      month: x.month,
      quantity: safeNum(x.quantity, 4),
      revenue: safeMoney(x.revenue),
      gp: safeMoney(x.gp),
      gpPct: x.revenue ? safeNum((x.gp / x.revenue) * 100, 2) : 0
    })).sort((a, b) => String(a.month).localeCompare(String(b.month))).slice(0, 24),
    rawRows: list.slice(0, 120).map((r) => ({
      docDate: r.docDate,
      cardCode: r.cardCode,
      cardName: r.cardName,
      quantity: safeNum(r.quantity, 4),
      revenue: safeMoney(r.revenue),
      gp: safeMoney(r.gp),
      area: r.area,
      grupo: r.grupo
    }))
  };
}

async function openaiEstratificacionChat({ question, dashboard, itemRows = [], itemLabel = "", itemDetails = [], requestedCodes = [], matchedItems = [] }) {
  const apiKey = String(process.env.OPENAI_API_KEY || "").trim();
  const model = String(process.env.OPENAI_MODEL || "gpt-5-mini").trim();
  if (!apiKey) throw new Error("OPENAI_API_KEY no configurada");

  const safeNum = (x, d = 0) => {
    const n = Number(x || 0);
    return Number.isFinite(n) ? Number(n.toFixed(d)) : 0;
  };
  const safeMoney = (x) => safeNum(x, 2);

  const items = Array.isArray(dashboard?.items) ? dashboard.items : [];
  const groupAgg = Array.isArray(dashboard?.groupAgg) ? dashboard.groupAgg : [];

  const compactDashboard = {
    range: { from: dashboard?.from || "", to: dashboard?.to || "" },
    filters: {
      area: dashboard?.area || "__ALL__",
      grupo: dashboard?.grupo || "__ALL__",
      q: dashboard?.q || ""
    },
    totals: dashboard?.totals || {},
    summary: {
      totalItems: items.length,
      stockBelowMin: items.filter((x) => Number(x.stock || 0) < Number(x.stockMin || 0)).length,
      stockAtOrOverMax: items.filter((x) => Number(x.stockMax || 0) > 0 && Number(x.stock || 0) >= Number(x.stockMax || 0)).length,
      stockZeroOrNegative: items.filter((x) => Number(x.stock || 0) <= 0).length
    },
    topGroups: groupAgg.slice(0, 20).map((x) => ({
      area: x.area,
      grupo: x.grupo,
      items: Number(x.items || 0),
      revenue: safeMoney(x.revenue),
      gp: safeMoney(x.gp),
      gpPct: safeNum(x.gpPct, 2),
      shareRevenue: safeNum(x.shareRevenue, 2),
      shareGP: safeNum(x.shareGP, 2),
      label: x.label
    })),
    topItems: items.slice(0, 40).map((x) => ({
      rankTotal: Number(x.rankTotal || 0),
      rankArea: Number(x.rankArea || 0),
      itemCode: x.itemCode,
      itemDesc: x.itemDesc,
      area: x.area,
      grupo: x.grupo,
      revenue: safeMoney(x.revenue),
      gp: safeMoney(x.gp),
      gpPct: safeNum(x.gpPct, 2),
      totalLabel: x.totalLabel,
      abcRevenue: x.abcRevenue,
      abcGP: x.abcGP,
      abcGPPct: x.abcGPPct,
      stock: safeNum(x.stock, 2),
      stockMin: safeNum(x.stockMin, 2),
      stockMax: safeNum(x.stockMax, 2),
      available: safeNum(x.available, 2),
      committed: safeNum(x.committed, 2),
      ordered: safeNum(x.ordered, 2),
      faltanteVsMin: safeNum(Number(x.stockMin || 0) - Number(x.stock || 0), 2),
      excesoVsMax: safeNum(Number(x.stock || 0) - Number(x.stockMax || 0), 2),
    })),
  };

  const selectedItem = estratCompactSelectedItem(itemRows, itemLabel);
  const requestedItems = (Array.isArray(itemDetails) ? itemDetails : [])
    .map((x) => estratCompactSelectedItem(x?.rows || [], x?.label || x?.itemCode || ''))
    .filter(Boolean)
    .slice(0, 5);

  const compact = {
    dashboard: compactDashboard,
    selectedItem,
    requestedCodes: Array.isArray(requestedCodes) ? requestedCodes : [],
    matchedItems: (Array.isArray(matchedItems) ? matchedItems : []).map((x) => ({
      itemCode: x?.itemCode || '',
      itemDesc: x?.itemDesc || '',
      area: x?.area || '',
      grupo: x?.grupo || '',
      totalLabel: x?.totalLabel || '',
      stock: safeNum(x?.stock, 2),
      stockMin: safeNum(x?.stockMin, 2),
      stockMax: safeNum(x?.stockMax, 2),
      revenue: safeMoney(x?.revenue),
      gp: safeMoney(x?.gp),
      gpPct: safeNum(x?.gpPct, 2),
    })),
    requestedItems
  };

  const system = [
    "Eres un analista comercial interno senior de PRODIMA especializado en estratificación de productos, rentabilidad, clasificación ABC, niveles de inventario y riesgo comercial.",
    "Usa exclusivamente la información entregada por el sistema como fuente de verdad. No menciones formatos internos ni digas que estás usando JSON.",
    "La fuente es la base de datos sincronizada del sistema, no SAP en vivo.",
    "Respeta estrictamente los filtros activos de fecha, área, grupo, búsqueda y artículo seleccionado cuando existan.",
    "No inventes datos, no asumas ventas, stock ni márgenes que no estén presentes en el contexto.",
    "Responde siempre en español, con lenguaje claro, ejecutivo, útil y orientado a decisiones.",
    "Te pueden preguntar, Dame o dime el detalle del codigo; ejemplo 7270, el detalle por cliente y a que clientes tienen el mayor porcentaje de ganancia bruta (Gross Margin (%)), haz la lista de mayor a menor para saber que clientes dan mayor ganancia de ese producto.",
    "Prioriza hallazgos concretos sobre revenue, margen bruto, % margen, clasificación ABC, ranking, concentración, stock, mínimos, máximos, disponible, comprometido y ordenado.",
    "Cuando hables de stock, distingue con precisión si el artículo está por debajo del mínimo, dentro de rango, en máximo o por encima del máximo.",
    "Cuando hables de clasificación, explica tanto la clasificación total como ABC de revenue, GP y GP%.",
    "Si el usuario pregunta por artículos críticos, prioriza artículos A o AB, especialmente los que estén bajo mínimo o con alta participación de ventas.",
    "Si el usuario pregunta por oportunidades, identifica artículos con buen margen, artículos con stock alto y baja venta, y artículos con potencial de mejora.",
    "Si hay un artículo seleccionado, analízalo primero. Luego compáralo contra el contexto general solo si eso agrega valor.",
    "IMPORTANTE: aunque no exista artículo seleccionado manualmente, si requestedCodes, matchedItems o requestedItems traen uno o más artículos inferidos desde la pregunta o la búsqueda, debes analizarlos como contexto principal. No digas que un código no aparece si está en requestedCodes, matchedItems o requestedItems.",
    "Si requestedItems trae varios artículos, compáralos directamente en la respuesta y usa tablas markdown cuando eso ayude.",
    "Si hay datos del artículo por cliente o por mes, menciona qué clientes lo compran más, cómo se comporta en el tiempo y si su margen cambia.",
    "Cuando compares artículos, usa métricas concretas: revenue, GP, GP%, stock, disponible, comprometido, ordenado y clasificación.",
    "Si detectas riesgo, explica por qué: por ejemplo alto revenue con stock bajo, margen bajo, sobrestock, dependencia de un grupo o concentración excesiva.",
    "Si detectas sobrestock, menciona el exceso frente al máximo cuando esté disponible.",
    "Si detectas faltante, menciona el faltante frente al mínimo cuando esté disponible.",
    "Cuando el usuario pida resumen, responde con estructura: resumen ejecutivo, hallazgos clave, riesgos, oportunidades y acciones sugeridas.",
    "Cuando el usuario pida un artículo específico, responde con estructura: resumen del artículo, desempeño comercial, margen, clientes, comportamiento mensual, stock y recomendación.",
    "No des respuestas genéricas. Siempre que sea posible menciona códigos, descripciones, grupos, área, montos, porcentajes y niveles de stock concretos.",
    "Si no hay suficiente detalle para concluir una causa, dilo claramente y plantea la causa como hipótesis basada en los datos visibles.",
    "Tu objetivo es ayudar a ventas, gerencia y planificación a decidir qué artículos proteger, impulsar, revisar o depurar."
  ].join(" ");

  const reasoning = model.startsWith("gpt-5.1") || model.startsWith("gpt-5.2")
    ? { effort: "none" }
    : model.startsWith("gpt-5")
      ? { effort: "minimal" }
      : undefined;

  const payload = {
    model,
    input: [
      { role: "system", content: [{ type: "input_text", text: system }] },
      {
        role: "user",
        content: [{
          type: "input_text",
          text:
            `Pregunta del usuario:\n${String(question || "").trim()}\n\n` +
            `Contexto del sistema:\n${JSON.stringify(compact)}`
        }]
      }
    ],
    text: { format: { type: "text" } },
    max_output_tokens: 2200,
    ...(reasoning ? { reasoning } : {}),
  };

  const resp = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify(payload),
  });

  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    throw new Error(
      data?.error?.message ||
      data?.message ||
      `OpenAI HTTP ${resp.status}`
    );
  }

  const answer = estratExtractResponseText(data);
  if (!answer) {
    console.error("OpenAI empty output [estratificacion]", {
      model,
      status: data?.status || null,
      incomplete_details: data?.incomplete_details || null,
      output_types: Array.isArray(data?.output) ? data.output.map((x) => x?.type) : [],
      usage: data?.usage || null,
    });

    throw new Error(
      data?.incomplete_details?.reason
        ? `OpenAI devolvió salida vacía (${data.incomplete_details.reason})`
        : "OpenAI devolvió salida vacía"
    );
  }

  return {
    answer,
    model,
    raw: data,
  };
}

async function loadEstratificacionDashboardForAi(args) {
  const fn =
    (typeof globalThis.dashboardFromDbEstratificacion === "function" && globalThis.dashboardFromDbEstratificacion) ||
    (typeof dashboardFromDbEstratificacion === "function" && dashboardFromDbEstratificacion) ||
    (typeof globalThis.dashboardFromDb === "function" && globalThis.dashboardFromDb) ||
    (typeof dashboardFromDb === "function" && dashboardFromDb) ||
    null;

  if (!fn) {
    throw new Error("No existe función de dashboard para Estratificación");
  }

  return await fn(args);
}

app.post("/api/admin/estratificacion/ai-chat", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const question = String(req.body?.question || "").trim();
    if (!question) return safeJson(res, 400, { ok: false, message: "question requerida" });

    const fromQ = String(req.body?.from || "");
    const toQ = String(req.body?.to || "");
    const area = String(req.body?.area || "__ALL__");
    const grupo = String(req.body?.grupo || "__ALL__");
    const q = String(req.body?.q || "");
    const itemCode = String(req.body?.itemCode || "").trim();
    const itemLabel = String(req.body?.itemLabel || "").trim();

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = isISO(fromQ) ? fromQ : "2025-01-01";
    const to = isISO(toQ) ? toQ : today;

    const dashboard = await loadEstratificacionDashboardForAi({ from, to, area, grupo, q });
    const requestedCodes = estratResolveRequestedCodes({ question, q, itemCode, dashboard });
    const matchedItems = requestedCodes
      .map((code) => estratFindDashboardItemByCode(dashboard?.items || [], code))
      .filter(Boolean);

    const itemDetails = [];
    for (const code of requestedCodes.slice(0, 5)) {
      try {
        const rows = await estratLoadItemDocsForAi({ itemCode: code, from, to, area, grupo });
        const matched = estratFindDashboardItemByCode(dashboard?.items || [], code);
        if (rows.length || matched) {
          itemDetails.push({
            itemCode: String(matched?.itemCode || code || '').trim(),
            label: String(matched?.itemDesc || itemLabel || code || '').trim(),
            rows,
          });
        }
      } catch {}
    }

    const primaryDetail = itemDetails[0] || null;

    const out = await openaiEstratificacionChat({
      question,
      dashboard,
      itemRows: primaryDetail?.rows || [],
      itemLabel: primaryDetail?.label || itemLabel || itemCode,
      itemDetails,
      requestedCodes,
      matchedItems,
    });

    return safeJson(res, 200, {
      ok: true,
      answer: out.answer,
      model: out.model,
      source: "db",
      range: { from, to },
      filters: { area, grupo, q },
      focus: primaryDetail ? {
        itemCode: primaryDetail.itemCode,
        itemLabel: primaryDetail.label || primaryDetail.itemCode,
        label: primaryDetail.label || primaryDetail.itemCode,
        inferred: !itemCode || String(primaryDetail.itemCode || '') !== String(itemCode || ''),
      } : null,
      requestedCodes,
      matchedItems: matchedItems.map((x) => String(x?.itemCode || '').trim()).filter(Boolean),
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});


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
