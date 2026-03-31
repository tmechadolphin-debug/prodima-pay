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
    if (!["user","admin"].includes(String(decoded?.role || ""))) return safeJson(res, 403, { ok: false, message: "Forbidden" });
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
    if (!["user","admin"].includes(String(decoded?.role || ""))) return safeJson(res, 403, { ok: false, message: "Forbidden" });
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

function attachmentBaseName(name, fallback = "archivo") {
  const safe = sanitizeAttachmentName(name, fallback);
  return safe.replace(/\.[A-Za-z0-9]{1,8}$/, "") || fallback;
}

function parseJpegMeta(buf) {
  if (!Buffer.isBuffer(buf) || buf.length < 4) return null;
  if (buf[0] !== 0xff || buf[1] !== 0xd8) return null;
  let i = 2;
  while (i + 9 < buf.length) {
    while (i < buf.length && buf[i] !== 0xff) i += 1;
    while (i < buf.length && buf[i] === 0xff) i += 1;
    if (i >= buf.length) break;
    const marker = buf[i++];
    if (marker === 0xd9 || marker === 0xda) break;
    if (i + 1 >= buf.length) break;
    const size = buf.readUInt16BE(i);
    if (size < 2 || i + size > buf.length) break;
    const isSOF = [0xc0,0xc1,0xc2,0xc3,0xc5,0xc6,0xc7,0xc9,0xca,0xcb,0xcd,0xce,0xcf].includes(marker);
    if (isSOF && size >= 8) {
      return {
        width: buf.readUInt16BE(i + 5),
        height: buf.readUInt16BE(i + 3),
        components: buf[i + 7] || 3,
      };
    }
    i += size;
  }
  return null;
}

function buildMinimalPdfFromJpegs(images, filenameBase = "archivo") {
  const pages = (Array.isArray(images) ? images : [])
    .map((img) => {
      const buf = Buffer.isBuffer(img?.buf) ? img.buf : null;
      const meta = buf ? parseJpegMeta(buf) : null;
      if (!buf || !meta || !meta.width || !meta.height) return null;
      const comps = Number(meta.components || 3);
      const colorSpace = comps === 1 ? "/DeviceGray" : (comps === 4 ? "/DeviceCMYK" : "/DeviceRGB");
      const width = meta.width;
      const height = meta.height;
      const contentText = `q\n${width} 0 0 ${height} 0 0 cm\n/Im0 Do\nQ\n`;
      return {
        buf,
        width,
        height,
        colorSpace,
        contentText,
        contentBuf: Buffer.from(contentText, "ascii"),
      };
    })
    .filter(Boolean);

  if (!pages.length) {
    throw new Error("No hay imágenes JPEG válidas para PDF");
  }

  const objects = [];
  const kids = [];
  let nextObjNum = 3;

  for (const page of pages) {
    const pageObjNum = nextObjNum++;
    const imgObjNum = nextObjNum++;
    const contentObjNum = nextObjNum++;
    kids.push(`${pageObjNum} 0 R`);

    objects.push(Buffer.from(
      `${pageObjNum} 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 ${page.width} ${page.height}] /Resources << /XObject << /Im0 ${imgObjNum} 0 R >> >> /Contents ${contentObjNum} 0 R >>\nendobj\n`,
      "ascii"
    ));

    objects.push(Buffer.concat([
      Buffer.from(
        `${imgObjNum} 0 obj\n<< /Type /XObject /Subtype /Image /Width ${page.width} /Height ${page.height} /ColorSpace ${page.colorSpace} /BitsPerComponent 8 /Filter /DCTDecode /Length ${page.buf.length} >>\nstream\n`,
        "ascii"
      ),
      page.buf,
      Buffer.from(`\nendstream\nendobj\n`, "ascii"),
    ]));

    objects.push(Buffer.from(
      `${contentObjNum} 0 obj\n<< /Length ${page.contentBuf.length} >>\nstream\n${page.contentText}endstream\nendobj\n`,
      "ascii"
    ));
  }

  const catalogObj = Buffer.from(`1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n`, "ascii");
  const pagesObj = Buffer.from(`2 0 obj\n<< /Type /Pages /Kids [${kids.join(" ")}] /Count ${pages.length} >>\nendobj\n`, "ascii");
  const allObjects = [catalogObj, pagesObj, ...objects];

  const header = Buffer.from("%PDF-1.4\n%\xE2\xE3\xCF\xD3\n", "binary");
  const parts = [header];
  const offsets = [0];
  let offset = header.length;

  for (const obj of allObjects) {
    offsets.push(offset);
    parts.push(obj);
    offset += obj.length;
  }

  const xrefOffset = offset;
  let xref = `xref\n0 ${allObjects.length + 1}\n0000000000 65535 f \n`;
  for (let i = 1; i <= allObjects.length; i += 1) {
    xref += `${String(offsets[i]).padStart(10, "0")} 00000 n \n`;
  }

  const safeTitle = String(filenameBase || "archivo").replace(/[()\\]/g, "");
  parts.push(Buffer.from(xref, "ascii"));
  parts.push(Buffer.from(
    `trailer\n<< /Size ${allObjects.length + 1} /Root 1 0 R /Info << /Title (${safeTitle}) >> >>\nstartxref\n${xrefOffset}\n%%EOF\n`,
    "ascii"
  ));
  return Buffer.concat(parts);
}

function buildMinimalPdfFromJpeg(buf, filenameBase = "archivo") {
  return buildMinimalPdfFromJpegs([{ buf }], filenameBase);
}

function isMergeableJpegAttachment(file) {
  const lowerMime = String(file?.mimeType || file?.type || "").trim().toLowerCase();
  return ["image/jpeg", "image/jpg"].includes(lowerMime);
}

function maybeConvertAttachmentToPdf({ filename, mimeType, contentBase64 }) {
  const lowerMime = String(mimeType || "").toLowerCase();
  if (!["image/jpeg", "image/jpg"].includes(lowerMime)) {
    return { filename, mimeType, contentBase64 };
  }
  const srcBuf = Buffer.from(contentBase64, "base64");
  const pdfBuf = buildMinimalPdfFromJpeg(srcBuf, attachmentBaseName(filename, "archivo"));
  return {
    filename: `${attachmentBaseName(filename, "archivo")}.pdf`,
    mimeType: "application/pdf",
    contentBase64: pdfBuf.toString("base64"),
  };
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
  const normalizedInputs = [];

  for (const file of incoming.slice(0, 5)) {
    const filename = sanitizeAttachmentName(file?.filename || file?.name || "archivo");
    const mimeType = String(file?.mimeType || file?.type || "application/octet-stream").trim().toLowerCase();
    const contentBase64 = String(file?.contentBase64 || file?.base64 || file?.content || "").trim();

    if (!filename || !contentBase64) continue;
    if (!allowed.has(mimeType)) continue;

    normalizedInputs.push({ filename, mimeType, contentBase64 });
  }

  const jpegImages = [];
  const out = [];
  let totalBytes = 0;

  for (const file of normalizedInputs) {
    if (isMergeableJpegAttachment(file)) {
      jpegImages.push(file);
      continue;
    }

    let normalized = { ...file };
    try {
      normalized = maybeConvertAttachmentToPdf(normalized);
    } catch {
      normalized = { ...file };
    }

    let bytes = 0;
    try {
      bytes = Buffer.byteLength(normalized.contentBase64, "base64");
    } catch {
      continue;
    }

    if (!bytes || bytes > 8 * 1024 * 1024) continue;
    if (totalBytes + bytes > 18 * 1024 * 1024) break;
    totalBytes += bytes;

    out.push({
      filename: normalized.filename,
      mimeType: normalized.mimeType,
      contentBase64: normalized.contentBase64,
      size: bytes,
    });
  }

  if (jpegImages.length) {
    try {
      const mergedPdf = buildMinimalPdfFromJpegs(
        jpegImages.map((img) => ({ buf: Buffer.from(img.contentBase64, "base64") })),
        attachmentBaseName(jpegImages[0]?.filename || "imagenes", "imagenes")
      );
      const mergedBytes = mergedPdf.length;
      if (mergedBytes > 0 && mergedBytes <= 18 * 1024 * 1024 && totalBytes + mergedBytes <= 18 * 1024 * 1024) {
        totalBytes += mergedBytes;
        out.unshift({
          filename: `${attachmentBaseName(jpegImages[0]?.filename || "imagenes", "imagenes")}_imagenes.pdf`,
          mimeType: "application/pdf",
          contentBase64: mergedPdf.toString("base64"),
          size: mergedBytes,
        });
      } else {
        for (const img of jpegImages) {
          let normalized = { ...img };
          try {
            normalized = maybeConvertAttachmentToPdf(normalized);
          } catch {
            normalized = { ...img };
          }
          let bytes = 0;
          try {
            bytes = Buffer.byteLength(normalized.contentBase64, "base64");
          } catch {
            continue;
          }
          if (!bytes || bytes > 8 * 1024 * 1024) continue;
          if (totalBytes + bytes > 18 * 1024 * 1024) break;
          totalBytes += bytes;
          out.push({
            filename: normalized.filename,
            mimeType: normalized.mimeType,
            contentBase64: normalized.contentBase64,
            size: bytes,
          });
        }
      }
    } catch {
      for (const img of jpegImages) {
        let normalized = { ...img };
        try {
          normalized = maybeConvertAttachmentToPdf(normalized);
        } catch {
          normalized = { ...img };
        }
        let bytes = 0;
        try {
          bytes = Buffer.byteLength(normalized.contentBase64, "base64");
        } catch {
          continue;
        }
        if (!bytes || bytes > 8 * 1024 * 1024) continue;
        if (totalBytes + bytes > 18 * 1024 * 1024) break;
        totalBytes += bytes;
        out.push({
          filename: normalized.filename,
          mimeType: normalized.mimeType,
          contentBase64: normalized.contentBase64,
          size: bytes,
        });
      }
    }
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
      num_per_msr NUMERIC(18,4) NOT NULL DEFAULT 1,
      quantity_base NUMERIC(18,4) NOT NULL DEFAULT 0,
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
  await dbQuery(`ALTER TABLE sales_item_lines ADD COLUMN IF NOT EXISTS num_per_msr NUMERIC(18,4) NOT NULL DEFAULT 1;`);
  await dbQuery(`ALTER TABLE sales_item_lines ADD COLUMN IF NOT EXISTS quantity_base NUMERIC(18,4) NOT NULL DEFAULT 0;`);

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

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS customer_category_cache (
      card_code TEXT PRIMARY KEY,
      category_code INTEGER,
      category_name TEXT NOT NULL DEFAULT 'Sin categoría',
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

function normalizeCustomerCategoryName(v) {
  const txt = String(v || "").trim();
  return txt || "Sin categoría";
}

async function getCachedCustomerCategories(cardCodes = []) {
  const list = Array.from(new Set((Array.isArray(cardCodes) ? cardCodes : []).map((x) => String(x || "").trim()).filter(Boolean)));
  if (!list.length) return new Map();

  const r = await dbQuery(
    `SELECT card_code, category_code, category_name, updated_at
       FROM customer_category_cache
      WHERE card_code = ANY($1::text[])`,
    [list]
  );

  const map = new Map();
  for (const row of (r.rows || [])) {
    map.set(String(row.card_code || "").trim(), {
      cardCode: String(row.card_code || "").trim(),
      categoryCode: row.category_code == null ? null : Number(row.category_code),
      categoryName: normalizeCustomerCategoryName(row.category_name),
      updatedAt: row.updated_at ? new Date(row.updated_at) : null,
    });
  }
  return map;
}

async function upsertCustomerCategory(entry = {}) {
  const cardCode = String(entry.cardCode || "").trim();
  if (!cardCode) return;
  await dbQuery(
    `INSERT INTO customer_category_cache(card_code, category_code, category_name, updated_at)
     VALUES ($1,$2,$3,NOW())
     ON CONFLICT (card_code) DO UPDATE SET
       category_code = EXCLUDED.category_code,
       category_name = EXCLUDED.category_name,
       updated_at = NOW()`,
    [
      cardCode,
      entry.categoryCode == null ? null : Number(entry.categoryCode),
      normalizeCustomerCategoryName(entry.categoryName),
    ]
  );
}

async function getBusinessPartnerCategoryFromSap(cardCode) {
  const code = String(cardCode || "").trim();
  if (!code || missingSapEnv()) {
    return { cardCode: code, categoryCode: null, categoryName: "Sin categoría" };
  }

  const safeCode = code.replace(/'/g, "''");
  const encoded = encodeURIComponent(code);
  let bp = null;

  for (const p of [
    `/BusinessPartners('${encoded}')?$select=CardCode,CardName,GroupCode`,
    `/BusinessPartners?$select=CardCode,CardName,GroupCode&$filter=CardCode eq '${safeCode}'&$top=1`,
    `/BusinessPartners('${encoded}')`,
  ]) {
    try {
      const data = await slFetch(p, { timeoutMs: 45000 });
      bp = Array.isArray(data?.value) ? data.value[0] : data;
      if (bp) break;
    } catch {}
  }

  const groupCode = Number(bp?.GroupCode ?? bp?.groupCode ?? bp?.GroupNum ?? bp?.groupNum ?? null);
  if (!Number.isFinite(groupCode) || groupCode <= 0) {
    return { cardCode: code, categoryCode: null, categoryName: "Sin categoría" };
  }

  let row = null;
  for (const p of [
    `/BusinessPartnerGroups(${groupCode})?$select=Code,Name,Type`,
    `/BusinessPartnerGroups?$select=Code,Name,Type&$filter=Code eq ${groupCode}&$top=1`,
    `/BusinessPartnerGroups(${groupCode})`,
  ]) {
    try {
      const data = await slFetch(p, { timeoutMs: 45000 });
      row = Array.isArray(data?.value) ? data.value[0] : data;
      if (row) break;
    } catch {}
  }

  return {
    cardCode: code,
    categoryCode: groupCode,
    categoryName: normalizeCustomerCategoryName(row?.Name || row?.GroupName || `Grupo ${groupCode}`),
  };
}

async function ensureCustomerCategoriesForCardCodes(cardCodes = []) {
  const uniqueCodes = Array.from(new Set((Array.isArray(cardCodes) ? cardCodes : []).map((x) => String(x || "").trim()).filter(Boolean)));
  if (!uniqueCodes.length || !hasDb()) return new Map();

  const cache = await getCachedCustomerCategories(uniqueCodes);
  const now = Date.now();
  const staleMs = 7 * 24 * 60 * 60 * 1000;

  const pending = uniqueCodes.filter((code) => {
    const hit = cache.get(code);
    const ts = hit?.updatedAt instanceof Date && !Number.isNaN(hit.updatedAt.getTime()) ? hit.updatedAt.getTime() : 0;
    return !hit || !ts || (now - ts) > staleMs;
  });

  for (const code of pending) {
    try {
      const info = await getBusinessPartnerCategoryFromSap(code);
      await upsertCustomerCategory(info);
      cache.set(code, {
        cardCode: code,
        categoryCode: info.categoryCode,
        categoryName: normalizeCustomerCategoryName(info.categoryName),
        updatedAt: new Date(),
      });
    } catch {
      if (!cache.has(code)) {
        cache.set(code, {
          cardCode: code,
          categoryCode: null,
          categoryName: "Sin categoría",
          updatedAt: new Date(),
        });
      }
    }
  }

  return cache;
}

async function enrichRowsWithCustomerCategory(rows = []) {
  const list = Array.isArray(rows) ? rows : [];
  if (!list.length) return list;

  const cache = await ensureCustomerCategoriesForCardCodes(list.map((r) => r.cardCode));
  for (const row of list) {
    row.categoria = normalizeCustomerCategoryName(
      cache.get(String(row.cardCode || "").trim())?.categoryName
    );
  }
  return list;
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
  const itemMasterCache = new Map();

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
    let itemMaster = null;
    const lineNeedsFallback = !(Number(Math.abs(Number(ln?.ItemsPerUnit || 0))) > 0 || Number(Math.abs(Number(ln?.NumPerMsr || 0))) > 0);
    if (lineNeedsFallback) {
      if (itemMasterCache.has(itemCode)) itemMaster = itemMasterCache.get(itemCode);
      else {
        itemMaster = await prodGetFullItem(itemCode).catch(() => null);
        itemMasterCache.set(itemCode, itemMaster || null);
      }
    }
    const numPerMsr = prodLineItemsPerUnit(ln, itemMaster);
    const qtyBase = qty * numPerMsr;
    const rev = Math.abs(revRaw) * sign;
    const gp = Math.abs(gpRaw) * sign;

    await dbQuery(
      `
      INSERT INTO sales_item_lines(
        doc_entry,line_num,doc_type,doc_date,doc_num,card_code,card_name,
        item_code,item_desc,quantity,num_per_msr,quantity_base,revenue,gross_profit,updated_at
      )
      VALUES($1,$2,$3,$4::date,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,NOW())
      ON CONFLICT(doc_entry,line_num,doc_type) DO UPDATE SET
        doc_date=EXCLUDED.doc_date,
        doc_num=EXCLUDED.doc_num,
        card_code=EXCLUDED.card_code,
        card_name=EXCLUDED.card_name,
        item_code=EXCLUDED.item_code,
        item_desc=EXCLUDED.item_desc,
        quantity=EXCLUDED.quantity,
        num_per_msr=EXCLUDED.num_per_msr,
        quantity_base=EXCLUDED.quantity_base,
        revenue=EXCLUDED.revenue,
        gross_profit=EXCLUDED.gross_profit,
        updated_at=NOW()
      `,
      [docEntry, lineNum, docType, docDate, docNum, cardCode, cardName, itemCode, itemDesc, qty, numPerMsr, qtyBase, rev, gp]
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
    FROM production_demand_lines
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
    const sizeUom = String(req.query?.sizeUom || req.query?.size || "__ALL__");
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

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS customer_category_cache (
      card_code TEXT PRIMARY KEY,
      category_code INTEGER,
      category_name TEXT NOT NULL DEFAULT 'Sin categoría',
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

function normalizeCustomerCategoryName(v) {
  const txt = String(v || "").trim();
  return txt || "Sin categoría";
}

async function getCachedCustomerCategories(cardCodes = []) {
  const list = Array.from(new Set((Array.isArray(cardCodes) ? cardCodes : []).map((x) => String(x || "").trim()).filter(Boolean)));
  if (!list.length) return new Map();

  const r = await dbQuery(
    `SELECT card_code, category_code, category_name, updated_at
       FROM customer_category_cache
      WHERE card_code = ANY($1::text[])`,
    [list]
  );

  const map = new Map();
  for (const row of (r.rows || [])) {
    map.set(String(row.card_code || "").trim(), {
      cardCode: String(row.card_code || "").trim(),
      categoryCode: row.category_code == null ? null : Number(row.category_code),
      categoryName: normalizeCustomerCategoryName(row.category_name),
      updatedAt: row.updated_at ? new Date(row.updated_at) : null,
    });
  }
  return map;
}

async function upsertCustomerCategory(entry = {}) {
  const cardCode = String(entry.cardCode || "").trim();
  if (!cardCode) return;
  await dbQuery(
    `INSERT INTO customer_category_cache(card_code, category_code, category_name, updated_at)
     VALUES ($1,$2,$3,NOW())
     ON CONFLICT (card_code) DO UPDATE SET
       category_code = EXCLUDED.category_code,
       category_name = EXCLUDED.category_name,
       updated_at = NOW()`,
    [
      cardCode,
      entry.categoryCode == null ? null : Number(entry.categoryCode),
      normalizeCustomerCategoryName(entry.categoryName),
    ]
  );
}

async function getBusinessPartnerCategoryFromSap(cardCode) {
  const code = String(cardCode || "").trim();
  if (!code || missingSapEnv()) {
    return { cardCode: code, categoryCode: null, categoryName: "Sin categoría" };
  }

  const safeCode = code.replace(/'/g, "''");
  const encoded = encodeURIComponent(code);
  let bp = null;

  for (const path of [
    `/BusinessPartners('${encoded}')?$select=CardCode,CardName,GroupCode`,
    `/BusinessPartners?$select=CardCode,CardName,GroupCode&$filter=CardCode eq '${safeCode}'&$top=1`,
    `/BusinessPartners('${encoded}')`,
  ]) {
    try {
      const data = await slFetch(path, { timeoutMs: 45000 });
      bp = Array.isArray(data?.value) ? data.value[0] : data;
      if (bp) break;
    } catch {}
  }

  const groupCode = Number(bp?.GroupCode ?? bp?.groupCode ?? bp?.GroupNum ?? bp?.groupNum ?? null);
  if (!Number.isFinite(groupCode) || groupCode <= 0) {
    return { cardCode: code, categoryCode: null, categoryName: "Sin categoría" };
  }

  let row = null;
  for (const path of [
    `/BusinessPartnerGroups(${groupCode})?$select=Code,Name,Type`,
    `/BusinessPartnerGroups?$select=Code,Name,Type&$filter=Code eq ${groupCode}&$top=1`,
    `/BusinessPartnerGroups(${groupCode})`,
  ]) {
    try {
      const data = await slFetch(path, { timeoutMs: 45000 });
      row = Array.isArray(data?.value) ? data.value[0] : data;
      if (row) break;
    } catch {}
  }

  return {
    cardCode: code,
    categoryCode: groupCode,
    categoryName: normalizeCustomerCategoryName(row?.Name || row?.GroupName || `Grupo ${groupCode}`),
  };
}

async function ensureCustomerCategoriesForCardCodes(cardCodes = []) {
  const uniqueCodes = Array.from(new Set((Array.isArray(cardCodes) ? cardCodes : []).map((x) => String(x || "").trim()).filter(Boolean)));
  if (!uniqueCodes.length || !hasDb()) return new Map();

  const cache = await getCachedCustomerCategories(uniqueCodes);
  const now = Date.now();
  const staleMs = 7 * 24 * 60 * 60 * 1000;

  const pending = uniqueCodes.filter((code) => {
    const hit = cache.get(code);
    const ts = hit?.updatedAt instanceof Date && !Number.isNaN(hit.updatedAt.getTime()) ? hit.updatedAt.getTime() : 0;
    return !hit || !ts || (now - ts) > staleMs;
  });

  for (const code of pending) {
    try {
      const info = await getBusinessPartnerCategoryFromSap(code);
      await upsertCustomerCategory(info);
      cache.set(code, {
        cardCode: code,
        categoryCode: info.categoryCode,
        categoryName: normalizeCustomerCategoryName(info.categoryName),
        updatedAt: new Date(),
      });
    } catch {
      if (!cache.has(code)) {
        cache.set(code, {
          cardCode: code,
          categoryCode: null,
          categoryName: "Sin categoría",
          updatedAt: new Date(),
        });
      }
    }
  }

  return cache;
}

async function enrichRowsWithCustomerCategory(rows = []) {
  const list = Array.isArray(rows) ? rows : [];
  if (!list.length) return list;

  const cache = await ensureCustomerCategoriesForCardCodes(list.map((r) => r.cardCode));
  for (const row of list) {
    row.categoria = normalizeCustomerCategoryName(cache.get(String(row.cardCode || "").trim())?.categoryName);
  }
  return list;
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
async function scanDocHeaders(entity, { f, t, from, to, maxDocs = 3000 }) {
  const fromDate = String(f || from || '').slice(0,10);
  const toDate = String(t || to || '').slice(0,10);
  if (!fromDate || !toDate) throw new Error('scanDocHeaders requiere rango de fechas');
  const toPlus1 = addDaysISO(toDate, 1);
  const batchTop = 200;
  let skipSap = 0;
  const out = [];

  for (let page = 0; page < 500; page++) {
    const raw = await slFetch(
      `/${entity}?$select=DocEntry,DocNum,DocDate,CardCode,CardName` +
        `&$filter=${encodeURIComponent(`DocDate ge '${fromDate}' and DocDate lt '${toPlus1}'`)}` +
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
  const rows = (r.rows || []).map((x) => {
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
      categoria: "Sin categoría",
    };
  });
  return enrichRowsWithCustomerCategory(rows);
}


function isoDateOnly(v) {
  if (!v) return "";
  if (typeof v === "string") return v.slice(0, 10);
  if (v instanceof Date && !Number.isNaN(v.getTime())) return v.toISOString().slice(0, 10);
  return String(v).slice(0, 10);
}

function applyAreaGroupFilters(rows, { area = "__ALL__", grupo = "__ALL__", categoria = "__ALL__" } = {}) {
  const areaSel = String(area || "__ALL__").trim().toUpperCase();
  const grupoSel = String(grupo || "__ALL__").trim();
  const categoriaSel = normalizeCustomerCategoryName(categoria === "__ALL__" ? "" : categoria);
  let out = Array.isArray(rows) ? rows.slice() : [];

  if (areaSel !== "__ALL__") {
    out = out.filter((r) => String(r.area || "").toUpperCase() === areaSel);
  }
  if (grupoSel !== "__ALL__") {
    const gSel = canonicalInvoiceGroup(grupoSel);
    out = out.filter((r) => canonicalInvoiceGroup(r.grupo) === gSel);
  }
  if (String(categoria || "__ALL__").trim() !== "__ALL__") {
    out = out.filter((r) => normalizeCustomerCategoryName(r.categoria) === categoriaSel);
  }
  return out;
}

function availableCategoriesFromRows(rows = []) {
  return Array.from(
    new Set((Array.isArray(rows) ? rows : []).map((r) => normalizeCustomerCategoryName(r.categoria)).filter(Boolean))
  ).sort((a, b) => a.localeCompare(b));
}

function availableGroupsForArea(area) {
  return getAllowedGroupsByArea(area).slice().sort((a, b) => a.localeCompare(b));
}

/* =========================================================
   ✅ Dashboard from DB (neto)
========================================================= */
async function dashboardFromDbAdminClientes({ from, to, area = "__ALL__", grupo = "__ALL__", categoria = "__ALL__", q = "" }) {
  const baseRows = await fetchInvoiceRows({ from, to, q });
  const scopeRows = applyAreaGroupFilters(baseRows, { area, grupo, categoria: "__ALL__" });
  const rows = applyAreaGroupFilters(scopeRows, { area: "__ALL__", grupo: "__ALL__", categoria });

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
      categoria: r.categoria,
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
        categoria: x.categoria || "Sin categoría",
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
    categoria,
    availableAreas: ["__ALL__", "CONS", "RCI"],
    availableGroups: availableGroupsForArea(area),
    availableCategories: availableCategoriesFromRows(scopeRows),
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
async function detailsFromDb({ from, to, cardCode, warehouse, area = "__ALL__", grupo = "__ALL__", categoria = "__ALL__" }) {
  let rows = await fetchInvoiceRows({ from, to, cardCode, warehouse });
  rows = applyAreaGroupFilters(rows, { area, grupo, categoria });

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
      categoria: r.categoria,
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

  return { ok: true, from, to, area, grupo, categoria, cardCode, warehouse, totals, invoices };
}

async function topProductsFromDb({ from, to, warehouse = "", cardCode = "", area = "__ALL__", grupo = "__ALL__", categoria = "__ALL__", limit = 10 }) {
  let rows = await fetchInvoiceRows({ from, to, cardCode, warehouse });
  rows = applyAreaGroupFilters(rows, { area, grupo, categoria });

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
      categoria: r.categoria,
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
    categoria,
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
          categoria: x.categoria,
        };
      })
      .sort((a, b) => Number(b.dollars || 0) - Number(a.dollars || 0))
      .slice(0, Math.max(1, Math.min(200, Number(limit || 10)))),
  };
}


/* =========================================================
   ✅ Artículos por categoría
========================================================= */
async function articlesFromDb({ from, to, area = "__ALL__", grupo = "__ALL__", categoria = "__ALL__", limit = 500 }) {
  let rows = await fetchInvoiceRows({ from, to, q: "" });
  rows = applyAreaGroupFilters(rows, { area, grupo, categoria });

  const map = new Map();
  for (const r of rows) {
    if (!String(r.itemCode || "").trim()) continue;
    const key = String(r.itemCode || "").trim();
    const cur = map.get(key) || {
      itemCode: key,
      itemDesc: String(r.itemDesc || ""),
      qty: 0,
      dollars: 0,
      grossProfit: 0,
      invSet: new Set(),
      custSet: new Set(),
    };
    cur.qty += Number(r.quantity || 0);
    cur.dollars += Number(r.dollars || 0);
    cur.grossProfit += Number(r.grossProfit || 0);
    if (r.docType === "INV") cur.invSet.add(`${r.docType}:${r.docEntry}`);
    if (r.cardCode) cur.custSet.add(String(r.cardCode));
    map.set(key, cur);
  }

  const items = Array.from(map.values())
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
        customers: x.custSet.size,
      };
    })
    .sort((a, b) => Number(b.dollars || 0) - Number(a.dollars || 0))
    .slice(0, Math.max(1, Math.min(2000, Number(limit || 500))));

  return {
    ok: true,
    from,
    to,
    area,
    grupo,
    categoria,
    categoriaLabel: categoria === "__ALL__" ? "Todas las categorías" : normalizeCustomerCategoryName(categoria),
    items,
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

async function buildAdminClientesAiAnalytics({ from, to, area = "__ALL__", grupo = "__ALL__", categoria = "__ALL__", q = "" }) {
  let rows = await fetchInvoiceRows({ from, to, q });
  rows = applyAreaGroupFilters(rows, { area, grupo, categoria });

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
    filters: { area, grupo, categoria, q },
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
      categoria: dashboard?.categoria || "__ALL__",
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

async function buildAdminClientesRecommendationAnalytics({ from, to, area = "__ALL__", grupo = "__ALL__", categoria = "__ALL__", targetCardCode = "", customerLabel = "", question = "" }) {
  let rows = await fetchInvoiceRows({ from, to, q: "" });
  rows = applyAreaGroupFilters(rows, { area, grupo, categoria });
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
    const categoria = String(req.query?.categoria || "__ALL__");
    const q = String(req.query?.q || "");

    const data = await dashboardFromDbAdminClientes({ from, to, area, grupo, categoria, q });
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
    const categoria = String(req.query?.categoria || "__ALL__");

    if (!cardCode || !warehouse) {
      return safeJson(res, 400, { ok: false, message: "cardCode y warehouse requeridos" });
    }

    const data = await detailsFromDb({ from, to, cardCode, warehouse, area, grupo, categoria });
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
    const categoria = String(req.query?.categoria || "__ALL__");
    const limit = Math.max(1, Math.min(200, Number(req.query?.limit || 10)));

    const data = await topProductsFromDb({ from, to, warehouse, cardCode, area, grupo, categoria, limit });
    return safeJson(res, 200, data);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.get("/api/admin/invoices/articles", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = isISO(req.query?.from) ? String(req.query.from) : addDaysISO(today, -30);
    const to = isISO(req.query?.to) ? String(req.query.to) : today;
    const area = String(req.query?.area || "__ALL__");
    const grupo = String(req.query?.grupo || "__ALL__");
    const categoria = String(req.query?.categoria || "__ALL__");
    const limit = Math.max(1, Math.min(2000, Number(req.query?.limit || 500)));

    const data = await articlesFromDb({ from, to, area, grupo, categoria, limit });
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
    const categoria = String(req.query?.categoria || "__ALL__");
    const q = String(req.query?.q || "");

    const data = await dashboardFromDbAdminClientes({ from, to, area, grupo, categoria, q });

    const wb = XLSX.utils.book_new();
    const rows = (data.table || []).map((r) => ({
      "Código cliente": r.cardCode,
      "Cliente": r.cardName,
      "Cliente label": r.customer,
      "Bodega": r.warehouse,
      "Categoría": r.categoria || "Sin categoría",
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
    const categoria = String(req.query?.categoria || "__ALL__");

    if (!cardCode || !warehouse) {
      return safeJson(res, 400, { ok: false, message: "cardCode y warehouse requeridos" });
    }

    const data = await detailsFromDb({ from, to, cardCode, warehouse, area, grupo, categoria });
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
          "Categoría": ln.categoria || "Sin categoría",
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
    const categoria = String(req.body?.categoria || req.query?.categoria || "__ALL__");
    const q = String(req.body?.q || req.query?.q || "").trim();

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const defaultFrom = addDaysISO(today, -30);
    const from = isISO(fromQ) ? fromQ : defaultFrom;
    const to = isISO(toQ) ? toQ : today;

    const dashboard = await dashboardFromDbAdminClientes({ from, to, area, grupo, categoria, q });
    const analytics = await buildAdminClientesAiAnalytics({ from, to, area, grupo, categoria, q });

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
      detail = await detailsFromDb({ from, to, cardCode: focusCardCode, warehouse: focusWarehouse, area, grupo, categoria });
    }

    const recommendationContext = await buildAdminClientesRecommendationAnalytics({
      from,
      to,
      area,
      grupo,
      categoria,
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
      filters: { area, grupo, categoria, q },
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

const PROD_FINISHED_WHS = ["01", "03", "10", "12", "200", "300", "500"];

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

const PROD_DASHBOARD_CACHE = new Map();
const PROD_DASHBOARD_CACHE_TTL_MS = Math.max(30000, Number(process.env.PROD_DASHBOARD_CACHE_TTL_MS || 300000));

function prodParseMultiValue(raw) {
  const src = Array.isArray(raw) ? raw.join("|") : String(raw || "").trim();
  if (!src || src === "__ALL__") return [];
  return Array.from(new Set(
    src
      .split(/[|,]/g)
      .map((x) => String(x || "").trim())
      .filter(Boolean)
  ));
}
function prodNormalizeTypeFilter(value) {
  const v = String(value || "").trim();
  const n = prodNorm(v);
  if (!v || v === "__ALL__") return "__ALL__";
  if (n.includes("no se fabrica")) return "No se fabrica en planta";
  if (n.includes("se fabrica")) return "Se fabrica en planta";
  return v;
}
function prodMatchesType(item, typeFilter) {
  const sel = prodNormalizeTypeFilter(typeFilter);
  if (sel === "__ALL__") return true;
  const label = prodProcurementMethodLabel(String(item?.procurementMethodLabel || item?.procurementMethod || ""));
  return label === sel;
}
function prodDashboardCacheKey(params) {
  return JSON.stringify({
    from: String(params?.from || ""),
    to: String(params?.to || ""),
    area: String(params?.area || "__ALL__"),
    grupo: prodParseMultiValue(params?.grupo),
    sizeUom: String(params?.sizeUom || "__ALL__"),
    abc: String(params?.abc || "__ALL__"),
    type: prodNormalizeTypeFilter(params?.typeFilter || params?.type || "Se fabrica en planta"),
    q: String(params?.q || "").trim().toLowerCase(),
    avgMonths: Math.max(1, Number(params?.avgMonths || params?.horizonMonths || 3)),
    horizonMonths: Math.max(1, Number(params?.horizonMonths || 3)),
  });
}
function prodDashboardClone(value) {
  return (typeof structuredClone === "function")
    ? structuredClone(value)
    : JSON.parse(JSON.stringify(value));
}
function prodGetDashboardCached(params) {
  const key = prodDashboardCacheKey(params);
  const row = PROD_DASHBOARD_CACHE.get(key);
  if (!row) return null;
  if ((Date.now() - row.ts) > PROD_DASHBOARD_CACHE_TTL_MS) {
    PROD_DASHBOARD_CACHE.delete(key);
    return null;
  }
  return prodDashboardClone(row.data);
}
function prodSetDashboardCached(params, data) {
  const key = prodDashboardCacheKey(params);
  PROD_DASHBOARD_CACHE.set(key, { ts: Date.now(), data: prodDashboardClone(data) });
}
function prodClearDashboardCache() {
  PROD_DASHBOARD_CACHE.clear();
}

const PROD_SIMULATION_CACHE = new Map();
const PROD_SIMULATION_CACHE_TTL_MS = Math.max(30000, Number(process.env.PROD_SIMULATION_CACHE_TTL_MS || 300000));

function prodSimulationClone(value) {
  return (typeof structuredClone === "function")
    ? structuredClone(value)
    : JSON.parse(JSON.stringify(value));
}
function prodSimulationCacheKey(params) {
  return JSON.stringify({
    from: String(params?.from || ""),
    to: String(params?.to || ""),
    area: String(params?.area || "__ALL__"),
    grupo: prodParseMultiValue(params?.grupo),
    sizeUom: String(params?.sizeUom || "__ALL__"),
    abc: String(params?.abc || "__ALL__"),
    type: prodNormalizeTypeFilter(params?.typeFilter || params?.type || "Se fabrica en planta"),
    q: String(params?.q || "").trim().toLowerCase(),
    avgMonths: Math.max(1, Number(params?.avgMonths || params?.horizonMonths || 3)),
    horizonMonths: Math.max(1, Number(params?.horizonMonths || 3)),
    shiftHours: Math.max(1, Number(params?.shiftHours || 8)),
    itemCodes: Array.from(new Set((Array.isArray(params?.itemCodes) ? params.itemCodes : [])
      .map((x) => String(x || "").trim())
      .filter(Boolean)
      .map((x) => prodNormalizeItemCodeLoose(x))
    )).sort(),
    maxDepth: Math.max(1, Number(params?.maxDepth || 3)),
  });
}
function prodGetSimulationCached(params) {
  const key = prodSimulationCacheKey(params);
  const row = PROD_SIMULATION_CACHE.get(key);
  if (!row) return null;
  if ((Date.now() - row.ts) > PROD_SIMULATION_CACHE_TTL_MS) {
    PROD_SIMULATION_CACHE.delete(key);
    return null;
  }
  return prodSimulationClone(row.data);
}
function prodSetSimulationCached(params, data) {
  const key = prodSimulationCacheKey(params);
  PROD_SIMULATION_CACHE.set(key, { ts: Date.now(), data: prodSimulationClone(data) });
}
function prodClearSimulationCache() {
  PROD_SIMULATION_CACHE.clear();
}
function prodClearProductionRuntimeCaches() {
  PROD_ITEM_RUNTIME_CACHE.clear();
  PROD_ORDERS_RUNTIME_CACHE.clear();
  PROD_BOM_RUNTIME_CACHE.clear();
  PROD_PLAN_RUNTIME_CACHE.clear();
}

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
    minimumRunQty: 600,
    familyChangeoverHours: 0.25,
    ratesMode: "PER_SHIFT",
    defaultRates: { SAUCES: 2160, CLEANING: 1800 },
    itemRates: { "68328": 666.67 },
    sizeRates: {
      SAUCES: {
        "24": 2160,
        "10.5": 4800,
        "5.5": 9600,
        "8": 6600,
        "16": 3360,
        "32": 1680,
        "TETRAPACK": 848
      },
      CLEANING_400: {
        "16": 3240,
        "29": 1800,
        "20": 2640,
        "500ML": 2760,
        "500G": 48
      },
      CLEANING_BATH_250: {
        "29": 1140,
        "20": 1728
      },
      CLEANING_REFILL: {
        "800ML": 2640,
        "450ML": 4800,
        "3.1L": 720
      }
    },
    preferredCleaningSource: "FASTEST_AVAILABLE",
    machineNames: { SAUCES: "Máquina de salsas", CLEANING: "Máquina de limpieza" },
  });
  capacity.shiftHours = Math.max(1, prodNum(capacity?.shiftHours || 8, 8));
  capacity.minimumRunQty = Math.max(1, Math.floor(prodNum(capacity?.minimumRunQty || capacity?.minRunQty || 600, 600)));
  capacity.familyChangeoverHours = Math.max(0, Math.min(2, prodNum(capacity?.familyChangeoverHours || 0.25, 0.25)));
  capacity.ratesMode = String(capacity?.ratesMode || capacity?.rateMode || "PER_SHIFT").trim().toUpperCase() || "PER_SHIFT";

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
function prodNormalizeCapacityKey(sizeUom = "", description = "") {
  const sizeRaw = String(sizeUom || "").trim().toUpperCase().replace(/\s+/g, "");
  const desc = prodNorm(description);
  if (/(tetrapack|tetra pak|tetra)/i.test(String(description || "")) || desc.includes('tetrapack')) return 'TETRAPACK';
  if (sizeRaw === '3.1' || sizeRaw === '3.1L' || sizeRaw === '3100' || sizeRaw === '3100ML') return '3.1L';
  if (sizeRaw in {'24':1,'10.5':1,'5.5':1,'8':1,'16':1,'20':1,'29':1,'32':1}) return sizeRaw;
  if (sizeRaw in {'450':1,'450ML':1}) return '450ML';
  if (sizeRaw in {'500':1,'500ML':1}) {
    if (/(\b500\s*g\b|500 g|gram)/i.test(String(description || ''))) return '500G';
    return '500ML';
  }
  if (sizeRaw in {'800':1,'800ML':1}) return '800ML';
  if (sizeRaw in {'1GAL':1,'1':1}) return '1GAL';
  if (/\b450\s*ml\b/i.test(String(description || ''))) return '450ML';
  if (/\b500\s*ml\b/i.test(String(description || ''))) return '500ML';
  if (/\b500\s*g\b/i.test(String(description || ''))) return '500G';
  if (/\b800\s*ml\b/i.test(String(description || ''))) return '800ML';
  if (/\b3(?:[\.,]1)?\s*l\b/i.test(String(description || ''))) return '3.1L';
  return sizeRaw || '';
}
function prodResolveUnitsPerHour({ machine = '', itemCode = '', itemDesc = '', sizeUom = '', capacity = null }) {
  const cap = capacity || loadProductionLocalData().capacity || {};
  const itemRates = cap?.itemRates || {};
  const defaultRates = cap?.defaultRates || {};
  const sizeRates = cap?.sizeRates || {};
  const shiftHours = Math.max(1, prodNum(cap?.shiftHours || 8, 8));
  const ratesMode = String(cap?.ratesMode || cap?.rateMode || 'PER_SHIFT').trim().toUpperCase() || 'PER_SHIFT';
  const normalizeRate = (rawRate, source) => {
    const baseRate = Math.max(0, prodNum(rawRate));
    if (!(baseRate > 0)) return { rate: 0, ratePerShift: 0, source };
    if (ratesMode === 'PER_HOUR') {
      return { rate: baseRate, ratePerShift: prodRound(baseRate * shiftHours, 2), source };
    }
    return { rate: prodRound(baseRate / shiftHours, 4), ratePerShift: baseRate, source };
  };

  const code = String(itemCode || '').trim();
  if (code && prodNum(itemRates[code]) > 0) {
    return normalizeRate(itemRates[code], `item:${code}`);
  }

  const key = prodNormalizeCapacityKey(sizeUom, itemDesc);
  const desc = prodNorm(itemDesc);
  const machineKey = String(machine || '').toUpperCase();

  if (machineKey === 'SAUCES') {
    const source = sizeRates?.SAUCES?.[key] ? `SAUCES:${key}` : 'SAUCES:DEFAULT';
    return normalizeRate(sizeRates?.SAUCES?.[key] || defaultRates?.SAUCES || 0, source);
  }

  if (machineKey === 'CLEANING') {
    const isRefill = /(refil|refill)/i.test(itemDesc || '') || desc.includes('refil') || desc.includes('refill');
    const isBath = /(bano|baño)/i.test(itemDesc || '') || desc.includes('bano');
    const candidates = [];
    const pushCandidate = (source, value) => {
      const normalized = normalizeRate(value, source);
      if (normalized.rate > 0) candidates.push(normalized);
    };

    if (isRefill) pushCandidate(`CLEANING_REFILL:${key}`, sizeRates?.CLEANING_REFILL?.[key]);
    if (isBath) pushCandidate(`CLEANING_BATH_250:${key}`, sizeRates?.CLEANING_BATH_250?.[key]);
    pushCandidate(`CLEANING_400:${key}`, sizeRates?.CLEANING_400?.[key]);

    if (candidates.length) {
      return candidates.sort((a, b) => b.ratePerShift - a.ratePerShift)[0];
    }

    return normalizeRate(defaultRates?.CLEANING || 0, 'CLEANING:DEFAULT');
  }

  return normalizeRate(defaultRates?.[machineKey] || 0, `${machineKey || 'UNKNOWN'}:DEFAULT`);
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


  await dbQuery(`
    CREATE TABLE IF NOT EXISTS production_item_cache (
      item_code TEXT PRIMARY KEY,
      item_desc TEXT NOT NULL DEFAULT '',
      weighted_cost NUMERIC(18,6) NOT NULL DEFAULT 0,
      procurement_method TEXT NOT NULL DEFAULT '',
      planning_system TEXT NOT NULL DEFAULT '',
      lead_time_days NUMERIC(18,4) NOT NULL DEFAULT 0,
      min_order_qty NUMERIC(18,4) NOT NULL DEFAULT 0,
      multiple_qty NUMERIC(18,4) NOT NULL DEFAULT 0,
      item_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      inventory_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS production_orders_cache (
      item_code TEXT NOT NULL,
      doc_num BIGINT NOT NULL,
      absolute_entry BIGINT,
      prod_name TEXT NOT NULL DEFAULT '',
      planned_qty NUMERIC(18,4) NOT NULL DEFAULT 0,
      completed_qty NUMERIC(18,4) NOT NULL DEFAULT 0,
      rejected_qty NUMERIC(18,4) NOT NULL DEFAULT 0,
      post_date DATE,
      status TEXT NOT NULL DEFAULT '',
      warehouse TEXT NOT NULL DEFAULT '',
      origin TEXT NOT NULL DEFAULT '',
      updated_at TIMESTAMP DEFAULT NOW(),
      PRIMARY KEY(item_code, doc_num)
    );
  `);

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS production_bom_cache (
      parent_item_code TEXT NOT NULL,
      line_no INTEGER NOT NULL,
      component_code TEXT NOT NULL DEFAULT '',
      component_desc TEXT NOT NULL DEFAULT '',
      quantity NUMERIC(18,6) NOT NULL DEFAULT 0,
      unit TEXT NOT NULL DEFAULT '',
      warehouse TEXT NOT NULL DEFAULT '',
      issue_method TEXT NOT NULL DEFAULT '',
      bom_header_qty NUMERIC(18,6) NOT NULL DEFAULT 1,
      bom_source TEXT NOT NULL DEFAULT 'SAP ProductTrees',
      updated_at TIMESTAMP DEFAULT NOW(),
      PRIMARY KEY(parent_item_code, line_no)
    );
  `);

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS production_demand_lines (
      doc_entry BIGINT NOT NULL,
      line_num INTEGER NOT NULL,
      doc_type TEXT NOT NULL DEFAULT 'ORD',
      doc_date DATE,
      doc_num BIGINT,
      card_code TEXT NOT NULL DEFAULT '',
      card_name TEXT NOT NULL DEFAULT '',
      item_code TEXT NOT NULL DEFAULT '',
      item_desc TEXT NOT NULL DEFAULT '',
      quantity NUMERIC(18,4) NOT NULL DEFAULT 0,
      revenue NUMERIC(18,2) NOT NULL DEFAULT 0,
      gross_profit NUMERIC(18,2) NOT NULL DEFAULT 0,
      area TEXT NOT NULL DEFAULT '',
      item_group TEXT NOT NULL DEFAULT '',
      updated_at TIMESTAMP DEFAULT NOW(),
      PRIMARY KEY(doc_entry, line_num, doc_type)
    );
  `);

  await dbQuery(`ALTER TABLE production_demand_lines ADD COLUMN IF NOT EXISTS num_per_msr NUMERIC(18,4) NOT NULL DEFAULT 1;`);
  await dbQuery(`ALTER TABLE production_demand_lines ADD COLUMN IF NOT EXISTS quantity_base NUMERIC(18,4) NOT NULL DEFAULT 0;`);
  await dbQuery(`ALTER TABLE production_orders_cache ADD COLUMN IF NOT EXISTS unit_cost NUMERIC(18,6) NOT NULL DEFAULT 0;`);
  await dbQuery(`ALTER TABLE production_orders_cache ADD COLUMN IF NOT EXISTS total_cost NUMERIC(18,6) NOT NULL DEFAULT 0;`);
  await dbQuery(`ALTER TABLE production_orders_cache ADD COLUMN IF NOT EXISTS cost_source TEXT NOT NULL DEFAULT '';`);
  await dbQuery(`ALTER TABLE production_orders_cache ADD COLUMN IF NOT EXISTS receipt_date DATE;`);
  await dbQuery(`ALTER TABLE production_orders_cache ADD COLUMN IF NOT EXISTS receipt_qty NUMERIC(18,4) NOT NULL DEFAULT 0;`);

  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_prod_item_cache_updated ON production_item_cache(updated_at);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_prod_orders_item_date ON production_orders_cache(item_code, post_date DESC);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_prod_bom_parent ON production_bom_cache(parent_item_code);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_prod_demand_item_date ON production_demand_lines(item_code, doc_date DESC);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_prod_demand_date ON production_demand_lines(doc_date DESC);`);

  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_prod_inv_wh_item ON production_inv_wh_cache(item_code);`);

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS production_item_monthly_summary (
      item_code TEXT NOT NULL,
      ym TEXT NOT NULL,
      sales_qty NUMERIC(18,4) NOT NULL DEFAULT 0,
      produced_qty NUMERIC(18,4) NOT NULL DEFAULT 0,
      weighted_cost NUMERIC(18,6) NOT NULL DEFAULT 0,
      avg_production_cost NUMERIC(18,6),
      total_cost_month NUMERIC(18,2),
      abc_label TEXT NOT NULL DEFAULT '',
      source TEXT NOT NULL DEFAULT 'sql_manual',
      updated_at TIMESTAMP DEFAULT NOW(),
      PRIMARY KEY(item_code, ym)
    );
  `);
  await dbQuery(`ALTER TABLE production_item_monthly_summary ADD COLUMN IF NOT EXISTS sales_qty NUMERIC(18,4) NOT NULL DEFAULT 0;`);
  await dbQuery(`ALTER TABLE production_item_monthly_summary ADD COLUMN IF NOT EXISTS produced_qty NUMERIC(18,4) NOT NULL DEFAULT 0;`);
  await dbQuery(`ALTER TABLE production_item_monthly_summary ADD COLUMN IF NOT EXISTS weighted_cost NUMERIC(18,6) NOT NULL DEFAULT 0;`);
  await dbQuery(`ALTER TABLE production_item_monthly_summary ADD COLUMN IF NOT EXISTS avg_production_cost NUMERIC(18,6);`);
  await dbQuery(`ALTER TABLE production_item_monthly_summary ADD COLUMN IF NOT EXISTS total_cost_month NUMERIC(18,2);`);
  await dbQuery(`ALTER TABLE production_item_monthly_summary ADD COLUMN IF NOT EXISTS abc_label TEXT NOT NULL DEFAULT '';`);
  await dbQuery(`ALTER TABLE production_item_monthly_summary ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'sql_manual';`);
  await dbQuery(`ALTER TABLE production_item_monthly_summary ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_prod_item_monthly_summary_item ON production_item_monthly_summary(item_code, ym DESC);`);
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



const PROD_ITEM_RUNTIME_CACHE = new Map();
const PROD_ORDERS_RUNTIME_CACHE = new Map();
const PROD_BOM_RUNTIME_CACHE = new Map();
const PROD_PLAN_RUNTIME_CACHE = new Map();
const PROD_DISPATCH_ALERTS_CACHE = new Map();
const PROD_CACHE_TTL_MS = 6 * 60 * 60 * 1000;
const PROD_PLAN_TTL_MS = 2 * 60 * 1000;
const PROD_DISPATCH_ALERTS_TTL_MS = 2 * 60 * 1000;

const PROD_SPECIAL_DISPATCH_CARD_CODE = 'C01600';
const PROD_SPECIAL_DISPATCH_WAREHOUSE = '01';
const PROD_SPECIAL_DISPATCH_LOOKBACK_DAYS = 120;
const PROD_SPECIAL_DISPATCH_LOOKAHEAD_DAYS = 45;

function prodParseJsonSafe(v, fallback = {}) {
  if (v == null || v === "") return fallback;
  if (typeof v === "object") return v;
  try { return JSON.parse(String(v)); } catch { return fallback; }
}
function prodClone(v) {
  if (v == null) return v;
  if (typeof structuredClone === "function") {
    try { return structuredClone(v); } catch {}
  }
  return JSON.parse(JSON.stringify(v));
}
function prodDispatchItemLookup(indexLike, itemCode = '') {
  const code = String(itemCode || '').trim();
  if (!code || indexLike == null) return null;
  if (typeof indexLike.get === 'function') {
    try { return indexLike.get(code) || null; } catch {}
  }
  if (Array.isArray(indexLike)) {
    const hit = indexLike.find((x) => String(x?.itemCode || '').trim() === code);
    return hit || null;
  }
  if (typeof indexLike === 'object') {
    return indexLike[code] || null;
  }
  return null;
}
function prodCacheFresh(ts, ttlMs = PROD_CACHE_TTL_MS) {
  const ms = new Date(ts || 0).getTime();
  return Number.isFinite(ms) && (Date.now() - ms) < ttlMs;
}
function prodRuntimeGet(map, key, ttlMs = PROD_CACHE_TTL_MS) {
  const hit = map.get(key);
  if (!hit) return null;
  if ((Date.now() - Number(hit.ts || 0)) > ttlMs) {
    map.delete(key);
    return null;
  }
  return prodClone(hit.data);
}
function prodRuntimeSet(map, key, data) {
  map.set(key, { ts: Date.now(), data: prodClone(data) });
}
async function prodReadItemCacheDb(itemCode, ttlMs = PROD_CACHE_TTL_MS) {
  if (!hasDb()) return null;
  const r = await dbQuery(
    `SELECT item_code, item_desc, weighted_cost, procurement_method, planning_system, lead_time_days,
            min_order_qty, multiple_qty, item_json, inventory_json, updated_at
       FROM production_item_cache
      WHERE item_code = $1
      LIMIT 1`,
    [itemCode]
  );
  const row = r.rows?.[0];
  if (!row || !prodCacheFresh(row.updated_at, ttlMs)) return null;
  const item = prodParseJsonSafe(row.item_json, {});
  const inv = prodParseJsonSafe(row.inventory_json, {});
  if (item && typeof item === "object") {
    item.ItemCode = item.ItemCode || row.item_code;
    item.ItemName = item.ItemName || row.item_desc || "";
    if (!Array.isArray(item.ItemWarehouseInfoCollection)) {
      item.ItemWarehouseInfoCollection = Array.isArray(inv?.rows) ? inv.rows : [];
    }
    if ((row.procurement_method || "") && !item.ProcurementMethod) item.ProcurementMethod = row.procurement_method;
    if ((row.planning_system || "") && !item.PlanningSystem) item.PlanningSystem = row.planning_system;
    if (Number(row.lead_time_days || 0) && !item.LeadTimeDays) item.LeadTimeDays = Number(row.lead_time_days);
    if (Number(row.min_order_qty || 0) && !item.MinimumOrderQuantity) item.MinimumOrderQuantity = Number(row.min_order_qty);
    if (Number(row.multiple_qty || 0) && !item.OrderMultiple) item.OrderMultiple = Number(row.multiple_qty);
    if (Number(row.weighted_cost || 0) > 0 && !item.AvgPrice) item.AvgPrice = Number(row.weighted_cost);
    return item;
  }
  return null;
}
async function prodUpsertItemCacheDb(item) {
  if (!hasDb() || !item || !item.ItemCode) return;
  const inv = prodExtractInventorySnapshotFromItem(item);
  const mrp = prodExtractMrpFromItem(item);
  const weightedCost = prodExtractWeightedCostFromItem(item);
  await dbQuery(
    `INSERT INTO production_item_cache(
       item_code, item_desc, weighted_cost, procurement_method, planning_system, lead_time_days,
       min_order_qty, multiple_qty, item_json, inventory_json, updated_at
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10::jsonb,NOW())
     ON CONFLICT (item_code) DO UPDATE SET
       item_desc = EXCLUDED.item_desc,
       weighted_cost = EXCLUDED.weighted_cost,
       procurement_method = EXCLUDED.procurement_method,
       planning_system = EXCLUDED.planning_system,
       lead_time_days = EXCLUDED.lead_time_days,
       min_order_qty = EXCLUDED.min_order_qty,
       multiple_qty = EXCLUDED.multiple_qty,
       item_json = EXCLUDED.item_json,
       inventory_json = EXCLUDED.inventory_json,
       updated_at = NOW()`,
    [
      String(item.ItemCode || "").trim(),
      String(item.ItemName || "").trim(),
      prodNum(weightedCost),
      String(mrp.procurementMethod || ""),
      String(mrp.planningSystem || ""),
      prodNum(mrp.leadTimeDays),
      prodNum(mrp.minOrderQty),
      prodNum(mrp.multipleQty),
      JSON.stringify(item || {}),
      JSON.stringify({ byWarehouse: inv.byWarehouse || {}, total: inv.total || 0, stockMin: inv.stockMin || 0, stockMax: inv.stockMax || 0, rows: Array.isArray(item.ItemWarehouseInfoCollection) ? item.ItemWarehouseInfoCollection : [] }),
    ]
  );
}
async function prodFetchFullItemFromSap(code, { timeoutMs = 120000 } = {}) {
  const itemCode = String(code || "").trim();
  if (!itemCode) return null;
  const safe = itemCode.replace(/'/g, "''");
  const safeTimeout = Math.max(8000, Number(timeoutMs || 120000));

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
      { timeoutMs: safeTimeout }
    );
  } catch (e1) {
    try {
      item = await slFetch(`/Items('${safe}')`, { timeoutMs: safeTimeout });
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
        { timeoutMs: safeTimeout }
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
async function prodGetFullItem(code, { forceFresh = false, ttlMs = PROD_CACHE_TTL_MS, timeoutMs = 120000 } = {}) {
  const itemCode = String(code || "").trim();
  if (!itemCode) return null;
  const runtimeKey = `item::${itemCode}`;
  if (!forceFresh) {
    const hit = prodRuntimeGet(PROD_ITEM_RUNTIME_CACHE, runtimeKey, ttlMs);
    if (hit) return hit;
    const dbHit = await prodReadItemCacheDb(itemCode, ttlMs).catch(() => null);
    if (dbHit) {
      prodRuntimeSet(PROD_ITEM_RUNTIME_CACHE, runtimeKey, dbHit);
      return dbHit;
    }
  }
  if (missingSapEnv()) return null;
  const item = await prodFetchFullItemFromSap(itemCode, { timeoutMs });
  if (item) {
    prodRuntimeSet(PROD_ITEM_RUNTIME_CACHE, runtimeKey, item);
    await prodUpsertItemCacheDb(item).catch(() => {});
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


function prodLineItemsPerUnit(line, itemMaster = null) {
  const candidates = [
    line?.ItemsPerUnit,
    line?.NumPerMsr,
    line?.UnitsOfMeasurment,
    line?.U_ArticulosPorUnidad,
    line?.U_ItemsPerUnit,
    line?.U_CantPorCaja,
    line?.U_CantidadCaja,
    line?.U_QtyPerBox,
  ];
  for (const raw of candidates) {
    const n = Math.abs(Number(raw || 0));
    if (Number.isFinite(n) && n > 0) return n;
  }

  const itemCandidates = [
    itemMaster?.SalesItemsPerUnit,
    itemMaster?.SalesQtyPerPackUnit,
    itemMaster?.SalesQtyPerPackage,
    itemMaster?.SalesPackagingUnit,
    itemMaster?.ItemsPerUnit,
    itemMaster?.NumPerMsr,
  ];
  for (const raw of itemCandidates) {
    const n = Math.abs(Number(raw || 0));
    if (Number.isFinite(n) && n > 0) return n;
  }

  try {
    const coll = Array.isArray(itemMaster?.ItemUnitOfMeasurementCollection)
      ? itemMaster.ItemUnitOfMeasurementCollection
      : [];
    const lineUom = String(
      line?.MeasureUnit ||
      line?.UnitMsr ||
      line?.UoMCode ||
      line?.UomCode ||
      line?.UomEntry ||
      ''
    ).trim().toLowerCase();

    let row = null;
    if (lineUom) {
      row = coll.find((x) => {
        const code = String(x?.UoMCode || x?.UomCode || x?.UoMEntry || '').trim().toLowerCase();
        const type = String(x?.UoMType || '').trim().toLowerCase();
        return code === lineUom || type === lineUom;
      }) || null;
    }
    if (!row) {
      row = coll.find((x) => String(x?.UoMType || '').toLowerCase().includes('sales')) ||
            coll.find((x) => Number(x?.BaseQuantity || 0) > 1) || null;
    }
    if (row) {
      const b = Number(row?.BaseQuantity ?? row?.BaseQty ?? null);
      const a = Number(row?.AlternateQuantity ?? row?.AltQty ?? row?.AlternativeQuantity ?? null);
      if (Number.isFinite(b) && b > 0 && Number.isFinite(a) && a > 0) {
        const f = b / a;
        if (Number.isFinite(f) && f > 0) return f;
      }
      if (Number.isFinite(b) && b > 0) return b;
    }
  } catch {}

  return 1;
}

function prodExtractInventorySnapshotFromItem(item) {
  const byWh = { "01": 0, "03": 0, "10": 0, "12": 0, "200": 0, "300": 0, "500": 0 };
  const whRows = Array.isArray(item?.ItemWarehouseInfoCollection) ? item.ItemWarehouseInfoCollection : [];
  let total = 0;
  let stockMin = 0;
  let stockMax = 0;

  for (const r of whRows) {
    const wh = String(r?.WarehouseCode ?? r?.WhsCode ?? "").trim();
    if (!Object.prototype.hasOwnProperty.call(byWh, wh)) continue;
    const stock = prodNum(r?.InStock ?? r?.OnHand ?? 0);
    const minStock = prodNum(r?.MinimalStock ?? r?.MinStock ?? 0);
    const maxStock = prodNum(r?.MaximalStock ?? r?.MaxStock ?? 0);
    total += stock;
    stockMin += minStock;
    stockMax += maxStock;
    byWh[wh] = prodRound(stock, 3);
  }

  return {
    byWarehouse: byWh,
    total: prodRound(total, 3),
    stockMin: prodRound(stockMin, 3),
    stockMax: prodRound(stockMax, 3),
  };
}

function prodIsIsoDate(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || "").slice(0, 10));
}
function prodToIsoDate(value) {
  if (!value) return "";
  if (value instanceof Date && !Number.isNaN(value.getTime())) return value.toISOString().slice(0, 10);
  const raw = String(value || "").trim();
  if (!raw) return "";
  if (/^\d{4}-\d{2}-\d{2}/.test(raw)) return raw.slice(0, 10);
  const sapTicks = raw.match(/\/Date\((\d+)(?:[+-]\d+)?\)\//i);
  if (sapTicks) {
    const d = new Date(Number(sapTicks[1] || 0));
    if (!Number.isNaN(d.getTime())) return d.toISOString().slice(0, 10);
  }
  if (/^[A-Za-z]{3}\s+[A-Za-z]{3}\s+\d{1,2}$/.test(raw)) return "";
  const parsed = new Date(raw);
  if (!Number.isNaN(parsed.getTime())) return parsed.toISOString().slice(0, 10);
  return "";
}

function prodExtractProductionOrderCosts(r) {
  const plannedQty = prodNum(r?.PlannedQuantity ?? r?.PlannedQty ?? r?.PlannedQtty ?? 0);
  const completedQty = prodNum(r?.CompletedQuantity ?? r?.CmpltQty ?? r?.CompletedQty ?? 0);
  const qtyBase = completedQty > 0 ? completedQty : plannedQty;
  const readNum = (...vals) => {
    for (const v of vals) {
      const n = prodNum(v);
      if (n > 0) return n;
    }
    return 0;
  };

  const directUnit = readNum(
    r?.ActualUnitCost,
    r?.AvgUnitCost,
    r?.AverageUnitCost,
    r?.UnitCost,
    r?.AvgCost,
    r?.AverageCost,
    r?.CalcPrice,
    r?.Price
  );

  const componentCost = readNum(
    r?.ActualComponentCost,
    r?.ActComponentCost,
    r?.ComponentCost,
    r?.CompCost
  );
  const additionalCost = readNum(
    r?.ActualAdditionalCost,
    r?.ActAdditionalCost,
    r?.AdditionalCost,
    r?.AddCost
  );
  const productCost = readNum(
    r?.ActualProductCost,
    r?.ActProductCost,
    r?.ProductCost,
    r?.ProdCost,
    r?.TotalProductCost
  );
  const explicitTotal = readNum(
    r?.ActualTotalCost,
    r?.TotalActualCost,
    r?.TotalCost,
    r?.ProductionCost,
    r?.ActualCost,
    r?.CmpltCost
  );

  let totalCost = 0;
  let costSource = "";
  if (componentCost > 0 || additionalCost > 0 || productCost > 0) {
    totalCost = componentCost + additionalCost + productCost;
    costSource = "SAP ProductionOrder cost components";
  } else if (explicitTotal > 0) {
    totalCost = explicitTotal;
    costSource = "SAP ProductionOrder total cost";
  }

  let unitCost = directUnit > 0 ? directUnit : 0;
  if (!(unitCost > 0) && totalCost > 0 && qtyBase > 0) {
    unitCost = totalCost / qtyBase;
  }
  if (!(totalCost > 0) && unitCost > 0 && qtyBase > 0) {
    totalCost = unitCost * qtyBase;
    if (!costSource) costSource = "SAP ProductionOrder unit cost";
  }
  if (directUnit > 0 && !costSource) costSource = "SAP ProductionOrder unit cost";

  return {
    unitCost: prodRound(unitCost || 0, 6),
    totalCost: prodRound(totalCost || 0, 6),
    costSource: String(costSource || "").trim(),
  };
}

function prodNormalizeProductionOrderRow(r) {
  const postDateRaw = r?.PostingDate ?? r?.PostDate ?? r?.StartDate ?? r?.DueDate ?? "";
  const postDate = prodToIsoDate(postDateRaw);
  const costInfo = prodExtractProductionOrderCosts(r || {});
  return {
    docNum: Number(r?.DocumentNumber ?? r?.DocNum ?? r?.AbsoluteEntry ?? r?.Absoluteentry ?? 0) || null,
    absoluteEntry: Number(r?.AbsoluteEntry ?? r?.Absoluteentry ?? r?.DocEntry ?? 0) || null,
    itemCode: String(r?.ItemNo ?? r?.ItemCode ?? "").trim(),
    prodName: String(r?.ProductDescription ?? r?.ProdName ?? r?.ItemName ?? "").trim(),
    plannedQty: prodRound(r?.PlannedQuantity ?? r?.PlannedQty ?? r?.PlannedQtty ?? 0, 3),
    completedQty: prodRound(r?.CompletedQuantity ?? r?.CmpltQty ?? r?.CompletedQty ?? 0, 3),
    rejectedQty: prodRound(r?.RejectedQuantity ?? r?.RejectedQty ?? 0, 3),
    postDate,
    receiptDate: prodToIsoDate(r?.ReceiptDate ?? r?.receiptDate ?? r?.LastReceiptDate ?? "") || "",
    receiptQty: prodRound(r?.ReceiptQuantity ?? r?.receiptQty ?? 0, 3),
    status: String(r?.ProductionOrderStatus ?? r?.Status ?? "").trim(),
    warehouse: String(r?.Warehouse ?? r?.WarehouseCode ?? r?.WhsCode ?? "").trim(),
    origin: String(r?.ProductionOrderOrigin ?? r?.Origin ?? "").trim(),
    unitCost: costInfo.unitCost,
    totalCost: costInfo.totalCost,
    costSource: costInfo.costSource,
  };
}

function prodIsClosedProductionStatus(status) {
  const s = String(status || "").trim().toUpperCase();
  return s === 'L' || s === 'C' || s === 'CLOSED' || s === 'BOPOSCLOSED' || s === 'BOPS_CLOSED';
}

function prodNormalizeDocumentLines(doc) {
  if (Array.isArray(doc?.DocumentLines)) return doc.DocumentLines;
  if (Array.isArray(doc?.DocumentLines?.value)) return doc.DocumentLines.value;
  if (Array.isArray(doc?.Lines)) return doc.Lines;
  if (Array.isArray(doc?.lines)) return doc.lines;
  return [];
}

function prodInventoryLineMatchesOrder(line, absoluteEntry, itemCode, forReceipt = false) {
  const baseEntry = Number(line?.BaseEntry ?? line?.BaseAbs ?? line?.BaseRefEntry ?? 0) || 0;
  const baseTypeRaw = line?.BaseType ?? line?.BaseObjectType ?? line?.BaseObjType ?? '';
  const baseType = String(baseTypeRaw).trim();
  const baseTypeOk = !baseType || baseType === '202' || baseType === '0xCA' || Number(baseTypeRaw) === 202;
  if (absoluteEntry && baseEntry !== absoluteEntry) return false;
  if (!baseTypeOk) return false;
  if (forReceipt && itemCode) {
    const lineCode = String(line?.ItemCode ?? line?.ItemNo ?? '').trim();
    if (lineCode && lineCode !== String(itemCode || '').trim()) return false;
  }
  return true;
}

function prodExtractInventoryLineQty(line) {
  return prodNum(line?.Quantity ?? line?.Qty ?? line?.BaseQuantity ?? 0);
}

function prodExtractInventoryLineAmount(line) {
  const qty = prodExtractInventoryLineQty(line);
  const direct = prodNum(line?.LineTotal ?? line?.RowTotal ?? line?.Total ?? line?.TotalLC ?? line?.OpenSum ?? 0);
  if (direct > 0) return direct;
  const stockPrice = prodNum(line?.StockPrice ?? line?.AvgPrice ?? line?.Price ?? line?.GrossBuyPrice ?? line?.UnitPrice ?? 0);
  if (stockPrice > 0 && qty > 0) return stockPrice * qty;
  return 0;
}

async function prodFetchInventoryDocsByBase(entitySet, absoluteEntry, itemCode, { forReceipt = false, top = 20 } = {}) {
  const abs = Number(absoluteEntry || 0) || 0;
  if (!abs || missingSapEnv()) return [];
  const lineSelect = 'BaseEntry,BaseType,ItemCode,Quantity,LineTotal,RowTotal,StockPrice,Price,GrossBuyPrice';
  const docSelect = 'DocEntry,DocNum,DocDate';
  const paths = [
    `/${entitySet}?$select=${docSelect}&$expand=DocumentLines($select=${lineSelect})&$filter=DocumentLines/any(d:d/BaseEntry eq ${abs} and d/BaseType eq 202)&$orderby=DocDate desc&$top=${Math.max(5, Number(top || 20))}`,
    `/${entitySet}?$select=${docSelect}&$expand=DocumentLines($select=${lineSelect})&$filter=DocumentLines/any(d:d/BaseEntry eq ${abs})&$orderby=DocDate desc&$top=${Math.max(5, Number(top || 20))}`,
    `/${entitySet}?$select=${docSelect}&$expand=DocumentLines($select=${lineSelect})&$orderby=DocDate desc&$top=${Math.max(40, Number(top || 20) * 4)}`,
  ];
  for (const path of paths) {
    try {
      const res = await slFetch(path, { timeoutMs: 120000 });
      const docs = prodNormalizeSlCollection(res);
      const hits = [];
      for (const doc of docs || []) {
        const lines = prodNormalizeDocumentLines(doc).filter((line) => prodInventoryLineMatchesOrder(line, abs, itemCode, forReceipt));
        if (lines.length) hits.push({ ...doc, __matchedLines: lines });
      }
      if (hits.length) return hits;
    } catch {}
  }
  return [];
}

async function prodFetchProductionOrderActualsFromSap(order) {
  const abs = Number(order?.absoluteEntry || order?.docEntry || 0) || 0;
  const itemCode = String(order?.itemCode || '').trim();
  if (!abs || missingSapEnv()) return null;
  const [receipts, issues] = await Promise.all([
    prodFetchInventoryDocsByBase('InventoryGenEntries', abs, itemCode, { forReceipt: true, top: 20 }).catch(() => []),
    prodFetchInventoryDocsByBase('InventoryGenExits', abs, itemCode, { forReceipt: false, top: 20 }).catch(() => []),
  ]);
  let receiptQty = 0;
  let receiptDate = '';
  for (const doc of receipts || []) {
    const docDate = prodToIsoDate(doc?.DocDate || doc?.TaxDate || doc?.DocDueDate || '');
    if (docDate && (!receiptDate || docDate > receiptDate)) receiptDate = docDate;
    for (const line of doc.__matchedLines || []) receiptQty += prodExtractInventoryLineQty(line);
  }
  let totalCost = 0;
  for (const doc of issues || []) {
    for (const line of doc.__matchedLines || []) totalCost += prodExtractInventoryLineAmount(line);
  }
  receiptQty = prodRound(receiptQty || 0, 3);
  totalCost = prodRound(totalCost || 0, 6);
  const unitCost = receiptQty > 0 && totalCost > 0 ? prodRound(totalCost / receiptQty, 6) : 0;
  if (!(receiptQty > 0) && !(totalCost > 0) && !receiptDate) return null;
  return {
    receiptDate,
    receiptQty,
    totalCost,
    unitCost,
    costSource: totalCost > 0 ? 'SAP issue/receipt actual' : '',
  };
}

async function prodFetchProductionOrderDetailFromSap(order) {
  const abs = Number(order?.absoluteEntry || order?.absoluteentry || order?.docEntry || 0) || null;
  const docNum = Number(order?.docNum || order?.DocumentNumber || 0) || null;
  if ((!abs && !docNum) || missingSapEnv()) return null;

  const tryPaths = [];
  if (abs) {
    tryPaths.push(`/ProductionOrders(${abs})`);
    tryPaths.push(`/ProductionOrders?$filter=AbsoluteEntry eq ${abs}&$top=1`);
    tryPaths.push(`/ProductionOrders?$filter=DocEntry eq ${abs}&$top=1`);
  }
  if (docNum) {
    tryPaths.push(`/ProductionOrders?$filter=DocumentNumber eq ${docNum}&$top=1`);
    tryPaths.push(`/ProductionOrders?$filter=DocNum eq ${docNum}&$top=1`);
  }

  for (const path of tryPaths) {
    try {
      const res = await slFetch(path, { timeoutMs: 120000 });
      const row = Array.isArray(res?.value) ? (res.value[0] || null) : (Array.isArray(res) ? (res[0] || null) : res);
      if (row && typeof row === 'object') return row;
    } catch {}
  }
  return null;
}

async function prodEnrichProductionOrdersCostsFromSap(orders, maxLookups = 40) {
  const src = Array.isArray(orders) ? orders : [];
  if (!src.length || missingSapEnv()) return src;
  let lookups = 0;
  const out = [];
  for (const order of src) {
    const current = { ...(order || {}) };
    const needsDetail = !(prodNum(current.unitCost) > 0 || prodNum(current.totalCost) > 0);
    if (needsDetail && lookups < Math.max(1, Number(maxLookups || 40))) {
      lookups += 1;
      const detail = await prodFetchProductionOrderDetailFromSap(current).catch(() => null);
      if (detail) {
        const merged = { ...detail, ...current, AbsoluteEntry: current.absoluteEntry || detail.AbsoluteEntry || detail.DocEntry, DocumentNumber: current.docNum || detail.DocumentNumber || detail.DocNum, ItemNo: current.itemCode || detail.ItemNo || detail.ItemCode, ItemCode: current.itemCode || detail.ItemCode || detail.ItemNo, ProductDescription: current.prodName || detail.ProductDescription || detail.ProdName || detail.ItemName, PostingDate: current.postDate || detail.PostingDate || detail.PostDate, PostDate: current.postDate || detail.PostDate || detail.PostingDate, PlannedQuantity: prodNum(current.plannedQty) || detail.PlannedQuantity || detail.PlannedQty, CompletedQuantity: prodNum(current.completedQty) || detail.CompletedQuantity || detail.CmpltQty || detail.CompletedQty, RejectedQuantity: prodNum(current.rejectedQty) || detail.RejectedQuantity || detail.RejectedQty };
        const normalized = prodNormalizeProductionOrderRow(merged);
        current.unitCost = normalized.unitCost;
        current.totalCost = normalized.totalCost;
        current.costSource = normalized.costSource || current.costSource || '';
        if (!current.prodName && normalized.prodName) current.prodName = normalized.prodName;
        if (!current.postDate && normalized.postDate) current.postDate = normalized.postDate;
        if (!current.receiptDate && normalized.receiptDate) current.receiptDate = normalized.receiptDate;
        if (!(prodNum(current.receiptQty) > 0) && prodNum(normalized.receiptQty) > 0) current.receiptQty = normalized.receiptQty;
      }
    }
    const needsActuals = prodIsClosedProductionStatus(current.status) && (!(prodNum(current.totalCost) > 0) || !(prodNum(current.receiptQty) > 0) || !prodIsIsoDate(current.receiptDate));
    if (needsActuals && lookups < Math.max(1, Number(maxLookups || 40))) {
      lookups += 1;
      const actuals = await prodFetchProductionOrderActualsFromSap(current).catch(() => null);
      if (actuals) {
        if (prodIsIsoDate(actuals.receiptDate)) current.receiptDate = actuals.receiptDate;
        if (prodNum(actuals.receiptQty) > 0) current.receiptQty = prodNum(actuals.receiptQty);
        if (prodNum(actuals.totalCost) > 0) current.totalCost = prodNum(actuals.totalCost);
        if (prodNum(actuals.unitCost) > 0) current.unitCost = prodNum(actuals.unitCost);
        if (actuals.costSource) current.costSource = actuals.costSource;
      }
    }
    out.push(current);
  }
  return out;
}

async function prodFetchProductionOrdersFromSap(itemCode, top = 80) {
  const code = String(itemCode || "").trim();
  if (!code || missingSapEnv()) return { orders: [], monthly: new Map(), monthlyAvgCost: new Map() };

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
    return { orders: [], monthly: new Map(), monthlyAvgCost: new Map(), warning: lastErr.message || String(lastErr) };
  }

  let orders = raw
    .map(prodNormalizeProductionOrderRow)
    .filter((x) => String(x.itemCode || "") === code || !x.itemCode)
    .sort((a, b) => String(b.postDate || "").localeCompare(String(a.postDate || "")) || Number(b.docNum || 0) - Number(a.docNum || 0));

  if (orders.some((x) => !(prodNum(x.unitCost) > 0 || prodNum(x.totalCost) > 0))) {
    orders = await prodEnrichProductionOrdersCostsFromSap(orders, Math.min(topSafe, 40));
  }

  return prodOrdersToResponse(orders);
}
function prodOrdersToResponse(orders) {
  const monthly = new Map();
  const monthlyCostAgg = new Map();
  const monthlyAvgCost = new Map();
  for (const o of orders || []) {
    const ym = prodYm(o.receiptDate || o.postDate || new Date());
    const producedQty = prodNum(o.receiptQty) > 0 ? prodNum(o.receiptQty) : prodNum(o.completedQty);
    const prev = monthly.get(ym) || 0;
    monthly.set(ym, prodRound(prev + producedQty, 3));

    const qtyForCost = producedQty > 0 ? producedQty : 0;
    const unitCost = prodNum(o.unitCost);
    const totalCost = prodNum(o.totalCost);
    if (prodIsClosedProductionStatus(o.status) && qtyForCost > 0 && (unitCost > 0 || totalCost > 0)) {
      const agg = monthlyCostAgg.get(ym) || { totalCost: 0, qty: 0 };
      agg.totalCost += totalCost > 0 ? totalCost : unitCost * qtyForCost;
      agg.qty += qtyForCost;
      monthlyCostAgg.set(ym, agg);
    }
  }
  for (const [ym, agg] of monthlyCostAgg.entries()) {
    monthlyAvgCost.set(ym, agg.qty > 0 ? prodRound(agg.totalCost / agg.qty, 4) : 0);
  }
  return { orders: Array.isArray(orders) ? orders : [], monthly, monthlyAvgCost };
}
async function prodReadOrdersCacheDb(itemCode, ttlMs = PROD_CACHE_TTL_MS, top = 80) {
  if (!hasDb()) return null;
  const stamp = await dbQuery(`SELECT MAX(updated_at) AS updated_at FROM production_orders_cache WHERE item_code = $1`, [itemCode]);
  const updatedAt = stamp.rows?.[0]?.updated_at;
  if (!updatedAt || !prodCacheFresh(updatedAt, ttlMs)) return null;
  const r = await dbQuery(
    `SELECT item_code, doc_num, absolute_entry, prod_name, planned_qty, completed_qty, rejected_qty,
            post_date, receipt_date, receipt_qty, status, warehouse, origin, unit_cost, total_cost, cost_source
       FROM production_orders_cache
      WHERE item_code = $1
      ORDER BY post_date DESC NULLS LAST, doc_num DESC
      LIMIT $2`,
    [itemCode, Math.max(20, Math.min(200, Number(top || 80)))]
  );
  const orders = (r.rows || []).map((x) => ({
    docNum: Number(x.doc_num || 0) || null,
    absoluteEntry: Number(x.absolute_entry || 0) || null,
    itemCode,
    prodName: String(x.prod_name || ""),
    plannedQty: prodRound(x.planned_qty || 0, 3),
    completedQty: prodRound(x.completed_qty || 0, 3),
    rejectedQty: prodRound(x.rejected_qty || 0, 3),
    postDate: prodToIsoDate(x.post_date),
    receiptDate: prodToIsoDate(x.receipt_date),
    receiptQty: prodRound(x.receipt_qty || 0, 3),
    status: String(x.status || ""),
    warehouse: String(x.warehouse || ""),
    origin: String(x.origin || ""),
    unitCost: prodRound(x.unit_cost || 0, 6),
    totalCost: prodRound(x.total_cost || 0, 6),
    costSource: String(x.cost_source || ""),
  }));
  if (orders.length && orders.every((o) => !prodIsIsoDate(o.postDate))) return null;
  return prodOrdersToResponse(orders);
}
async function prodUpsertOrdersCacheDb(itemCode, orders) {
  if (!hasDb() || !itemCode) return;
  await dbQuery(`DELETE FROM production_orders_cache WHERE item_code = $1`, [itemCode]);
  for (const o of Array.isArray(orders) ? orders : []) {
    await dbQuery(
      `INSERT INTO production_orders_cache(
         item_code, doc_num, absolute_entry, prod_name, planned_qty, completed_qty, rejected_qty,
         post_date, receipt_date, receipt_qty, status, warehouse, origin, unit_cost, total_cost, cost_source, updated_at
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,NOW())
       ON CONFLICT (item_code, doc_num) DO UPDATE SET
         absolute_entry = EXCLUDED.absolute_entry,
         prod_name = EXCLUDED.prod_name,
         planned_qty = EXCLUDED.planned_qty,
         completed_qty = EXCLUDED.completed_qty,
         rejected_qty = EXCLUDED.rejected_qty,
         post_date = EXCLUDED.post_date,
         receipt_date = EXCLUDED.receipt_date,
         receipt_qty = EXCLUDED.receipt_qty,
         status = EXCLUDED.status,
         warehouse = EXCLUDED.warehouse,
         origin = EXCLUDED.origin,
         unit_cost = EXCLUDED.unit_cost,
         total_cost = EXCLUDED.total_cost,
         cost_source = EXCLUDED.cost_source,
         updated_at = NOW()`,
      [
        itemCode,
        Number(o.docNum || 0),
        Number(o.absoluteEntry || 0) || null,
        String(o.prodName || ""),
        prodNum(o.plannedQty),
        prodNum(o.completedQty),
        prodNum(o.rejectedQty),
        prodToIsoDate(o.postDate) || null,
        prodToIsoDate(o.receiptDate) || null,
        prodNum(o.receiptQty),
        String(o.status || ""),
        String(o.warehouse || ""),
        String(o.origin || ""),
        prodNum(o.unitCost),
        prodNum(o.totalCost),
        String(o.costSource || ""),
      ]
    );
  }
}
async function prodFetchProductionOrders(itemCode, top = 80, { forceFresh = false, ttlMs = PROD_CACHE_TTL_MS } = {}) {
  const code = String(itemCode || "").trim();
  if (!code) return { orders: [], monthly: new Map(), monthlyAvgCost: new Map() };
  const runtimeKey = `orders::${code}::${Math.max(20, Math.min(200, Number(top || 80)))}`;
  if (!forceFresh) {
    const hit = prodRuntimeGet(PROD_ORDERS_RUNTIME_CACHE, runtimeKey, ttlMs);
    if (hit) return { orders: hit.orders || [], monthly: new Map(Object.entries(hit.monthly || {})), monthlyAvgCost: new Map(Object.entries(hit.monthlyAvgCost || {})) };
    const dbHit = await prodReadOrdersCacheDb(code, ttlMs, top).catch(() => null);
    if (dbHit) {
      prodRuntimeSet(PROD_ORDERS_RUNTIME_CACHE, runtimeKey, { orders: dbHit.orders, monthly: Object.fromEntries(dbHit.monthly.entries()), monthlyAvgCost: Object.fromEntries((dbHit.monthlyAvgCost || new Map()).entries()) });
      return dbHit;
    }
  }
  const sapHit = await prodFetchProductionOrdersFromSap(code, top);
  prodRuntimeSet(PROD_ORDERS_RUNTIME_CACHE, runtimeKey, { orders: sapHit.orders, monthly: Object.fromEntries((sapHit.monthly || new Map()).entries()), monthlyAvgCost: Object.fromEntries((sapHit.monthlyAvgCost || new Map()).entries()) });
  await prodUpsertOrdersCacheDb(code, sapHit.orders).catch(() => {});
  return sapHit;
}


async function prodReadMonthlySummaryDb(itemCode, fromDate, toDate) {
  const code = String(itemCode || '').trim();
  if (!code || !hasDb()) {
    return { found: false, sales: new Map(), produced: new Map(), avgCost: new Map(), weightedCost: 0, source: '' };
  }
  const fromYm = String(fromDate || '').slice(0, 7);
  const toYm = String(toDate || '').slice(0, 7);
  if (!fromYm || !toYm) {
    return { found: false, sales: new Map(), produced: new Map(), avgCost: new Map(), weightedCost: 0, source: '' };
  }
  try {
    const r = await dbQuery(
      `SELECT ym, sales_qty, produced_qty, weighted_cost, avg_production_cost, total_cost_month, source
         FROM production_item_monthly_summary
        WHERE item_code = $1
          AND ym >= $2
          AND ym <= $3
        ORDER BY ym`,
      [code, fromYm, toYm]
    );
    const rows = Array.isArray(r.rows) ? r.rows : [];
    if (!rows.length) {
      return { found: false, sales: new Map(), produced: new Map(), avgCost: new Map(), totalCostMonth: new Map(), weightedCost: 0, source: '' };
    }
    const sales = new Map();
    const produced = new Map();
    const avgCost = new Map();
    const totalCostMonth = new Map();
    let weightedCost = 0;
    let source = '';
    for (const row of rows) {
      const ym = String(row.ym || '').slice(0, 7);
      if (!ym) continue;
      sales.set(ym, prodNum(row.sales_qty));
      produced.set(ym, prodNum(row.produced_qty));
      avgCost.set(ym, prodNum(row.avg_production_cost));
      totalCostMonth.set(ym, prodNum(row.total_cost_month));
      if (!(weightedCost > 0) && prodNum(row.weighted_cost) > 0) weightedCost = prodNum(row.weighted_cost);
      if (!source && String(row.source || '').trim()) source = String(row.source || '').trim();
    }
    return {
      found: sales.size > 0 || produced.size > 0 || avgCost.size > 0 || totalCostMonth.size > 0,
      sales,
      produced,
      avgCost,
      totalCostMonth,
      weightedCost,
      source: source || 'production_item_monthly_summary',
    };
  } catch {
    return { found: false, sales: new Map(), produced: new Map(), avgCost: new Map(), totalCostMonth: new Map(), weightedCost: 0, source: '' };
  }
}

function prodLooksLikeResource({ code, description = "", item = null, line = null }) {
  const txt = `${code || ""} ${description || ""} ${item?.ItemName || ""} ${item?.ForeignName || ""} ${line?.IssueMethod || ""} ${line?.ItemType || ""} ${line?.Type || ""}`.toLowerCase();
  const codeTxt = String(code || "").trim().toLowerCase();
  const inventoryFlag = String(item?.InventoryItem || "").trim().toLowerCase();
  if (/(operari|supervisor|linea de produccion|línea de producción|mano de obra|recurso|resource|labor|overhead|gastos? de fabricaci|horas? hombre|maquina de limpieza|maquina de salsas|servicio interno)/i.test(txt)) return true;
  if (/^(ogf|mli|mlim|mosup|momez|m00\d|mo\d{2,}|res)/i.test(codeTxt)) return true;
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
  const preferredList = Array.isArray(preferredWh)
    ? preferredWh.map((x) => String(x || "").trim()).filter(Boolean)
    : String(preferredWh || "")
        .split(",")
        .map((x) => String(x || "").trim())
        .filter(Boolean);
  const preferredSet = new Set(preferredList);
  let stockSpecific = 0;
  let availableSpecific = 0;
  let hasSpecific = false;
  let stockTotal = 0;
  let availableTotal = 0;
  const byWarehouse = {};

  for (const r of whRows) {
    const wh = String(r?.WarehouseCode ?? r?.WhsCode ?? "").trim();
    const stock = prodNum(r?.InStock ?? r?.OnHand ?? 0);
    const committed = prodNum(r?.Committed ?? r?.IsCommited ?? 0);
    const ordered = prodNum(r?.Ordered ?? r?.OnOrder ?? 0);
    const available = stock - committed + ordered;

    stockTotal += stock;
    availableTotal += available;
    byWarehouse[wh] = prodRound(stock, 3);

    if (preferredSet.size && preferredSet.has(wh)) {
      stockSpecific += stock;
      availableSpecific += available;
      hasSpecific = true;
    }
  }

  return {
    stockQty: prodRound(hasSpecific ? stockSpecific : stockTotal, 3),
    availableQty: prodRound(hasSpecific ? availableSpecific : availableTotal, 3),
    stockTotal: prodRound(stockTotal, 3),
    availableTotal: prodRound(availableTotal, 3),
    byWarehouse,
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


async function prodFetchSapBomFromSap(itemCode) {
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
async function prodReadBomCacheDb(itemCode, ttlMs = PROD_CACHE_TTL_MS) {
  if (!hasDb()) return null;
  const stamp = await dbQuery(`SELECT MAX(updated_at) AS updated_at FROM production_bom_cache WHERE parent_item_code = $1`, [itemCode]);
  const updatedAt = stamp.rows?.[0]?.updated_at;
  if (!updatedAt || !prodCacheFresh(updatedAt, ttlMs)) return null;
  const r = await dbQuery(
    `SELECT parent_item_code, line_no, component_code, component_desc, quantity, unit, warehouse, issue_method, bom_header_qty, bom_source
       FROM production_bom_cache
      WHERE parent_item_code = $1
      ORDER BY line_no ASC`,
    [itemCode]
  );
  const rows = r.rows || [];
  if (!rows.length) return { source: "SAP ProductTrees", tree: null, headerQty: 1, lines: [] };
  return {
    source: String(rows[0].bom_source || "SAP ProductTrees"),
    tree: null,
    headerQty: prodNum(rows[0].bom_header_qty || 1),
    lines: rows.map((x) => ({
      code: String(x.component_code || ""),
      description: String(x.component_desc || ""),
      quantity: prodNum(x.quantity || 0),
      unit: String(x.unit || ""),
      warehouse: String(x.warehouse || ""),
      issueMethod: String(x.issue_method || ""),
      raw: {
        ItemCode: String(x.component_code || ""),
        ItemDescription: String(x.component_desc || ""),
        Quantity: prodNum(x.quantity || 0),
        UoMCode: String(x.unit || ""),
        Warehouse: String(x.warehouse || ""),
        IssueMethod: String(x.issue_method || ""),
      },
    })),
  };
}
async function prodUpsertBomCacheDb(itemCode, bom) {
  if (!hasDb() || !itemCode) return;
  await dbQuery(`DELETE FROM production_bom_cache WHERE parent_item_code = $1`, [itemCode]);
  const lines = Array.isArray(bom?.lines) ? bom.lines : [];
  const headerQty = Math.max(1, prodNum(bom?.headerQty || 1));
  let lineNo = 0;
  for (const line of lines) {
    await dbQuery(
      `INSERT INTO production_bom_cache(
         parent_item_code, line_no, component_code, component_desc, quantity, unit, warehouse, issue_method, bom_header_qty, bom_source, updated_at
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())`,
      [
        itemCode,
        ++lineNo,
        String(line.code || ""),
        String(line.description || ""),
        prodNum(line.quantity),
        String(line.unit || ""),
        String(line.warehouse || ""),
        String(line.issueMethod || ""),
        headerQty,
        String(bom?.source || "SAP ProductTrees"),
      ]
    );
  }
}
async function prodFetchSapBom(itemCode, { forceFresh = false, ttlMs = PROD_CACHE_TTL_MS } = {}) {
  const code = String(itemCode || "").trim();
  if (!code) return { source: "SAP ProductTrees", tree: null, headerQty: 1, lines: [] };
  const runtimeKey = `bom::${code}`;
  if (!forceFresh) {
    const hit = prodRuntimeGet(PROD_BOM_RUNTIME_CACHE, runtimeKey, ttlMs);
    if (hit) return hit;
    const dbHit = await prodReadBomCacheDb(code, ttlMs).catch(() => null);
    if (dbHit) {
      prodRuntimeSet(PROD_BOM_RUNTIME_CACHE, runtimeKey, dbHit);
      return dbHit;
    }
  }
  const sapHit = await prodFetchSapBomFromSap(code);
  prodRuntimeSet(PROD_BOM_RUNTIME_CACHE, runtimeKey, sapHit);
  await prodUpsertBomCacheDb(code, sapHit).catch(() => {});
  return sapHit;
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
      const preferredWh = ["03", "12"];
      const stockInfo = isResource ? { stockQty: 0, availableQty: 0, stockTotal: 0, byWarehouse: {} } : prodExtractComponentStockInfo(item, preferredWh);
      const stockQty = isResource ? 0 : prodNum(stockInfo.stockTotal || stockInfo.stockQty || 0);
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
        procurementMethod: String(item?.ProcurementMethod || item?.IssueMethod || "").trim(),
        procurementMethodLabel: prodProcurementMethodLabel(String(item?.ProcurementMethod || item?.IssueMethod || "").trim()),
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
  const limit = Math.max(100, Math.min(8000, Number(maxItems || 2500)));
  const r = await dbQuery(
    `
    WITH codes AS (
      SELECT DISTINCT item_code
      FROM sales_item_lines
      WHERE item_code <> ''
        AND doc_date >= LEAST($1::date, DATE '2025-01-01')
        AND doc_date <= $2::date

      UNION

      SELECT DISTINCT item_code
      FROM production_demand_lines
      WHERE item_code <> ''
        AND doc_date >= $1::date
        AND doc_date <= $2::date

      UNION

      SELECT DISTINCT item_code
      FROM production_inv_wh_cache
      WHERE item_code <> ''

      UNION

      SELECT DISTINCT item_code
      FROM production_mrp_cache
      WHERE item_code <> ''

      UNION

      SELECT DISTINCT item_code
      FROM item_group_cache
      WHERE item_code <> ''
    )
    SELECT item_code
    FROM codes
    WHERE item_code <> ''
    ORDER BY item_code
    LIMIT $3
    `,
    [from, to, limit]
  );
  const codes = (r.rows || []).map((x) => String(x.item_code || "").trim()).filter(Boolean);
  let saved = 0;
  const errors = [];

  for (let i = 0; i < codes.length; i++) {
    try {
      const itemCode = codes[i];
      const it = await prodGetFullItem(itemCode, { forceFresh: true, ttlMs: 0 });
      if (!it) throw new Error('SAP no devolvió item');
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


function prodCoverageMonthsByLabel(totalLabel) {
  const t = String(totalLabel || '').toLowerCase();
  if (t.includes('ab crítico') || t.includes('ab critico')) return 2;
  if (t.includes('c importante')) return 1;
  if (/^d/.test(t) || t.includes(' d ')) return 0.5;
  return 1;
}

function prodCoverageLabel(months) {
  const n = Number(months || 0);
  if (n === 2) return '2 meses de inv';
  if (n === 1) return '1 mes de inv';
  if (n === 0.5) return '0.5 mes de inv';
  return `${prodRound(n,2)} mes(es) de inv`;
}

async function prodScanDocHeadersForOrders({ from, to, maxDocs = 4000 }) {
  if (typeof scanDocHeaders === "function") {
    try { return await scanDocHeaders("Orders", { from, to, maxDocs }); } catch {}
    try { return await scanDocHeaders("Orders", { f: from, t: to, maxDocs }); } catch {}
  }
  const fromDate = String(from || "").slice(0,10);
  const toDate = String(to || "").slice(0,10);
  if (!fromDate || !toDate) throw new Error('prodScanDocHeadersForOrders requiere rango de fechas');
  const top = Math.max(50, Math.min(20000, Number(maxDocs || 4000)));
  const entity = "Orders";
  const dayChunks = [];
  let cur = fromDate;
  while (cur <= toDate) {
    const nxt = addDaysISO(cur, 29);
    const end = nxt < toDate ? nxt : toDate;
    dayChunks.push([cur, end]);
    cur = addDaysISO(end, 1);
  }
  const headers = [];
  const seen = new Set();
  for (const [f, t] of dayChunks) {
    let skip = 0;
    while (skip < top) {
      const filter = `DocDate ge '${f}' and DocDate le '${t}' and Cancelled eq 'tNO'`;
      const path = `/${entity}?$select=DocEntry,DocNum,DocDate,CardCode,CardName&$filter=${encodeURIComponent(filter)}&$orderby=DocEntry asc&$top=200&$skip=${skip}`;
      const res = await slFetch(path, { timeoutMs: 120000 });
      const batch = prodNormalizeSlCollection(res);
      if (!batch.length) break;
      for (const row of batch) {
        const key = String(row?.DocEntry ?? row?.DocNum ?? '').trim();
        if (!key || seen.has(key)) continue;
        seen.add(key);
        headers.push(row);
      }
      if (batch.length < 200) break;
      skip += batch.length;
      if (headers.length >= top) break;
    }
    if (headers.length >= top) break;
  }
  headers.sort((a, b) => String(a?.DocDate || '').localeCompare(String(b?.DocDate || '')) || Number(a?.DocEntry || 0) - Number(b?.DocEntry || 0));
  return headers.slice(0, top);
}

async function syncProductionDemandOrders({ from, to, maxDocs = 4000 }) {
  const headers = await prodScanDocHeadersForOrders({ from, to, maxDocs });
  let saved = 0;
  const itemMasterCache = new Map();
  for (const h of headers) {
    try {
      const full = await getDoc('Orders', h.DocEntry);
      const docEntry = Number(full?.DocEntry || h.DocEntry || 0);
      const docNum = full?.DocNum != null ? Number(full.DocNum) : (h.DocNum != null ? Number(h.DocNum) : null);
      const cardCode = String(full?.CardCode || '').trim();
      const cardName = String(full?.CardName || '').trim();
      const lines = Array.isArray(full?.DocumentLines) ? full.DocumentLines : [];
      for (const ln of lines) {
        const lineNum = Number(ln?.LineNum);
        if (!Number.isFinite(lineNum)) continue;
        const itemCode = String(ln?.ItemCode || '').trim();
        if (!itemCode) continue;
        const itemDesc = String(ln?.ItemDescription || ln?.ItemName || '').trim();
        const qty = Math.abs(Number(ln?.Quantity || 0));
        let itemMaster = null;
        const lineNeedsFallback = !(Number(Math.abs(Number(ln?.ItemsPerUnit || 0))) > 0 || Number(Math.abs(Number(ln?.NumPerMsr || 0))) > 0);
        if (lineNeedsFallback) {
          if (itemMasterCache.has(itemCode)) itemMaster = itemMasterCache.get(itemCode);
          else {
            itemMaster = await prodGetFullItem(itemCode).catch(() => null);
            itemMasterCache.set(itemCode, itemMaster || null);
          }
        }
        const numPerMsr = prodLineItemsPerUnit(ln, itemMaster);
        const qtyBase = qty * numPerMsr;
        const rev = Math.abs(Number(ln?.LineTotal || 0));
        const gp = Math.abs(Number(pickGrossProfit(ln) || 0));
        await dbQuery(`
          INSERT INTO production_demand_lines(
            doc_entry,line_num,doc_type,doc_date,doc_num,card_code,card_name,
            item_code,item_desc,quantity,num_per_msr,quantity_base,revenue,gross_profit,updated_at
          ) VALUES($1,$2,'ORD',$3::date,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NOW())
          ON CONFLICT(doc_entry,line_num,doc_type) DO UPDATE SET
            doc_date=EXCLUDED.doc_date,
            doc_num=EXCLUDED.doc_num,
            card_code=EXCLUDED.card_code,
            card_name=EXCLUDED.card_name,
            item_code=EXCLUDED.item_code,
            item_desc=EXCLUDED.item_desc,
            quantity=EXCLUDED.quantity,
            num_per_msr=EXCLUDED.num_per_msr,
            quantity_base=EXCLUDED.quantity_base,
            revenue=EXCLUDED.revenue,
            gross_profit=EXCLUDED.gross_profit,
            updated_at=NOW()
        `,[docEntry,lineNum,String(h.DocDate||'').slice(0,10),docNum,cardCode,cardName,itemCode,itemDesc,qty,numPerMsr,qtyBase,rev,gp]);
        saved += 1;
      }
    } catch {}
    await sleep(10);
  }
  
return saved;
}

function prodDocLineBaseQty(ln = {}) {
  const factor = Math.max(1, prodNum(ln?.NumPerMsr ?? ln?.ItemsPerUnit ?? 1, 1));
  const qty = Math.max(0, prodNum(ln?.Quantity ?? ln?.OpenQty ?? 0));
  return prodRound(qty * factor, 4);
}

function prodDaysLateFromTo(dueDate, refDate) {
  const due = new Date(`${String(dueDate || '').slice(0,10)}T00:00:00`);
  const ref = new Date(`${String(refDate || '').slice(0,10)}T00:00:00`);
  if (Number.isNaN(due.getTime()) || Number.isNaN(ref.getTime())) return 0;
  return Math.max(0, Math.floor((ref.getTime() - due.getTime()) / 86400000));
}

function prodDaysUntilFromTo(targetDate, refDate) {
  const target = new Date(`${String(targetDate || '').slice(0,10)}T00:00:00`);
  const ref = new Date(`${String(refDate || '').slice(0,10)}T00:00:00`);
  if (Number.isNaN(target.getTime()) || Number.isNaN(ref.getTime())) return 0;
  return Math.floor((target.getTime() - ref.getTime()) / 86400000);
}

function prodDocStatusIsOpen(status) {
  const s = String(status || '').trim().toLowerCase();
  return s === 'bost_open' || s === 'bo_open' || s === 'open' || s === 'o';
}
function prodDocStatusIsClosed(status) {
  const s = String(status || '').trim().toLowerCase();
  return s === 'bost_closed' || s === 'bo_closed' || s === 'closed' || s === 'c';
}

async function prodFetchDocHeadersByDateRange(entity, { from, to, maxDocs = 400, cardCode = '' } = {}) {
  if (missingSapEnv()) return [];
  const fromDate = String(from || '').slice(0,10);
  const toDate = String(to || '').slice(0,10);
  if (!fromDate || !toDate) return [];
  const top = Math.max(50, Math.min(2000, Number(maxDocs || 400)));
  const chunks = [];
  let cur = fromDate;
  while (cur <= toDate) {
    const end = addDaysISO(cur, 29) < toDate ? addDaysISO(cur, 29) : toDate;
    chunks.push([cur, end]);
    cur = addDaysISO(end, 1);
  }
  const safeCardCode = String(cardCode || '').replace(/'/g, "''");
  const out = [];
  const seen = new Set();
  for (const [f, t] of chunks) {
    let skip = 0;
    while (skip < top) {
      let filter = `DocDate ge '${f}' and DocDate le '${t}' and Cancelled eq 'tNO'`;
      if (safeCardCode) filter += ` and CardCode eq '${safeCardCode}'`;
      const path = `/${entity}?$select=DocEntry,DocNum,DocDate,DocDueDate,CardCode,CardName,DocumentStatus&$filter=${encodeURIComponent(filter)}&$orderby=DocDate desc,DocEntry desc&$top=200&$skip=${skip}`;
      const res = await slFetch(path, { timeoutMs: 120000 });
      const batch = prodNormalizeSlCollection(res);
      if (!batch.length) break;
      for (const row of batch) {
        const key = String(row?.DocEntry ?? row?.DocNum ?? '').trim();
        if (!key || seen.has(key)) continue;
        seen.add(key);
        out.push(row);
      }
      if (batch.length < 200 || out.length >= top) break;
      skip += batch.length;
    }
    if (out.length >= top) break;
  }
  return out.slice(0, top);
}

async function prodFetchOrderHeadersByCardCodeLoose({ cardCode, maxDocs = 400 } = {}) {
  if (missingSapEnv()) return [];
  const safeCardCode = String(cardCode || '').trim().replace(/'/g, "''");
  if (!safeCardCode) return [];
  const out = [];
  const seen = new Set();
  const top = Math.max(50, Math.min(2000, Number(maxDocs || 400)));
  const filterVariants = [
    `CardCode eq '${safeCardCode}' and Cancelled eq 'tNO'`,
    `CardCode eq '${safeCardCode}'`,
  ];
  for (const filter of filterVariants) {
    let skip = 0;
    while (skip < top) {
      const path = `/Orders?$select=DocEntry,DocNum,DocDate,DocDueDate,CardCode,CardName,DocumentStatus,Cancelled&$filter=${encodeURIComponent(filter)}&$orderby=DocDate desc,DocEntry desc&$top=200&$skip=${skip}`;
      let batch = [];
      try {
        const res = await slFetch(path, { timeoutMs: 120000 });
        batch = prodNormalizeSlCollection(res);
      } catch {
        batch = [];
      }
      if (!batch.length) break;
      for (const row of batch) {
        const key = String(row?.DocEntry ?? row?.DocNum ?? '').trim();
        if (!key || seen.has(key)) continue;
        seen.add(key);
        out.push(row);
      }
      if (batch.length < 200 || out.length >= top) break;
      skip += batch.length;
    }
    if (out.length >= top) break;
  }
  return out.slice(0, top);
}

function prodDispatchHeaderDateRelevant(h = {}, { from = '', to = '', today = '' } = {}) {
  const docDate = String(h?.DocDate || '').slice(0, 10);
  const dueDate = String(h?.DocDueDate || h?.DueDate || '').slice(0, 10);
  const status = String(h?.DocumentStatus || '').trim();
  if (!status || prodDocStatusIsOpen(status)) return true;
  if (dueDate && today && dueDate <= addDaysISO(today, 45)) return true;
  if (docDate && from && to && docDate >= from && docDate <= to) return true;
  return false;
}

async function prodBuildCustomerDispatchAlerts({ cardCode = PROD_SPECIAL_DISPATCH_CARD_CODE, warehouse = PROD_SPECIAL_DISPATCH_WAREHOUSE, today = getDateISOInOffset(TZ_OFFSET_MIN), lookbackDays = PROD_SPECIAL_DISPATCH_LOOKBACK_DAYS, lookaheadDays = PROD_SPECIAL_DISPATCH_LOOKAHEAD_DAYS, maxDocs = 250 } = {}) {
  const cacheKey = JSON.stringify({ v: 12, cardCode, warehouse, today, lookbackDays, lookaheadDays, maxDocs });
  const hit = prodRuntimeGet(PROD_DISPATCH_ALERTS_CACHE, cacheKey, PROD_DISPATCH_ALERTS_TTL_MS);
  if (hit) return hit;

  const safeSummary = (extra = {}) => ({
    cardCode: String(cardCode || '').trim(),
    warehouse: String(warehouse || '').trim(),
    orderHeadersFound: 0,
    ordersFound: 0,
    orderDetailsRead: 0,
    orderDetailErrors: 0,
    pendingLines: 0,
    pendingItems: 0,
    overdueItems: 0,
    pendingQty: 0,
    overdueQty: 0,
    latestDueDate: '',
    ...extra,
  });

  if (missingSapEnv()) {
    const empty = { alerts: [], byItem: [], byItemMap: new Map(), byItemIndex: {}, summary: safeSummary({ error: 'SAP no configurado' }) };
    prodRuntimeSet(PROD_DISPATCH_ALERTS_CACHE, cacheKey, empty);
    return empty;
  }

  const from = addDaysISO(today, -Math.max(1, Number(lookbackDays || 120)));
  const to = addDaysISO(today, Math.max(1, Number(lookaheadDays || 45)));
  const targetWarehouse = String(warehouse || '').trim();
  const targetCardCode = String(cardCode || '').trim();

  const normWh = (ln = {}) => String(
    ln?.WarehouseCode ??
    ln?.WhsCode ??
    ln?.Warehouse ??
    ln?.FromWarehouseCode ??
    ln?.Whs ??
    ''
  ).trim();

  const headerMap = new Map();
  const pushHeaders = (rows = []) => {
    for (const row of Array.isArray(rows) ? rows : []) {
      if (String(row?.CardCode || '').trim() !== targetCardCode) continue;
      const key = String(row?.DocEntry ?? row?.DocNum ?? '').trim();
      if (!key) continue;
      if (!prodDispatchHeaderDateRelevant(row, { from, to, today })) continue;
      if (!headerMap.has(key)) headerMap.set(key, row);
    }
  };

  pushHeaders(await prodFetchDocHeadersByDateRange('Orders', { from, to, maxDocs: Math.max(100, maxDocs * 2), cardCode }).catch(() => []));
  if (!headerMap.size) {
    pushHeaders(await prodScanDocHeadersForOrders({ from, to, maxDocs: Math.max(50, maxDocs) }).catch(() => []));
  }
  pushHeaders(await prodFetchOrderHeadersByCardCodeLoose({ cardCode, maxDocs: Math.max(150, maxDocs * 3) }).catch(() => []));

  const orderHeaders = Array.from(headerMap.values()).sort((a, b) =>
    String(b?.DocDate || '').localeCompare(String(a?.DocDate || '')) ||
    (Number(b?.DocEntry || 0) - Number(a?.DocEntry || 0))
  );

  const fetchFullOrder = async (docEntry) => {
    const de = Number(docEntry || 0);
    if (!(de > 0)) return null;
    let full = null;
    try { full = await getDoc('Orders', de); } catch {}
    let lines = Array.isArray(full?.DocumentLines) ? full.DocumentLines : [];

    if (!lines.length) {
      try {
        const expanded = await slFetch(`/Orders(${de})?$expand=DocumentLines`, { timeoutMs: 180000 });
        if (expanded) {
          full = expanded;
          lines = Array.isArray(expanded?.DocumentLines) ? expanded.DocumentLines : [];
        }
      } catch {}
    }

    if (!lines.length) {
      try {
        const search = await slFetch(`/Orders?$filter=${encodeURIComponent(`DocEntry eq ${de}`)}&$expand=DocumentLines&$top=1`, { timeoutMs: 180000 });
        const rows = prodNormalizeSlCollection(search);
        if (rows.length) {
          full = rows[0];
          lines = Array.isArray(full?.DocumentLines) ? full.DocumentLines : [];
        }
      } catch {}
    }

    // Fallback crítico para ambientes donde Service Layer no expone DocumentLines
    // dentro del documento expandido, pero sí responde la colección hija.
    if (!lines.length) {
      const linePaths = [
        `/Orders(${de})/DocumentLines`,
        `/Orders(${de})/DocumentLines?$top=500`,
        `/Orders?$filter=${encodeURIComponent(`DocEntry eq ${de}`)}&$select=DocEntry,DocNum,CardCode,CardName,DocDate,DocDueDate,DocumentStatus&$top=1`,
      ];
      for (const p of linePaths) {
        try {
          const raw = await slFetch(p, { timeoutMs: 180000 });
          const childRows = Array.isArray(raw?.DocumentLines)
            ? raw.DocumentLines
            : Array.isArray(raw?.value)
              ? raw.value
              : Array.isArray(raw)
                ? raw
                : prodNormalizeSlCollection(raw);
          if (Array.isArray(childRows) && childRows.length) {
            lines = childRows;
            full = {
              ...(full || {}),
              ...(p.includes('$select=') ? (Array.isArray(raw?.value) && raw.value[0] ? raw.value[0] : (Array.isArray(raw) && raw[0] ? raw[0] : {})) : {}),
              DocEntry: Number((full || {}).DocEntry || de),
              DocNum: (full || {}).DocNum ?? undefined,
              CardCode: String((full || {}).CardCode || ''),
              CardName: String((full || {}).CardName || ''),
              DocDate: String((full || {}).DocDate || ''),
              DocDueDate: String((full || {}).DocDueDate || ''),
              DocumentStatus: (full || {}).DocumentStatus || '',
              DocumentLines: childRows,
            };
            break;
          }
        } catch {}
      }
    }

    return full;
  };

  const orders = [];
  let orderDetailsRead = 0;
  let orderDetailErrors = 0;
  for (const h of orderHeaders) {
    try {
      const full = await fetchFullOrder(h?.DocEntry);
      const lines = Array.isArray(full?.DocumentLines) ? full.DocumentLines : [];
      orderDetailsRead += 1;
      if (!full || !lines.length) continue;

      const explicitWhLines = lines.filter((ln) => normWh(ln) === targetWarehouse);
      const anyWhValues = Array.from(new Set(lines.map(normWh).filter(Boolean)));
      let whLines = explicitWhLines;

      if (!whLines.length) {
        if (!targetWarehouse) {
          whLines = lines.slice();
        } else if (!anyWhValues.length) {
          whLines = lines.slice();
        } else if (anyWhValues.length === 1 && anyWhValues[0] === targetWarehouse) {
          whLines = lines.slice();
        }
      }
      if (!whLines.length) continue;

      // Solo queremos PEDIDOS ABIERTOS sin factura.
      // En algunos ambientes OpenQty puede venir inconsistente, por eso aceptamos
      // cualquiera de estas señales de apertura:
      // - cabecera abierta
      // - al menos una línea abierta
      // - al menos una línea con OpenQty > 0
      const headerOpenForDispatch = prodDocStatusIsOpen(full?.DocumentStatus || full?.Status || h?.DocumentStatus || h?.Status || '');
      const openLineCountForDispatch = whLines.filter((ln) => prodDocStatusIsOpen(ln?.LineStatus || ln?.DocumentLineStatus || ln?.Status || '')).length;
      const anyOpenQtyForDispatch = whLines.some((ln) => prodNum(ln?.OpenQty ?? ln?.RemainingOpenQuantity ?? 0) > 0.0001);
      if (!(headerOpenForDispatch || openLineCountForDispatch > 0 || anyOpenQtyForDispatch)) continue;

      // Regla operativa:
      // Para C01600 el pedido sigue pendiente hasta que exista FACTURA relacionada,
      // pero únicamente si el pedido sigue ABIERTO en SAP.
      orders.push({
        ...full,
        __dispatchWhLines: whLines,
        __dispatchHeaderOpen: !!headerOpenForDispatch,
      });
    } catch (err) {
      orderDetailErrors += 1;
    }
    if (orders.length && orders.length % 10 === 0) await sleep(10);
  }

  if (!orders.length) {
    const empty = {
      alerts: [],
      byItem: [],
      byItemMap: new Map(),
      byItemIndex: {},
      summary: safeSummary({
        orderHeadersFound: orderHeaders.length,
        ordersFound: 0,
        orderDetailsRead,
        orderDetailErrors,
        sampleDocNums: orderHeaders.slice(0, 10).map((x) => Number(x?.DocNum || 0)).filter(Boolean),
        hint: orderHeaders.length && !orders.length ? 'Se encontraron cabeceras, pero no se pudieron materializar líneas válidas del pedido. Revisar lectura de DocumentLines en Service Layer.' : '',
      }),
    };
    prodRuntimeSet(PROD_DISPATCH_ALERTS_CACHE, cacheKey, empty);
    return empty;
  }

  const orderEntrySet = new Set(orders.map((o) => Number(o?.DocEntry || 0)).filter(Boolean));
  const deliveries = [];
  const deliveryHeaders = await prodFetchDocHeadersByDateRange('DeliveryNotes', { from, to, maxDocs: Math.max(100, maxDocs * 2), cardCode });
  for (const h of deliveryHeaders) {
    try {
      const full = await getDoc('DeliveryNotes', h.DocEntry);
      const lines = Array.isArray(full?.DocumentLines) ? full.DocumentLines : [];
      const linked = lines.some((ln) => Number(ln?.BaseType) === 17 && orderEntrySet.has(Number(ln?.BaseEntry || 0)));
      if (linked) deliveries.push(full);
    } catch {}
    if (deliveries.length && deliveries.length % 10 === 0) await sleep(10);
  }

  const deliveryLineToOrderLine = new Map();
  for (const dd of deliveries) {
    const lines = Array.isArray(dd?.DocumentLines) ? dd.DocumentLines : [];
    for (const ln of lines) {
      if (Number(ln?.BaseType) !== 17) continue;
      const orderEntry = Number(ln?.BaseEntry || 0);
      const orderLine = Number(ln?.BaseLine ?? ln?.BaseLineNumber ?? -1);
      const lineNum = Number(ln?.LineNum ?? -1);
      if (!orderEntrySet.has(orderEntry) || orderLine < 0 || lineNum < 0) continue;
      deliveryLineToOrderLine.set(`${Number(dd?.DocEntry || 0)}:${lineNum}`, `${orderEntry}:${orderLine}`);
    }
  }

  const invoicedQtyByOrderLine = new Map();
  const invoiceHeaders = await prodFetchDocHeadersByDateRange('Invoices', { from, to: addDaysISO(today, 15), maxDocs: Math.max(120, maxDocs * 3), cardCode });
  for (const h of invoiceHeaders) {
    try {
      const full = await getDoc('Invoices', h.DocEntry);
      const lines = Array.isArray(full?.DocumentLines) ? full.DocumentLines : [];
      for (const ln of lines) {
        const qtyBase = prodDocLineBaseQty(ln);
        if (!(qtyBase > 0)) continue;
        if (Number(ln?.BaseType) === 17 && orderEntrySet.has(Number(ln?.BaseEntry || 0))) {
          const key = `${Number(ln?.BaseEntry || 0)}:${Number(ln?.BaseLine ?? ln?.BaseLineNumber ?? -1)}`;
          invoicedQtyByOrderLine.set(key, prodRound(prodNum(invoicedQtyByOrderLine.get(key)) + qtyBase, 4));
        } else if (Number(ln?.BaseType) === 15) {
          const key = deliveryLineToOrderLine.get(`${Number(ln?.BaseEntry || 0)}:${Number(ln?.BaseLine ?? ln?.BaseLineNumber ?? -1)}`);
          if (!key) continue;
          invoicedQtyByOrderLine.set(key, prodRound(prodNum(invoicedQtyByOrderLine.get(key)) + qtyBase, 4));
        }
      }
    } catch {}
    if (invoiceHeaders.length > 20) await sleep(5);
  }

  const alerts = [];
  const byItemMap = new Map();
  for (const order of orders) {
    const docEntry = Number(order?.DocEntry || 0);
    const docNum = Number(order?.DocNum || 0);
    const dueDate = String(order?.DocDueDate || order?.DueDate || order?.TaxDate || order?.DocDate || '').slice(0,10);
    const docDate = String(order?.DocDate || '').slice(0,10);
    const lines = Array.isArray(order?.__dispatchWhLines) ? order.__dispatchWhLines : (Array.isArray(order?.DocumentLines) ? order.DocumentLines : []);
    for (const ln of lines) {
      const wh = normWh(ln) || targetWarehouse;
      if (targetWarehouse && wh !== targetWarehouse) continue;
      const lineNum = Number(ln?.LineNum ?? -1);
      if (lineNum < 0) continue;
      const itemCode = String(ln?.ItemCode || '').trim();
      if (!itemCode) continue;
      const itemDesc = String(ln?.ItemDescription || ln?.ItemName || '').trim();
      const factor = Math.max(1, prodNum(ln?.NumPerMsr ?? ln?.ItemsPerUnit ?? 1, 1));
      const qtyBase = Math.max(0, prodNum(ln?.Quantity || 0) * factor);
      const openQtyBase = Math.max(0, prodNum(ln?.OpenQty ?? ln?.RemainingOpenQuantity ?? 0) * factor);
      const invoicedQtyBase = Math.max(0, prodNum(invoicedQtyByOrderLine.get(`${docEntry}:${lineNum}`)));
      const headerIsOpen = !!order?.__dispatchHeaderOpen || prodDocStatusIsOpen(order?.DocumentStatus || order?.Status || '');
      const lineIsOpen = prodDocStatusIsOpen(ln?.LineStatus || ln?.DocumentLineStatus || ln?.Status || '');
      // Solo incluir líneas abiertas. Si la cabecera y la línea vienen cerradas
      // y OpenQty es 0, se considera documento cerrado y no debe mostrarse.
      if (!(headerIsOpen || lineIsOpen || openQtyBase > 0.01)) continue;
      // El criterio final es FACTURA:
      // si no hay factura suficiente, la línea sigue pendiente aunque OpenQty no ayude.
      let pendingQty = prodRound(Math.max(openQtyBase, qtyBase - invoicedQtyBase), 2);
      if (!(pendingQty > 0.01) && qtyBase > invoicedQtyBase + 0.01) {
        pendingQty = prodRound(qtyBase - invoicedQtyBase, 2);
      }
      if (!(pendingQty > 0.01) && invoicedQtyBase <= 0.01 && qtyBase > 0.01) {
        pendingQty = prodRound(Math.max(qtyBase, openQtyBase), 2);
      }
      if (!(pendingQty > 0.01)) continue;
      const lineDueDate = String(ln?.ShipDate || ln?.DueDate || dueDate || '').slice(0,10);
      const effectiveDueDate = lineDueDate || dueDate;
      const daysLate = prodDaysLateFromTo(effectiveDueDate, today);
      const daysUntilDue = prodDaysUntilFromTo(effectiveDueDate, today);
      const priorityState = daysLate > 0 ? 'overdue' : daysUntilDue <= 0 ? 'today' : daysUntilDue <= 3 ? 'soon' : 'open';
      const priorityScore = (daysLate * 1000) + (daysLate > 0 ? 5000 : daysUntilDue <= 3 ? 1000 - Math.max(daysUntilDue, 0) * 100 : 0) + pendingQty;
      const message = daysLate > 0
        ? `Atraso de ${daysLate} día(s). Pedido sin factura relacionada; despachar con prioridad.`
        : daysUntilDue === 0
          ? 'Entrega vence hoy. Pedido sin factura relacionada; revisar despacho.'
          : daysUntilDue > 0 && daysUntilDue <= 3
            ? `Entrega próxima en ${daysUntilDue} día(s). Pedido pendiente por despachar.`
            : 'Pedido abierto pendiente por despachar.';
      const row = {
        cardCode: targetCardCode,
        warehouse: wh,
        docEntry,
        docNum,
        lineNum,
        docDate,
        dueDate: effectiveDueDate,
        itemCode,
        itemDesc,
        qtyOrdered: prodRound(qtyBase, 2),
        qtyOpen: prodRound(openQtyBase, 2),
        qtyInvoiced: prodRound(invoicedQtyBase, 2),
        qtyPending: pendingQty,
        daysLate,
        daysUntilDue,
        priorityState,
        priorityScore: prodRound(priorityScore, 2),
        hasInvoice: invoicedQtyBase > 0,
        message,
      };
      alerts.push(row);
      const agg = byItemMap.get(itemCode) || {
        itemCode,
        itemDesc,
        qtyPending: 0,
        qtyOverdue: 0,
        maxDaysLate: 0,
        earliestDueDate: effectiveDueDate,
        docNums: [],
        lines: 0,
        priorityScore: 0,
      };
      agg.qtyPending = prodRound(agg.qtyPending + pendingQty, 2);
      if (daysLate > 0) agg.qtyOverdue = prodRound(agg.qtyOverdue + pendingQty, 2);
      agg.maxDaysLate = Math.max(agg.maxDaysLate, daysLate);
      agg.earliestDueDate = !agg.earliestDueDate || (effectiveDueDate && effectiveDueDate < agg.earliestDueDate) ? effectiveDueDate : agg.earliestDueDate;
      if (docNum && !agg.docNums.includes(docNum)) agg.docNums.push(docNum);
      agg.lines += 1;
      agg.priorityScore = Math.max(agg.priorityScore, priorityScore);
      agg.priorityState = agg.maxDaysLate > 0 ? 'overdue' : (daysUntilDue <= 3 ? 'soon' : 'open');
      byItemMap.set(itemCode, agg);
    }
  }

  alerts.sort((a, b) => {
    const late = prodNum(b?.daysLate) - prodNum(a?.daysLate);
    if (late) return late;
    const dueCmp = String(a?.dueDate || '').localeCompare(String(b?.dueDate || ''));
    if (dueCmp) return dueCmp;
    const prio = prodNum(b?.priorityScore) - prodNum(a?.priorityScore);
    if (prio) return prio;
    return String(a?.itemCode || '').localeCompare(String(b?.itemCode || ''));
  });

  const byItem = Array.from(byItemMap.values()).sort((a, b) =>
    prodNum(b?.priorityScore) - prodNum(a?.priorityScore) ||
    prodNum(b?.qtyPending) - prodNum(a?.qtyPending) ||
    String(a?.itemCode || '').localeCompare(String(b?.itemCode || ''))
  );

  const summary = safeSummary({
    orderHeadersFound: orderHeaders.length,
    ordersFound: orders.length,
    orderDetailsRead,
    orderDetailErrors,
    pendingLines: alerts.length,
    pendingItems: byItem.length,
    overdueItems: byItem.filter((x) => prodNum(x?.maxDaysLate) > 0).length,
    pendingQty: prodRound(alerts.reduce((acc, row) => acc + prodNum(row?.qtyPending), 0), 2),
    overdueQty: prodRound(alerts.filter((x) => prodNum(x?.daysLate) > 0).reduce((acc, row) => acc + prodNum(row?.qtyPending), 0), 2),
    latestDueDate: alerts[0]?.dueDate || '',
    sampleDocNums: orderHeaders.slice(0, 10).map((x) => Number(x?.DocNum || 0)).filter(Boolean),
  });

  const byItemIndex = Object.fromEntries(byItem.map((x) => [String(x?.itemCode || '').trim(), x]));
  const result = { alerts, byItem, byItemMap, byItemIndex, summary };
  prodRuntimeSet(PROD_DISPATCH_ALERTS_CACHE, cacheKey, result);
  return result;
}


function prodUnitsExpr(alias = '') {
  const p = alias ? `${alias}.` : '';
  // Igual que en SAP análisis de ventas: Quantity * NumPerMsr = unidades reales.
  // Si quantity_base está vacío o quedó viejo, recalculamos en línea para no depender del sync previo.
  return `COALESCE(NULLIF(${p}quantity_base,0), (COALESCE(${p}quantity,0) * COALESCE(NULLIF(${p}num_per_msr,0),1)), COALESCE(${p}quantity,0))`;
}

function prodDemandQtyExpr(alias = '') {
  return prodUnitsExpr(alias);
}

function prodSalesQtyExpr(alias = '') {
  return prodUnitsExpr(alias);
}

function prodMonthWindowEnd(dateIso) {
  const d = new Date(`${String(dateIso).slice(0,10)}T00:00:00Z`);
  if (!Number.isFinite(d.getTime())) return String(dateIso || '').slice(0,10);
  const y=d.getUTCFullYear();
  const m=d.getUTCMonth();
  const last=new Date(Date.UTC(y,m+1,0));
  return last.toISOString().slice(0,10);
}


function prodExtractSizeUom(description = "", item = null) {
  const src = `${String(description || "")} ${String(item?.ItemName || "")} ${String(item?.SalesUnit || "")}`.trim();
  const txt = prodNorm(src);
  const patterns = [
    /\b(5\.5|10\.5|12|16|20|24|29|32)\s*(?:oz|onz|onz\.|onzas?)\b/i,
    /\b(450|500|355|473|591|710|800|946|1000|3785)\s*ml\b/i,
    /\b(3(?:[\.,]1)?)\s*l\b/i,
    /\b(1)\s*gal\b/i,
  ];
  for (const rx of patterns) {
    const m = src.match(rx);
    if (m) return String(m[1] || "").replace(',', '.').trim();
  }
  if (/tetrapack|tetra pak|tetra/i.test(src)) return 'TETRAPACK';
  const loose = txt.match(/\b(5\.5|10\.5|12|16|20|24|29|32|450|500|355|473|591|710|800|946|1000|3785|3\.1)\b/);
  return loose ? String(loose[1] || "").trim() : "";
}

function prodCalcProductionMetrics({ stockMax = 0, stockTotal = 0, avgMonthlyQty = 0, projectedQty = 0, horizonMonths = 1 }) {
  const horizonSafe = Math.max(1, Number(horizonMonths || 1));
  const avgMonthlySafe = Math.max(0, Number(avgMonthlyQty || 0));
  const projectedSafe = Math.max(0, Number(projectedQty || 0));
  const monthlyDemand = Math.max(avgMonthlySafe, projectedSafe > 0 ? (projectedSafe / horizonSafe) : 0);
  const demandByHorizon = Math.max(projectedSafe, monthlyDemand * horizonSafe);
  const endStockTargetQty = Math.max(0, Number(stockMax || 0));
  const sapTargetByHorizon = endStockTargetQty;
  const productionNeeded = Math.max(0, demandByHorizon + endStockTargetQty);
  const productionAdjusted = Math.max(0, productionNeeded - Math.max(0, Number(stockTotal || 0)));
  return {
    horizonMonths: horizonSafe,
    monthlyDemand: prodRound(monthlyDemand, 2),
    demandByHorizon: prodRound(demandByHorizon, 2),
    sapTargetByHorizon: prodRound(sapTargetByHorizon, 2),
    endStockTargetQty: prodRound(endStockTargetQty, 2),
    productionNeeded: prodRound(productionNeeded, 2),
    productionAdjusted: prodRound(productionAdjusted, 2),
  };
}

async function prodRefreshInventoryForCodes(itemCodes = [], options = {}) {
  const allCodes = Array.from(new Set((Array.isArray(itemCodes) ? itemCodes : []).map((x) => String(x || "").trim()).filter(Boolean)));
  const maxItems = Math.max(1, Number(options?.maxItems || allCodes.length || 0));
  const concurrency = Math.max(1, Math.min(12, Number(options?.concurrency || 6)));
  const timeoutMs = Math.max(8000, Number(options?.timeoutMs || 25000));
  const codes = allCodes.slice(0, maxItems);
  if (!codes.length) return { saved: 0, items: 0, requestedItems: allCodes.length, refreshedItems: 0, errors: [] };

  let saved = 0;
  let cursor = 0;
  const errors = [];

  async function refreshOne(itemCode) {
    try {
      const it = await prodGetFullItem(itemCode, { forceFresh: true, ttlMs: 0, timeoutMs });
      if (!it) throw new Error('SAP no devolvió item');
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
      if (errors.length < 25) errors.push({ itemCode, message: e.message || String(e) });
    }
  }

  async function worker() {
    while (true) {
      const idx = cursor++;
      if (idx >= codes.length) return;
      await refreshOne(codes[idx]);
    }
  }

  await Promise.all(Array.from({ length: Math.min(concurrency, codes.length) }, () => worker()));
  return { saved, items: codes.length, requestedItems: allCodes.length, refreshedItems: codes.length, errors };
}

async function productionDashboardFromDb({ from, to, area, grupo, q, sizeUom = '__ALL__', abc = '__ALL__', typeFilter = 'Se fabrica en planta', avgMonths = 0, horizonMonths = 3 }) {
  const monthTo = String(to || getDateISOInOffset(TZ_OFFSET_MIN)).slice(0,10);
  const maxMonthsWindow = Math.max(1, Number(avgMonths || horizonMonths || 5), Number(horizonMonths || 3));
  const monthBaseStart = prodAddMonthsISO(`${String(monthTo).slice(0,7)}-01`, -(maxMonthsWindow - 1));
  const monthRangeEnd = String(monthTo || '').slice(0,10);

  const demandCountRes = await dbQuery(`SELECT COUNT(*)::int AS c FROM production_demand_lines WHERE doc_date >= $1::date AND doc_date <= $2::date`, [monthBaseStart, monthRangeEnd]);
  const demandCount = Number(demandCountRes.rows?.[0]?.c || 0);
  const useDemandFallback = demandCount <= 0;
  const demandTable = useDemandFallback ? 'sales_item_lines' : 'production_demand_lines';
  const demandSourceLabel = useDemandFallback ? 'Facturas SAP (fallback temporal; ejecuta Sync para Pedidos)' : 'Pedidos SAP (unidades)';
  const demandQtyExpr = prodDemandQtyExpr('d');

  const rows = await dbQuery(
    `
    WITH base AS (
      SELECT DISTINCT item_code
      FROM sales_item_lines
      WHERE doc_date >= $1::date AND doc_date <= $2::date AND item_code <> ''
      UNION
      SELECT DISTINCT item_code
      FROM ${demandTable}
      WHERE doc_date >= $3::date AND doc_date <= $4::date AND item_code <> ''
    ),
    sales AS (
      SELECT
        s.item_code,
        MAX(NULLIF(s.item_desc,'')) AS item_desc,
        COALESCE(SUM(s.revenue),0)::numeric(18,2) AS revenue,
        COALESCE(SUM(s.gross_profit),0)::numeric(18,2) AS gp,
        COALESCE(SUM(${prodSalesQtyExpr('s')}),0)::numeric(18,4) AS qty,
        MAX(NULLIF(s.area,'')) AS area_s,
        MAX(NULLIF(s.item_group,'')) AS grupo_s
      FROM sales_item_lines s
      WHERE s.doc_date >= $1::date AND s.doc_date <= $2::date
      GROUP BY s.item_code
    ),
    demand AS (
      SELECT
        d.item_code,
        MAX(NULLIF(d.item_desc,'')) AS item_desc,
        COALESCE(SUM(${demandQtyExpr}),0)::numeric(18,4) AS demand_qty,
        MAX(NULLIF(d.area,'')) AS area_d,
        MAX(NULLIF(d.item_group,'')) AS grupo_d
      FROM ${demandTable} d
      WHERE d.doc_date >= $3::date AND d.doc_date <= $4::date
      GROUP BY d.item_code
    ),
    inv AS (
      SELECT
        item_code,
        SUM(CASE WHEN warehouse='01'  THEN stock ELSE 0 END)::float AS wh_01,
        SUM(CASE WHEN warehouse='03'  THEN stock ELSE 0 END)::float AS wh_03,
        SUM(CASE WHEN warehouse='10'  THEN stock ELSE 0 END)::float AS wh_10,
        SUM(CASE WHEN warehouse='12'  THEN stock ELSE 0 END)::float AS wh_12,
        SUM(CASE WHEN warehouse='200' THEN stock ELSE 0 END)::float AS wh_200,
        SUM(CASE WHEN warehouse='300' THEN stock ELSE 0 END)::float AS wh_300,
        SUM(CASE WHEN warehouse='500' THEN stock ELSE 0 END)::float AS wh_500,
        SUM(CASE WHEN warehouse IN ('01','03','10','12','200','300','500') THEN stock ELSE 0 END)::float AS stock_total,
        SUM(CASE WHEN warehouse IN ('01','03','10','12','200','300','500') THEN stock_min ELSE 0 END)::float AS stock_min,
        SUM(CASE WHEN warehouse IN ('01','03','10','12','200','300','500') THEN stock_max ELSE 0 END)::float AS stock_max
      FROM production_inv_wh_cache
      GROUP BY item_code
    )
    SELECT
      base.item_code,
      COALESCE(NULLIF(sales.item_desc,''), NULLIF(demand.item_desc,''), NULLIF(g.item_desc,''), '') AS item_desc,
      COALESCE(NULLIF(sales.grupo_s,''), NULLIF(demand.grupo_d,''), NULLIF(g.grupo,''), NULLIF(g.group_name,''), 'Sin grupo') AS grupo,
      COALESCE(NULLIF(sales.area_s,''), NULLIF(demand.area_d,''), NULLIF(g.area,''), '') AS area,
      COALESCE(sales.revenue,0) AS revenue,
      COALESCE(sales.gp,0) AS gp,
      COALESCE(sales.qty,0) AS sold_qty,
      COALESCE(demand.demand_qty,0) AS demand_qty,
      COALESCE(inv.wh_01,0) AS wh_01,
      COALESCE(inv.wh_03,0) AS wh_03,
      COALESCE(inv.wh_10,0) AS wh_10,
      COALESCE(inv.wh_12,0) AS wh_12,
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
    FROM base
    LEFT JOIN sales ON sales.item_code = base.item_code
    LEFT JOIN demand ON demand.item_code = base.item_code
    LEFT JOIN item_group_cache g ON g.item_code = base.item_code
    LEFT JOIN inv ON inv.item_code = base.item_code
    LEFT JOIN production_mrp_cache m ON m.item_code = base.item_code
    ORDER BY COALESCE(sales.revenue,0) DESC, base.item_code ASC
    `,
    [from, to, monthBaseStart, monthRangeEnd]
  );

  const monthRows = await dbQuery(
    `
    SELECT
      item_code,
      to_char(date_trunc('month', doc_date), 'YYYY-MM') AS ym,
      COALESCE(SUM(${prodDemandQtyExpr()}),0)::numeric(18,4) AS qty
    FROM ${demandTable}
    WHERE doc_date >= $1::date AND doc_date <= $2::date
    GROUP BY item_code, to_char(date_trunc('month', doc_date), 'YYYY-MM')
    `,
    [monthBaseStart, monthRangeEnd]
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

    const horizonSafe = Math.max(1, Number(horizonMonths || 3));
    const avgMonthsSafe = Math.max(1, Number(avgMonths || horizonSafe || 5));
    let sumAvgMonths = 0;
    let sumHorizonMonths = 0;
    for (let i = avgMonthsSafe - 1; i >= 0; i--) {
      const ym = prodYm(prodAddMonthsISO(`${String(monthTo).slice(0,7)}-01`, -i));
      sumAvgMonths += prodNum(monthsMap.get(ym));
    }
    for (let i = horizonSafe - 1; i >= 0; i--) {
      const ym = prodYm(prodAddMonthsISO(`${String(monthTo).slice(0,7)}-01`, -i));
      sumHorizonMonths += prodNum(monthsMap.get(ym));
    }
    const avgQty = sumAvgMonths / avgMonthsSafe;
    const pedidosQty = sumHorizonMonths;
    const stockTotal = prodNum(r.stock_total);

    return {
      itemCode: String(r.item_code || ""),
      itemDesc: String(r.item_desc || ""),
      grupo: grupoTxt,
      area: areaFinal,
      revenue: rev,
      gp,
      gpPct: prodRound(gpPct, 2),
      soldQty: prodNum(r.sold_qty),
      avgMonthlyQty: prodRound(avgQty, 2),
      projectedQty: prodRound(pedidosQty, 2),
      stockTotal,
      wh01: prodNum(r.wh_01),
      wh03: prodNum(r.wh_03),
      wh10: prodNum(r.wh_10),
      wh12: prodNum(r.wh_12),
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
      productionNeeded: 0,
      productionAdjusted: 0,
      coverageMonthsTarget: 1,
      coveragePolicyLabel: '',
      demandSource: demandSourceLabel,
      demandUomLabel: 'Unidades SAP',
    };
  });

  const areaSel = String(area || "__ALL__");
  const grupoValues = prodParseMultiValue(grupo);
  const grupoNormSet = new Set(grupoValues.map((x) => prodNormGroupName(x)));
  const grupoSel = grupoValues.length ? grupoValues.join("|") : "__ALL__";
  const sizeSel = String(sizeUom || "__ALL__").trim();
  const typeSel = prodNormalizeTypeFilter(typeFilter || "Se fabrica en planta");
  const qq = String(q || "").trim().toLowerCase();

  let availableGroups = [];
  if (areaSel === "CONS") availableGroups = Array.from(PROD_GROUPS_CONS);
  else if (areaSel === "RCI") availableGroups = Array.from(PROD_GROUPS_RCI);
  else availableGroups = Array.from(new Set([...PROD_GROUPS_CONS, ...PROD_GROUPS_RCI]));
  availableGroups.sort((a, b) => a.localeCompare(b));

  let universe = items.slice();
  if (areaSel !== "__ALL__") universe = universe.filter((x) => x.area === areaSel);
  if (grupoNormSet.size) universe = universe.filter((x) => grupoNormSet.has(prodNormGroupName(x.grupo)));
  if (typeSel !== "__ALL__") universe = universe.filter((x) => prodMatchesType(x, typeSel));

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
    const coverageMonthsTarget = prodCoverageMonthsByLabel(total.label);
    const prodMetrics = prodCalcProductionMetrics({
      stockMax: it.stockMax,
      stockTotal: it.stockTotal,
      avgMonthlyQty: it.avgMonthlyQty,
      projectedQty: it.projectedQty,
      horizonMonths: Math.max(1, Number(horizonMonths || 3)),
    });
    const targetInventoryQty = prodMetrics.sapTargetByHorizon;
    const productionNeeded = prodMetrics.productionNeeded;
    const productionAdjusted = prodMetrics.productionAdjusted;

    const sapItemForMeta = null;
    const sizeUom = prodExtractSizeUom(it.itemDesc, sapItemForMeta);
    const sapLotSize = Math.max(prodNum(it.multipleQty), prodNum(it.minOrderQty), 0);

    const rateInfo = prodResolveUnitsPerHour({ machine, itemCode: it.itemCode, itemDesc: it.itemDesc, sizeUom, capacity: local.capacity });
    const rate = prodNum(rateInfo?.rate || 0);
    const hoursNeeded = rate > 0 ? productionAdjusted / rate : 0;

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
      coverageMonthsTarget,
      coveragePolicyLabel: prodCoverageLabel(coverageMonthsTarget),
      targetInventoryQty: prodRound(targetInventoryQty, 2),
      productionNeeded: prodRound(productionNeeded, 2),
      productionAdjusted: prodRound(productionAdjusted, 2),
      sizeUom,
      sapLotSize: prodRound(sapLotSize, 2),
      productionBasis: {
        demandByHorizon: prodMetrics.demandByHorizon,
        sapTargetByHorizon: prodMetrics.sapTargetByHorizon,
      },
      unitsPerHour: rate,
      hoursNeeded: prodRound(hoursNeeded, 2),
      hoursSource: rate > 0 ? "config_local_fallback" : "sin_fuente_sap",
      hasFormula: !!meta,
      baseLiquidCode: meta?.baseLiquidCode || "",
    };
  });

  const availableSizes = Array.from(new Set(
    items
      .map((x) => String(x.sizeUom || "").trim())
      .filter(Boolean)
  )).sort((a, b) => a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' }));
  const availableAbc = ["AB Crítico", "C Importante", "D"];
  const availableTypes = ["Se fabrica en planta", "No se fabrica en planta"];

  if (areaSel !== "__ALL__") items = items.filter((x) => x.area === areaSel);
  if (grupoNormSet.size) items = items.filter((x) => grupoNormSet.has(prodNormGroupName(x.grupo)));
  if (typeSel !== "__ALL__") items = items.filter((x) => prodMatchesType(x, typeSel));
  if (sizeSel !== "__ALL__") items = items.filter((x) => String(x.sizeUom || "").trim() === sizeSel);
  if (String(abc || "__ALL__") !== "__ALL__") items = items.filter((x) => String(x.totalLabel || "") === String(abc));
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
    from, to, area: areaSel, grupo: grupoSel, grupos: grupoValues, sizeUom: sizeSel, abc: String(abc || "__ALL__"), type: typeSel, q: qq,
    avgMonths: Math.max(3, Number(avgMonths || horizonMonths || 5)),
    horizonMonths: Math.max(1, Number(horizonMonths || 3)),
    lastSyncAt: await getState("production_last_sync_at"),
    demandSource: demandSourceLabel,
      demandUomLabel: 'Unidades SAP',
    demandCount,
    useDemandFallback,
    availableGroups,
    availableSizes,
    availableAbc,
    availableTypes,
    totals: {
      revenue: prodRound(totals.revenue, 2),
      projectedQty: prodRound(totals.projectedQty, 2),
      stockTotal: prodRound(totals.stockTotal, 2),
      productionNeeded: prodRound(totals.productionNeeded, 2),
      productionAdjusted: prodRound(prodNum(totals.productionNeeded) - prodNum(totals.stockTotal), 2),
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

  const itemDemandCountRes = await dbQuery(`SELECT COUNT(*)::int AS c FROM production_demand_lines WHERE item_code = $1`, [code]);
  const itemUseFallback = Number(itemDemandCountRes.rows?.[0]?.c || 0) <= 0;
  const itemDemandTable = itemUseFallback ? 'sales_item_lines' : 'production_demand_lines';
  const itemDemandQtyExpr = prodDemandQtyExpr();
  const itemDemandSourceLabel = itemUseFallback ? 'Ventas desde facturas SAP convertidas a unidades (fallback temporal)' : 'Pedidos SAP convertidos a unidades';

  const itemMaster = await dbQuery(
    `
    SELECT
      COALESCE(MAX(NULLIF(s.item_desc,'')), MAX(NULLIF(g.item_desc,'')), '') AS item_desc,
      COALESCE(MAX(NULLIF(g.grupo,'')), MAX(NULLIF(g.group_name,'')), MAX(NULLIF(s.item_group,'')), 'Sin grupo') AS grupo,
      COALESCE(MAX(NULLIF(g.area,'')), MAX(NULLIF(s.area,'')), '') AS area
    FROM ${itemDemandTable} s
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
    SELECT to_char(date_trunc('month', doc_date), 'YYYY-MM') AS ym, COALESCE(SUM(${itemDemandQtyExpr}),0)::numeric(18,4) AS qty
    FROM ${itemDemandTable}
    WHERE item_code = $1 AND doc_date >= $2::date AND doc_date <= $3::date
    GROUP BY 1
    ORDER BY 1
    `,
    [code, histFrom, end]
  );
  const monthMap = new Map((monthlyRows.rows || []).map((r) => [String(r.ym || ""), prodNum(r.qty)]));
  const monthlySummary = await prodReadMonthlySummaryDb(code, histFrom, end).catch(() => ({ found: false, sales: new Map(), produced: new Map(), avgCost: new Map(), weightedCost: 0, source: '' }));
  if (monthlySummary?.found) {
    for (const [ym, qty] of (monthlySummary.sales || new Map()).entries()) {
      monthMap.set(String(ym || ''), prodNum(qty));
    }
  }

  let weightedCost = prodExtractWeightedCostFromItem(sapItem);
  if (!(weightedCost > 0)) {
    try {
      const cachedItem = await prodReadItemCacheDb(code, 365 * 24 * 60 * 60 * 1000);
      const cachedWeighted = prodExtractWeightedCostFromItem(cachedItem);
      if (cachedWeighted > 0) weightedCost = cachedWeighted;
    } catch {}
  }
  if (!(weightedCost > 0) && hasDb()) {
    try {
      const wcRes = await dbQuery(`SELECT weighted_cost FROM production_item_cache WHERE item_code = $1 LIMIT 1`, [code]);
      const dbWeighted = prodNum(wcRes.rows?.[0]?.weighted_cost || 0);
      if (dbWeighted > 0) weightedCost = dbWeighted;
    } catch {}
  }
  if (!(weightedCost > 0)) {
    try {
      const freshItem = await prodGetFullItem(code, { forceFresh: true, ttlMs: 0 });
      const freshWeighted = prodExtractWeightedCostFromItem(freshItem);
      if (freshWeighted > 0) weightedCost = freshWeighted;
    } catch {}
  }
  if (!(weightedCost > 0) && prodNum(monthlySummary?.weightedCost) > 0) {
    weightedCost = prodNum(monthlySummary.weightedCost);
  }
  const prodOrders = await prodFetchProductionOrders(code, 120).catch(() => ({ orders: [], monthly: new Map(), monthlyAvgCost: new Map() }));
  const prodMonthMap = prodOrders?.monthly instanceof Map ? new Map(prodOrders.monthly) : new Map();
  const prodMonthAvgCostMap = prodOrders?.monthlyAvgCost instanceof Map ? new Map(prodOrders.monthlyAvgCost) : new Map();
  const totalCostMonthMap = monthlySummary?.totalCostMonth instanceof Map ? new Map(monthlySummary.totalCostMonth) : new Map();
  if (monthlySummary?.found) {
    for (const [ym, qty] of (monthlySummary.produced || new Map()).entries()) {
      prodMonthMap.set(String(ym || ''), prodNum(qty));
    }
    for (const [ym, avgCost] of (monthlySummary.avgCost || new Map()).entries()) {
      prodMonthAvgCostMap.set(String(ym || ''), prodNum(avgCost));
    }
  }

  const salesHistory = [];
  for (let i = 11; i >= 0; i--) {
    const ym = prodYm(prodAddMonthsISO(monthStart, -i));
    salesHistory.push({
      ym,
      label: prodFormatMonthName(ym),
      qty: prodRound(monthMap.get(ym) || 0, 2),
      producedQty: prodRound(prodMonthMap.get(ym) || 0, 2),
      weightedCost: prodRound(weightedCost || 0, 4),
      avgProductionCost: prodRound(prodMonthAvgCostMap.get(ym) || 0, 4),
      totalCostMonth: prodRound(totalCostMonthMap.get(ym) || 0, 2),
      source: (monthlySummary?.found ? `Resumen mensual cargado (${monthlySummary.source || 'production_item_monthly_summary'})` : itemDemandSourceLabel),
    });
  }

  const avgMonthsSafe = Math.max(3, Number(avgMonths || horizonMonths || 5));
  let avgQty = 0;
  for (let i = avgMonthsSafe - 1; i >= 0; i--) {
    const ym = prodYm(prodAddMonthsISO(monthStart, -i));
    avgQty += prodNum(monthMap.get(ym) || 0);
  }
  avgQty = avgQty / avgMonthsSafe;
  let projectedQty = 0;
  for (let i = Math.max(1, Number(horizonMonths || 3)) - 1; i >= 0; i--) {
    const ym = prodYm(prodAddMonthsISO(monthStart, -i));
    projectedQty += prodNum(monthMap.get(ym) || 0);
  }

  const invRows = await dbQuery(
    `SELECT warehouse, stock, stock_min, stock_max, committed, ordered, available
     FROM production_inv_wh_cache
     WHERE item_code = $1`,
    [code]
  );
  const byWh = { "01": 0, "03": 0, "10": 0, "12": 0, "200": 0, "300": 0, "500": 0 };
  let stockTotal = 0;
  let stockMin = 0;
  let stockMax = 0;
  for (const r of invRows.rows || []) {
    const wh = String(r.warehouse || "").trim();
    if (!Object.prototype.hasOwnProperty.call(byWh, wh)) continue;
    byWh[wh] = prodNum(r.stock);
    stockTotal += prodNum(r.stock);
    stockMin += prodNum(r.stock_min);
    stockMax += prodNum(r.stock_max);
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

  const dashForAbc = await productionDashboardFromDb({ from: '2025-01-01', to: end, area: '__ALL__', grupo: '__ALL__', q: code, avgMonths, horizonMonths });
  const dashRow = (dashForAbc.items || []).find((x) => String(x.itemCode || '') === code) || null;
  const coverageMonthsTarget = dashRow ? prodNum(dashRow.coverageMonthsTarget || 1, 1) : 1;
  const coveragePolicyLabel = prodCoverageLabel(coverageMonthsTarget);
const horizonSafePlan = Math.max(1, Number(horizonMonths || 3));
const targetInventoryQty = Math.max(0, stockMax);
const manualPlanQty = Math.max(0, prodNum(plannedQtyOverride));
const autoProdMetrics = prodCalcProductionMetrics({
  stockMax,
  stockTotal,
  avgMonthlyQty: avgQty,
  projectedQty,
  horizonMonths: horizonSafePlan,
});
const effectiveProjectedQty = manualPlanQty > 0 ? manualPlanQty : autoProdMetrics.productionNeeded;
const productionNeeded = manualPlanQty > 0 ? manualPlanQty : autoProdMetrics.productionNeeded;
const mrpAdjustedQty = manualPlanQty > 0 ? Math.max(0, manualPlanQty - stockTotal) : autoProdMetrics.productionAdjusted;
const productionAdjusted = mrpAdjustedQty;
const sapLotSize = Math.max(prodNum(mrp.multipleQty), prodNum(mrp.minOrderQty), 0);
const lotBaseQty = sapLotSize > 0 ? sapLotSize : Math.max(1, prodNum(local.capacity?.minimumRunQty || local.capacity?.minRunQty || 600, 600));
const recommendedLotQty = productionAdjusted > 0 ? Math.ceil(productionAdjusted / Math.max(1, lotBaseQty)) * Math.max(1, lotBaseQty) : 0;
const sizeUom = prodExtractSizeUom(itemDesc, sapItem);

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

  const rateInfo = prodResolveUnitsPerHour({ machine, itemCode: code, itemDesc, sizeUom, capacity: local.capacity });
  const rate = prodNum(rateInfo?.rate || 0);
  const hoursNeeded = rate > 0 ? productionAdjusted / rate : 0;
  const planStart = addDaysISO(end, 1);
  const horizonPlanDays = Math.max(1, Math.round(Math.max(1, Number(horizonMonths || 1)) * 30));
  const planEnd = addDaysISO(planStart, Math.max(0, horizonPlanDays - 1));
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
      avgMonths: Math.max(1, Number(avgMonths || horizonMonths || 5)),
      horizonMonths: Math.max(1, Number(horizonMonths || 3)),
      planStart,
      planEnd
    },
    salesHistory,
    salesHistorySource: itemDemandSourceLabel,
    recentProductionOrders: (prodOrders?.orders || []).slice(0, 20),
    costing,
    avgMonthlyQty: prodRound(avgQty, 2),
    projectedQty: prodRound(autoProdMetrics ? autoProdMetrics.demandByHorizon : targetInventoryQty, 2),
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
      sapLotSize: prodRound(sapLotSize, 2),
      sizeUom,
    },
    production: {
      litersPerUnit: prodRound(litersPerUnit, 6),
      baseLiquidCode,
      litersRequired: prodRound(litersRequired, 3),
      neededQty: prodRound(productionNeeded, 2),
      mrpAdjustedQty: prodRound(mrpAdjustedQty, 2),
      adjustedQty: prodRound(productionAdjusted, 2),
      lotBaseQty: prodRound(lotBaseQty, 2),
      recommendedLotQty: prodRound(recommendedLotQty, 2),
      coverageMonthsTarget: prodRound(coverageMonthsTarget,2),
      coveragePolicyLabel,
      targetInventoryQty: prodRound(targetInventoryQty,2),
      manualPlanQty: prodRound(manualPlanQty, 3),
      planBasis: manualPlanQty > 0 ? "SUBPLAN_COMPONENTE" : "DEMANDA_PROYECTADA",
      maxUnitsToday,
      practicalRule: `Política vigente: ${coveragePolicyLabel}; producción necesaria = demanda del horizonte + inventario objetivo para cerrar en stock máximo SAP.`,
    },
    capacity: {
      unitsPerHour: prodRound(rate, 4),
      unitsPerShift: prodRound(rateInfo?.ratePerShift || (rate * shiftHoursSafe), 2),
      rateSource: String(rateInfo?.source || ''),
      hoursNeeded: prodRound(hoursNeeded, 2),
      hoursSource: rate > 0 ? "config_local_fallback" : "sin_fuente_sap",
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

function prodQuestionNeedsMaterialInventory(question) {
  const q = String(question || "").toLowerCase();
  return /(inventario|insumos|materia prima|materias primas|empaque|empaques|fabricar|se pueden fabricar|puede fabricarse|producci[oó]n posible|seg[uú]n el inventario|con el inventario)/.test(q);
}

function prodBuildFallbackPlanCodes(question, dashboard) {
  const items = Array.isArray(dashboard?.items) ? dashboard.items : [];
  const urgent = prodBuildUrgentAbStockItems(items).map((x) => x.codigo || x.itemCode || x.code).filter(Boolean);
  const filtered = items
    .filter((x) => {
      const stockTotal = prodNum(x?.stockTotal);
      const stockMin = prodNum(x?.stockMin);
      return prodNum(x?.productionNeeded) > 0 || (stockMin > 0 && stockTotal < stockMin);
    })
    .sort((a, b) => prodNum(b?.revenue) - prodNum(a?.revenue) || prodNum(b?.productionNeeded) - prodNum(a?.productionNeeded))
    .map((x) => String(x?.itemCode || '').trim())
    .filter(Boolean);
  const out = [];
  const seen = new Set();
  for (const code of [...urgent, ...filtered]) {
    const key = prodNormalizeItemCodeLoose(code);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(code);
    if (out.length >= 8) break;
  }
  return out;
}

function prodMachineAiLabel(code) {
  const c = String(code || '').trim().toUpperCase();
  if (c === 'SAUCES') return 'SALSAS';
  if (c === 'CLEANING') return 'LIMPIEZA';
  return c || '';
}

function prodCleanText(v) {
  return String(v == null ? '' : v).replace(/JSON/gi, 'información').replace(/base de datos sincronizada/gi, 'sistema').trim();
}

function prodCompactItemForAi(x) {
  const stockTotal = prodNum(x?.stockTotal);
  const stockMin = prodNum(x?.stockMin);
  const stockMax = prodNum(x?.stockMax);
  const produccionNecesaria = prodNum(x?.productionNeeded);
  const faltanteMinimo = stockMin > stockTotal ? prodRound(stockMin - stockTotal, 2) : 0;
  return {
    codigo: x?.itemCode || "",
    descripcion: x?.itemDesc || "",
    grupo: x?.grupo || "",
    area: x?.area || "",
    clasificacion: x?.totalLabel || "",
    ingreso: prodNum(x?.revenue),
    ganancia: prodNum(x?.gp),
    margenPct: prodNum(x?.gpPct),
    promedioMensualUnidades: prodNum(x?.avgMonthlyQty),
    demandaHorizonteUnidades: prodNum(x?.projectedQty),
    stockActual: stockTotal,
    stockMinimo: stockMin,
    stockMaximo: stockMax,
    faltanteVsMinimo: faltanteMinimo,
    produccionNecesaria,
    produccionAjustada: prodNum(x?.productionAdjusted),
    linea: prodMachineAiLabel(x?.machine || ""),
    horasProduccion: prodNum(x?.hoursNeeded),
    diasAbastecimiento: prodNum(x?.leadTimeDays),
    debajoDelMinimo: stockMin > 0 ? stockTotal < stockMin : produccionNecesaria > 0,
    porEncimaDelMaximo: stockMax > 0 ? stockTotal > stockMax : false,
  };
}



async function productionBuildItemPlanCached(params = {}) {
  const key = JSON.stringify({
    itemCode: String(params.itemCode || "").trim(),
    toDate: String(params.toDate || ""),
    avgMonths: Number(params.avgMonths || 5),
    horizonMonths: Number(params.horizonMonths || 3),
    shiftHours: Number(params.shiftHours || 8),
    plannedQtyOverride: Number(params.plannedQtyOverride || 0),
  });
  const hit = prodRuntimeGet(PROD_PLAN_RUNTIME_CACHE, key, PROD_PLAN_TTL_MS);
  if (hit) return hit;
  const plan = await productionBuildItemPlan(params);
  prodRuntimeSet(PROD_PLAN_RUNTIME_CACHE, key, plan);
  return plan;
}


async function prodBuildSimulationNode({
  itemCode,
  plannedQty = 0,
  toDate,
  avgMonths = 5,
  horizonMonths = 3,
  shiftHours = 8,
  depth = 0,
  maxDepth = 3,
  trail = [],
}) {
  const code = String(itemCode || "").trim();
  if (!code) throw new Error("Falta itemCode en simulación");

  const qtyToPlan = Math.max(0, prodNum(plannedQty));
  const plan = await productionBuildItemPlanCached({
    itemCode: code,
    toDate,
    avgMonths,
    horizonMonths,
    shiftHours,
    plannedQtyOverride: qtyToPlan,
  });

  const pathKeys = new Set((Array.isArray(trail) ? trail : []).map((x) => prodNormalizeItemCodeLoose(x)).filter(Boolean));
  pathKeys.add(prodNormalizeItemCodeLoose(code));

  const baseMaterials = [
    ...(Array.isArray(plan?.requirements?.rawMaterials) ? plan.requirements.rawMaterials : []),
    ...(Array.isArray(plan?.requirements?.packaging) ? plan.requirements.packaging : []),
  ]
    .map((x) => ({ ...x }))
    .sort((a, b) =>
      prodNum(b?.shortageQty) - prodNum(a?.shortageQty) ||
      prodNum(b?.requiredQty) - prodNum(a?.requiredQty) ||
      String(a?.description || "").localeCompare(String(b?.description || ""))
    );

  const resources = (Array.isArray(plan?.requirements?.resources) ? plan.requirements.resources : [])
    .map((x) => ({
      code: String(x?.code || "").trim(),
      description: String(x?.description || "").trim(),
      requiredQty: prodRound(x?.requiredQty, 3),
      unit: String(x?.unit || "").trim(),
      status: String(x?.status || "OK"),
      componentType: String(x?.componentType || "RESOURCE"),
      isResource: true,
    }))
    .sort((a, b) => prodNum(b?.requiredQty) - prodNum(a?.requiredQty));

  const materials = [];
  for (const row of baseMaterials) {
    const componentCode = String(row?.code || "").trim();
    const subPlanQty = Math.max(
      prodNum(row?.shortageQty),
      prodNum(row?.subPlanQty),
      prodNum(row?.requiredQty)
    );

    const materialNode = {
      code: componentCode,
      description: String(row?.description || "").trim(),
      requiredQty: prodRound(row?.requiredQty, 3),
      stockQty: prodRound(row?.stockQty, 3),
      shortageQty: prodRound(row?.shortageQty, 3),
      unit: String(row?.unit || "").trim(),
      supplier: String(row?.supplier || "").trim(),
      status: String(row?.status || (prodNum(row?.shortageQty) > 0 ? "FALTANTE" : "OK")),
      componentType: String(row?.componentType || ""),
      procurementMethodLabel: "",
      canExpand: false,
      child: null,
      cycleBlocked: false,
      plannedQtyForChild: prodRound(subPlanQty, 3),
    };

    const cycleKey = prodNormalizeItemCodeLoose(componentCode);
    const shouldTryExpand =
      depth < Math.max(1, Number(maxDepth || 3)) &&
      !!componentCode &&
      subPlanQty > 0 &&
      !pathKeys.has(cycleKey) &&
      String(materialNode.componentType || "").toUpperCase() !== "RESOURCE";

    if (shouldTryExpand) {
      try {
        const subPlan = await productionBuildItemPlanCached({
          itemCode: componentCode,
          toDate,
          avgMonths,
          horizonMonths,
          shiftHours,
          plannedQtyOverride: subPlanQty,
        });

        const procurementMethodLabel =
          String(subPlan?.mrp?.procurementMethodLabel || "") ||
          prodProcurementMethodLabel(String(subPlan?.mrp?.procurementMethod || ""));
        materialNode.procurementMethodLabel = procurementMethodLabel;

        const hasNestedRequirements = (
          Array.isArray(subPlan?.requirements?.rawMaterials) && subPlan.requirements.rawMaterials.length
        ) || (
          Array.isArray(subPlan?.requirements?.packaging) && subPlan.requirements.packaging.length
        ) || (
          Array.isArray(subPlan?.requirements?.resources) && subPlan.requirements.resources.length
        );

        if (procurementMethodLabel === "Se fabrica en planta" && hasNestedRequirements) {
          materialNode.canExpand = true;
          materialNode.child = await prodBuildSimulationNode({
            itemCode: componentCode,
            plannedQty: subPlanQty,
            toDate,
            avgMonths,
            horizonMonths,
            shiftHours,
            depth: depth + 1,
            maxDepth,
            trail: [...trail, code],
          });
        }
      } catch (_err) {}
    } else if (componentCode && pathKeys.has(cycleKey)) {
      materialNode.cycleBlocked = true;
    }

    materials.push(materialNode);
  }

  const shortageMaterials = materials.filter((x) => prodNum(x?.shortageQty) > 0);
  const totalShortageQty = shortageMaterials.reduce((acc, x) => acc + prodNum(x?.shortageQty), 0);

  return {
    nodeType: "ITEM",
    depth,
    itemCode: plan.itemCode,
    itemDesc: plan.itemDesc,
    grupo: plan.grupo,
    area: plan.area,
    machine: plan.machine,
    machineLabel: plan?.capacity?.machineLabel || plan.machine,
    procurementMethodLabel: String(plan?.mrp?.procurementMethodLabel || plan?.procurementMethodLabel || ""),
    plannedQty: prodRound(qtyToPlan > 0 ? qtyToPlan : (plan?.production?.adjustedQty || plan?.production?.neededQty || 0), 3),
    stockTotal: prodRound(plan?.inventory?.total || 0, 3),
    neededQty: prodRound(plan?.production?.neededQty || 0, 3),
    adjustedQty: prodRound(plan?.production?.adjustedQty || 0, 3),
    recommendedLotQty: prodRound(plan?.production?.recommendedLotQty || 0, 3),
    hoursNeeded: prodRound(plan?.capacity?.hoursNeeded || 0, 3),
    unitsPerHour: prodRound(plan?.capacity?.unitsPerHour || 0, 3),
    materials,
    resources,
    summary: {
      materialsCount: materials.length,
      shortageMaterialsCount: shortageMaterials.length,
      totalShortageQty: prodRound(totalShortageQty, 3),
      resourcesCount: resources.length,
    },
  };
}

function prodAccumulateSimulationSummaryNode(node, acc, depth = 0) {
  if (!node || typeof node !== "object") return;
  const countsAsFinished = depth === 0 && prodIsFinishedGoodCode(node?.itemCode, node?.itemDesc, node?.sizeUom || "");
  if (countsAsFinished) {
    acc.totalHours += prodNum(node?.hoursNeeded);
    acc.totalPlannedQty += prodNum(node?.plannedQty);
    acc.finishedItemsCount += 1;
    for (const res of Array.isArray(node?.resources) ? node.resources : []) {
      const key = String(res?.code || res?.description || "").trim();
      if (key) acc.resourceSet.add(key);
    }
  }

  for (const mat of Array.isArray(node?.materials) ? node.materials : []) {
    const key = String(mat?.code || mat?.description || "").trim();
    if (key) acc.materialSet.add(key);
    if (prodNum(mat?.shortageQty) > 0) {
      acc.shortageMaterialsCount += 1;
      acc.totalShortageQty += prodNum(mat?.shortageQty);
      if (key) acc.shortageMaterialSet.add(key);
    }
    if (mat?.child) prodAccumulateSimulationSummaryNode(mat.child, acc, depth + 1);
  }
}

async function productionBuildSimulationTree({
  from,
  to,
  area,
  grupo,
  sizeUom = "__ALL__",
  abc = "__ALL__",
  typeFilter = "Se fabrica en planta",
  q = "",
  avgMonths = 3,
  horizonMonths = 3,
  shiftHours = 8,
  itemCodes = [],
  maxDepth = 3,
}) {
  const normalizedCodes = Array.from(new Set((Array.isArray(itemCodes) ? itemCodes : [])
    .map((x) => String(x || "").trim())
    .filter(Boolean)
    .map((x) => prodNormalizeItemCodeLoose(x))
  ));

  if (!normalizedCodes.length) {
    throw new Error("Selecciona al menos un artículo para simular");
  }

  const dash = await productionDashboardFromDb({
    from,
    to,
    area,
    grupo,
    sizeUom,
    abc,
    typeFilter,
    q,
    avgMonths,
    horizonMonths,
  });

  const itemMap = new Map(
    (Array.isArray(dash?.items) ? dash.items : []).map((x) => [prodNormalizeItemCodeLoose(x?.itemCode), x])
  );

  const selectedItems = normalizedCodes
    .map((code) => itemMap.get(code))
    .filter(Boolean)
    .slice(0, 15);

  if (!selectedItems.length) {
    throw new Error("Los artículos seleccionados no están dentro del filtro actual del dashboard");
  }

  const tree = [];
  for (const row of selectedItems) {
    const plannedQty = Math.max(prodNum(row?.productionAdjusted), prodNum(row?.productionNeeded));
    const node = await prodBuildSimulationNode({
      itemCode: row.itemCode,
      plannedQty,
      toDate: to,
      avgMonths,
      horizonMonths,
      shiftHours,
      depth: 0,
      maxDepth: Math.max(1, Number(maxDepth || 3)),
      trail: [],
    });
    tree.push(node);
  }

  const acc = {
    totalHours: 0,
    totalPlannedQty: 0,
    finishedItemsCount: 0,
    shortageMaterialsCount: 0,
    totalShortageQty: 0,
    materialSet: new Set(),
    shortageMaterialSet: new Set(),
    resourceSet: new Set(),
  };
  for (const node of tree) prodAccumulateSimulationSummaryNode(node, acc);

  return {
    ok: true,
    generatedAt: new Date().toISOString(),
    lastSyncAt: await getState("production_last_sync_at"),
    filters: {
      from, to, area: String(area || "__ALL__"), grupo: String(grupo || "__ALL__"),
      sizeUom: String(sizeUom || "__ALL__"), abc: String(abc || "__ALL__"),
      type: prodNormalizeTypeFilter(typeFilter || "Se fabrica en planta"),
      q: String(q || ""),
      avgMonths: Math.max(1, Number(avgMonths || horizonMonths || 3)),
      horizonMonths: Math.max(1, Number(horizonMonths || 3)),
      shiftHours: Math.max(1, Number(shiftHours || 8)),
      maxDepth: Math.max(1, Number(maxDepth || 3)),
    },
    selectedItems: selectedItems.map((x) => ({
      itemCode: x.itemCode,
      itemDesc: x.itemDesc,
      grupo: x.grupo,
      area: x.area,
      productionNeeded: prodRound(x.productionNeeded, 3),
      productionAdjusted: prodRound(x.productionAdjusted, 3),
      stockTotal: prodRound(x.stockTotal, 3),
      hoursNeeded: prodRound(x.hoursNeeded, 3),
      machine: x.machine,
    })),
    summary: {
      selectedItems: acc.finishedItemsCount || tree.length,
      totalPlannedQty: prodRound(acc.totalPlannedQty, 3),
      totalHours: prodRound(acc.totalHours, 3),
      uniqueMaterials: acc.materialSet.size,
      uniqueMissingMaterials: acc.shortageMaterialSet.size,
      shortageMaterialsCount: acc.shortageMaterialsCount,
      totalShortageQty: prodRound(acc.totalShortageQty, 3),
      uniqueResources: acc.resourceSet.size,
    },
    tree,
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
  const urgentAbStockRiskItems = prodBuildUrgentAbStockItems(items).slice(0, 80).map((x) => ({
    ...x,
    linea: prodMachineAiLabel(x?.machine || ''),
    urgenciaPctVsMinimo: prodNum(x?.urgencyPctVsMin),
  }));
  const total = data?.totals || {};
  const maquinas = Array.isArray(data?.machineAgg) ? data.machineAgg : [];
  return {
    filtros: {
      desde: data?.from || "",
      hasta: data?.to || "",
      area: data?.area || "__ALL__",
      grupo: data?.grupo || "__ALL__",
      busqueda: data?.q || "",
      horizonteMeses: data?.horizonMonths || data?.avgMonths || 3,
    },
    resumen: {
      ingreso: prodNum(total.revenue),
      produccionNecesaria: prodNum(total.productionNeeded),
      produccionAjustada: prodNum(total.productionAdjusted),
      horasEstimadas: prodNum(total.hoursNeeded),
      stockActual: prodNum(total.stockTotal),
      itemsEnRiesgo: prodNum(total.riskCount),
      itemsAbCriticos: prodNum(total.abCount),
    },
    cargaPorLinea: maquinas.map((m) => ({
      linea: prodMachineAiLabel(m?.machine || ''),
      items: prodNum(m?.items),
      produccionAjustada: prodNum(m?.productionAdjusted),
      horasEstimadas: prodNum(m?.hoursNeeded),
    })),
    cantidadItemsFiltrados: items.length,
    itemsFiltrados: compactItems,
    itemsAbCriticosUrgentes: urgentAbStockRiskItems,
  };
}

function prodAiCompactPlan(plan) {
  if (!plan) return null;

  const rawMaterials = Array.isArray(plan.requirements?.rawMaterials) ? plan.requirements.rawMaterials : [];
  const packaging = Array.isArray(plan.requirements?.packaging) ? plan.requirements.packaging : [];
  const bottlenecks = Array.isArray(plan.requirements?.bottlenecks) ? plan.requirements.bottlenecks : [];
  const resources = Array.isArray(plan.requirements?.resources) ? plan.requirements.resources : [];
  const inv = plan.inventory || {};
  const mrp = plan.mrp || {};
  const prod = plan.production || {};
  const cost = plan.costing || {};
  const cap = plan.capacity || {};

  const materialRows = [...rawMaterials, ...packaging]
    .map((x) => ({
      codigo: String(x?.code || ''),
      descripcion: String(x?.description || ''),
      tipo: String(x?.componentType === 'PACKAGING' ? 'EMPAQUE' : 'MATERIA PRIMA'),
      requerido: prodNum(x?.requiredQty),
      stockActual: prodNum(x?.stockQty),
      faltante: prodNum(x?.shortageQty),
      unidad: String(x?.unit || ''),
      proveedor: prodCleanText(x?.supplier || ''),
      estado: String(x?.status || ''),
    }))
    .sort((a, b) => prodNum(b.faltante) - prodNum(a.faltante) || prodNum(b.requerido) - prodNum(a.requerido))
    .slice(0, 120);

  const totalFaltanteMateriales = materialRows.reduce((acc, x) => acc + prodNum(x.faltante), 0);
  const componentesConFaltante = materialRows.filter((x) => prodNum(x.faltante) > 0).length;
  const puedeFabricarseConInventario = componentesConFaltante === 0;

  return {
    codigo: plan.itemCode,
    descripcion: plan.itemDesc,
    grupo: plan.grupo,
    area: plan.area,
    linea: prodMachineAiLabel(plan.machine),
    periodo: plan.period,
    promedioMensualUnidades: plan.avgMonthlyQty,
    demandaHorizonteUnidades: plan.projectedQty,
    historialMensual: (plan.salesHistory || []).map((x) => ({
      mes: x.label,
      ventasUnidades: x.qty,
      produccionUnidades: x.producedQty || 0,
      costoPonderado: x.weightedCost || 0,
      costoPromedio: x.avgProductionCost || 0,
    })),
    costo: {
      costoPonderadoUnitario: cost.weightedCost || 0,
      valorStock: cost.stockValue || 0,
      costoDemanda: cost.projectedDemandCost || 0,
      costoProduccionAjustada: cost.adjustedProductionCost || 0,
    },
    ordenesRecientes: (plan.recentProductionOrders || []).slice(0, 20).map((x) => ({
      numero: x.docNum,
      fecha: x.postDate,
      planificado: x.plannedQty,
      completado: x.completedQty,
      estado: x.status,
      bodega: x.warehouse,
    })),
    inventario: {
      bodega01: prodNum(inv.wh01),
      bodega03: prodNum(inv.wh03),
      bodega12: prodNum(inv.wh12),
      bodega200: prodNum(inv.wh200),
      bodega300: prodNum(inv.wh300),
      bodega500: prodNum(inv.wh500),
      stockTotal: prodNum(inv.stockTotal),
      stockMinimo: prodNum(inv.stockMin),
      stockMaximo: prodNum(inv.stockMax),
    },
    abastecimiento: {
      seProduceEnPlanta: prodCleanText(plan.procurementMethodLabel || mrp.procurementMethodLabel || ''),
      diasAbastecimiento: prodNum(mrp.leadTimeDays),
      minimoCompra: prodNum(mrp.minOrderQty),
      multiplo: prodNum(mrp.multipleQty),
    },
    produccion: {
      politicaInventario: prod.coveragePolicyLabel || '',
      objetivoInventario: prodNum(prod.targetInventoryQty),
      produccionNecesaria: prodNum(prod.neededQty),
      produccionAjustada: prodNum(prod.adjustedQty),
      loteRecomendado: prodNum(prod.recommendedQty),
      horasRequeridas: prodNum(cap.hoursNeeded),
      horasPorTurno: prodNum(cap.shiftHours),
      unidadesPorHora: prodNum(cap.unitsPerHour),
    },
    inventarioMateriales: {
      puedeFabricarseConInventario,
      componentesConFaltante,
      totalFaltante: prodRound(totalFaltanteMateriales, 3),
      materialesEvaluados: materialRows.length,
    },
    materialesResumen: materialRows,
    materiales: rawMaterials,
    empaques: packaging,
    cuellosBotella: bottlenecks,
    recursos: resources,
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
    "Responde siempre en español claro y ejecutivo.",
    "Usa solo la información entregada por el sistema.",
    "Nunca menciones nombres internos de campos, estructuras técnicas, variables, fórmulas con símbolos, código, ni palabras como JSON.",
    "El inventario de materias primas y empaques es prioritario. Si la información del plan incluye materiales, debes usarla como base principal para decidir si un producto se puede fabricar o no.",
    "No uses términos técnicos como avgMonthlyQty, stockMin, productionAdjusted, hoursNeeded, belowMin, source o similares.",
    "Cuando hables de líneas de producción usa siempre SALSAS y LIMPIEZA. Nunca uses SAUCES ni CLEANING.",
    "Habla como usuario de negocio: ventas, stock, mínimo, máximo, producción necesaria, producción ajustada, horas, costo, materiales, empaques, cuellos de botella y recursos.",
    "Las cantidades de producto terminado siempre se expresan en UNIDADES.",
    "Si el usuario pide ranking, tabla, lista, top, columnas, comparación o detalle, responde con tabla markdown completa y encabezados claros en español.",
    "No inventes datos. Si algo no viene en la información, dilo de forma simple y breve.",
    "Nunca digas que solo hay inventario de producto terminado si el contexto incluye materiales, empaques, cuellos de botella o inventario de componentes.",
    "Cuando el usuario pregunte por un plan de producción, responde con este enfoque: 1) demanda y proyección 2) inventario y cobertura 3) producción necesaria y producción ajustada 4) materiales y empaques 5) cuellos de botella y recursos 6) capacidad y turnos 7) conclusión con acciones.",
    "Para productos en riesgo urgente, usa criterio estricto: stock actual por debajo del mínimo.",
    "Si hay varios códigos pedidos por el usuario, compáralos y priorízalos.",
    "Usa revenue como ingreso, gp como ganancia y gpPct como margen cuando estén disponibles, pero exprésalo con palabras de negocio.",
    "Ten en cuenta todo el sistema: ingreso, ganancia, margen, promedio mensual, producción necesaria, producción ajustada, horas, stock, mínimo, máximo, lead time, múltiplos, materiales, empaques, recursos, cuellos de botella, órdenes recientes, costo ponderado y capacidad. El inventario de materiales y empaques debe ser lo principal cuando el usuario pregunte qué se puede fabricar según el inventario.",
    "Preguntas frecuentes que debes resolver bien: priorización por riesgo, optimización de horas, simulación de demanda, brecha contra capacidad, redistribución, comparación por línea, forecast, sobreproducción, reducción de costos, alertas tempranas, recalculo de ABC, plan semanal, desbalance demanda vs producción, productos ineficientes, cambios de política de inventario, escenarios extremos e impacto total en el negocio.",
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


function prodSizeSortValue(v = "") {
  const txt = String(v == null ? "" : v).replace(',', '.').trim();
  const n = Number.parseFloat(txt);
  return Number.isFinite(n) ? n : -1;
}
function prodAbcPriority(v = "") {
  const t = String(v || "");
  if (t.startsWith("AB")) return 3;
  if (t.startsWith("C")) return 2;
  return 1;
}
function prodUrgentFinishedPriority(row = {}) {
  const abc = prodAbcPriority(row?.totalLabel || row?.abc || '');
  const stockTotal = Math.max(0, prodNum(row?.stockTotal || row?.currentStockQty || 0));
  const stockMin = Math.max(0, prodNum(row?.stockMin || 0));
  const productionAdjusted = Math.max(0, prodNum(row?.productionAdjusted || row?.neededQty || 0));
  const gapVsMin = Math.max(0, stockMin - stockTotal);
  const gapVsDemand = Math.max(0, productionAdjusted - stockTotal);
  const zeroStockBoost = stockTotal <= 0.0001 ? 250000 : 0;
  const abBoost = abc >= 3 ? 120000 : abc >= 2 ? 40000 : 0;
  return zeroStockBoost + abBoost + (gapVsMin * 100) + gapVsDemand;
}
function prodIsFinishedGoodCode(code = "", desc = "", sizeUom = "") {
  const c = String(code || "").trim().toUpperCase();
  const d = String(desc || "").toLowerCase();
  const s = String(sizeUom || "").trim();
  if (/^\d{3,6}(?:-[A-Z0-9]+)?$/.test(c)) return true;
  if (s && s !== "__ALL__" && s !== "S/Tamaño" && /^\d+(?:[.,]\d+)?(?:\s?[a-zA-Z]+)?$/.test(s)) return true;
  if (/\b\d+(?:[.,]\d+)?\s?(oz|onz|ml|lt|l|gal)\b/i.test(d) && !/(semi|semielab|liquido base|líquido base|granel)/i.test(d)) return true;
  return false;
}
function prodMonthLabelFromDate(dateStr) {
  const d = new Date(`${String(dateStr || "").slice(0,10)}T00:00:00`);
  if (Number.isNaN(d.getTime())) return "Mes";
  return d.toLocaleDateString("es-PA", { month: "short", year: "numeric" });
}
function prodPlanFamilyKey(row = {}, plan = null) {
  const src = `${row?.area || ""} ${row?.grupo || ""} ${row?.machine || ""} ${row?.itemDesc || ""} ${plan?.itemDesc || ""}`.toLowerCase();
  if (/(pisc|pool|cloro|hipoclor|algic|alguic)/i.test(src)) return "PISCINA";
  if (/(limp|multi cleaner|deterg|lavanda|suavizante|planchado|bano|baño|bath|clean)/i.test(src)) return "LIMPIEZA";
  return "SAZONADORES";
}
function prodPlanFamilyMeta(key = "") {
  const k = String(key || "").toUpperCase();
  if (k === "LIMPIEZA") return { key: "LIMPIEZA", label: "Productos de limpieza", minDays: 3, priority: 2 };
  if (k === "PISCINA") return { key: "PISCINA", label: "Químicos de piscina", minDays: 3, priority: 1 };
  return { key: "SAZONADORES", label: "Sazonadores y salsas", minDays: 2, priority: 3 };
}
function prodWeekLabelFromDate(dateStr) {
  const d = new Date(`${String(dateStr || '').slice(0,10)}T00:00:00`);
  if (Number.isNaN(d.getTime())) return "Semana";
  const start = new Date(d);
  const day = start.getDay() || 7;
  start.setDate(start.getDate() - day + 1);
  const end = new Date(start);
  end.setDate(end.getDate() + 4);
  const fmt = (x) => `${String(x.getDate()).padStart(2,'0')}/${String(x.getMonth()+1).padStart(2,'0')}`;
  return `Sem ${fmt(start)}-${fmt(end)}`;
}
function prodPlanNextWorkday(dateStr) {
  let d = new Date(`${String(dateStr || getDateISOInOffset(TZ_OFFSET_MIN)).slice(0,10)}T00:00:00`);
  if (Number.isNaN(d.getTime())) d = new Date(`${getDateISOInOffset(TZ_OFFSET_MIN)}T00:00:00`);
  while ([0,6].includes(d.getDay())) d.setDate(d.getDate() + 1);
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
}
function prodIsoDateTime(dateStr, hoursFloat, baseHour = 7) {
  const totalMinutes = Math.round((Number(hoursFloat || 0) + baseHour) * 60);
  const hh = Math.floor(totalMinutes / 60);
  const mm = totalMinutes % 60;
  return `${String(dateStr || '').slice(0,10)}T${String(hh).padStart(2,'0')}:${String(mm).padStart(2,'0')}:00`;
}
function prodMoveToNextWorkday(dateStr) {
  const d = new Date(`${String(dateStr || '').slice(0,10)}T00:00:00`);
  d.setDate(d.getDate() + 1);
  while ([0,6].includes(d.getDay())) d.setDate(d.getDate() + 1);
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
}

const PROD_PLAN_DAY_START_HOUR = 7;
const PROD_PLAN_DAY_END_HOUR = 16;
const PROD_PLAN_BREAKS = [
  { code: 'breakfast', thresholdHours: 2, durationHours: 0.25, label: 'desayuno' },
  { code: 'lunch', thresholdHours: 5, durationHours: 0.75, label: 'almuerzo' },
];
function prodPlanEffectiveDayHours(requestedHours = 9) {
  const hours = prodRound(prodNum(requestedHours || 9), 2);
  return Math.max(0.5, Math.min(9, hours || 9));
}
function prodPlanLaneKey(familyKey = '', row = {}, plan = null) {
  const fam = String(familyKey || '').toUpperCase();
  const src = `${row?.machine || ''} ${row?.area || ''} ${row?.grupo || ''} ${row?.itemDesc || ''} ${plan?.itemDesc || ''}`.toLowerCase();
  if (fam === 'SAZONADORES' || /(salsa|sazon|vinagre|condiment|food|alimento)/i.test(src)) return 'LINEA_SALSAS';
  return 'LINEA_LIMPIEZA';
}
function prodPlanLaneMeta(laneKey = '') {
  const k = String(laneKey || '').toUpperCase();
  if (k === 'LINEA_LIMPIEZA') {
    return { key: 'LINEA_LIMPIEZA', label: 'Máquina de productos de limpieza', shortLabel: 'Limpieza', priority: 2 };
  }
  return { key: 'LINEA_SALSAS', label: 'Máquina de salsas', shortLabel: 'Salsas', priority: 3 };
}
function prodClockOffsetFromProductiveOffset(productiveHours = 0, edge = 'end') {
  const hours = Math.max(0, prodNum(productiveHours));
  let offset = hours;
  for (const br of PROD_PLAN_BREAKS) {
    const threshold = prodNum(br?.thresholdHours);
    const duration = Math.max(0, prodNum(br?.durationHours));
    const shouldApply = edge === 'start' ? hours >= threshold - 0.0001 : hours > threshold + 0.0001;
    if (shouldApply) offset += duration;
  }
  return prodRound(offset, 4);
}
function prodClockLabelFromOffset(clockOffset = 0, baseHour = PROD_PLAN_DAY_START_HOUR) {
  const totalMinutes = Math.round((prodNum(baseHour) + prodNum(clockOffset)) * 60);
  const hh = Math.floor(totalMinutes / 60);
  const mm = totalMinutes % 60;
  return `${String(hh).padStart(2,'0')}:${String(mm).padStart(2,'0')}`;
}
function prodIsoDateTimeByProductiveOffset(dateStr, productiveOffset = 0, baseHour = PROD_PLAN_DAY_START_HOUR, edge = 'end') {
  return prodIsoDateTime(dateStr, prodClockOffsetFromProductiveOffset(productiveOffset, edge), baseHour);
}
function prodTimeRangeLabelFromProductiveOffsets(startProd = 0, endProd = 0, baseHour = PROD_PLAN_DAY_START_HOUR) {
  return `${prodClockLabelFromOffset(prodClockOffsetFromProductiveOffset(startProd, 'start'), baseHour)} → ${prodClockLabelFromOffset(prodClockOffsetFromProductiveOffset(endProd, 'end'), baseHour)}`;
}
function prodPlanPolicy(capacity = {}) {
  const raw = capacity?.planPolicy || capacity?.rules || {};
  return {
    excludeCategoryD: raw.excludeCategoryD !== false,
    nonAbPreventOverstock: raw.nonAbPreventOverstock !== false,
    nonAbRequireHalfShift: raw.nonAbRequireHalfShift !== false,
    nonAbHalfShiftPct: Math.max(0.25, Math.min(1, prodNum(raw.nonAbHalfShiftPct || 0.5, 0.5))),
    cMinFillPctOfStockMax: Math.max(0.5, Math.min(1, prodNum(raw.cMinFillPctOfStockMax || 0.8, 0.8))),
    cMinRunQty: Math.max(0, prodNum(raw.cMinRunQty || raw.nonAbMinRunQty || 2000, 2000)),
    abMinRunQty: Math.max(0, prodNum(raw.abMinRunQty || 2000, 2000)),
    abMaxDailyOvertimeFactor: Math.max(1, Math.min(2, prodNum(raw.abMaxDailyOvertimeFactor || 1.5, 1.5))),
  };
}
function prodIsPlanAb(label) {
  return String(label || '').trim().toUpperCase().startsWith('AB');
}
function prodIsPlanC(label) {
  return String(label || '').trim().toUpperCase().startsWith('C');
}
function prodIsPlanD(label) {
  return String(label || '').trim().toUpperCase() === 'D';
}

function prodBuildPolicyBlockedItem({ row, familyKey = '', familyMeta = {}, laneKey = '', laneMeta = {}, neededQty = 0, possibleQty = 0, directMaterials = [], dispatchDemand = null, reason = '' }) {
  return {
    itemCode: row?.itemCode,
    itemDesc: row?.itemDesc,
    sizeUom: row?.sizeUom,
    abc: row?.totalLabel,
    familyKey,
    familyLabel: familyMeta?.label || 'Producción',
    laneKey,
    laneLabel: laneMeta?.label || '',
    neededQty: prodRound(neededQty || 0, 2),
    objectiveQty: prodRound(neededQty || 0, 2),
    possibleQty: prodRound(possibleQty || 0, 2),
    pendingQty: prodRound(Math.max(0, prodNum(neededQty) - prodNum(possibleQty)), 2),
    mainConstraint: String(reason || 'Regla de planificación automática'),
    currentStockQty: prodRound(row?.stockTotal || 0, 3),
    materials: directMaterials,
    dispatchDemand: dispatchDemand ? {
      qtyPending: prodRound(dispatchDemand.qtyPending || 0, 2),
      qtyOverdue: prodRound(dispatchDemand.qtyOverdue || 0, 2),
      maxDaysLate: prodNum(dispatchDemand.maxDaysLate),
      earliestDueDate: String(dispatchDemand.earliestDueDate || ''),
      docNums: Array.isArray(dispatchDemand.docNums) ? dispatchDemand.docNums.slice(0, 6) : [],
      priorityState: String(dispatchDemand.priorityState || ''),
    } : null,
  };
}
function prodPlanBalancedDayChunks(totalQty = 0, unitsPerHour = 0, effectiveDayHours = 8, { isAbCritical = false, maxDailyOvertimeFactor = 1.5 } = {}) {
  const total = Math.max(0, prodNum(totalQty));
  if (!(total > 0)) return [];
  const hourRate = Math.max(0, prodNum(unitsPerHour));
  const dayHours = Math.max(1, prodNum(effectiveDayHours, 8));
  if (!(hourRate > 0)) return [prodRound(total, 2)];
  const nominalDayQty = Math.max(1, hourRate * dayHours);
  const dayCapQty = isAbCritical
    ? Math.max(nominalDayQty, nominalDayQty * Math.max(1, prodNum(maxDailyOvertimeFactor, 1.5)))
    : nominalDayQty;
  const dayCount = Math.max(1, Math.ceil(total / Math.max(1, dayCapQty)));
  const chunks = [];
  let remaining = total;
  for (let i = 0; i < dayCount; i += 1) {
    const slotsLeft = Math.max(1, dayCount - i);
    let target = remaining / slotsLeft;
    if (isAbCritical) {
      if (remaining <= dayCapQty + 0.0001) target = remaining;
      target = Math.min(dayCapQty, target);
    } else {
      if (remaining <= nominalDayQty + 0.0001) target = remaining;
      target = Math.min(nominalDayQty, target);
    }
    target = Math.max(1, target);
    target = slotsLeft === 1 ? remaining : Math.min(remaining, target);
    const rounded = prodRound(target, 2);
    chunks.push(rounded);
    remaining = Math.max(0, prodRound(remaining - rounded, 6));
  }
  if (remaining > 0.001 && chunks.length) chunks[chunks.length - 1] = prodRound(chunks[chunks.length - 1] + remaining, 2);
  return chunks.filter((x) => prodNum(x) > 0.0001);
}


function prodConsolidateDashboardPlanItems(items = []) {
  const byCode = new Map();
  for (const raw of (Array.isArray(items) ? items : [])) {
    const itemCode = String(raw?.itemCode || '').trim();
    if (!itemCode) continue;
    const prev = byCode.get(itemCode);
    if (!prev) {
      byCode.set(itemCode, {
        ...raw,
        itemCode,
        productionAdjusted: prodNum(raw?.productionAdjusted || 0),
        productionNeeded: prodNum(raw?.productionNeeded || 0),
        revenue: prodNum(raw?.revenue || 0),
        avgMonthlyQty: prodNum(raw?.avgMonthlyQty || 0),
        hoursNeeded: prodNum(raw?.hoursNeeded || 0),
        sourceRows: 1,
      });
      continue;
    }
    prev.productionAdjusted = prodRound(prodNum(prev.productionAdjusted) + prodNum(raw?.productionAdjusted || 0), 3);
    prev.productionNeeded = prodRound(prodNum(prev.productionNeeded) + prodNum(raw?.productionNeeded || 0), 3);
    prev.revenue = prodRound(prodNum(prev.revenue) + prodNum(raw?.revenue || 0), 2);
    prev.avgMonthlyQty = prodRound(Math.max(prodNum(prev.avgMonthlyQty || 0), prodNum(raw?.avgMonthlyQty || 0)), 3);
    prev.hoursNeeded = prodRound(prodNum(prev.hoursNeeded || 0) + prodNum(raw?.hoursNeeded || 0), 3);
    prev.stockTotal = prodRound(Math.max(prodNum(prev.stockTotal || 0), prodNum(raw?.stockTotal || 0)), 3);
    prev.stockMin = prodRound(Math.max(prodNum(prev.stockMin || 0), prodNum(raw?.stockMin || 0)), 3);
    prev.stockMax = prodRound(Math.max(prodNum(prev.stockMax || 0), prodNum(raw?.stockMax || 0)), 3);
    prev.unitsPerHour = prodRound(Math.max(prodNum(prev.unitsPerHour || 0), prodNum(raw?.unitsPerHour || 0)), 4);
    prev.projectedQty = prodRound(Math.max(prodNum(prev.projectedQty || 0), prodNum(raw?.projectedQty || 0)), 3);
    prev.productionType = prev.productionType || raw?.productionType || '';
    prev.itemDesc = prev.itemDesc || raw?.itemDesc || '';
    prev.totalLabel = prev.totalLabel || raw?.totalLabel || '';
    prev.machine = prev.machine || raw?.machine || '';
    prev.area = prev.area || raw?.area || '';
    prev.grupo = prev.grupo || raw?.grupo || '';
    prev.sizeUom = prev.sizeUom || raw?.sizeUom || '';
    prev.sourceRows += 1;
  }
  return Array.from(byCode.values());
}

async function prodBuildLaneAwareGanttPlan({ from, to, area, grupo, sizeUom = '__ALL__', abc = '__ALL__', typeFilter = 'Se fabrica en planta', q = '', avgMonths = 0, horizonMonths = 1, shiftHours = 8, startDate = '', fullMaterials = false }) {
  const local = loadProductionLocalData();
  const planPolicy = prodPlanPolicy(local.capacity || {});
  const planningHorizonMonths = Math.max(1, Number(horizonMonths || 1));
  const planningAvgMonths = Math.max(3, Number(avgMonths || planningHorizonMonths || 3));
  const dash = await productionDashboardFromDb({ from, to, area, grupo, sizeUom, abc, typeFilter, q, avgMonths: planningAvgMonths, horizonMonths: planningHorizonMonths });
  const effectiveDayHours = prodPlanEffectiveDayHours(local.capacity?.shiftHours || shiftHours || 8);
  const dayStartHour = PROD_PLAN_DAY_START_HOUR;
  const dayEndHour = PROD_PLAN_DAY_END_HOUR;
  const today = getDateISOInOffset(TZ_OFFSET_MIN);
  const start = prodPlanNextWorkday(startDate || today);
  const planningHorizonDays = Math.max(30, Math.round(planningHorizonMonths * 30));
  const planEnd = addDaysISO(start, Math.max(0, planningHorizonDays - 1));
  const materialPool = new Map();
  const blockedItems = [];
  const rows = [];
  const dispatchInfo = await prodBuildCustomerDispatchAlerts({ today }).catch((err) => ({ alerts: [], byItem: [], byItemMap: new Map(), byItemIndex: {}, summary: { cardCode: PROD_SPECIAL_DISPATCH_CARD_CODE, warehouse: PROD_SPECIAL_DISPATCH_WAREHOUSE, error: err?.message || String(err) } }));
  const calendarSet = new Set();
  const laneStates = new Map();
  let totalNeededQty = 0;
  let totalPossibleQty = 0;
  let totalBlockedQty = 0;
  let totalScheduledHours = 0;

  const baseItems = prodConsolidateDashboardPlanItems(Array.isArray(dash?.items) ? dash.items : [])
    .filter((x) => prodNum(x?.productionAdjusted) > 0 && prodIsFinishedGoodCode(x?.itemCode, x?.itemDesc, x?.sizeUom))
    .slice(0, 180);

  const candidates = [];
  for (const row of baseItems) {
    const plan = await productionBuildItemPlanCached({ itemCode: row.itemCode, toDate: to, avgMonths: planningAvgMonths, horizonMonths: planningHorizonMonths, shiftHours: effectiveDayHours });
    prodPlanMaterialPoolSeed(materialPool, plan?.requirements?.all || [], row.productionAdjusted);

    const familyKey = prodPlanFamilyKey(row, plan);
    const familyMeta = prodPlanFamilyMeta(familyKey);
    const laneKey = prodPlanLaneKey(familyKey, row, plan);
    const laneMeta = prodPlanLaneMeta(laneKey);
    const litersPerUnit = prodNum(plan?.production?.litersPerUnit);
    const semiRate = String(row.itemCode || '') === '68243' ? 728 : (litersPerUnit > 0 ? 946.353 / litersPerUnit : 0);
    let unitsPerHour = Math.max(0, prodNum(plan?.capacity?.unitsPerHour || row?.unitsPerHour));
    const unitsPerShift = Math.max(1, prodNum(plan?.capacity?.unitsPerShift || (unitsPerHour * effectiveDayHours) || 0, 0));
    if (semiRate > 0) {
      const semiRatePerHour = semiRate / Math.max(1, effectiveDayHours);
      unitsPerHour = unitsPerHour > 0 ? Math.min(unitsPerHour, semiRatePerHour) : semiRatePerHour;
    }
    if (!(unitsPerHour > 0)) unitsPerHour = Math.max(1 / Math.max(1, effectiveDayHours), unitsPerShift / Math.max(1, effectiveDayHours));

    const dispatchDemand = prodDispatchItemLookup(dispatchInfo?.byItemMap || dispatchInfo?.byItemIndex || dispatchInfo?.byItem, String(row?.itemCode || '').trim()) || null;
    const dispatchGapQty = Math.max(0, prodNum(dispatchDemand?.qtyPending) - Math.max(0, prodNum(row?.stockTotal)));
    const rawFinishedQtyNeeded = Math.max(0, Math.ceil(Math.max(prodNum(row?.productionAdjusted), dispatchGapQty)));
    const abcLabel = String(row?.totalLabel || '');
    const isAbCritical = prodIsPlanAb(abcLabel);
    const isCImportant = prodIsPlanC(abcLabel);
    const isCategoryD = prodIsPlanD(abcLabel);
    const machineBatchQty = Math.max(1, Math.floor(prodNum(plan?.capacity?.unitsPerShift || 0) || Math.max(1, unitsPerHour * effectiveDayHours)));
    const gapToMax = Math.max(0, prodNum(row?.stockMax) - prodNum(row?.stockTotal));

    const directMaterials = [
      ...(Array.isArray(plan?.requirements?.rawMaterials) ? plan.requirements.rawMaterials : []),
      ...(Array.isArray(plan?.requirements?.packaging) ? plan.requirements.packaging : [])
    ]
      .map((x) => ({
        code: String(x?.code || '').trim(),
        description: String(x?.description || '').trim(),
        requiredQty: prodRound(x?.requiredQty, 3),
        stockQty: prodRound(x?.stockQty, 3),
        shortageQty: prodRound(x?.shortageQty, 3),
        unit: String(x?.unit || '').trim(),
        supplier: String(x?.supplier || '').trim(),
        procurementMethodLabel: String(x?.procurementMethodLabel || ''),
        componentType: String(x?.componentType || ''),
      }))
      .sort((a, b) => prodNum(b?.shortageQty) - prodNum(a?.shortageQty) || prodNum(b?.requiredQty) - prodNum(a?.requiredQty))
      .slice(0, 14);

    if (planPolicy.excludeCategoryD && isCategoryD) {
      blockedItems.push(prodBuildPolicyBlockedItem({
        row,
        familyKey,
        familyMeta,
        laneKey,
        laneMeta,
        neededQty: rawFinishedQtyNeeded,
        possibleQty: 0,
        directMaterials,
        dispatchDemand,
        reason: 'Regla automática: no fabricar productos categoría D.',
      }));
      continue;
    }

    let planningTargetQty = rawFinishedQtyNeeded;
    let policyReason = '';
    if (!isAbCritical) {
      planningTargetQty = gapToMax > 0 ? Math.min(rawFinishedQtyNeeded, gapToMax) : rawFinishedQtyNeeded;
      if (planPolicy.nonAbPreventOverstock && !(gapToMax > 0)) {
        policyReason = 'Regla automática: no sobre stockear; el artículo ya está en o por encima del stock máximo SAP.';
      } else if (isCImportant && prodNum(row?.stockMax) > 0) {
        const cThreshold = prodNum(row?.stockMax) * prodNum(planPolicy.cMinFillPctOfStockMax || 0.8);
        const cTargetQty = Math.max(0, cThreshold - prodNum(row?.stockTotal));
        if (gapToMax < cThreshold) {
          policyReason = `Regla automática: categoría C solo se fabrica si falta al menos ${prodRound((planPolicy.cMinFillPctOfStockMax || 0.8) * 100, 0)}% del stock máximo SAP.`;
        } else if (cTargetQty > 0) {
          planningTargetQty = Math.max(planningTargetQty, cTargetQty);
        }
      }
      if (!policyReason && isCImportant && prodNum(planPolicy.cMinRunQty || 0) > 0) {
        const cMinRunQty = prodNum(planPolicy.cMinRunQty || 0);
        if (planningTargetQty > 0 && planningTargetQty < cMinRunQty) {
          policyReason = `Regla automática: categoría C no se fabrica por debajo de ${prodRound(cMinRunQty, 0)} unidades.`;
        }
      }
      if (!policyReason && planPolicy.nonAbRequireHalfShift && machineBatchQty > 0) {
        const halfShiftQty = machineBatchQty * prodNum(planPolicy.nonAbHalfShiftPct || 0.5);
        if (planningTargetQty > 0 && planningTargetQty < halfShiftQty) {
          policyReason = `Regla automática: no producir menos de ${prodRound((planPolicy.nonAbHalfShiftPct || 0.5) * 100, 0)}% del lote práctico de la máquina.`;
        }
      }
    } else if (planPolicy.abMinRunQty > 0 && rawFinishedQtyNeeded > 0) {
      planningTargetQty = Math.max(planningTargetQty, prodNum(planPolicy.abMinRunQty));
    }

    if (policyReason) {
      blockedItems.push(prodBuildPolicyBlockedItem({
        row,
        familyKey,
        familyMeta,
        laneKey,
        laneMeta,
        neededQty: rawFinishedQtyNeeded,
        possibleQty: 0,
        directMaterials,
        dispatchDemand,
        reason: policyReason,
      }));
      continue;
    }

    planningTargetQty = Math.max(0, Math.ceil(planningTargetQty));
    const sapLotSize = Math.max(0, prodNum(plan?.mrp?.sapLotSize || 0));
    const minBatchQty = isAbCritical
      ? Math.max(1, prodNum(planPolicy.abMinRunQty || 0), sapLotSize)
      : Math.max(1, sapLotSize || 1);
    const finishedQtyNeeded = isAbCritical && sapLotSize > 0 && planningTargetQty > 0
      ? Math.ceil(planningTargetQty / sapLotSize) * sapLotSize
      : planningTargetQty;
    if (!(finishedQtyNeeded > 0)) continue;

    candidates.push({
      row,
      plan,
      familyKey,
      familyMeta,
      laneKey,
      laneMeta,
      unitsPerHour,
      rawFinishedQtyNeeded,
      finishedQtyNeeded,
      minBatchQty,
      directMaterials,
      isAbCritical,
      gapToMax: prodRound(gapToMax, 2),
      machineBatchQty: prodRound(machineBatchQty, 2),
      sharedSemiCode: String((Array.isArray(plan?.requirements?.all) ? plan.requirements.all : []).find((x) => String(x?.procurementMethodLabel || '') === 'Se fabrica en planta')?.code || '').trim(),
      dispatchDemand,
      dispatchGapQty: prodRound(dispatchGapQty, 2),
    });
  }

  candidates.sort((a, b) => {
    const ab = prodAbcPriority(b?.row?.totalLabel) - prodAbcPriority(a?.row?.totalLabel);
    if (ab) return ab;
    const stockLow = prodNum(a?.row?.stockTotal) - prodNum(b?.row?.stockTotal);
    if (stockLow) return stockLow;
    const gapVsMax = prodNum(b?.gapToMax) - prodNum(a?.gapToMax);
    if (gapVsMax) return gapVsMax;
    const dispatchPrio = prodNum(b?.dispatchDemand?.priorityScore) - prodNum(a?.dispatchDemand?.priorityScore);
    if (dispatchPrio) return dispatchPrio;
    const dispatchLate = prodNum(b?.dispatchDemand?.maxDaysLate) - prodNum(a?.dispatchDemand?.maxDaysLate);
    if (dispatchLate) return dispatchLate;
    const dispatchPending = prodNum(b?.dispatchDemand?.qtyPending) - prodNum(a?.dispatchDemand?.qtyPending);
    if (dispatchPending) return dispatchPending;
    const urgent = prodUrgentFinishedPriority(b?.row) - prodUrgentFinishedPriority(a?.row);
    if (urgent) return urgent;
    const lane = prodNum(b?.laneMeta?.priority) - prodNum(a?.laneMeta?.priority);
    if (lane) return lane;
    const fam = prodNum(b?.familyMeta?.priority) - prodNum(a?.familyMeta?.priority);
    if (fam) return fam;
    const deficit = prodNum(b?.row?.productionAdjusted) - prodNum(a?.row?.productionAdjusted);
    if (deficit) return deficit;
    const s = prodSizeSortValue(b?.row?.sizeUom) - prodSizeSortValue(a?.row?.sizeUom);
    if (s) return s;
    const p = prodNum(b?.finishedQtyNeeded) - prodNum(a?.finishedQtyNeeded);
    if (p) return p;
    return prodNum(b?.row?.revenue) - prodNum(a?.row?.revenue);
  });

  const getLaneState = (laneKey, laneMeta) => {
    if (!laneStates.has(laneKey)) {
      laneStates.set(laneKey, {
        laneKey,
        laneMeta,
        currentDate: start,
        currentHourOffset: 0,
        currentFamilyKey: '',
        currentFamilyMeta: null,
        producedQtyByItem: new Map(),
      });
    }
    return laneStates.get(laneKey);
  };

  for (const candidate of candidates) {
    const { row, plan, familyKey, familyMeta, laneKey, laneMeta, unitsPerHour, rawFinishedQtyNeeded, finishedQtyNeeded, minBatchQty, directMaterials, dispatchDemand, dispatchGapQty, isAbCritical } = candidate;
    if (!(finishedQtyNeeded > 0)) continue;

    const laneState = getLaneState(laneKey, laneMeta);
    if (laneState.currentFamilyKey && familyKey !== laneState.currentFamilyKey && laneState.currentHourOffset > 0.0001) {
      const changeoverHours = Math.max(0, Math.min(2, prodNum(local.capacity?.familyChangeoverHours || 0.25, 0.25)));
      laneState.currentHourOffset += changeoverHours;
      while (laneState.currentHourOffset >= effectiveDayHours - 0.0001) {
        laneState.currentDate = prodMoveToNextWorkday(laneState.currentDate);
        laneState.currentHourOffset = Math.max(0, laneState.currentHourOffset - effectiveDayHours);
      }
    }
    if (familyKey !== laneState.currentFamilyKey) {
      laneState.currentFamilyKey = familyKey;
      laneState.currentFamilyMeta = familyMeta;
    }

    const possibleInfo = fullMaterials
      ? { possibleQty: finishedQtyNeeded, mainConstraint: '', assumedFullMaterials: true }
      : await prodPlanPossibleQtyFromPoolDeep(plan, finishedQtyNeeded, materialPool, 0, [prodNormalizeItemCodeLoose(row.itemCode)]);
    let possibleQty = prodNum(possibleInfo?.possibleQty);
    const mainConstraint = String(possibleInfo?.mainConstraint || '');

    if (isAbCritical && minBatchQty > 0) {
      possibleQty = possibleQty >= minBatchQty ? Math.floor(possibleQty) : 0;
    } else {
      possibleQty = Math.floor(possibleQty);
    }
    possibleQty = Math.max(0, Math.min(finishedQtyNeeded, Math.floor(possibleQty)));

    totalNeededQty += finishedQtyNeeded;
    totalPossibleQty += possibleQty;
    totalBlockedQty += Math.max(0, finishedQtyNeeded - possibleQty);

    if (!(possibleQty > 0)) {
      blockedItems.push({
        itemCode: row.itemCode,
        itemDesc: row.itemDesc,
        sizeUom: row.sizeUom,
        abc: row.totalLabel,
        familyKey,
        familyLabel: familyMeta.label,
        laneKey,
        laneLabel: laneMeta.label,
        neededQty: finishedQtyNeeded,
        objectiveQty: rawFinishedQtyNeeded,
        possibleQty: 0,
        pendingQty: finishedQtyNeeded,
        mainConstraint: mainConstraint || 'Materia prima insuficiente',
        currentStockQty: prodRound(row?.stockTotal || 0, 3),
        materials: directMaterials,
        dispatchDemand: dispatchDemand ? {
          qtyPending: prodRound(dispatchDemand.qtyPending || 0, 2),
          qtyOverdue: prodRound(dispatchDemand.qtyOverdue || 0, 2),
          maxDaysLate: prodNum(dispatchDemand.maxDaysLate),
          earliestDueDate: String(dispatchDemand.earliestDueDate || ''),
          docNums: Array.isArray(dispatchDemand.docNums) ? dispatchDemand.docNums.slice(0, 6) : [],
          priorityState: String(dispatchDemand.priorityState || ''),
        } : null,
      });
      continue;
    }

const tasks = [];
let cumulativeProduced = 0;

if (laneState.currentHourOffset > 0.0001) {
  laneState.currentDate = prodMoveToNextWorkday(laneState.currentDate);
  laneState.currentHourOffset = 0;
}

const taskDate = laneState.currentDate;
const startOffset = 0;
const endOffset = effectiveDayHours;
const startStockQty = prodRound(prodNum(row?.stockTotal) + cumulativeProduced, 3);
cumulativeProduced += possibleQty;
const endStockQty = prodRound(prodNum(row?.stockTotal) + cumulativeProduced, 3);
calendarSet.add(taskDate);
tasks.push({
  taskId: `${row.itemCode}_${laneKey}_${taskDate}_1`,
  date: taskDate,
  weekLabel: prodWeekLabelFromDate(taskDate),
  monthLabel: prodMonthLabelFromDate(taskDate),
  qty: prodRound(possibleQty, 2),
  hours: prodRound(effectiveDayHours, 2),
  startAt: prodIsoDateTimeByProductiveOffset(taskDate, startOffset, dayStartHour, 'start'),
  endAt: prodIsoDateTimeByProductiveOffset(taskDate, endOffset, dayStartHour, 'end'),
  timeLabel: '07:00 → 16:00',
  fillPct: 100,
  stockStartQty: startStockQty,
  stockEndQty: endStockQty,
  laneKey,
  laneLabel: laneMeta.label,
});
laneState.currentDate = prodMoveToNextWorkday(laneState.currentDate);
laneState.currentHourOffset = 0;

totalScheduledHours += effectiveDayHours;
    if (!fullMaterials) {
      await prodPlanConsumePoolDeep(plan, possibleQty, materialPool, 0, [prodNormalizeItemCodeLoose(row.itemCode)]);
    }

    const blockedQty = Math.max(0, finishedQtyNeeded - possibleQty);
    rows.push({
      itemCode: row.itemCode,
      itemDesc: row.itemDesc,
      sizeUom: row.sizeUom,
      abc: row.totalLabel,
      machine: row.machine,
      machineLabel: laneMeta.label,
      familyKey,
      familyLabel: familyMeta.label,
      laneKey,
      laneLabel: laneMeta.label,
      neededQty: finishedQtyNeeded,
      objectiveQty: rawFinishedQtyNeeded,
      possibleQty,
      blockedQty,
      currentStockQty: prodRound(row?.stockTotal || 0, 3),
      targetStockQty: prodRound(prodNum(row?.stockTotal) + finishedQtyNeeded, 3),
      targetStockPolicyQty: prodRound(prodNum(row?.stockTotal) + rawFinishedQtyNeeded, 3),
      unitsPerHour: prodRound(unitsPerHour, 2),
      minBatchQty: prodRound(minBatchQty, 2),
      sharedSemiCode: candidate.sharedSemiCode,
      materials: directMaterials,
      tasks,
      dispatchDemand: dispatchDemand ? {
        qtyPending: prodRound(dispatchDemand.qtyPending || 0, 2),
        qtyOverdue: prodRound(dispatchDemand.qtyOverdue || 0, 2),
        maxDaysLate: prodNum(dispatchDemand.maxDaysLate),
        earliestDueDate: String(dispatchDemand.earliestDueDate || ''),
        docNums: Array.isArray(dispatchDemand.docNums) ? dispatchDemand.docNums.slice(0, 6) : [],
        priorityState: String(dispatchDemand.priorityState || ''),
      } : null,
      dispatchGapQty: prodRound(dispatchGapQty || 0, 2),
    });

    if (blockedQty > 0) {
      blockedItems.push({
        itemCode: row.itemCode,
        itemDesc: row.itemDesc,
        sizeUom: row.sizeUom,
        abc: row.totalLabel,
        familyKey,
        familyLabel: familyMeta.label,
        laneKey,
        laneLabel: laneMeta.label,
        neededQty: finishedQtyNeeded,
        objectiveQty: rawFinishedQtyNeeded,
        possibleQty,
        pendingQty: blockedQty,
        mainConstraint: mainConstraint || 'Materia prima insuficiente',
        currentStockQty: prodRound(row?.stockTotal || 0, 3),
        materials: directMaterials,
        dispatchDemand: dispatchDemand ? {
          qtyPending: prodRound(dispatchDemand.qtyPending || 0, 2),
          qtyOverdue: prodRound(dispatchDemand.qtyOverdue || 0, 2),
          maxDaysLate: prodNum(dispatchDemand.maxDaysLate),
          earliestDueDate: String(dispatchDemand.earliestDueDate || ''),
          docNums: Array.isArray(dispatchDemand.docNums) ? dispatchDemand.docNums.slice(0, 6) : [],
          priorityState: String(dispatchDemand.priorityState || ''),
        } : null,
      });
    }
  }

  rows.sort((a, b) => {
    const aTask = Array.isArray(a?.tasks) && a.tasks.length ? a.tasks[0] : null;
    const bTask = Array.isArray(b?.tasks) && b.tasks.length ? b.tasks[0] : null;
    const aDate = String(aTask?.date || '9999-12-31');
    const bDate = String(bTask?.date || '9999-12-31');
    if (aDate !== bDate) return aDate.localeCompare(bDate);
    const aStart = String(aTask?.startAt || aTask?.timeLabel || '');
    const bStart = String(bTask?.startAt || bTask?.timeLabel || '');
    if (aStart !== bStart) return aStart.localeCompare(bStart);
    const aLane = String(a?.laneLabel || a?.machineLabel || '');
    const bLane = String(b?.laneLabel || b?.machineLabel || '');
    if (aLane !== bLane) return aLane.localeCompare(bLane);
    return String(a?.itemCode || '').localeCompare(String(b?.itemCode || ''));
  });

  const calendarDays = [];
  for (let i = 0; i < planningHorizonDays; i += 1) {
    const date = addDaysISO(start, i);
    calendarDays.push({
      date,
      label: prodHumanDateShort(date),
      weekLabel: prodWeekLabelFromDate(date),
      monthLabel: prodMonthLabelFromDate(date),
    });
  }

  return {
    ok: true,
    generatedAt: new Date().toISOString(),
    lastSyncAt: await getState('production_last_sync_at'),
    scenario: { fullMaterials: !!fullMaterials, planStart: start, planEnd, planningHorizonDays },
    filters: { from, to, area, grupo, sizeUom, abc, type: typeFilter, q, requestedHorizonMonths: horizonMonths, horizonMonths: planningHorizonMonths, avgMonths: planningAvgMonths, shiftHours: effectiveDayHours, planningHorizonDays, planStart: start, planEnd },
    workRule: {
      shiftHours: prodRound(effectiveDayHours, 2),
      effectiveHours: prodRound(effectiveDayHours, 2),
      startHour: dayStartHour,
      endHour: dayEndHour,
      breaksHours: 1,
      breakfastHours: 0.25,
      lunchHours: 0.75,
      label: `Jornada 07:00–16:00 · ${prodRound(effectiveDayHours,2)} h productivas`,
      breaksLabel: '15 min de desayuno + 45 min de almuerzo',
      lanes: [
        prodPlanLaneMeta('LINEA_SALSAS'),
        prodPlanLaneMeta('LINEA_LIMPIEZA'),
      ],
    },
    calendarDays,
    items: rows,
    blockedItems,
    dispatchAlerts: dispatchInfo.alerts,
    dispatchSummary: dispatchInfo.summary,
    summary: {
      itemsPlanned: rows.length,
      totalNeededQty: prodRound(totalNeededQty, 2),
      totalPossibleQty: prodRound(totalPossibleQty, 2),
      totalBlockedQty: prodRound(totalBlockedQty, 2),
      totalScheduledHours: prodRound(totalScheduledHours, 2),
      fullMaterialsScenario: !!fullMaterials,
      priorityDispatchItems: prodNum(dispatchInfo?.summary?.pendingItems),
      priorityDispatchOverdue: prodNum(dispatchInfo?.summary?.overdueItems),
      priorityDispatchQty: prodRound(dispatchInfo?.summary?.pendingQty || 0, 2),
      rulesApplied: {
        excludeCategoryD: !!planPolicy.excludeCategoryD,
        nonAbPreventOverstock: !!planPolicy.nonAbPreventOverstock,
        nonAbRequireHalfShift: !!planPolicy.nonAbRequireHalfShift,
        cMinFillPctOfStockMax: prodRound(planPolicy.cMinFillPctOfStockMax || 0.8, 2),
        cMinRunQty: prodRound(planPolicy.cMinRunQty || 0, 0),
        abMinRunQty: prodRound(planPolicy.abMinRunQty || 0, 0),
        abMaxDailyOvertimeFactor: prodRound(planPolicy.abMaxDailyOvertimeFactor || 1.5, 2),
      },
    },
  };
}

function prodHumanDateShort(dateStr) {
  const d = new Date(`${String(dateStr || '').slice(0,10)}T00:00:00`);
  if (Number.isNaN(d.getTime())) return String(dateStr || '');
  return `${String(d.getDate()).padStart(2,'0')}/${String(d.getMonth()+1).padStart(2,'0')}`;
}
function prodPlanMaterialPoolSeed(pool, requirements = [], plannedQty = 0) {
  for (const req of Array.isArray(requirements) ? requirements : []) {
    if (String(req?.componentType || '').toUpperCase() === 'RESOURCE') continue;
    const code = String(req?.code || '').trim();
    if (!code) continue;
    const current = pool.get(code);
    const stockQty = prodNum(req?.stockQty);
    if (current == null || stockQty > current) pool.set(code, stockQty);
  }
  return pool;
}
function prodPlanPossibleQtyFromPool(plan, neededQty, pool) {
  const requirements = Array.isArray(plan?.requirements?.all) ? plan.requirements.all.filter((x) => String(x?.componentType || '').toUpperCase() !== 'RESOURCE') : [];
  if (!requirements.length) return Math.max(0, Math.floor(prodNum(neededQty)));
  const baseQty = Math.max(1, prodNum(plan?.production?.adjustedQty || plan?.production?.neededQty || neededQty || 1));
  let maxQty = Number.POSITIVE_INFINITY;
  let mainConstraint = '';
  for (const req of requirements) {
    const code = String(req?.code || '').trim();
    if (!code) continue;
    const perUnit = prodNum(req?.requiredQty) / baseQty;
    if (!(perUnit > 0)) continue;
    const available = prodNum(pool.get(code));
    const qty = Math.floor(available / perUnit);
    if (qty < maxQty) {
      maxQty = qty;
      mainConstraint = `${code} · ${String(req?.description || '').trim()}`.trim();
    }
  }
  if (!Number.isFinite(maxQty)) return Math.max(0, Math.floor(prodNum(neededQty)));
  return { possibleQty: Math.max(0, Math.min(Math.floor(prodNum(neededQty)), maxQty)), mainConstraint };
}
function prodPlanConsumePool(plan, qty, pool) {
  const requirements = Array.isArray(plan?.requirements?.all) ? plan.requirements.all.filter((x) => String(x?.componentType || '').toUpperCase() !== 'RESOURCE') : [];
  const baseQty = Math.max(1, prodNum(plan?.production?.adjustedQty || plan?.production?.neededQty || qty || 1));
  for (const req of requirements) {
    const code = String(req?.code || '').trim();
    if (!code) continue;
    const perUnit = prodNum(req?.requiredQty) / baseQty;
    if (!(perUnit > 0)) continue;
    const next = Math.max(0, prodNum(pool.get(code)) - (perUnit * prodNum(qty)));
    pool.set(code, prodRound(next, 6));
  }
}

async function prodPlanPossibleQtyFromPoolDeep(plan, neededQty, pool, depth = 0, trail = []) {
  const requirements = Array.isArray(plan?.requirements?.all)
    ? plan.requirements.all.filter((x) => String(x?.componentType || '').toUpperCase() !== 'RESOURCE')
    : [];
  if (!requirements.length) {
    return { possibleQty: Math.max(0, Math.floor(prodNum(neededQty))), mainConstraint: '' };
  }
  const baseQty = Math.max(1, prodNum(plan?.production?.adjustedQty || plan?.production?.neededQty || neededQty || 1));
  let maxQty = Number.POSITIVE_INFINITY;
  let mainConstraint = '';

  for (const req of requirements) {
    const code = String(req?.code || '').trim();
    if (!code) continue;
    const perUnit = prodNum(req?.requiredQty) / baseQty;
    if (!(perUnit > 0)) continue;

    let availableComponentQty = prodNum(pool.get(code));
    const isMake = String(req?.procurementMethodLabel || '') === 'Se fabrica en planta';
    const normalizedCode = prodNormalizeItemCodeLoose(code);

    if (isMake && depth < 2 && !trail.includes(normalizedCode)) {
      try {
        const childNeedQty = Math.max(prodNum(req?.requiredQty), prodNum(req?.subPlanQty), perUnit * Math.max(1, prodNum(neededQty)));
        const childPlan = await productionBuildItemPlanCached({
          itemCode: code,
          toDate: plan?.period?.endDate || getDateISOInOffset(TZ_OFFSET_MIN),
          avgMonths: plan?.period?.avgMonths || 3,
          horizonMonths: plan?.period?.horizonMonths || 1,
          shiftHours: plan?.capacity?.shiftHours || 8,
          plannedQtyOverride: childNeedQty,
        });
        prodPlanMaterialPoolSeed(pool, childPlan?.requirements?.all || [], childNeedQty);
        const childInfo = await prodPlanPossibleQtyFromPoolDeep(childPlan, childNeedQty, pool, depth + 1, [...trail, normalizedCode]);
        availableComponentQty += prodNum(childInfo?.possibleQty);
        if (!mainConstraint && childInfo?.mainConstraint) mainConstraint = String(childInfo.mainConstraint || '');
      } catch {}
    }

    const qty = Math.floor(availableComponentQty / perUnit);
    if (qty < maxQty) {
      maxQty = qty;
      mainConstraint = `${code} · ${String(req?.description || '').trim()}`.trim() || mainConstraint;
    }
  }

  if (!Number.isFinite(maxQty)) {
    return { possibleQty: Math.max(0, Math.floor(prodNum(neededQty))), mainConstraint };
  }
  return {
    possibleQty: Math.max(0, Math.min(Math.floor(prodNum(neededQty)), Math.floor(maxQty))),
    mainConstraint,
  };
}

async function prodPlanConsumePoolDeep(plan, qty, pool, depth = 0, trail = []) {
  const requirements = Array.isArray(plan?.requirements?.all)
    ? plan.requirements.all.filter((x) => String(x?.componentType || '').toUpperCase() !== 'RESOURCE')
    : [];
  const baseQty = Math.max(1, prodNum(plan?.production?.adjustedQty || plan?.production?.neededQty || qty || 1));

  for (const req of requirements) {
    const code = String(req?.code || '').trim();
    if (!code) continue;
    const perUnit = prodNum(req?.requiredQty) / baseQty;
    if (!(perUnit > 0)) continue;
    const neededComponentQty = prodRound(perUnit * prodNum(qty), 6);
    const isMake = String(req?.procurementMethodLabel || '') === 'Se fabrica en planta';
    const normalizedCode = prodNormalizeItemCodeLoose(code);

    if (isMake && depth < 2 && !trail.includes(normalizedCode)) {
      const currentStock = prodNum(pool.get(code));
      const fromStock = Math.min(currentStock, neededComponentQty);
      if (fromStock > 0) {
        pool.set(code, prodRound(currentStock - fromStock, 6));
      }
      const remainingChildQty = Math.max(0, neededComponentQty - fromStock);
      if (remainingChildQty > 0) {
        try {
          const childPlan = await productionBuildItemPlanCached({
            itemCode: code,
            toDate: plan?.period?.endDate || getDateISOInOffset(TZ_OFFSET_MIN),
            avgMonths: plan?.period?.avgMonths || 3,
            horizonMonths: plan?.period?.horizonMonths || 1,
            shiftHours: plan?.capacity?.shiftHours || 8,
            plannedQtyOverride: remainingChildQty,
          });
          prodPlanMaterialPoolSeed(pool, childPlan?.requirements?.all || [], remainingChildQty);
          await prodPlanConsumePoolDeep(childPlan, remainingChildQty, pool, depth + 1, [...trail, normalizedCode]);
        } catch {}
      }
      continue;
    }

    const next = Math.max(0, prodNum(pool.get(code)) - neededComponentQty);
    pool.set(code, prodRound(next, 6));
  }
}

async function productionBuildGanttPlan({ from, to, area, grupo, sizeUom = '__ALL__', abc = '__ALL__', typeFilter = 'Se fabrica en planta', q = '', avgMonths = 0, horizonMonths = 1, shiftHours = 8, startDate = '' }) {
  return prodBuildLaneAwareGanttPlan({ from, to, area, grupo, sizeUom, abc, typeFilter, q, avgMonths, horizonMonths, shiftHours, startDate, fullMaterials: false });
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
    const sizeUom = String(req.query?.sizeUom || req.query?.size || "__ALL__");
    const abc = String(req.query?.abc || "__ALL__");
    const typeFilter = String(req.query?.type || req.query?.tipo || "Se fabrica en planta");
    const horizonMonths = Math.max(1, Math.min(12, prodNum(req.query?.horizonMonths, 3)));
    const avgMonths = Math.max(1, Math.min(12, prodNum(req.query?.avgMonths, horizonMonths)));

    const cacheParams = { from, to, area, grupo, sizeUom, abc, typeFilter, q, avgMonths, horizonMonths };
    const cached = prodGetDashboardCached(cacheParams);
    if (cached) {
      cached.fromCache = true;
      return safeJson(res, 200, cached);
    }

    const out = await productionDashboardFromDb(cacheParams);
    out.fromCache = false;
    prodSetDashboardCached(cacheParams, out);
    return safeJson(res, 200, out);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});


function prodSapEscapeLiteral(v) {
  return String(v || "").replace(/'/g, "''");
}

function prodAsArray(v) {
  if (Array.isArray(v)) return v;
  if (Array.isArray(v?.value)) return v.value;
  return [];
}

function prodDateOnly(v) {
  return String(v || "").slice(0, 10);
}

function prodDiffDaysFromToday(isoDate) {
  const d = prodDateOnly(isoDate);
  if (!d) return 0;
  const a = new Date(`${d}T00:00:00`);
  const b = new Date(`${getDateISOInOffset(TZ_OFFSET_MIN)}T00:00:00`);
  return Math.round((b.getTime() - a.getTime()) / 86400000);
}

function prodLooseEqCode(a, b) {
  return prodNormalizeItemCodeLoose(a) === prodNormalizeItemCodeLoose(b);
}

function prodBuildItemSearchVariants(itemCode = "", itemDesc = "") {
  const raw = String(itemCode || "").trim();
  const rawDesc = String(itemDesc || "").trim();
  const codes = new Set();
  const push = (v) => { const s = String(v || "").trim(); if (s) codes.add(s); };
  push(raw);
  push(raw.replace(/\s+/g, ""));
  push(raw.replace(/PS$/i, ""));
  push(raw.replace(/MP$/i, ""));
  push(raw.replace(/EMPAQUE$/i, ""));
  const norm = prodNormalizeItemCodeLoose(raw);
  if (norm) push(norm);
  const tokens = Array.from(new Set(
    rawDesc
      .toUpperCase()
      .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
      .replace(/[^A-Z0-9 ]+/g, ' ')
      .split(/\s+/)
      .map((x) => x.trim())
      .filter((x) => x && x.length >= 3 && !['PARA','CON','DEL','LAS','LOS','THE','AND'].includes(x))
  )).slice(0, 6);
  return { codes: Array.from(codes), descTokens: tokens };
}

function prodLineMatchesProcurementSearch(lineCode = "", lineDesc = "", variants = {}) {
  const code = String(lineCode || "").trim();
  const desc = String(lineDesc || "")
    .toUpperCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  const codeList = Array.isArray(variants?.codes) ? variants.codes : [];
  for (const c of codeList) {
    if (!c) continue;
    if (code === c || prodLooseEqCode(code, c)) return true;
  }
  const tokens = Array.isArray(variants?.descTokens) ? variants.descTokens : [];
  if (tokens.length >= 2 && tokens.every((t) => desc.includes(String(t)))) return true;
  if (tokens.length >= 3) {
    let hits = 0;
    for (const t of tokens) if (desc.includes(String(t))) hits++;
    if (hits >= Math.min(3, tokens.length)) return true;
  }
  return false;
}

function prodProcurementRowIsOpen(row = null) {
  return !!(
    row && (
      prodDocStatusIsOpen(row?.documentStatus || row?.status || '') ||
      prodNum(row?.openQty || 0) > 0.0001
    )
  );
}

function prodSortProcurementRows(rows = []) {
  return [...(Array.isArray(rows) ? rows : [])].sort((a, b) => {
    const openCmp = Number(prodProcurementRowIsOpen(b)) - Number(prodProcurementRowIsOpen(a));
    if (openCmp) return openCmp;
    const dueCmp = String(b?.dueDate || '').localeCompare(String(a?.dueDate || ''));
    if (dueCmp) return dueCmp;
    const dateCmp = String(b?.docDate || '').localeCompare(String(a?.docDate || ''));
    if (dateCmp) return dateCmp;
    const docCmp = Number(b?.docNum || 0) - Number(a?.docNum || 0);
    if (docCmp) return docCmp;
    return Number(a?.lineNum || 0) - Number(b?.lineNum || 0);
  });
}

async function prodMapLimit(items = [], limit = 8, worker = async () => null) {
  const arr = Array.isArray(items) ? items : [];
  const size = Math.max(1, Number(limit || 1));
  const out = new Array(arr.length);
  let idx = 0;
  const runners = Array.from({ length: Math.min(size, arr.length || 0) }, async () => {
    while (idx < arr.length) {
      const cur = idx++;
      out[cur] = await worker(arr[cur], cur);
    }
  });
  await Promise.all(runners);
  return out;
}

function prodBuildProcurementItemAnyFilter(itemCode = '') {
  const code = String(itemCode || '').trim();
  if (!code) return '';
  const exact = prodSapEscapeLiteral(code);
  return `DocumentLines/any(l: l/ItemCode eq '${exact}')`;
}

async function prodReadPurchaseOrderDetails(docEntry) {
  const de = Number(docEntry);
  if (!(de > 0)) return null;
  const tries = [
    `/PurchaseOrders(${de})?$expand=DocumentLines`,
    `/PurchaseOrders?$filter=DocEntry eq ${de}&$expand=DocumentLines`,
  ];
  for (const path of tries) {
    try {
      const raw = await slFetchFreshSession(path);
      if (raw?.DocEntry || raw?.DocumentLines || raw?.value?.length) {
        const doc = raw?.DocEntry ? raw : (Array.isArray(raw?.value) && raw.value.length ? raw.value[0] : null);
        if (doc) return doc;
      }
    } catch {}
  }
  try {
    const base = await slFetchFreshSession(`/PurchaseOrders(${de})`);
    if (!base) return null;
    try {
      const linesRes = await slFetchFreshSession(`/PurchaseOrders(${de})/DocumentLines?$top=500`);
      const lines = prodAsArray(linesRes);
      if (lines.length) base.DocumentLines = lines;
    } catch {}
    return base;
  } catch {
    return null;
  }
}

async function prodReadPurchaseInvoiceDetails(docEntry) {
  const de = Number(docEntry);
  if (!(de > 0)) return null;
  const tries = [
    `/PurchaseInvoices(${de})?$expand=DocumentLines`,
    `/PurchaseInvoices?$filter=DocEntry eq ${de}&$expand=DocumentLines`,
  ];
  for (const path of tries) {
    try {
      const raw = await slFetchFreshSession(path);
      if (raw?.DocEntry || raw?.DocumentLines || raw?.value?.length) {
        const doc = raw?.DocEntry ? raw : (Array.isArray(raw?.value) && raw.value.length ? raw.value[0] : null);
        if (doc) return doc;
      }
    } catch {}
  }
  try {
    const base = await slFetchFreshSession(`/PurchaseInvoices(${de})`);
    if (!base) return null;
    try {
      const linesRes = await slFetchFreshSession(`/PurchaseInvoices(${de})/DocumentLines?$top=500`);
      const lines = prodAsArray(linesRes);
      if (lines.length) base.DocumentLines = lines;
    } catch {}
    return base;
  } catch {
    return null;
  }
}

async function prodFetchRecentPurchaseInvoicesForItem(itemCode, itemDesc = "", top = 5) {
  const code = String(itemCode || "").trim();
  const desc = String(itemDesc || "").trim();
  if (!code && !desc) return [];
  const out = [];
  const seen = new Set();
  const variants = prodBuildItemSearchVariants(code, desc);

  const collectFromDoc = (doc) => {
    if (!doc) return;
    const lines = prodAsArray(doc?.DocumentLines);
    for (const ln of lines) {
      const lineCode = String(ln?.ItemCode || "").trim();
      const lineDesc = String(ln?.ItemDescription || ln?.ItemDetails || "");
      if (!lineCode && !lineDesc) continue;
      if (!prodLineMatchesProcurementSearch(lineCode, lineDesc, variants)) continue;
      const key = `FACTURA_PROVEEDOR::${Number(doc?.DocEntry || 0)}::${Number(ln?.LineNum || 0)}`;
      if (seen.has(key)) continue;
      seen.add(key);
      out.push({
        docEntry: Number(doc?.DocEntry || 0),
        docNum: Number(doc?.DocNum || 0),
        docDate: prodDateOnly(doc?.DocDate),
        dueDate: prodDateOnly(doc?.DocDueDate || doc?.TaxDate),
        quantity: prodRound(prodNum(ln?.Quantity || 0), 3),
        lineTotal: prodRound(prodNum(ln?.LineTotal || 0), 2),
        docTotal: prodRound(prodNum(doc?.DocTotal || 0), 2),
        currency: String(doc?.DocCurrency || "USD"),
        supplierCode: String(doc?.CardCode || ""),
        supplierName: String(doc?.CardName || ""),
        documentStatus: String(doc?.DocumentStatus || doc?.Status || ""),
        openQty: prodRound(prodNum(ln?.OpenQuantity || ln?.OpenQty || 0), 3),
        lineNum: Number(ln?.LineNum || 0),
        itemCode: lineCode,
        itemDesc: lineDesc,
        sourceDocType: "FACTURA_PROVEEDOR",
      });
    }
  };

  const fastAnyFilter = prodBuildProcurementItemAnyFilter(code);
  if (fastAnyFilter) {
    try {
      const fastPath = `/PurchaseInvoices?$select=DocEntry,DocNum,DocDate,DocDueDate,CardCode,CardName,DocumentStatus,DocTotal,DocCurrency&$filter=${fastAnyFilter}&$orderby=DocDate desc&$top=${Math.max(20, Number(top || 5) * 4)}&$expand=DocumentLines`;
      const fastDocs = await slFetchFreshSession(fastPath);
      for (const doc of prodAsArray(fastDocs)) collectFromDoc(doc);
    } catch {}
  }

  if (out.length < top) {
    try {
      const expanded = await slFetchFreshSession(`/PurchaseInvoices?$orderby=DocDate desc&$top=80&$expand=DocumentLines`);
      for (const doc of prodAsArray(expanded)) collectFromDoc(doc);
    } catch {}
  }

  const scanBatch = async (skip = 0, topBatch = 100) => {
    const path = `/PurchaseInvoices?$select=DocEntry,DocNum,DocDate,DocDueDate,CardCode,CardName,DocumentStatus,DocTotal,DocCurrency&$orderby=DocDate desc&$top=${topBatch}&$skip=${skip}`;
    const headers = prodAsArray(await slFetchFreshSession(path).catch(() => []));
    const docs = await prodMapLimit(headers, 10, async (h) => {
      const de = Number(h?.DocEntry || 0);
      if (!(de > 0)) return null;
      return await prodReadPurchaseInvoiceDetails(de).catch(() => h || null);
    }).catch(() => []);
    for (const doc of docs) collectFromDoc(doc);
    return headers.length;
  };

  if (out.length < top) {
    for (let skip = 0; skip < 600 && out.length < Math.max(top, 5); skip += 100) {
      const count = await scanBatch(skip, 100).catch(() => 0);
      if (!(count > 0)) break;
    }
  }

  return prodSortProcurementRows(out).slice(0, Math.max(1, Number(top || 5)));
}

async function prodFetchRecentPurchaseOrdersForItem(itemCode, itemDesc = "", top = 5) {
  const code = String(itemCode || "").trim();
  const desc = String(itemDesc || "").trim();
  if (!code && !desc) return [];
  const out = [];
  const seen = new Set();
  const variants = prodBuildItemSearchVariants(code, desc);

  const collectFromDoc = (doc) => {
    if (!doc) return;
    const lines = prodAsArray(doc?.DocumentLines);
    for (const ln of lines) {
      const lineCode = String(ln?.ItemCode || "").trim();
      const lineDesc = String(ln?.ItemDescription || ln?.ItemDetails || "");
      if (!lineCode && !lineDesc) continue;
      if (!prodLineMatchesProcurementSearch(lineCode, lineDesc, variants)) continue;
      const key = `ORDEN_COMPRA::${Number(doc?.DocEntry || 0)}::${Number(ln?.LineNum || 0)}`;
      if (seen.has(key)) continue;
      seen.add(key);
      out.push({
        docEntry: Number(doc?.DocEntry || 0),
        docNum: Number(doc?.DocNum || 0),
        docDate: prodDateOnly(doc?.DocDate),
        dueDate: prodDateOnly(doc?.DocDueDate || doc?.TaxDate),
        quantity: prodRound(prodNum(ln?.Quantity || 0), 3),
        lineTotal: prodRound(prodNum(ln?.LineTotal || 0), 2),
        docTotal: prodRound(prodNum(doc?.DocTotal || 0), 2),
        currency: String(doc?.DocCurrency || "USD"),
        supplierCode: String(doc?.CardCode || ""),
        supplierName: String(doc?.CardName || ""),
        documentStatus: String(doc?.DocumentStatus || doc?.Status || ""),
        openQty: prodRound(prodNum(ln?.OpenQuantity || ln?.OpenQty || 0), 3),
        lineNum: Number(ln?.LineNum || 0),
        itemCode: lineCode,
        itemDesc: lineDesc,
        sourceDocType: "ORDEN_COMPRA",
      });
    }
  };

  const fastAnyFilter = prodBuildProcurementItemAnyFilter(code);
  if (fastAnyFilter) {
    try {
      const fastPath = `/PurchaseOrders?$select=DocEntry,DocNum,DocDate,DocDueDate,CardCode,CardName,DocumentStatus,DocTotal,DocCurrency&$filter=${fastAnyFilter}&$orderby=DocDate desc&$top=${Math.max(20, Number(top || 5) * 4)}&$expand=DocumentLines`;
      const fastDocs = await slFetchFreshSession(fastPath);
      for (const doc of prodAsArray(fastDocs)) collectFromDoc(doc);
    } catch {}
  }

  if (out.length < top) {
    try {
      const expanded = await slFetchFreshSession(`/PurchaseOrders?$orderby=DocDate desc&$top=80&$expand=DocumentLines`);
      for (const doc of prodAsArray(expanded)) collectFromDoc(doc);
    } catch {}
  }

  const scanBatch = async (skip = 0, topBatch = 100) => {
    const path = `/PurchaseOrders?$select=DocEntry,DocNum,DocDate,DocDueDate,CardCode,CardName,DocumentStatus,DocTotal,DocCurrency&$orderby=DocDate desc&$top=${topBatch}&$skip=${skip}`;
    const headers = prodAsArray(await slFetchFreshSession(path).catch(() => []));
    const docs = await prodMapLimit(headers, 10, async (h) => {
      const de = Number(h?.DocEntry || 0);
      if (!(de > 0)) return null;
      return await prodReadPurchaseOrderDetails(de).catch(() => h || null);
    }).catch(() => []);
    for (const doc of docs) collectFromDoc(doc);
    return headers.length;
  };

  if (out.length < top) {
    for (let skip = 0; skip < 600 && out.length < Math.max(top, 5); skip += 100) {
      const count = await scanBatch(skip, 100).catch(() => 0);
      if (!(count > 0)) break;
    }
  }

  return prodSortProcurementRows(out).slice(0, Math.max(1, Number(top || 5)));
}

async function prodFetchVendorCreditTerms(cardCode, fallbackName = "") {
  const code = String(cardCode || "").trim();
  if (!code) return {
    supplierCode: code,
    supplierName: String(fallbackName || ""),
    paymentTermsName: "",
    creditDays: 0,
    balanceRaw: 0,
    balanceDebt: 0,
    rawBusinessPartner: null,
  };

  let bp = null;
  const safe = prodSapEscapeLiteral(code);
  const bpPaths = [
    `/BusinessPartners?$select=CardCode,CardName,Balance,CurrentAccountBalance,DebitBalance,PayTermsGrpCode,GroupNum&$filter=CardCode eq '${safe}'&$top=1`,
    `/BusinessPartners('${encodeURIComponent(code)}')?$select=CardCode,CardName,Balance,CurrentAccountBalance,DebitBalance,PayTermsGrpCode,GroupNum`,
  ];
  for (const path of bpPaths) {
    try {
      const raw = await slFetchFreshSession(path);
      const row = Array.isArray(raw) ? (raw[0] || null) : (prodAsArray(raw)[0] || raw || null);
      if (row) { bp = row; break; }
    } catch {}
  }

  const result = {
    supplierCode: code,
    supplierName: String(bp?.CardName || fallbackName || ""),
    paymentTermsName: "",
    creditDays: 0,
    balanceRaw: 0,
    balanceDebt: 0,
    currentAccountBalance: 0,
    ocrdBalance: 0,
    debitBalance: 0,
    balanceSource: "",
    rawBusinessPartner: bp || null,
  };

  const currentAccountBalance = prodNum(bp?.CurrentAccountBalance || 0);
  const ocrdBalance = prodNum(bp?.Balance || 0);
  const debitBalance = prodNum(bp?.DebitBalance || 0);

  result.currentAccountBalance = currentAccountBalance;
  result.ocrdBalance = ocrdBalance;
  result.debitBalance = debitBalance;

  if (Number.isFinite(currentAccountBalance) && Math.abs(currentAccountBalance) > 0.0001) {
    result.balanceRaw = currentAccountBalance;
    result.balanceSource = "CurrentAccountBalance";
  } else if (Number.isFinite(ocrdBalance) && Math.abs(ocrdBalance) > 0.0001) {
    result.balanceRaw = ocrdBalance;
    result.balanceSource = "Balance";
  } else if (Number.isFinite(debitBalance) && Math.abs(debitBalance) > 0.0001) {
    result.balanceRaw = debitBalance;
    result.balanceSource = "DebitBalance";
  } else {
    result.balanceRaw = 0;
    result.balanceSource = "";
  }

  result.balanceDebt = Math.abs(prodNum(result.balanceRaw || 0));

  const termCode = Number(bp?.PayTermsGrpCode ?? bp?.GroupNum ?? 0);
  if (!(termCode > 0)) return result;

  const termPaths = [
    `/PaymentTermsTypes?$select=GroupNumber,PaymentTermsGroupName,NumberOfAdditionalDays,ExtraDays,ExtraMonth&$filter=GroupNumber eq ${termCode}&$top=1`,
    `/PaymentTermsTypes(${termCode})?$select=GroupNumber,PaymentTermsGroupName,NumberOfAdditionalDays,ExtraDays,ExtraMonth`,
    `/PaymentTermsTypes?$filter=GroupNumber eq ${termCode}`,
  ];

  for (const path of termPaths) {
    try {
      const raw = await slFetchFreshSession(path);
      const row = Array.isArray(raw) ? (raw[0] || null) : (prodAsArray(raw)[0] || raw || null);
      if (!row) continue;
      const name = String(row?.PaymentTermsGroupName || row?.Name || "").trim();
      let creditDays = Math.max(0, Number(row?.NumberOfAdditionalDays || 0), Number(row?.ExtraDays || 0));
      if (!(creditDays > 0) && name) {
        const m = name.match(/(\d+)\s*d[ií]a/i);
        if (m) creditDays = Number(m[1] || 0);
      }
      result.paymentTermsName = name;
      result.creditDays = creditDays > 0 ? creditDays : 0;
      return result;
    } catch {}
  }

  return result;
}

async function prodFetchVendorPayablesStatus(cardCode, cardName = "") {
  const code = String(cardCode || "").trim();
  const name = String(cardName || "").trim();
  const base = {
    supplierCode: code,
    supplierName: name,
    paymentStatus: "SIN_DATOS",
    amountDue: 0,
    oldestDebtDate: "",
    daysDue: 0,
    source: "",
    paymentTermsName: "",
    creditDays: 0,
    balanceRaw: 0,
    debtNote: "",
  };
  if (!code) return base;

  const termInfo = await prodFetchVendorCreditTerms(code, name).catch(() => ({
    supplierCode: code,
    supplierName: name,
    paymentTermsName: "",
    creditDays: 0,
    balanceRaw: 0,
    balanceDebt: 0,
  }));

  try {
    const safe = prodSapEscapeLiteral(code);
    const invoiceSelect = `DocNum,DocDate,DocDueDate,CardCode,CardName,DocumentStatus,DocTotal,PaidToDate,DocCurrency`;
    let inv = [];
    const invoicePaths = [
      `/PurchaseInvoices?$select=${invoiceSelect}&$filter=CardCode eq '${safe}' and DocumentStatus eq 'bost_Open'&$orderby=DocDueDate asc&$top=100`,
      `/PurchaseInvoices?$select=${invoiceSelect}&$filter=CardCode eq '${safe}' and DocumentStatus eq 'bo_Open'&$orderby=DocDueDate asc&$top=100`,
      `/PurchaseInvoices?$select=${invoiceSelect}&$filter=CardCode eq '${safe}'&$orderby=DocDueDate asc&$top=200`,
    ];
    for (const path of invoicePaths) {
      try {
        inv = prodAsArray(await slFetchFreshSession(path));
      } catch {
        inv = [];
      }
      if (inv.some((x) => prodDocStatusIsOpen(x?.DocumentStatus || x?.Status || ''))) break;
      if (inv.length && path === invoicePaths[invoicePaths.length - 1]) break;
    }
    const todayIso = getDateISOInOffset(TZ_OFFSET_MIN);
    const rows = prodAsArray(inv).map((x) => {
      const total = prodNum(x?.DocTotal || 0);
      const paid = prodNum(x?.PaidToDate || 0);
      const outstanding = Math.max(0, total - paid);
      const due = prodDateOnly(x?.DocDueDate || x?.DocDate);
      return {
        docNum: Number(x?.DocNum || 0),
        dueDate: due,
        docDate: prodDateOnly(x?.DocDate),
        outstanding: prodRound(outstanding, 2),
        status: String(x?.DocumentStatus || x?.Status || ""),
        currency: String(x?.DocCurrency || "USD"),
        overdue: !!(due && due < todayIso && outstanding > 0.009),
      };
    });

    const open = rows.filter((x) => x.outstanding > 0.009 || prodDocStatusIsOpen(x.status));
    const overdueRows = open.filter((x) => x.overdue);
    const oldest = overdueRows.map((x) => x.dueDate || x.docDate).filter(Boolean).sort()[0] || open.map((x) => x.dueDate || x.docDate).filter(Boolean).sort()[0] || "";
    const invoiceDebt = prodRound(open.reduce((s, x) => s + prodNum(x.outstanding || 0), 0), 2);
    const overdueDebt = prodRound(overdueRows.reduce((s, x) => s + prodNum(x.outstanding || 0), 0), 2);
    const accountDebt = prodRound(prodNum(termInfo?.balanceDebt || 0), 2);

    const useAccountBalance = accountDebt > 0.009;
    const finalDebt = useAccountBalance ? accountDebt : Math.max(invoiceDebt, 0);
    const effectiveDue = useAccountBalance ? 0 : Math.max(overdueDebt, 0);
    const debtBasis = useAccountBalance ? "SALDO_CUENTA" : (open.length ? "FACTURAS_ABIERTAS" : "");

    if (finalDebt > 0.009 || open.length || effectiveDue > 0.009) {
      return {
        supplierCode: code,
        supplierName: String(termInfo?.supplierName || name || ""),
        paymentStatus: "SE_DEBE",
        amountDue: finalDebt,
        overdueAmount: effectiveDue,
        oldestDebtDate: useAccountBalance ? "" : oldest,
        daysDue: useAccountBalance ? 0 : (oldest ? Math.max(0, prodDiffDaysFromToday(oldest)) : 0),
        source: useAccountBalance
          ? `BusinessPartners.${String(termInfo?.balanceSource || "CurrentAccountBalance")}`
          : (open.length ? "PurchaseInvoices" : "BusinessPartners"),
        debtBasis,
        openInvoices: open.slice(0, 10),
        paymentTermsName: String(termInfo?.paymentTermsName || ""),
        creditDays: Number(termInfo?.creditDays || 0),
        balanceRaw: prodRound(prodNum(termInfo?.balanceRaw || 0), 2),
        debtNote: useAccountBalance
          ? (Number(termInfo?.creditDays || 0) > 0
            ? `Saldo pendiente detectado por saldo de cuenta del proveedor en SAP. Condición de pago: ${termInfo.paymentTermsName || `Crédito ${termInfo.creditDays} días`}.`
            : "Saldo pendiente detectado por saldo de cuenta del proveedor en SAP.")
          : (oldest ? "" : (Number(termInfo?.creditDays || 0) > 0
            ? `Saldo pendiente visible en facturas / documentos del proveedor. Condición de pago: ${termInfo.paymentTermsName || `Crédito ${termInfo.creditDays} días`}.`
            : "Saldo pendiente visible en facturas / documentos del proveedor.")),
      };
    }

    return {
      supplierCode: code,
      supplierName: String(termInfo?.supplierName || name),
      paymentStatus: "PAZ_Y_SALVO",
      amountDue: 0,
      overdueAmount: 0,
      oldestDebtDate: "",
      daysDue: 0,
      source: `BusinessPartners.${String(termInfo?.balanceSource || "CurrentAccountBalance") || "CurrentAccountBalance"}`,
      debtBasis: "SALDO_CUENTA",
      openInvoices: [],
      paymentTermsName: String(termInfo?.paymentTermsName || ""),
      creditDays: Number(termInfo?.creditDays || 0),
      balanceRaw: prodRound(prodNum(termInfo?.balanceRaw || 0), 2),
      debtNote: "",
    };
  } catch {}

  const fallbackDebt = prodRound(prodNum(termInfo?.balanceDebt || 0), 2);
  return {
    supplierCode: code,
    supplierName: String(termInfo?.supplierName || name),
    paymentStatus: fallbackDebt > 0.009 ? "SE_DEBE" : "PAZ_Y_SALVO",
    amountDue: fallbackDebt,
    overdueAmount: 0,
    oldestDebtDate: "",
    daysDue: 0,
    source: `BusinessPartners.${String(termInfo?.balanceSource || "CurrentAccountBalance") || "CurrentAccountBalance"}`,
    debtBasis: "SALDO_CUENTA",
    openInvoices: [],
    paymentTermsName: String(termInfo?.paymentTermsName || ""),
    creditDays: Number(termInfo?.creditDays || 0),
    balanceRaw: prodRound(prodNum(termInfo?.balanceRaw || 0), 2),
    debtNote: fallbackDebt > 0.009
      ? (Number(termInfo?.creditDays || 0) > 0
        ? `Saldo pendiente detectado por saldo de cuenta del proveedor en SAP. Condición de pago: ${termInfo.paymentTermsName || `Crédito ${termInfo.creditDays} días`}.`
        : "Saldo pendiente detectado por saldo de cuenta del proveedor en SAP.")
      : "",
  };
}

app.get("/api/admin/production/component-procurement", verifyAdmin, async (req, res) => {
  try {
    const itemCode = String(req.query?.itemCode || "").trim();
    const itemDesc = String(req.query?.itemDesc || "").trim();
    const top = Math.max(1, Math.min(10, prodNum(req.query?.top, 5)));
    if (!itemCode) return safeJson(res, 400, { ok: false, message: "Falta itemCode" });

    const [purchaseInvoices, purchaseOrdersOnly] = await Promise.all([
      prodFetchRecentPurchaseInvoicesForItem(itemCode, itemDesc, top).catch(() => []),
      prodFetchRecentPurchaseOrdersForItem(itemCode, itemDesc, top).catch(() => []),
    ]);

    const mergedDocs = prodSortProcurementRows(
      [...purchaseInvoices, ...purchaseOrdersOnly].filter(Boolean)
    ).filter((row, idx, arr) => {
      const key = `${String(row?.sourceDocType || '')}::${Number(row?.docEntry || 0)}::${Number(row?.lineNum || 0)}`;
      return arr.findIndex((x) => `${String(x?.sourceDocType || '')}::${Number(x?.docEntry || 0)}::${Number(x?.lineNum || 0)}` === key) === idx;
    }).slice(0, top);

    const suppliers = Array.from(new Map(
      mergedDocs
        .map((x) => [String(x.supplierCode || "").trim(), String(x.supplierName || "").trim()])
        .filter(([code]) => !!code)
    ).entries()).map(([code, name]) => ({ code, name }));

    const vendorStatuses = await Promise.all(suppliers.map(async (supplier) => {
      return await prodFetchVendorPayablesStatus(supplier.code, supplier.name).catch(() => ({
        supplierCode: supplier.code,
        supplierName: supplier.name,
        paymentStatus: "SIN_DATOS",
        amountDue: 0,
        oldestDebtDate: "",
        daysDue: 0,
        source: "",
      }));
    }));

    const vendorMap = new Map(vendorStatuses.map((x) => [String(x.supplierCode || "").trim(), x]));
    const enrichedOrders = mergedDocs.map((row) => ({
      ...row,
      vendorStatus: vendorMap.get(String(row.supplierCode || "").trim()) || null,
    }));

    const purchaseSource = purchaseInvoices.length && purchaseOrdersOnly.length
      ? "Mixed"
      : purchaseInvoices.length
        ? "PurchaseInvoices"
        : purchaseOrdersOnly.length
          ? "PurchaseOrders"
          : "";

    return safeJson(res, 200, {
      ok: true,
      itemCode,
      itemDesc,
      purchaseOrders: enrichedOrders,
      purchaseSource,
      vendorStatuses,
      generatedAt: new Date().toISOString(),
      summary: {
        purchaseOrders: enrichedOrders.length,
        suppliers: vendorStatuses.length,
        anyDebt: vendorStatuses.some((x) => String(x?.paymentStatus || "") === "SE_DEBE"),
        openDocs: enrichedOrders.filter((x) => prodProcurementRowIsOpen(x)).length,
      },
    });
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
    const horizonMonths = Math.max(1, Math.min(12, prodNum(req.query?.horizonMonths, 3)));
    const avgMonths = Math.max(1, Math.min(12, prodNum(req.query?.avgMonths, horizonMonths)));
    const shiftHours = Math.max(1, Math.min(24, prodNum(req.query?.shiftHours, 8)));
    const plannedQty = Math.max(0, prodNum(req.query?.plannedQty, 0));

    await prodRefreshInventoryForCodes([itemCode]).catch(() => {});
    const plan = await productionBuildItemPlanCached({ itemCode, toDate, avgMonths, horizonMonths, shiftHours, plannedQtyOverride: plannedQty });

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


app.post("/api/admin/production/simulate", verifyAdmin, async (req, res) => {
  try {
    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = String(req.body?.from || req.query?.from || "2025-01-01");
    const to = String(req.body?.to || req.query?.to || today);
    const area = String(req.body?.area || req.query?.area || "__ALL__");
    const grupo = String(req.body?.grupo || req.query?.grupo || "__ALL__");
    const sizeUom = String(req.body?.sizeUom || req.query?.sizeUom || "__ALL__");
    const abc = String(req.body?.abc || req.query?.abc || "__ALL__");
    const typeFilter = String(req.body?.type || req.query?.type || "Se fabrica en planta");
    const q = String(req.body?.q || req.query?.q || "");
    const avgMonths = Math.max(1, Math.min(24, prodNum(req.body?.avgMonths || req.query?.avgMonths, 3)));
    const horizonMonths = Math.max(1, Math.min(12, prodNum(req.body?.horizonMonths || req.query?.horizonMonths, 3)));
    const shiftHours = Math.max(1, Math.min(24, prodNum(req.body?.shiftHours || req.query?.shiftHours, 8)));
    const maxDepth = Math.max(1, Math.min(5, prodNum(req.body?.maxDepth || req.query?.maxDepth, 3)));
    const itemCodes = Array.from(new Set((Array.isArray(req.body?.itemCodes) ? req.body.itemCodes : [])
      .map((x) => String(x || "").trim())
      .filter(Boolean)));

    if (!itemCodes.length) {
      return safeJson(res, 400, { ok: false, message: "Selecciona al menos un artículo para simular" });
    }

    const cacheParams = { from, to, area, grupo, sizeUom, abc, typeFilter, q, avgMonths, horizonMonths, shiftHours, itemCodes, maxDepth };
    const cached = prodGetSimulationCached(cacheParams);
    if (cached) {
      cached.fromCache = true;
      return safeJson(res, 200, cached);
    }

    const out = await productionBuildSimulationTree(cacheParams);
    out.fromCache = false;
    prodSetSimulationCached(cacheParams, out);
    return safeJson(res, 200, out);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});


app.get("/api/admin/production/gantt-plan", verifyAdmin, async (req, res) => {
  try {
    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = String(req.query?.from || "2025-01-01");
    const to = String(req.query?.to || today);
    const area = String(req.query?.area || "__ALL__");
    const grupo = String(req.query?.grupo || "__ALL__");
    const sizeUom = String(req.query?.sizeUom || req.query?.size || "__ALL__");
    const abc = String(req.query?.abc || "__ALL__");
    const typeFilter = String(req.query?.type || req.query?.tipo || "Se fabrica en planta");
    const q = String(req.query?.q || "");
    const horizonMonths = Math.max(1, Math.min(12, prodNum(req.query?.horizonMonths, 1)));
    const avgMonths = Math.max(1, Math.min(12, prodNum(req.query?.avgMonths, horizonMonths)));
    const shiftHours = Math.max(1, Math.min(24, prodNum(req.query?.shiftHours, 8)));
    const startDate = String(req.query?.startDate || today).slice(0,10);
    const out = await productionBuildGanttPlan({ from, to, area, grupo, sizeUom, abc, typeFilter, q, avgMonths, horizonMonths, shiftHours, startDate });
    return safeJson(res, 200, out);
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
    const sizeUom = String(req.body?.sizeUom || req.body?.size || "__ALL__");
    const abc = String(req.body?.abc || "__ALL__");
    const typeFilter = String(req.body?.type || req.body?.tipo || "Se fabrica en planta");
    const horizonMonths = Math.max(1, Math.min(12, prodNum(req.body?.horizonMonths, 3)));
    const avgMonths = Math.max(1, Math.min(12, prodNum(req.body?.avgMonths, horizonMonths)));
    const shiftHours = Math.max(1, Math.min(24, prodNum(req.body?.shiftHours, 8)));
    const itemCode = String(req.body?.itemCode || "").trim();

    const dashboard = await productionDashboardFromDb({ from, to, area, grupo, q, sizeUom, abc, typeFilter, avgMonths, horizonMonths });
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

    const needsMaterialView = prodQuestionNeedsMaterialInventory(question);
    if ((!plans.length && prodQuestionNeedsPlanDetails(question) && prodQuestionNeedsUrgentAbList(question)) || (needsMaterialView && !plans.length)) {
      const fallbackCodes = prodBuildFallbackPlanCodes(question, dashboard);
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
    const source = 'Sistema de producción PRODIMA';
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
      const days = Math.max(1, Math.min(120, prodNum(req.body?.days || req.query?.days, 5)));
      from = addDaysISO(today, -days);
      to = today;
    }

    const maxDocs = Math.max(50, Math.min(20000, prodNum(req.body?.maxDocs || req.query?.maxDocs, 4000)));

    const syncErrors = [];

    const salesSaved = await globalThis.syncSales({ from, to, maxDocs });
    const demandSaved = await syncProductionDemandOrders({ from, to, maxDocs });

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
    prodClearDashboardCache();
    prodClearSimulationCache();
    prodClearProductionRuntimeCaches();

    return safeJson(res, 200, {
      ok: true,
      from, to, maxDocs,
      salesSaved, demandSaved, groupsSaved, invSaved, invWhSaved, mrpSaved,
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
   Producción — Plan persistente + cierre diario SAP (v18)
========================================================= */
const PROD_PLAN_PERSIST_FILE = path.join(PROD_DATA_DIR, 'production_saved_plans_v18.json');

function prodEnsureProductionDir() {
  try { fs.mkdirSync(PROD_DATA_DIR, { recursive: true }); } catch {}
}
function prodSavedPlanEmptyStore() {
  return { records: [], activeByUser: {} };
}
function prodReadSavedPlanFile() {
  prodEnsureProductionDir();
  try {
    const raw = fs.readFileSync(PROD_PLAN_PERSIST_FILE, 'utf8');
    const data = JSON.parse(raw || '{}');
    if (!data || typeof data !== 'object') return prodSavedPlanEmptyStore();
    if (!Array.isArray(data.records)) data.records = [];
    if (!data.activeByUser || typeof data.activeByUser !== 'object') data.activeByUser = {};
    return data;
  } catch {
    return prodSavedPlanEmptyStore();
  }
}
function prodWriteSavedPlanFile(data) {
  prodEnsureProductionDir();
  fs.writeFileSync(PROD_PLAN_PERSIST_FILE, JSON.stringify(data || prodSavedPlanEmptyStore(), null, 2), 'utf8');
}
async function prodEnsureSavedPlanTable() {
  if (!hasDb()) return;
  await dbQuery(`
    create table if not exists admin_production_saved_plans_v18 (
      id text primary key,
      admin_user text not null,
      is_active boolean not null default true,
      payload jsonb not null,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    )
  `);
  await dbQuery(`create index if not exists idx_admin_production_saved_plans_v18_active on admin_production_saved_plans_v18(admin_user, is_active, updated_at desc)`);
}

__extraBootTasks.push(prodEnsureSavedPlanTable);

const PROD_PLAN_DAY_CLOSURE_FILE = path.join(PROD_DATA_DIR, 'production_day_closures_v1.json');

function prodDayClosureEmptyStore() {
  return { records: [] };
}
function prodReadDayClosureFile() {
  prodEnsureProductionDir();
  try {
    const raw = fs.readFileSync(PROD_PLAN_DAY_CLOSURE_FILE, 'utf8');
    const data = JSON.parse(raw || '{}');
    if (!data || typeof data !== 'object') return prodDayClosureEmptyStore();
    if (!Array.isArray(data.records)) data.records = [];
    return data;
  } catch {
    return prodDayClosureEmptyStore();
  }
}
function prodWriteDayClosureFile(data) {
  prodEnsureProductionDir();
  fs.writeFileSync(PROD_PLAN_DAY_CLOSURE_FILE, JSON.stringify(data || prodDayClosureEmptyStore(), null, 2), 'utf8');
}
async function prodEnsureDayClosureTable() {
  if (!hasDb()) return;
  await dbQuery(`
    create table if not exists admin_production_day_closures_v1 (
      admin_user text not null,
      closure_date date not null,
      plan_id text default '',
      summary jsonb not null,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      primary key (admin_user, closure_date)
    )
  `);
  await dbQuery(`create index if not exists idx_admin_production_day_closures_v1_user_updated on admin_production_day_closures_v1(admin_user, updated_at desc)`);
}
__extraBootTasks.push(prodEnsureDayClosureTable);

function prodNormalizeDayClosureSummary(summary = {}, meta = {}) {
  const out = prodClone(summary || {}) || {};
  const day = String(out.date || meta.date || '').slice(0,10);
  out.date = day;
  out.planId = String(out.planId || meta.planId || '');
  out.persistedAt = String(meta.persistedAt || out.persistedAt || new Date().toISOString());
  out.adminUser = String(meta.adminUser || out.adminUser || 'admin');
  if (!Array.isArray(out.items)) out.items = [];
  out.plannedQty = prodRound(out.plannedQty || 0, 2);
  out.actualQtySap = prodRound(out.actualQtySap || 0, 2);
  out.compliancePct = prodRound(out.compliancePct || 0, 2);
  out.neededQtyContext = prodRound(out.neededQtyContext || 0, 2);
  out.possibleQtyContext = prodRound(out.possibleQtyContext || 0, 2);
  return out;
}
function prodMergeDayClosures(...lists) {
  const byDate = new Map();
  for (const list of lists) {
    for (const raw of (Array.isArray(list) ? list : [])) {
      const item = prodNormalizeDayClosureSummary(raw || {}, {
        adminUser: raw?.adminUser,
        planId: raw?.planId,
        date: raw?.date,
        persistedAt: raw?.persistedAt || raw?.updatedAt || raw?.closedAt || raw?.createdAt,
      });
      const date = String(item?.date || '').slice(0,10);
      if (!date) continue;
      const prev = byDate.get(date);
      const prevTs = String(prev?.persistedAt || prev?.closedAt || prev?.updatedAt || prev?.createdAt || '');
      const nextTs = String(item?.persistedAt || item?.closedAt || item?.updatedAt || item?.createdAt || '');
      if (!prev || nextTs >= prevTs) byDate.set(date, { ...(prev || {}), ...(item || {}), date });
    }
  }
  return Array.from(byDate.values()).sort((a, b) => String(b?.date || '').localeCompare(String(a?.date || '')));
}
async function prodDayClosureUpsert(adminUser, planId, summary) {
  const normalized = prodNormalizeDayClosureSummary(summary, { adminUser, planId, date: summary?.date, persistedAt: new Date().toISOString() });
  const date = String(normalized.date || '').slice(0,10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) throw new Error('Fecha inválida para guardar cierre del día');
  if (hasDb()) {
    await dbQuery(`
      insert into admin_production_day_closures_v1(admin_user, closure_date, plan_id, summary, created_at, updated_at)
      values ($1,$2,$3,$4::jsonb,now(),now())
      on conflict (admin_user, closure_date) do update set plan_id=excluded.plan_id, summary=excluded.summary, updated_at=now()
    `, [adminUser, date, String(planId || ''), JSON.stringify(normalized)]);
    return normalized;
  }
  const store = prodReadDayClosureFile();
  store.records = (store.records || []).filter((x) => !(String(x?.adminUser || '') === String(adminUser || '') && String(x?.date || '').slice(0,10) === date));
  store.records.push({ adminUser, date, planId: String(planId || ''), updatedAt: new Date().toISOString(), summary: normalized });
  prodWriteDayClosureFile(store);
  return normalized;
}
async function prodDayClosureGet(adminUser, date) {
  const day = String(date || '').slice(0,10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) return null;
  if (hasDb()) {
    const q = await dbQuery(`
      select summary, updated_at
      from admin_production_day_closures_v1
      where admin_user=$1 and closure_date=$2
      limit 1
    `, [adminUser, day]);
    const row = q.rows?.[0];
    return row ? prodNormalizeDayClosureSummary(row.summary || {}, { adminUser, date: day, persistedAt: row.updated_at }) : null;
  }
  const row = (prodReadDayClosureFile().records || []).find((x) => String(x?.adminUser || '') === String(adminUser || '') && String(x?.date || '').slice(0,10) === day);
  return row ? prodNormalizeDayClosureSummary(row.summary || {}, { adminUser, date: day, persistedAt: row.updatedAt }) : null;
}
async function prodDayClosureDelete(adminUser, date) {
  const day = String(date || '').slice(0,10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) throw new Error('Fecha inválida para abrir el día');
  if (hasDb()) {
    const q = await dbQuery(`
      delete from admin_production_day_closures_v1
      where admin_user=$1 and closure_date=$2
      returning summary, updated_at
    `, [adminUser, day]);
    const row = q.rows?.[0];
    return row ? prodNormalizeDayClosureSummary(row.summary || {}, { adminUser, date: day, persistedAt: row.updated_at }) : null;
  }
  const store = prodReadDayClosureFile();
  let removed = null;
  store.records = (store.records || []).filter((x) => {
    const hit = String(x?.adminUser || '') === String(adminUser || '') && String(x?.date || '').slice(0,10) === day;
    if (hit && !removed) removed = x;
    return !hit;
  });
  prodWriteDayClosureFile(store);
  return removed ? prodNormalizeDayClosureSummary(removed.summary || {}, { adminUser, date: day, persistedAt: removed.updatedAt }) : null;
}
async function prodDayClosureList(adminUser, limit = 180) {
  const max = Math.max(1, Math.min(365, Number(limit || 180)));
  if (hasDb()) {
    const q = await dbQuery(`
      select summary, updated_at
      from admin_production_day_closures_v1
      where admin_user=$1
      order by closure_date desc
      limit $2
    `, [adminUser, max]);
    return (q.rows || []).map((row) => prodNormalizeDayClosureSummary(row.summary || {}, { adminUser, persistedAt: row.updated_at })).filter((x) => x?.date);
  }
  return (prodReadDayClosureFile().records || [])
    .filter((x) => String(x?.adminUser || '') === String(adminUser || ''))
    .sort((a, b) => String(b?.date || '').localeCompare(String(a?.date || '')))
    .slice(0, max)
    .map((row) => prodNormalizeDayClosureSummary(row.summary || {}, { adminUser, persistedAt: row.updatedAt }))
    .filter((x) => x?.date);
}

function prodSavedPlanUser(req) {

  return String(req?.admin?.user || req?.admin?.sub || 'admin').trim().toLowerCase() || 'admin';
}
function prodSavedPlanId() {
  return `plan_${Date.now()}_${crypto.randomBytes(6).toString('hex')}`;
}
function prodNormalizeSavedPlanPayload(payload, meta = {}) {
  const base = prodClone(payload || {}) || {};
  base.planRecordId = String(base.planRecordId || meta.id || prodSavedPlanId());
  base.persistedAt = String(meta.persistedAt || new Date().toISOString());
  base.persistedBy = String(meta.adminUser || base.persistedBy || 'admin');
  if (!Array.isArray(base.dayClosures)) base.dayClosures = [];
  if (!base._serverMeta || typeof base._serverMeta !== 'object') base._serverMeta = {};
  base._serverMeta.active = true;
  return base;
}
async function prodSavedPlanSetCurrent(adminUser, payload) {
  const normalized = prodNormalizeSavedPlanPayload(payload, { adminUser, persistedAt: new Date().toISOString(), id: payload?.planRecordId });
  const id = String(normalized.planRecordId);
  if (hasDb()) {
    await dbQuery(`update admin_production_saved_plans_v18 set is_active=false, updated_at=now() where admin_user=$1`, [adminUser]);
    await dbQuery(`
      insert into admin_production_saved_plans_v18(id, admin_user, is_active, payload, created_at, updated_at)
      values ($1,$2,true,$3::jsonb,now(),now())
      on conflict (id) do update set admin_user=excluded.admin_user, is_active=true, payload=excluded.payload, updated_at=now()
    `, [id, adminUser, JSON.stringify(normalized)]);
    return normalized;
  }
  const store = prodReadSavedPlanFile();
  store.records = (store.records || []).filter((x) => String(x?.id || '') !== id);
  store.records.push({ id, adminUser, isActive: true, updatedAt: new Date().toISOString(), payload: normalized });
  store.activeByUser[adminUser] = id;
  prodWriteSavedPlanFile(store);
  return normalized;
}
async function prodSavedPlanGetCurrent(adminUser) {
  let payload = null;
  if (hasDb()) {
    const q = await dbQuery(`
      select id, admin_user, payload, created_at, updated_at
      from admin_production_saved_plans_v18
      where admin_user=$1 and is_active=true
      order by updated_at desc
      limit 1
    `, [adminUser]);
    const row = q.rows?.[0];
    if (row) {
      payload = prodNormalizeSavedPlanPayload(row.payload || {}, { adminUser, persistedAt: row.updated_at, id: row.id });
    }
  } else {
    const store = prodReadSavedPlanFile();
    const activeId = String(store.activeByUser?.[adminUser] || '');
    const row = (store.records || []).find((x) => String(x?.id || '') === activeId && String(x?.adminUser || '') === adminUser) || null;
    if (row) payload = prodNormalizeSavedPlanPayload(row.payload || {}, { adminUser, persistedAt: row.updatedAt, id: row.id });
  }

  const history = await prodDayClosureList(adminUser, 180).catch(() => []);
  if (!payload) {
    if (!history.length) return null;
    payload = prodNormalizeSavedPlanPayload({
      ok: true,
      dayClosures: history,
      latestDaySummary: history[0] || null,
      items: [],
      blockedItems: [],
      calendarDays: [],
      summary: {},
      filters: {},
      scenario: {},
      workRule: null,
    }, { adminUser, persistedAt: new Date().toISOString(), id: prodSavedPlanId() });
    return payload;
  }

  payload.dayClosures = prodMergeDayClosures(payload.dayClosures, history);
  if (!payload.latestDaySummary && payload.dayClosures.length) payload.latestDaySummary = payload.dayClosures[0];
  return payload;
}
async function prodSavedPlanUpdateById(adminUser, planId, updater) {
  const current = await prodSavedPlanGetCurrent(adminUser);
  if (!current) return null;
  if (planId && String(current.planRecordId || '') !== String(planId || '')) return null;
  const nextPayload = typeof updater === 'function' ? await updater(prodClone(current)) : prodClone(updater);
  if (!nextPayload) return null;
  nextPayload.planRecordId = current.planRecordId;
  return await prodSavedPlanSetCurrent(adminUser, nextPayload);
}
function prodOrdersActualQtyForDate(orders, date) {
  return prodRound((Array.isArray(orders) ? orders : []).filter((x) => String(x?.postDate || '').slice(0,10) === String(date || '').slice(0,10)).reduce((acc, x) => acc + prodNum(x?.completedQty || 0), 0), 2);
}
async function prodBuildPlanDayCloseSummary(planPayload, date, { forceFresh = true } = {}) {
  const day = String(date || '').slice(0,10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) throw new Error('Fecha inválida para cierre del día');
  const plan = prodClone(planPayload || {}) || {};
  const rows = Array.isArray(plan?.items) ? plan.items : [];
  const rowsForDay = rows.map((row) => {
    const tasks = (Array.isArray(row?.tasks) ? row.tasks : []).filter((task) => String(task?.date || '') === day);
    return { ...row, tasks };
  }).filter((row) => row.tasks.length);
  if (!rowsForDay.length) throw new Error('No hay producción programada para esa fecha');

  const codes = Array.from(new Set(rowsForDay.map((x) => String(x?.itemCode || '').trim()).filter(Boolean)));
  if (forceFresh) {
    await prodRefreshInventoryForCodes(codes, { forceFresh: true, ttlMs: 0, timeoutMs: 120000 }).catch(() => {});
  }

  let plannedQty = 0;
  let actualQtySap = 0;
  let totalNeededContext = 0;
  let totalPossibleContext = 0;
  const items = [];

  for (const row of rowsForDay) {
    const itemCode = String(row?.itemCode || '').trim();
    const plannedDayQty = prodRound((Array.isArray(row?.tasks) ? row.tasks : []).reduce((acc, task) => acc + prodNum(task?.qty || 0), 0), 2);
    const orders = await prodFetchProductionOrders(itemCode, 120, { forceFresh: true, ttlMs: 0 }).catch(() => ({ orders: [] }));
    const actualDayQty = prodOrdersActualQtyForDate(orders?.orders || [], day);
    const fullItem = await prodGetFullItem(itemCode, { forceFresh: true, ttlMs: 0 }).catch(() => null);
    const inv = prodExtractInventorySnapshotFromItem(fullItem);

    plannedQty += plannedDayQty;
    actualQtySap += actualDayQty;
    totalNeededContext += prodNum(row?.neededQty || 0);
    totalPossibleContext += prodNum(row?.possibleQty || 0);

    items.push({
      itemCode,
      itemDesc: String(row?.itemDesc || ''),
      familyLabel: String(row?.familyLabel || 'Producción'),
      plannedDayQty: prodRound(plannedDayQty, 2),
      neededQtyContext: prodRound(row?.neededQty || 0, 2),
      possibleQtyContext: prodRound(row?.possibleQty || 0, 2),
      actualQtySap: prodRound(actualDayQty, 2),
      compliancePct: plannedDayQty > 0 ? prodRound((actualDayQty / plannedDayQty) * 100, 1) : 0,
      stockActualQty: prodRound(inv?.stockTotal || row?.currentStockQty || 0, 2),
      materials: (Array.isArray(row?.materials) ? row.materials : []).slice(0, 8),
    });
  }

  const compliancePct = plannedQty > 0 ? prodRound((actualQtySap / plannedQty) * 100, 1) : 0;
  return {
    date: day,
    closedAt: new Date().toISOString(),
    plannedQty: prodRound(plannedQty, 2),
    neededQtyContext: prodRound(totalNeededContext, 2),
    possibleQtyContext: prodRound(totalPossibleContext, 2),
    actualQtySap: prodRound(actualQtySap, 2),
    compliancePct,
    status: compliancePct >= 98 ? 'ok' : compliancePct >= 85 ? 'warn' : 'bad',
    message: `Producción concluida para ${day}. Planificadas ${prodRound(plannedQty,2)} u, SAP registró ${prodRound(actualQtySap,2)} u. Cumplimiento: ${compliancePct}%.`,
    items,
  };
}

async function productionBuildGanttPlanV18({ from, to, area, grupo, sizeUom = '__ALL__', abc = '__ALL__', typeFilter = 'Se fabrica en planta', q = '', avgMonths = 0, horizonMonths = 1, shiftHours = 8, startDate = '', fullMaterials = false }) {
  return prodBuildLaneAwareGanttPlan({ from, to, area, grupo, sizeUom, abc, typeFilter, q, avgMonths, horizonMonths, shiftHours, startDate, fullMaterials });
}

app.get('/api/admin/production/gantt-plan-v18', verifyAdmin, async (req, res) => {
  try {
    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = String(req.query?.from || '2025-01-01');
    const to = String(req.query?.to || today);
    const area = String(req.query?.area || '__ALL__');
    const grupo = String(req.query?.grupo || '__ALL__');
    const sizeUom = String(req.query?.sizeUom || req.query?.size || '__ALL__');
    const abc = String(req.query?.abc || '__ALL__');
    const typeFilter = String(req.query?.type || req.query?.tipo || 'Se fabrica en planta');
    const q = String(req.query?.q || '');
    const horizonMonths = Math.max(1, Math.min(12, prodNum(req.query?.horizonMonths, 1)));
    const avgMonths = Math.max(1, Math.min(12, prodNum(req.query?.avgMonths, horizonMonths)));
    const shiftHours = Math.max(1, Math.min(24, prodNum(req.query?.shiftHours, 8)));
    const startDate = String(req.query?.startDate || today).slice(0,10);
    const fullMaterials = ['1','true','yes','si'].includes(String(req.query?.fullMaterials || req.query?.incomingMp || '').trim().toLowerCase());
    const out = await productionBuildGanttPlanV18({ from, to, area, grupo, sizeUom, abc, typeFilter, q, avgMonths, horizonMonths, shiftHours, startDate, fullMaterials });
    out.scenario = { ...(out.scenario || {}), fullMaterials };
    const saved = await prodSavedPlanSetCurrent(prodSavedPlanUser(req), out);
    return safeJson(res, 200, saved);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});


app.post('/api/admin/production/plans/current/save', verifyAdmin, async (req, res) => {
  try {
    const payload = req.body?.payload || req.body || {};
    if (!payload || typeof payload !== 'object') return safeJson(res, 400, { ok:false, message:'Payload inválido' });
    const saved = await prodSavedPlanSetCurrent(prodSavedPlanUser(req), payload);
    return safeJson(res, 200, saved);
  } catch (e) {
    return safeJson(res, 500, { ok:false, message: e.message || String(e) });
  }
});

app.get('/api/admin/production/plans/current', verifyAdmin, async (req, res) => {
  try {
    const payload = await prodSavedPlanGetCurrent(prodSavedPlanUser(req));
    if (!payload) return safeJson(res, 404, { ok: false, message: 'No hay un plan guardado todavía' });
    return safeJson(res, 200, payload);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.post('/api/admin/production/plans/day-close', verifyAdmin, async (req, res) => {
  try {
    const adminUser = prodSavedPlanUser(req);
    const planId = String(req.body?.planId || '').trim();
    const date = String(req.body?.date || req.query?.date || '').slice(0,10);
    const payload = await prodSavedPlanGetCurrent(adminUser);
    if (!payload) return safeJson(res, 404, { ok: false, message: 'No hay plan guardado' });
    if (planId && String(payload.planRecordId || '') !== planId) return safeJson(res, 409, { ok: false, message: 'El plan cambió; vuelve a cargar el plan actual' });
    const summary = await prodBuildPlanDayCloseSummary(payload, date, { forceFresh: true });
    await prodDayClosureUpsert(adminUser, payload.planRecordId, summary).catch(() => {});
    const history = await prodDayClosureList(adminUser, 180).catch(() => [summary]);
    const updated = await prodSavedPlanUpdateById(adminUser, payload.planRecordId, (plan) => {
      plan.dayClosures = prodMergeDayClosures([summary], plan?.dayClosures, history);
      plan.lastSapSyncAt = new Date().toISOString();
      plan.latestDaySummary = summary;
      return plan;
    });
    return safeJson(res, 200, { ok: true, summary, plan: updated || payload });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get('/api/admin/production/plans/day-close', verifyAdmin, async (req, res) => {
  try {
    const adminUser = prodSavedPlanUser(req);
    const date = String(req.query?.date || '').slice(0, 10);
    if (date) {
      const hit = await prodDayClosureGet(adminUser, date);
      if (!hit) return safeJson(res, 404, { ok: false, message: 'No existe cierre guardado para esa fecha' });
      return safeJson(res, 200, { ok: true, summary: hit });
    }
    const items = await prodDayClosureList(adminUser, 180);
    return safeJson(res, 200, { ok: true, items });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.post('/api/admin/production/plans/day-open', verifyAdmin, async (req, res) => {
  try {
    const adminUser = prodSavedPlanUser(req);
    const planId = String(req.body?.planId || '').trim();
    const date = String(req.body?.date || req.query?.date || '').slice(0,10);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return safeJson(res, 400, { ok: false, message: 'Fecha inválida' });
    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    if (date !== today) return safeJson(res, 400, { ok: false, message: 'Solo se puede abrir nuevamente el día de hoy' });
    const payload = await prodSavedPlanGetCurrent(adminUser);
    if (!payload) return safeJson(res, 404, { ok: false, message: 'No hay plan guardado' });
    if (planId && String(payload.planRecordId || '') !== planId) return safeJson(res, 409, { ok: false, message: 'El plan cambió; vuelve a cargar el plan actual' });
    await prodDayClosureDelete(adminUser, date).catch(() => null);
    const history = await prodDayClosureList(adminUser, 180).catch(() => []);
    const updated = await prodSavedPlanUpdateById(adminUser, payload.planRecordId, (plan) => {
      const nextClosures = prodMergeDayClosures((Array.isArray(plan?.dayClosures) ? plan.dayClosures : []).filter((x) => String(x?.date || '').slice(0,10) !== date), history);
      plan.dayClosures = nextClosures;
      plan.latestDaySummary = nextClosures[0] || null;
      plan.lastSapSyncAt = new Date().toISOString();
      return plan;
    });
    return safeJson(res, 200, { ok: true, reopened: true, date, plan: updated || payload });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});


/* =========================================================
   Compras — proveedor fijo por artículo + saldo proveedor
========================================================= */
async function comprasEnsureDb() {
  if (!hasDb()) return;
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS admin_compras_supplier_cache (
      item_code TEXT PRIMARY KEY,
      item_name TEXT DEFAULT '',
      supplier_code TEXT DEFAULT '',
      supplier_name TEXT DEFAULT '',
      supplier_source TEXT DEFAULT '',
      supplier_hint TEXT DEFAULT '',
      payment_status TEXT DEFAULT 'SIN_DATOS',
      amount_due NUMERIC(19,6) DEFAULT 0,
      overdue_amount NUMERIC(19,6) DEFAULT 0,
      balance_raw NUMERIC(19,6) DEFAULT 0,
      balance_source TEXT DEFAULT '',
      payment_terms_name TEXT DEFAULT '',
      credit_days INT DEFAULT 0,
      debt_note TEXT DEFAULT '',
      item_json JSONB DEFAULT '{}'::jsonb,
      supplier_json JSONB DEFAULT '{}'::jsonb,
      result_json JSONB DEFAULT '{}'::jsonb,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_admin_compras_supplier_cache_updated_at ON admin_compras_supplier_cache(updated_at DESC)`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_admin_compras_supplier_cache_supplier_code ON admin_compras_supplier_cache(supplier_code)`);
}
__extraBootTasks.push(comprasEnsureDb);

function comprasNum(v, d = 2) {
  const n = Number(v || 0);
  return Number.isFinite(n) ? Number(n.toFixed(d)) : 0;
}
function comprasStr(v) {
  return String(v || '').trim();
}
function comprasToArray(raw) {
  if (Array.isArray(raw)) return raw;
  if (Array.isArray(raw?.value)) return raw.value;
  if (raw && typeof raw === 'object') return [raw];
  return [];
}
function comprasNormalizeDateTime(value) {
  try {
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return '';
    return d.toISOString();
  } catch {
    return '';
  }
}
function comprasResponseFromRow(row, source = 'db') {
  if (!row) return null;
  const resultJson = row.result_json && typeof row.result_json === 'object' ? row.result_json : {};
  const baseResult = resultJson && typeof resultJson === 'object' ? resultJson : {};
  const suppliers = comprasDedupResolvedSuppliers(Array.isArray(baseResult.suppliers) ? baseResult.suppliers : []);
  const aggregate = comprasAggregateSupplierStatuses(suppliers);
  const primary = suppliers[0] || {
    cardCode: comprasStr(row.supplier_code),
    cardName: comprasStr(row.supplier_name),
    source: comprasStr(row.supplier_source),
    balance: {
      paymentStatus: comprasStr(row.payment_status || 'SIN_DATOS'),
      amountDue: comprasNum(row.amount_due || 0),
      overdueAmount: comprasNum(row.overdue_amount || 0),
      balanceRaw: comprasNum(row.balance_raw || 0),
      balanceSource: comprasStr(row.balance_source),
      paymentTermsName: comprasStr(row.payment_terms_name),
      creditDays: Number(row.credit_days || 0),
      debtNote: comprasStr(row.debt_note),
    },
  };
  const hasMany = suppliers.length > 1;
  return {
    itemCode: comprasStr(row.item_code),
    itemName: comprasStr(row.item_name),
    supplier: {
      cardCode: hasMany ? suppliers.map((x) => comprasStr(x?.cardCode)).filter(Boolean).join(', ') : comprasStr(primary?.cardCode || row.supplier_code),
      cardName: hasMany ? suppliers.map((x) => comprasStr(x?.cardName)).filter(Boolean).join(' · ') : comprasStr(primary?.cardName || row.supplier_name),
      source: comprasStr(primary?.source || row.supplier_source),
      hint: comprasStr(row.supplier_hint),
    },
    suppliers,
    balance: {
      paymentStatus: hasMany ? comprasStr(aggregate.paymentStatus) : comprasStr(primary?.balance?.paymentStatus || row.payment_status || 'SIN_DATOS'),
      amountDue: hasMany ? comprasNum(aggregate.totalAmountDue || 0) : comprasNum(primary?.balance?.amountDue || row.amount_due || 0),
      overdueAmount: hasMany ? comprasNum(aggregate.totalOverdue || 0) : comprasNum(primary?.balance?.overdueAmount || row.overdue_amount || 0),
      balanceRaw: hasMany ? comprasNum(aggregate.totalAmountDue || 0) : comprasNum(primary?.balance?.balanceRaw || row.balance_raw || 0),
      balanceSource: hasMany ? comprasStr(aggregate.balanceSource) : comprasStr(primary?.balance?.balanceSource || row.balance_source),
      paymentTermsName: hasMany ? comprasStr(aggregate.paymentTermsName) : comprasStr(primary?.balance?.paymentTermsName || row.payment_terms_name),
      creditDays: hasMany ? Number(aggregate.creditDays || 0) : Number(primary?.balance?.creditDays || row.credit_days || 0),
      debtNote: hasMany ? comprasStr(aggregate.debtNote) : comprasStr(primary?.balance?.debtNote || row.debt_note),
    },
    updatedAt: comprasNormalizeDateTime(row.updated_at),
    source,
    cacheHit: source === 'db',
    raw: {
      item: row.item_json || baseResult?.raw?.item || null,
      supplier: row.supplier_json || baseResult?.raw?.supplier || null,
      itemRaw: baseResult?.raw?.itemRaw || null,
      supplierCandidates: Array.isArray(baseResult?.raw?.supplierCandidates) ? baseResult.raw.supplierCandidates : [],
    },
  };
}

async function comprasReadCache(itemCode) {
  if (!hasDb()) return null;
  const code = comprasStr(itemCode);
  if (!code) return null;
  const q = await dbQuery(`
    SELECT item_code, item_name, supplier_code, supplier_name, supplier_source, supplier_hint,
           payment_status, amount_due, overdue_amount, balance_raw, balance_source,
           payment_terms_name, credit_days, debt_note,
           item_json, supplier_json, result_json, updated_at
    FROM admin_compras_supplier_cache
    WHERE item_code = $1
    LIMIT 1
  `, [code]);
  return q.rows?.[0] || null;
}

async function comprasUpsertCache(result) {
  if (!hasDb() || !result?.itemCode) return null;
  await dbQuery(`
    INSERT INTO admin_compras_supplier_cache (
      item_code, item_name, supplier_code, supplier_name, supplier_source, supplier_hint,
      payment_status, amount_due, overdue_amount, balance_raw, balance_source,
      payment_terms_name, credit_days, debt_note,
      item_json, supplier_json, result_json, updated_at
    ) VALUES (
      $1,$2,$3,$4,$5,$6,
      $7,$8,$9,$10,$11,
      $12,$13,$14,
      $15::jsonb,$16::jsonb,$17::jsonb,NOW()
    )
    ON CONFLICT (item_code) DO UPDATE SET
      item_name = EXCLUDED.item_name,
      supplier_code = EXCLUDED.supplier_code,
      supplier_name = EXCLUDED.supplier_name,
      supplier_source = EXCLUDED.supplier_source,
      supplier_hint = EXCLUDED.supplier_hint,
      payment_status = EXCLUDED.payment_status,
      amount_due = EXCLUDED.amount_due,
      overdue_amount = EXCLUDED.overdue_amount,
      balance_raw = EXCLUDED.balance_raw,
      balance_source = EXCLUDED.balance_source,
      payment_terms_name = EXCLUDED.payment_terms_name,
      credit_days = EXCLUDED.credit_days,
      debt_note = EXCLUDED.debt_note,
      item_json = EXCLUDED.item_json,
      supplier_json = EXCLUDED.supplier_json,
      result_json = EXCLUDED.result_json,
      updated_at = NOW()
  `, [
    comprasStr(result.itemCode),
    comprasStr(result.itemName),
    comprasStr(result.supplier?.cardCode),
    comprasStr(result.supplier?.cardName),
    comprasStr(result.supplier?.source),
    comprasStr(result.supplier?.hint),
    comprasStr(result.balance?.paymentStatus || 'SIN_DATOS'),
    comprasNum(result.balance?.amountDue || 0, 6),
    comprasNum(result.balance?.overdueAmount || 0, 6),
    comprasNum(result.balance?.balanceRaw || 0, 6),
    comprasStr(result.balance?.balanceSource),
    comprasStr(result.balance?.paymentTermsName),
    Number(result.balance?.creditDays || 0),
    comprasStr(result.balance?.debtNote),
    JSON.stringify(result.raw?.item || {}),
    JSON.stringify(result.raw?.supplier || {}),
    JSON.stringify(result || {}),
  ]);
  return comprasReadCache(result.itemCode);
}

async function comprasFindSupplierByHint(hint, { itemName = '', itemCode = '' } = {}) {
  const q = comprasStr(hint);
  if (!q) return null;
  if (!comprasLooksLikeSupplierName(q, { itemName, itemCode })) return null;
  const safe = q.replace(/'/g, "''");
  const paths = [
    `/BusinessPartners?$select=CardCode,CardName,CardType,Balance,CurrentAccountBalance,DebitBalance,PayTermsGrpCode,GroupNum&$filter=CardCode eq '${safe}'&$top=1`,
    `/BusinessPartners?$select=CardCode,CardName,CardType,Balance,CurrentAccountBalance,DebitBalance,PayTermsGrpCode,GroupNum&$filter=CardName eq '${safe}'&$top=1`,
    `/BusinessPartners?$select=CardCode,CardName,CardType,Balance,CurrentAccountBalance,DebitBalance,PayTermsGrpCode,GroupNum&$filter=contains(CardName,'${safe}')&$top=5`,
    `/BusinessPartners?$select=CardCode,CardName,CardType,Balance,CurrentAccountBalance,DebitBalance,PayTermsGrpCode,GroupNum&$filter=substringof('${safe}',CardName)&$top=5`,
  ];
  for (const path of paths) {
    try {
      const raw = await slFetchFreshSession(path);
      const rows = comprasToArray(raw);
      const exact = rows.find((row) => comprasSameText(row?.CardName, q) || comprasSameText(row?.CardCode, q));
      const best = exact || rows.find((row) => comprasLooksLikeSupplierCode(row?.CardCode, { itemCode })) || rows[0] || null;
      if (best?.CardCode) return best;
    } catch {}
  }
  return null;
}

function comprasNormalizeText(v) {
  return String(v || '')
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, ' ')
    .trim();
}

function comprasSameText(a, b) {
  return comprasNormalizeText(a) && comprasNormalizeText(a) === comprasNormalizeText(b);
}

function comprasLooksLikeSupplierCode(v, { itemCode = '' } = {}) {
  const s = comprasStr(v);
  if (!s) return false;
  if (s.length > 60) return false;
  if (comprasSameText(s, itemCode)) return false;
  if (/\s{2,}/.test(s)) return false;
  return true;
}

function comprasLooksLikeSupplierName(v, { itemName = '', itemCode = '' } = {}) {
  const s = comprasStr(v);
  if (!s) return false;
  if (s.length < 2 || s.length > 120) return false;
  if (comprasSameText(s, itemName) || comprasSameText(s, itemCode)) return false;
  return true;
}

function comprasPushSupplierCandidate(out, seen, cand, ctx = {}) {
  const cardCode = comprasStr(cand?.cardCode);
  const cardName = comprasStr(cand?.cardName);
  if (!comprasLooksLikeSupplierCode(cardCode, ctx) && !comprasLooksLikeSupplierName(cardName, ctx)) return;
  const key = `${comprasNormalizeText(cardCode)}|${comprasNormalizeText(cardName)}|${comprasStr(cand?.source)}`;
  if (seen.has(key)) return;
  seen.add(key);
  out.push({
    cardCode: comprasLooksLikeSupplierCode(cardCode, ctx) ? cardCode : '',
    cardName: comprasLooksLikeSupplierName(cardName, ctx) ? cardName : '',
    source: comprasStr(cand?.source),
    raw: cand?.raw || null,
  });
}

function comprasExtractSupplierCandidatesFromObject(node, ctx = {}, out = [], seen = new Set(), walk = new WeakSet(), path = 'item') {
  if (!node || typeof node !== 'object') return out;
  if (walk.has(node)) return out;
  walk.add(node);

  const codeKeyRe = /(mainsupplier|preferredvendor|defaultsupplier|defaultvendor|vendorcode|suppliercode|bpcode|cardcode|supplier|vendor)/i;
  const nameKeyRe = /(suppliername|vendorname|bpname|cardname|mainsuppliername|preferredvendorname|defaultsuppliername|defaultvendorname)/i;

  if (Array.isArray(node)) {
    for (let i = 0; i < node.length && i < 25; i++) {
      comprasExtractSupplierCandidatesFromObject(node[i], ctx, out, seen, walk, `${path}[${i}]`);
    }
    return out;
  }

  let localCode = '';
  let localName = '';
  for (const [k, v] of Object.entries(node)) {
    if (v == null) continue;
    if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') {
      const sv = comprasStr(v);
      if (!sv) continue;
      if (!localCode && codeKeyRe.test(k) && comprasLooksLikeSupplierCode(sv, ctx)) localCode = sv;
      if (!localName && nameKeyRe.test(k) && comprasLooksLikeSupplierName(sv, ctx)) localName = sv;
    }
  }

  if (localCode || localName) {
    comprasPushSupplierCandidate(out, seen, { cardCode: localCode, cardName: localName, source: `SAP ${path}` }, ctx);
  }

  for (const [k, v] of Object.entries(node)) {
    if (!v || typeof v !== 'object') continue;
    comprasExtractSupplierCandidatesFromObject(v, ctx, out, seen, walk, `${path}.${k}`);
  }
  return out;
}

function comprasRankSupplierCandidate(cand, ctx = {}) {
  let score = 0;
  const src = comprasStr(cand?.source).toLowerCase();
  if (cand?.cardCode) score += 120;
  if (cand?.cardName) score += 25;
  if (src.includes('mainsupplier')) score += 80;
  if (src.includes('preferredvendor')) score += 70;
  if (src.includes('defaultsupplier') || src.includes('defaultvendor')) score += 65;
  if (src.includes('businesspartners')) score += 45;
  if (src.includes('historial')) score += 40;
  if (src.includes('purchaseorders')) score += 30;
  if (src.includes('purchaseinvoices')) score += 25;
  if (cand?.cardName && !comprasLooksLikeSupplierName(cand.cardName, ctx)) score -= 80;
  if (cand?.cardCode && !comprasLooksLikeSupplierCode(cand.cardCode, ctx)) score -= 80;
  return score;
}

function comprasChooseBestSupplierCandidate(candidates, ctx = {}) {
  const arr = Array.isArray(candidates) ? candidates.slice() : [];
  arr.sort((a, b) => comprasRankSupplierCandidate(b, ctx) - comprasRankSupplierCandidate(a, ctx) || comprasStr(a?.source).localeCompare(comprasStr(b?.source)));
  return arr[0] || null;
}

function comprasBuildSupplierList(candidates, ctx = {}) {
  const map = new Map();
  for (const cand of Array.isArray(candidates) ? candidates : []) {
    const cardCode = comprasStr(cand?.cardCode);
    const cardName = comprasStr(cand?.cardName);
    if (!cardCode && !cardName) continue;
    const key = cardCode ? `C:${comprasNormalizeText(cardCode)}` : `N:${comprasNormalizeText(cardName)}`;
    const prev = map.get(key);
    if (!prev) {
      map.set(key, {
        cardCode,
        cardName,
        source: comprasStr(cand?.source),
        raw: cand?.raw || null,
      });
      continue;
    }
    if (!prev.cardCode && cardCode) prev.cardCode = cardCode;
    if ((!prev.cardName || comprasSameText(prev.cardName, prev.cardCode)) && cardName) prev.cardName = cardName;
    if (!prev.source && cand?.source) prev.source = comprasStr(cand?.source);
    if (!prev.raw && cand?.raw) prev.raw = cand.raw;
  }
  return Array.from(map.values());
}

function comprasDedupResolvedSuppliers(list = []) {
  const map = new Map();
  for (const row of Array.isArray(list) ? list : []) {
    const cardCode = comprasStr(row?.cardCode);
    const cardName = comprasStr(row?.cardName);
    if (!cardCode && !cardName) continue;
    const key = cardCode ? `C:${comprasNormalizeText(cardCode)}` : `N:${comprasNormalizeText(cardName)}`;
    const prev = map.get(key);
    if (!prev) {
      map.set(key, {
        ...row,
        cardCode,
        cardName,
        source: comprasStr(row?.source),
        balance: { ...(row?.balance || {}) },
      });
      continue;
    }
    if (!prev.cardCode && cardCode) prev.cardCode = cardCode;
    if ((!prev.cardName || comprasSameText(prev.cardName, prev.cardCode)) && cardName) prev.cardName = cardName;
    if (!prev.source && row?.source) prev.source = comprasStr(row?.source);
    const prevAmt = comprasNum(prev?.balance?.amountDue || prev?.balance?.balanceRaw || 0);
    const nextAmt = comprasNum(row?.balance?.amountDue || row?.balance?.balanceRaw || 0);
    if (nextAmt > prevAmt + 0.0001) prev.balance = { ...(row?.balance || {}) };
    if (!prev.raw && row?.raw) prev.raw = row.raw;
  }
  return Array.from(map.values());
}

function comprasExtractBusinessPartnerName(node, fallback = '') {
  const fb = comprasStr(fallback);
  if (!node) return fb;

  const direct = [
    node?.CardName,
    node?.cardName,
    node?.BPName,
    node?.bpName,
    node?.SupplierName,
    node?.supplierName,
    node?.VendorName,
    node?.vendorName,
    node?.Name,
    node?.name,
    node?.ForeignName,
    node?.foreignName,
    node?.AliasName,
    node?.aliasName,
    node?.CompanyName,
    node?.companyName,
    node?.CardFName,
    node?.cardFName,
  ].map((x) => comprasStr(x)).find(Boolean);
  if (direct) return direct;

  const queue = [];
  const seen = new WeakSet();
  if (node && typeof node === 'object') queue.push(node);

  while (queue.length) {
    const cur = queue.shift();
    if (!cur || typeof cur !== 'object') continue;
    if (seen.has(cur)) continue;
    seen.add(cur);

    const nestedDirect = [
      cur?.CardName,
      cur?.cardName,
      cur?.BPName,
      cur?.bpName,
      cur?.SupplierName,
      cur?.supplierName,
      cur?.VendorName,
      cur?.vendorName,
      cur?.Name,
      cur?.name,
      cur?.ForeignName,
      cur?.foreignName,
      cur?.AliasName,
      cur?.aliasName,
      cur?.CompanyName,
      cur?.companyName,
      cur?.CardFName,
      cur?.cardFName,
    ].map((x) => comprasStr(x)).find(Boolean);
    if (nestedDirect) return nestedDirect;

    if (Array.isArray(cur)) {
      for (const v of cur.slice(0, 20)) {
        if (v && typeof v === 'object') queue.push(v);
      }
      continue;
    }

    for (const v of Object.values(cur)) {
      if (v && typeof v === 'object') queue.push(v);
    }
  }

  return fb;
}

async function comprasFetchBusinessPartnerNameFromDocs(cardCode, fallbackName = '') {
  const code = comprasStr(cardCode);
  const fallback = comprasStr(fallbackName);
  if (!code) return { cardCode: code, cardName: fallback, raw: null, source: '' };
  const safe = prodSapEscapeLiteral(code);
  const paths = [
    { path: `/BusinessPartners('${encodeURIComponent(code)}')`, source: 'BusinessPartners.byKey' },
    { path: `/BusinessPartners?$filter=CardCode eq '${safe}'&$top=1`, source: 'BusinessPartners.eq' },
    { path: `/PurchaseInvoices?$select=CardCode,CardName,DocDate&$filter=CardCode eq '${safe}'&$orderby=DocDate desc&$top=3`, source: 'PurchaseInvoices' },
    { path: `/PurchaseOrders?$select=CardCode,CardName,DocDate&$filter=CardCode eq '${safe}'&$orderby=DocDate desc&$top=3`, source: 'PurchaseOrders' },
  ];

  for (const cfg of paths) {
    try {
      const raw = await slFetchFreshSession(cfg.path);
      const rows = comprasToArray(raw);
      const row = rows.find((x) => comprasSameText(x?.CardCode, code)) || rows[0] || (raw && typeof raw === 'object' ? raw : null);
      const cardName = comprasExtractBusinessPartnerName(row, comprasExtractBusinessPartnerName(raw, fallback));
      if (cardName) {
        return { cardCode: comprasStr(row?.CardCode || code), cardName, raw: row || raw || null, source: cfg.source };
      }
    } catch {}
  }

  return { cardCode: code, cardName: fallback, raw: null, source: '' };
}

async function comprasFetchBusinessPartnerSummary(cardCode, fallbackName = '') {
  const code = comprasStr(cardCode);
  const fallback = comprasStr(fallbackName);
  if (!code) return { cardCode: code, cardName: fallback, raw: null };
  const safe = code.replace(/'/g, "''");
  const select = 'CardCode,CardName,CardType,ForeignName,AliasName,CardFName,Balance,CurrentAccountBalance,DebitBalance,PayTermsGrpCode,GroupNum';
  const paths = [
    `/BusinessPartners?$select=${select}&$filter=CardCode eq '${safe}'&$top=1`,
    `/BusinessPartners('${encodeURIComponent(code)}')?$select=${select}`,
    `/BusinessPartners('${encodeURIComponent(code)}')`,
    `/BusinessPartners?$select=${select}&$filter=contains(CardCode,'${safe}')&$top=5`,
  ];
  for (const path of paths) {
    try {
      const raw = await slFetchFreshSession(path);
      const rows = Array.isArray(raw) ? raw : comprasToArray(raw);
      const row = rows.find((x) => comprasSameText(x?.CardCode, code)) || rows[0] || (raw && typeof raw === 'object' ? raw : null);
      const lookedUpName = comprasExtractBusinessPartnerName(row, comprasExtractBusinessPartnerName(raw, fallback));
      if ((row?.CardCode || row?.CardName || row?.ForeignName || row?.AliasName || row?.Name || row?.CardFName) && lookedUpName) {
        return {
          cardCode: comprasStr(row?.CardCode || code),
          cardName: lookedUpName,
          raw: row || raw || null,
        };
      }
      if (row?.CardCode || lookedUpName) {
        return {
          cardCode: comprasStr(row?.CardCode || code),
          cardName: lookedUpName || fallback,
          raw: row || raw || null,
        };
      }
    } catch {}
  }
  const docLookup = await comprasFetchBusinessPartnerNameFromDocs(code, fallback).catch(() => null);
  if (docLookup?.cardName || docLookup?.raw) {
    return {
      cardCode: comprasStr(docLookup?.cardCode || code),
      cardName: comprasStr(docLookup?.cardName || fallback),
      raw: docLookup?.raw || null,
    };
  }
  return { cardCode: code, cardName: fallback, raw: null };
}

function comprasAggregateSupplierStatuses(suppliers = []) {
  const list = Array.isArray(suppliers) ? suppliers : [];
  const totalAmountDue = prodRound(list.reduce((s, x) => s + comprasNum(x?.balance?.amountDue || 0), 0), 2);
  const totalOverdue = prodRound(list.reduce((s, x) => s + comprasNum(x?.balance?.overdueAmount || 0), 0), 2);
  const anyDebt = list.some((x) => comprasNum(x?.balance?.amountDue || 0) > 0.009 || comprasStr(x?.balance?.paymentStatus) === 'SE_DEBE');
  const anyUnknown = list.some((x) => !comprasStr(x?.balance?.paymentStatus) || comprasStr(x?.balance?.paymentStatus) === 'SIN_DATOS');
  const allClear = list.length > 0 && list.every((x) => comprasNum(x?.balance?.amountDue || 0) <= 0.009 && comprasStr(x?.balance?.paymentStatus) === 'PAZ_Y_SALVO');
  const sources = Array.from(new Set(list.map((x) => comprasStr(x?.balance?.balanceSource || x?.balance?.source)).filter(Boolean)));
  return {
    totalAmountDue,
    totalOverdue,
    paymentStatus: anyDebt ? 'SE_DEBE' : (allClear ? 'PAZ_Y_SALVO' : (anyUnknown ? 'SIN_DATOS' : 'SIN_DATOS')),
    balanceSource: list.length > 1 ? `Suma ${list.length} proveedores` : (sources[0] || ''),
    paymentTermsName: list.length > 1 ? `${list.length} proveedores` : comprasStr(list[0]?.balance?.paymentTermsName),
    creditDays: list.length > 1 ? 0 : Number(list[0]?.balance?.creditDays || 0),
    debtNote: list.length > 1
      ? `Se detectaron ${list.length} proveedores predefinidos en SAP. El saldo mostrado arriba es la suma de sus estados de cuenta.`
      : comprasStr(list[0]?.balance?.debtNote),
  };
}

async function comprasResolveSupplierFromHistory(itemCode, itemName = '') {
  const code = comprasStr(itemCode);
  if (!code) return null;
  const ctx = { itemCode: code, itemName };
  const [poRows, invRows] = await Promise.all([
    prodFetchRecentPurchaseOrdersForItem(code, itemName, 8).catch(() => []),
    prodFetchRecentPurchaseInvoicesForItem(code, itemName, 8).catch(() => []),
  ]);
  const candidates = [];
  const seen = new Set();
  for (const row of [...comprasToArray(poRows), ...comprasToArray(invRows)]) {
    comprasPushSupplierCandidate(candidates, seen, {
      cardCode: row?.supplierCode,
      cardName: row?.supplierName,
      source: `Historial ${comprasStr(row?.sourceDocType || '').toUpperCase() || 'compras'}`,
      raw: row,
    }, ctx);
  }
  return comprasChooseBestSupplierCandidate(candidates, ctx);
}

async function comprasFetchRawItemForSupplier(itemCode) {
  const code = comprasStr(itemCode);
  if (!code) return null;
  try {
    return await slFetchFreshSession(`/Items('${code.replace(/'/g, "''")}')`);
  } catch {
    return null;
  }
}

async function comprasResolveSupplierCandidate({ itemCode, itemName, item, hint }) {
  const ctx = { itemCode, itemName };
  const candidates = [];
  const seen = new Set();

  comprasPushSupplierCandidate(candidates, seen, {
    cardCode: item?.MainSupplier,
    cardName: '',
    source: 'Item.MainSupplier',
    raw: item,
  }, ctx);
  comprasPushSupplierCandidate(candidates, seen, {
    cardCode: item?.PreferredVendor,
    cardName: '',
    source: 'Item.PreferredVendor',
    raw: item,
  }, ctx);

  comprasExtractSupplierCandidatesFromObject(item, ctx, candidates, seen);

  let rawItem = null;
  if (!candidates.some((x) => x.cardCode)) {
    rawItem = await comprasFetchRawItemForSupplier(itemCode).catch(() => null);
    if (rawItem) comprasExtractSupplierCandidatesFromObject(rawItem, ctx, candidates, seen, new WeakSet(), 'itemRaw');
  }

  if (!candidates.some((x) => x.cardCode) && hint) {
    const bp = await comprasFindSupplierByHint(hint, ctx).catch(() => null);
    if (bp?.CardCode) {
      comprasPushSupplierCandidate(candidates, seen, {
        cardCode: bp.CardCode,
        cardName: bp.CardName,
        source: 'BusinessPartners por nombre',
        raw: bp,
      }, ctx);
    }
  }

  if (!candidates.some((x) => x.cardCode)) {
    const hist = await comprasResolveSupplierFromHistory(itemCode, itemName).catch(() => null);
    if (hist) comprasPushSupplierCandidate(candidates, seen, hist, ctx);
  }

  const chosen = comprasChooseBestSupplierCandidate(candidates, ctx);
  return {
    chosen,
    candidates,
    rawItem,
  };
}

async function comprasFetchLive(itemCode) {
  const code = comprasStr(itemCode);
  if (!code) throw new Error('ItemCode requerido');
  if (missingSapEnv()) throw new Error('Faltan variables SAP');

  const item = await prodFetchFullItemFromSap(code, { timeoutMs: 45000 });
  if (!item || !comprasStr(item?.ItemCode)) throw new Error(`No se encontró el artículo ${code} en SAP`);

  const itemName = comprasStr(item?.ItemName);
  const rawHint = comprasStr(typeof prodExtractSupplierFromItem === 'function' ? prodExtractSupplierFromItem(item) : '');
  const hint = comprasLooksLikeSupplierName(rawHint, { itemName, itemCode: code }) ? rawHint : '';

  const supplierResolved = await comprasResolveSupplierCandidate({ itemCode: code, itemName, item, hint });
  const chosen = supplierResolved?.chosen || null;
  const supplierList = comprasBuildSupplierList(supplierResolved?.candidates || [], { itemCode: code, itemName });

  if (!supplierList.length && chosen) {
    supplierList.push({
      cardCode: comprasStr(chosen?.cardCode),
      cardName: comprasStr(chosen?.cardName),
      source: comprasStr(chosen?.source),
      raw: chosen?.raw || null,
    });
  }

  const suppliers = await Promise.all(supplierList.map(async (cand) => {
    const supplierCode = comprasStr(cand?.cardCode);
    const supplierName = comprasStr(cand?.cardName);
    let balanceInfo = {
      supplierCode,
      supplierName,
      paymentStatus: supplierCode ? 'SIN_DATOS' : 'SIN_PROVEEDOR',
      amountDue: 0,
      overdueAmount: 0,
      source: '',
      paymentTermsName: '',
      creditDays: 0,
      balanceRaw: 0,
      debtNote: supplierCode ? '' : 'Proveedor sin CardCode identificable en SAP.',
      rawBusinessPartner: cand?.raw || null,
    };

    if (supplierCode) {
      balanceInfo = await prodFetchVendorPayablesStatus(supplierCode, supplierName || '').catch(async () => {
        const credit = await prodFetchVendorCreditTerms(supplierCode, supplierName || '').catch(() => null);
        return {
          supplierCode,
          supplierName: comprasStr(credit?.supplierName || supplierName),
          paymentStatus: 'SIN_DATOS',
          amountDue: 0,
          overdueAmount: 0,
          source: credit?.balanceSource ? `BusinessPartners.${credit.balanceSource}` : 'BusinessPartners',
          paymentTermsName: comprasStr(credit?.paymentTermsName),
          creditDays: Number(credit?.creditDays || 0),
          balanceRaw: comprasNum(credit?.balanceRaw || 0),
          debtNote: '',
          rawBusinessPartner: credit?.rawBusinessPartner || null,
        };
      });
    }

    let finalCardName = comprasExtractBusinessPartnerName(balanceInfo?.rawBusinessPartner, balanceInfo?.supplierName || supplierName);
    let bpRaw = balanceInfo?.rawBusinessPartner || cand?.raw || null;
    if (supplierCode) {
      const bp = await comprasFetchBusinessPartnerSummary(supplierCode, finalCardName || supplierName).catch(() => null);
      const lookedUpName = comprasExtractBusinessPartnerName(bp?.raw, bp?.cardName || finalCardName || supplierName);
      if (lookedUpName) finalCardName = comprasStr(lookedUpName);
      if (bp?.raw) bpRaw = bp.raw;

      if (!finalCardName || comprasSameText(finalCardName, supplierCode)) {
        const docBp = await comprasFetchBusinessPartnerNameFromDocs(supplierCode, finalCardName || supplierName).catch(() => null);
        const docName = comprasExtractBusinessPartnerName(docBp?.raw, docBp?.cardName || finalCardName || supplierName);
        if (docName) finalCardName = comprasStr(docName);
        if (!bpRaw && docBp?.raw) bpRaw = docBp.raw;
      }
    }
    if (!finalCardName) finalCardName = comprasExtractBusinessPartnerName(bpRaw, supplierName);

    return {
      cardCode: supplierCode,
      cardName: finalCardName,
      source: comprasStr(cand?.source),
      hint,
      balance: {
        paymentStatus: comprasStr(balanceInfo?.paymentStatus || (supplierCode ? 'SIN_DATOS' : 'SIN_PROVEEDOR')),
        amountDue: comprasNum(balanceInfo?.amountDue || 0),
        overdueAmount: comprasNum(balanceInfo?.overdueAmount || 0),
        balanceRaw: comprasNum(balanceInfo?.balanceRaw || 0),
        balanceSource: comprasStr(balanceInfo?.source || balanceInfo?.balanceSource),
        paymentTermsName: comprasStr(balanceInfo?.paymentTermsName),
        creditDays: Number(balanceInfo?.creditDays || 0),
        debtNote: comprasStr(balanceInfo?.debtNote),
      },
      raw: bpRaw,
    };
  }));

  const validSuppliers = comprasDedupResolvedSuppliers(suppliers.filter((x) => x.cardCode || x.cardName));
  const aggregate = comprasAggregateSupplierStatuses(validSuppliers);
  const primary = validSuppliers[0] || {
    cardCode: comprasStr(chosen?.cardCode),
    cardName: comprasStr(chosen?.cardName),
    source: comprasStr(chosen?.source),
    hint,
    balance: {
      paymentStatus: 'SIN_PROVEEDOR',
      amountDue: 0,
      overdueAmount: 0,
      balanceRaw: 0,
      balanceSource: '',
      paymentTermsName: '',
      creditDays: 0,
      debtNote: 'El artículo no tiene proveedor fijo identificable en SAP. Se intentó Item Master, búsqueda del socio de negocio y el historial de compras.',
    },
    raw: chosen?.raw || null,
  };

  const result = {
    itemCode: code,
    itemName,
    supplier: {
      cardCode: comprasStr(primary?.cardCode),
      cardName: comprasStr(primary?.cardName),
      source: comprasStr(primary?.source || (primary?.cardCode ? 'SAP' : '')),
      hint,
    },
    suppliers: validSuppliers,
    balance: {
      paymentStatus: validSuppliers.length > 1 ? comprasStr(aggregate.paymentStatus) : comprasStr(primary?.balance?.paymentStatus || (primary?.cardCode ? 'SIN_DATOS' : 'SIN_PROVEEDOR')),
      amountDue: validSuppliers.length > 1 ? comprasNum(aggregate.totalAmountDue || 0) : comprasNum(primary?.balance?.amountDue || 0),
      overdueAmount: validSuppliers.length > 1 ? comprasNum(aggregate.totalOverdue || 0) : comprasNum(primary?.balance?.overdueAmount || 0),
      balanceRaw: validSuppliers.length > 1 ? comprasNum(aggregate.totalAmountDue || 0) : comprasNum(primary?.balance?.balanceRaw || 0),
      balanceSource: validSuppliers.length > 1 ? comprasStr(aggregate.balanceSource) : comprasStr(primary?.balance?.balanceSource),
      paymentTermsName: validSuppliers.length > 1 ? comprasStr(aggregate.paymentTermsName) : comprasStr(primary?.balance?.paymentTermsName),
      creditDays: validSuppliers.length > 1 ? Number(aggregate.creditDays || 0) : Number(primary?.balance?.creditDays || 0),
      debtNote: validSuppliers.length > 1 ? comprasStr(aggregate.debtNote) : comprasStr(primary?.balance?.debtNote),
    },
    updatedAt: new Date().toISOString(),
    source: 'sap_live',
    cacheHit: false,
    raw: {
      item,
      itemRaw: supplierResolved?.rawItem || null,
      supplier: primary?.raw || null,
      supplierCandidates: supplierResolved?.candidates || [],
    },
  };

  const saved = await comprasUpsertCache(result).catch(() => null);
  return saved ? comprasResponseFromRow(saved, 'sap_live') : result;
}

async function comprasResolveRequest(req, { forceLive = false } = {}) {
  const itemCode = comprasStr(req.query?.itemCode || req.body?.itemCode || req.query?.code || req.body?.code);
  if (!itemCode) throw new Error('ItemCode requerido');

  if (!forceLive) {
    const cached = await comprasReadCache(itemCode).catch(() => null);
    if (cached) return comprasResponseFromRow(cached, 'db');
  }
  return comprasFetchLive(itemCode);
}

app.get('/api/admin/compras/item', verifyAdmin, async (req, res) => {
  try {
    const forceLive = ['1','true','yes','si'].includes(comprasStr(req.query?.forceLive).toLowerCase());
    const result = await comprasResolveRequest(req, { forceLive });
    return safeJson(res, 200, { ok: true, result, meta: { forceLive, source: result?.source || '' } });
  } catch (e) {
    return safeJson(res, 400, { ok: false, message: e.message || String(e) });
  }
});

app.post('/api/admin/compras/item', verifyAdmin, async (req, res) => {
  try {
    const forceLive = ['1','true','yes','si'].includes(comprasStr(req.body?.forceLive).toLowerCase());
    const result = await comprasResolveRequest(req, { forceLive });
    return safeJson(res, 200, { ok: true, result, meta: { forceLive, source: result?.source || '' } });
  } catch (e) {
    return safeJson(res, 400, { ok: false, message: e.message || String(e) });
  }
});

app.post('/api/admin/compras/item/sync', verifyAdmin, async (req, res) => {
  try {
    const itemCode = comprasStr(req.body?.itemCode || req.query?.itemCode || req.body?.code || req.query?.code);
    if (!itemCode) return safeJson(res, 400, { ok: false, message: 'ItemCode requerido' });
    const result = await comprasFetchLive(itemCode);
    return safeJson(res, 200, { ok: true, result, meta: { source: 'sap_live', sync: true } });
  } catch (e) {
    return safeJson(res, 400, { ok: false, message: e.message || String(e) });
  }
});

app.get('/api/admin/compras/recent', verifyAdmin, async (_req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 200, { ok: true, rows: [] });
    const limit = Math.max(1, Math.min(100, Number(_req.query?.limit || 30)));
    const q = await dbQuery(`
      SELECT item_code, item_name, supplier_code, supplier_name, supplier_source, supplier_hint,
             payment_status, amount_due, overdue_amount, balance_raw, balance_source,
             payment_terms_name, credit_days, debt_note,
             item_json, supplier_json, result_json, updated_at
      FROM admin_compras_supplier_cache
      ORDER BY updated_at DESC
      LIMIT $1
    `, [limit]);
    const rows = (q.rows || []).map((row) => comprasResponseFromRow(row, 'db'));
    return safeJson(res, 200, { ok: true, rows });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});


/* =========================================================
   Start
========================================================= */
process.on("unhandledRejection", (e) => console.error("unhandledRejection:", e));
process.on("uncaughtException", (e) => console.error("uncaughtException:", e));


/* =========================================================
   Producción — Cierre de órdenes de fabricación (tablets)
========================================================= */
const PROD_CLOSE_HTML_FILE = path.join(process.cwd(), 'production-close.html');

function poCloseBool(v) {
  const s = String(v ?? '').trim().toLowerCase();
  return s === 'true' || s === '1' || s === 'yes' || s === 'tyes' || s === 'y' || s === 'si';
}
function poCloseIso(v) {
  return String(v || '').slice(0, 10);
}
function poCloseStatus(raw) {
  const s = String(raw || '').trim().toUpperCase();
  if (!s) return '';
  if (['BOPOSPLANNED', 'PLANNED', 'PLANIFICADO', 'P', '0'].includes(s)) return 'PLANNED';
  if (['BOPOSRELEASED', 'RELEASED', 'LIBERADO', 'R', '1'].includes(s)) return 'RELEASED';
  if (['BOPOSCLOSED', 'BOPS_CLOSED', 'CLOSED', 'CERRADO', 'C', 'L', '2'].includes(s)) return 'CLOSED';
  if (['BOPOSCANCELLED', 'BOPS_CANCELLED', 'CANCELLED', 'CANCELED', 'CANCELADO', '3'].includes(s)) return 'CANCELLED';
  return s;
}
function poCloseStatusLabel(raw) {
  const s = poCloseStatus(raw);
  if (s === 'PLANNED') return 'Planificado';
  if (s === 'RELEASED') return 'Liberado';
  if (s === 'CLOSED') return 'Cerrado';
  if (s === 'CANCELLED') return 'Cancelado';
  return String(raw || '—');
}
function poCloseRemainingQty(row = {}) {
  const planned = Number(row?.plannedQty ?? row?.PlannedQuantity ?? 0) || 0;
  const completed = Number(row?.completedQty ?? row?.CompletedQuantity ?? 0) || 0;
  const rejected = Number(row?.rejectedQty ?? row?.RejectedQuantity ?? 0) || 0;
  return Math.max(0, Math.round((planned - completed - rejected) * 1000) / 1000);
}
function poCloseNormalizeOrderRow(row = {}) {
  const normalized = {
    absoluteEntry: Number(row?.AbsoluteEntry ?? row?.absoluteEntry ?? row?.DocEntry ?? 0) || 0,
    docNum: Number(row?.DocumentNumber ?? row?.DocNum ?? row?.docNum ?? 0) || 0,
    itemCode: String(row?.ItemNo ?? row?.ItemCode ?? row?.itemCode ?? '').trim(),
    itemName: String(row?.ProductDescription ?? row?.ProdName ?? row?.ItemName ?? row?.itemName ?? '').trim(),
    plannedQty: Number(row?.PlannedQuantity ?? row?.PlannedQty ?? row?.plannedQty ?? 0) || 0,
    completedQty: Number(row?.CompletedQuantity ?? row?.CmpltQty ?? row?.CompletedQty ?? row?.completedQty ?? 0) || 0,
    rejectedQty: Number(row?.RejectedQuantity ?? row?.RejectedQty ?? row?.rejectedQty ?? 0) || 0,
    postDate: poCloseIso(row?.PostingDate ?? row?.PostDate ?? row?.postDate ?? ''),
    dueDate: poCloseIso(row?.DueDate ?? row?.ClosingDate ?? row?.dueDate ?? ''),
    startDate: poCloseIso(row?.StartDate ?? row?.startDate ?? ''),
    warehouse: String(row?.Warehouse ?? row?.WarehouseCode ?? row?.WhsCode ?? row?.warehouse ?? '').trim(),
    status: poCloseStatus(row?.ProductionOrderStatus ?? row?.Status ?? row?.status ?? ''),
    statusLabel: poCloseStatusLabel(row?.ProductionOrderStatus ?? row?.Status ?? row?.status ?? ''),
    origin: String(row?.ProductionOrderOrigin ?? row?.Origin ?? row?.origin ?? '').trim(),
    series: String(row?.Series ?? row?.series ?? '').trim(),
  };
  normalized.remainingQty = poCloseRemainingQty(normalized);
  return normalized;
}
async function poCloseEnsureTables() {
  if (!hasDb()) return;
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS admin_production_close_events (
      id BIGSERIAL PRIMARY KEY,
      action TEXT NOT NULL DEFAULT '',
      absolute_entry BIGINT NOT NULL DEFAULT 0,
      doc_num BIGINT NOT NULL DEFAULT 0,
      item_code TEXT NOT NULL DEFAULT '',
      item_name TEXT NOT NULL DEFAULT '',
      warehouse TEXT NOT NULL DEFAULT '',
      status_before TEXT NOT NULL DEFAULT '',
      status_after TEXT NOT NULL DEFAULT '',
      batch_number TEXT NOT NULL DEFAULT '',
      expiry_date DATE,
      reported_qty NUMERIC(19,6) NOT NULL DEFAULT 0,
      receipt_doc_entry BIGINT,
      receipt_doc_num BIGINT,
      note TEXT NOT NULL DEFAULT '',
      order_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      receipt_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      admin_user TEXT NOT NULL DEFAULT '',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_admin_production_close_events_created ON admin_production_close_events(created_at DESC)`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_admin_production_close_events_order ON admin_production_close_events(absolute_entry, doc_num, created_at DESC)`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_admin_production_close_events_item ON admin_production_close_events(item_code, created_at DESC)`);
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS admin_production_close_timers (
      id BIGSERIAL PRIMARY KEY,
      absolute_entry BIGINT NOT NULL UNIQUE,
      doc_num BIGINT NOT NULL DEFAULT 0,
      item_code TEXT NOT NULL DEFAULT '',
      item_name TEXT NOT NULL DEFAULT '',
      warehouse TEXT NOT NULL DEFAULT '',
      status TEXT NOT NULL DEFAULT 'IDLE',
      accumulated_seconds BIGINT NOT NULL DEFAULT 0,
      started_at TIMESTAMPTZ,
      last_started_at TIMESTAMPTZ,
      finalized_at TIMESTAMPTZ,
      applied_hours NUMERIC(19,6) NOT NULL DEFAULT 0,
      affected_lines INT NOT NULL DEFAULT 0,
      note TEXT NOT NULL DEFAULT '',
      order_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
      admin_user TEXT NOT NULL DEFAULT '',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_admin_production_close_timers_status ON admin_production_close_timers(status, updated_at DESC)`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_admin_production_close_timers_doc ON admin_production_close_timers(doc_num, updated_at DESC)`);
  await dbQuery(`
    INSERT INTO app_state(k, v, updated_at)
    VALUES ('production_close_next_batch_seq', '3000', NOW())
    ON CONFLICT (k) DO NOTHING
  `).catch(() => {});
}
__extraBootTasks.push(poCloseEnsureTables);

async function poCloseGetNextLotPreview() {
  if (!hasDb()) return '3000';
  const r = await dbQuery(`SELECT v FROM app_state WHERE k='production_close_next_batch_seq' LIMIT 1`);
  const raw = String(r.rows?.[0]?.v || '3000').trim();
  const n = Number(raw);
  return String(Number.isFinite(n) && n > 0 ? Math.floor(n) : 3000);
}
async function poCloseTakeNextLotNumber() {
  if (!hasDb()) return String(Date.now());
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const sel = await client.query(`SELECT v FROM app_state WHERE k='production_close_next_batch_seq' FOR UPDATE`);
    let current = Number(sel.rows?.[0]?.v || 3000);
    if (!Number.isFinite(current) || current < 1) current = 3000;
    const nextBatch = String(Math.floor(current));
    await client.query(
      `INSERT INTO app_state(k, v, updated_at)
       VALUES ('production_close_next_batch_seq', $1, NOW())
       ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v, updated_at = NOW()`,
      [String(Math.floor(current + 1))]
    );
    await client.query('COMMIT');
    return nextBatch;
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch {}
    throw e;
  } finally {
    client.release();
  }
}
async function poCloseLogEvent(payload = {}) {
  if (!hasDb()) return;
  const p = payload || {};
  await dbQuery(`
    INSERT INTO admin_production_close_events(
      action, absolute_entry, doc_num, item_code, item_name, warehouse,
      status_before, status_after, batch_number, expiry_date, reported_qty,
      receipt_doc_entry, receipt_doc_num, note, order_payload, receipt_payload,
      admin_user, created_at
    )
    VALUES (
      $1,$2,$3,$4,$5,$6,
      $7,$8,$9,$10,$11,
      $12,$13,$14,$15::jsonb,$16::jsonb,
      $17,NOW()
    )
  `, [
    String(p.action || ''),
    Number(p.absoluteEntry || 0),
    Number(p.docNum || 0),
    String(p.itemCode || ''),
    String(p.itemName || ''),
    String(p.warehouse || ''),
    String(p.statusBefore || ''),
    String(p.statusAfter || ''),
    String(p.batchNumber || ''),
    p.expiryDate ? String(p.expiryDate).slice(0, 10) : null,
    Number(p.reportedQty || 0),
    p.receiptDocEntry != null ? Number(p.receiptDocEntry) : null,
    p.receiptDocNum != null ? Number(p.receiptDocNum) : null,
    String(p.note || ''),
    JSON.stringify(p.orderPayload || {}),
    JSON.stringify(p.receiptPayload || {}),
    String(p.adminUser || ''),
  ]);
}

function poCloseFormatElapsed(totalSeconds = 0) {
  const sec = Math.max(0, Math.floor(Number(totalSeconds || 0)));
  const hh = String(Math.floor(sec / 3600)).padStart(2, '0');
  const mm = String(Math.floor((sec % 3600) / 60)).padStart(2, '0');
  const ss = String(sec % 60).padStart(2, '0');
  return `${hh}:${mm}:${ss}`;
}
function poCloseTimerElapsedSeconds(row = {}) {
  const base = Math.max(0, Number(row?.accumulated_seconds ?? row?.accumulatedSeconds ?? 0) || 0);
  const startedAt = row?.started_at || row?.startedAt;
  const status = String(row?.status || '').toUpperCase();
  if (status !== 'RUNNING' || !startedAt) return Math.floor(base);
  const startedMs = new Date(startedAt).getTime();
  if (!Number.isFinite(startedMs) || startedMs <= 0) return Math.floor(base);
  return Math.max(0, Math.floor(base + ((Date.now() - startedMs) / 1000)));
}
function poCloseTimerStateFromRow(row = {}, fallback = {}) {
  const elapsedSeconds = poCloseTimerElapsedSeconds(row);
  const status = String(row?.status || fallback?.status || 'IDLE').toUpperCase();
  return {
    absoluteEntry: Number(row?.absolute_entry ?? row?.absoluteEntry ?? fallback?.absoluteEntry ?? 0) || 0,
    docNum: Number(row?.doc_num ?? row?.docNum ?? fallback?.docNum ?? 0) || 0,
    itemCode: String(row?.item_code ?? row?.itemCode ?? fallback?.itemCode ?? ''),
    itemName: String(row?.item_name ?? row?.itemName ?? fallback?.itemName ?? ''),
    warehouse: String(row?.warehouse ?? fallback?.warehouse ?? ''),
    status,
    elapsedSeconds,
    elapsedLabel: poCloseFormatElapsed(elapsedSeconds),
    accumulatedSeconds: Math.max(0, Number(row?.accumulated_seconds ?? row?.accumulatedSeconds ?? 0) || 0),
    startedAt: row?.started_at || row?.startedAt || null,
    lastStartedAt: row?.last_started_at || row?.lastStartedAt || null,
    finalizedAt: row?.finalized_at || row?.finalizedAt || null,
    appliedHours: Number(row?.applied_hours ?? row?.appliedHours ?? 0) || 0,
    affectedLines: Number(row?.affected_lines ?? row?.affectedLines ?? 0) || 0,
    note: String(row?.note || ''),
    adminUser: String(row?.admin_user ?? row?.adminUser ?? ''),
    updatedAt: row?.updated_at || row?.updatedAt || null,
  };
}
async function poCloseGetTimerRow(absoluteEntry) {
  if (!hasDb()) return null;
  const abs = Number(absoluteEntry || 0) || 0;
  if (!abs) return null;
  const r = await dbQuery(`SELECT * FROM admin_production_close_timers WHERE absolute_entry=$1 LIMIT 1`, [abs]);
  return r.rows?.[0] || null;
}
async function poCloseGetTimerState(absoluteEntry, fallback = {}) {
  const row = await poCloseGetTimerRow(absoluteEntry);
  return row ? poCloseTimerStateFromRow(row, fallback) : poCloseTimerStateFromRow({ status: 'IDLE' }, fallback);
}
function poCloseIsGeneralOperatorLine(line = {}) {
  const code = String(line?.itemCode ?? line?.ItemNo ?? line?.ItemCode ?? '').trim().toUpperCase();
  const name = String(line?.itemName ?? line?.ItemName ?? line?.ItemDescription ?? '').trim().toLowerCase();
  return name.includes('operario general') || ['MO01', 'M001', 'M0O1', 'MOO1'].includes(code);
}
async function poCloseStartTimer(absoluteEntry, docNum = 0, adminUser = 'admin', note = '') {
  if (!hasDb()) throw new Error('DB no configurada para guardar el tiempo de producción');
  const abs = Number(absoluteEntry || 0) || 0;
  if (!abs) throw new Error('AbsoluteEntry inválido');
  const order = await poCloseFetchOrderDetail(abs, docNum);
  if (!order) throw new Error('Orden no encontrada');
  const current = await poCloseGetTimerRow(abs);
  const now = new Date().toISOString();

  if (current && String(current.status || '').toUpperCase() === 'RUNNING') {
    return { timer: poCloseTimerStateFromRow(current, order), order, message: `El tiempo de la orden #${order.docNum} ya estaba en marcha.` };
  }

  const reset = current && String(current.status || '').toUpperCase() === 'FINALIZED';
  const accumulated = reset ? 0 : Math.max(0, Number(current?.accumulated_seconds ?? 0) || 0);
  await dbQuery(
    `INSERT INTO admin_production_close_timers(
      absolute_entry, doc_num, item_code, item_name, warehouse, status,
      accumulated_seconds, started_at, last_started_at, finalized_at,
      applied_hours, affected_lines, note, order_snapshot, admin_user, created_at, updated_at
    ) VALUES (
      $1,$2,$3,$4,$5,'RUNNING',
      $6,$7,$7,NULL,
      0,0,$8,$9::jsonb,$10,NOW(),NOW()
    )
    ON CONFLICT (absolute_entry) DO UPDATE SET
      doc_num=EXCLUDED.doc_num,
      item_code=EXCLUDED.item_code,
      item_name=EXCLUDED.item_name,
      warehouse=EXCLUDED.warehouse,
      status='RUNNING',
      accumulated_seconds=$6,
      started_at=$7,
      last_started_at=$7,
      finalized_at=NULL,
      applied_hours=CASE WHEN $11 THEN 0 ELSE admin_production_close_timers.applied_hours END,
      affected_lines=CASE WHEN $11 THEN 0 ELSE admin_production_close_timers.affected_lines END,
      note=$8,
      order_snapshot=$9::jsonb,
      admin_user=$10,
      updated_at=NOW()`,
    [abs, Number(order.docNum || 0), order.itemCode || '', order.itemName || '', order.warehouse || '', accumulated, now, String(note || ''), JSON.stringify(order || {}), String(adminUser || ''), !!reset]
  );

  const timer = await poCloseGetTimerState(abs, order);
  await poCloseLogEvent({
    action: reset ? 'TIMER_RESTART' : 'TIMER_START',
    absoluteEntry: order.absoluteEntry,
    docNum: order.docNum,
    itemCode: order.itemCode,
    itemName: order.itemName,
    warehouse: order.warehouse,
    statusBefore: current?.status || 'IDLE',
    statusAfter: 'RUNNING',
    note: String(note || ''),
    orderPayload: { order, timer },
    adminUser,
  }).catch(() => {});
  return { timer, order, message: reset ? 'Tiempo reiniciado y en marcha.' : 'Tiempo de producción iniciado.' };
}
async function poClosePauseTimer(absoluteEntry, docNum = 0, adminUser = 'admin', note = '') {
  if (!hasDb()) throw new Error('DB no configurada para guardar el tiempo de producción');
  const abs = Number(absoluteEntry || 0) || 0;
  if (!abs) throw new Error('AbsoluteEntry inválido');
  const order = await poCloseFetchOrderDetail(abs, docNum);
  const current = await poCloseGetTimerRow(abs);
  if (!current) {
    const timer = poCloseTimerStateFromRow({ status: 'IDLE' }, order || { absoluteEntry: abs, docNum });
    return { timer, order, message: 'La orden no tiene un tiempo iniciado todavía.' };
  }
  const wasRunning = String(current.status || '').toUpperCase() === 'RUNNING';
  let accumulated = Math.max(0, Number(current.accumulated_seconds || 0) || 0);
  if (wasRunning && current.started_at) {
    const startedMs = new Date(current.started_at).getTime();
    if (Number.isFinite(startedMs) && startedMs > 0) accumulated += Math.max(0, Math.floor((Date.now() - startedMs) / 1000));
  }
  await dbQuery(
    `UPDATE admin_production_close_timers
       SET doc_num=$2,
           item_code=$3,
           item_name=$4,
           warehouse=$5,
           status='PAUSED',
           accumulated_seconds=$6,
           started_at=NULL,
           note=$7,
           order_snapshot=$8::jsonb,
           admin_user=$9,
           updated_at=NOW()
     WHERE absolute_entry=$1`,
    [abs, Number(order?.docNum || current.doc_num || 0), order?.itemCode || current.item_code || '', order?.itemName || current.item_name || '', order?.warehouse || current.warehouse || '', accumulated, String(note || current.note || ''), JSON.stringify(order || {}), String(adminUser || current.admin_user || '')]
  );
  const rowAfter = await poCloseGetTimerRow(abs);
  const timer = poCloseTimerStateFromRow(rowAfter, order || current);
  await poCloseLogEvent({
    action: 'TIMER_PAUSE',
    absoluteEntry: abs,
    docNum: timer.docNum,
    itemCode: timer.itemCode,
    itemName: timer.itemName,
    warehouse: timer.warehouse,
    statusBefore: current.status || 'IDLE',
    statusAfter: 'PAUSED',
    note: String(note || ''),
    orderPayload: { order, timer },
    adminUser,
  }).catch(() => {});
  return { timer, order, message: 'Tiempo de producción pausado.' };
}
async function poCloseFinalizeTimer(absoluteEntry, docNum = 0, adminUser = 'admin', note = '') {
  if (!hasDb()) throw new Error('DB no configurada para guardar el tiempo de producción');
  const abs = Number(absoluteEntry || 0) || 0;
  if (!abs) throw new Error('AbsoluteEntry inválido');
  const pauseResult = await poClosePauseTimer(abs, docNum, adminUser, note);
  const timerPaused = pauseResult.timer;
  const order = pauseResult.order || await poCloseFetchOrderDetail(abs, docNum);
  const elapsedSeconds = Math.max(0, Number(timerPaused?.elapsedSeconds || 0) || 0);
  if (!(elapsedSeconds > 0)) throw new Error('No hay tiempo acumulado para finalizar.');
  if (!order) throw new Error('Orden no encontrada');

  const appliedHours = Number((elapsedSeconds / 3600).toFixed(6));
  const operatorLines = (Array.isArray(order.lines) ? order.lines : []).filter((line) => poCloseIsGeneralOperatorLine(line));
  if (!operatorLines.length) {
    throw new Error('No se encontraron líneas de Operario General para aplicar el tiempo.');
  }

  const updates = operatorLines.map((line) => ({
    lineNumber: Number(line.lineNumber || 0) || 0,
    itemCode: String(line.itemCode || '').trim(),
    itemName: String(line.itemName || '').trim(),
    warehouse: String(line.warehouse || order.warehouse || '').trim(),
    baseQuantity: Number(line.baseQuantity || 0) || 0,
    plannedQuantity: appliedHours,
    additionalQuantity: Number(line.additionalQuantity || 0) || 0,
    issueMethod: String(line.issueMethod || '').trim(),
    itemType: String(line.itemType || '').trim(),
    isResource: !!line.isResource,
    _delete: false,
    _isNew: false,
  }));

  const orderAfter = await poCloseSaveResourceLines(abs, updates, adminUser, `Tiempo aplicado automáticamente. ${note || ''}`.trim());
  await dbQuery(
    `UPDATE admin_production_close_timers
       SET doc_num=$2,
           item_code=$3,
           item_name=$4,
           warehouse=$5,
           status='FINALIZED',
           accumulated_seconds=$6,
           started_at=NULL,
           finalized_at=NOW(),
           applied_hours=$7,
           affected_lines=$8,
           note=$9,
           order_snapshot=$10::jsonb,
           admin_user=$11,
           updated_at=NOW()
     WHERE absolute_entry=$1`,
    [abs, Number(orderAfter?.docNum || order.docNum || 0), orderAfter?.itemCode || order.itemCode || '', orderAfter?.itemName || order.itemName || '', orderAfter?.warehouse || order.warehouse || '', elapsedSeconds, appliedHours, operatorLines.length, String(note || ''), JSON.stringify(orderAfter || order || {}), String(adminUser || '')]
  );
  const timer = await poCloseGetTimerState(abs, orderAfter || order);
  await poCloseLogEvent({
    action: 'TIMER_FINALIZE',
    absoluteEntry: abs,
    docNum: timer.docNum,
    itemCode: timer.itemCode,
    itemName: timer.itemName,
    warehouse: timer.warehouse,
    statusBefore: pauseResult.timer?.status || 'PAUSED',
    statusAfter: 'FINALIZED',
    reportedQty: appliedHours,
    note: `Tiempo aplicado a ${operatorLines.length} línea(s) de Operario General. ${note || ''}`.trim(),
    orderPayload: { before: order, after: orderAfter, timer },
    adminUser,
  }).catch(() => {});
  return {
    timer,
    order: orderAfter || order,
    elapsedSeconds,
    appliedHours,
    affectedLines: operatorLines.length,
    message: `Tiempo finalizado (${poCloseFormatElapsed(elapsedSeconds)}) y aplicado en ${operatorLines.length} línea(s) de Operario General.`
  };
}

function poCloseGetOrderLinesProp(raw = {}) {
  if (Array.isArray(raw?.ProductionOrderLines)) return 'ProductionOrderLines';
  if (Array.isArray(raw?.Lines)) return 'Lines';
  if (Array.isArray(raw?.DocumentLines)) return 'DocumentLines';
  return 'ProductionOrderLines';
}
function poCloseLooksLikeResourceLine(line = {}) {
  const rawType = String(line?.ItemType ?? line?.itemType ?? line?.Type ?? '').trim().toLowerCase();
  if (['290', 'pit_resource', 'resource', 'r'].includes(rawType)) return true;
  const desc = String(line?.ItemName ?? line?.ItemDescription ?? line?.ProductDescription ?? line?.LineText ?? '').toLowerCase();
  if (desc.includes('recurso')) return true;
  return false;
}
function poCloseNormalizeOrderLine(line = {}, order = {}) {
  const lineNumber = Number(line?.LineNumber ?? line?.LineNum ?? line?.VisOrder ?? line?.VisualOrder ?? 0) || 0;
  const code = String(line?.ItemNo ?? line?.ItemCode ?? line?.ResourceCode ?? line?.Code ?? '').trim();
  const name = String(line?.ItemName ?? line?.ItemDescription ?? line?.ProductDescription ?? line?.LineText ?? '').trim();
  const baseQuantity = Number(line?.BaseQuantity ?? line?.BaseQty ?? 0) || 0;
  const plannedQuantity = Number(line?.PlannedQuantity ?? line?.PlannedQty ?? line?.RequiredQuantity ?? 0) || 0;
  const additionalQuantity = Number(line?.AdditionalQuantity ?? line?.AdditQty ?? 0) || 0;
  const issuedQuantity = Number(line?.IssuedQuantity ?? line?.IssuedQty ?? 0) || 0;
  const warehouse = String(line?.Warehouse ?? line?.WarehouseCode ?? line?.WhsCode ?? order?.warehouse ?? '').trim();
  const rawType = String(line?.ItemType ?? line?.itemType ?? line?.Type ?? '').trim();
  return {
    lineNumber,
    itemCode: code,
    itemName: name,
    warehouse,
    baseQuantity,
    plannedQuantity,
    additionalQuantity,
    issuedQuantity,
    issueMethod: String(line?.ProductionOrderIssueType ?? line?.IssueType ?? '').trim(),
    itemType: rawType,
    isResource: poCloseLooksLikeResourceLine(line),
    raw: line,
  };
}
function poCloseOrderFromRaw(raw = {}) {
  const normalized = poCloseNormalizeOrderRow(raw);
  const linesPropKey = poCloseGetOrderLinesProp(raw);
  const rawLines = Array.isArray(raw?.[linesPropKey]) ? raw[linesPropKey] : [];
  const lines = rawLines.map((line) => poCloseNormalizeOrderLine(line, normalized));
  return {
    ...normalized,
    linesPropKey,
    lines,
    resourceLines: lines.filter((x) => x.isResource),
    componentLines: lines.filter((x) => !x.isResource),
    rawOrder: raw,
  };
}
async function poCloseFetchOrderRaw(absoluteEntry, docNum = 0, { includeLines = true } = {}) {
  const abs = Number(absoluteEntry || 0) || 0;
  const num = Number(docNum || 0) || 0;
  const expand = includeLines ? '?$expand=ProductionOrderLines' : '';
  const tries = [];
  if (abs) {
    tries.push(`/ProductionOrders(${abs})${expand}`);
    tries.push(`/ProductionOrders?$filter=${encodeURIComponent(`AbsoluteEntry eq ${abs}`)}${includeLines ? '&$expand=ProductionOrderLines' : ''}&$top=1`);
    tries.push(`/ProductionOrders?$filter=${encodeURIComponent(`DocEntry eq ${abs}`)}${includeLines ? '&$expand=ProductionOrderLines' : ''}&$top=1`);
    tries.push(`/ProductionOrders(${abs})`);
  }
  if (num) {
    tries.push(`/ProductionOrders?$filter=${encodeURIComponent(`DocumentNumber eq ${num}`)}${includeLines ? '&$expand=ProductionOrderLines' : ''}&$top=1`);
    tries.push(`/ProductionOrders?$filter=${encodeURIComponent(`DocNum eq ${num}`)}${includeLines ? '&$expand=ProductionOrderLines' : ''}&$top=1`);
  }
  for (const pathTry of tries) {
    try {
      const res = await slFetchFreshSession(pathTry);
      const row = Array.isArray(res?.value) ? (res.value[0] || null) : res;
      if (row && typeof row === 'object') return row;
    } catch {}
  }
  return null;
}
async function poCloseFetchOrderDetail(absoluteEntry, docNum = 0) {
  const raw = await poCloseFetchOrderRaw(absoluteEntry, docNum, { includeLines: true });
  return raw ? poCloseOrderFromRaw(raw) : null;
}
async function poCloseSaveResourceLines(absoluteEntry, resourceLines = [], adminUser = 'admin', note = '') {
  const abs = Number(absoluteEntry || 0) || 0;
  if (!abs) throw new Error('AbsoluteEntry inválido');
  const detailBefore = await poCloseFetchOrderDetail(abs);
  if (!detailBefore) throw new Error('Orden no encontrada');
  const raw = detailBefore.rawOrder || await poCloseFetchOrderRaw(abs, detailBefore.docNum, { includeLines: true });
  if (!raw) throw new Error('No se pudo leer la orden completa');
  const prop = detailBefore.linesPropKey || poCloseGetOrderLinesProp(raw);
  const originalLines = Array.isArray(raw?.[prop]) ? raw[prop] : [];
  const normalizedBefore = originalLines.map((line) => poCloseNormalizeOrderLine(line, detailBefore));
  const updates = Array.isArray(resourceLines) ? resourceLines : [];

  const normalizeEditableInput = (row = {}) => {
    const ln = Number(row?.lineNumber || 0) || 0;
    const rawType = String(row?.itemType || '').trim();
    const normalizedType = rawType || (poCloseLooksLikeResourceLine(row) ? '290' : '4');
    return {
      lineNumber: ln,
      itemCode: String(row?.itemCode || '').trim(),
      itemName: String(row?.itemName || '').trim(),
      warehouse: String(row?.warehouse || detailBefore.warehouse || '').trim(),
      baseQuantity: Number(row?.baseQuantity ?? 0) || 0,
      plannedQuantity: Number(row?.plannedQuantity ?? 0) || 0,
      additionalQuantity: Number(row?.additionalQuantity ?? 0) || 0,
      issueMethod: String(row?.issueMethod || '').trim(),
      itemType: normalizedType,
      isResource: ['290', 'pit_resource', 'resource', 'r'].includes(normalizedType.toLowerCase()),
      _delete: !!row?._delete,
      _isNew: !!row?._isNew || !(ln > 0),
    };
  };

  const sameNum = (a, b, decimals = 6) => Number((Number(a || 0)).toFixed(decimals)) === Number((Number(b || 0)).toFixed(decimals));
  const currentLines = normalizedBefore.map((x) => ({
    lineNumber: Number(x.lineNumber || 0) || 0,
    itemCode: String(x.itemCode || '').trim(),
    itemName: String(x.itemName || '').trim(),
    warehouse: String(x.warehouse || detailBefore.warehouse || '').trim(),
    baseQuantity: Number(x.baseQuantity ?? 0) || 0,
    plannedQuantity: Number(x.plannedQuantity ?? 0) || 0,
    additionalQuantity: Number(x.additionalQuantity ?? 0) || 0,
    issueMethod: String(x.issueMethod || '').trim(),
    itemType: String(x.itemType || '').trim(),
    isResource: !!x.isResource,
  }));

  const normalizedUpdates = updates.map((row) => normalizeEditableInput(row));
  const activeUpdates = normalizedUpdates.filter((row) => !row._delete);

  const currentByLine = new Map(currentLines.map((row) => [String(row.lineNumber), row]));
  const hasStructuralChanges =
    normalizedUpdates.some((row) => row._delete || row._isNew || !(row.lineNumber > 0)) ||
    activeUpdates.length !== currentLines.length;
  const hasValueChanges = activeUpdates.some((row) => {
    const prev = currentByLine.get(String(row.lineNumber));
    if (!prev) return true;
    return (
      String(prev.warehouse || '') !== String(row.warehouse || '') ||
      !sameNum(prev.baseQuantity, row.baseQuantity) ||
      !sameNum(prev.plannedQuantity, row.plannedQuantity) ||
      !sameNum(prev.additionalQuantity, row.additionalQuantity)
    );
  });

  if (!hasStructuralChanges && !hasValueChanges) {
    return detailBefore;
  }

  const existingByLine = new Map();
  for (const row of normalizedUpdates) {
    if (row.lineNumber > 0) existingByLine.set(String(row.lineNumber), row);
  }

  const additions = normalizedUpdates.filter((row) => row._isNew && !row._delete && row.itemCode);

  const stripInvalidLineAliases = (line = {}) => {
    const copy = { ...line };
    delete copy.ItemCode;
    delete copy.WarehouseCode;
    delete copy.ProductDescription;
    delete copy.ItemDescription;
    delete copy.LineText;
    delete copy.ItemName;
    return copy;
  };

  const updatedLines = [];
  for (let i = 0; i < originalLines.length; i++) {
    const rawLine = originalLines[i];
    const normLine = normalizedBefore[i];
    const desired = existingByLine.get(String(normLine.lineNumber));

    if (!desired) {
      updatedLines.push(stripInvalidLineAliases(rawLine));
      continue;
    }
    if (desired._delete) continue;

    const merged = stripInvalidLineAliases(rawLine);
    merged.BaseQuantity = Number(desired.baseQuantity || 0);
    merged.PlannedQuantity = Number(desired.plannedQuantity || 0);
    merged.AdditionalQuantity = Number(desired.additionalQuantity || 0);
    if (desired.warehouse) {
      merged.Warehouse = desired.warehouse;
    }
    updatedLines.push(merged);
  }

  const pickTemplateForAddition = (desired = {}) => {
    const desiredIsResource = !!desired.isResource;
    const templateRaw = originalLines.find((line) => {
      const normalized = poCloseNormalizeOrderLine(line, detailBefore);
      return !!normalized?.isResource === desiredIsResource;
    }) || originalLines[0] || {};
    return stripInvalidLineAliases(templateRaw);
  };

  for (const desired of additions) {
    const merged = { ...pickTemplateForAddition(desired) };
    delete merged.LineNumber;
    delete merged.LineNum;
    delete merged.VisualOrder;
    delete merged.VisOrder;
    delete merged.DocumentAbsoluteEntry;
    delete merged.DocEntry;
    merged.ItemType = desired.isResource ? 290 : (merged.ItemType ?? 4);
    merged.ItemNo = desired.itemCode;
    merged.BaseQuantity = Number(desired.baseQuantity || 0);
    merged.PlannedQuantity = Number(desired.plannedQuantity || 0);
    merged.AdditionalQuantity = Number(desired.additionalQuantity || 0);
    const wh = desired.warehouse || detailBefore.warehouse || '';
    if (wh) merged.Warehouse = wh;
    updatedLines.push(merged);
  }

  await slFetchFreshSession(`/ProductionOrders(${abs})`, {
    method: 'PATCH',
    headers: { 'B1S-ReplaceCollectionsOnPatch': 'true' },
    body: JSON.stringify({ [prop]: updatedLines }),
  });

  const detailAfter = await poCloseFetchOrderDetail(abs);
  await poCloseLogEvent({
    action: 'SAVE_EDITABLE_LINES',
    absoluteEntry: detailAfter?.absoluteEntry || detailBefore.absoluteEntry,
    docNum: detailAfter?.docNum || detailBefore.docNum,
    itemCode: detailAfter?.itemCode || detailBefore.itemCode,
    itemName: detailAfter?.itemName || detailBefore.itemName,
    warehouse: detailAfter?.warehouse || detailBefore.warehouse,
    statusBefore: detailBefore.status,
    statusAfter: detailAfter?.status || detailBefore.status,
    note: String(note || ''),
    orderPayload: { before: detailBefore, after: detailAfter, editableLines: updates },
    adminUser,
  }).catch(() => {});
  return detailAfter || detailBefore;
}

async function poCloseFetchOpenOrdersFromSap({ q = '', top = 250 } = {}) {
  if (missingSapEnv()) throw new Error('SAP no configurado');
  const select = [
    'AbsoluteEntry','DocumentNumber','ItemNo','ProductDescription',
    'PlannedQuantity','CompletedQuantity','RejectedQuantity',
    'PostingDate','DueDate','StartDate','Warehouse','ProductionOrderStatus','ProductionOrderOrigin'
  ].join(',');
  const tries = [
    `/ProductionOrders?$select=${select}&$filter=${encodeURIComponent(`(ProductionOrderStatus eq 'boposPlanned' or ProductionOrderStatus eq 'boposReleased')`)}&$orderby=DueDate asc,DocumentNumber desc&$top=${Math.max(20, Math.min(400, Number(top || 250)))}`,
    `/ProductionOrders?$select=${select}&$filter=${encodeURIComponent(`(ProductionOrderStatus eq 'P' or ProductionOrderStatus eq 'R')`)}&$orderby=DueDate asc,DocumentNumber desc&$top=${Math.max(20, Math.min(400, Number(top || 250)))}`,
    `/ProductionOrders?$select=${select}&$orderby=DueDate asc,DocumentNumber desc&$top=${Math.max(50, Math.min(600, Number(top || 250) * 2))}`
  ];
  let rows = [];
  for (const p of tries) {
    try {
      const res = await slFetchFreshSession(p);
      const batch = Array.isArray(res?.value) ? res.value : (Array.isArray(res) ? res : []);
      if (batch.length) {
        rows = batch;
        if (tries.indexOf(p) < 2) break;
      }
    } catch {}
  }
  let orders = rows.map(poCloseNormalizeOrderRow).filter((x) => {
    const s = poCloseStatus(x.status);
    return s === 'PLANNED' || s === 'RELEASED';
  });
  const qq = String(q || '').trim().toLowerCase();
  if (qq) {
    orders = orders.filter((x) =>
      String(x.itemCode || '').toLowerCase().includes(qq) ||
      String(x.itemName || '').toLowerCase().includes(qq) ||
      String(x.docNum || '').includes(qq)
    );
  }
  orders.sort((a, b) => {
    const ad = String(a.dueDate || a.postDate || '');
    const bd = String(b.dueDate || b.postDate || '');
    if (ad && bd && ad !== bd) return ad < bd ? -1 : 1;
    return Number(b.docNum || 0) - Number(a.docNum || 0);
  });
  return orders.slice(0, Math.max(20, Math.min(500, Number(top || 250))));
}
async function poClosePatchStatus(absoluteEntry, targetStatus) {
  const abs = Number(absoluteEntry || 0) || 0;
  if (!abs) throw new Error('AbsoluteEntry inválido');
  const options = [];
  if (targetStatus === 'RELEASED') {
    options.push('boposReleased', 1, 'R');
  } else if (targetStatus === 'CLOSED') {
    options.push('boposClosed', 2, 'L', 'C');
  } else if (targetStatus === 'PLANNED') {
    options.push('boposPlanned', 0, 'P');
  } else {
    options.push(targetStatus);
  }
  let lastErr = null;
  for (const val of options) {
    try {
      await slFetchFreshSession(`/ProductionOrders(${abs})`, {
        method: 'PATCH',
        body: JSON.stringify({ ProductionOrderStatus: val }),
      });
      const after = await poCloseFetchOrderDetail(abs);
      if (after) return after;
      return { absoluteEntry: abs, status: targetStatus, statusLabel: poCloseStatusLabel(targetStatus) };
    } catch (e) {
      lastErr = e;
    }
  }
  throw lastErr || new Error('No se pudo actualizar el estado de la orden');
}
async function poCloseFetchItemFlags(itemCode) {
  const code = String(itemCode || '').trim();
  if (!code) return { batchManaged: false, serialManaged: false };
  try {
    const item = await slFetchFreshSession(`/Items('${encodeURIComponent(code)}')?$select=ItemCode,ItemName,ManageBatchNumbers,ManageSerialNumbers`);
    return {
      batchManaged: poCloseBool(item?.ManageBatchNumbers),
      serialManaged: poCloseBool(item?.ManageSerialNumbers),
      itemName: String(item?.ItemName || '').trim(),
    };
  } catch {
    return { batchManaged: false, serialManaged: false };
  }
}

async function poCloseCreateReceiptFromProduction(order, { quantity, postingDate = '', expiryDate = '', batchNumber = '', note = '', adminUser = '' } = {}) {
  const abs = Number(order?.absoluteEntry || 0) || 0;
  const docNum = Number(order?.docNum || 0) || 0;
  if (!abs && !docNum) throw new Error('Orden inválida');

  const qty = Number(quantity || order?.remainingQty || 0);
  if (!(qty > 0)) throw new Error('No hay cantidad pendiente para reportar');

  const itemCode = String(order?.itemCode || '').trim();
  const rawOrder = order?.rawOrder || {};
  const warehouseCandidates = Array.from(new Set([
    String(order?.warehouse || '').trim(),
    String(rawOrder?.Warehouse || '').trim(),
    String(rawOrder?.WarehouseCode || '').trim(),
    String(rawOrder?.WhsCode || '').trim(),
  ].filter(Boolean)));
  const flags = await poCloseFetchItemFlags(itemCode);
  let lot = String(batchNumber || '').trim();
  if (flags.batchManaged && !lot) lot = await poCloseTakeNextLotNumber();

  const postDate = poCloseIso(postingDate || getDateISOInOffset(TZ_OFFSET_MIN));
  const baseEntries = Array.from(new Set([docNum, abs].filter((n) => Number(n) > 0)));
  const comments = truncate(`[prod-close][user:${adminUser || 'admin'}] Orden ${docNum || abs} cierre web ${note || ''}`.trim(), 250);
  const journalMemo = truncate(`Receipt from Production orden ${docNum || abs}`, 50);

  const buildBatchNumbers = ({ includeBaseLineNumber = false, includeExpiry = false } = {}) => {
    if (!flags.batchManaged) return undefined;
    const row = {
      BatchNumber: lot,
      Quantity: qty,
    };
    if (includeBaseLineNumber) row.BaseLineNumber = 0;
    if (includeExpiry && expiryDate) row.ExpiryDate = poCloseIso(expiryDate);
    return [row];
  };

  const compact = (value) => {
    if (Array.isArray(value)) {
      return value.map(compact).filter((x) => x !== undefined);
    }
    if (value && typeof value === 'object') {
      const out = {};
      for (const [k, v] of Object.entries(value)) {
        const cv = compact(v);
        if (cv === undefined) continue;
        if (cv && typeof cv === 'object' && !Array.isArray(cv) && !Object.keys(cv).length) continue;
        if (Array.isArray(cv) && !cv.length) continue;
        out[k] = cv;
      }
      return out;
    }
    if (value === undefined || value === null || value === '') return undefined;
    return value;
  };

  const payloadVariants = [];
  const seen = new Set();
  const pushVariant = (label, header, line, batchCfg = null) => {
    const payload = compact({
      ...header,
      DocumentLines: [
        {
          ...line,
          ...(batchCfg ? { BatchNumbers: buildBatchNumbers(batchCfg) } : {}),
        },
      ],
    });
    const key = JSON.stringify(payload);
    if (seen.has(key)) return;
    seen.add(key);
    payloadVariants.push({ label, payload });
  };

  const headerVariants = [
    { DocDate: postDate, Comments: comments, JournalMemo: journalMemo },
    { DocDate: postDate, DocDueDate: postDate, Comments: comments, JournalMemo: journalMemo },
  ];
  const whVariants = warehouseCandidates.length ? warehouseCandidates : [''];
  const txVariants = [0, '0', 'botrntComplete'];
  const batchVariants = flags.batchManaged
    ? [
        { includeBaseLineNumber: false, includeExpiry: true },
        { includeBaseLineNumber: true, includeExpiry: true },
        { includeBaseLineNumber: false, includeExpiry: false },
      ]
    : [null];

  const makeLinkedLine = ({ baseEntry, wh = '', tx, includeWh = true, includeBaseLine = false, includeBaseLineNumber = false, includeTx = true } = {}) => {
    return compact({
      BaseType: 202,
      BaseEntry: baseEntry,
      ...(includeBaseLine ? { BaseLine: 0 } : {}),
      ...(includeBaseLineNumber ? { BaseLineNumber: 0 } : {}),
      ...(includeWh ? { WarehouseCode: wh } : {}),
      Quantity: qty,
      ...(includeTx ? { TransactionType: tx } : {}),
    });
  };

  for (const baseEntry of baseEntries) {
    for (const wh of whVariants) {
      for (const header of headerVariants) {
        // Más probable: orden enlazada, sin ItemCode ni TaxDate, usando número de orden en BaseEntry.
        for (const tx of txVariants) {
          for (const batchCfg of batchVariants) {
            pushVariant(`base:${baseEntry}|linked|tx:${String(tx)}|wh:${wh || 'omit'}|doc`, header, makeLinkedLine({ baseEntry, wh, tx, includeWh: !!wh, includeTx: true }), batchCfg);
            pushVariant(`base:${baseEntry}|linked|tx:${String(tx)}|wh:${wh || 'omit'}|doc|baseline`, header, makeLinkedLine({ baseEntry, wh, tx, includeWh: !!wh, includeBaseLine: true, includeTx: true }), batchCfg);
            pushVariant(`base:${baseEntry}|linked|tx:${String(tx)}|wh:${wh || 'omit'}|doc|baseline+num`, header, makeLinkedLine({ baseEntry, wh, tx, includeWh: !!wh, includeBaseLine: true, includeBaseLineNumber: true, includeTx: true }), batchCfg);
            pushVariant(`base:${baseEntry}|linked|tx:${String(tx)}|nowh|doc`, header, makeLinkedLine({ baseEntry, wh, tx, includeWh: false, includeTx: true }), batchCfg);
          }
        }

        // Variante alineada al ejemplo oficial de Service Layer para InventoryGenEntries basado en orden de producción.
        for (const batchCfg of batchVariants) {
          pushVariant(`base:${baseEntry}|linked|tx:omit|wh:${wh || 'omit'}|doc|official`, header, makeLinkedLine({ baseEntry, wh, includeWh: !!wh, includeTx: false }), batchCfg);
          pushVariant(`base:${baseEntry}|linked|tx:omit|wh:${wh || 'omit'}|doc|official|baseline`, header, makeLinkedLine({ baseEntry, wh, includeWh: !!wh, includeBaseLine: true, includeTx: false }), batchCfg);
          pushVariant(`base:${baseEntry}|linked|tx:omit|nowh|doc|official`, header, makeLinkedLine({ baseEntry, wh, includeWh: false, includeTx: false }), batchCfg);
        }

        // Fallback final por compatibilidad con ambientes más estrictos.
        for (const tx of [0, '0', undefined]) {
          for (const batchCfg of batchVariants) {
            pushVariant(`base:${baseEntry}|compat|tx:${tx === undefined ? 'omit' : String(tx)}|wh:${wh || 'omit'}|doc`, header, compact({
              BaseType: 202,
              BaseEntry: baseEntry,
              BaseLine: 0,
              Quantity: qty,
              WarehouseCode: wh || undefined,
              TransactionType: tx,
            }), batchCfg);
          }
        }
      }
    }
  }

  let created = null;
  let usedPayload = null;
  let usedLabel = '';
  let lastErr = null;
  const attempts = [];

  for (const attempt of payloadVariants) {
    try {
      created = await slFetchFreshSession('/InventoryGenEntries', {
        method: 'POST',
        body: JSON.stringify(attempt.payload),
      });
      usedPayload = attempt.payload;
      usedLabel = attempt.label;
      break;
    } catch (e) {
      lastErr = e;
      attempts.push(`${attempt.label}: ${String(e?.message || e)}`);
    }
  }

  if (!created) {
    const err = new Error((lastErr?.message || 'No se pudo crear el recibo de producción') + (attempts.length ? ` | Intentos: ${attempts.join(' | ')}` : ''));
    err.attempts = attempts;
    throw err;
  }

  return {
    receipt: created || {},
    receiptDocEntry: Number(created?.DocEntry || 0) || null,
    receiptDocNum: Number(created?.DocNum || 0) || null,
    batchNumber: lot,
    batchManaged: !!flags.batchManaged,
    payload: usedPayload || (payloadVariants[0]?.payload || {}),
    payloadLabel: usedLabel,
  };
}


async function poCloseProcessOrder(absoluteEntry, body = {}, adminUser = 'admin') {
  const abs = Number(absoluteEntry || 0) || 0;
  if (!abs) throw new Error('AbsoluteEntry inválido');
  let order = await poCloseFetchOrderDetail(abs, body?.docNum);
  if (!order) throw new Error('No se encontró la orden en SAP');
  const before = poCloseStatus(order.status);
  const steps = [];

  if (Array.isArray(body?.resourceLines) && body.resourceLines.length) {
    order = await poCloseSaveResourceLines(abs, body.resourceLines, adminUser, body?.note || '');
    steps.push('save_resources');
  }

  if (before === 'PLANNED') {
    order = await poClosePatchStatus(abs, 'RELEASED');
    steps.push('release');
    await poCloseLogEvent({
      action: 'RELEASE',
      absoluteEntry: order.absoluteEntry,
      docNum: order.docNum,
      itemCode: order.itemCode,
      itemName: order.itemName,
      warehouse: order.warehouse,
      statusBefore: before,
      statusAfter: poCloseStatus(order.status),
      note: String(body?.note || ''),
      orderPayload: order,
      adminUser,
    }).catch(() => {});
  }

  const qty = Number(body?.quantity || order.remainingQty || 0);
  const receiptInfo = await poCloseCreateReceiptFromProduction(order, {
    quantity: qty,
    postingDate: body?.postingDate,
    expiryDate: body?.expiryDate,
    batchNumber: body?.batchNumber,
    note: body?.note,
    adminUser,
  });
  steps.push('report_completion');

  await poCloseLogEvent({
    action: 'REPORT_COMPLETION',
    absoluteEntry: order.absoluteEntry,
    docNum: order.docNum,
    itemCode: order.itemCode,
    itemName: order.itemName,
    warehouse: order.warehouse,
    statusBefore: poCloseStatus(order.status),
    statusAfter: 'RELEASED',
    batchNumber: receiptInfo.batchNumber || '',
    expiryDate: body?.expiryDate || '',
    reportedQty: qty,
    receiptDocEntry: receiptInfo.receiptDocEntry,
    receiptDocNum: receiptInfo.receiptDocNum,
    note: String(body?.note || ''),
    orderPayload: order,
    receiptPayload: receiptInfo.payload,
    adminUser,
  }).catch(() => {});

  order = await poClosePatchStatus(abs, 'CLOSED');
  steps.push('close');

  await poCloseLogEvent({
    action: 'CLOSE',
    absoluteEntry: order.absoluteEntry,
    docNum: order.docNum,
    itemCode: order.itemCode,
    itemName: order.itemName,
    warehouse: order.warehouse,
    statusBefore: 'RELEASED',
    statusAfter: poCloseStatus(order.status),
    batchNumber: receiptInfo.batchNumber || '',
    expiryDate: body?.expiryDate || '',
    reportedQty: qty,
    receiptDocEntry: receiptInfo.receiptDocEntry,
    receiptDocNum: receiptInfo.receiptDocNum,
    note: String(body?.note || ''),
    orderPayload: order,
    receiptPayload: receiptInfo.payload,
    adminUser,
  }).catch(() => {});

  return { order, receiptInfo, steps };
}


app.get('/production-close', async (_req, res) => {
  try {
    const htmlPath = PROD_CLOSE_HTML_FILE;
    if (!fs.existsSync(htmlPath)) {
      res.setHeader('Content-Type', 'text/html; charset=utf-8');
      return res.status(200).send('<!doctype html><html><body style="font-family:Arial;padding:24px"><h2>production-close.html no encontrado</h2><p>Coloca el archivo en la misma carpeta del server.js.</p></body></html>');
    }
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    return res.status(200).send(fs.readFileSync(htmlPath, 'utf8'));
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});
app.get('/production-close.html', async (_req, res) => {
  try {
    const htmlPath = PROD_CLOSE_HTML_FILE;
    if (!fs.existsSync(htmlPath)) {
      res.setHeader('Content-Type', 'text/html; charset=utf-8');
      return res.status(200).send('<!doctype html><html><body style="font-family:Arial;padding:24px"><h2>production-close.html no encontrado</h2><p>Coloca el archivo en la misma carpeta del server.js.</p></body></html>');
    }
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    return res.status(200).send(fs.readFileSync(htmlPath, 'utf8'));
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get('/api/admin/production-close/open-orders', verifyAdmin, async (req, res) => {
  try {
    const q = String(req.query?.q || '').trim();
    const top = Math.max(20, Math.min(500, Number(req.query?.top || 250)));
    const orders = await poCloseFetchOpenOrdersFromSap({ q, top });
    const planned = orders.filter((x) => x.status === 'PLANNED').length;
    const released = orders.filter((x) => x.status === 'RELEASED').length;
    return safeJson(res, 200, {
      ok: true,
      generatedAt: new Date().toISOString(),
      counts: { total: orders.length, planned, released },
      nextLot: await poCloseGetNextLotPreview(),
      orders,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get('/api/admin/production-close/history', verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 200, { ok: true, rows: [], message: 'DB no configurada' });
    const limit = Math.max(10, Math.min(500, Number(req.query?.limit || 120)));
    const rows = await dbQuery(`
      SELECT id, action, absolute_entry, doc_num, item_code, item_name, warehouse,
             status_before, status_after, batch_number, expiry_date, reported_qty,
             receipt_doc_entry, receipt_doc_num, note, admin_user, created_at
      FROM admin_production_close_events
      ORDER BY created_at DESC
      LIMIT $1
    `, [limit]);
    return safeJson(res, 200, { ok: true, rows: rows.rows || [] });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get('/api/admin/production-close/next-lot', verifyAdmin, async (_req, res) => {
  try {
    return safeJson(res, 200, { ok: true, nextLot: await poCloseGetNextLotPreview() });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get('/api/admin/production-close/orders/:absoluteEntry/detail', verifyAdmin, async (req, res) => {
  try {
    const abs = Number(req.params.absoluteEntry || 0);
    const order = await poCloseFetchOrderDetail(abs, req.query?.docNum || 0);
    if (!order) return safeJson(res, 404, { ok: false, message: 'Orden no encontrada' });
    return safeJson(res, 200, { ok: true, order, nextLot: await poCloseGetNextLotPreview() });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.get('/api/admin/production-close/orders/:absoluteEntry/timer', verifyAdmin, async (req, res) => {
  try {
    const abs = Number(req.params.absoluteEntry || 0);
    const order = await poCloseFetchOrderDetail(abs, req.query?.docNum || 0).catch(() => null);
    const timer = await poCloseGetTimerState(abs, order || { absoluteEntry: abs, docNum: req.query?.docNum || 0 });
    return safeJson(res, 200, { ok: true, timer, order });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.post('/api/admin/production-close/orders/:absoluteEntry/timer/start', verifyAdmin, async (req, res) => {
  try {
    const abs = Number(req.params.absoluteEntry || 0);
    const out = await poCloseStartTimer(abs, req.body?.docNum || 0, String(req.admin?.user || 'admin'), String(req.body?.note || ''));
    return safeJson(res, 200, { ok: true, timer: out.timer, order: out.order, message: out.message });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.post('/api/admin/production-close/orders/:absoluteEntry/timer/pause', verifyAdmin, async (req, res) => {
  try {
    const abs = Number(req.params.absoluteEntry || 0);
    const out = await poClosePauseTimer(abs, req.body?.docNum || 0, String(req.admin?.user || 'admin'), String(req.body?.note || ''));
    return safeJson(res, 200, { ok: true, timer: out.timer, order: out.order, message: out.message });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.post('/api/admin/production-close/orders/:absoluteEntry/timer/finalize', verifyAdmin, async (req, res) => {
  try {
    const abs = Number(req.params.absoluteEntry || 0);
    const out = await poCloseFinalizeTimer(abs, req.body?.docNum || 0, String(req.admin?.user || 'admin'), String(req.body?.note || ''));
    return safeJson(res, 200, { ok: true, timer: out.timer, order: out.order, elapsedSeconds: out.elapsedSeconds, appliedHours: out.appliedHours, affectedLines: out.affectedLines, message: out.message });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.post('/api/admin/production-close/orders/:absoluteEntry/resources/save', verifyAdmin, async (req, res) => {
  try {
    const abs = Number(req.params.absoluteEntry || 0);
    const order = await poCloseSaveResourceLines(abs, req.body?.resourceLines || [], String(req.admin?.user || 'admin'), String(req.body?.note || ''));
    return safeJson(res, 200, { ok: true, order, message: 'Líneas editables actualizadas' });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.post('/api/admin/production-close/orders/:absoluteEntry/release', verifyAdmin, async (req, res) => {
  try {
    const abs = Number(req.params.absoluteEntry || 0);
    const before = await poCloseFetchOrderDetail(abs, req.body?.docNum);
    if (!before) return safeJson(res, 404, { ok: false, message: 'Orden no encontrada' });
    const after = before.status === 'RELEASED' ? before : await poClosePatchStatus(abs, 'RELEASED');
    await poCloseLogEvent({
      action: 'RELEASE',
      absoluteEntry: after.absoluteEntry,
      docNum: after.docNum,
      itemCode: after.itemCode,
      itemName: after.itemName,
      warehouse: after.warehouse,
      statusBefore: before.status,
      statusAfter: after.status,
      note: String(req.body?.note || ''),
      orderPayload: after,
      adminUser: String(req.admin?.user || 'admin'),
    }).catch(() => {});
    return safeJson(res, 200, { ok: true, order: after, message: 'Orden liberada' });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.post('/api/admin/production-close/orders/:absoluteEntry/report-completion', verifyAdmin, async (req, res) => {
  try {
    const abs = Number(req.params.absoluteEntry || 0);
    let order = await poCloseFetchOrderDetail(abs, req.body?.docNum);
    if (!order) return safeJson(res, 404, { ok: false, message: 'Orden no encontrada' });
    if (Array.isArray(req.body?.resourceLines) && req.body.resourceLines.length) {
      order = await poCloseSaveResourceLines(abs, req.body.resourceLines, String(req.admin?.user || 'admin'), String(req.body?.note || ''));
    }
    if (order.status === 'PLANNED') order = await poClosePatchStatus(abs, 'RELEASED');
    const qty = Number(req.body?.quantity || order.remainingQty || 0);
    const receiptInfo = await poCloseCreateReceiptFromProduction(order, {
      quantity: qty,
      postingDate: req.body?.postingDate,
      expiryDate: req.body?.expiryDate,
      batchNumber: req.body?.batchNumber,
      note: req.body?.note,
      adminUser: String(req.admin?.user || 'admin'),
    });
    await poCloseLogEvent({
      action: 'REPORT_COMPLETION',
      absoluteEntry: order.absoluteEntry,
      docNum: order.docNum,
      itemCode: order.itemCode,
      itemName: order.itemName,
      warehouse: order.warehouse,
      statusBefore: order.status,
      statusAfter: order.status,
      batchNumber: receiptInfo.batchNumber || '',
      expiryDate: req.body?.expiryDate || '',
      reportedQty: qty,
      receiptDocEntry: receiptInfo.receiptDocEntry,
      receiptDocNum: receiptInfo.receiptDocNum,
      note: String(req.body?.note || ''),
      orderPayload: order,
      receiptPayload: receiptInfo.payload,
      adminUser: String(req.admin?.user || 'admin'),
    }).catch(() => {});
    const refreshed = await poCloseFetchOrderDetail(abs, req.body?.docNum);
    return safeJson(res, 200, {
      ok: true,
      message: 'Terminación de reporte creada',
      order: refreshed || order,
      batchNumber: receiptInfo.batchNumber || '',
      receiptDocEntry: receiptInfo.receiptDocEntry,
      receiptDocNum: receiptInfo.receiptDocNum,
      nextLot: await poCloseGetNextLotPreview(),
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.post('/api/admin/production-close/orders/:absoluteEntry/close', verifyAdmin, async (req, res) => {
  try {
    const abs = Number(req.params.absoluteEntry || 0);
    const before = await poCloseFetchOrderDetail(abs, req.body?.docNum);
    if (!before) return safeJson(res, 404, { ok: false, message: 'Orden no encontrada' });
    const after = before.status === 'CLOSED' ? before : await poClosePatchStatus(abs, 'CLOSED');
    await poCloseLogEvent({
      action: 'CLOSE',
      absoluteEntry: after.absoluteEntry,
      docNum: after.docNum,
      itemCode: after.itemCode,
      itemName: after.itemName,
      warehouse: after.warehouse,
      statusBefore: before.status,
      statusAfter: after.status,
      note: String(req.body?.note || ''),
      orderPayload: after,
      adminUser: String(req.admin?.user || 'admin'),
    }).catch(() => {});
    return safeJson(res, 200, { ok: true, order: after, message: 'Orden cerrada' });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

app.post('/api/admin/production-close/orders/:absoluteEntry/process', verifyAdmin, async (req, res) => {
  try {
    const abs = Number(req.params.absoluteEntry || 0);
    const out = await poCloseProcessOrder(abs, req.body || {}, String(req.admin?.user || 'admin'));
    return safeJson(res, 200, {
      ok: true,
      message: 'Proceso completado: liberada, terminación creada y orden cerrada',
      order: out.order,
      receiptDocEntry: out.receiptInfo?.receiptDocEntry || null,
      receiptDocNum: out.receiptInfo?.receiptDocNum || null,
      batchNumber: out.receiptInfo?.batchNumber || '',
      nextLot: await poCloseGetNextLotPreview(),
      steps: out.steps || [],
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

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
