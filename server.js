import express from "express";
import pg from "pg";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";

const { Pool } = pg;
const app = express();
app.use(express.json({ limit: "4mb" }));

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

    await slFetchFreshSession(`/BusinessPartners('${encodeURIComponent(cardCode)}')?$select=CardCode,CardName`);

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
      return res.json({
        ok: true,
        message: "Cotización creada",
        docEntry: created.DocEntry,
        docNum: created.DocNum,
        warehouse: warehouseCode,
        bodega: warehouseCode,
        fallback: false,
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
      return res.json({
        ok: true,
        message: "Cotización creada (fallback sin WarehouseCode por -2028)",
        docEntry: created2.DocEntry,
        docNum: created2.DocNum,
        warehouse: warehouseCode,
        bodega: warehouseCode,
        fallback: true,
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

    await slFetchFreshSession(`/BusinessPartners('${encodeURIComponent(cardCode)}')?$select=CardCode,CardName`);

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
      DocumentLines: cleanLines.map((ln) => ({
        ItemCode: ln.ItemCode,
        ItemDescription: ln.ItemDescription,
        Quantity: ln.Quantity,
        UnitPrice: ln.Price,
        Price: ln.Price,
        WarehouseCode: warehouseCode,
        CostingCode: dim1,
      })),
    };

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

    return res.json({
      ok: true,
      message: "Solicitud de devolución creada",
      reqNum,
      reqEntry,
      warehouse: warehouseCode,
      bodega: warehouseCode,
      entity: String(SAP_RETURN_ENTITY || "Returns"),
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
async function fetchQuotationHeaders(fromDate, toDate, top = 200) {
  const fromExpr = quoteODataString(fromDate);
  const toPlus1 = addDaysISO(toDate, 1);
  const toExpr = quoteODataString(toPlus1);

  const path =
    `/Quotations?$select=DocEntry,DocNum,DocDate,DocTime,CardCode,CardName,DocTotal,Comments,DocumentStatus,CancelStatus` +
    `&$filter=DocDate ge ${fromExpr} and DocDate lt ${toExpr}` +
    `&$orderby=DocDate desc,DocNum desc&$top=${top}`;

  const r = await slFetchFreshSession(path);
  return Array.isArray(r?.value) ? r.value : [];
}

app.post("/api/admin/quotes/sync", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const mode = String(req.body?.mode || req.query?.mode || "window").trim();
    const n = Math.max(1, Math.min(365, Number(req.body?.n || req.query?.n || 30)));
    const maxDocs = Math.max(1, Math.min(500, Number(req.body?.maxDocs || req.query?.maxDocs || 150)));
    const maxLinesCalc = Math.max(0, Math.min(300, Number(req.body?.maxLinesCalc || req.query?.maxLinesCalc || 150)));

    const fromDate = isISO(req.body?.fromDate || req.query?.fromDate)
      ? String(req.body?.fromDate || req.query?.fromDate)
      : mode === "today"
      ? today
      : addDaysISO(today, -n);
    const toDate = isISO(req.body?.toDate || req.query?.toDate)
      ? String(req.body?.toDate || req.query?.toDate)
      : today;

    const headers = await fetchQuotationHeaders(fromDate, toDate, maxDocs);
    let scanned = 0;
    let saved = 0;

    for (const q of headers) {
      scanned++;

      const qFull = await sapGetByDocEntry("Quotations", q.DocEntry);
      const qLines = Array.isArray(qFull?.DocumentLines) ? qFull.DocumentLines : [];
      const docNum = Number(q.DocNum || qFull?.DocNum || 0);
      const docDate = String(q.DocDate || qFull?.DocDate || "").slice(0, 10);
      const comments = String(q.Comments || qFull?.Comments || "");
      const usuario = parseUserFromComments(comments);
      const warehouse = parseWhFromComments(comments);
      const cardCode = String(q.CardCode || qFull?.CardCode || "");
      const cardName = String(q.CardName || qFull?.CardName || "");
      const status = String(q.DocumentStatus || q.Status || qFull?.DocumentStatus || "");
      const cancelStatus = String(q.CancelStatus || q.cancelStatus || qFull?.CancelStatus || "");
      const docTotal = Number(q.DocTotal || qFull?.DocTotal || 0);

      let deliveredTotal = 0;
      for (const ln of qLines) {
        const qty = Number(ln?.Quantity || 0);
        const openQty = Number(ln?.RemainingOpenQuantity ?? ln?.OpenQty ?? ln?.OpenQuantity ?? 0);
        const lineTotal = Number(ln?.LineTotal ?? 0);
        let deliveredQty = qty > 0 ? Math.max(qty - openQty, 0) : 0;
        if (!Number.isFinite(deliveredQty)) deliveredQty = 0;
        const lineDelivered = qty > 0 ? lineTotal * Math.min(deliveredQty / qty, 1) : 0;
        deliveredTotal += Number.isFinite(lineDelivered) ? lineDelivered : 0;
      }

      await upsertQuoteCache({
        docNum,
        docEntry: q.DocEntry,
        docDate,
        docTime: Number(q.DocTime || 0),
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

      if (saved < maxLinesCalc) {
        for (const ln of qLines) {
          const itemCode = String(ln?.ItemCode || "").trim();
          if (!itemCode) continue;
          const qtyQuoted = Number(ln?.Quantity || 0);
          const openQty = Number(ln?.RemainingOpenQuantity ?? ln?.OpenQty ?? ln?.OpenQuantity ?? 0);
          const qtyDelivered = qtyQuoted > 0 ? Math.max(qtyQuoted - openQty, 0) : 0;
          const dollarsQuoted = Number(ln?.LineTotal ?? 0);
          const dollarsDelivered = qtyQuoted > 0 ? dollarsQuoted * Math.min(qtyDelivered / qtyQuoted, 1) : 0;
          const itemDesc = String(ln?.ItemDescription || ln?.ItemName || "").trim();

          await upsertQuoteLineCache({
            docNum,
            docDate,
            itemCode,
            itemDesc,
            qtyQuoted,
            qtyDelivered,
            dollarsQuoted,
            dollarsDelivered,
          });
        }
        await sleep(10);
      }

      saved++;
      if (saved >= maxDocs) break;
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
      lastSyncAt: stamp,
      window: { fromDate, toDate },
      note: "Sync guardado en DB. delivered_total y líneas se calculan desde OpenQty/RemainingOpenQuantity cuando SAP lo devuelve.",
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
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

    const totalsR = await dbQuery(
      `SELECT
        COUNT(*)::int AS quotes,
        COALESCE(SUM(q.doc_total),0)::float AS cotizado,
        COALESCE(SUM(q.delivered_total),0)::float AS entregado
       FROM quotes_cache q
       WHERE q.doc_date BETWEEN $1 AND $2
       ${createdJoin}`,
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
   Start
========================================================= */
process.on("unhandledRejection", (e) => console.error("unhandledRejection:", e));
process.on("uncaughtException", (e) => console.error("uncaughtException:", e));

(async () => {
  try {
    await ensureDb();
    console.log(hasDb() ? "DB ready ✅" : "DB not configured ⚠️");
  } catch (e) {
    console.error("DB init error:", e.message || String(e));
  }

  app.listen(Number(PORT), () => {
    console.log(`PRODIMA API UNIFICADA listening on :${PORT}`);
  });
})();
