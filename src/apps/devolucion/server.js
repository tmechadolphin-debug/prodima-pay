import express from "express";
import cors from "cors";
import pg from "pg";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";

const { Pool } = pg;

const app = express();
app.use(express.json({ limit: "2mb" }));

/* =========================================================
   ENV (Render)
========================================================= */
const {
  PORT = 3000,
  CORS_ORIGIN = "*",

  DATABASE_URL = "",
  JWT_SECRET = "change_me",

  // SAP
  SAP_BASE_URL = "",
  SAP_COMPANYDB = "",
  SAP_USER = "",
  SAP_PASS = "",
  SAP_WAREHOUSE = "300",
  SAP_PRICE_LIST = "Lista de Precios 99 2018",

  // Allowed items by warehouse
  ACTIVE_CODES_200 = "",
  ACTIVE_CODES_300 = "",
  ACTIVE_CODES_500 = "",

  // Users who can choose any warehouse
  ADMIN_FREE_WHS_USERS = "soto,liliana,daniel11,respinosa,test",

  // fallback warehouses if SAP doesn't respond
  WAREHOUSE_FALLBACK = "200,300,500,01",

  // ✅ Motivo/Causa (CSV)
  RETURN_MOTIVOS = "Producto vencido,Producto dañado,Error de despacho,Cliente rechazó,Otro",
  RETURN_CAUSAS = "Empaque roto,Fuga,Sin rótulo,Fecha corta,Pedido incorrecto,Otro",
} = process.env;

/* =========================================================
   CORS
========================================================= */
app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? true : CORS_ORIGIN,
    credentials: false,
  })
);

/* =========================================================
   DB (Postgres / Supabase)
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

  // Users
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

  // ✅ Return requests header
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS return_requests (
      req_num BIGSERIAL PRIMARY KEY,
      req_date DATE,
      usuario TEXT,
      full_name TEXT DEFAULT '',
      province TEXT DEFAULT '',
      warehouse TEXT DEFAULT '',
      card_code TEXT DEFAULT '',
      card_name TEXT DEFAULT '',
      motivo TEXT NOT NULL,
      causa TEXT NOT NULL,
      comments TEXT DEFAULT '',
      status TEXT DEFAULT 'Open',
      lines_count INT DEFAULT 0,
      total_qty NUMERIC(19,6) DEFAULT 0,
      total_amount NUMERIC(19,6) DEFAULT 0,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  // ✅ Return request lines
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS return_request_lines (
      id BIGSERIAL PRIMARY KEY,
      req_num BIGINT NOT NULL REFERENCES return_requests(req_num) ON DELETE CASCADE,
      item_code TEXT NOT NULL,
      item_desc TEXT DEFAULT '',
      qty NUMERIC(19,6) DEFAULT 0,
      price NUMERIC(19,6) DEFAULT 0,
      amount NUMERIC(19,6) DEFAULT 0,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_ret_date ON return_requests(req_date);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_ret_user ON return_requests(usuario);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_ret_wh ON return_requests(warehouse);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_ret_card ON return_requests(card_code);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_ret_lines_req ON return_request_lines(req_num);`);
}

/* =========================================================
   Time helpers (Panamá UTC-5)
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

/* =========================================================
   Warehouse mapping + override
========================================================= */
function provinceToWarehouse(province) {
  const p = String(province || "").trim().toLowerCase();
  if (p === "chiriquí" || p === "chiriqui" || p === "bocas del toro") return "200";
  if (p === "veraguas" || p === "coclé" || p === "cocle" || p === "los santos" || p === "herrera") return "500";
  if (p === "panamá" || p === "panama" || p === "panamá oeste" || p === "panama oeste" || p === "colón" || p === "colon")
    return "300";
  if (p === "rci") return "01";
  return SAP_WAREHOUSE || "300";
}

function parseCsvSet(str) {
  return new Set(
    String(str || "")
      .split(",")
      .map((x) => x.trim().toLowerCase())
      .filter(Boolean)
  );
}
const ADMIN_FREE_WHS_SET = parseCsvSet(ADMIN_FREE_WHS_USERS);

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

  const whHeader = String(req.headers["x-warehouse"] || "").trim();
  if (whHeader) return whHeader;

  const whQuery = String(req.query?.warehouse || req.query?.wh || "").trim();
  if (whQuery) return whQuery;

  return "";
}

function getWarehouseFromReq(req) {
  if (req.user && canOverrideWarehouse(req)) {
    const requested = getRequestedWarehouse(req);
    if (requested) return requested;
  }
  return getWarehouseFromUserToken(req);
}

/* =========================================================
   Allowed items by warehouse (200/300/500)
========================================================= */
function parseCodesEnv(str) {
  return String(str || "").split(",").map((x) => x.trim()).filter(Boolean);
}

const ALLOWED_BY_WH = {
  "200": parseCodesEnv(ACTIVE_CODES_200),
  "300": parseCodesEnv(ACTIVE_CODES_300),
  "500": parseCodesEnv(ACTIVE_CODES_500),
};

function isRestrictedWarehouse(wh) {
  return wh === "200" || wh === "300" || wh === "500";
}

function getAllowedSetForWh(wh) {
  if (!isRestrictedWarehouse(wh)) return null;
  const arr = Array.isArray(ALLOWED_BY_WH[wh]) ? ALLOWED_BY_WH[wh] : [];
  return new Set(arr.map((x) => String(x).trim()));
}

function assertItemAllowedOrThrow(wh, itemCode) {
  const code = String(itemCode || "").trim();
  if (!code) throw new Error("ItemCode vacío");

  if (!isRestrictedWarehouse(wh)) return true;

  const set = getAllowedSetForWh(wh);
  // si no configuraste lista, no bloqueamos
  if (!set || set.size === 0) return true;

  if (!set.has(code)) throw new Error(`ItemCode no permitido en bodega ${wh}: ${code}`);
  return true;
}

/* =========================================================
   Auth helpers
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

/* =========================================================
   SAP Service Layer (Session cookie)
========================================================= */
function missingSapEnv() {
  return !SAP_BASE_URL || !SAP_COMPANYDB || !SAP_USER || !SAP_PASS;
}

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
   SAP: PRICE LIST cache + ITEM cache
========================================================= */
let PRICE_LIST_CACHE = { name: "", no: null, ts: 0 };
const PRICE_LIST_TTL_MS = 6 * 60 * 60 * 1000;

const ITEM_CACHE = new Map();
const ITEM_TTL_MS = 10 * 60 * 1000;

async function getPriceListNoByNameCached(name) {
  const now = Date.now();
  if (PRICE_LIST_CACHE.name === name && PRICE_LIST_CACHE.no !== null && now - PRICE_LIST_CACHE.ts < PRICE_LIST_TTL_MS) {
    return PRICE_LIST_CACHE.no;
  }

  const safe = name.replace(/'/g, "''");
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
    itemFull = await slFetch(`/Items('${encodeURIComponent(code)}')`);
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
   HEALTH
========================================================= */
app.get("/api/health", async (req, res) => {
  safeJson(res, 200, {
    ok: true,
    app: "devoluciones-pedidos-api",
    message: "✅ PRODIMA DEVOLUCIONES (PEDIDOS) API activa",
    db: hasDb() ? "on" : "off",
    sap: missingSapEnv() ? "missing" : "ok",
  });
});

/* =========================================================
   USER LOGIN (mercaderistas)
========================================================= */
async function handleUserLogin(req, res) {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const username = String(req.body?.username || req.body?.user || "").trim().toLowerCase();
    const pin = String(req.body?.pin || req.body?.pass || "").trim();

    if (!username || !pin) return safeJson(res, 400, { ok: false, message: "username y pin requeridos" });

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
    return safeJson(res, 500, { ok: false, message: e.message });
  }
}

app.post("/api/login", handleUserLogin);
app.post("/api/auth/login", handleUserLogin);

app.get("/api/me", verifyUser, async (req, res) => safeJson(res, 200, { ok: true, user: req.user }));
app.get("/api/auth/me", verifyUser, async (req, res) => safeJson(res, 200, { ok: true, user: req.user }));

/* =========================================================
   SAP endpoints (customers/items/warehouses)
========================================================= */
app.get("/api/sap/warehouses", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) {
      const fb = String(WAREHOUSE_FALLBACK || "300").split(",").map((x) => x.trim()).filter(Boolean);
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
      const fb = String(WAREHOUSE_FALLBACK || "300").split(",").map((x) => x.trim()).filter(Boolean);
      return res.json({ ok: true, warehouses: fb.map((w) => ({ WarehouseCode: w, WarehouseName: "" })) });
    }
    return res.json({ ok: true, warehouses });
  } catch (err) {
    return res.status(500).json({ ok: false, message: String(err.message || err) });
  }
});

app.get("/api/sap/items/search", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const q = String(req.query?.q || "").trim();
    const top = Math.min(Math.max(Number(req.query?.top || 15), 5), 50);
    if (q.length < 2) return res.json({ ok: true, q, results: [] });

    const warehouseCode = getWarehouseFromReq(req);
    const allowedSet = getAllowedSetForWh(warehouseCode);

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
    function isActiveSapItem(it) {
      const v = String(it?.Valid ?? "").toLowerCase();
      const f = String(it?.FrozenFor ?? "").toLowerCase();
      const validOk = !v || v.includes("tyes") || v === "yes" || v === "true";
      const frozenOk = !f || f.includes("tno") || f === "no" || f === "false";
      return validOk && frozenOk;
    }

    let filtered = values.filter((it) => it?.ItemCode).filter(isActiveSapItem);
    if (allowedSet && allowedSet.size > 0) filtered = filtered.filter((it) => allowedSet.has(String(it.ItemCode).trim()));

    const results = filtered.slice(0, top).map((it) => ({
      ItemCode: it.ItemCode,
      ItemName: it.ItemName,
      SalesUnit: "Caja",
    }));

    return res.json({ ok: true, q, warehouse: warehouseCode, results });
  } catch (err) {
    return res.status(500).json({ ok: false, message: err.message });
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
      priceList: SAP_PRICE_LIST,
      priceListNo,
      price: Number(r.price ?? 0),
      stock: r.stock,
      disponible: r?.stock?.available ?? null,
      enStock: r?.stock?.hasStock ?? null,
    });
  } catch (err) {
    return res.status(400).json({ ok: false, message: err.message });
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
    return res.status(500).json({ ok: false, message: err.message });
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
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ DEVOLUCIONES: Meta (Motivo/Causa)
========================================================= */
function parseCsvList(str) {
  return String(str || "").split(",").map(s => s.trim()).filter(Boolean);
}
const MOTIVOS = parseCsvList(RETURN_MOTIVOS);
const CAUSAS  = parseCsvList(RETURN_CAUSAS);

app.get("/api/returns/meta", verifyUser, async (req, res) => {
  return res.json({ ok: true, motivos: MOTIVOS, causas: CAUSAS });
});

/* =========================================================
   ✅ DEVOLUCIONES: Crear solicitud
   POST /api/returns
   Body: { cardCode, cardName?, whsCode?, motivo, causa, comments?, lines:[{itemCode, qty}] }
========================================================= */
async function runTx(fn) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const out = await fn(client);
    await client.query("COMMIT");
    return out;
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

function inAllowedListOrOther(value, allowedArr) {
  const v = String(value || "").trim();
  if (!v) return false;
  // si quieres permitir cualquier texto, comenta lo de abajo
  return allowedArr.includes(v) || v.toLowerCase() === "otro";
}

app.post("/api/returns", verifyUser, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const cardCode = String(req.body?.cardCode || "").trim();
    const cardName = String(req.body?.cardName || "").trim();
    const motivo = String(req.body?.motivo || "").trim();
    const causa  = String(req.body?.causa || "").trim();
    const comments = String(req.body?.comments || "").trim();
    const linesIn = Array.isArray(req.body?.lines) ? req.body.lines : [];

    if (!cardCode) return safeJson(res, 400, { ok: false, message: "cardCode requerido" });
    if (!motivo) return safeJson(res, 400, { ok: false, message: "Motivo requerido" });
    if (!causa)  return safeJson(res, 400, { ok: false, message: "Causa requerida" });

    // validación por lista (recomendado)
    if (!inAllowedListOrOther(motivo, MOTIVOS)) return safeJson(res, 400, { ok:false, message:"Motivo inválido" });
    if (!inAllowedListOrOther(causa, CAUSAS))   return safeJson(res, 400, { ok:false, message:"Causa inválida" });

    const warehouseCode = getWarehouseFromReq(req);

    const cleanLines = linesIn
      .map(l => ({
        itemCode: String(l?.itemCode || "").trim(),
        qty: Number(l?.qty || 0),
      }))
      .filter(x => x.itemCode && x.qty > 0);

    if (!cleanLines.length) return safeJson(res, 400, { ok: false, message: "Agrega al menos 1 línea válida (qty>0)" });

    for (const ln of cleanLines) assertItemAllowedOrThrow(warehouseCode, ln.itemCode);

    const usuario = String(req.user?.username || "").trim().toLowerCase();
    const full_name = String(req.user?.full_name || "").trim();
    const province = String(req.user?.province || "").trim();

    const reqDate = getDateISOInOffset(TZ_OFFSET_MIN);

    // si SAP está OK, traemos desc/precio y calculamos totales más exactos
    let priceListNo = null;
    const canSap = !missingSapEnv();
    if (canSap) {
      try { priceListNo = await getPriceListNoByNameCached(SAP_PRICE_LIST); } catch {}
    }

    const detailedLines = [];
    let totalQty = 0;
    let totalAmount = 0;

    for (const ln of cleanLines) {
      let desc = "";
      let price = 0;
      let available = null;

      if (canSap && priceListNo != null) {
        try {
          const it = await getOneItem(ln.itemCode, priceListNo, warehouseCode);
          desc = String(it?.item?.ItemName || "").trim();
          price = Number(it?.price ?? 0) || 0;
          available = it?.stock?.available ?? null;
        } catch {}
      }

      const qty = Number(ln.qty || 0);
      const amount = Number((qty * price).toFixed(2));

      totalQty += qty;
      totalAmount += amount;

      detailedLines.push({
        item_code: ln.itemCode,
        item_desc: desc || "",
        qty,
        price,
        amount,
        available,
      });
    }

    const linesCount = detailedLines.length;

    const created = await runTx(async (c) => {
      const headR = await c.query(
        `INSERT INTO return_requests(
          req_date, usuario, full_name, province, warehouse,
          card_code, card_name, motivo, causa, comments,
          status, lines_count, total_qty, total_amount, updated_at
        ) VALUES (
          $1,$2,$3,$4,$5,
          $6,$7,$8,$9,$10,
          'Open',$11,$12,$13,NOW()
        )
        RETURNING req_num`,
        [
          reqDate,
          usuario,
          full_name,
          province,
          warehouseCode,
          cardCode,
          cardName,
          motivo,
          causa,
          comments,
          linesCount,
          totalQty,
          totalAmount,
        ]
      );

      const reqNum = Number(headR.rows?.[0]?.req_num || 0);
      if (!reqNum) throw new Error("No se pudo generar req_num");

      for (const ln of detailedLines) {
        await c.query(
          `INSERT INTO return_request_lines(req_num,item_code,item_desc,qty,price,amount)
           VALUES ($1,$2,$3,$4,$5,$6)`,
          [reqNum, ln.item_code, ln.item_desc, ln.qty, ln.price, ln.amount]
        );
      }

      return { reqNum };
    });

    return safeJson(res, 200, {
      ok: true,
      message: "Solicitud creada",
      reqNum: created.reqNum,
      warehouse: warehouseCode,
      totals: {
        lines: linesCount,
        qty: Number(totalQty || 0),
        amount: Number(totalAmount || 0),
      },
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================================================
   START
========================================================= */
(async () => {
  try {
    await ensureDb();
    console.log(hasDb() ? "DB ready ✅" : "DB not configured (skipped init) ⚠️");
  } catch (e) {
    console.error("DB init error:", e.message);
  }

  app.listen(Number(PORT), () => console.log(`DEVOLUCIONES (PEDIDOS) API listening on :${PORT}`));
})();
