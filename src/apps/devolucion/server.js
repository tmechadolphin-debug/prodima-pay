import express from "express";
import cors from "cors";
import pg from "pg";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";

const { Pool } = pg;
const app = express();
app.use(express.json({ limit: "2mb" }));

/* =========================================================
   ENV
========================================================= */
const {
  PORT = 3000,
  CORS_ORIGIN = "*",
  DATABASE_URL = "",
  JWT_SECRET = "change_me",

  SAP_BASE_URL = "",
  SAP_COMPANYDB = "",
  SAP_USER = "",
  SAP_PASS = "",

  SAP_WAREHOUSE = "300",
  SAP_PRICE_LIST = "",
  SAP_RETURN_ENTITY = "ReturnRequests",
  SAP_WEB_CARD_CODE = "",

  SAP_RETURN_CAUSE_UDF = "",
  SAP_RETURN_MOTIVE_UDF = "",
  SAP_RETURN_LOCALNO_UDF = "",

  RETURN_CAUSAS = "Producto vencido,Producto dañado,Error de despacho,Cliente rechazó,Empaque defectuoso,Otro",
  RETURN_MOTIVOS = "Cambio,Crédito,Reposición,Revisión comercial,Revisión calidad,Otro",

  ACTIVE_CODES_01 = "",
  ACTIVE_CODES_200 = "",
  ACTIVE_CODES_300 = "",
  ACTIVE_CODES_500 = "",
  ADMIN_FREE_WHS_USERS = "soto,liliana,daniel11,respinosa,test",
  ALLOWED_STOCK_WH = "200,300,500,01",
  WAREHOUSE_FALLBACK = "200,300,500,01",
} = process.env;

/* =========================================================
   CORS
========================================================= */
app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? true : CORS_ORIGIN.split(",").map((x) => x.trim()).filter(Boolean),
    credentials: false,
    allowedHeaders: ["Content-Type", "Authorization", "x-warehouse"],
    methods: ["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
  })
);

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
      request_no TEXT UNIQUE NOT NULL,
      created_at TIMESTAMP DEFAULT NOW(),
      created_by_user_id INTEGER,
      created_by_username TEXT DEFAULT '',
      created_by_name TEXT DEFAULT '',
      province TEXT DEFAULT '',
      warehouse_code TEXT DEFAULT '',
      card_code TEXT NOT NULL,
      card_name TEXT DEFAULT '',
      causa TEXT NOT NULL,
      motivo TEXT NOT NULL,
      comments TEXT DEFAULT '',
      total_lines INTEGER DEFAULT 0,
      total_qty NUMERIC(19,6) DEFAULT 0,
      status TEXT DEFAULT 'CREATED',
      sap_entity TEXT DEFAULT '',
      sap_doc_entry BIGINT,
      sap_doc_num BIGINT,
      sap_payload JSONB,
      sap_response JSONB,
      raw_error TEXT DEFAULT ''
    );
  `);

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS return_request_lines (
      id BIGSERIAL PRIMARY KEY,
      request_id BIGINT NOT NULL REFERENCES return_requests(id) ON DELETE CASCADE,
      line_num INTEGER NOT NULL,
      item_code TEXT NOT NULL,
      item_name TEXT DEFAULT '',
      quantity NUMERIC(19,6) DEFAULT 0
    );
  `);

  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_rr_created_at ON return_requests(created_at);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_rr_user ON return_requests(created_by_username);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_rr_card ON return_requests(card_code);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_rr_wh ON return_requests(warehouse_code);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_rr_lines_request ON return_request_lines(request_id);`);
}

/* =========================================================
   HELPERS
========================================================= */
const TZ_OFFSET_MIN = -300; // Panamá UTC-5

function safeJson(res, status, obj) {
  return res.status(status).json(obj);
}

function getDateISOInOffset(offsetMin = 0) {
  const now = new Date();
  const ms = now.getTime() + now.getTimezoneOffset() * 60000 + Number(offsetMin) * 60000;
  const d = new Date(ms);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ");
}

function provinceToWarehouse(province) {
  const p = norm(province);
  if (p === "chiriqui" || p === "bocas del toro") return "200";
  if (p === "veraguas" || p === "cocle" || p === "los santos" || p === "herrera") return "500";
  if (p === "panama" || p === "panama oeste" || p === "colon") return "300";
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

function parseCodesEnv(str) {
  return String(str || "")
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}

function parseListEnv(str) {
  const raw = String(str || "").trim();
  if (!raw) return [];

  if ((raw.startsWith("[") && raw.endsWith("]")) || (raw.startsWith("{") && raw.endsWith("}"))) {
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) return parsed.map((x) => String(x).trim()).filter(Boolean);
    } catch {}
  }

  return raw
    .split(/\r?\n|;|,/)
    .map((x) => String(x).trim())
    .filter(Boolean);
}

const ADMIN_FREE_WHS_SET = parseCsvSet(ADMIN_FREE_WHS_USERS);
const ALLOWED_STOCK_WH_SET = parseCsvSet(ALLOWED_STOCK_WH);

const ALLOWED_BY_WH = {
  "01": parseCodesEnv(ACTIVE_CODES_01),
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
  return wh === "01" || wh === "200" || wh === "300" || wh === "500";
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
  if (!set || set.size === 0) return true;
  if (!set.has(code)) throw new Error(`ItemCode no permitido en bodega ${wh}: ${code}`);
  return true;
}

function buildRequestNo(id) {
  const ymd = getDateISOInOffset(TZ_OFFSET_MIN).replace(/-/g, "");
  return `SD-${ymd}-${String(id).padStart(6, "0")}`;
}

function buildComments({ requestNo, username, warehouse, causa, motivo, comments }) {
  const parts = [
    `[req:${requestNo}]`,
    `[user:${username || "unknown"}]`,
    `[wh:${warehouse || ""}]`,
    `[causa:${causa}]`,
    `[motivo:${motivo}]`,
  ];
  if (comments) parts.push(String(comments).trim());
  return parts.join(" ").slice(0, 254);
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

function missingSapEnv() {
  return !SAP_BASE_URL || !SAP_COMPANYDB || !SAP_USER || !SAP_PASS;
}

/* =========================================================
   SAP Service Layer
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
   ITEM HELPERS
========================================================= */
let PRICE_LIST_CACHE = { name: "", no: null, ts: 0 };
const PRICE_LIST_TTL_MS = 6 * 60 * 60 * 1000;
const ITEM_CACHE = new Map();
const ITEM_TTL_MS = 10 * 60 * 1000;

async function getPriceListNoByNameCached(name) {
  if (!name) return null;
  const now = Date.now();
  if (
    PRICE_LIST_CACHE.name === name &&
    PRICE_LIST_CACHE.no !== null &&
    now - PRICE_LIST_CACHE.ts < PRICE_LIST_TTL_MS
  ) {
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
  const key = `${code}::${warehouseCode}::${priceListNo || "noprice"}`;
  const cached = ITEM_CACHE.get(key);
  if (cached && now - cached.ts < ITEM_TTL_MS) return cached.data;

  let itemFull;
  try {
    itemFull = await slFetch(
      `/Items('${encodeURIComponent(code)}')?$select=ItemCode,ItemName,SalesUnit,InventoryItem,Valid,FrozenFor,ItemPrices,ItemWarehouseInfoCollection`
    );
  } catch {
    itemFull = await slFetch(`/Items('${encodeURIComponent(code)}')`);
  }

  if (!Array.isArray(itemFull?.ItemWarehouseInfoCollection) && ALLOWED_STOCK_WH_SET.has(String(warehouseCode).toLowerCase())) {
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
   LOGIN USER
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
       FROM app_users WHERE username=$1 LIMIT 1`,
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

/* =========================================================
   HEALTH + AUTH
========================================================= */
app.get("/api/health", async (req, res) => {
  safeJson(res, 200, {
    ok: true,
    app: "solicitud-devoluciones-api",
    message: "✅ PRODIMA SOLICITUDES DE DEVOLUCIÓN API activa",
    db: hasDb() ? "on" : "off",
    sap: missingSapEnv() ? "missing" : "ok",
    returnEntity: SAP_RETURN_ENTITY || "ReturnRequests",
    causes: parseListEnv(RETURN_CAUSAS).length,
    motives: parseListEnv(RETURN_MOTIVOS).length,
  });
});

app.post("/api/login", handleUserLogin);
app.post("/api/auth/login", handleUserLogin);
app.get("/api/me", verifyUser, async (req, res) => safeJson(res, 200, { ok: true, user: req.user }));
app.get("/api/auth/me", verifyUser, async (req, res) => safeJson(res, 200, { ok: true, user: req.user }));

app.get("/api/return/meta", verifyUser, async (req, res) => {
  return safeJson(res, 200, {
    ok: true,
    causas: parseListEnv(RETURN_CAUSAS),
    motivos: parseListEnv(RETURN_MOTIVOS),
  });
});

/* =========================================================
   SAP LOOKUPS
========================================================= */
app.get("/api/sap/warehouses", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) {
      const fb = String(WAREHOUSE_FALLBACK || "300")
        .split(",")
        .map((x) => x.trim())
        .filter(Boolean);
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
      const fb = String(WAREHOUSE_FALLBACK || "300")
        .split(",")
        .map((x) => x.trim())
        .filter(Boolean);
      return res.json({ ok: true, warehouses: fb.map((w) => ({ WarehouseCode: w, WarehouseName: "" })) });
    }

    return res.json({ ok: true, warehouses });
  } catch (err) {
    return res.status(500).json({ ok: false, message: String(err.message || err) });
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
      price: Number(r.price ?? 0),
      priceUnit: r.priceUnit,
      factorCaja: r.factorCaja,
      stock: r.stock,
      disponible: r?.stock?.available ?? null,
      enStock: r?.stock?.hasStock ?? null,
    });
  } catch (err) {
    return res.status(400).json({ ok: false, message: err.message });
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
        `/Items?$select=ItemCode,ItemName,SalesUnit,Valid,FrozenFor&$filter=${encodeURIComponent(
          `(contains(ItemCode,'${safe}') or contains(ItemName,'${safe}'))`
        )}&$orderby=ItemName asc&$top=${preTop}`
      );
    } catch {
      raw = await slFetch(
        `/Items?$select=ItemCode,ItemName,SalesUnit,Valid,FrozenFor&$filter=${encodeURIComponent(
          `(substringof('${safe}',ItemCode) or substringof('${safe}',ItemName))`
        )}&$orderby=ItemName asc&$top=${preTop}`
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
    if (allowedSet && allowedSet.size > 0) {
      filtered = filtered.filter((it) => allowedSet.has(String(it.ItemCode).trim()));
    }

    const results = filtered.slice(0, top).map((it) => ({
      ItemCode: it.ItemCode,
      ItemName: it.ItemName,
      SalesUnit: it.SalesUnit || "",
    }));

    return res.json({ ok: true, q, warehouse: warehouseCode, results });
  } catch (err) {
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   CREATE RETURN REQUEST
========================================================= */
app.post("/api/return-request", verifyUser, async (req, res) => {
  const client = await pool.connect();
  try {
    if (!hasDb()) return res.status(500).json({ ok: false, message: "DB no configurada" });
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const cardCode = String(req.body?.cardCode || SAP_WEB_CARD_CODE || "").trim();
    const causa = String(req.body?.causa || "").trim();
    const motivo = String(req.body?.motivo || "").trim();
    const comments = String(req.body?.comments || "").trim();
    const lines = Array.isArray(req.body?.lines) ? req.body.lines : [];
    const warehouseCode = getWarehouseFromReq(req);

    if (!cardCode) return res.status(400).json({ ok: false, message: "cardCode requerido." });
    if (!causa) return res.status(400).json({ ok: false, message: "causa requerida." });
    if (!motivo) return res.status(400).json({ ok: false, message: "motivo requerido." });
    if (!lines.length) return res.status(400).json({ ok: false, message: "lines requerido." });

    const cleanLines = lines
      .map((l, idx) => ({
        lineNum: idx,
        ItemCode: String(l.itemCode || l.ItemCode || "").trim(),
        ItemName: String(l.itemName || l.ItemName || "").trim(),
        Quantity: Number(l.qty || l.quantity || l.Quantity || 0),
      }))
      .filter((x) => x.ItemCode && x.Quantity > 0);

    if (!cleanLines.length) {
      return res.status(400).json({ ok: false, message: "No hay líneas válidas (qty>0)." });
    }

    for (const ln of cleanLines) assertItemAllowedOrThrow(warehouseCode, ln.ItemCode);

    const docDate = getDateISOInOffset(TZ_OFFSET_MIN);
    const username = req.user?.username || "unknown";
    const fullName = req.user?.full_name || username;

    const bp = await slFetch(
      `/BusinessPartners('${encodeURIComponent(cardCode)}')?$select=CardCode,CardName`
    );
    const cardName = String(bp?.CardName || "").trim();

    await client.query("BEGIN");

    const ins = await client.query(
      `INSERT INTO return_requests(
         request_no, created_by_user_id, created_by_username, created_by_name, province,
         warehouse_code, card_code, card_name, causa, motivo, comments,
         total_lines, total_qty, status, sap_entity
       ) VALUES (
         $1,$2,$3,$4,$5,
         $6,$7,$8,$9,$10,$11,
         $12,$13,$14,$15
       ) RETURNING id`,
      [
        `TMP-${Date.now()}`,
        req.user?.id || null,
        username,
        fullName,
        req.user?.province || "",
        warehouseCode,
        cardCode,
        cardName,
        causa,
        motivo,
        comments,
        cleanLines.length,
        cleanLines.reduce((a, b) => a + Number(b.Quantity || 0), 0),
        "CREATING",
        SAP_RETURN_ENTITY || "ReturnRequests",
      ]
    );

    const requestId = Number(ins.rows[0].id);
    const requestNo = buildRequestNo(requestId);

    await client.query(`UPDATE return_requests SET request_no=$1 WHERE id=$2`, [requestNo, requestId]);

    for (let i = 0; i < cleanLines.length; i++) {
      const ln = cleanLines[i];
      let itemName = ln.ItemName;
      if (!itemName) {
        try {
          const r = await getOneItem(ln.ItemCode, await getPriceListNoByNameCached(SAP_PRICE_LIST), warehouseCode);
          itemName = r?.item?.ItemName || "";
        } catch {}
      }

      await client.query(
        `INSERT INTO return_request_lines(request_id, line_num, item_code, item_name, quantity)
         VALUES($1,$2,$3,$4,$5)`,
        [requestId, i, ln.ItemCode, itemName || "", ln.Quantity]
      );
      ln.ItemName = itemName || "";
    }

    const sapPayload = {
      CardCode: cardCode,
      DocDate: docDate,
      DocDueDate: docDate,
      Comments: buildComments({ requestNo, username, warehouse: warehouseCode, causa, motivo, comments }),
      JournalMemo: `Solicitud devolución web ${requestNo}`.slice(0, 50),
      DocumentLines: cleanLines.map((ln) => ({
        ItemCode: ln.ItemCode,
        Quantity: ln.Quantity,
        WarehouseCode: warehouseCode,
      })),
    };

    if (SAP_RETURN_CAUSE_UDF) sapPayload[SAP_RETURN_CAUSE_UDF] = causa;
    if (SAP_RETURN_MOTIVE_UDF) sapPayload[SAP_RETURN_MOTIVE_UDF] = motivo;
    if (SAP_RETURN_LOCALNO_UDF) sapPayload[SAP_RETURN_LOCALNO_UDF] = requestNo;

    let created;
    let fallback = false;

    try {
      created = await slFetch(`/${SAP_RETURN_ENTITY || "ReturnRequests"}`, {
        method: "POST",
        body: JSON.stringify(sapPayload),
      });
    } catch (err1) {
      const msg1 = String(err1?.message || err1);
      const isNoMatch = msg1.includes("ODBC -2028") || msg1.toLowerCase().includes("no matching records found");
      if (!isNoMatch) throw err1;

      fallback = true;
      const fallbackPayload = {
        ...sapPayload,
        Comments: `${buildComments({ requestNo, username, warehouse: warehouseCode, causa, motivo, comments })} [wh_fallback:1]`.slice(0, 254),
        DocumentLines: cleanLines.map((ln) => ({
          ItemCode: ln.ItemCode,
          Quantity: ln.Quantity,
        })),
      };

      created = await slFetch(`/${SAP_RETURN_ENTITY || "ReturnRequests"}`, {
        method: "POST",
        body: JSON.stringify(fallbackPayload),
      });
    }

    await client.query(
      `UPDATE return_requests
       SET status=$1, sap_doc_entry=$2, sap_doc_num=$3, sap_payload=$4::jsonb, sap_response=$5::jsonb, raw_error=''
       WHERE id=$6`,
      [
        fallback ? "CREATED_FALLBACK" : "CREATED",
        created?.DocEntry || null,
        created?.DocNum || null,
        JSON.stringify(sapPayload),
        JSON.stringify(created || {}),
        requestId,
      ]
    );

    await client.query("COMMIT");

    return res.json({
      ok: true,
      message: fallback
        ? "Solicitud de devolución creada (fallback sin WarehouseCode por -2028)"
        : "Solicitud de devolución creada",
      requestId,
      requestNo,
      sapEntity: SAP_RETURN_ENTITY || "ReturnRequests",
      docEntry: created?.DocEntry || null,
      docNum: created?.DocNum || null,
      warehouse: warehouseCode,
      fallback,
    });
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch {}

    const msg = String(err?.message || err);
    const hint = msg.toLowerCase().includes("unrecognized resource path")
      ? `${msg}. Revisa SAP_RETURN_ENTITY. Para solicitud de devolución normalmente debe ser ReturnRequests.`
      : msg;
    return res.status(400).json({ ok: false, message: hint });
  } finally {
    client.release();
  }
});

app.get("/api/requests/my", verifyUser, async (req, res) => {
  try {
    if (!hasDb()) return res.status(500).json({ ok: false, message: "DB no configurada" });

    const r = await dbQuery(
      `SELECT
         id, request_no, created_at, warehouse_code, card_code, card_name,
         causa, motivo, total_lines, total_qty,
         status, sap_doc_entry, sap_doc_num
       FROM return_requests
       WHERE LOWER(created_by_username)=LOWER($1)
       ORDER BY id DESC
       LIMIT 100`,
      [req.user?.username || ""]
    );

    return res.json({ ok: true, requests: r.rows || [] });
  } catch (e) {
    return res.status(500).json({ ok: false, message: e.message || String(e) });
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

  app.listen(Number(PORT), () => {
    console.log(`SOLICITUD DEVOLUCION API listening on :${PORT}`);
  });
})();
