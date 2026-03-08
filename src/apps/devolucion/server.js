// src/apps/devolucion/server.js
import express from "express";
import cors from "cors";
import pg from "pg";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";

const { Pool } = pg;

const app = express();
app.use(express.json({ limit: "2mb" }));

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
  SAP_RETURN_ENTITY = "ReturnRequest",

  SAP_DIM1_DEFAULT = "",
  SAP_DIM1_200 = "",
  SAP_DIM1_300 = "",
  SAP_DIM1_500 = "",
  SAP_DIM1_01 = "",

  RETURN_MOTIVOS = "",
  RETURN_CAUSAS = "",

  ACTIVE_CODES_200 = "",
  ACTIVE_CODES_300 = "",
  ACTIVE_CODES_500 = "",

  ADMIN_FREE_WHS_USERS = "soto,liliana,daniel11,respinosa,test",
  WAREHOUSE_FALLBACK = "200,300,500,01",
} = process.env;

app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? true : CORS_ORIGIN,
    credentials: false,
  })
);

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

  const headerAlter = [
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
  ];
  for (const q of headerAlter) {
    try { await dbQuery(q); } catch {}
  }

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

  const lineAlter = [
    `ALTER TABLE return_lines ADD COLUMN IF NOT EXISTS req_num BIGINT`,
    `ALTER TABLE return_lines ADD COLUMN IF NOT EXISTS doc_date DATE`,
    `ALTER TABLE return_lines ADD COLUMN IF NOT EXISTS item_code TEXT`,
    `ALTER TABLE return_lines ADD COLUMN IF NOT EXISTS item_desc TEXT DEFAULT ''`,
    `ALTER TABLE return_lines ADD COLUMN IF NOT EXISTS qty NUMERIC(19,6) DEFAULT 0`,
    `ALTER TABLE return_lines ADD COLUMN IF NOT EXISTS price NUMERIC(19,6) DEFAULT 0`,
    `ALTER TABLE return_lines ADD COLUMN IF NOT EXISTS line_total NUMERIC(19,6) DEFAULT 0`,
    `ALTER TABLE return_lines ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()`,
  ];
  for (const q of lineAlter) {
    try { await dbQuery(q); } catch {}
  }

  try {
    await dbQuery(`CREATE UNIQUE INDEX IF NOT EXISTS idx_return_requests_req_num ON return_requests(req_num);`);
    await dbQuery(`CREATE INDEX IF NOT EXISTS idx_return_requests_date ON return_requests(doc_date);`);
    await dbQuery(`CREATE INDEX IF NOT EXISTS idx_return_requests_user ON return_requests(usuario);`);
    await dbQuery(`CREATE INDEX IF NOT EXISTS idx_return_requests_wh ON return_requests(warehouse);`);
    await dbQuery(`CREATE INDEX IF NOT EXISTS idx_return_requests_card ON return_requests(card_code);`);
    await dbQuery(`CREATE INDEX IF NOT EXISTS idx_return_lines_req ON return_lines(req_num);`);
  } catch {}
}

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
function parseCsvList(str) {
  return String(str || "")
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}
function parseCodesEnv(str) {
  return String(str || "")
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
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
function assertItemAllowedOrThrow(wh, itemCode) {
  const code = String(itemCode || "").trim();
  if (!code) throw new Error("ItemCode vacío");
  if (!isRestrictedWarehouse(wh)) return true;
  const set = getAllowedSetForWh(wh);
  if (!set || set.size === 0) return true;
  if (!set.has(code)) throw new Error(`ItemCode no permitido en bodega ${wh}: ${code}`);
  return true;
}
function filterItemsByWarehouse(warehouseCode, rows) {
  const set = getAllowedSetForWh(warehouseCode);
  if (!set || set.size === 0) return rows;
  return (rows || []).filter((x) => set.has(String(x.ItemCode || "").trim()));
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
function getSapServiceRoot() {
  let base = String(SAP_BASE_URL || "").trim().replace(/\/$/, "");
  if (!base) return "";
  if (!/\/b1s\/v[12]$/i.test(base)) base += "/b1s/v2";
  return base;
}
function missingSapEnv() {
  return !getSapServiceRoot() || !SAP_COMPANYDB || !SAP_USER || !SAP_PASS;
}
function stripOuterQuotes(s) {
  const x = String(s || "").trim();
  return x.replace(/^['"]|['"]$/g, "");
}
function quoteODataString(v) {
  return `'${String(v || "").replace(/'/g, "''")}'`;
}

app.get("/api/health", async (req, res) => {
  safeJson(res, 200, {
    ok: true,
    app: "devoluciones-api",
    message: "✅ PRODIMA DEVOLUCIONES API activa",
    db: hasDb() ? "on" : "off",
    sap: missingSapEnv() ? "missing" : "ok",
    return_entity: SAP_RETURN_ENTITY,
    sap_base_url: getSapServiceRoot() || "",
    dim1_default_set: !!String(SAP_DIM1_DEFAULT || "").trim(),
    warehouse_default: SAP_WAREHOUSE,
    priceList: SAP_PRICE_LIST,
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

let SL_COOKIE = "";
let SL_COOKIE_AT = 0;

async function slLogin() {
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
  try { data = JSON.parse(txt); } catch {}

  if (!r.ok) {
    throw new Error(`SAP login failed: HTTP ${r.status} ${data?.error?.message?.value || txt}`);
  }

  const rawCookie = r.headers.get("set-cookie") || "";
  const b1 = rawCookie.match(/B1SESSION=([^;]+)/i);
  const route = rawCookie.match(/ROUTEID=([^;]+)/i);
  const cookies = [];
  if (b1) cookies.push(`B1SESSION=${b1[1]}`);
  if (route) cookies.push(`ROUTEID=${route[1]}`);
  SL_COOKIE = cookies.join("; ");
  SL_COOKIE_AT = Date.now();
  return true;
}

async function slFetch(path, options = {}) {
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
    if (r.status === 401 || r.status === 403) {
      SL_COOKIE = "";
      await slLogin();
      return slFetch(path, options);
    }
    throw new Error(`SAP error ${r.status}: ${data?.error?.message?.value || txt}`);
  }

  return data;
}

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
        warehouse_code: u.warehouse_code || wh || "",
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

    const q = stripOuterQuotes(req.query?.q || "");
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
      `/BusinessPartners(${quoteODataString(code)})?$select=CardCode,CardName,Phone1,Phone2,EmailAddress,Address,City,Country,ZipCode`
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

function pickPriceFromItem(item) {
  const prices = Array.isArray(item?.ItemPrices)
    ? item.ItemPrices
    : Array.isArray(item?.ItemPrices?.value)
      ? item.ItemPrices.value
      : [];

  if (!prices.length) {
    return Number(item?.Price || 0) || 0;
  }

  const wanted = String(SAP_PRICE_LIST || "").trim();
  const wantedNorm = norm(wanted);
  const exact = prices.find((p) => {
    const listNum = String(p?.PriceList || p?.PriceListNo || p?.PriceListNum || "").trim();
    const listName = String(p?.PriceListName || p?.ListName || "").trim();
    return wanted && (listNum === wanted || norm(listName) === wantedNorm);
  });

  const row = exact || prices[0] || {};
  return Number(row?.Price ?? row?.CurrencyPrice ?? row?.UnitPrice ?? 0) || 0;
}
function pickStockFromItem(item, warehouseCode) {
  const rows = []
    .concat(Array.isArray(item?.ItemWarehouseInfoCollection) ? item.ItemWarehouseInfoCollection : [])
    .concat(Array.isArray(item?.WhsInfo) ? item.WhsInfo : [])
    .concat(Array.isArray(item?.Warehouses) ? item.Warehouses : []);

  const match = rows.find((x) => {
    const wh = String(x?.WarehouseCode || x?.WhsCode || x?.Code || "").trim();
    return wh === warehouseCode;
  });

  const src = match || item || {};
  const inStock = Number(src?.InStock ?? src?.OnHand ?? src?.QuantityOnStock ?? 0) || 0;
  const committed = Number(src?.Committed ?? src?.IsCommitedStock ?? 0) || 0;
  const ordered = Number(src?.Ordered ?? src?.OnOrder ?? 0) || 0;
  const available = Number.isFinite(inStock - committed) ? inStock - committed : inStock;

  return { inStock, committed, ordered, available };
}

app.get("/api/sap/items/search", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const q = stripOuterQuotes(req.query?.q || "");
    const top = Math.min(Math.max(Number(req.query?.top || 20), 5), 50);
    if (q.length < 2) return res.json({ ok: true, results: [] });

    const warehouseCode = getWarehouseFromReq(req);
    const safe = q.replace(/'/g, "''");
    let raw;

    try {
      raw = await slFetch(
        `/Items?$select=ItemCode,ItemName,ItemsGroupCode,InventoryItem&$filter=(contains(ItemName,'${safe}') or contains(ItemCode,'${safe}')) and InventoryItem eq 'tYES'&$orderby=ItemName asc&$top=${top}`
      );
    } catch {
      raw = await slFetch(
        `/Items?$select=ItemCode,ItemName,ItemsGroupCode,InventoryItem&$filter=(substringof('${safe}',ItemName) or substringof('${safe}',ItemCode)) and InventoryItem eq 'tYES'&$orderby=ItemName asc&$top=${top}`
      );
    }

    let values = Array.isArray(raw?.value) ? raw.value : [];
    values = filterItemsByWarehouse(warehouseCode, values);

    const results = values.map((x) => ({
      ItemCode: x.ItemCode,
      ItemName: x.ItemName,
    }));

    return res.json({ ok: true, q, results });
  } catch (err) {
    return res.status(500).json({ ok: false, message: err.message || String(err) });
  }
});

app.get("/api/sap/item/:code", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const itemCode = String(req.params.code || "").trim();
    if (!itemCode) return res.status(400).json({ ok: false, message: "ItemCode vacío." });

    const warehouseCode = getWarehouseFromReq(req);
    assertItemAllowedOrThrow(warehouseCode, itemCode);

    const item = await slFetch(`/Items(${quoteODataString(itemCode)})`);
    const price = pickPriceFromItem(item);
    const stock = pickStockFromItem(item, warehouseCode);

    return res.json({
      ok: true,
      item: {
        ItemCode: item.ItemCode,
        ItemName: item.ItemName,
      },
      price,
      stock,
      warehouse: warehouseCode,
    });
  } catch (err) {
    return res.status(500).json({ ok: false, message: err.message || String(err) });
  }
});

async function upsertReturnRequest(row) {
  if (!hasDb()) return;
  const r = row || {};
  await dbQuery(
    `INSERT INTO return_requests(
      req_num, req_entry, doc_date, doc_time,
      card_code, card_name, usuario, warehouse,
      motivo, causa, total_amount, total_qty,
      status, comments, updated_at
    ) VALUES(
      $1,$2,$3,$4,
      $5,$6,$7,$8,
      $9,$10,$11,$12,
      $13,$14,NOW()
    )
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
      r.reqNum != null ? Number(r.reqNum) : null,
      r.reqEntry != null ? Number(r.reqEntry) : null,
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

async function upsertReturnLine(row) {
  if (!hasDb()) return;
  const r = row || {};
  await dbQuery(
    `INSERT INTO return_lines(
      req_num, doc_date, item_code, item_desc, qty, price, line_total, updated_at
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
    ON CONFLICT (req_num, item_code) DO UPDATE SET
      doc_date=EXCLUDED.doc_date,
      item_desc=EXCLUDED.item_desc,
      qty=EXCLUDED.qty,
      price=EXCLUDED.price,
      line_total=EXCLUDED.line_total,
      updated_at=NOW()`,
    [
      r.reqNum != null ? Number(r.reqNum) : null,
      r.docDate ? String(r.docDate).slice(0, 10) : null,
      String(r.itemCode || ""),
      String(r.itemDesc || ""),
      Number(r.qty || 0),
      Number(r.price || 0),
      Number(r.lineTotal || 0),
    ]
  );
}

function truncate(s, max = 240) {
  const x = String(s || "").trim();
  return x.length > max ? x.slice(0, max) : x;
}

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

    const DocumentLines = cleanLines.map((ln) => ({
      ItemCode: ln.ItemCode,
      ItemDescription: ln.ItemDescription || undefined,
      Quantity: ln.Quantity,
      WarehouseCode: warehouseCode,
      UnitPrice: ln.Price || 0,
      Price: ln.Price || 0,
      CostingCode: dim1,
      COGSCostingCode: dim1,
    }));

    const docDate = getDateISOInOffset(TZ_OFFSET_MIN);
    const creator = String(req.user?.username || "unknown").trim();
    const baseComments = [
      `[user:${creator}]`,
      `[wh:${warehouseCode}]`,
      `[motivo:${truncate(motivo, 60)}]`,
      `[causa:${truncate(causa, 60)}]`,
    ].join(" ");
    const Comments = truncate(`${baseComments}${extraComments ? " " + extraComments : ""}`, 240);

    const entity = String(SAP_RETURN_ENTITY || "ReturnRequest").trim() || "ReturnRequest";
    const payload = {
      CardCode: cardCode,
      DocDate: docDate,
      DocDueDate: docDate,
      Comments,
      JournalMemo: entity === "ReturnRequest" ? "Solicitud devolución web" : "Devolución web",
      DocumentLines,
    };

    const created = await slFetch(`/${entity}`, {
      method: "POST",
      body: JSON.stringify(payload),
    });

    const reqEntry = created?.DocEntry ?? created?.docEntry ?? null;
    const reqNum = created?.DocNum ?? created?.docNum ?? null;

    if (!reqEntry && !reqNum) {
      return res.status(500).json({
        ok: false,
        message: "SAP respondió sin DocEntry/DocNum. No se guardó en base de datos.",
        raw: created,
      });
    }

    const totalQty = cleanLines.reduce((acc, ln) => acc + Number(ln.Quantity || 0), 0);
    const totalAmount = cleanLines.reduce((acc, ln) => acc + Number(ln.Quantity || 0) * Number(ln.Price || 0), 0);

    try {
      await upsertReturnRequest({
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
        status: "Open",
        comments: Comments,
      });

      for (const ln of cleanLines) {
        await upsertReturnLine({
          reqNum,
          docDate,
          itemCode: ln.ItemCode,
          itemDesc: ln.ItemDescription,
          qty: ln.Quantity,
          price: ln.Price,
          lineTotal: Number(ln.Quantity || 0) * Number(ln.Price || 0),
        });
      }
    } catch (e) {
      return res.status(200).json({
        ok: true,
        message: "✅ Creada en SAP, ⚠️ pero falló guardado en Supabase",
        reqEntry,
        reqNum,
        docEntry: reqEntry,
        docNum: reqNum,
        warehouse: warehouseCode,
        bodega: warehouseCode,
        warning: String(e?.message || e),
        entityUsed: entity,
        dim1Used: dim1,
      });
    }

    return res.json({
      ok: true,
      message: "✅ Solicitud creada en SAP y guardada en Supabase",
      reqEntry,
      reqNum,
      docEntry: reqEntry,
      docNum: reqNum,
      warehouse: warehouseCode,
      bodega: warehouseCode,
      dim1Used: dim1,
      entityUsed: entity,
    });
  } catch (err) {
    return res.status(400).json({ ok: false, message: String(err?.message || err) });
  }
}

app.post("/api/sap/return-request", verifyUser, createReturnRequestHandler);
app.post("/api/sap/return", verifyUser, createReturnRequestHandler);

process.on("unhandledRejection", (e) => console.error("unhandledRejection:", e));
process.on("uncaughtException", (e) => console.error("uncaughtException:", e));

(async () => {
  try {
    await ensureDb();
    console.log(hasDb() ? "DB ready ✅" : "DB not configured (skipped init) ⚠️");
  } catch (e) {
    console.error("DB init error:", e.message);
  }

  app.listen(Number(PORT), () => {
    console.log(`DEVOLUCIONES API listening on :${PORT}`);
  });
})();
