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

  SAP_BASE_URL = "",
  SAP_COMPANYDB = "",
  SAP_USER = "",
  SAP_PASS = "",

  SAP_WAREHOUSE = "300",
  SAP_PRICE_LIST = "Lista de Precios 99 2018",

  // users con bodega libre
  ADMIN_FREE_WHS_USERS = "soto,liliana,daniel11,respinosa,test",
  // fallback bodegas
  WAREHOUSE_FALLBACK = "200,300,500,01",

    // ✅ listas de códigos permitidos por bodega (comma separated)
  ACTIVE_CODES_200 = "",
  ACTIVE_CODES_300 = "",
  ACTIVE_CODES_500 = "",

  // Motivos/Causas (CSV)
  RETURN_MOTIVOS = "Producto vencido,Cliente rechazó,Producto dañado,Error de facturación,Otro",
  RETURN_CAUSAS = "Empaque roto,Pedido incorrecto,Producto incorrecto,Faltante,Otro",

  // opcional: si lo pones, NO hace discover en metadata
  SAP_RETURN_ENTITY = "",
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
   DB
========================================================= */
const pool = new Pool({
  connectionString: DATABASE_URL || undefined,
  ssl: DATABASE_URL && DATABASE_URL.includes("sslmode") ? { rejectUnauthorized: false } : undefined,
});

function hasDb() { return Boolean(DATABASE_URL); }
async function dbQuery(text, params = []) { return pool.query(text, params); }

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

  // Solicitudes de devolución (cache DB)
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS return_requests (
      id BIGSERIAL PRIMARY KEY,
      req_num BIGINT UNIQUE NOT NULL,          -- SAP DocNum
      req_entry BIGINT,                         -- SAP DocEntry
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

  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_returns_date ON return_requests(doc_date);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_returns_user ON return_requests(usuario);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_returns_wh ON return_requests(warehouse);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_returns_card ON return_requests(card_code);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_return_lines_req ON return_lines(req_num);`);
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
   Warehouse map + override
========================================================= */
function parseCsvSet(str) {
  return new Set(
    String(str || "")
      .split(",")
      .map((x) => x.trim().toLowerCase())
      .filter(Boolean)
  );
}
const ADMIN_FREE_WHS_SET = parseCsvSet(ADMIN_FREE_WHS_USERS);

function provinceToWarehouse(province) {
  const p = String(province || "").trim().toLowerCase();
  if (p === "chiriquí" || p === "chiriqui" || p === "bocas del toro") return "200";
  if (p === "veraguas" || p === "coclé" || p === "cocle" || p === "los santos" || p === "herrera") return "500";
  if (p === "panamá" || p === "panama" || p === "panamá oeste" || p === "panama oeste" || p === "colón" || p === "colon")
    return "300";
  if (p === "rci") return "01";
  return SAP_WAREHOUSE || "300";
}

function canOverrideWarehouse(req) {
  const u = String(req.user?.username || "").trim().toLowerCase();
  return u && ADMIN_FREE_WHS_SET.has(u);
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
function getWarehouseFromUserToken(req) {
  const whToken = String(req.user?.warehouse_code || "").trim();
  if (whToken) return whToken;
  const prov = String(req.user?.province || "").trim();
  if (prov) return provinceToWarehouse(prov);
  return SAP_WAREHOUSE || "300";
}
function getWarehouseFromReq(req) {
  if (req.user && canOverrideWarehouse(req)) {
    const requested = getRequestedWarehouse(req);
    if (requested) return requested;
  }
  return getWarehouseFromUserToken(req);
}

/* =========================================================
   Auth helpers
========================================================= */
function safeJson(res, status, obj) { res.status(status).json(obj); }

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
   Motivos / Causas
========================================================= */
function parseCsvList(str) {
  return String(str || "")
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}
const MOTIVOS = parseCsvList(RETURN_MOTIVOS);
const CAUSAS = parseCsvList(RETURN_CAUSAS);

function assertMotivoCausa(motivo, causa) {
  const m = String(motivo || "").trim();
  const c = String(causa || "").trim();
  if (!m) throw new Error("Motivo es obligatorio");
  if (!c) throw new Error("Causa es obligatoria");
  // si deseas “bloquear” a listas exactas:
  if (MOTIVOS.length && !MOTIVOS.includes(m)) throw new Error("Motivo inválido (no está en lista)");
  if (CAUSAS.length && !CAUSAS.includes(c)) throw new Error("Causa inválida (no está en lista)");
}

/* =========================================================
   HEALTH
========================================================= */
function missingSapEnv() {
  return !SAP_BASE_URL || !SAP_COMPANYDB || !SAP_USER || !SAP_PASS;
}

app.get("/api/health", async (req, res) => {
  safeJson(res, 200, {
    ok: true,
    app: "devoluciones-pedidos-api",
    message: "✅ PRODIMA DEVOLUCIONES (PEDIDOS) API activa",
    db: hasDb() ? "on" : "off",
    sap: missingSapEnv() ? "missing" : "ok",
    warehouse_default: SAP_WAREHOUSE,
    motivos: MOTIVOS.length,
    causas: CAUSAS.length,
  });
});

/* =========================================================
   SAP Service Layer (cookie + fetch)
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

async function slFetchJson(path, options = {}) {
  if (missingSapEnv()) throw new Error("Missing SAP env");
  if (!SL_COOKIE || Date.now() - SL_COOKIE_AT > 25 * 60 * 1000) await slLogin();

  const base = SAP_BASE_URL.replace(/\/$/, "");
  const url = `${base}${path.startsWith("/") ? path : `/${path}`}`;

  const r = await httpFetch(url, {
    method: String(options.method || "GET").toUpperCase(),
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
      return slFetchJson(path, options);
    }
    throw new Error(`SAP error ${r.status}: ${data?.error?.message?.value || txt}`);
  }
  return data;
}

async function slFetchText(path, options = {}) {
  if (missingSapEnv()) throw new Error("Missing SAP env");
  if (!SL_COOKIE || Date.now() - SL_COOKIE_AT > 25 * 60 * 1000) await slLogin();

  const base = SAP_BASE_URL.replace(/\/$/, "");
  const url = `${base}${path.startsWith("/") ? path : `/${path}`}`;

  const r = await httpFetch(url, {
    method: String(options.method || "GET").toUpperCase(),
    headers: {
      Cookie: SL_COOKIE,
      ...(options.headers || {}),
    },
    body: options.body,
  });

  const txt = await r.text();
  if (!r.ok) {
    if (r.status === 401 || r.status === 403) {
      SL_COOKIE = "";
      await slLogin();
      return slFetchText(path, options);
    }
    throw new Error(`SAP text error ${r.status}: ${txt}`);
  }
  return txt;
}

/* =========================================================
   SAP: PriceList cache + Item helper (re-uso para total)
========================================================= */
let PRICE_LIST_CACHE = { name: "", no: null, ts: 0 };
const PRICE_LIST_TTL_MS = 6 * 60 * 60 * 1000;

async function getPriceListNoByNameCached(name) {
  const now = Date.now();
  if (PRICE_LIST_CACHE.name === name && PRICE_LIST_CACHE.no !== null && now - PRICE_LIST_CACHE.ts < PRICE_LIST_TTL_MS) {
    return PRICE_LIST_CACHE.no;
  }
  const safe = name.replace(/'/g, "''");
  let no = null;

  try {
    const r1 = await slFetchJson(`/PriceLists?$select=PriceListNo,PriceListName&$filter=PriceListName eq '${safe}'`);
    if (r1?.value?.length) no = r1.value[0].PriceListNo;
  } catch {}

  if (no === null) {
    try {
      const r2 = await slFetchJson(`/PriceLists?$select=PriceListNo,ListName&$filter=ListName eq '${safe}'`);
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

async function sapGetItemFull(code) {
  const safe = encodeURIComponent(code);
  try {
    return await slFetchJson(
      `/Items('${safe}')?$select=ItemCode,ItemName,SalesUnit,ItemPrices,ItemWarehouseInfoCollection&$expand=ItemUnitOfMeasurementCollection($select=UoMType,BaseQuantity,AlternateQuantity)`
    );
  } catch {
    return await slFetchJson(`/Items('${safe}')`);
  }
}

/* =========================================================
   ✅ Discover entity set para Return Request via $metadata
========================================================= */
let RETURN_ENTITY_CACHE = { name: "", ts: 0 };
const RETURN_ENTITY_TTL_MS = 6 * 60 * 60 * 1000;

async function discoverReturnEntitySet() {
  if (SAP_RETURN_ENTITY) return String(SAP_RETURN_ENTITY).trim();
  const now = Date.now();
  if (RETURN_ENTITY_CACHE.name && now - RETURN_ENTITY_CACHE.ts < RETURN_ENTITY_TTL_MS) return RETURN_ENTITY_CACHE.name;

  const xml = await slFetchText(`/$metadata`, { headers: { "Accept": "application/xml" } });

  const names = [];
  const re = /EntitySet\s+Name="([^"]+)"/g;
  let m;
  while ((m = re.exec(xml))) names.push(m[1]);

  const pick =
    names.find((n) => /return/i.test(n) && /request/i.test(n)) ||
    names.find((n) => /return/i.test(n) && /req/i.test(n)) ||
    names.find((n) => /return/i.test(n)) ||
    "";

  if (!pick) throw new Error("No pude descubrir el entity set de Return Request en $metadata");
  RETURN_ENTITY_CACHE = { name: pick, ts: now };
  return pick;
}

/* =========================================================
   USER LOGIN (igual)
========================================================= */
async function handleUserLogin(req, res) {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const username = String(req.body?.username || req.body?.user || "").trim().toLowerCase();
    const pin = String(req.body?.pin || req.body?.pass || "").trim();

    if (!username || !pin) return safeJson(res, 400, { ok: false, message: "username y pin requeridos" });

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
      try { await dbQuery(`UPDATE app_users SET warehouse_code=$1 WHERE id=$2`, [wh, u.id]); } catch {}
      u.warehouse_code = wh;
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

app.get("/api/auth/me", verifyUser, (req, res) => safeJson(res, 200, { ok: true, user: req.user }));

/* =========================================================
   Meta devoluciones
========================================================= */
app.get("/api/returns/meta", verifyUser, async (req, res) => {
  return safeJson(res, 200, { ok: true, motivos: MOTIVOS, causas: CAUSAS });
});

/* =========================================================
   SAP helpers (warehouses/customers/items) — igual que antes
========================================================= */
app.get("/api/sap/warehouses", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) {
      const fb = String(WAREHOUSE_FALLBACK || "300").split(",").map((x) => x.trim()).filter(Boolean);
      return res.json({ ok: true, warehouses: fb.map((w) => ({ WarehouseCode: w })) });
    }

    let raw;
    try {
      raw = await slFetchJson(`/Warehouses?$select=WarehouseCode,WarehouseName&$orderby=WarehouseCode asc&$top=200`);
    } catch {
      raw = await slFetchJson(`/Warehouses?$select=WhsCode,WhsName&$orderby=WhsCode asc&$top=200`);
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

app.get("/api/sap/customers/search", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const q = String(req.query?.q || "").trim();
    const top = Math.min(Math.max(Number(req.query?.top || 15), 5), 50);
    if (q.length < 2) return res.json({ ok: true, results: [] });

    const safe = q.replace(/'/g, "''");

    let r;
    try {
      r = await slFetchJson(
        `/BusinessPartners?$select=CardCode,CardName,Phone1,EmailAddress&$filter=contains(CardName,'${safe}') or contains(CardCode,'${safe}')&$orderby=CardName asc&$top=${top}`
      );
    } catch {
      r = await slFetchJson(
        `/BusinessPartners?$select=CardCode,CardName,Phone1,EmailAddress&$filter=substringof('${safe}',CardName) or substringof('${safe}',CardCode)&$orderby=CardName asc&$top=${top}`
      );
    }

    const values = Array.isArray(r?.value) ? r.value : [];
    return res.json({
      ok: true,
      q,
      results: values.map((x) => ({
        CardCode: x.CardCode,
        CardName: x.CardName,
        Phone1: x.Phone1 || "",
        EmailAddress: x.EmailAddress || "",
      })),
    });
  } catch (err) {
    return res.status(500).json({ ok: false, message: err.message });
  }
});

app.get("/api/sap/customer/:code", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const code = String(req.params.code || "").trim();
    if (!code) return res.status(400).json({ ok: false, message: "CardCode vacío." });

    const bp = await slFetchJson(
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

app.get("/api/sap/items/search", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const q = String(req.query?.q || "").trim();
    const top = Math.min(Math.max(Number(req.query?.top || 15), 5), 50);
    if (q.length < 2) return res.json({ ok: true, q, results: [] });

    const safe = q.replace(/'/g, "''");
    const preTop = Math.min(100, top * 5);

    let raw;
    try {
      raw = await slFetchJson(
        `/Items?$select=ItemCode,ItemName,SalesUnit,Valid,FrozenFor` +
          `&$filter=${encodeURIComponent(`(contains(ItemCode,'${safe}') or contains(ItemName,'${safe}'))`)}` +
          `&$orderby=ItemName asc&$top=${preTop}`
      );
    } catch {
      raw = await slFetchJson(
        `/Items?$select=ItemCode,ItemName,SalesUnit,Valid,FrozenFor` +
          `&$filter=${encodeURIComponent(`(substringof('${safe}',ItemCode) or substringof('${safe}',ItemName))`)}` +
          `&$orderby=ItemName asc&$top=${preTop}`
      );
    }

    const values = Array.isArray(raw?.value) ? raw.value : [];
    const results = values.slice(0, top).map((it) => ({
      ItemCode: it.ItemCode,
      ItemName: it.ItemName,
      SalesUnit: "Caja",
    }));
    return res.json({ ok: true, q, results });
  } catch (err) {
    return res.status(500).json({ ok: false, message: err.message });
  }
});

app.get("/api/sap/item/:code", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const code = String(req.params.code || "").trim();
    if (!code) return res.status(400).json({ ok: false, message: "ItemCode vacío." });

    const wh = getWarehouseFromReq(req);
    const priceListNo = await getPriceListNoByNameCached(SAP_PRICE_LIST);
    const itemFull = await sapGetItemFull(code);

    const unitPrice = getPriceFromPriceList(itemFull, priceListNo);
    const factor = getSalesUomFactor(itemFull);
    const priceCaja = unitPrice != null && factor != null ? unitPrice * factor : unitPrice;

    // stock por bodega (si está)
    let warehouseRow = null;
    if (Array.isArray(itemFull?.ItemWarehouseInfoCollection)) {
      warehouseRow = itemFull.ItemWarehouseInfoCollection.find(
        (w) => String(w?.WarehouseCode || "").trim() === String(wh || "").trim()
      ) || null;
    }
    const onHand = warehouseRow?.InStock != null ? Number(warehouseRow.InStock) : null;
    const committed = warehouseRow?.Committed != null ? Number(warehouseRow.Committed) : null;
    const available = Number.isFinite(onHand) && Number.isFinite(committed) ? onHand - committed : null;

    return res.json({
      ok: true,
      item: { ItemCode: itemFull.ItemCode ?? code, ItemName: itemFull.ItemName ?? `Producto ${code}` },
      warehouse: wh,
      priceList: SAP_PRICE_LIST,
      priceListNo,
      price: Number(priceCaja ?? 0),
      stock: { available },
    });
  } catch (err) {
    return res.status(400).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ CREATE RETURN REQUEST (SAP primero, luego Supabase)
   POST /api/returns/create
========================================================= */
async function upsertReturnRequest(r) {
  await dbQuery(
    `INSERT INTO return_requests(
      req_num, req_entry, doc_date, doc_time,
      card_code, card_name, usuario, warehouse, motivo, causa,
      total_amount, total_qty, status, comments, updated_at
    ) VALUES (
      $1,$2,$3,$4,
      $5,$6,$7,$8,$9,$10,
      $11,$12,$13,$14,NOW()
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
      status=COALESCE(return_requests.status,'Open'),
      comments=EXCLUDED.comments,
      updated_at=NOW()`,
    [
      Number(r.reqNum),
      Number(r.reqEntry || 0) || null,
      String(r.docDate || "").slice(0, 10) || null,
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

async function upsertReturnLine(ln) {
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
      Number(ln.reqNum),
      String(ln.docDate || "").slice(0, 10) || null,
      String(ln.itemCode || ""),
      String(ln.itemDesc || ""),
      Number(ln.qty || 0),
      Number(ln.price || 0),
      Number(ln.lineTotal || 0),
    ]
  );
}

app.post("/api/returns/create", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const cardCode = String(req.body?.cardCode || "").trim();
    const motivo = String(req.body?.motivo || "").trim();
    const causa = String(req.body?.causa || "").trim();
    const commentsRaw = String(req.body?.comments || "").trim();

    const lines = Array.isArray(req.body?.lines) ? req.body.lines : [];

    if (!cardCode) return safeJson(res, 400, { ok: false, message: "cardCode requerido" });
    if (!lines.length) return safeJson(res, 400, { ok: false, message: "lines requerido" });

    assertMotivoCausa(motivo, causa);

    const warehouse = getWarehouseFromReq(req);
    const creator = req.user?.username || "unknown";
    const docDate = getDateISOInOffset(TZ_OFFSET_MIN);

    const cleanLines = lines
      .map((l) => ({ ItemCode: String(l.itemCode || "").trim(), Quantity: Number(l.qty || 0) }))
      .filter((x) => x.ItemCode && x.Quantity > 0);

    if (!cleanLines.length) return safeJson(res, 400, { ok: false, message: "No hay líneas válidas (qty>0)." });

    // 1) Crear en SAP
    const entity = await discoverReturnEntitySet();

    const tag = [`[user:${creator}]`, `[wh:${warehouse}]`, `[motivo:${motivo}]`, `[causa:${causa}]`].join(" ");
    const sapComments = commentsRaw ? `${tag} ${commentsRaw}` : tag;

    const payloadSap = {
      CardCode: cardCode,
      DocDate: docDate,
      DocDueDate: docDate,
      Comments: sapComments,
      JournalMemo: "Solicitud de devolución (web)",
      DocumentLines: cleanLines.map((ln) => ({
        ItemCode: ln.ItemCode,
        Quantity: ln.Quantity,
        WarehouseCode: warehouse,
      })),
    };

    let created;
    try {
      created = await slFetchJson(`/${entity}`, { method: "POST", body: JSON.stringify(payloadSap) });
    } catch (e) {
      // fallback: algunos SL usan plural/singular distinto
      // (por si metadata no devolvió lo correcto o cambió)
      const fallbacks = ["ReturnRequest", "ReturnRequests", "ReturnsRequest", "ReturnsRequests"];
      let lastErr = e;
      for (const cand of fallbacks) {
        try {
          created = await slFetchJson(`/${cand}`, { method: "POST", body: JSON.stringify(payloadSap) });
          break;
        } catch (ee) {
          lastErr = ee;
        }
      }
      if (!created) throw lastErr;
    }

    const reqEntry = created?.DocEntry;
    const reqNum = created?.DocNum;
    if (!reqNum) throw new Error("SAP creó el documento, pero no devolvió DocNum");

    // 2) Guardar en Supabase (con totales calculados)
    let cardName = "";
    try {
      const bp = await slFetchJson(`/BusinessPartners('${encodeURIComponent(cardCode)}')?$select=CardName`);
      cardName = String(bp?.CardName || "").trim();
    } catch {}

    // totales: usamos price list para estimado
    const priceListNo = await getPriceListNoByNameCached(SAP_PRICE_LIST);

    let totalAmount = 0;
    let totalQty = 0;

    for (const ln of cleanLines) {
      totalQty += Number(ln.Quantity || 0);

      let itemDesc = "";
      let priceCaja = 0;
      try {
        const itemFull = await sapGetItemFull(ln.ItemCode);
        itemDesc = String(itemFull?.ItemName || "").trim();
        const unitPrice = getPriceFromPriceList(itemFull, priceListNo);
        const factor = getSalesUomFactor(itemFull);
        priceCaja = Number(unitPrice != null && factor != null ? unitPrice * factor : unitPrice || 0);
      } catch {}

      const lineTotal = Number((Number(ln.Quantity || 0) * Number(priceCaja || 0)).toFixed(6));
      totalAmount += lineTotal;

      if (hasDb()) {
        await upsertReturnLine({
          reqNum,
          docDate,
          itemCode: ln.ItemCode,
          itemDesc,
          qty: ln.Quantity,
          price: priceCaja,
          lineTotal,
        });
      }
    }

    if (hasDb()) {
      await upsertReturnRequest({
        reqNum,
        reqEntry,
        docDate,
        docTime: 0,
        cardCode,
        cardName,
        usuario: creator,
        warehouse,
        motivo,
        causa,
        totalAmount,
        totalQty,
        status: "Open",
        comments: commentsRaw,
      });
    }

    return safeJson(res, 200, {
      ok: true,
      message: "Solicitud creada en SAP y guardada en Supabase",
      reqNum,
      reqEntry,
      warehouse,
      savedDb: hasDb(),
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
  app.listen(Number(PORT), () => console.log(`DEVOLUCIONES PEDIDOS API listening on :${PORT}`));
})();
