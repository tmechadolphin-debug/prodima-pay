// server.js (DEVOLUCIÓN NORMAL) — COMPLETO
// ESM: "type":"module"

import express from "express";
import cors from "cors";
import pg from "pg";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";

const { Pool } = pg;

const app = express();
app.use(express.json({ limit: "2mb" }));

/* =========================
   ENV (Render)
========================= */
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

  // listas permitidas por bodega (opcional)
  ACTIVE_CODES_200 = "",
  ACTIVE_CODES_300 = "",
  ACTIVE_CODES_500 = "",

  // usuarios que pueden elegir bodega libre
  ADMIN_FREE_WHS_USERS = "soto,liliana,daniel11,respinosa,test",

  // fallback bodegas (si SAP no responde)
  WAREHOUSE_FALLBACK = "200,300,500,01",

  // ✅ Motivo/Causa (para dropdowns)
  RETURN_MOTIVOS = "Producto vencido,Cliente rechazó,Producto dañado,Error de facturación,Otro",
  RETURN_CAUSAS = "Empaque roto,Pedido incorrecto,Producto incorrecto,Faltante,Otro",
} = process.env;

/* =========================
   CORS
========================= */
app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? true : CORS_ORIGIN,
    credentials: false,
  })
);

/* =========================
   DB
========================= */
const pool = new Pool({
  connectionString: DATABASE_URL || undefined,
  ssl:
    DATABASE_URL && DATABASE_URL.includes("sslmode")
      ? { rejectUnauthorized: false }
      : undefined,
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

  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_returns_date ON return_requests(doc_date);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_returns_user ON return_requests(usuario);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_returns_wh ON return_requests(warehouse);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_returns_card ON return_requests(card_code);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_return_lines_req ON return_lines(req_num);`);
}

/* =========================
   Helpers (Auth)
========================= */
function safeJson(res, status, obj) { res.status(status).json(obj); }
function signToken(payload, ttl = "12h") { return jwt.sign(payload, JWT_SECRET, { expiresIn: ttl }); }
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
function missingSapEnv() { return !SAP_BASE_URL || !SAP_COMPANYDB || !SAP_USER || !SAP_PASS; }

/* =========================
   Time helpers (Panamá UTC-5)
========================= */
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

/* =========================
   Warehouse map + override
========================= */
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
    String(str || "").split(",").map((x) => x.trim().toLowerCase()).filter(Boolean)
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
  return "";
}
function getWarehouseFromReq(req) {
  if (req.user && canOverrideWarehouse(req)) {
    const requested = getRequestedWarehouse(req);
    if (requested) return requested;
  }
  return getWarehouseFromUserToken(req);
}

/* =========================
   Allowed items by warehouse (opcional)
========================= */
function parseCodesEnv(str) {
  return String(str || "").split(",").map((x) => x.trim()).filter(Boolean);
}
const ALLOWED_BY_WH = {
  "200": parseCodesEnv(ACTIVE_CODES_200),
  "300": parseCodesEnv(ACTIVE_CODES_300),
  "500": parseCodesEnv(ACTIVE_CODES_500),
};
function isRestrictedWarehouse(wh) { return wh === "200" || wh === "300" || wh === "500"; }
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
  // si NO configuraste lista => NO bloqueamos
  if (!set || set.size === 0) return true;

  if (!set.has(code)) throw new Error(`ItemCode no permitido en bodega ${wh}: ${code}`);
  return true;
}

/* =========================
   SAP Service Layer (cookie)
========================= */
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

  const r = await fetch(url, {
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
      return slFetch(path, options);
    }
    throw new Error(`SAP error ${r.status}: ${data?.error?.message?.value || txt}`);
  }
  return data;
}

/* =========================
   PRICE LIST + ITEM (igual que pedidos)
========================= */
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

function buildItemResponse(itemFull, code, priceListNo, warehouseCode) {
  const item = {
    ItemCode: itemFull.ItemCode ?? code,
    ItemName: itemFull.ItemName ?? `Producto ${code}`,
    SalesUnit: itemFull.SalesUnit ?? "",
  };

  const priceUnit = getPriceFromPriceList(itemFull, priceListNo);
  const price = priceUnit != null ? priceUnit : 0;

  let warehouseRow = null;
  if (Array.isArray(itemFull?.ItemWarehouseInfoCollection)) {
    warehouseRow =
      itemFull.ItemWarehouseInfoCollection.find(
        (w) => String(w?.WarehouseCode || "").trim() === String(warehouseCode || "").trim()
      ) || null;
  }

  const onHand = warehouseRow?.InStock != null ? Number(warehouseRow.InStock) : null;
  const committed = warehouseRow?.Committed != null ? Number(warehouseRow.Committed) : null;
  let available = null;
  if (Number.isFinite(onHand) && Number.isFinite(committed)) available = onHand - committed;

  return {
    item,
    price,
    stock: {
      warehouse: warehouseCode,
      available: Number.isFinite(available) ? available : null,
    },
  };
}

async function getOneItem(code, priceListNo, warehouseCode) {
  const itemFull = await slFetch(
    `/Items('${encodeURIComponent(code)}')` +
      `?$select=ItemCode,ItemName,SalesUnit,ItemPrices,ItemWarehouseInfoCollection`
  );

  // si no vino warehouse info, intentamos colección
  if (!Array.isArray(itemFull?.ItemWarehouseInfoCollection)) {
    try {
      const whInfo = await slFetch(
        `/Items('${encodeURIComponent(code)}')/ItemWarehouseInfoCollection?$select=WarehouseCode,InStock,Committed`
      );
      if (Array.isArray(whInfo?.value)) itemFull.ItemWarehouseInfoCollection = whInfo.value;
    } catch {}
  }

  return buildItemResponse(itemFull, code, priceListNo, warehouseCode);
}

/* =========================
   HEALTH + META
========================= */
function parseCsvList(str) {
  return String(str || "").split(",").map((x) => x.trim()).filter(Boolean);
}
const MOTIVOS = parseCsvList(RETURN_MOTIVOS);
const CAUSAS  = parseCsvList(RETURN_CAUSAS);

app.get("/api/health", async (req, res) => {
  safeJson(res, 200, {
    ok: true,
    app: "devoluciones-api",
    message: "✅ PRODIMA DEVOLUCIONES API activa",
    db: hasDb() ? "on" : "off",
    sap: missingSapEnv() ? "missing" : "ok",
  });
});

app.get("/api/meta", verifyUser, async (req, res) => {
  return safeJson(res, 200, { ok: true, motivos: MOTIVOS, causas: CAUSAS });
});

/* =========================
   LOGIN USER
========================= */
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
app.post("/api/auth/login", handleUserLogin);

/* =========================
   SAP: Warehouses / Customers / Items
========================= */
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
    let filtered = values.filter((it) => it?.ItemCode);

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
    });
  } catch (err) {
    return res.status(400).json({ ok: false, message: err.message });
  }
});

/* =========================
   ✅ CREAR SOLICITUD DEVOLUCIÓN:
   SAP primero -> luego Supabase
========================= */
async function upsertReturnRequestHead(head) {
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
      Number(head.reqNum),
      Number(head.reqEntry || 0) || null,
      String(head.docDate || "").slice(0,10) || null,
      Number(head.docTime || 0) || 0,
      String(head.cardCode || ""),
      String(head.cardName || ""),
      String(head.usuario || ""),
      String(head.warehouse || ""),
      String(head.motivo || ""),
      String(head.causa || ""),
      Number(head.totalAmount || 0),
      Number(head.totalQty || 0),
      String(head.status || "Open"),
      String(head.comments || ""),
    ]
  );
}

async function upsertReturnLine(line) {
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
      Number(line.reqNum),
      String(line.docDate || "").slice(0,10) || null,
      String(line.itemCode || ""),
      String(line.itemDesc || ""),
      Number(line.qty || 0),
      Number(line.price || 0),
      Number(line.lineTotal || 0),
    ]
  );
}

async function createSapReturnRequest(payload) {
  // Intentamos varias entidades para ser robustos (depende de tu SAP)
  const tryPaths = [
    { path: "/ReturnsRequests", label: "ReturnsRequests" },
    { path: "/ReturnRequests",  label: "ReturnRequests"  },
    { path: "/ReturnsRequest",  label: "ReturnsRequest"  },
    { path: "/ReturnRequest",   label: "ReturnRequest"   },
    // fallback: crea devolución (documento) si tu SAP no tiene “request”
    { path: "/Returns",         label: "Returns (fallback)" },
  ];

  let lastErr = null;
  for (const t of tryPaths) {
    try {
      const created = await slFetch(t.path, { method:"POST", body: JSON.stringify(payload) });
      return { created, used: t.label };
    } catch (e) {
      lastErr = e;
      const msg = String(e?.message || e);
      // si es 404/405 o entidad no existe, intentamos la siguiente
      if (msg.includes("404") || msg.includes("Not Found") || msg.includes("405")) continue;
      // otros errores (validación SAP) => no seguir
      throw e;
    }
  }
  throw new Error(`No pude crear en SAP. Probé: ${tryPaths.map(x=>x.label).join(", ")}. Último error: ${String(lastErr?.message || lastErr)}`);
}

app.post("/api/sap/return-request", verifyUser, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok:false, message:"DB no configurada" });
    if (missingSapEnv()) return safeJson(res, 400, { ok:false, message:"Faltan variables SAP" });

    const cardCode = String(req.body?.cardCode || "").trim();
    const motivo = String(req.body?.motivo || "").trim();
    const causa  = String(req.body?.causa || "").trim();
    const commentsUser = String(req.body?.comments || "").trim();

    const wh = getWarehouseFromReq(req);

    const linesIn = Array.isArray(req.body?.lines) ? req.body.lines : [];
    const cleanLines = linesIn
      .map(l => ({
        ItemCode: String(l.itemCode || "").trim(),
        Quantity: Number(l.qty || 0),
        Price: Number(l.price || 0),
        ItemDesc: String(l.itemDesc || "").trim()
      }))
      .filter(x => x.ItemCode && x.Quantity > 0);

    if(!cardCode) return safeJson(res, 400, { ok:false, message:"cardCode requerido" });
    if(!motivo) return safeJson(res, 400, { ok:false, message:"Motivo requerido" });
    if(!causa)  return safeJson(res, 400, { ok:false, message:"Causa requerida" });
    if(!cleanLines.length) return safeJson(res, 400, { ok:false, message:"Agrega al menos 1 línea (qty>0)" });

    // Validar allowed items
    for(const ln of cleanLines) assertItemAllowedOrThrow(wh, ln.ItemCode);

    const docDate = getDateISOInOffset(TZ_OFFSET_MIN);
    const creator = String(req.user?.username || "unknown").trim().toLowerCase();

    // Guardamos tags en comments para trazabilidad
    const baseComments = [`[user:${creator}]`, `[wh:${wh}]`, `[motivo:${motivo}]`, `[causa:${causa}]`].join(" ");
    const finalComments = commentsUser ? `${baseComments} ${commentsUser}` : baseComments;

    const DocumentLines = cleanLines.map(ln => ({
      ItemCode: ln.ItemCode,
      Quantity: ln.Quantity,
      WarehouseCode: wh
    }));

    // Payload SAP
    const sapPayload = {
      CardCode: cardCode,
      DocDate: docDate,
      DocDueDate: docDate,
      Comments: finalComments,
      JournalMemo: "Solicitud de devolución web",
      DocumentLines
    };

    // 1) Crear en SAP
    const { created, used } = await createSapReturnRequest(sapPayload);

    const reqEntry = Number(created?.DocEntry || 0) || null;
    const reqNum   = Number(created?.DocNum || 0) || null;
    if(!reqNum) throw new Error(`SAP no devolvió DocNum. Respuesta: ${JSON.stringify(created).slice(0,300)}`);

    // 2) Guardar en Supabase (usando DocNum como req_num)
    const totalQty = cleanLines.reduce((a,b)=> a + Number(b.Quantity||0), 0);
    const totalAmount = cleanLines.reduce((a,b)=> a + (Number(b.Quantity||0) * Number(b.Price||0)), 0);

    await upsertReturnRequestHead({
      reqNum,
      reqEntry,
      docDate,
      docTime: 0,
      cardCode,
      cardName: String(req.body?.cardName || ""),
      usuario: creator,
      warehouse: wh,
      motivo,
      causa,
      totalAmount,
      totalQty,
      status: "Open",
      comments: commentsUser || "",
    });

    for(const ln of cleanLines){
      await upsertReturnLine({
        reqNum,
        docDate,
        itemCode: ln.ItemCode,
        itemDesc: ln.ItemDesc || "",
        qty: ln.Quantity,
        price: ln.Price,
        lineTotal: Number(ln.Quantity||0) * Number(ln.Price||0),
      });
    }

    return safeJson(res, 200, {
      ok: true,
      message: "Solicitud creada en SAP y guardada en Supabase",
      reqEntry,
      reqNum,
      warehouse: wh,
      sapEntityUsed: used,
    });

  } catch (e) {
    return safeJson(res, 500, { ok:false, message: e.message || String(e) });
  }
});

/* =========================
   START
========================= */
(async () => {
  try {
    await ensureDb();
    console.log(hasDb() ? "DB ready ✅" : "DB not configured (skipped init) ⚠️");
  } catch (e) {
    console.error("DB init error:", e.message);
  }

  app.listen(Number(PORT), () => console.log(`DEVOLUCIONES API listening on :${PORT}`));
})();
