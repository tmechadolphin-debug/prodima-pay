// src/apps/devolucion/server.js
import express from "express";
import cors from "cors";
import pg from "pg";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";

const { Pool } = pg;

const app = express();
app.use(express.json({ limit: "2mb" }));

/* =========================================================
   ✅ ENV (Render)
========================================================= */
const {
  PORT = 3000,
  CORS_ORIGIN = "*",

  DATABASE_URL = "",
  JWT_SECRET = "change_me",

  // SAP B1 Service Layer
  SAP_BASE_URL = "",
  SAP_COMPANYDB = "",
  SAP_USER = "",
  SAP_PASS = "",

  // Defaults
  SAP_WAREHOUSE = "300",
  SAP_PRICE_LIST = "Lista de Precios 99 2018",

  // ✅ Entidad de SAP para devoluciones:
  // - "Returns" (Devolución)
  // - "ReturnRequest" (Solicitud de devolución, si tu SL lo tiene)
  SAP_RETURN_ENTITY = "Returns",

  // ✅ Dimensión 1 requerida por tu cuenta 5101-01-01 (Distribution Rule)
  // PON AQUÍ EL CÓDIGO REAL DE DIM 1 EN SAP (Financials -> Cost Accounting -> Distribution Rules)
  SAP_DIM1_DEFAULT = "",
  SAP_DIM1_200 = "",
  SAP_DIM1_300 = "",
  SAP_DIM1_500 = "",
  SAP_DIM1_01 = "",

  // ✅ Listas desplegables (comma separated)
  // Ej: "Producto vencido,Empaque dañado,Error de despacho"
  RETURN_MOTIVOS = "",
  // Ej: "Cliente devolvió,Almacén,Transporte"
  RETURN_CAUSAS = "",

  // ✅ listas de códigos permitidos por bodega (comma separated)
  ACTIVE_CODES_200 = "",
  ACTIVE_CODES_300 = "",
  ACTIVE_CODES_500 = "",

  // ✅ usuarios que pueden elegir cualquier bodega (comma separated)
  ADMIN_FREE_WHS_USERS = "soto,liliana,daniel11,respinosa,test",

  // ✅ fallback bodegas (si SAP no responde)
  WAREHOUSE_FALLBACK = "200,300,500,01",
} = process.env;

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
   ✅ DB (Postgres / Supabase)
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

/**
 * ✅ Migración tolerante:
 * - Crea tablas si no existen
 * - Si existen pero les faltan columnas, las agrega (evita errores tipo "column doc_date does not exist")
 */
async function ensureDb() {
  if (!hasDb()) return;

  // Usuarios (compartida con pedidos)
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

  // Cache de devoluciones (header)
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS returns_cache (
      id BIGSERIAL PRIMARY KEY,
      doc_num BIGINT UNIQUE,
      doc_entry BIGINT,
      doc_date DATE,
      doc_time INT,
      card_code TEXT,
      card_name TEXT,
      usuario TEXT,
      warehouse TEXT,
      status TEXT,
      cancel_status TEXT,
      motivo TEXT,
      causa TEXT,
      comments TEXT,
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  // Agregar columnas faltantes sin romper si la tabla ya existía con otro esquema
  const alterCols = [
    `ALTER TABLE returns_cache ADD COLUMN IF NOT EXISTS doc_num BIGINT UNIQUE`,
    `ALTER TABLE returns_cache ADD COLUMN IF NOT EXISTS doc_entry BIGINT`,
    `ALTER TABLE returns_cache ADD COLUMN IF NOT EXISTS doc_date DATE`,
    `ALTER TABLE returns_cache ADD COLUMN IF NOT EXISTS doc_time INT`,
    `ALTER TABLE returns_cache ADD COLUMN IF NOT EXISTS card_code TEXT`,
    `ALTER TABLE returns_cache ADD COLUMN IF NOT EXISTS card_name TEXT`,
    `ALTER TABLE returns_cache ADD COLUMN IF NOT EXISTS usuario TEXT`,
    `ALTER TABLE returns_cache ADD COLUMN IF NOT EXISTS warehouse TEXT`,
    `ALTER TABLE returns_cache ADD COLUMN IF NOT EXISTS status TEXT`,
    `ALTER TABLE returns_cache ADD COLUMN IF NOT EXISTS cancel_status TEXT`,
    `ALTER TABLE returns_cache ADD COLUMN IF NOT EXISTS motivo TEXT`,
    `ALTER TABLE returns_cache ADD COLUMN IF NOT EXISTS causa TEXT`,
    `ALTER TABLE returns_cache ADD COLUMN IF NOT EXISTS comments TEXT`,
    `ALTER TABLE returns_cache ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()`,
  ];
  for (const q of alterCols) {
    try {
      await dbQuery(q);
    } catch {
      // tolerante
    }
  }

  // Cache de líneas (detalle)
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS return_lines_cache (
      id BIGSERIAL PRIMARY KEY,
      doc_num BIGINT,
      doc_date DATE,
      item_code TEXT NOT NULL,
      item_desc TEXT DEFAULT '',
      qty_requested NUMERIC(19,6) DEFAULT 0,
      updated_at TIMESTAMP DEFAULT NOW(),
      UNIQUE(doc_num, item_code)
    );
  `);

  const alterLineCols = [
    `ALTER TABLE return_lines_cache ADD COLUMN IF NOT EXISTS doc_num BIGINT`,
    `ALTER TABLE return_lines_cache ADD COLUMN IF NOT EXISTS doc_date DATE`,
    `ALTER TABLE return_lines_cache ADD COLUMN IF NOT EXISTS item_code TEXT`,
    `ALTER TABLE return_lines_cache ADD COLUMN IF NOT EXISTS item_desc TEXT DEFAULT ''`,
    `ALTER TABLE return_lines_cache ADD COLUMN IF NOT EXISTS qty_requested NUMERIC(19,6) DEFAULT 0`,
    `ALTER TABLE return_lines_cache ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()`,
  ];
  for (const q of alterLineCols) {
    try {
      await dbQuery(q);
    } catch {}
  }

  // Estado (last sync / etc.)
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS app_state (
      k TEXT PRIMARY KEY,
      v TEXT NOT NULL,
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  // Indexes tolerantes
  try {
    await dbQuery(`CREATE INDEX IF NOT EXISTS idx_returns_cache_date ON returns_cache(doc_date);`);
    await dbQuery(`CREATE INDEX IF NOT EXISTS idx_returns_cache_user ON returns_cache(usuario);`);
    await dbQuery(`CREATE INDEX IF NOT EXISTS idx_returns_cache_wh ON returns_cache(warehouse);`);
    await dbQuery(`CREATE INDEX IF NOT EXISTS idx_returns_cache_card ON returns_cache(card_code);`);
    await dbQuery(`CREATE INDEX IF NOT EXISTS idx_return_lines_doc ON return_lines_cache(doc_num);`);
  } catch {}
}

/* =========================================================
   ✅ Time helpers (Panamá UTC-5)
========================================================= */
const TZ_OFFSET_MIN = -300;

function getDateISOInOffset(offsetMin = 0) {
  const now = new Date();
  const ms =
    now.getTime() + now.getTimezoneOffset() * 60_000 + Number(offsetMin) * 60_000;
  const d = new Date(ms);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

/* =========================================================
   ✅ Warehouse map + override (igual a pedidos)
========================================================= */
function provinceToWarehouse(province) {
  const p = String(province || "").trim().toLowerCase();
  if (p === "chiriquí" || p === "chiriqui" || p === "bocas del toro") return "200";
  if (p === "veraguas" || p === "coclé" || p === "cocle" || p === "los santos" || p === "herrera")
    return "500";
  if (
    p === "panamá" ||
    p === "panama" ||
    p === "panamá oeste" ||
    p === "panama oeste" ||
    p === "colón" ||
    p === "colon"
  )
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

/* =========================================================
   ✅ Allowed items by warehouse (200/300/500)
========================================================= */
function parseCodesEnv(str) {
  return String(str || "")
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}

const STATIC_ALLOWED = { "200": [], "300": [], "500": [] };
const ALLOWED_BY_WH = {
  "200": parseCodesEnv(ACTIVE_CODES_200).length ? parseCodesEnv(ACTIVE_CODES_200) : STATIC_ALLOWED["200"],
  "300": parseCodesEnv(ACTIVE_CODES_300).length ? parseCodesEnv(ACTIVE_CODES_300) : STATIC_ALLOWED["300"],
  "500": parseCodesEnv(ACTIVE_CODES_500).length ? parseCodesEnv(ACTIVE_CODES_500) : STATIC_ALLOWED["500"],
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
  // ✅ si NO configuraste lista, no bloqueamos
  if (!set || set.size === 0) return true;

  if (!set.has(code)) throw new Error(`ItemCode no permitido en bodega ${wh}: ${code}`);
  return true;
}

/* =========================================================
   ✅ DIM1 (Distribution Rule) según bodega
   - Esto arregla el error: 540000062 ... needs DR assignment for dimension 1
========================================================= */
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
   ✅ Helpers auth
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

function missingSapEnv() {
  return !SAP_BASE_URL || !SAP_COMPANYDB || !SAP_USER || !SAP_PASS;
}

/* =========================================================
   ✅ HEALTH
========================================================= */
app.get("/api/health", async (req, res) => {
  safeJson(res, 200, {
    ok: true,
    app: "devoluciones-api",
    message: "✅ PRODIMA DEVOLUCIONES API activa",
    db: hasDb() ? "on" : "off",
    sap: missingSapEnv() ? "missing" : "ok",
    return_entity: SAP_RETURN_ENTITY,
    dim1_default_set: !!String(SAP_DIM1_DEFAULT || "").trim(),
    warehouse_default: SAP_WAREHOUSE,
    priceList: SAP_PRICE_LIST,
  });
});

/* =========================================================
   ✅ Opciones (Motivo / Causa) para dropdowns
========================================================= */
function parseCsvList(str) {
  return String(str || "")
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}
app.get("/api/returns/options", verifyUser, async (req, res) => {
  return safeJson(res, 200, {
    ok: true,
    motivos: parseCsvList(RETURN_MOTIVOS),
    causas: parseCsvList(RETURN_CAUSAS),
  });
});

/* =========================================================
   ✅ SAP Service Layer (Session cookie)
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
   ✅ USER LOGIN (mercaderistas)
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
   ✅ SAP endpoints mínimos (bodegas / clientes / items)
   (copiados de pedidos para que el front funcione igual)
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

/* =========================================================
   ✅ Guardar en Supabase SOLO si SAP creó OK
========================================================= */
async function upsertReturnCache(row) {
  if (!hasDb()) return;
  const r = row || {};
  await dbQuery(
    `INSERT INTO returns_cache(
      doc_num, doc_entry, doc_date, doc_time,
      card_code, card_name, usuario, warehouse,
      status, cancel_status, motivo, causa, comments, updated_at
    ) VALUES(
      $1,$2,$3,$4,
      $5,$6,$7,$8,
      $9,$10,$11,$12,$13,NOW()
    )
    ON CONFLICT (doc_num) DO UPDATE SET
      doc_entry=EXCLUDED.doc_entry,
      doc_date=EXCLUDED.doc_date,
      doc_time=EXCLUDED.doc_time,
      card_code=EXCLUDED.card_code,
      card_name=EXCLUDED.card_name,
      usuario=EXCLUDED.usuario,
      warehouse=EXCLUDED.warehouse,
      status=EXCLUDED.status,
      cancel_status=EXCLUDED.cancel_status,
      motivo=EXCLUDED.motivo,
      causa=EXCLUDED.causa,
      comments=EXCLUDED.comments,
      updated_at=NOW()`,
    [
      r.docNum != null ? Number(r.docNum) : null,
      r.docEntry != null ? Number(r.docEntry) : null,
      r.docDate ? String(r.docDate).slice(0, 10) : null,
      Number(r.docTime || 0) || 0,
      String(r.cardCode || ""),
      String(r.cardName || ""),
      String(r.usuario || ""),
      String(r.warehouse || ""),
      String(r.status || ""),
      String(r.cancelStatus || ""),
      String(r.motivo || ""),
      String(r.causa || ""),
      String(r.comments || ""),
    ]
  );
}

async function upsertReturnLineCache(row) {
  if (!hasDb()) return;
  const r = row || {};
  await dbQuery(
    `INSERT INTO return_lines_cache(
      doc_num, doc_date, item_code, item_desc, qty_requested, updated_at
    ) VALUES ($1,$2,$3,$4,$5,NOW())
    ON CONFLICT (doc_num, item_code) DO UPDATE SET
      doc_date=EXCLUDED.doc_date,
      item_desc=EXCLUDED.item_desc,
      qty_requested=EXCLUDED.qty_requested,
      updated_at=NOW()`,
    [
      r.docNum != null ? Number(r.docNum) : null,
      r.docDate ? String(r.docDate).slice(0, 10) : null,
      String(r.itemCode || ""),
      String(r.itemDesc || ""),
      Number(r.qtyRequested || 0),
    ]
  );
}

/* =========================================================
   ✅ CREAR SOLICITUD/DEVOLUCIÓN EN SAP + guardar en Supabase
   POST /api/sap/return
   body: { cardCode, motivo, causa, comments?, lines:[{itemCode, qty}] , whsCode? }
========================================================= */
function truncate(s, max = 240) {
  const x = String(s || "").trim();
  return x.length > max ? x.slice(0, max) : x;
}

app.post("/api/sap/return", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const cardCode = String(req.body?.cardCode || "").trim();
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
        Quantity: Number(l.qty || 0),
      }))
      .filter((x) => x.ItemCode && x.Quantity > 0);

    if (!cleanLines.length) return res.status(400).json({ ok: false, message: "No hay líneas válidas (qty>0)." });

    // validar allowed items por bodega
    for (const ln of cleanLines) assertItemAllowedOrThrow(warehouseCode, ln.ItemCode);

    // ✅ Dimensión 1 (Distribution Rule) obligatoria en tu SAP
    const dim1 = getDim1ForWh(warehouseCode);
    if (!dim1) {
      return res.status(400).json({
        ok: false,
        message:
          "SAP requiere Dimensión 1 (Distribution Rule) para esa cuenta. Configura SAP_DIM1_DEFAULT (o SAP_DIM1_300/200/500/01) con un código válido.",
      });
    }

    const DocumentLines = cleanLines.map((ln) => ({
      ItemCode: ln.ItemCode,
      Quantity: ln.Quantity,
      WarehouseCode: warehouseCode,

      // ✅ Dimensión 1
      CostingCode: dim1,
      // ✅ Dimensión 1 para COGS (clave para tu error 5101-01-01)
      COGSCostingCode: dim1,
    }));

    const docDate = getDateISOInOffset(TZ_OFFSET_MIN);
    const creator = String(req.user?.username || "unknown").trim();

    // guardamos motivo/causa en comments para auditoría
    const baseComments = [
      `[user:${creator}]`,
      `[wh:${warehouseCode}]`,
      `[motivo:${truncate(motivo, 60)}]`,
      `[causa:${truncate(causa, 60)}]`,
    ].join(" ");

    const Comments = truncate(`${baseComments}${extraComments ? " " + extraComments : ""}`, 240);

    const payload = {
      CardCode: cardCode,
      DocDate: docDate,
      DocDueDate: docDate,
      Comments,
      JournalMemo: "Solicitud devolución web",
      DocumentLines,
    };

    // ✅ 1) Crear en SAP (si falla, NO guardamos en Supabase)
    const entity = String(SAP_RETURN_ENTITY || "Returns").trim();
    const created = await slFetch(`/${entity}`, {
      method: "POST",
      body: JSON.stringify(payload),
    });

    const docEntry = created?.DocEntry ?? created?.docEntry ?? null;
    const docNum = created?.DocNum ?? created?.docNum ?? null;

    if (!docEntry && !docNum) {
      // SAP respondió raro: evitamos “guardar fantasma”
      return res.status(500).json({
        ok: false,
        message: "SAP creó pero no devolvió DocEntry/DocNum. No se guardó en Supabase.",
        raw: created,
      });
    }

    // ✅ 2) Guardar en Supabase (header + líneas)
    try {
      await upsertReturnCache({
        docNum,
        docEntry,
        docDate,
        docTime: 0,
        cardCode,
        cardName: "", // si quieres, puedes buscar en SAP el nombre
        usuario: creator,
        warehouse: warehouseCode,
        status: "Open",
        cancelStatus: "",
        motivo,
        causa,
        comments: Comments,
      });

      // guardamos líneas
      for (const ln of cleanLines) {
        await upsertReturnLineCache({
          docNum,
          docDate,
          itemCode: ln.ItemCode,
          itemDesc: "",
          qtyRequested: ln.Quantity,
        });
      }
    } catch (e) {
      // SAP sí creó, pero DB falló: devolvemos warning
      return res.status(200).json({
        ok: true,
        message: "✅ Creada en SAP, ⚠️ pero falló guardado en Supabase",
        docEntry,
        docNum,
        warehouse: warehouseCode,
        bodega: warehouseCode,
        warning: String(e?.message || e),
      });
    }

    return res.json({
      ok: true,
      message: "✅ Solicitud/Devolución creada en SAP y guardada en Supabase",
      docEntry,
      docNum,
      warehouse: warehouseCode,
      bodega: warehouseCode,
      dim1Used: dim1,
      entityUsed: entity,
    });
  } catch (err) {
    return res.status(400).json({ ok: false, message: String(err?.message || err) });
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
    console.log(hasDb() ? "DB ready ✅" : "DB not configured (skipped init) ⚠️");
  } catch (e) {
    console.error("DB init error:", e.message);
  }

  app.listen(Number(PORT), () => {
    console.log(`DEVOLUCIONES API listening on :${PORT}`);
  });
})();
