import express from "express";
import cors from "cors";
import pg from "pg";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";

const { Pool } = pg;

const app = express();
app.use(express.json({ limit: "2mb" }));

/* =========================================================
   ✅ ENV
========================================================= */
const SAP_BASE_URL = process.env.SAP_BASE_URL || "";
const SAP_COMPANYDB = process.env.SAP_COMPANYDB || "";
const SAP_USER = process.env.SAP_USER || "";
const SAP_PASS = process.env.SAP_PASS || "";

const SAP_WAREHOUSE = process.env.SAP_WAREHOUSE || "01";

const SAP_PRICE_LIST =
  process.env.SAP_PRICE_LIST || "Lista 02 Res. Com. Ind. Analitic";

const YAPPY_ALIAS = process.env.YAPPY_ALIAS || "@prodimasansae";
const CORS_ORIGIN = process.env.CORS_ORIGIN || "*";

const DATABASE_URL = process.env.DATABASE_URL || "";

const ADMIN_USER = process.env.ADMIN_USER || "PRODIMA";
const ADMIN_PASS = process.env.ADMIN_PASS || "ADMINISTRADOR";
const JWT_SECRET = process.env.JWT_SECRET || "prodima_change_this_secret";

const TZ_OFFSET_MIN = Number(process.env.TZ_OFFSET_MIN || -300);

/* =========================================================
   ✅ CORS
========================================================= */
app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? "*" : [CORS_ORIGIN],
    methods: ["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  })
);
app.options("*", cors());

/* =========================================================
   ✅ Provincias -> Bodegas
========================================================= */
function provinceToWarehouse(province) {
  const p = String(province || "").trim().toLowerCase();

  if (p === "chiriquí" || p === "chiriqui" || p === "bocas del toro") return "200";

  if (
    p === "veraguas" ||
    p === "coclé" ||
    p === "cocle" ||
    p === "los santos" ||
    p === "herrera"
  )
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

  if (p === "darién" || p === "darien") return "300";

  return SAP_WAREHOUSE || "01";
}

/* =========================================================
   ✅ DB Pool (Supabase)
========================================================= */
let pool = null;

function hasDb() {
  return !!DATABASE_URL;
}

function getPool() {
  if (!pool) {
    if (!DATABASE_URL) throw new Error("DATABASE_URL no está configurado.");

    pool = new Pool({
      connectionString: DATABASE_URL,
      ssl: { rejectUnauthorized: false },
      max: 6,
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 8_000,
    });

    pool.on("error", (err) => console.error("❌ DB pool error:", err.message));
  }
  return pool;
}

async function dbQuery(text, params = []) {
  const p = getPool();
  return p.query(text, params);
}

/* =========================================================
   ✅ Schema
========================================================= */
async function ensureSchema() {
  if (!hasDb()) {
    console.log("⚠️ DATABASE_URL no configurado (DB deshabilitada)");
    return;
  }

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS app_users (
      id BIGSERIAL PRIMARY KEY,
      username TEXT UNIQUE NOT NULL,
      full_name TEXT DEFAULT '',
      pin_hash TEXT NOT NULL,
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      province TEXT DEFAULT '',
      warehouse_code TEXT DEFAULT '',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS audit_events (
      id BIGSERIAL PRIMARY KEY,
      event_type TEXT NOT NULL,
      actor TEXT DEFAULT '',
      ip TEXT DEFAULT '',
      user_agent TEXT DEFAULT '',
      payload JSONB DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  try {
    await dbQuery(`ALTER TABLE app_users ADD COLUMN IF NOT EXISTS province TEXT DEFAULT '';`);
    await dbQuery(`ALTER TABLE app_users ADD COLUMN IF NOT EXISTS warehouse_code TEXT DEFAULT '';`);
  } catch {}

  console.log("✅ DB Schema OK");
}

async function audit(event_type, req, actor = "", payload = {}) {
  if (!hasDb()) return;
  try {
    await dbQuery(
      `INSERT INTO audit_events(event_type, actor, ip, user_agent, payload)
       VALUES ($1,$2,$3,$4,$5)`,
      [
        String(event_type || ""),
        String(actor || ""),
        String(req.headers["x-forwarded-for"] || req.socket?.remoteAddress || ""),
        String(req.headers["user-agent"] || ""),
        JSON.stringify(payload || {}),
      ]
    );
  } catch (e) {
    console.error("⚠️ audit insert error:", e.message);
  }
}

/* =========================================================
   ✅ JWT Helpers
========================================================= */
function signAdminToken() {
  return jwt.sign({ typ: "admin" }, JWT_SECRET, { expiresIn: "2h" });
}

function signUserToken(user) {
  return jwt.sign(
    {
      typ: "user",
      uid: user.id,
      username: user.username,
      full_name: user.full_name || "",
      province: user.province || "",
      warehouse_code: user.warehouse_code || "",
    },
    JWT_SECRET,
    { expiresIn: "30d" }
  );
}

function verifyAdmin(req, res, next) {
  try {
    const auth = String(req.headers.authorization || "");
    if (!auth.startsWith("Bearer "))
      return res.status(401).json({ ok: false, message: "Falta Bearer token" });

    const decoded = jwt.verify(auth.replace("Bearer ", "").trim(), JWT_SECRET);
    if (!decoded || decoded.typ !== "admin")
      return res.status(403).json({ ok: false, message: "Token inválido" });

    req.admin = decoded;
    next();
  } catch {
    return res.status(401).json({ ok: false, message: "Token expirado o inválido" });
  }
}

function verifyUser(req, res, next) {
  try {
    const auth = String(req.headers.authorization || "");
    if (!auth.startsWith("Bearer "))
      return res.status(401).json({ ok: false, message: "Falta Bearer token" });

    const decoded = jwt.verify(auth.replace("Bearer ", "").trim(), JWT_SECRET);
    if (!decoded || decoded.typ !== "user")
      return res.status(403).json({ ok: false, message: "Token inválido" });

    req.user = decoded;
    next();
  } catch {
    return res.status(401).json({ ok: false, message: "Token expirado o inválido" });
  }
}

function missingSapEnv() {
  return !SAP_BASE_URL || !SAP_COMPANYDB || !SAP_USER || !SAP_PASS;
}

function getWarehouseFromReq(req) {
  const wh = String(req.user?.warehouse_code || "").trim();
  return wh || SAP_WAREHOUSE || "01";
}

function parseUserFromComments(comments = "") {
  const m = String(comments).match(/\[user:([^\]]+)\]/i);
  return m ? String(m[1]).trim() : "";
}
function parseWhFromComments(comments = "") {
  const m = String(comments).match(/\[wh:([^\]]+)\]/i);
  return m ? String(m[1]).trim() : "";
}

/* =========================================================
   ✅ SAP: Cookie + Timeout + Retry
========================================================= */
let SL_COOKIE = null;
let SL_COOKIE_TIME = 0;

const SAP_TIMEOUT_MS = Number(process.env.SAP_TIMEOUT_MS || 12_000);
const SAP_RETRY_ON_NETWORK = 1;

function readSetCookies(res) {
  try {
    if (res?.headers?.getSetCookie) return res.headers.getSetCookie();
  } catch {}
  const sc = res?.headers?.get?.("set-cookie");
  return sc ? [sc] : [];
}

async function slLogin() {
  if (missingSapEnv()) return;

  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), SAP_TIMEOUT_MS);

  const res = await fetch(`${SAP_BASE_URL}/Login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      CompanyDB: SAP_COMPANYDB,
      UserName: SAP_USER,
      Password: SAP_PASS,
    }),
    signal: controller.signal,
  }).finally(() => clearTimeout(t));

  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Login SAP falló (${res.status}): ${txt}`);
  }

  const cookies = readSetCookies(res);
  if (!cookies || !cookies.length) throw new Error("No se recibió cookie del Service Layer.");

  SL_COOKIE = cookies
    .map((c) => String(c).split(";")[0])
    .filter(Boolean)
    .join("; ");

  SL_COOKIE_TIME = Date.now();
}

async function slFetch(path, options = {}) {
  if (!SL_COOKIE || Date.now() - SL_COOKIE_TIME > 25 * 60 * 1000) {
    await slLogin();
  }

  let lastErr = null;

  for (let attempt = 0; attempt <= SAP_RETRY_ON_NETWORK; attempt++) {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), SAP_TIMEOUT_MS);

    try {
      const res = await fetch(`${SAP_BASE_URL}${path}`, {
        ...options,
        headers: {
          "Content-Type": "application/json",
          Cookie: SL_COOKIE,
          ...(options.headers || {}),
        },
        signal: controller.signal,
      });

      const text = await res.text().catch(() => "");

      if (res.status === 401 || res.status === 403) {
        SL_COOKIE = null;
        await slLogin();
        continue;
      }

      let json;
      try {
        json = text ? JSON.parse(text) : {};
      } catch {
        json = { raw: text };
      }

      if (!res.ok) throw new Error(`SAP error ${res.status}: ${text}`);
      return json;
    } catch (e) {
      lastErr = e;
      if (attempt < SAP_RETRY_ON_NETWORK) continue;
      throw lastErr;
    } finally {
      clearTimeout(t);
    }
  }

  throw lastErr || new Error("SAP fetch error");
}

/* =========================================================
   ✅ FIX FECHA SAP
========================================================= */
function getDateISOInOffset(offsetMinutes = -300) {
  const now = new Date();
  const utcMs = now.getTime() + now.getTimezoneOffset() * 60000;
  const localMs = utcMs + offsetMinutes * 60000;
  const local = new Date(localMs);
  return local.toISOString().slice(0, 10);
}

/* =========================================================
   ✅ Health
========================================================= */
app.get("/api/health", async (req, res) => {
  res.json({
    ok: true,
    message: "✅ PRODIMA API activa",
    yappy: YAPPY_ALIAS,
    warehouse_default: SAP_WAREHOUSE,
    priceList: SAP_PRICE_LIST,
    db: hasDb() ? "on" : "off",
  });
});

/* =========================================================
   ✅ ADMIN LOGIN
========================================================= */
app.post("/api/admin/login", async (req, res) => {
  try {
    const user = String(req.body?.user || "").trim();
    const pass = String(req.body?.pass || "").trim();

    if (!user || !pass) return res.status(400).json({ ok: false, message: "user y pass requeridos" });

    if (user !== ADMIN_USER || pass !== ADMIN_PASS) {
      await audit("ADMIN_LOGIN_FAIL", req, user, { user });
      return res.status(401).json({ ok: false, message: "Credenciales inválidas" });
    }

    const token = signAdminToken();
    await audit("ADMIN_LOGIN_OK", req, user, { user });
    return res.json({ ok: true, token });
  } catch (e) {
    return res.status(500).json({ ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ ADMIN: USERS + CRUD (igual que tu base)
========================================================= */
let ALLOWED_USERS_CACHE = { ts: 0, set: null };
const ALLOWED_USERS_TTL_MS = 60_000;

async function getAllowedUsersSetCached() {
  if (!hasDb()) return new Set();
  const now = Date.now();
  if (ALLOWED_USERS_CACHE.set && now - ALLOWED_USERS_CACHE.ts < ALLOWED_USERS_TTL_MS) {
    return ALLOWED_USERS_CACHE.set;
  }
  const r = await dbQuery(`SELECT username FROM app_users;`);
  const set = new Set(
    (r.rows || [])
      .map((x) => String(x.username || "").trim().toLowerCase())
      .filter(Boolean)
  );
  ALLOWED_USERS_CACHE = { ts: now, set };
  return set;
}

app.get("/api/admin/users", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return res.status(500).json({ ok: false, message: "DB no configurada" });

    const r = await dbQuery(`
      SELECT id, username, full_name, is_active, province, warehouse_code, created_at
      FROM app_users
      ORDER BY created_at DESC;
    `);

    return res.json({ ok: true, users: r.rows || [] });
  } catch (e) {
    return res.status(500).json({ ok: false, message: e.message });
  }
});

app.post("/api/admin/users", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return res.status(500).json({ ok: false, message: "DB no configurada" });

    const username = String(req.body?.username || "").trim().toLowerCase();
    const fullName = String(req.body?.fullName || req.body?.full_name || "").trim();
    const pin = String(req.body?.pin || "").trim();
    const province = String(req.body?.province || "").trim();
    let warehouse_code = String(req.body?.warehouse_code || "").trim();

    if (!username) return res.status(400).json({ ok: false, message: "username requerido" });
    if (!pin || pin.length < 4) return res.status(400).json({ ok: false, message: "PIN mínimo 4" });

    if (!warehouse_code) warehouse_code = provinceToWarehouse(province);

    const pin_hash = await bcrypt.hash(pin, 10);

    const ins = await dbQuery(
      `
      INSERT INTO app_users(username, full_name, pin_hash, is_active, province, warehouse_code)
      VALUES ($1,$2,$3,TRUE,$4,$5)
      RETURNING id, username, full_name, is_active, province, warehouse_code, created_at;
      `,
      [username, fullName, pin_hash, province, warehouse_code]
    );

    ALLOWED_USERS_CACHE = { ts: 0, set: null };

    await audit("USER_CREATED", req, "ADMIN", { username, fullName, province, warehouse_code });
    return res.json({ ok: true, user: ins.rows[0] });
  } catch (e) {
    const msg = String(e.message || e);
    if (msg.includes("duplicate") || msg.includes("unique"))
      return res.status(400).json({ ok: false, message: "Ese username ya existe" });
    return res.status(500).json({ ok: false, message: msg });
  }
});

app.delete("/api/admin/users/:id", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return res.status(500).json({ ok: false, message: "DB no configurada" });

    const id = Number(req.params.id || 0);
    if (!id) return res.status(400).json({ ok: false, message: "id inválido" });

    const r = await dbQuery(`DELETE FROM app_users WHERE id = $1 RETURNING id, username;`, [id]);
    if (!r.rowCount) return res.status(404).json({ ok: false, message: "Usuario no encontrado" });

    ALLOWED_USERS_CACHE = { ts: 0, set: null };

    await audit("USER_DELETED", req, "ADMIN", { id, username: r.rows[0]?.username });
    return res.json({ ok: true, message: "Usuario eliminado" });
  } catch (e) {
    return res.status(500).json({ ok: false, message: e.message });
  }
});

app.patch("/api/admin/users/:id/toggle", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return res.status(500).json({ ok: false, message: "DB no configurada" });

    const id = Number(req.params.id || 0);
    if (!id) return res.status(400).json({ ok: false, message: "id inválido" });

    const r = await dbQuery(
      `
      UPDATE app_users
      SET is_active = NOT is_active
      WHERE id = $1
      RETURNING id, username, full_name, is_active, province, warehouse_code, created_at;
      `,
      [id]
    );

    if (!r.rowCount) return res.status(404).json({ ok: false, message: "Usuario no encontrado" });

    await audit("USER_TOGGLE", req, "ADMIN", { id });
    return res.json({ ok: true, user: r.rows[0] });
  } catch (e) {
    return res.status(500).json({ ok: false, message: e.message });
  }
});

app.get("/api/admin/audit", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return res.status(500).json({ ok: false, message: "DB no configurada" });

    const r = await dbQuery(`
      SELECT id, event_type, actor, ip, created_at, payload
      FROM audit_events
      ORDER BY created_at DESC
      LIMIT 200;
    `);

    return res.json({ ok: true, events: r.rows || [] });
  } catch (e) {
    return res.status(500).json({ ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ LOGIN USUARIOS
========================================================= */
app.post("/api/auth/login", async (req, res) => {
  try {
    if (!hasDb()) return res.status(500).json({ ok: false, message: "DB no configurada" });

    const username = String(req.body?.username || "").trim().toLowerCase();
    const pin = String(req.body?.pin || "").trim();
    if (!username || !pin) return res.status(400).json({ ok: false, message: "username y pin requeridos" });

    const r = await dbQuery(
      `SELECT id, username, full_name, pin_hash, is_active, province, warehouse_code
       FROM app_users WHERE username = $1 LIMIT 1;`,
      [username]
    );

    if (!r.rowCount) {
      await audit("USER_LOGIN_FAIL", req, username, { username, reason: "not_found" });
      return res.status(401).json({ ok: false, message: "Credenciales inválidas" });
    }

    const user = r.rows[0];
    if (!user.is_active) {
      await audit("USER_LOGIN_FAIL", req, username, { username, reason: "inactive" });
      return res.status(401).json({ ok: false, message: "Usuario desactivado" });
    }

    const okPin = await bcrypt.compare(pin, user.pin_hash);
    if (!okPin) {
      await audit("USER_LOGIN_FAIL", req, username, { username, reason: "bad_pin" });
      return res.status(401).json({ ok: false, message: "Credenciales inválidas" });
    }

    let wh = String(user.warehouse_code || "").trim();
    if (!wh) {
      wh = provinceToWarehouse(user.province || "");
      try {
        await dbQuery(`UPDATE app_users SET warehouse_code=$1 WHERE id=$2`, [wh, user.id]);
        user.warehouse_code = wh;
      } catch {}
    }

    const token = signUserToken(user);
    await audit("USER_LOGIN_OK", req, username, { username, province: user.province, warehouse_code: user.warehouse_code });

    return res.json({
      ok: true,
      token,
      user: {
        id: user.id,
        username: user.username,
        full_name: user.full_name || "",
        province: user.province || "",
        warehouse_code: user.warehouse_code || "",
      },
    });
  } catch (e) {
    return res.status(500).json({ ok: false, message: e.message });
  }
});

app.get("/api/auth/me", verifyUser, async (req, res) => {
  return res.json({ ok: true, user: req.user });
});

/* =========================================================
   ✅ PriceListNo cache
========================================================= */
let PRICE_LIST_CACHE = { name: "", no: null, ts: 0 };
const PRICE_LIST_TTL_MS = 6 * 60 * 60 * 1000;

async function getPriceListNoByNameCached(name) {
  const now = Date.now();
  if (
    PRICE_LIST_CACHE.name === name &&
    PRICE_LIST_CACHE.no !== null &&
    now - PRICE_LIST_CACHE.ts < PRICE_LIST_TTL_MS
  ) return PRICE_LIST_CACHE.no;

  const safe = name.replace(/'/g, "''");
  let no = null;

  try {
    const r1 = await slFetch(
      `/PriceLists?$select=PriceListNo,PriceListName&$filter=PriceListName eq '${safe}'`
    );
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

/* =========================================================
   ✅ CACHE + SINGLE-FLIGHT (CLAVE para que no tarde 1 minuto)
========================================================= */
function cacheGet(map, key, ttl) {
  const c = map.get(key);
  if (!c) return null;
  if (Date.now() - c.ts > ttl) return null;
  return c.data;
}
function cacheSet(map, key, data, maxSize = 5000) {
  map.set(key, { ts: Date.now(), data });
  if (map.size > maxSize) {
    const firstKey = map.keys().next().value;
    map.delete(firstKey);
  }
}

const ITEM_META_CACHE = new Map(); // code::priceListNo -> meta
const ITEM_STOCK_CACHE = new Map(); // code::wh -> stock
const INFLIGHT_ITEM = new Map(); // key -> promise

const ITEM_META_TTL_MS = 6 * 60 * 60 * 1000; // 6 horas
const ITEM_STOCK_TTL_MS = 10_000; // 10s

async function fetchItemMeta(code, priceListNo) {
  const key = `${code}::${priceListNo}`;
  const cached = cacheGet(ITEM_META_CACHE, key, ITEM_META_TTL_MS);
  if (cached) return cached;

  let itemFull;
  try {
    itemFull = await slFetch(
      `/Items('${encodeURIComponent(code)}')` +
        `?$select=ItemCode,ItemName,SalesUnit,InventoryItem,ItemPrices,SalesItemsPerUnit,SalesQtyPerPackUnit,SalesQtyPerPackage` +
        `&$expand=ItemUnitOfMeasurementCollection($select=UoMType,UoMCode,UoMEntry,BaseQuantity,AlternateQuantity)`
    );
  } catch {
    itemFull = await slFetch(`/Items('${encodeURIComponent(code)}')`);
  }

  const priceUnit = getPriceFromPriceList(itemFull, priceListNo);
  const factorCaja = getSalesUomFactor(itemFull);
  const priceCaja = priceUnit != null && factorCaja != null ? priceUnit * factorCaja : priceUnit;

  const meta = {
    item: {
      ItemCode: itemFull.ItemCode ?? code,
      ItemName: itemFull.ItemName ?? `Producto ${code}`,
      SalesUnit: itemFull.SalesUnit ?? "",
      InventoryItem: itemFull.InventoryItem ?? null,
    },
    price: priceCaja,
    priceUnit,
    factorCaja,
  };

  cacheSet(ITEM_META_CACHE, key, meta);
  return meta;
}

async function fetchItemStock(code, warehouseCode) {
  const key = `${code}::${warehouseCode}`;
  const cached = cacheGet(ITEM_STOCK_CACHE, key, ITEM_STOCK_TTL_MS);
  if (cached) return cached;

  const whSafe = String(warehouseCode).replace(/'/g, "''");

  let row = null;

  // ✅ Intento 1: navegación con filtro normal (sin double-encode)
  try {
    const r = await slFetch(
      `/Items('${encodeURIComponent(code)}')/ItemWarehouseInfoCollection` +
        `?$select=WarehouseCode,InStock,Committed,Ordered` +
        `&$filter=WarehouseCode eq '${whSafe}'&$top=1`
    );
    const values = Array.isArray(r?.value) ? r.value : [];
    row = values[0] || null;
  } catch {
    row = null;
  }

  // ✅ Fallback 2: si no soporta filtro en navegación, traer top y filtrar en JS
  if (!row) {
    try {
      const r2 = await slFetch(
        `/Items('${encodeURIComponent(code)}')/ItemWarehouseInfoCollection` +
          `?$select=WarehouseCode,InStock,Committed,Ordered&$top=50`
      );
      const values2 = Array.isArray(r2?.value) ? r2.value : [];
      row = values2.find((x) => String(x?.WarehouseCode || "") === String(warehouseCode)) || null;
    } catch {
      row = null;
    }
  }

  const onHand = row?.InStock != null ? Number(row.InStock) : null;
  const committed = row?.Committed != null ? Number(row.Committed) : null;
  const ordered = row?.Ordered != null ? Number(row.Ordered) : null;

  let available = null;
  if (Number.isFinite(onHand) && Number.isFinite(committed)) available = onHand - committed;

  const stock = {
    warehouse: warehouseCode,
    onHand: Number.isFinite(onHand) ? onHand : null,
    committed: Number.isFinite(committed) ? committed : null,
    ordered: Number.isFinite(ordered) ? ordered : null,
    available: Number.isFinite(available) ? available : null,
    hasStock: available != null ? available > 0 : null,
  };

  cacheSet(ITEM_STOCK_CACHE, key, stock);
  return stock;
}

/* =========================================================
   ✅ MEGA OPT: 1 SOLA LLAMADA (si SAP lo soporta)
========================================================= */
async function fetchItemAllInOne(code, priceListNo, warehouseCode) {
  const whSafe = String(warehouseCode).replace(/'/g, "''");
  const whTrim = String(warehouseCode || "").trim();

  // 1) Intento rápido: $expand con $filter interno (si SAP lo soporta)
  const pathFiltered =
    `/Items('${encodeURIComponent(code)}')` +
    `?$select=ItemCode,ItemName,SalesUnit,InventoryItem,ItemPrices,SalesItemsPerUnit,SalesQtyPerPackUnit,SalesQtyPerPackage` +
    `&$expand=` +
    `ItemUnitOfMeasurementCollection($select=UoMType,UoMCode,UoMEntry,BaseQuantity,AlternateQuantity),` +
    `ItemWarehouseInfoCollection(` +
      `$select=WarehouseCode,InStock,Committed,Ordered;` +
      `$filter=WarehouseCode eq '${whSafe}'` +
    `)`;

  let itemFull = await slFetch(pathFiltered);

  // Helper: buscar la fila de bodega correcta (como el código viejo)
// ✅ Seleccionar la fila correcta de la bodega (NO usar [0])
function pickWarehouseRow(itemFull, warehouseCode) {
  if (!Array.isArray(itemFull?.ItemWarehouseInfoCollection)) return null;

  return (
    itemFull.ItemWarehouseInfoCollection.find(
      (w) => String(w?.WarehouseCode || "").trim() === String(warehouseCode || "").trim()
    ) || null
  );
}


  let wrow = pickWarehouseRow(itemFull);

  // 2) Fallback: si no vino la bodega correcta (o vino vacío), traemos la colección completa
  //    (solo pasa cuando el Service Layer no soporta $filter dentro del $expand)
  if (!wrow) {
    const pathFullWarehouses =
      `/Items('${encodeURIComponent(code)}')` +
      `?$select=ItemCode,ItemName,SalesUnit,InventoryItem,ItemPrices,SalesItemsPerUnit,SalesQtyPerPackUnit,SalesQtyPerPackage,ItemWarehouseInfoCollection` +
      `&$expand=` +
      `ItemUnitOfMeasurementCollection($select=UoMType,UoMCode,UoMEntry,BaseQuantity,AlternateQuantity),` +
      `ItemWarehouseInfoCollection($select=WarehouseCode,InStock,Committed,Ordered)`;

    itemFull = await slFetch(pathFullWarehouses);
    wrow = pickWarehouseRow(itemFull);
  }

  // Precios / factor caja
  const priceUnit = getPriceFromPriceList(itemFull, priceListNo);
  const factorCaja = getSalesUomFactor(itemFull);
  const priceCaja = priceUnit != null && factorCaja != null ? priceUnit * factorCaja : priceUnit;

  // Stock (si no existe esa bodega, queda null)
  const onHand = wrow?.InStock != null ? Number(wrow.InStock) : null;
  const committed = wrow?.Committed != null ? Number(wrow.Committed) : null;
  const ordered = wrow?.Ordered != null ? Number(wrow.Ordered) : null;

  let available = null;
  if (Number.isFinite(onHand) && Number.isFinite(committed)) {
    available = onHand - committed; // ✅ DISPONIBLE REAL
  }

  return {
    item: {
      ItemCode: itemFull.ItemCode ?? code,
      ItemName: itemFull.ItemName ?? `Producto ${code}`,
      SalesUnit: itemFull.SalesUnit ?? "",
      InventoryItem: itemFull.InventoryItem ?? null,
    },
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

/* =========================================================
   ✅ getOneItem Optimizado + SINGLE-FLIGHT
========================================================= */
async function getOneItemOptimized(code, priceListNo, warehouseCode) {
  const inflightKey = `${code}::${priceListNo}::${warehouseCode}`;

  if (INFLIGHT_ITEM.has(inflightKey)) {
    return INFLIGHT_ITEM.get(inflightKey);
  }

  const p = (async () => {
    try {
      const r = await fetchItemAllInOne(code, priceListNo, warehouseCode);

      cacheSet(ITEM_META_CACHE, `${code}::${priceListNo}`, {
        item: r.item,
        price: r.price,
        priceUnit: r.priceUnit,
        factorCaja: r.factorCaja,
      });

      cacheSet(ITEM_STOCK_CACHE, `${code}::${warehouseCode}`, r.stock);

      return r;
    } catch {
      const [meta, stock] = await Promise.all([
        fetchItemMeta(code, priceListNo),
        fetchItemStock(code, warehouseCode),
      ]);
      return { ...meta, stock };
    }
  })();

  INFLIGHT_ITEM.set(inflightKey, p);

  try {
    return await p;
  } finally {
    INFLIGHT_ITEM.delete(inflightKey);
  }
}

/* =========================================================
   ✅ SAP: ITEM (rápido)  + ✅ INVENTARIO VISIBLE
========================================================= */
app.get("/api/sap/item/:code", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const code = String(req.params.code || "").trim();
    if (!code) return res.status(400).json({ ok: false, message: "ItemCode vacío." });

    const warehouseCode = getWarehouseFromReq(req);
    const priceListNo = await getPriceListNoByNameCached(SAP_PRICE_LIST);

    const r = await getOneItemOptimized(code, priceListNo, warehouseCode);

    // ✅ campos “visibles” para el frontend
    const disponible = r?.stock?.available ?? null;
    const enStock = r?.stock?.hasStock ?? null;

    return res.json({
      ok: true,
      item: r.item,
      warehouse: warehouseCode,
      bodega: warehouseCode,            // ✅ alias
      priceList: SAP_PRICE_LIST,
      priceListNo,
      price: Number(r.price ?? 0),
      priceUnit: r.priceUnit,
      factorCaja: r.factorCaja,
      uom: r.item?.SalesUnit || "Caja",
      stock: r.stock,                   // ✅ detalle
      disponible,                       // ✅ visible
      enStock,                          // ✅ visible
    });
  } catch (err) {
    console.error("❌ /api/sap/item:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ NUEVO: SEARCH ITEMS (autocomplete)
========================================================= */
const ITEM_SEARCH_CACHE = new Map();
const ITEM_SEARCH_TTL_MS = 20_000;

app.get("/api/sap/items/search", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const q = String(req.query?.q || "").trim();
    const top = Math.min(Math.max(Number(req.query?.top || 20), 5), 50);

    if (q.length < 2) return res.json({ ok: true, q, results: [] });

    const key = `${q.toLowerCase()}::${top}`;
    const cached = cacheGet(ITEM_SEARCH_CACHE, key, ITEM_SEARCH_TTL_MS);
    if (cached) return res.json({ ok: true, q, results: cached });

    const safe = q.replace(/'/g, "''");

    let r;
    try {
      r = await slFetch(
        `/Items?$select=ItemCode,ItemName,SalesUnit,InventoryItem` +
          `&$filter=(contains(ItemCode,'${safe}') or contains(ItemName,'${safe}'))` +
          `&$orderby=ItemName asc&$top=${top}`
      );
    } catch {
      r = await slFetch(
        `/Items?$select=ItemCode,ItemName,SalesUnit,InventoryItem` +
          `&$filter=substringof('${safe}',ItemCode) or substringof('${safe}',ItemName)` +
          `&$orderby=ItemName asc&$top=${top}`
      );
    }

    const values = Array.isArray(r?.value) ? r.value : [];
    const results = values.map((x) => ({
      ItemCode: x.ItemCode,
      ItemName: x.ItemName,
      SalesUnit: x.SalesUnit || "",
      InventoryItem: x.InventoryItem ?? null,
    }));

    cacheSet(ITEM_SEARCH_CACHE, key, results, 4000);

    return res.json({ ok: true, q, results });
  } catch (err) {
    console.error("❌ /api/sap/items/search:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ SAP: SEARCH CUSTOMERS
========================================================= */
const CUSTOMER_SEARCH_CACHE = new Map();
const CUSTOMER_SEARCH_TTL_MS = 20_000;

app.get("/api/sap/customers/search", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const q = String(req.query?.q || "").trim();
    const top = Math.min(Math.max(Number(req.query?.top || 15), 5), 50);

    if (q.length < 2) return res.json({ ok: true, results: [] });

    const cacheKey = `${q.toLowerCase()}::${top}`;
    const cached = cacheGet(CUSTOMER_SEARCH_CACHE, cacheKey, CUSTOMER_SEARCH_TTL_MS);
    if (cached) return res.json({ ok: true, q, results: cached });

    const safe = q.replace(/'/g, "''");

    let r;
    try {
      r = await slFetch(
        `/BusinessPartners?$select=CardCode,CardName,Phone1,EmailAddress` +
          `&$filter=CardType eq 'cCustomer' and (contains(CardName,'${safe}') or contains(CardCode,'${safe}'))` +
          `&$orderby=CardName asc&$top=${top}`
      );
    } catch (e) {
      r = await slFetch(
        `/BusinessPartners?$select=CardCode,CardName,Phone1,EmailAddress` +
          `&$filter=contains(CardName,'${safe}') or contains(CardCode,'${safe}')` +
          `&$orderby=CardName asc&$top=${top}`
      );
    }

    const values = Array.isArray(r?.value) ? r.value : [];
    const results = values.map((x) => ({
      CardCode: x.CardCode,
      CardName: x.CardName,
      Phone1: x.Phone1 || "",
      EmailAddress: x.EmailAddress || "",
    }));

    cacheSet(CUSTOMER_SEARCH_CACHE, cacheKey, results, 4000);

    return res.json({ ok: true, q, results });
  } catch (err) {
    console.error("❌ /api/sap/customers/search:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ SAP: CUSTOMER
========================================================= */
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
   ✅ ADMIN: HISTÓRICO / DASHBOARD (igual a tu lógica anterior)
========================================================= */
app.get("/api/admin/quotes", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const userFilter = String(req.query?.user || "").trim().toLowerCase();
    const clientFilter = String(req.query?.client || "").trim().toLowerCase();
    const from = String(req.query?.from || "").trim();
    const to = String(req.query?.to || "").trim();

    const limit = Math.min(Math.max(Number(req.query?.limit || req.query?.top || 500), 1), 500);
    const skip = Math.max(Number(req.query?.skip || 0), 0);

    const SAP_PAGE = 500;
    const MAX_PAGES = 80;

    const bpCache = new Map();
    async function getBPName(cardCode) {
      if (!cardCode) return "";
      if (bpCache.has(cardCode)) return bpCache.get(cardCode);
      try {
        const bp = await slFetch(`/BusinessPartners('${encodeURIComponent(cardCode)}')?$select=CardCode,CardName`);
        const name = String(bp?.CardName || "").trim();
        bpCache.set(cardCode, name);
        return name;
      } catch {
        bpCache.set(cardCode, "");
        return "";
      }
    }

    const filterParts = [];
    if (from) filterParts.push(`DocDate ge '${from}'`);
    if (to) filterParts.push(`DocDate le '${to}'`);
    const sapFilter = filterParts.length ? `&$filter=${encodeURIComponent(filterParts.join(" and "))}` : "";

    const SELECT = `DocEntry,DocNum,CardCode,CardName,DocTotal,DocDate,DocumentStatus,CancelStatus,Comments`;

    const need = skip + limit;
    const matched = [];

    let sapSkip = 0;
    for (let page = 0; page < MAX_PAGES && matched.length < need; page++) {
      const sap = await slFetch(
        `/Quotations?$select=${SELECT}&$orderby=DocDate desc&$top=${SAP_PAGE}&$skip=${sapSkip}${sapFilter}`
      );

      const values = Array.isArray(sap?.value) ? sap.value : [];
      if (!values.length) break;

      for (const q of values) {
        const fechaISO = String(q.DocDate || "").slice(0, 10);
        const usuario = parseUserFromComments(q.Comments || "");
        const cardCode = String(q.CardCode || "").trim();

        if (userFilter && !String(usuario || "").toLowerCase().includes(userFilter)) continue;

        let cardName = String(q.CardName || "").trim();
        if (!cardName) cardName = await getBPName(cardCode);

        if (clientFilter) {
          const cc = String(cardCode || "").toLowerCase();
          const cn = String(cardName || "").toLowerCase();
          if (!cc.includes(clientFilter) && !cn.includes(clientFilter)) continue;
        }

        const cancelStatus = String(q.CancelStatus || "").trim();
        const isCancelled = cancelStatus.toLowerCase() === "csyes";

        const estado = isCancelled
          ? "Cancelled"
          : q.DocumentStatus === "bost_Open"
          ? "Open"
          : q.DocumentStatus === "bost_Close"
          ? "Close"
          : String(q.DocumentStatus || "");

        matched.push({
          docEntry: q.DocEntry,
          docNum: q.DocNum,
          cardCode,
          cardName,
          montoCotizacion: Number(q.DocTotal || 0),
          montoEntregado: 0,
          fecha: fechaISO,
          estado,
          cancelStatus,
          isCancelled,
          usuario,
          comments: q.Comments || "",
        });

        if (matched.length >= need) break;
      }

      if (from) {
        const last = values[values.length - 1];
        const lastDate = String(last?.DocDate || "").slice(0, 10);
        if (lastDate && lastDate < from) break;
      }

      sapSkip += SAP_PAGE;
    }

    const pageRows = matched.slice(skip, skip + limit);
    return res.json({ ok: true, limit, skip, count: pageRows.length, quotes: pageRows });
  } catch (e) {
    return res.status(500).json({ ok: false, message: e.message });
  }
});

app.get("/api/admin/dashboard", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const scope = String(req.query?.scope || "created").trim().toLowerCase();
    const fromQ = String(req.query?.from || "").trim();
    const toQ = String(req.query?.to || "").trim();

    const from = fromQ || "2020-01-01";
    const to = toQ || "";

    const PAGE_SIZE = Math.min(Math.max(Number(req.query?.top || 500), 50), 500);
    const MAX_PAGES = Math.min(Math.max(Number(req.query?.maxPages || 20), 1), 80);

    let allowedUsersSet = null;
    if (scope === "created") allowedUsersSet = await getAllowedUsersSetCached();

    const filterParts = [];
    if (from) filterParts.push(`DocDate ge '${from}'`);
    if (to) filterParts.push(`DocDate le '${to}'`);
    const sapFilter = filterParts.length ? `&$filter=${encodeURIComponent(filterParts.join(" and "))}` : "";

    const SELECT =
      `DocEntry,DocNum,CardCode,CardName,DocTotal,DocDate,DocumentStatus,CancelStatus,Cancelled,Comments`;

    const all = [];
    let skip = 0;

    for (let page = 0; page < MAX_PAGES; page++) {
      const sap = await slFetch(
        `/Quotations?$select=${SELECT}&$orderby=DocDate desc&$top=${PAGE_SIZE}&$skip=${skip}${sapFilter}`
      );

      const values = Array.isArray(sap?.value) ? sap.value : [];
      if (!values.length) break;

      for (const q of values) {
        const fechaISO = String(q.DocDate || "").slice(0, 10);

        const usuario = parseUserFromComments(q.Comments || "");
        const usuarioKey = String(usuario || "").trim().toLowerCase();

        if (allowedUsersSet && scope === "created") {
          if (!usuarioKey || !allowedUsersSet.has(usuarioKey)) continue;
        }

        const warehouse = parseWhFromComments(q.Comments || "") || "sin_wh";

        const cancelStatus = String(q.CancelStatus || "").trim();
        const cancelledFlag = String(q.Cancelled || "").trim();
        const isCancelled =
          cancelStatus.toLowerCase() === "csyes" ||
          cancelledFlag.toLowerCase() === "tyes" ||
          cancelledFlag.toLowerCase() === "true";

        if (isCancelled) continue;

        const estado =
          q.DocumentStatus === "bost_Open"
            ? "Open"
            : q.DocumentStatus === "bost_Close"
            ? "Close"
            : String(q.DocumentStatus || "");

        all.push({
          docEntry: q.DocEntry,
          docNum: q.DocNum,
          cardCode: String(q.CardCode || "").trim(),
          cardName: String(q.CardName || "").trim(),
          montoCotizacion: Number(q.DocTotal || 0),
          montoEntregado: 0,
          fecha: fechaISO,
          estado,
          cancelStatus,
          usuario: usuario || "sin_user",
          warehouse,
        });
      }

      if (from) {
        const last = values[values.length - 1];
        const lastDate = String(last?.DocDate || "").slice(0, 10);
        if (lastDate && lastDate < from) break;
      }

      skip += PAGE_SIZE;
    }

    const sumCot = all.reduce((a, x) => a + (Number(x.montoCotizacion) || 0), 0);
    const sumEnt = all.reduce((a, x) => a + (Number(x.montoEntregado) || 0), 0);
    const fillRate = sumCot > 0 ? sumEnt / sumCot : 0;

    return res.json({
      ok: true,
      scope,
      from,
      to: to || null,
      fetched: all.length,
      kpis: {
        totalCotizaciones: all.length,
        montoCotizado: sumCot,
        montoEntregado: sumEnt,
        fillRate,
      },
    });
  } catch (e) {
    return res.status(500).json({ ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ CREAR COTIZACIÓN
========================================================= */
app.post("/api/sap/quote", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const cardCode = String(req.body?.cardCode || "").trim();
    const comments = String(req.body?.comments || "").trim();
    const lines = Array.isArray(req.body?.lines) ? req.body.lines : [];

    if (!cardCode) return res.status(400).json({ ok: false, message: "cardCode requerido." });
    if (!lines.length) return res.status(400).json({ ok: false, message: "lines requerido." });

    const warehouseCode = getWarehouseFromReq(req);

    const DocumentLines = lines
      .map((l) => ({
        ItemCode: String(l.itemCode || "").trim(),
        Quantity: Number(l.qty || 0),
        WarehouseCode: warehouseCode,
      }))
      .filter((x) => x.ItemCode && x.Quantity > 0);

    if (!DocumentLines.length) return res.status(400).json({ ok: false, message: "No hay líneas válidas." });

    const docDate = getDateISOInOffset(TZ_OFFSET_MIN);

    const creator = req.user?.username || "unknown";
    const province = String(req.user?.province || "").trim();

    const sapComments = [
      `[WEB PEDIDOS]`,
      `[user:${creator}]`,
      province ? `[prov:${province}]` : "",
      warehouseCode ? `[wh:${warehouseCode}]` : "",
      comments ? comments : "Cotización mercaderista",
    ]
      .filter(Boolean)
      .join(" ");

    const created = await slFetch(`/Quotations`, {
      method: "POST",
      body: JSON.stringify({
        CardCode: cardCode,
        DocDate: docDate,
        DocDueDate: docDate,
        Comments: sapComments,
        JournalMemo: "Cotización web mercaderistas",
        DocumentLines,
      }),
    });

    await audit("QUOTE_CREATED", req, creator, {
      cardCode,
      lines: DocumentLines.length,
      docDate,
      province,
      warehouseCode,
    });

    return res.json({
      ok: true,
      message: "Cotización creada",
      docEntry: created.DocEntry,
      docNum: created.DocNum,
      warehouse: warehouseCode,
    });
  } catch (e) {
    return res.status(500).json({ ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ START
========================================================= */
const PORT = process.env.PORT || 10000;

ensureSchema()
  .then(() => app.listen(PORT, () => console.log("✅ Server listo en puerto", PORT)))
  .catch(() => app.listen(PORT, () => console.log("✅ Server listo en puerto", PORT, "(sin DB)")));
