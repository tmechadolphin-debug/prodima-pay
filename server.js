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
// ---- SAP ----
const SAP_BASE_URL = process.env.SAP_BASE_URL || "";
const SAP_COMPANYDB = process.env.SAP_COMPANYDB || "";
const SAP_USER = process.env.SAP_USER || "";
const SAP_PASS = process.env.SAP_PASS || "";

// ⚠️ DEFAULT WAREHOUSE (fallback si usuario no tiene)
const SAP_WAREHOUSE = process.env.SAP_WAREHOUSE || "300";

// Default (solo se usa si NO se pasa cardCode o si no se puede leer la lista del BP)
const SAP_PRICE_LIST = process.env.SAP_PRICE_LIST || "Lista 02 Res. Com. Ind. Analitic";

// ✅ Solo bodegas permitidas para inventario (para ventas)
const ALLOWED_STOCK_WH = (process.env.ALLOWED_STOCK_WH || "200,300,500")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const SEARCH_WH_ONLY = String(process.env.SEARCH_WH_ONLY || "300").trim();


// ✅ (Opcional) Solo grupos de PRODUCTO TERMINADO (ItemsGroupCode)
// Si lo dejas vacío, NO filtra por grupo.
const FINISHED_GROUP_CODES = (process.env.FINISHED_GROUP_CODES || "")
  .split(",")
  .map((s) => Number(String(s).trim()))
  .filter((n) => Number.isFinite(n));

// ---- Web / CORS ----
const YAPPY_ALIAS = process.env.YAPPY_ALIAS || "@prodimasansae";
const CORS_ORIGIN = process.env.CORS_ORIGIN || "*";

// ---- DB Supabase ----
const DATABASE_URL = process.env.DATABASE_URL || "";

// ---- Admin ----
const ADMIN_USER = process.env.ADMIN_USER || "PRODIMA";
const ADMIN_PASS = process.env.ADMIN_PASS || "ADMINISTRADOR";
const JWT_SECRET = process.env.JWT_SECRET || "prodima_change_this_secret";

// ---- Timezone Fix (para fecha SAP) ----
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
   ✅ Provincias + Bodegas (Auto)
========================================================= */
const PROVINCES = [
  "Bocas del Toro",
  "Chiriquí",
  "Coclé",
  "Colón",
  "Darién",
  "Herrera",
  "Los Santos",
  "Panamá",
  "Panamá Oeste",
  "Veraguas",
];

function provinceToWarehouse(province) {
  const p = String(province || "").trim().toLowerCase();

  // 200
  if (p === "chiriquí" || p === "chiriqui" || p === "bocas del toro") return "200";

  // 500
  if (p === "veraguas" || p === "coclé" || p === "cocle" || p === "los santos" || p === "herrera")
    return "500";

  // 300
  if (
    p === "panamá" ||
    p === "panama" ||
    p === "panamá oeste" ||
    p === "panama oeste" ||
    p === "colón" ||
    p === "colon"
  )
    return "300";

  // Darién -> 300
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
      max: 3,
    });

    pool.on("error", (err) => {
      console.error("❌ DB pool error:", err.message);
    });
  }
  return pool;
}

async function dbQuery(text, params = []) {
  const p = getPool();
  return p.query(text, params);
}

/* =========================================================
   ✅ DB Schema
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
  } catch (e) {
    console.log("⚠️ ALTER TABLE app_users:", e.message);
  }

  console.log("✅ DB Schema OK (app_users, audit_events) + province/warehouse_code");
}

async function audit(event_type, req, actor = "", payload = {}) {
  if (!hasDb()) return;
  try {
    await dbQuery(
      `
      INSERT INTO audit_events(event_type, actor, ip, user_agent, payload)
      VALUES ($1,$2,$3,$4,$5)
      `,
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
    if (!auth.startsWith("Bearer ")) {
      return res.status(401).json({ ok: false, message: "Falta Authorization Bearer token" });
    }

    const token = auth.replace("Bearer ", "").trim();
    const decoded = jwt.verify(token, JWT_SECRET);

    if (!decoded || decoded.typ !== "admin") {
      return res.status(403).json({ ok: false, message: "Token inválido" });
    }

    req.admin = decoded;
    next();
  } catch (e) {
    return res.status(401).json({ ok: false, message: "Token expirado o inválido" });
  }
}

function verifyUser(req, res, next) {
  try {
    const auth = String(req.headers.authorization || "");
    if (!auth.startsWith("Bearer ")) {
      return res.status(401).json({ ok: false, message: "Falta Authorization Bearer token" });
    }

    const token = auth.replace("Bearer ", "").trim();
    const decoded = jwt.verify(token, JWT_SECRET);

    if (!decoded || decoded.typ !== "user") {
      return res.status(403).json({ ok: false, message: "Token inválido" });
    }

    req.user = decoded;
    next();
  } catch (e) {
    return res.status(401).json({ ok: false, message: "Token expirado o inválido" });
  }
}

/* =========================================================
   ✅ SAP Helpers (Service Layer Cookie + Cache)
========================================================= */
let SL_COOKIE = null;
let SL_COOKIE_TIME = 0;

let PRICE_LIST_CACHE = { name: "", no: null, ts: 0 };
const PRICE_LIST_TTL_MS = 6 * 60 * 60 * 1000;

const BP_PL_CACHE = new Map(); // cardCode -> { ts, priceListNo }
const BP_PL_TTL_MS = 30 * 60 * 1000;

const ITEM_CACHE = new Map();
const ITEM_TTL_MS = 20 * 1000;

function missingSapEnv() {
  return !SAP_BASE_URL || !SAP_COMPANYDB || !SAP_USER || !SAP_PASS;
}

async function slLogin() {
  if (missingSapEnv()) {
    console.log("⚠️ Faltan variables SAP en Render > Environment");
    return;
  }

  const payload = {
    CompanyDB: SAP_COMPANYDB,
    UserName: SAP_USER,
    Password: SAP_PASS,
  };

  const res = await fetch(`${SAP_BASE_URL}/Login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const t = await res.text();
    throw new Error(`Login SAP falló (${res.status}): ${t}`);
  }

  const setCookie = res.headers.get("set-cookie");
  if (!setCookie) throw new Error("No se recibió cookie del Service Layer.");

  SL_COOKIE = setCookie
    .split(",")
    .map((s) => s.split(";")[0])
    .join("; ");

  SL_COOKIE_TIME = Date.now();
  console.log("✅ Login SAP OK (cookie guardada)");
}

async function slFetch(path, options = {}) {
  if (!SL_COOKIE || Date.now() - SL_COOKIE_TIME > 25 * 60 * 1000) {
    await slLogin();
  }

  const res = await fetch(`${SAP_BASE_URL}${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      Cookie: SL_COOKIE,
      ...(options.headers || {}),
    },
  });

  const text = await res.text();

  if (res.status === 401 || res.status === 403) {
    SL_COOKIE = null;
    await slLogin();
    return slFetch(path, options);
  }

  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }

  if (!res.ok) {
    throw new Error(`SAP error ${res.status}: ${text}`);
  }

  return json;
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
   ✅ Warehouse (por usuario) + solo permitidas
========================================================= */
function isAllowedWarehouse(wh) {
  const w = String(wh || "").trim();
  return ALLOWED_STOCK_WH.includes(w);
}

function getWarehouseFromReq(req) {
  const wh = String(req.user?.warehouse_code || "").trim();
  return wh || SAP_WAREHOUSE || "01";
}

function getSafeWarehouseFromReq(req) {
  const wh = getWarehouseFromReq(req);
  if (isAllowedWarehouse(wh)) return wh;
  return ALLOWED_STOCK_WH[0] || wh; // cae a la primera permitida
}

const ALLOWED_WAREHOUSES = ["200", "300", "500"];

// Cache corto para no re-consultar warehouses en cada tecla
const ITEM_WH_CACHE = new Map(); // key: itemCode -> { ts, data }
const ITEM_WH_TTL_MS = 30 * 1000;

function getWarehouseScopeForUser(req) {
  const userWh = String(getWarehouseFromReq(req) || "").trim();
  // Si el usuario tiene 200/300/500, filtramos SOLO esa (lo que tú pediste "por usuario")
  if (ALLOWED_WAREHOUSES.includes(userWh)) return [userWh];
  // Si no, fallback a las permitidas
  return [...ALLOWED_WAREHOUSES];
}

async function getWarehouseInfoForItemCached(itemCode) {
  const now = Date.now();
  const key = String(itemCode || "").trim();
  if (!key) return [];

  const cached = ITEM_WH_CACHE.get(key);
  if (cached && now - cached.ts < ITEM_WH_TTL_MS) return cached.data;

  let item;
  try {
    // liviano: solo bodegas y cantidades
    item = await slFetch(
      `/Items('${encodeURIComponent(key)}')?$select=ItemCode&$expand=ItemWarehouseInfoCollection($select=WarehouseCode,InStock,Committed,Ordered)`
    );
  } catch (e) {
    // fallback si tu SL no soporta expand (raro, pero por si acaso)
    item = await slFetch(`/Items('${encodeURIComponent(key)}')`);
  }

  const rows = Array.isArray(item?.ItemWarehouseInfoCollection)
    ? item.ItemWarehouseInfoCollection
    : [];

  ITEM_WH_CACHE.set(key, { ts: now, data: rows });
  return rows;
}

function summarizeWarehouses(rows, scopeList) {
  const scope = new Set((scopeList || []).map(String));
  let onHand = 0, committed = 0, ordered = 0;

  for (const w of (rows || [])) {
    const wh = String(w?.WarehouseCode || "").trim();
    if (!scope.has(wh)) continue;

    const a = Number(w?.InStock);
    const c = Number(w?.Committed);
    const o = Number(w?.Ordered);

    if (Number.isFinite(a)) onHand += a;
    if (Number.isFinite(c)) committed += c;
    if (Number.isFinite(o)) ordered += o;
  }

  const available = onHand - committed;

  return {
    onHand: Number.isFinite(onHand) ? onHand : null,
    committed: Number.isFinite(committed) ? committed : null,
    ordered: Number.isFinite(ordered) ? ordered : null,
    available: Number.isFinite(available) ? available : null,
    hasStock: Number.isFinite(available) ? (available > 0) : null,
  };
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
    allowedWarehouses: ALLOWED_STOCK_WH,
    priceList: SAP_PRICE_LIST,
    db: hasDb() ? "on" : "off",
  });
});

/* =========================================================
   ✅ ADMIN: LOGIN
========================================================= */
app.post("/api/admin/login", async (req, res) => {
  try {
    const user = String(req.body?.user || "").trim();
    const pass = String(req.body?.pass || "").trim();

    if (!user || !pass) {
      return res.status(400).json({ ok: false, message: "user y pass requeridos" });
    }

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
   ✅ ADMIN: HISTÓRICO DE COTIZACIONES (SAP)
========================================================= */
app.get("/api/admin/quotes", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const userFilter = String(req.query?.user || "").trim().toLowerCase();
    const clientFilter = String(req.query?.client || "").trim().toLowerCase();
    const from = String(req.query?.from || "").trim();
    const to = String(req.query?.to || "").trim();
    const limit = Math.min(Number(req.query?.limit || 200), 500);

    const sap = await slFetch(
      `/Quotations?$select=DocEntry,DocNum,CardCode,CardName,DocTotal,DocDate,DocumentStatus,Comments&$orderby=DocDate desc&$top=${limit}`
    );

    const values = Array.isArray(sap?.value) ? sap.value : [];

    const parseUserFromComments = (comments = "") => {
      const m = String(comments).match(/\[user:([^\]]+)\]/i);
      return m ? String(m[1]).trim() : "";
    };

    const bpCache = new Map();
    async function getBPName(cardCode) {
      if (!cardCode) return "";
      if (bpCache.has(cardCode)) return bpCache.get(cardCode);
      try {
        const bp = await slFetch(
          `/BusinessPartners('${encodeURIComponent(cardCode)}')?$select=CardCode,CardName`
        );
        const name = String(bp?.CardName || "").trim();
        bpCache.set(cardCode, name);
        return name;
      } catch (e) {
        bpCache.set(cardCode, "");
        return "";
      }
    }

    let rows = [];
    for (const q of values) {
      const docDate = q.DocDate || "";
      const usuario = parseUserFromComments(q.Comments || "");
      const cardCode = String(q.CardCode || "").trim();

      const estado =
        q.DocumentStatus === "bost_Open"
          ? "Open"
          : q.DocumentStatus === "bost_Close"
          ? "Close"
          : String(q.DocumentStatus || "");

      let cardName = String(q.CardName || "").trim();
      if (!cardName) cardName = await getBPName(cardCode);

      let mes = "";
      let anio = "";
      try {
        const d = new Date(docDate);
        mes = d.toLocaleString("es-PA", { month: "long" });
        anio = String(d.getFullYear());
      } catch {}

      rows.push({
        docEntry: q.DocEntry,
        docNum: q.DocNum,
        cardCode,
        cardName,
        customerName: cardName,
        nombreCliente: cardName,
        montoCotizacion: Number(q.DocTotal || 0),
        montoEntregado: 0,
        fecha: docDate,
        estado,
        mes,
        anio,
        usuario,
        comments: q.Comments || "",
      });
    }

    if (userFilter) rows = rows.filter((r) => String(r.usuario || "").toLowerCase().includes(userFilter));
    if (clientFilter) {
      rows = rows.filter(
        (r) =>
          String(r.cardCode || "").toLowerCase().includes(clientFilter) ||
          String(r.cardName || "").toLowerCase().includes(clientFilter)
      );
    }
    if (from) rows = rows.filter((r) => String(r.fecha || "") >= from);
    if (to) rows = rows.filter((r) => String(r.fecha || "") <= to);

    return res.json({ ok: true, quotes: rows });
  } catch (err) {
    console.error("❌ /api/admin/quotes:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ ADMIN: LIST USERS
========================================================= */
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
    console.error("❌ users list:", e.message);
    return res.status(500).json({ ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ ADMIN: CREATE USER
========================================================= */
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

    await audit("USER_CREATED", req, "ADMIN", { username, fullName, province, warehouse_code });

    return res.json({ ok: true, user: ins.rows[0] });
  } catch (e) {
    const msg = String(e.message || e);
    if (msg.includes("duplicate key value") || msg.includes("unique")) {
      return res.status(400).json({ ok: false, message: "Ese username ya existe" });
    }
    console.error("❌ user create:", msg);
    return res.status(500).json({ ok: false, message: msg });
  }
});

/* =========================================================
   ✅ ADMIN: DELETE USER
========================================================= */
app.delete("/api/admin/users/:id", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return res.status(500).json({ ok: false, message: "DB no configurada" });

    const id = Number(req.params.id || 0);
    if (!id) return res.status(400).json({ ok: false, message: "id inválido" });

    const r = await dbQuery(`DELETE FROM app_users WHERE id = $1 RETURNING id, username;`, [id]);

    if (!r.rowCount) return res.status(404).json({ ok: false, message: "Usuario no encontrado" });

    await audit("USER_DELETED", req, "ADMIN", { id, username: r.rows[0]?.username });

    return res.json({ ok: true, message: "Usuario eliminado" });
  } catch (e) {
    console.error("❌ user delete:", e.message);
    return res.status(500).json({ ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ ADMIN: TOGGLE ACTIVO
========================================================= */
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
    console.error("❌ user toggle:", e.message);
    return res.status(500).json({ ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ ADMIN: AUDIT
========================================================= */
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
   ✅ MERCADERISTAS: LOGIN
========================================================= */
app.post("/api/auth/login", async (req, res) => {
  try {
    if (!hasDb()) return res.status(500).json({ ok: false, message: "DB no configurada" });

    const username = String(req.body?.username || "").trim().toLowerCase();
    const pin = String(req.body?.pin || "").trim();

    if (!username || !pin) return res.status(400).json({ ok: false, message: "username y pin requeridos" });

    const r = await dbQuery(
      `
      SELECT id, username, full_name, pin_hash, is_active, province, warehouse_code
      FROM app_users
      WHERE username = $1
      LIMIT 1;
      `,
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
    console.error("❌ /api/auth/login:", e.message);
    return res.status(500).json({ ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ MERCADERISTAS: ME
========================================================= */
app.get("/api/auth/me", verifyUser, async (req, res) => {
  return res.json({ ok: true, user: req.user });
});

/* =========================================================
   ✅ PriceListNo cached (por nombre)
========================================================= */
async function getPriceListNoByNameCached(name) {
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

/* =========================================================
   ✅ PriceListNo por cliente (BusinessPartner.PriceListNum)
========================================================= */
async function getPriceListNoForCardCodeCached(cardCode) {
  const cc = String(cardCode || "").trim();
  if (!cc) return null;

  const now = Date.now();
  const cached = BP_PL_CACHE.get(cc);
  if (cached && now - cached.ts < BP_PL_TTL_MS) return cached.priceListNo;

  try {
    const bp = await slFetch(
      `/BusinessPartners('${encodeURIComponent(cc)}')?$select=CardCode,CardName,PriceListNum`
    );
    const pl = bp?.PriceListNum ?? null;
    const n = Number(pl);
    const priceListNo = Number.isFinite(n) ? n : null;
    BP_PL_CACHE.set(cc, { ts: now, priceListNo });
    return priceListNo;
  } catch {
    BP_PL_CACHE.set(cc, { ts: now, priceListNo: null });
    return null;
  }
}

function getPriceFromPriceList(itemFull, priceListNo) {
  const listNo = Number(priceListNo);

  const row = Array.isArray(itemFull?.ItemPrices)
    ? itemFull.ItemPrices.find((p) => Number(p?.PriceList) === listNo)
    : null;

  const price = row && row.Price != null ? Number(row.Price) : null;
  return Number.isFinite(price) ? price : null;
}

/* =========================================================
   ✅ Factor UoM ventas (Caja)
========================================================= */
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

  const baseQty = row?.BaseQuantity ?? row?.BaseQty ?? null;
  const altQty = row?.AlternateQuantity ?? row?.AltQty ?? row?.AlternativeQuantity ?? null;

  const b = Number(baseQty);
  const a = Number(altQty);

  if (Number.isFinite(b) && b > 0 && Number.isFinite(a) && a > 0) {
    const f = b / a;
    return Number.isFinite(f) && f > 0 ? f : null;
  }

  if (Number.isFinite(b) && b > 0) return b;
  return null;
}

/* =========================================================
   ✅ Filtros de "Producto Vendible" / Grupo terminado
========================================================= */
function isSalesInventoryItem(it) {
  const inv = String(it?.InventoryItem || "").toLowerCase(); // tYES/tNO
  const sal = String(it?.SalesItem || "").toLowerCase(); // tYES/tNO
  return inv.includes("yes") && sal.includes("yes");
}

function isFinishedGroup(it) {
  if (!FINISHED_GROUP_CODES.length) return true;
  const g = Number(it?.ItemsGroupCode);
  return FINISHED_GROUP_CODES.includes(g);
}

/* =========================================================
   ✅ buildItemResponse
========================================================= */
function buildItemResponse(itemFull, code, priceListNo, warehouseCode) {
  const item = {
    ItemCode: itemFull.ItemCode ?? code,
    ItemName: itemFull.ItemName ?? `Producto ${code}`,
    SalesUnit: itemFull.SalesUnit ?? "",
    InventoryItem: itemFull.InventoryItem ?? null,
    SalesItem: itemFull.SalesItem ?? null,
    ItemsGroupCode: itemFull.ItemsGroupCode ?? null,
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

  // ✅ Importante: expand UoM collection
  try {
    itemFull = await slFetch(
      `/Items('${encodeURIComponent(code)}')` +
        `?$select=ItemCode,ItemName,SalesUnit,InventoryItem,SalesItem,ItemsGroupCode,ItemPrices,ItemWarehouseInfoCollection` +
        `&$expand=ItemUnitOfMeasurementCollection($select=UoMType,UoMCode,UoMEntry,BaseQuantity,AlternateQuantity)`
    );
  } catch (e1) {
    try {
      itemFull = await slFetch(`/Items('${encodeURIComponent(code)}')?$expand=ItemUnitOfMeasurementCollection`);
    } catch (e2) {
      itemFull = await slFetch(`/Items('${encodeURIComponent(code)}')`);
    }
  }

  const data = buildItemResponse(itemFull, code, priceListNo, warehouseCode);
  ITEM_CACHE.set(key, { ts: now, data });
  return data;
}

/* =========================================================
   ✅ SAP: ITEM (1) + price list por cliente (cardCode)
========================================================= */
app.get("/api/sap/item/:code", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const code = String(req.params.code || "").trim();
    if (!code) return res.status(400).json({ ok: false, message: "ItemCode vacío." });

    const warehouseCode = getSafeWarehouseFromReq(req);

    // ✅ si viene cardCode, usar lista del BP
    const cardCode = String(req.query?.cardCode || "").trim();
    let priceListNo = await getPriceListNoForCardCodeCached(cardCode);

    if (priceListNo == null) {
      priceListNo = await getPriceListNoByNameCached(SAP_PRICE_LIST);
    }

    const r = await getOneItem(code, priceListNo, warehouseCode);

    return res.json({
      ok: true,
      item: r.item,
      warehouse: warehouseCode,
      priceList: cardCode ? "BP.PriceListNum" : SAP_PRICE_LIST,
      priceListNo,
      price: Number(r.price ?? 0),
      priceUnit: r.priceUnit,
      factorCaja: r.factorCaja,
      uom: r.item?.SalesUnit || "Caja",
      stock: r.stock,
    });
  } catch (err) {
    console.error("❌ /api/sap/item:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ SAP: SEARCH CUSTOMERS (autocomplete)
   GET /api/sap/customers/search?q=ricamar&top=20
========================================================= */
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
    console.error("❌ /api/sap/customers/search:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ SAP: SEARCH ITEMS (autocomplete) FILTRADO POR BODEGA
   GET /api/sap/items/search?q=salsa&top=20

   - Primero busca en /Items (rápido)
   - Luego filtra por bodegas permitidas (200/300/500) o la del usuario
   - Así NO te salen etiquetas, envases, etc (si no existen en esas bodegas)
========================================================= */
app.get("/api/sap/items/search", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) {
      return res.status(400).json({ ok: false, message: "Faltan variables SAP" });
    }

    const q = String(req.query?.q || "").trim();
    const top = Math.min(Math.max(Number(req.query?.top || 20), 5), 50);

    if (q.length < 2) return res.json({ ok: true, results: [] });

    const safe = q.replace(/'/g, "''");

    // 1) Búsqueda liviana en maestro de Items
    let r;
    try {
      r = await slFetch(
        `/Items?$select=ItemCode,ItemName,SalesUnit,InventoryItem,Frozen` +
          `&$filter=(contains(ItemCode,'${safe}') or contains(ItemName,'${safe}'))` +
          `&$orderby=ItemName asc&$top=${top}`
      );
    } catch (e) {
      r = await slFetch(
        `/Items?$select=ItemCode,ItemName,SalesUnit,InventoryItem,Frozen` +
          `&$filter=(substringof('${safe}',ItemCode) or substringof('${safe}',ItemName))` +
          `&$orderby=ItemName asc&$top=${top}`
      );
    }

    const values = Array.isArray(r?.value) ? r.value : [];

    // 2) Filtrado básico: inventariable y no congelado
    const candidates = values
      .filter((it) => it?.InventoryItem !== false)
      .filter((it) => String(it?.Frozen || "").toLowerCase() !== "t")
      .map((it) => ({
        ItemCode: String(it.ItemCode || "").trim(),
        ItemName: String(it.ItemName || "").trim(),
        SalesUnit: String(it.SalesUnit || "").trim(),
      }))
      .filter((x) => x.ItemCode && x.ItemName)
      .slice(0, top);

    // 3) ✅ FILTRO por bodegas permitidas / por usuario
// ✅ SOLO sugerencias de la bodega 300
const scopeList = [SEARCH_WH_ONLY || "300"];
const scopeSet = new Set(scopeList);



    // Concurrencia para no pegarle duro al SL
    const CONCURRENCY = 6;
    const out = [];
    let idx = 0;

    async function worker() {
      while (idx < candidates.length) {
        const i = idx++;
        const it = candidates[i];

        try {
          const rows = await getWarehouseInfoForItemCached(it.ItemCode);

          // ✅ Regla clave:
          // "el item debe EXISTIR en la(s) bodega(s) del scope"
          const existsInScope = Array.isArray(rows) && rows.some(w =>
            scopeSet.has(String(w?.WarehouseCode || "").trim())
          );

          if (!existsInScope) continue;

          // (Opcional) si quieres además mostrar disponible total del scope:
          const stock = summarizeWarehouses(rows, scopeList);

          out.push({
            ...it,
            // opcional: para ordenar o mostrar
            stockAvailable: stock.available,
            stockHas: stock.hasStock,
          });
        } catch (e) {
          // si falla un item, lo ignoramos para no romper autocomplete
          continue;
        }
      }
    }

    await Promise.all(Array.from({ length: CONCURRENCY }, worker));

    // (Opcional) ordena: primero los que tienen stock
    out.sort((a, b) => {
      const ah = a.stockHas ? 1 : 0;
      const bh = b.stockHas ? 1 : 0;
      if (bh !== ah) return bh - ah;
      const av = Number(a.stockAvailable || 0);
      const bv = Number(b.stockAvailable || 0);
      return bv - av;
    });

    return res.json({
      ok: true,
      q,
      scope: scopeList, // para debug
      results: out.slice(0, top).map(x => ({
        ItemCode: x.ItemCode,
        ItemName: x.ItemName,
        SalesUnit: x.SalesUnit,
      })),
    });
  } catch (err) {
    console.error("❌ /api/sap/items/search:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ SAP: MULTI ITEMS
========================================================= */
app.get("/api/sap/items", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const codes = String(req.query.codes || "")
      .split(",")
      .map((x) => x.trim())
      .filter(Boolean);

    if (!codes.length) return res.status(400).json({ ok: false, message: "codes vacío" });

    const warehouseCode = getSafeWarehouseFromReq(req);
    const priceListNo = await getPriceListNoByNameCached(SAP_PRICE_LIST);

    const CONCURRENCY = 5;
    const items = {};
    let i = 0;

    async function worker() {
      while (i < codes.length) {
        const idx = i++;
        const code = codes[idx];

        try {
          const r = await getOneItem(code, priceListNo, warehouseCode);
          items[code] = {
            ok: true,
            name: r.item.ItemName,
            unit: r.item.SalesUnit,
            price: r.price,
            priceUnit: r.priceUnit,
            factorCaja: r.factorCaja,
            stock: r.stock,
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
      priceList: SAP_PRICE_LIST,
      priceListNo,
      items,
    });
  } catch (err) {
    console.error("❌ /api/sap/items:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ SAP: CUSTOMER (detalle)
========================================================= */
app.get("/api/sap/customer/:code", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const code = String(req.params.code || "").trim();
    if (!code) return res.status(400).json({ ok: false, message: "CardCode vacío." });

    const bp = await slFetch(
      `/BusinessPartners('${encodeURIComponent(code)}')?$select=CardCode,CardName,Phone1,Phone2,EmailAddress,Address,City,Country,ZipCode,PriceListNum`
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
        PriceListNum: bp.PriceListNum ?? null,
      },
    });
  } catch (err) {
    console.error("❌ /api/sap/customer:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ SAP: CREAR COTIZACIÓN
========================================================= */
app.post("/api/sap/quote", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const cardCode = String(req.body?.cardCode || "").trim();
    const comments = String(req.body?.comments || "").trim();
    const lines = Array.isArray(req.body?.lines) ? req.body.lines : [];

    if (!cardCode) return res.status(400).json({ ok: false, message: "cardCode requerido." });
    if (!lines.length) return res.status(400).json({ ok: false, message: "lines requerido." });

    const warehouseCode = getSafeWarehouseFromReq(req);

    const DocumentLines = lines
      .map((l) => ({
        ItemCode: String(l.itemCode || "").trim(),
        Quantity: Number(l.qty || 0),
        WarehouseCode: warehouseCode,
      }))
      .filter((x) => x.ItemCode && x.Quantity > 0);

    if (!DocumentLines.length) {
      return res.status(400).json({ ok: false, message: "No hay líneas válidas (qty>0)." });
    }

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

    const payload = {
      CardCode: cardCode,
      DocDate: docDate,
      DocDueDate: docDate,
      Comments: sapComments,
      JournalMemo: "Cotización web mercaderistas",
      DocumentLines,
    };

    const created = await slFetch(`/Quotations`, {
      method: "POST",
      body: JSON.stringify(payload),
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
  } catch (err) {
    console.error("❌ /api/sap/quote:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ START
========================================================= */
const PORT = process.env.PORT || 10000;

ensureSchema()
  .then(() => {
    app.listen(PORT, () => console.log("✅ Server listo en puerto", PORT));
  })
  .catch((e) => {
    console.error("❌ Error creando schema DB:", e.message);
    app.listen(PORT, () => console.log("✅ Server listo en puerto", PORT, "(sin DB)"));
  });
