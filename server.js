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
const SAP_WAREHOUSE = process.env.SAP_WAREHOUSE || "01";

const SAP_PRICE_LIST =
  process.env.SAP_PRICE_LIST || "Lista 02 Res. Com. Ind. Analitic";

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

// ✅ mapping EXACTO como pediste
function provinceToWarehouse(province) {
  const p = String(province || "").trim().toLowerCase();

  // 200
  if (p === "chiriquí" || p === "chiriqui" || p === "bocas del toro") return "200";

  // 500
  if (
    p === "veraguas" ||
    p === "coclé" ||
    p === "cocle" ||
    p === "los santos" ||
    p === "herrera"
  )
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

  // ✅ Darién
  if (p === "darién" || p === "darien") return "300";

  // fallback
  return SAP_WAREHOUSE || "01";
}

/* =========================================================
   ✅ Parse helpers (comentarios)
========================================================= */
function parseUserFromComments(comments = "") {
  const m = String(comments).match(/\[user:([^\]]+)\]/i);
  return m ? String(m[1]).trim() : "";
}
function parseWhFromComments(comments = "") {
  const m = String(comments).match(/\[wh:([^\]]+)\]/i);
  return m ? String(m[1]).trim() : "";
}
function hasWebTag(comments = "") {
  return String(comments || "").includes("[WEB PEDIDOS]");
}
function safeOdataString(s) {
  return String(s || "").replace(/'/g, "''");
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
   ✅ DB Schema (crear tablas si no existen)
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

// Cache items + stock
const ITEM_CACHE = new Map();
const ITEM_TTL_MS = 90 * 1000; // ✅ más cache (antes 20s) para que no “pegue” al SAP
const STOCK_CACHE = new Map();
const STOCK_TTL_MS = 30 * 1000;

// Evita duplicar requests concurrentes por el mismo item/stock
const PENDING_ITEM = new Map();
const PENDING_STOCK = new Map();

// Cache usernames (scope created)
let USERS_CACHE = { ts: 0, set: null };
const USERS_CACHE_TTL = 60 * 1000;

// Cache búsquedas
const ITEM_SEARCH_CACHE = new Map();
const ITEM_SEARCH_TTL_MS = 20 * 1000;

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
   ✅ Helper: warehouse por usuario
========================================================= */
function getWarehouseFromReq(req) {
  const wh = String(req.user?.warehouse_code || "").trim();
  return wh || SAP_WAREHOUSE || "01";
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
   FIX:
   - Orden estable (DocDate desc, DocEntry desc) => NO salta meses
   - Filtra en SAP por [WEB PEDIDOS] cuando aplica (created / userFilter)
   - Paginación sobre resultados filtrados (skip/limit)
========================================================= */
app.get("/api/admin/quotes", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const scope = String(req.query?.scope || "all").trim().toLowerCase(); // all | created
    const userFilter = String(req.query?.user || "").trim().toLowerCase();
    const clientFilter = String(req.query?.client || "").trim().toLowerCase();
    const from = String(req.query?.from || "").trim();
    const to = String(req.query?.to || "").trim();

    const limit = Math.min(Math.max(Number(req.query?.limit || req.query?.top || 50), 1), 500);
    const skip = Math.max(Number(req.query?.skip || 0), 0);

    // ✅ usuarios creados cacheados
    let allowedUsersSet = null;
    if (scope === "created") {
      if (!hasDb()) {
        allowedUsersSet = new Set();
      } else {
        const now = Date.now();
        if (USERS_CACHE.set && now - USERS_CACHE.ts < USERS_CACHE_TTL) {
          allowedUsersSet = USERS_CACHE.set;
        } else {
          const r = await dbQuery(`SELECT username FROM app_users;`);
          allowedUsersSet = new Set(
            (r.rows || []).map(x => String(x.username || "").trim().toLowerCase()).filter(Boolean)
          );
          USERS_CACHE = { ts: now, set: allowedUsersSet };
        }
      }
    }

    // ✅ filtro en SAP: fecha + (si created o userFilter) => solo [WEB PEDIDOS] para no excluir miles
    const filterParts = [];
    if (from) filterParts.push(`DocDate ge '${from}'`);
    if (to) filterParts.push(`DocDate le '${to}'`);

    const mustBeWeb = (scope === "created") || !!userFilter;
    if (mustBeWeb) {
      const tag = safeOdataString("[WEB PEDIDOS]");
      // substringof es el más compatible en SL
      filterParts.push(`substringof('${tag}',Comments)`);
    }

    const sapFilter = filterParts.length ? `&$filter=${encodeURIComponent(filterParts.join(" and "))}` : "";

    const SELECT =
      `DocEntry,DocNum,CardCode,CardName,DocTotal,DocDate,DocumentStatus,CancelStatus,Comments`;

    // ✅ Paginación: traemos bloques del SAP y filtramos hasta completar (skip+limit)
    const SAP_PAGE = 500;
    const MAX_PAGES = 200; // con filtro WEB ya no es pesado
    const need = skip + limit;

    const matched = [];
    let sapSkip = 0;

    for (let page = 0; page < MAX_PAGES && matched.length < need; page++) {
      const sap = await slFetch(
        `/Quotations?$select=${SELECT}` +
          `&$orderby=DocDate desc,DocEntry desc&$top=${SAP_PAGE}&$skip=${sapSkip}${sapFilter}`
      );

      const values = Array.isArray(sap?.value) ? sap.value : [];
      if (!values.length) break;

      for (const q of values) {
        const fechaISO = String(q.DocDate || "").slice(0, 10);

        // usuario desde comments
        const usuario = parseUserFromComments(q.Comments || "");
        const usuarioKey = String(usuario || "").trim().toLowerCase();

        if (allowedUsersSet && scope === "created") {
          if (!usuarioKey || !allowedUsersSet.has(usuarioKey)) continue;
        }

        if (userFilter && !usuarioKey.includes(userFilter)) continue;

        const cardCode = String(q.CardCode || "").trim();
        const cardName = String(q.CardName || "").trim();

        if (clientFilter) {
          const cc = cardCode.toLowerCase();
          const cn = cardName.toLowerCase();
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

        let mes = "";
        let anio = "";
        try {
          const d = new Date(fechaISO);
          mes = d.toLocaleString("es-PA", { month: "long" });
          anio = String(d.getFullYear());
        } catch {}

        matched.push({
          docEntry: q.DocEntry,
          docNum: q.DocNum,
          cardCode,
          cardName,
          customerName: cardName,
          nombreCliente: cardName,
          montoCotizacion: Number(q.DocTotal || 0),
          montoEntregado: 0,
          fecha: fechaISO,
          estado,
          cancelStatus,
          isCancelled,
          mes,
          anio,
          usuario,
          comments: q.Comments || "",
        });

        if (matched.length >= need) break;
      }

      // corte rápido
      if (from) {
        const last = values[values.length - 1];
        const lastDate = String(last?.DocDate || "").slice(0, 10);
        if (lastDate && lastDate < from) break;
      }

      sapSkip += SAP_PAGE;
    }

    const pageRows = matched.slice(skip, skip + limit);

    return res.json({
      ok: true,
      scope,
      limit,
      skip,
      count: pageRows.length,
      quotes: pageRows,
    });
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
   ✅ ADMIN: CREATE USER (province -> warehouse auto)
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

    if (!warehouse_code) {
      warehouse_code = provinceToWarehouse(province);
    }

    const pin_hash = await bcrypt.hash(pin, 10);

    const ins = await dbQuery(
      `
      INSERT INTO app_users(username, full_name, pin_hash, is_active, province, warehouse_code)
      VALUES ($1,$2,$3,TRUE,$4,$5)
      RETURNING id, username, full_name, is_active, province, warehouse_code, created_at;
      `,
      [username, fullName, pin_hash, province, warehouse_code]
    );

    // invalida cache usuarios (created)
    USERS_CACHE = { ts: 0, set: null };

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

    if (!r.rowCount) {
      return res.status(404).json({ ok: false, message: "Usuario no encontrado" });
    }

    USERS_CACHE = { ts: 0, set: null };

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

    if (!r.rowCount) {
      return res.status(404).json({ ok: false, message: "Usuario no encontrado" });
    }

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
    if (!hasDb()) {
      return res.status(500).json({ ok: false, message: "DB no configurada" });
    }

    const username = String(req.body?.username || "").trim().toLowerCase();
    const pin = String(req.body?.pin || "").trim();

    if (!username || !pin) {
      return res.status(400).json({ ok: false, message: "username y pin requeridos" });
    }

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
    await audit("USER_LOGIN_OK", req, username, {
      username,
      province: user.province,
      warehouse_code: user.warehouse_code,
    });

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
   ✅ PriceListNo cached
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

  const safe = safeOdataString(name);
  let no = null;

  try {
    const r1 = await slFetch(
      `/PriceLists?$select=PriceListNo,PriceListName&$filter=PriceListName eq '${safe}'`
    );
    if (r1?.value?.length) no = r1.value[0].PriceListNo;
  } catch {}

  if (no === null) {
    try {
      const r2 = await slFetch(
        `/PriceLists?$select=PriceListNo,ListName&$filter=ListName eq '${safe}'`
      );
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

/* =========================================================
   ✅ Factor UoM de VENTAS (Caja)
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

  if (!row) {
    row = coll.find((x) => Number(x?.BaseQuantity) > 1) || null;
  }

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
   ✅ Stock por warehouse (rápido)
   - NO expandimos todo el ItemWarehouseInfoCollection
   - consultamos solo la bodega del usuario
========================================================= */
async function getStockForItemWarehouse(code, warehouseCode) {
  const now = Date.now();
  const key = `${code}::${warehouseCode}`;

  const cached = STOCK_CACHE.get(key);
  if (cached && now - cached.ts < STOCK_TTL_MS) return cached.data;

  if (PENDING_STOCK.has(key)) return PENDING_STOCK.get(key);

  const p = (async () => {
    // Intento 1: navegar a la colección y filtrar por warehouse (más liviano)
    try {
      const whSafe = safeOdataString(warehouseCode);
      const r = await slFetch(
        `/Items('${encodeURIComponent(code)}')/ItemWarehouseInfoCollection` +
          `?$select=WarehouseCode,InStock,Committed,Ordered` +
          `&$filter=WarehouseCode eq '${whSafe}'&$top=1`
      );

      const row = Array.isArray(r?.value) && r.value.length ? r.value[0] : null;
      const onHand = row?.InStock != null ? Number(row.InStock) : null;
      const committed = row?.Committed != null ? Number(row.Committed) : null;
      const ordered = row?.Ordered != null ? Number(row.Ordered) : null;

      let available = null;
      if (Number.isFinite(onHand) && Number.isFinite(committed)) {
        available = onHand - committed;
      }

      const data = {
        warehouse: warehouseCode,
        onHand: Number.isFinite(onHand) ? onHand : null,
        committed: Number.isFinite(committed) ? committed : null,
        ordered: Number.isFinite(ordered) ? ordered : null,
        available: Number.isFinite(available) ? available : null,
        hasStock: available != null ? available > 0 : null,
      };

      STOCK_CACHE.set(key, { ts: now, data });
      return data;
    } catch (e) {
      // fallback: si el SL no soporta filtro en navegación, devolvemos nulls
      const data = {
        warehouse: warehouseCode,
        onHand: null,
        committed: null,
        ordered: null,
        available: null,
        hasStock: null,
      };
      STOCK_CACHE.set(key, { ts: now, data });
      return data;
    }
  })();

  PENDING_STOCK.set(key, p);
  try {
    return await p;
  } finally {
    PENDING_STOCK.delete(key);
  }
}

function buildItemResponse(itemFull, code, priceListNo, warehouseCode, stockObj) {
  const item = {
    ItemCode: itemFull.ItemCode ?? code,
    ItemName: itemFull.ItemName ?? `Producto ${code}`,
    SalesUnit: itemFull.SalesUnit ?? "",
    InventoryItem: itemFull.InventoryItem ?? null,
  };

  const priceUnit = getPriceFromPriceList(itemFull, priceListNo);
  const factorCaja = getSalesUomFactor(itemFull);

  // ✅ Si hay factor, lo aplica. Si no hay factor, deja unitario.
  const priceCaja =
    priceUnit != null && factorCaja != null ? priceUnit * factorCaja : priceUnit;

  return {
    item,
    price: priceCaja,
    priceUnit,
    factorCaja,
    stock: stockObj || {
      warehouse: warehouseCode,
      onHand: null,
      committed: null,
      ordered: null,
      available: null,
      hasStock: null,
    },
  };
}

async function getOneItem(code, priceListNo, warehouseCode) {
  const now = Date.now();
  const key = `${code}::${warehouseCode}::${priceListNo}`;

  const cached = ITEM_CACHE.get(key);
  if (cached && now - cached.ts < ITEM_TTL_MS) return cached.data;

  if (PENDING_ITEM.has(key)) return PENDING_ITEM.get(key);

  const p = (async () => {
    let itemFull;

    // ✅ Item liviano (sin warehouses) + UoM para factor caja
    try {
      itemFull = await slFetch(
        `/Items('${encodeURIComponent(code)}')` +
          `?$select=ItemCode,ItemName,SalesUnit,InventoryItem,ItemPrices` +
          `&$expand=ItemUnitOfMeasurementCollection($select=UoMType,UoMCode,UoMEntry,BaseQuantity,AlternateQuantity)`
      );
    } catch (e1) {
      // fallback suave
      itemFull = await slFetch(`/Items('${encodeURIComponent(code)}')`);
    }

    // ✅ Stock rápido por warehouse
    const stockObj = await getStockForItemWarehouse(code, warehouseCode);

    const data = buildItemResponse(itemFull, code, priceListNo, warehouseCode, stockObj);
    ITEM_CACHE.set(key, { ts: now, data });
    return data;
  })();

  PENDING_ITEM.set(key, p);
  try {
    return await p;
  } finally {
    PENDING_ITEM.delete(key);
  }
}

/* =========================================================
   ✅ SAP: ITEM (warehouse dinámico)  => incluye "available"
========================================================= */
app.get("/api/sap/item/:code", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const code = String(req.params.code || "").trim();
    if (!code) return res.status(400).json({ ok: false, message: "ItemCode vacío." });

    const warehouseCode = getWarehouseFromReq(req);
    const priceListNo = await getPriceListNoByNameCached(SAP_PRICE_LIST);

    const r = await getOneItem(code, priceListNo, warehouseCode);

    return res.json({
      ok: true,
      item: r.item,
      warehouse: warehouseCode,
      priceList: SAP_PRICE_LIST,
      priceListNo,
      price: Number(r.price ?? 0),
      priceUnit: r.priceUnit,
      factorCaja: r.factorCaja,
      uom: r.item?.SalesUnit || "Caja",
      stock: r.stock, // ✅ aquí viene available/hasStock para "Disponible"
    });
  } catch (err) {
    console.error("❌ /api/sap/item:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ SAP: SUGERENCIAS DE ARTÍCULOS (autocomplete)
   GET /api/sap/items/search?q=salsa&top=20
========================================================= */
app.get("/api/sap/items/search", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const q = String(req.query?.q || "").trim();
    const top = Math.min(Math.max(Number(req.query?.top || 20), 5), 50);

    if (q.length < 2) return res.json({ ok: true, q, results: [] });

    const key = `${q.toLowerCase()}::${top}`;
    const now = Date.now();

    const cached = ITEM_SEARCH_CACHE.get(key);
    if (cached && now - cached.ts < ITEM_SEARCH_TTL_MS) return res.json({ ok: true, q, results: cached.data });

    const safe = safeOdataString(q);

    // OJO: Esto solo sugiere (ItemCode/ItemName). El precio/stock se calcula cuando eliges el código (endpoint /item/:code)
    let r;
    try {
      r = await slFetch(
        `/Items?$select=ItemCode,ItemName,SalesUnit` +
          `&$filter=contains(ItemName,'${safe}') or contains(ItemCode,'${safe}')` +
          `&$orderby=ItemName asc&$top=${top}`
      );
    } catch (e) {
      r = await slFetch(
        `/Items?$select=ItemCode,ItemName,SalesUnit` +
          `&$filter=substringof('${safe}',ItemName) or substringof('${safe}',ItemCode)` +
          `&$orderby=ItemName asc&$top=${top}`
      );
    }

    const values = Array.isArray(r?.value) ? r.value : [];
    const results = values.map(x => ({
      ItemCode: String(x.ItemCode || "").trim(),
      ItemName: String(x.ItemName || "").trim(),
      SalesUnit: String(x.SalesUnit || "").trim(),
    })).filter(x => x.ItemCode);

    ITEM_SEARCH_CACHE.set(key, { ts: now, data: results });

    return res.json({ ok: true, q, results });
  } catch (err) {
    console.error("❌ /api/sap/items/search:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ SAP: SEARCH CUSTOMERS (autocomplete)
========================================================= */
app.get("/api/sap/customers/search", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const q = String(req.query?.q || "").trim();
    const top = Math.min(Math.max(Number(req.query?.top || 15), 5), 50);

    if (q.length < 2) return res.json({ ok: true, results: [] });

    const safe = safeOdataString(q);

    let r;
    try {
      r = await slFetch(
        `/BusinessPartners?$select=CardCode,CardName,Phone1,EmailAddress` +
          `&$filter=contains(CardName,'${safe}') or contains(CardCode,'${safe}')` +
          `&$orderby=CardName asc&$top=${top}`
      );
    } catch (e) {
      r = await slFetch(
        `/BusinessPartners?$select=CardCode,CardName,Phone1,EmailAddress` +
          `&$filter=substringof('${safe}',CardName) or substringof('${safe}',CardCode)` +
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

    return res.json({ ok: true, q, results });
  } catch (err) {
    console.error("❌ /api/sap/customers/search:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ SAP: MULTI ITEMS (warehouse dinámico)
========================================================= */
app.get("/api/sap/items", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const codes = String(req.query.codes || "")
      .split(",")
      .map((x) => x.trim())
      .filter(Boolean);

    if (!codes.length) return res.status(400).json({ ok: false, message: "codes vacío" });

    const warehouseCode = getWarehouseFromReq(req);
    const priceListNo = await getPriceListNoByNameCached(SAP_PRICE_LIST);

    const CONCURRENCY = 6;
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
            stock: r.stock, // ✅ trae available
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
    console.error("❌ /api/sap/customer:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ ADMIN: DASHBOARD
   FIX:
   - scope=created rápido: filtra en SAP por [WEB PEDIDOS] primero
   - omite canceladas
========================================================= */
app.get("/api/admin/dashboard", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const scope = String(req.query?.scope || "created").trim().toLowerCase(); // created | all
    const fromQ = String(req.query?.from || "").trim();
    const toQ = String(req.query?.to || "").trim();

    const DEFAULT_FROM = "2020-01-01";
    const from = fromQ || DEFAULT_FROM;
    const to = toQ || "";

    const PAGE_SIZE = Math.min(Math.max(Number(req.query?.top || 500), 50), 500);
    const MAX_PAGES = Math.min(Math.max(Number(req.query?.maxPages || 20), 1), 120);

    // ✅ allowed users (cached)
    let allowedUsersSet = null;
    if (scope === "created") {
      if (!hasDb()) {
        allowedUsersSet = new Set();
      } else {
        const now = Date.now();
        if (USERS_CACHE.set && now - USERS_CACHE.ts < USERS_CACHE_TTL) {
          allowedUsersSet = USERS_CACHE.set;
        } else {
          const r = await dbQuery(`SELECT username FROM app_users;`);
          allowedUsersSet = new Set(
            (r.rows || []).map((x) => String(x.username || "").trim().toLowerCase()).filter(Boolean)
          );
          USERS_CACHE = { ts: now, set: allowedUsersSet };
        }
      }
    }

    // ✅ SAP filter (fecha + web tag)
    const filterParts = [];
    if (from) filterParts.push(`DocDate ge '${from}'`);
    if (to) filterParts.push(`DocDate le '${to}'`);

    // dashboard siempre de web pedidos (si quieres dashboard all incluyendo SAP manual, quita esto)
    const tag = safeOdataString("[WEB PEDIDOS]");
    filterParts.push(`substringof('${tag}',Comments)`);

    const sapFilter = filterParts.length ? `&$filter=${encodeURIComponent(filterParts.join(" and "))}` : "";

    const SELECT =
      `DocEntry,DocNum,CardCode,CardName,DocTotal,DocDate,DocumentStatus,CancelStatus,Cancelled,Comments`;

    let all = [];
    let skip = 0;

    for (let page = 0; page < MAX_PAGES; page++) {
      const sap = await slFetch(
        `/Quotations?$select=${SELECT}` +
          `&$orderby=DocDate desc,DocEntry desc&$top=${PAGE_SIZE}&$skip=${skip}${sapFilter}`
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

        const cancelStatus = String(q.CancelStatus || "").trim();
        const cancelledFlag = String(q.Cancelled || "").trim();

        const isCancelled =
          cancelStatus.toLowerCase() === "csyes" ||
          cancelledFlag.toLowerCase() === "tyes" ||
          cancelledFlag.toLowerCase() === "yes" ||
          cancelledFlag.toLowerCase() === "y" ||
          cancelledFlag.toLowerCase() === "true";

        if (isCancelled) continue; // ✅ no mostrar ni contabilizar

        const cardCode = String(q.CardCode || "").trim();
        const cardName = String(q.CardName || "").trim();

        const warehouse = parseWhFromComments(q.Comments || "") || "sin_wh";

        const estado =
          q.DocumentStatus === "bost_Open"
            ? "Open"
            : q.DocumentStatus === "bost_Close"
            ? "Close"
            : String(q.DocumentStatus || "");

        let mes = "";
        let anio = "";
        try {
          const d = new Date(fechaISO);
          mes = d.toLocaleString("es-PA", { month: "long" });
          anio = String(d.getFullYear());
        } catch {}

        all.push({
          docEntry: q.DocEntry,
          docNum: q.DocNum,
          cardCode,
          cardName,
          montoCotizacion: Number(q.DocTotal || 0),
          montoEntregado: 0,
          fecha: fechaISO,
          estado,
          cancelStatus,
          mes,
          anio,
          usuario: usuario || "sin_user",
          warehouse,
          bodega: warehouse,
        });
      }

      if (from) {
        const last = values[values.length - 1];
        const lastDate = String(last?.DocDate || "").slice(0, 10);
        if (lastDate && lastDate < from) break;
      }

      skip += PAGE_SIZE;
    }

    const sumCot = all.reduce((acc, x) => acc + (Number(x.montoCotizacion) || 0), 0);
    const sumEnt = all.reduce((acc, x) => acc + (Number(x.montoEntregado) || 0), 0);
    const fillRate = sumCot > 0 ? sumEnt / sumCot : 0;

    function topBy(keyFn, valueFn, n = 10) {
      const m = new Map();
      for (const row of all) {
        const k = keyFn(row);
        const v = valueFn(row);
        m.set(k, (m.get(k) || 0) + v);
      }
      return [...m.entries()]
        .map(([k, v]) => ({ key: k, value: v }))
        .sort((a, b) => b.value - a.value)
        .slice(0, n);
    }

    function countBy(keyFn) {
      const m = new Map();
      for (const row of all) {
        const k = keyFn(row);
        m.set(k, (m.get(k) || 0) + 1);
      }
      return [...m.entries()].map(([k, v]) => ({ key: k, count: v }));
    }

    const topUsuariosMonto = topBy(
      (r) => String(r.usuario || "sin_user"),
      (r) => Number(r.montoCotizacion || 0),
      10
    ).map((x) => ({ usuario: x.key, monto: x.value }));

    const topClientesMonto = topBy(
      (r) => String(r.cardName || r.cardCode || "sin_cliente"),
      (r) => Number(r.montoCotizacion || 0),
      10
    ).map((x) => ({ cliente: x.key, monto: x.value }));

    const porBodegaMonto = topBy(
      (r) => String(r.warehouse || "sin_wh"),
      (r) => Number(r.montoCotizacion || 0),
      20
    ).map((x) => ({ bodega: x.key, monto: x.value }));

    const porDia = topBy(
      (r) => String(r.fecha || ""),
      (r) => Number(r.montoCotizacion || 0),
      400
    )
      .map((x) => ({ fecha: x.key, monto: x.value }))
      .sort((a, b) => a.fecha.localeCompare(b.fecha));

    const porMes = topBy(
      (r) => String(r.fecha || "").slice(0, 7),
      (r) => Number(r.montoCotizacion || 0),
      200
    )
      .map((x) => ({ mes: x.key, monto: x.value }))
      .sort((a, b) => a.mes.localeCompare(b.mes));

    const estados = countBy((r) => String(r.estado || "Unknown"))
      .map((x) => ({ estado: x.key, cantidad: x.count }))
      .sort((a, b) => b.cantidad - a.cantidad);

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
      charts: {
        topUsuariosMonto,
        topClientesMonto,
        porBodegaMonto,
        porDia,
        porMes,
        estados,
        pieCotVsEnt: {
          cotizado: sumCot,
          entregado: sumEnt,
          fillRate,
        },
      },
    });
  } catch (err) {
    console.error("❌ /api/admin/dashboard:", err.message);
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

    const warehouseCode = getWarehouseFromReq(req);

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
