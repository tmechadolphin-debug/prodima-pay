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

  // ✅ Darién (no estaba en tu lista)
  if (p === "darién" || p === "darien") return "300";

  // fallback
  return SAP_WAREHOUSE || "01";
}

/* =========================================================
   ✅ DB Pool (Supabase)
   FIX SSL: self-signed certificate chain
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
      ssl: { rejectUnauthorized: false }, // ✅ FIX CERT
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
  const t0 = Date.now();

  if (!SL_COOKIE || Date.now() - SL_COOKIE_TIME > 25 * 60 * 1000) {
    console.log("🔐 slFetch: haciendo login (cookie vacía/expirada)");
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
  const ms = Date.now() - t0;

  console.log(`⏱️ SAP ${res.status} ${path.slice(0,80)}... (${ms}ms)`);

  if (res.status === 401 || res.status === 403) {
    console.log("♻️ SAP 401/403 -> relogin y retry");
    SL_COOKIE = null;
    await slLogin();
    return slFetch(path, options);
  }

  // ... igual que ya lo tienes
}

/* =========================================================
   ✅ FIX FECHA SAP (evitar fecha futura)
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
   ✅ FIX:
   - Paginación real (no se queda en 20/200)
   - Soporta skip/limit para el front
   - Aplica filtros (user/client/from/to) ANTES de paginar resultados (evita “solo aparecen en página 2/3”)
   - Agrega CancelStatus en SAP select y en rows.push
========================================================= */
app.get("/api/admin/quotes", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) {
      return res.status(400).json({ ok: false, message: "Faltan variables SAP" });
    }

    const userFilter = String(req.query?.user || "").trim().toLowerCase();
    const clientFilter = String(req.query?.client || "").trim().toLowerCase();

    const from = String(req.query?.from || "").trim();
    const to = String(req.query?.to || "").trim();

    // ✅ paginación para el front (sobre resultados filtrados)
    const limit = Math.min(Math.max(Number(req.query?.limit || req.query?.top || 500), 1), 500);
    const skip = Math.max(Number(req.query?.skip || 0), 0);

    // ✅ Traemos de SAP en bloques de 500 y filtramos server-side hasta completar (skip+limit)
    const SAP_PAGE = 500;
    const MAX_PAGES = 80; // 80*500 = 40,000 docs (suficiente para meses con 60/día)

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
        console.error("❌ BP lookup fail:", cardCode, e.message);
        bpCache.set(cardCode, "");
        return "";
      }
    }

    // ✅ filtro por fechas directo en SAP (reduce carga)
    const filterParts = [];
    if (from) filterParts.push(`DocDate ge '${from}'`);
    if (to) filterParts.push(`DocDate le '${to}'`);

    const sapFilter = filterParts.length
      ? `&$filter=${encodeURIComponent(filterParts.join(" and "))}`
      : "";

    // ✅ IMPORTANTE: agregar CancelStatus
    const SELECT =
      `DocEntry,DocNum,CardCode,CardName,DocTotal,DocDate,DocumentStatus,CancelStatus,Comments`;

    const need = skip + limit;
    const matched = [];

    let sapSkip = 0;
    for (let page = 0; page < MAX_PAGES && matched.length < need; page++) {
      const sap = await slFetch(
        `/Quotations?$select=${SELECT}` +
          `&$orderby=DocDate desc&$top=${SAP_PAGE}&$skip=${sapSkip}${sapFilter}`
      );

      const values = Array.isArray(sap?.value) ? sap.value : [];
      if (!values.length) break;

      for (const q of values) {
        const docDateRaw = String(q.DocDate || "");
        const fechaISO = docDateRaw.slice(0, 10);

        const usuario = parseUserFromComments(q.Comments || "");
        const cardCode = String(q.CardCode || "").trim();

        // ✅ filtros (antes de paginar)
        if (userFilter && !String(usuario || "").toLowerCase().includes(userFilter)) continue;

        let cardName = String(q.CardName || "").trim();
        if (!cardName) cardName = await getBPName(cardCode);

        if (clientFilter) {
          const cc = String(cardCode || "").toLowerCase();
          const cn = String(cardName || "").toLowerCase();
          if (!cc.includes(clientFilter) && !cn.includes(clientFilter)) continue;
        }

        // ✅ estado
        const cancelStatus = String(q.CancelStatus || "").trim(); // csYes/csNo
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

          // ✅ nuevo: cancel status en output
          cancelStatus,
          isCancelled,

          mes,
          anio,
          usuario,
          comments: q.Comments || "",
        });

        if (matched.length >= need) break;
      }

      // corte rápido si ya estamos por debajo del from
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

/* =========================================================
   ✅ FIX REAL: Factor UoM de VENTAS (Caja)
   - Tu error era: no expand + match incorrecto por SalesUnit
========================================================= */
function getSalesUomFactor(itemFull) {
  // 1) Fallbacks directos si tu SL los trae (según setup)
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

  // 2) Colección UoM (requiere $expand en muchos SL)
  const coll = itemFull?.ItemUnitOfMeasurementCollection;
  if (!Array.isArray(coll) || !coll.length) return null;

  // Busca UoM de ventas
  let row =
    coll.find((x) => String(x?.UoMType || "").toLowerCase().includes("sales")) ||
    coll.find((x) => String(x?.UoMType || "").toLowerCase().includes("iut_sales")) ||
    null;

  // Si no encontró, intenta al menos la primera con BaseQuantity > 1
  if (!row) {
    row = coll.find((x) => Number(x?.BaseQuantity) > 1) || null;
  }

  if (!row) return null;

  const baseQty = row?.BaseQuantity ?? row?.BaseQty ?? null;
  const altQty = row?.AlternateQuantity ?? row?.AltQty ?? row?.AlternativeQuantity ?? null;

  const b = Number(baseQty);
  const a = Number(altQty);

  // Si trae ambos, la conversión real es Base/Alt
  if (Number.isFinite(b) && b > 0 && Number.isFinite(a) && a > 0) {
    const f = b / a;
    return Number.isFinite(f) && f > 0 ? f : null;
  }

  // Si trae solo BaseQuantity (común), úsalo
  if (Number.isFinite(b) && b > 0) return b;

  return null;
}

function buildItemResponse(itemFull, code, priceListNo, warehouseCode) {
  const item = {
    ItemCode: itemFull.ItemCode ?? code,
    ItemName: itemFull.ItemName ?? `Producto ${code}`,
    SalesUnit: itemFull.SalesUnit ?? "",
    InventoryItem: itemFull.InventoryItem ?? null,
  };

  const priceUnit = getPriceFromPriceList(itemFull, priceListNo);
  const factorCaja = getSalesUomFactor(itemFull);

  // ✅ Si hay factor, lo aplica. Si no hay factor, deja unitario.
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
  if (Number.isFinite(onHand) && Number.isFinite(committed)) {
    available = onHand - committed;
  }

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
  if (cached && now - cached.ts < ITEM_TTL_MS) {
    return cached.data;
  }

  let itemFull;

  // ✅ AQUÍ estaba la otra parte del problema: falta $expand
  try {
    itemFull = await slFetch(
      `/Items('${encodeURIComponent(code)}')` +
        `?$select=ItemCode,ItemName,SalesUnit,InventoryItem,ItemPrices,ItemWarehouseInfoCollection` +
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
   ✅ SAP: ITEM (warehouse dinámico)
========================================================= */
app.get("/api/sap/item/:code", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) {
      return res.status(400).json({ ok: false, message: "Faltan variables SAP" });
    }

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
      price: r.price,
      stock: r.stock,
    });
  } catch (err) {
    console.error("❌ /api/sap/item:", err.message);
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

    const safe = q.replace(/'/g, "''");

    let r;
    try {
      r = await slFetch(
        `/BusinessPartners?$select=CardCode,CardName,Phone1,EmailAddress&$filter=contains(CardName,'${safe}') or contains(CardCode,'${safe}')&$orderby=CardName asc&$top=${top}`
      );
    } catch (e) {
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
            unit: r.item.SalesUnit, // Caja
            price: r.price, // Caja (si factor existe)
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
   ✅ FIX:
   - define parseUserFromComments/parseWhFromComments
   - agrega CancelStatus en select + rows.push
   - OMITE canceladas (csYes) sin contarlas
========================================================= */
/* =========================================================
   ✅ ADMIN: DASHBOARD
   ✅ FIX:
   - scope=created ahora filtra en SAP por Comments ([user:...]) para que sea rápido
   - si no hay usuarios creados => responde inmediato
   - mantiene exclusión de canceladas (csYes)
========================================================= */
app.get("/api/admin/dashboard", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) {
      return res.status(400).json({ ok: false, message: "Faltan variables SAP" });
    }

    const scope = String(req.query?.scope || "created").trim().toLowerCase(); // created | all
    const fromQ = String(req.query?.from || "").trim();
    const toQ = String(req.query?.to || "").trim();

    const DEFAULT_FROM = "2020-01-01";
    const from = fromQ || DEFAULT_FROM;
    const to = toQ || "";

    const PAGE_SIZE = Math.min(Math.max(Number(req.query?.top || 500), 50), 500);
    const MAX_PAGES = Math.min(Math.max(Number(req.query?.maxPages || 20), 1), 80);

    // ✅ helpers de parseo
    const parseUserFromComments = (comments = "") => {
      const m = String(comments).match(/\[user:([^\]]+)\]/i);
      return m ? String(m[1]).trim() : "";
    };
    const parseWhFromComments = (comments = "") => {
      const m = String(comments).match(/\[wh:([^\]]+)\]/i);
      return m ? String(m[1]).trim() : "";
    };

    // ✅ scope=created -> solo usuarios existentes en app_users
    let allowedUsersSet = null;

    // (opcional) cache corto para no pegarle a DB siempre
    // si no quieres cache, puedes borrar este bloque y dejar el query directo.
    if (!global.__ALLOWED_USERS_CACHE__) {
      global.__ALLOWED_USERS_CACHE__ = { ts: 0, set: null };
    }
    const USERS_TTL_MS = 60 * 1000; // 1 min

    if (scope === "created") {
      if (!hasDb()) {
        allowedUsersSet = new Set(); // no DB => no hay "created"
      } else {
        const now = Date.now();
        const cache = global.__ALLOWED_USERS_CACHE__;
        if (cache.set && now - cache.ts < USERS_TTL_MS) {
          allowedUsersSet = cache.set;
        } else {
          const r = await dbQuery(`SELECT username FROM app_users WHERE is_active = TRUE;`);
          allowedUsersSet = new Set(
            (r.rows || [])
              .map((x) => String(x.username || "").trim().toLowerCase())
              .filter(Boolean)
          );
          global.__ALLOWED_USERS_CACHE__ = { ts: now, set: allowedUsersSet };
        }
      }

      // ✅ si no hay usuarios creados, responde inmediato (evita “cargando”)
      if (!allowedUsersSet || allowedUsersSet.size === 0) {
        return res.json({
          ok: true,
          scope,
          from,
          to: to || null,
          fetched: 0,
          kpis: { totalCotizaciones: 0, montoCotizado: 0, montoEntregado: 0, fillRate: 0 },
          charts: {
            topUsuariosMonto: [],
            topClientesMonto: [],
            porBodegaMonto: [],
            porDia: [],
            porMes: [],
            estados: [],
            pieCotVsEnt: { cotizado: 0, entregado: 0, fillRate: 0 },
          },
        });
      }
    }

    // ✅ arma $filter base por fechas
    const baseFilterParts = [];
    if (from) baseFilterParts.push(`DocDate ge '${from}'`);
    if (to) baseFilterParts.push(`DocDate le '${to}'`);

    // ✅ si scope=created y la lista es “razonable”, filtramos en SAP por Comments
    // (evita URL enorme si hay demasiados usuarios)
    const MAX_USERS_IN_SAP_FILTER = 25;
    const usersForSap =
      scope === "created" && allowedUsersSet
        ? [...allowedUsersSet].slice(0, MAX_USERS_IN_SAP_FILTER)
        : [];

    function buildUserCommentsFilter(useSubstringof) {
      if (!usersForSap.length) return "";

      const clauses = usersForSap.map((u) => {
        const safeU = String(u).replace(/'/g, "''");
        const needle = `[user:${safeU}]`;
        return useSubstringof
          ? `substringof('${needle.replace(/'/g, "''")}',Comments)`
          : `contains(Comments,'${needle.replace(/'/g, "''")}')`;
      });

      return `(${clauses.join(" or ")})`;
    }

    // ✅ Construye filtro completo (intentamos contains, fallback substringof)
    function buildSapFilter(useSubstringof) {
      const parts = [...baseFilterParts];

      if (scope === "created" && usersForSap.length) {
        const userPart = buildUserCommentsFilter(useSubstringof);
        if (userPart) parts.push(userPart);
      }

      return parts.length ? `&$filter=${encodeURIComponent(parts.join(" and "))}` : "";
    }

    const SELECT =
      `DocEntry,DocNum,CardCode,CardName,DocTotal,DocDate,DocumentStatus,CancelStatus,Cancelled,Comments`;

    async function fetchPage(sapSkip, useSubstringof) {
      const sapFilter = buildSapFilter(useSubstringof);
      return slFetch(
        `/Quotations?$select=${SELECT}` +
          `&$orderby=DocDate desc&$top=${PAGE_SIZE}&$skip=${sapSkip}${sapFilter}`
      );
    }

    const all = [];
    let sapSkip = 0;

    // ✅ probamos contains primero; si falla, usamos substringof
    let useSubstringof = false;
    let triedFallback = false;

    for (let page = 0; page < MAX_PAGES; page++) {
      let sap;
      try {
        sap = await fetchPage(sapSkip, useSubstringof);
      } catch (e) {
        const msg = String(e?.message || e);
        // fallback típico cuando contains no existe en tu SL
        if (!triedFallback && msg.toLowerCase().includes("contains")) {
          triedFallback = true;
          useSubstringof = true;
          page--; // reintenta misma página con substringof
          continue;
        }
        throw e;
      }

      const values = Array.isArray(sap?.value) ? sap.value : [];
      if (!values.length) break;

      for (const q of values) {
        const rawDate = String(q.DocDate || "");
        const fechaISO = rawDate.slice(0, 10);

        const usuario = parseUserFromComments(q.Comments || "");
        const usuarioKey = String(usuario || "").trim().toLowerCase();

        // ✅ si por alguna razón no aplicó el filtro de SAP (ej: usersForSap truncado),
        // mantenemos el check local para no colar usuarios no creados.
        if (scope === "created" && allowedUsersSet) {
          if (!usuarioKey || !allowedUsersSet.has(usuarioKey)) continue;
        }

        const cardCode = String(q.CardCode || "").trim();
        const cardName = String(q.CardName || "").trim();
        const warehouse = parseWhFromComments(q.Comments || "") || "sin_wh";

        // ✅ excluir canceladas (csYes) + fallback a Cancelled
        const cancelStatus = String(q.CancelStatus || "").trim(); // csYes/csNo
        const cancelledFlag = String(q.Cancelled || "").trim();   // tYES/tNO

        const isCancelled =
          cancelStatus.toLowerCase() === "csyes" ||
          cancelledFlag.toLowerCase() === "tyes" ||
          cancelledFlag.toLowerCase() === "yes" ||
          cancelledFlag.toLowerCase() === "y" ||
          cancelledFlag.toLowerCase() === "true";

        if (isCancelled) continue;

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
          cancelStatus, // ✅ agregado
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

      sapSkip += PAGE_SIZE;
    }

    // --- Agregaciones ---
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
        pieCotVsEnt: { cotizado: sumCot, entregado: sumEnt, fillRate },
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
