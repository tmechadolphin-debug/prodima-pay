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
   ✅ DB Pool (Supabase) - FIX SSL
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

    pool.on("error", (err) => console.error("❌ DB pool error:", err.message));
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
      return res.status(401).json({ ok: false, message: "Falta Authorization Bearer token" });

    const token = auth.replace("Bearer ", "").trim();
    const decoded = jwt.verify(token, JWT_SECRET);

    if (!decoded || decoded.typ !== "admin")
      return res.status(403).json({ ok: false, message: "Token inválido" });

    req.admin = decoded;
    next();
  } catch {
    return res.status(401).json({ ok: false, message: "Token expirado o inválido" });
  }
}

/**
 * ✅ FIX #1: verifyUser rehidrata desde DB
 * - No dependes del warehouse_code viejo del JWT
 * - Valida is_active real
 */
async function verifyUser(req, res, next) {
  try {
    const auth = String(req.headers.authorization || "");
    if (!auth.startsWith("Bearer "))
      return res.status(401).json({ ok: false, message: "Falta Authorization Bearer token" });

    const token = auth.replace("Bearer ", "").trim();
    const decoded = jwt.verify(token, JWT_SECRET);

    if (!decoded || decoded.typ !== "user")
      return res.status(403).json({ ok: false, message: "Token inválido" });

    // ✅ rehidratar desde DB (si existe)
    if (hasDb()) {
      const uid = Number(decoded.uid || 0);
      let r = null;

      if (uid) {
        r = await dbQuery(
          `SELECT id, username, full_name, is_active, province, warehouse_code
           FROM app_users
           WHERE id = $1
           LIMIT 1`,
          [uid]
        );
      } else {
        const uname = String(decoded.username || "").trim().toLowerCase();
        r = await dbQuery(
          `SELECT id, username, full_name, is_active, province, warehouse_code
           FROM app_users
           WHERE username = $1
           LIMIT 1`,
          [uname]
        );
      }

      if (!r || !r.rowCount) {
        return res.status(401).json({ ok: false, message: "Usuario no existe (DB)" });
      }

      const u = r.rows[0];
      if (!u.is_active) {
        return res.status(401).json({ ok: false, message: "Usuario desactivado" });
      }

      // ✅ usa datos reales de DB
      req.user = {
        typ: "user",
        uid: u.id,
        username: u.username,
        full_name: u.full_name || "",
        province: u.province || "",
        warehouse_code: String(u.warehouse_code || "").trim(),
      };
    } else {
      // fallback sin DB
      req.user = decoded;
    }

    next();
  } catch {
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

  if (!res.ok) throw new Error(`SAP error ${res.status}: ${text}`);

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
   ✅ Helpers: Comments -> usuario / cancelado
========================================================= */
function parseUserFromComments(comments = "") {
  const m = String(comments).match(/\[user:([^\]]+)\]/i);
  return m ? String(m[1]).trim() : "";
}

function isCancelledByCancelStatus(cancelStatus) {
  const cs = String(cancelStatus || "").trim().toLowerCase();
  return cs === "csyes";
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
    if (msg.includes("duplicate") || msg.includes("unique"))
      return res.status(400).json({ ok: false, message: "Ese username ya existe" });
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
    return res.status(500).json({ ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ ENTREGADO (Cotizado vs Entregado) - ✅ FIX REAL
   - Fast path: any() (si SAP lo soporta)
   - Fallback: leer DocumentLines y verificar BaseType/BaseEntry
   - Cache corto para rendimiento
========================================================= */
const DELIVERED_CACHE = new Map(); // key: quoteDocEntry -> { ts, deliveredTotal }
const DELIVERED_TTL_MS = 60 * 1000;

function addDaysISO(isoYYYYMMDD, days) {
  // isoYYYYMMDD: "2026-02-02"
  const d = new Date(`${isoYYYYMMDD}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + Number(days || 0));
  return d.toISOString().slice(0, 10);
}

async function slTry(path, options = {}) {
  try {
    const json = await slFetch(path, options);
    return { ok: true, json };
  } catch (e) {
    return { ok: false, error: String(e?.message || e) };
  }
}

async function fetchPaged(entityPath, select, orderby, filter, top = 500, max = 5000) {
  const out = [];
  for (let skip = 0; skip < max; skip += top) {
    const url =
      `/${entityPath}?$select=${select}` +
      (orderby ? `&$orderby=${encodeURIComponent(orderby)}` : "") +
      (filter ? `&$filter=${encodeURIComponent(filter)}` : "") +
      `&$top=${top}&$skip=${skip}`;

    const r = await slFetch(url);
    const vals = Array.isArray(r?.value) ? r.value : [];
    if (!vals.length) break;
    out.push(...vals);
    if (vals.length < top) break;
  }
  return out;
}

async function getQuotationHeader(docEntry) {
  const r = await slFetch(
    `/Quotations(${Number(docEntry)})?$select=DocEntry,DocNum,DocDate,CancelStatus,DocTotal`
  );
  return r || null;
}

/** Fast path (si any() funciona) */
async function getOrdersBasedOnQuotationAny(quoteDocEntry) {
  const filter = `DocumentLines/any(d: d/BaseType eq 23 and d/BaseEntry eq ${Number(quoteDocEntry)})`;
  const r = await slFetch(
    `/Orders?$select=DocEntry,DocNum,DocTotal,DocDate,CancelStatus&$filter=${encodeURIComponent(
      filter
    )}&$top=200`
  );
  return Array.isArray(r?.value) ? r.value : [];
}

/** Fallback: buscar Orders por rango de fecha y luego revisar DocumentLines(BaseType/BaseEntry) */
async function getOrdersBasedOnQuotationFallback(quoteDocEntry) {
  const q = await getQuotationHeader(quoteDocEntry);
  const qDate = String(q?.DocDate || "").slice(0, 10);
  if (!qDate) return [];

  // ventana razonable alrededor de la cotización
  const from = addDaysISO(qDate, -45);
  const to = addDaysISO(qDate, 45);

  const orders = await fetchPaged(
    "Orders",
    "DocEntry,DocNum,DocTotal,DocDate,CancelStatus",
    "DocDate desc,DocEntry desc",
    `DocDate ge '${from}' and DocDate le '${to}'`,
    500,
    5000
  );

  // revisar líneas de cada Order (concurrency)
  const matches = [];
  let idx = 0;
  const CONC = 4;

  async function worker() {
    while (idx < orders.length) {
      const i = idx++;
      const o = orders[i];
      const de = o?.DocEntry;
      if (!de) continue;
      if (String(o?.CancelStatus || "").toLowerCase() === "csyes") continue;

      const r = await slTry(
        `/Orders(${Number(de)})?$select=DocEntry&$expand=DocumentLines($select=BaseType,BaseEntry)`
      );
      if (!r.ok) continue;

      const lines = Array.isArray(r.json?.DocumentLines) ? r.json.DocumentLines : [];
      const hit = lines.some(
        (ln) => Number(ln?.BaseType) === 23 && Number(ln?.BaseEntry) === Number(quoteDocEntry)
      );
      if (hit) matches.push(o);
    }
  }

  await Promise.all(Array.from({ length: Math.min(CONC, orders.length || 1) }, worker));
  return matches;
}

/** Fast path deliveries */
async function getDeliveriesBasedOnOrderAny(orderDocEntry) {
  const filter = `DocumentLines/any(d: d/BaseType eq 17 and d/BaseEntry eq ${Number(orderDocEntry)})`;
  const r = await slFetch(
    `/DeliveryNotes?$select=DocEntry,DocNum,DocTotal,DocDate,CancelStatus&$filter=${encodeURIComponent(
      filter
    )}&$top=500`
  );
  return Array.isArray(r?.value) ? r.value : [];
}

/** Fallback deliveries: buscar por rango de fecha y revisar líneas */
async function getDeliveriesBasedOnOrderFallback(orderDocEntry, baseDateISO) {
  // baseDateISO ayuda a recortar la búsqueda
  const base = String(baseDateISO || "").slice(0, 10);
  const from = base ? addDaysISO(base, -45) : "2000-01-01";
  const to = base ? addDaysISO(base, 45) : addDaysISO(getDateISOInOffset(TZ_OFFSET_MIN), 1);

  const dels = await fetchPaged(
    "DeliveryNotes",
    "DocEntry,DocNum,DocTotal,DocDate,CancelStatus",
    "DocDate desc,DocEntry desc",
    `DocDate ge '${from}' and DocDate le '${to}'`,
    500,
    5000
  );

  const matches = [];
  let idx = 0;
  const CONC = 4;

  async function worker() {
    while (idx < dels.length) {
      const i = idx++;
      const d = dels[i];
      const de = d?.DocEntry;
      if (!de) continue;
      if (String(d?.CancelStatus || "").toLowerCase() === "csyes") continue;

      const r = await slTry(
        `/DeliveryNotes(${Number(de)})?$select=DocEntry&$expand=DocumentLines($select=BaseType,BaseEntry)`
      );
      if (!r.ok) continue;

      const lines = Array.isArray(r.json?.DocumentLines) ? r.json.DocumentLines : [];
      const hit = lines.some(
        (ln) => Number(ln?.BaseType) === 17 && Number(ln?.BaseEntry) === Number(orderDocEntry)
      );
      if (hit) matches.push(d);
    }
  }

  await Promise.all(Array.from({ length: Math.min(CONC, dels.length || 1) }, worker));
  return matches;
}

async function computeDeliveredForQuotation(quoteDocEntry) {
  const key = String(quoteDocEntry);
  const now = Date.now();
  const cached = DELIVERED_CACHE.get(key);
  if (cached && now - cached.ts < DELIVERED_TTL_MS) return cached.deliveredTotal;

  let deliveredTotal = 0;

  const qHead = await slTry(
    `/Quotations(${Number(quoteDocEntry)})?$select=DocEntry,DocDate,CancelStatus`
  );
  if (!qHead.ok) {
    DELIVERED_CACHE.set(key, { ts: now, deliveredTotal: 0 });
    return 0;
  }
  const qDate = String(qHead.json?.DocDate || "").slice(0, 10);
  const qCancelled = String(qHead.json?.CancelStatus || "").toLowerCase() === "csyes";
  if (qCancelled) {
    DELIVERED_CACHE.set(key, { ts: now, deliveredTotal: 0 });
    return 0;
  }

  // 1) Orders: try any() primero
  let orders = [];
  const tryAnyOrders = await slTry(
    `/Orders?$select=DocEntry,DocNum,DocTotal,DocDate,CancelStatus&$filter=${encodeURIComponent(
      `DocumentLines/any(d: d/BaseType eq 23 and d/BaseEntry eq ${Number(quoteDocEntry)})`
    )}&$top=200`
  );

  if (tryAnyOrders.ok) {
    orders = Array.isArray(tryAnyOrders.json?.value) ? tryAnyOrders.json.value : [];
  } else {
    // fallback 100% compatible
    orders = await getOrdersBasedOnQuotationFallback(quoteDocEntry);
  }

  if (!orders.length) {
    DELIVERED_CACHE.set(key, { ts: now, deliveredTotal: 0 });
    return 0;
  }

  // 2) Deliveries por cada Order
  let idx = 0;
  const CONCURRENCY = 4;

  async function worker() {
    while (idx < orders.length) {
      const i = idx++;
      const o = orders[i];
      const orderDocEntry = o?.DocEntry;
      if (!orderDocEntry) continue;
      if (String(o?.CancelStatus || "").toLowerCase() === "csyes") continue;

      // try any()
      const tryAnyDels = await slTry(
        `/DeliveryNotes?$select=DocEntry,DocNum,DocTotal,DocDate,CancelStatus&$filter=${encodeURIComponent(
          `DocumentLines/any(d: d/BaseType eq 17 and d/BaseEntry eq ${Number(orderDocEntry)})`
        )}&$top=500`
      );

      let deliveries = [];
      if (tryAnyDels.ok) {
        deliveries = Array.isArray(tryAnyDels.json?.value) ? tryAnyDels.json.value : [];
      } else {
        deliveries = await getDeliveriesBasedOnOrderFallback(orderDocEntry, qDate);
      }

      for (const d of deliveries) {
        if (String(d?.CancelStatus || "").toLowerCase() === "csyes") continue;
        const n = Number(d?.DocTotal || 0);
        if (Number.isFinite(n)) deliveredTotal += n;
      }
    }
  }

  await Promise.all(Array.from({ length: Math.min(CONCURRENCY, orders.length || 1) }, worker));

  DELIVERED_CACHE.set(key, { ts: now, deliveredTotal });
  return deliveredTotal;
}

function calcEntregaFields(montoCotizado, montoEntregado, isCancelled) {
  const cot = Number(montoCotizado || 0);
  const ent = Number(montoEntregado || 0);

  const safeCot = Number.isFinite(cot) ? cot : 0;
  const safeEnt = Number.isFinite(ent) ? ent : 0;

  const pendiente = Math.max(0, safeCot - safeEnt);
  const pct = safeCot > 0 ? Math.min(100, Math.max(0, (safeEnt / safeCot) * 100)) : 0;

  const estadoEntrega = isCancelled
    ? "Cancelled"
    : safeCot > 0 && safeEnt >= safeCot - 0.01
    ? "Entregado"
    : safeEnt > 0
    ? "Parcial"
    : "No entregado";

  return {
    montoEntregado: safeEnt,
    montoPendiente: pendiente,
    entregadoPct: pct,
    entregado: estadoEntrega === "Entregado",
    estadoEntrega,
  };
}

/* =========================================================
   ✅ ADMIN: HISTÓRICO DE COTIZACIONES (SAP)  ✅ CORREGIDO
   - ✅ FIX DUPLICADO: orden estable (DocDate desc, DocEntry desc)
   - ✅ DEDUPE por DocEntry (por si SAP repite)
   - ✅ AGREGA Entregado (cotizado vs entregado)
   - Paginación real (hasta 500 por página)
========================================================= */
app.get("/api/admin/quotes", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) {
      return res.status(400).json({ ok: false, message: "Faltan variables SAP" });
    }

    const limit = Math.min(Math.max(Number(req.query?.limit || req.query?.top || 500), 1), 500);
    const skip = Math.max(Number(req.query?.skip || 0), 0);

    const from = String(req.query?.from || "").trim();
    const to = String(req.query?.to || "").trim();

    const filterParts = [];
    if (from) filterParts.push(`DocDate ge '${from}'`);
    if (to) filterParts.push(`DocDate le '${to}'`);

    const sapFilter = filterParts.length
      ? `&$filter=${encodeURIComponent(filterParts.join(" and "))}`
      : "";

    const SELECT =
      `DocEntry,DocNum,CardCode,CardName,DocTotal,DocDate,DocumentStatus,CancelStatus,Comments`;

    // ✅ FIX DUPLICADO: order estable, no solo DocDate
    const sap = await slFetch(
      `/Quotations?$select=${SELECT}` +
        `&$orderby=DocDate desc,DocEntry desc&$top=${limit}&$skip=${skip}${sapFilter}`
    );

    const values = Array.isArray(sap?.value) ? sap.value : [];

    // ✅ DEDUPE por DocEntry
    const seen = new Set();
    const deduped = [];
    for (const q of values) {
      const de = q?.DocEntry;
      if (de == null) continue;
      const k = String(de);
      if (seen.has(k)) continue;
      seen.add(k);
      deduped.push(q);
    }

    // ✅ Entregado: calcular en paralelo (con cache)
    const CONCURRENCY = 4;
    let idx = 0;
    const enriched = new Array(deduped.length);

    async function worker() {
      while (idx < deduped.length) {
        const i = idx++;
        const q = deduped[i];

        const fechaISO = String(q.DocDate || "").slice(0, 10);

        const cancelStatus = String(q.CancelStatus || "").trim(); // csYes/csNo
        const isCancelled = isCancelledByCancelStatus(cancelStatus);

        const estado = isCancelled
          ? "Cancelled"
          : q.DocumentStatus === "bost_Open"
          ? "Open"
          : q.DocumentStatus === "bost_Close"
          ? "Close"
          : String(q.DocumentStatus || "");

        const usuario = parseUserFromComments(q.Comments || "");

        const montoCotizacion = Number(q.DocTotal || 0);

        let montoEntregado = 0;
        if (!isCancelled) {
          try {
            montoEntregado = await computeDeliveredForQuotation(q.DocEntry);
          } catch {
            montoEntregado = 0;
          }
        }

        const entrega = calcEntregaFields(montoCotizacion, montoEntregado, isCancelled);

        enriched[i] = {
          docEntry: q.DocEntry,
          docNum: q.DocNum,
          cardCode: String(q.CardCode || "").trim(),
          cardName: String(q.CardName || "").trim(),
          montoCotizacion: Number(montoCotizacion || 0),
          fecha: fechaISO,
          estado,
          cancelStatus,
          isCancelled,
          usuario,
          comments: q.Comments || "",

          // ✅ NUEVO (NO ROMPE LO EXISTENTE)
          Entregado: entrega.estadoEntrega, // "Entregado" | "Parcial" | "No entregado" | "Cancelled"
          montoEntregado: entrega.montoEntregado,
          montoPendiente: entrega.montoPendiente,
          entregadoPct: entrega.entregadoPct,
          entregado: entrega.entregado,
        };
      }
    }

    await Promise.all(Array.from({ length: Math.min(CONCURRENCY, deduped.length || 1) }, worker));

    return res.json({
      ok: true,
      limit,
      skip,
      count: enriched.length,
      quotes: enriched.filter(Boolean),
    });
  } catch (err) {
    console.error("❌ /api/admin/quotes:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ ADMIN: DASHBOARD (MEJORA + ENTREGADO)
   - Trae cotizaciones por rango (paginando en SAP) y agrega:
     total cotizado / entregado / pendiente
     por usuario y por día
   - NO TOCA LOGIN NI CREAR COTIZACIÓN
========================================================= */
app.get("/api/admin/dashboard", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) {
      return res.status(400).json({ ok: false, message: "Faltan variables SAP" });
    }

    const from = String(req.query?.from || "").trim(); // YYYY-MM-DD
    const to = String(req.query?.to || "").trim(); // YYYY-MM-DD
    const userFilter = String(req.query?.user || req.query?.usuario || "").trim().toLowerCase();

    // Safety: si no mandan fechas, usamos hoy
    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from2 = from || today;
    const to2 = to || today;

    // Traer en páginas de 500 (hasta un máximo razonable)
    const PAGE = 500;
    const MAX_DOCS = 5000;

    const filterParts = [];
    if (from2) filterParts.push(`DocDate ge '${from2}'`);
    if (to2) filterParts.push(`DocDate le '${to2}'`);
    const sapFilter = filterParts.length
      ? `&$filter=${encodeURIComponent(filterParts.join(" and "))}`
      : "";

    const SELECT =
      `DocEntry,DocNum,CardCode,CardName,DocTotal,DocDate,DocumentStatus,CancelStatus,Comments`;

    let all = [];
    for (let skip = 0; skip < MAX_DOCS; skip += PAGE) {
      const sap = await slFetch(
        `/Quotations?$select=${SELECT}` +
          `&$orderby=DocDate desc,DocEntry desc&$top=${PAGE}&$skip=${skip}${sapFilter}`
      );
      const vals = Array.isArray(sap?.value) ? sap.value : [];
      if (!vals.length) break;
      all = all.concat(vals);
      if (vals.length < PAGE) break;
    }

    // Dedupe DocEntry
    const seen = new Set();
    const quotes = [];
    for (const q of all) {
      const de = q?.DocEntry;
      if (de == null) continue;
      const k = String(de);
      if (seen.has(k)) continue;
      seen.add(k);

      const usuario = parseUserFromComments(q.Comments || "");
      if (userFilter && String(usuario || "").toLowerCase() !== userFilter) continue;

      quotes.push(q);
    }

    // Calcular entregado en paralelo
    const CONCURRENCY = 6;
    let idx = 0;

    let totalCotizado = 0;
    let totalEntregado = 0;
    let totalPendiente = 0;

    let countCotizaciones = 0;
    let countEntregadas = 0;
    let countPendientes = 0;

    const byUser = new Map();
    const byDay = new Map();

    async function worker() {
      while (idx < quotes.length) {
        const i = idx++;
        const q = quotes[i];

        const cancelStatus = String(q.CancelStatus || "").trim();
        const isCancelled = isCancelledByCancelStatus(cancelStatus);
        if (isCancelled) continue;

        const fecha = String(q.DocDate || "").slice(0, 10);
        const usuario = parseUserFromComments(q.Comments || "") || "(sin usuario)";
        const cot = Number(q.DocTotal || 0);

        let ent = 0;
        try {
          ent = await computeDeliveredForQuotation(q.DocEntry);
        } catch {
          ent = 0;
        }

        const entrega = calcEntregaFields(cot, ent, false);

        totalCotizado += Number.isFinite(cot) ? cot : 0;
        totalEntregado += Number.isFinite(ent) ? ent : 0;
        totalPendiente += Number.isFinite(entrega.montoPendiente) ? entrega.montoPendiente : 0;

        countCotizaciones += 1;
        if (entrega.entregado) countEntregadas += 1;
        else countPendientes += 1;

        // byUser
        const ukey = String(usuario);
        const u = byUser.get(ukey) || {
          usuario: ukey,
          count: 0,
          cotizado: 0,
          entregado: 0,
          pendiente: 0,
          entregadas: 0,
          pendientes: 0,
        };
        u.count += 1;
        u.cotizado += Number.isFinite(cot) ? cot : 0;
        u.entregado += Number.isFinite(ent) ? ent : 0;
        u.pendiente += Number.isFinite(entrega.montoPendiente) ? entrega.montoPendiente : 0;
        if (entrega.entregado) u.entregadas += 1;
        else u.pendientes += 1;
        byUser.set(ukey, u);

        // byDay
        const dkey = String(fecha);
        const d = byDay.get(dkey) || {
          fecha: dkey,
          count: 0,
          cotizado: 0,
          entregado: 0,
          pendiente: 0,
          entregadas: 0,
          pendientes: 0,
        };
        d.count += 1;
        d.cotizado += Number.isFinite(cot) ? cot : 0;
        d.entregado += Number.isFinite(ent) ? ent : 0;
        d.pendiente += Number.isFinite(entrega.montoPendiente) ? entrega.montoPendiente : 0;
        if (entrega.entregado) d.entregadas += 1;
        else d.pendientes += 1;
        byDay.set(dkey, d);
      }
    }

    await Promise.all(Array.from({ length: Math.min(CONCURRENCY, quotes.length || 1) }, worker));

    const byUserArr = Array.from(byUser.values()).sort((a, b) => b.cotizado - a.cotizado);
    const byDayArr = Array.from(byDay.values()).sort((a, b) => (a.fecha < b.fecha ? -1 : 1));

    return res.json({
      ok: true,
      from: from2,
      to: to2,
      user: userFilter || "",
      totals: {
        cotizado: totalCotizado,
        entregado: totalEntregado,
        pendiente: totalPendiente,
        entregadoPct: totalCotizado > 0 ? Math.min(100, (totalEntregado / totalCotizado) * 100) : 0,
      },
      counts: {
        cotizaciones: countCotizaciones,
        entregadas: countEntregadas,
        pendientes: countPendientes,
      },
      byUser: byUserArr,
      byDay: byDayArr,
    });
  } catch (err) {
    console.error("❌ /api/admin/dashboard:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
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
   ⚠️ NO TOCADO
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
   ✅ FIX #2: Stock usando el patrón que A TI te funciona
   - ItemWarehouseInfoCollection en $select (NO $expand)
   - fallback si no viene colección
========================================================= */
function buildItemResponse(itemFull, code, priceListNo, warehouseCode) {
  const item = {
    ItemCode: itemFull.ItemCode ?? code,
    ItemName: itemFull.ItemName ?? `Producto ${code}`,
    SalesUnit: itemFull.SalesUnit ?? "",
    InventoryItem: itemFull.InventoryItem ?? null,
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

  // ✅ IMPORTANTE: NO expandimos ItemWarehouseInfoCollection (como tu viejo)
  try {
    itemFull = await slFetch(
      `/Items('${encodeURIComponent(code)}')` +
        `?$select=ItemCode,ItemName,SalesUnit,InventoryItem,ItemPrices,ItemWarehouseInfoCollection` +
        `&$expand=ItemUnitOfMeasurementCollection($select=UoMType,UoMCode,UoMEntry,BaseQuantity,AlternateQuantity)`
    );
  } catch (e1) {
    try {
      itemFull = await slFetch(
        `/Items('${encodeURIComponent(code)}')` +
          `?$select=ItemCode,ItemName,SalesUnit,InventoryItem,ItemPrices,ItemWarehouseInfoCollection`
      );
    } catch (e2) {
      itemFull = await slFetch(`/Items('${encodeURIComponent(code)}')`);
    }
  }

  // ✅ fallback: si SAP no trajo la colección, la pedimos por endpoint alterno
  if (!Array.isArray(itemFull?.ItemWarehouseInfoCollection)) {
    try {
      const whInfo = await slFetch(
        `/Items('${encodeURIComponent(code)}')/ItemWarehouseInfoCollection?$select=WarehouseCode,InStock,Committed,Ordered`
      );
      if (Array.isArray(whInfo?.value)) {
        itemFull.ItemWarehouseInfoCollection = whInfo.value;
      }
    } catch {}
  }

  const data = buildItemResponse(itemFull, code, priceListNo, warehouseCode);
  ITEM_CACHE.set(key, { ts: now, data });
  return data;
}

/* =========================================================
   ✅ SAP: ITEM (warehouse dinámico) + ✅ disponible top-level
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
      bodega: warehouseCode,
      priceList: SAP_PRICE_LIST,
      priceListNo,
      price: Number(r.price ?? 0),
      priceUnit: r.priceUnit,
      factorCaja: r.factorCaja,
      uom: r.item?.SalesUnit || "Caja",
      stock: r.stock,

      // ✅ para tu columna “Disponible”
      disponible: r?.stock?.available ?? null,
      enStock: r?.stock?.hasStock ?? null,
    });
  } catch (err) {
    console.error("❌ /api/sap/item:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ SAP: MULTI ITEMS (warehouse dinámico) + disponible
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
            unit: r.item.SalesUnit,
            price: r.price,
            priceUnit: r.priceUnit,
            factorCaja: r.factorCaja,
            stock: r.stock,

            // ✅ para tu columna “Disponible”
            disponible: r?.stock?.available ?? null,
            enStock: r?.stock?.hasStock ?? null,
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
      bodega: warehouseCode,
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
   ✅ SAP: SEARCH CUSTOMERS
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
   ✅ SAP: CREAR COTIZACIÓN
   ⚠️ NO TOCADO
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

    if (!DocumentLines.length)
      return res.status(400).json({ ok: false, message: "No hay líneas válidas (qty>0)." });

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
      bodega: warehouseCode,
    });
  } catch (err) {
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ START
========================================================= */
const PORT = process.env.PORT || 10000;

ensureSchema()
  .then(() => app.listen(PORT, () => console.log("✅ Server listo en puerto", PORT)))
  .catch(() => app.listen(PORT, () => console.log("✅ Server listo en puerto", PORT, "(sin DB)")));
