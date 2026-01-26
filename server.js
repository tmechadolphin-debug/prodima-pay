import "dotenv/config";
import express from "express";
import cors from "cors";
import pg from "pg";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";
import multer from "multer";
import path from "path";
import fs from "fs";
import { fileURLToPath } from "url";

const { Pool } = pg;

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();

// ✅ JSON normal (para endpoints normales)
app.use(express.json({ limit: "10mb" }));

/* =========================================================
   ✅ ENV
========================================================= */
// ---- SAP ----
const SAP_BASE_URL = process.env.SAP_BASE_URL || "";
const SAP_COMPANYDB = process.env.SAP_COMPANYDB || "";
const SAP_USER = process.env.SAP_USER || "";
const SAP_PASS = process.env.SAP_PASS || "";
const SAP_WAREHOUSE = process.env.SAP_WAREHOUSE || "01";
const SAP_PRICE_LIST = process.env.SAP_PRICE_LIST || "Lista Distribuidor";

// ✅ Path donde se guardan archivos para anexos reales SAP
const SAP_ATTACH_PATH =
  process.env.SAP_ATTACH_PATH || "C:\\Documentos SAP\\Attachments";

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
// Panamá es -05:00 => -300 minutos
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

  console.log("✅ DB Schema OK (app_users, audit_events)");
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
    { typ: "user", uid: user.id, username: user.username },
    JWT_SECRET,
    { expiresIn: "30d" }
  );
}

function verifyAdmin(req, res, next) {
  try {
    const auth = String(req.headers.authorization || "");
    if (!auth.startsWith("Bearer ")) {
      return res
        .status(401)
        .json({ ok: false, message: "Falta Authorization Bearer token" });
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
      return res
        .status(401)
        .json({ ok: false, message: "Falta Authorization Bearer token" });
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
    console.log("⚠️ Faltan variables SAP en Environment");
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
   ✅ FECHA SAP FIX (Panamá)
========================================================= */
function getDateISOInOffset(offsetMinutes = -300) {
  const now = new Date();
  const utcMs = now.getTime() + now.getTimezoneOffset() * 60000;
  const localMs = utcMs + offsetMinutes * 60000;
  const local = new Date(localMs);
  return local.toISOString().slice(0, 10);
}

/* =========================================================
   ✅ HEALTH
========================================================= */
app.get("/api/health", async (req, res) => {
  res.json({
    ok: true,
    message: "✅ PRODIMA API activa",
    yappy: YAPPY_ALIAS,
    warehouse: SAP_WAREHOUSE,
    priceList: SAP_PRICE_LIST,
    db: hasDb() ? "on" : "off",
    attachPath: SAP_ATTACH_PATH,
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
   GET /api/admin/quotes?user=&client=&from=&to=&top=&skip=
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

    const top = Math.min(Number(req.query?.top || req.query?.limit || 200), 500);
    const skip = Math.max(Number(req.query?.skip || 0), 0);

    const filterParts = [];
    if (from) filterParts.push(`DocDate ge '${from}'`);
    if (to) filterParts.push(`DocDate le '${to}'`);

    const sapFilter = filterParts.length
      ? `&$filter=${encodeURIComponent(filterParts.join(" and "))}`
      : "";

    const sap = await slFetch(
      `/Quotations?$select=DocEntry,DocNum,CardCode,CardName,DocTotal,DocDate,DocumentStatus,Comments` +
        `&$orderby=DocDate desc&$top=${top}&$skip=${skip}${sapFilter}`
    );

    const values = Array.isArray(sap?.value) ? sap.value : [];

    const parseUserFromComments = (comments = "") => {
      const m = String(comments).match(/\[user:([^\]]+)\]/i);
      return m ? String(m[1]).trim() : "";
    };

    let rows = [];

    for (const q of values) {
      const rawDate = String(q.DocDate || "");
      const fechaISO = rawDate.slice(0, 10);

      const usuario = parseUserFromComments(q.Comments || "");
      const cardCode = String(q.CardCode || "").trim();

      const estado =
        q.DocumentStatus === "bost_Open"
          ? "Open"
          : q.DocumentStatus === "bost_Close"
          ? "Close"
          : String(q.DocumentStatus || "");

      const cardName = String(q.CardName || "").trim();

      let mes = "";
      let anio = "";
      try {
        const d = new Date(fechaISO);
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
        fecha: fechaISO,
        estado,
        mes,
        anio,
        usuario,
        comments: q.Comments || "",
      });
    }

    if (userFilter) {
      rows = rows.filter((r) =>
        String(r.usuario || "").toLowerCase().includes(userFilter)
      );
    }

    if (clientFilter) {
      rows = rows.filter(
        (r) =>
          String(r.cardCode || "").toLowerCase().includes(clientFilter) ||
          String(r.cardName || "").toLowerCase().includes(clientFilter)
      );
    }

    return res.json({
      ok: true,
      top,
      skip,
      count: rows.length,
      quotes: rows,
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
    if (!hasDb())
      return res.status(500).json({ ok: false, message: "DB no configurada" });

    const r = await dbQuery(`
      SELECT id, username, full_name, is_active, created_at
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
    if (!hasDb())
      return res.status(500).json({ ok: false, message: "DB no configurada" });

    const username = String(req.body?.username || "").trim().toLowerCase();
    const fullName = String(req.body?.fullName || req.body?.full_name || "").trim();
    const pin = String(req.body?.pin || "").trim();

    if (!username)
      return res.status(400).json({ ok: false, message: "username requerido" });
    if (!pin || pin.length < 4)
      return res.status(400).json({ ok: false, message: "PIN mínimo 4" });

    const pin_hash = await bcrypt.hash(pin, 10);

    const ins = await dbQuery(
      `
      INSERT INTO app_users(username, full_name, pin_hash, is_active)
      VALUES ($1,$2,$3,TRUE)
      RETURNING id, username, full_name, is_active, created_at;
      `,
      [username, fullName, pin_hash]
    );

    await audit("USER_CREATED", req, "ADMIN", { username, fullName });

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
    if (!hasDb())
      return res.status(500).json({ ok: false, message: "DB no configurada" });

    const id = Number(req.params.id || 0);
    if (!id) return res.status(400).json({ ok: false, message: "id inválido" });

    const r = await dbQuery(`DELETE FROM app_users WHERE id = $1 RETURNING id, username;`, [
      id,
    ]);

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
   ✅ ADMIN: TOGGLE ACTIVO (opcional)
========================================================= */
app.patch("/api/admin/users/:id/toggle", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb())
      return res.status(500).json({ ok: false, message: "DB no configurada" });

    const id = Number(req.params.id || 0);
    if (!id) return res.status(400).json({ ok: false, message: "id inválido" });

    const r = await dbQuery(
      `
      UPDATE app_users
      SET is_active = NOT is_active
      WHERE id = $1
      RETURNING id, username, full_name, is_active, created_at;
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
      SELECT id, username, full_name, pin_hash, is_active
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

    const token = signUserToken(user);
    await audit("USER_LOGIN_OK", req, username, { username });

    return res.json({
      ok: true,
      token,
      user: {
        id: user.id,
        username: user.username,
        full_name: user.full_name || "",
      },
    });
  } catch (e) {
    console.error("❌ /api/auth/login:", e.message);
    return res.status(500).json({ ok: false, message: e.message });
  }
});

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
      const r2 = await slFetch(
        `/PriceLists?$select=PriceListNo,ListName&$filter=ListName eq '${safe}'`
      );
      if (r2?.value?.length) no = r2.value[0].PriceListNo;
    } catch {}
  }

  PRICE_LIST_CACHE = { name, no, ts: now };
  return no;
}

function buildItemResponse(itemFull, code, priceListNo) {
  const item = {
    ItemCode: itemFull.ItemCode ?? code,
    ItemName: itemFull.ItemName ?? `Producto ${code}`,
    SalesUnit: itemFull.SalesUnit ?? "",
    InventoryItem: itemFull.InventoryItem ?? null,
  };

  let price = null;
  if (priceListNo !== null && Array.isArray(itemFull.ItemPrices)) {
    const p = itemFull.ItemPrices.find((x) => Number(x.PriceList) === Number(priceListNo));
    if (p && p.Price != null) price = Number(p.Price);
  }

  let wh = null;
  if (Array.isArray(itemFull.ItemWarehouseInfoCollection)) {
    wh = itemFull.ItemWarehouseInfoCollection.find(
      (x) => String(x.WarehouseCode) === String(SAP_WAREHOUSE)
    );
  }

  const onHand = wh?.InStock ?? wh?.OnHand ?? wh?.QuantityOnStock ?? null;
  const committed = wh?.Committed ?? 0;
  const available = onHand !== null ? Number(onHand) - Number(committed) : null;

  return {
    ok: true,
    item,
    price,
    stock: {
      onHand,
      committed,
      available,
      hasStock: available !== null ? available > 0 : null,
    },
  };
}

async function getOneItem(code, priceListNo) {
  const now = Date.now();
  const cached = ITEM_CACHE.get(code);
  if (cached && now - cached.ts < ITEM_TTL_MS) {
    return cached.data;
  }

  let itemFull;
  try {
    itemFull = await slFetch(
      `/Items('${encodeURIComponent(code)}')?$select=ItemCode,ItemName,SalesUnit,InventoryItem,ItemPrices,ItemWarehouseInfoCollection`
    );
  } catch {
    itemFull = await slFetch(`/Items('${encodeURIComponent(code)}')`);
  }

  const data = buildItemResponse(itemFull, code, priceListNo);
  ITEM_CACHE.set(code, { ts: now, data });
  return data;
}

/* =========================================================
   ✅ SAP: ITEM (1)
========================================================= */
app.get("/api/sap/item/:code", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) {
      return res.status(400).json({ ok: false, message: "Faltan variables SAP" });
    }

    const code = String(req.params.code || "").trim();
    if (!code) return res.status(400).json({ ok: false, message: "ItemCode vacío." });

    const priceListNo = await getPriceListNoByNameCached(SAP_PRICE_LIST);
    const r = await getOneItem(code, priceListNo);

    return res.json({
      ok: true,
      item: r.item,
      warehouse: SAP_WAREHOUSE,
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
   ✅ SAP: CUSTOMER
========================================================= */
app.get("/api/sap/customer/:code", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) {
      return res.status(400).json({ ok: false, message: "Faltan variables SAP" });
    }

    const code = String(req.params.code || "").trim();
    if (!code) return res.status(400).json({ ok: false, message: "CardCode vacío." });

    const bp = await slFetch(
      `/BusinessPartners('${encodeURIComponent(
        code
      )}')?$select=CardCode,CardName,Phone1,Phone2,EmailAddress,Address,City,Country,ZipCode`
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
   ✅ ATTACHMENTS (MULTER + SAP Attachments2)
========================================================= */
function ensureDirSync(dir) {
  try {
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  } catch (e) {
    console.error("❌ No se pudo crear carpeta attach:", dir, e.message);
  }
}

ensureDirSync(SAP_ATTACH_PATH);

// ✅ Config Multer (guardado en disco)
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    ensureDirSync(SAP_ATTACH_PATH);
    cb(null, SAP_ATTACH_PATH);
  },
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname || "");
    const base = path
      .basename(file.originalname || "archivo", ext)
      .replace(/[^\w\-]+/g, "_")
      .slice(0, 60);

    const stamp = new Date().toISOString().replace(/[:.]/g, "-");
    cb(null, `${base}_${stamp}${ext}`);
  },
});

const upload = multer({
  storage,
  limits: { fileSize: 15 * 1024 * 1024 }, // 15MB por archivo
});

// ✅ Crea entry de anexos en SAP (Attachments2)
async function createSapAttachmentEntry(files) {
  // files: [{ filename, path }]
  if (!files || !files.length) return null;

  const lines = files.map((f) => {
    const ext = path.extname(f.filename).replace(".", "").toLowerCase();
    const base = path.basename(f.filename, path.extname(f.filename));

    return {
      FileName: base,
      FileExtension: ext,
      SourcePath: SAP_ATTACH_PATH, // ✅ carpeta en servidor
    };
  });

  // SAP espera Attachments2_Lines
  const payload = {
    Attachments2_Lines: lines,
  };

  const created = await slFetch(`/Attachments2`, {
    method: "POST",
    body: JSON.stringify(payload),
  });

  // Normalmente retorna AbsoluteEntry
  const entry = created?.AbsoluteEntry ?? created?.AttachmentEntry ?? null;

  if (!entry) {
    console.log("⚠️ SAP Attachments2 creado pero no vino AbsoluteEntry, respuesta:", created);
  }

  return entry;
}

/* =========================================================
   ✅ SAP: CREAR COTIZACIÓN (JSON normal sin archivos)
========================================================= */
app.post("/api/sap/quote", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) {
      return res.status(400).json({ ok: false, message: "Faltan variables SAP" });
    }

    const cardCode = String(req.body?.cardCode || "").trim();
    const comments = String(req.body?.comments || "").trim();
    const lines = Array.isArray(req.body?.lines) ? req.body.lines : [];

    if (!cardCode) return res.status(400).json({ ok: false, message: "cardCode requerido." });
    if (!lines.length) return res.status(400).json({ ok: false, message: "lines requerido." });

    const DocumentLines = lines
      .map((l) => ({
        ItemCode: String(l.itemCode || "").trim(),
        Quantity: Number(l.qty || 0),
      }))
      .filter((x) => x.ItemCode && x.Quantity > 0);

    if (!DocumentLines.length) {
      return res.status(400).json({ ok: false, message: "No hay líneas válidas (qty>0)." });
    }

    const docDate = getDateISOInOffset(TZ_OFFSET_MIN);
    const creator = req.user?.username || "unknown";

    const sapComments = [
      `[WEB PEDIDOS]`,
      `[user:${creator}]`,
      comments ? comments : "Cotización mercaderista",
    ].join(" ");

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
      attachments: 0,
    });

    return res.json({
      ok: true,
      message: "Cotización creada",
      docEntry: created.DocEntry,
      docNum: created.DocNum,
    });
  } catch (err) {
    console.error("❌ /api/sap/quote:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ SAP: CREAR COTIZACIÓN + ARCHIVOS (MULTIPART)
   POST /api/sap/quote-multipart
   FormData:
     - cardCode: "C01133"
     - comments: "...."
     - lines: JSON string
     - files: (1..n)
========================================================= */
app.post(
  "/api/sap/quote-multipart",
  verifyUser,
  upload.array("files", 10),
  async (req, res) => {
    try {
      if (missingSapEnv()) {
        return res.status(400).json({ ok: false, message: "Faltan variables SAP" });
      }

      const cardCode = String(req.body?.cardCode || "").trim();
      const comments = String(req.body?.comments || "").trim();
      const linesRaw = String(req.body?.lines || "[]");

      let lines = [];
      try {
        lines = JSON.parse(linesRaw);
      } catch {
        lines = [];
      }

      if (!cardCode)
        return res.status(400).json({ ok: false, message: "cardCode requerido." });
      if (!Array.isArray(lines) || !lines.length)
        return res.status(400).json({ ok: false, message: "lines requerido." });

      const DocumentLines = lines
        .map((l) => ({
          ItemCode: String(l.itemCode || "").trim(),
          Quantity: Number(l.qty || 0),
        }))
        .filter((x) => x.ItemCode && x.Quantity > 0);

      if (!DocumentLines.length) {
        return res.status(400).json({ ok: false, message: "No hay líneas válidas (qty>0)." });
      }

      const docDate = getDateISOInOffset(TZ_OFFSET_MIN);
      const creator = req.user?.username || "unknown";

      const sapComments = [
        `[WEB PEDIDOS]`,
        `[user:${creator}]`,
        comments ? comments : "Cotización mercaderista",
      ].join(" ");

      // ✅ 1) Archivos guardados en servidor
      const files = Array.isArray(req.files) ? req.files : [];
      let attachmentEntry = null;

      // ✅ 2) Crear Attachments2 en SAP
      if (files.length) {
        attachmentEntry = await createSapAttachmentEntry(files);
      }

      // ✅ 3) Crear quotation con AttachmentEntry
      const payload = {
        CardCode: cardCode,
        DocDate: docDate,
        DocDueDate: docDate,
        Comments: sapComments,
        JournalMemo: "Cotización web mercaderistas",
        DocumentLines,
        ...(attachmentEntry ? { AttachmentEntry: Number(attachmentEntry) } : {}),
      };

      const created = await slFetch(`/Quotations`, {
        method: "POST",
        body: JSON.stringify(payload),
      });

      await audit("QUOTE_CREATED", req, creator, {
        cardCode,
        lines: DocumentLines.length,
        docDate,
        attachments: files.length,
        attachmentEntry: attachmentEntry || null,
      });

      return res.json({
        ok: true,
        message: "Cotización creada con anexos",
        docEntry: created.DocEntry,
        docNum: created.DocNum,
        attachmentEntry: attachmentEntry || null,
        filesSaved: files.map((f) => f.filename),
      });
    } catch (err) {
      console.error("❌ /api/sap/quote-multipart:", err.message);
      return res.status(500).json({ ok: false, message: err.message });
    }
  }
);

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
