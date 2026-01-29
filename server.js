import express from "express";
import cors from "cors";
import pg from "pg";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";

import multer from "multer";
import fs from "fs";
import path from "path";

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
const SAP_WAREHOUSE = process.env.SAP_WAREHOUSE || "01";
const SAP_PRICE_LIST = process.env.SAP_PRICE_LIST || "Lista Distribuidor";

// ✅ Ruta de anexos SAP (IMPORTANTE)
const SAP_ATTACH_PATH = process.env.SAP_ATTACH_PATH || "";

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
   ✅ Upload (Multer) - para fotos/pdf
========================================================= */
const upload = multer({
  dest: "/tmp",
  limits: {
    files: 5,
    fileSize: 10 * 1024 * 1024, // 10MB c/u
  },
});

// ✅ Middleware: solo aplica multer si el request es multipart
function maybeUpload(req, res, next) {
  const ct = String(req.headers["content-type"] || "");
  if (ct.includes("multipart/form-data")) {
    return upload.array("files", 5)(req, res, next);
  }
  return next();
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
   ✅ DB Schema + MIGRACIONES
========================================================= */
async function ensureSchema() {
  if (!hasDb()) {
    console.log("⚠️ DATABASE_URL no configurado (DB deshabilitada)");
    return;
  }

  // Base
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

  // ✅ Migraciones seguras (para que no te falle el SELECT de admin/users)
  await dbQuery(`ALTER TABLE app_users ADD COLUMN IF NOT EXISTS province TEXT DEFAULT '';`);
  await dbQuery(`ALTER TABLE app_users ADD COLUMN IF NOT EXISTS warehouse_code TEXT DEFAULT '';`);

  console.log("✅ DB Schema OK (app_users, audit_events) + migraciones");
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
   ✅ Helpers parse tags en Comments
========================================================= */
function parseTag(comments = "", tag = "user") {
  const re = new RegExp(`\\[${tag}:([^\\]]+)\\]`, "i");
  const m = String(comments || "").match(re);
  return m ? String(m[1]).trim() : "";
}

function parseUserFromComments(comments = "") {
  return parseTag(comments, "user");
}

function parseWhFromComments(comments = "") {
  return parseTag(comments, "wh");
}

/* =========================================================
   ✅ Cache: mapa username->warehouse_code (para inferir bodega)
========================================================= */
let USER_WH_CACHE = { ts: 0, map: new Map() };
const USER_WH_TTL_MS = 30 * 1000;

async function getUserWarehouseMapCached() {
  if (!hasDb()) return new Map();
  const now = Date.now();
  if (USER_WH_CACHE.map.size && now - USER_WH_CACHE.ts < USER_WH_TTL_MS) {
    return USER_WH_CACHE.map;
  }
  const r = await dbQuery(`SELECT username, warehouse_code FROM app_users;`);
  const m = new Map();
  for (const row of r.rows || []) {
    const u = String(row.username || "").trim().toLowerCase();
    const wh = String(row.warehouse_code || "").trim();
    if (u) m.set(u, wh);
  }
  USER_WH_CACHE = { ts: now, map: m };
  return m;
}

async function getWarehouseForUsername(username) {
  const u = String(username || "").trim().toLowerCase();
  if (!u) return "";
  const map = await getUserWarehouseMapCached();
  return String(map.get(u) || "").trim();
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

async function slFetch(pathUrl, options = {}) {
  if (!SL_COOKIE || Date.now() - SL_COOKIE_TIME > 25 * 60 * 1000) {
    await slLogin();
  }

  const res = await fetch(`${SAP_BASE_URL}${pathUrl}`, {
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
    return slFetch(pathUrl, options);
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
   ✅ Helper: crear Attachments2 en SAP
========================================================= */
async function createSapAttachmentEntry(files = []) {
  if (!files.length) return null;

  if (!SAP_ATTACH_PATH) {
    throw new Error("SAP_ATTACH_PATH no configurado. No se puede adjuntar.");
  }

  try {
    if (!fs.existsSync(SAP_ATTACH_PATH)) {
      fs.mkdirSync(SAP_ATTACH_PATH, { recursive: true });
    }
  } catch (e) {
    console.warn("⚠️ No pude crear/verificar SAP_ATTACH_PATH:", e.message);
  }

  const lines = [];

  for (const f of files) {
    const originalName = String(f.originalname || "archivo");
    const ext = path.extname(originalName).replace(".", "").toLowerCase() || "dat";
    const base = path.basename(originalName, path.extname(originalName));

    const safeBase = base.replace(/[^\w\- ]+/g, "").replace(/\s+/g, "_").slice(0, 50);
    const finalName = `${safeBase}_${Date.now()}_${Math.floor(Math.random() * 9999)}.${ext}`;

    const dest = path.join(SAP_ATTACH_PATH, finalName);

    fs.copyFileSync(f.path, dest);

    try { fs.unlinkSync(f.path); } catch {}

    lines.push({
      FileName: path.basename(finalName, "." + ext),
      FileExtension: ext,
      SourcePath: SAP_ATTACH_PATH,
    });
  }

  const att = await slFetch(`/Attachments2`, {
    method: "POST",
    body: JSON.stringify({ Attachments2_Lines: lines }),
  });

  const absoluteEntry = att?.AbsoluteEntry;
  if (!absoluteEntry) throw new Error("No se creó AttachmentEntry en SAP.");

  return absoluteEntry;
}

/* =========================================================
   ✅ Health
========================================================= */
app.get("/api/health", async (req, res) => {
  res.json({
    ok: true,
    message: "✅ PRODIMA API activa",
    yappy: YAPPY_ALIAS,
    warehouse: SAP_WAREHOUSE,
    priceList: SAP_PRICE_LIST,
    db: hasDb() ? "on" : "off",
    attachmentsPath: SAP_ATTACH_PATH ? "set" : "missing",
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
   FIX: bodega visible + scope=created paginado correctamente
========================================================= */
app.get("/api/admin/quotes", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) {
      return res.status(400).json({ ok: false, message: "Faltan variables SAP" });
    }

    const scope = String(req.query?.scope || "all").trim().toLowerCase(); // created | all

    const userFilter = String(req.query?.user || "").trim().toLowerCase();
    const clientFilter = String(req.query?.client || "").trim().toLowerCase();

    const from = String(req.query?.from || "").trim();
    const to = String(req.query?.to || "").trim();

    // "top" = tamaño página que quieres devolver YA filtrado
    const top = Math.min(Number(req.query?.top || req.query?.limit || 20), 200);
    const skip = Math.max(Number(req.query?.skip || 0), 0); // para UI (página)

    // Para construir "página" con filtros, hay que traer más de SAP (porque filtro se hace en Node)
    const SAP_PAGE = Math.min(Math.max(Number(req.query?.sapPage || 200), 50), 500);
    const MAX_PAGES = Math.min(Math.max(Number(req.query?.maxPages || 30), 1), 80);

    // ✅ allowed users para scope=created
    let allowedUsersSet = null;
    if (scope === "created") {
      const whMap = await getUserWarehouseMapCached();
      allowedUsersSet = new Set([...whMap.keys()]); // usernames
    }

    const filterParts = [];
    if (from) filterParts.push(`DocDate ge '${from}'`);
    if (to) filterParts.push(`DocDate le '${to}'`);

    const sapFilter = filterParts.length
      ? `&$filter=${encodeURIComponent(filterParts.join(" and "))}`
      : "";

    // ✅ Traemos CancelStatus + Comments (para usuario y wh)
    const SELECT =
      `DocEntry,DocNum,CardCode,CardName,DocTotal,DocDate,DocumentStatus,CancelStatus,Comments`;

    const whMap = await getUserWarehouseMapCached();

    // Queremos devolver "top" registros DESPUÉS de aplicar filtros, empezando en "skip"
    const targetStart = skip;
    const targetEnd = skip + top;

    let rowsAllFiltered = [];
    let sapSkip = 0;

    for (let page = 0; page < MAX_PAGES; page++) {
      const sap = await slFetch(
        `/Quotations?$select=${SELECT}` +
          `&$orderby=DocDate desc&$top=${SAP_PAGE}&$skip=${sapSkip}${sapFilter}`
      );

      const values = Array.isArray(sap?.value) ? sap.value : [];
      if (!values.length) break;

      for (const q of values) {
        const fechaISO = String(q.DocDate || "").slice(0, 10);
        const cardCode = String(q.CardCode || "").trim();
        const cardName = String(q.CardName || "").trim();

        const usuario = parseUserFromComments(q.Comments || "");
        const usuarioKey = String(usuario || "").trim().toLowerCase();

        // ✅ scope created
        if (allowedUsersSet && scope === "created") {
          if (!usuarioKey || !allowedUsersSet.has(usuarioKey)) continue;
        }

        // ✅ filtros UI
        if (userFilter && !String(usuarioKey).includes(userFilter)) continue;

        if (clientFilter) {
          const cc = String(cardCode || "").toLowerCase();
          const cn = String(cardName || "").toLowerCase();
          if (!cc.includes(clientFilter) && !cn.includes(clientFilter)) continue;
        }

        // ✅ Estado Cancelled correcto
        const cancelStatus = String(q.CancelStatus || "").trim();
        const isCancelled =
          cancelStatus.toLowerCase() === "csyes" ||
          cancelStatus.toLowerCase() === "yes" ||
          cancelStatus.toLowerCase() === "tyes";

        const estado = isCancelled
          ? "Cancelled"
          : q.DocumentStatus === "bost_Open"
          ? "Open"
          : q.DocumentStatus === "bost_Close"
          ? "Close"
          : String(q.DocumentStatus || "");

        // ✅ Bodega: 1) comments [wh:] 2) inferir por usuario en DB 3) fallback
        const whFromComments = parseWhFromComments(q.Comments || "");
        const whFromDb = usuarioKey ? String(whMap.get(usuarioKey) || "") : "";
        const warehouse = (whFromComments || whFromDb || "").trim() || "sin_wh";

        rowsAllFiltered.push({
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
          isCancelled,
          usuario: usuario || "sin_user",
          bodega: warehouse,      // ✅ para UI
          warehouse: warehouse,   // ✅ alias
          comments: q.Comments || "",
        });
      }

      // Ya tenemos suficiente para cubrir hasta targetEnd
      if (rowsAllFiltered.length >= targetEnd) break;

      sapSkip += SAP_PAGE;
    }

    const pageRows = rowsAllFiltered.slice(targetStart, targetEnd);

    return res.json({
      ok: true,
      scope,
      top,
      skip,
      totalFilteredSoFar: rowsAllFiltered.length, // útil para debug
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
    if (!hasDb()) {
      return res.status(500).json({ ok: false, message: "DB no configurada" });
    }

    const r = await dbQuery(`
      SELECT
        id,
        username,
        full_name,
        is_active,
        province,
        warehouse_code,
        created_at
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
   ✅ ADMIN: CHANGE USER PIN
========================================================= */
app.patch("/api/admin/users/:id/pin", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) {
      return res.status(500).json({ ok: false, message: "DB no configurada" });
    }

    const id = Number(req.params.id);
    const pin = String(req.body?.pin || "").trim();

    if (!id || id <= 0) {
      return res.status(400).json({ ok: false, message: "ID inválido" });
    }
    if (!pin || pin.length < 4) {
      return res.status(400).json({ ok: false, message: "PIN mínimo 4" });
    }

    const pin_hash = await bcrypt.hash(pin, 10);

    const r = await dbQuery(
      `
      UPDATE app_users
      SET pin_hash = $1
      WHERE id = $2
      RETURNING id, username, full_name, is_active, province, warehouse_code, created_at;
      `,
      [pin_hash, id]
    );

    if (!r.rows?.length) {
      return res.status(404).json({ ok: false, message: "Usuario no existe" });
    }

    return res.json({ ok: true, user: r.rows[0] });
  } catch (e) {
    console.error("❌ change pin:", e.message);
    return res.status(500).json({ ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ ADMIN: DASHBOARD (KPIs + agrupaciones)
   FIX: bodega visible + Cancelled correcto + inferencia DB
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
    const MAX_PAGES = Math.min(Math.max(Number(req.query?.maxPages || 20), 1), 60);

    const whMap = await getUserWarehouseMapCached();

    let allowedUsersSet = null;
    if (scope === "created") {
      allowedUsersSet = new Set([...whMap.keys()]);
    }

    const filterParts = [];
    if (from) filterParts.push(`DocDate ge '${from}'`);
    if (to) filterParts.push(`DocDate le '${to}'`);

    const sapFilter = filterParts.length
      ? `&$filter=${encodeURIComponent(filterParts.join(" and "))}`
      : "";

    // ✅ Usamos CancelStatus (más confiable para estado)
    const SELECT =
      `DocEntry,DocNum,CardCode,CardName,DocTotal,DocDate,DocumentStatus,CancelStatus,Comments`;

    let all = [];
    let skip = 0;

    for (let page = 0; page < MAX_PAGES; page++) {
      const sap = await slFetch(
        `/Quotations?$select=${SELECT}` +
          `&$orderby=DocDate desc&$top=${PAGE_SIZE}&$skip=${skip}${sapFilter}`
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

        const cardCode = String(q.CardCode || "").trim();
        const cardName = String(q.CardName || "").trim();

        // ✅ Bodega: comments -> DB -> fallback
        const whFromComments = parseWhFromComments(q.Comments || "");
        const whFromDb = usuarioKey ? String(whMap.get(usuarioKey) || "") : "";
        const warehouse = (whFromComments || whFromDb || "").trim() || "sin_wh";

        // ✅ Estado Cancelled correcto
        const cancelStatus = String(q.CancelStatus || "").trim();
        const isCancelled =
          cancelStatus.toLowerCase() === "csyes" ||
          cancelStatus.toLowerCase() === "yes" ||
          cancelStatus.toLowerCase() === "tyes";

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

        all.push({
          docEntry: q.DocEntry,
          docNum: q.DocNum,
          cardCode,
          cardName,
          montoCotizacion: Number(q.DocTotal || 0),
          montoEntregado: 0,
          fecha: fechaISO,
          estado,
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
        pieCotVsEnt: { cotizado: sumCot, entregado: sumEnt, fillRate },
      },
    });
  } catch (err) {
    console.error("❌ /api/admin/dashboard:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ ADMIN: CREATE USER
   (agrega province + warehouse_code opcional)
========================================================= */
app.post("/api/admin/users", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return res.status(500).json({ ok: false, message: "DB no configurada" });

    const username = String(req.body?.username || "").trim().toLowerCase();
    const fullName = String(req.body?.fullName || req.body?.full_name || "").trim();
    const pin = String(req.body?.pin || "").trim();

    const province = String(req.body?.province || "").trim();
    const warehouse_code = String(req.body?.warehouse_code || req.body?.warehouse || "").trim();

    if (!username) return res.status(400).json({ ok: false, message: "username requerido" });
    if (!pin || pin.length < 4) return res.status(400).json({ ok: false, message: "PIN mínimo 4" });

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

    const token = signUserToken(user);
    await audit("USER_LOGIN_OK", req, username, { username });

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
   ✅ SAP: MULTI ITEMS
========================================================= */
app.get("/api/sap/items", verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) {
      return res.status(400).json({ ok: false, message: "Faltan variables SAP" });
    }

    const codes = String(req.query.codes || "")
      .split(",")
      .map((x) => x.trim())
      .filter(Boolean);

    if (!codes.length) {
      return res.status(400).json({ ok: false, message: "codes vacío" });
    }

    const priceListNo = await getPriceListNoByNameCached(SAP_PRICE_LIST);

    const CONCURRENCY = 5;
    const items = {};
    let i = 0;

    async function worker() {
      while (i < codes.length) {
        const idx = i++;
        const code = codes[idx];
        try {
          const r = await getOneItem(code, priceListNo);
          items[code] = {
            ok: true,
            name: r.item.ItemName,
            unit: r.item.SalesUnit,
            price: r.price,
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
      warehouse: SAP_WAREHOUSE,
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
   ✅ SAP: CREAR COTIZACIÓN
   FIX: WarehouseCode por línea + tag [wh:] + qty en cajas
========================================================= */
app.post("/api/sap/quote", maybeUpload, verifyUser, async (req, res) => {
  try {
    if (missingSapEnv()) {
      return res.status(400).json({ ok: false, message: "Faltan variables SAP" });
    }

    let body = req.body || {};
    const isMultipart = String(req.headers["content-type"] || "").includes("multipart/form-data");

    if (isMultipart) {
      if (req.body?.payload) {
        try {
          body = JSON.parse(req.body.payload);
        } catch (e) {
          return res.status(400).json({ ok: false, message: "payload JSON inválido" });
        }
      }
    }

    const cardCode = String(body?.cardCode || "").trim();
    const comments = String(body?.comments || "").trim();
    const lines = Array.isArray(body?.lines) ? body.lines : [];

    if (!cardCode) return res.status(400).json({ ok: false, message: "cardCode requerido." });
    if (!lines.length) return res.status(400).json({ ok: false, message: "lines requerido." });

    const creator = req.user?.username || "unknown";

    // ✅ bodega del usuario (DB) -> fallback env
    const userWh = (await getWarehouseForUsername(creator)).trim();
    const whToUse = userWh || SAP_WAREHOUSE || "01";

    // ✅ Construye líneas (cantidad EN CAJAS)
    // Prioridad: qtyBoxes -> cajas -> qty
    const DocumentLines = lines
      .map((l) => {
        const item = String(l.itemCode || "").trim();
        const qtyBoxes =
          l.qtyBoxes ?? l.cajas ?? l.qty ?? 0;

        const qty = Number(qtyBoxes || 0);

        return {
          ItemCode: item,
          Quantity: qty,                 // ✅ cajas (tal cual)
          WarehouseCode: String(whToUse) // ✅ evita ODBC -2028 por wh
        };
      })
      .filter((x) => x.ItemCode && x.Quantity > 0);

    if (!DocumentLines.length) {
      return res.status(400).json({ ok: false, message: "No hay líneas válidas (qty>0)." });
    }

    const docDate = getDateISOInOffset(TZ_OFFSET_MIN);

    // ✅ Comments con tags para dashboard/histórico
    const sapComments = [
      `[WEB PEDIDOS]`,
      `[user:${creator}]`,
      `[wh:${whToUse}]`,
      comments ? comments : "Cotización mercaderista",
    ].join(" ");

    const files = Array.isArray(req.files) ? req.files : [];
    let attachmentEntry = null;

    if (files.length) {
      attachmentEntry = await createSapAttachmentEntry(files);
    }

    const payload = {
      CardCode: cardCode,
      DocDate: docDate,
      DocDueDate: docDate,
      Comments: sapComments,
      JournalMemo: "Cotización web mercaderistas",
      DocumentLines,
      ...(attachmentEntry ? { AttachmentEntry: attachmentEntry } : {}),
    };

    const created = await slFetch(`/Quotations`, {
      method: "POST",
      body: JSON.stringify(payload),
    });

    await audit("QUOTE_CREATED", req, creator, {
      cardCode,
      lines: DocumentLines.length,
      docDate,
      hasAttachments: files.length > 0,
      attachmentEntry: attachmentEntry || null,
      warehouse: whToUse,
    });

    return res.json({
      ok: true,
      message: "Cotización creada",
      docEntry: created.DocEntry,
      docNum: created.DocNum,
      attachmentEntry: attachmentEntry || null,
      warehouse: whToUse,
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
