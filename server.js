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
const {
  PORT = 3000,
  CORS_ORIGIN = "*",

  DATABASE_URL = "",

  JWT_SECRET = "change_me",

  ADMIN_USER = "PRODIMA",
  ADMIN_PASS = "ADMINISTRADOR",

  SAP_BASE_URL = "",
  SAP_COMPANYDB = "",
  SAP_USER = "",
  SAP_PASS = "",

  SAP_WAREHOUSE = "300",
  SAP_PRICE_LIST = "Lista 02 Res. Com. Ind. Analitic",
  YAPPY_ALIAS = "@prodimasansae",

  // ✅ NUEVO: listas de códigos permitidos por bodega (comma separated)
  ACTIVE_CODES_200 = "",
  ACTIVE_CODES_300 = "",
  ACTIVE_CODES_500 = "",
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
   ✅ DB (Postgres)
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

async function ensureDb() {
  if (!hasDb()) return;

  await pool.query(`
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

function addDaysISO(iso, days) {
  const d = new Date(String(iso || "").slice(0, 10));
  if (Number.isNaN(d.getTime())) return "";
  d.setDate(d.getDate() + Number(days || 0));
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

/* =========================================================
   ✅ Warehouse map
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

function getWarehouseFromReq(req) {
  const whToken = String(req.user?.warehouse_code || "").trim();
  if (whToken) return whToken;

  const whQuery = String(req.query?.warehouse || req.query?.wh || "").trim();
  if (whQuery) return whQuery;

  const whHeader = String(req.headers["x-warehouse"] || "").trim();
  if (whHeader) return whHeader;

  const prov = String(req.user?.province || "").trim();
  if (prov) return provinceToWarehouse(prov);

  return SAP_WAREHOUSE || "300";
}

/* =========================================================
   ✅ NUEVO: Allowed items by warehouse (200/300/500)
========================================================= */
function parseCodesEnv(str) {
  return String(str || "")
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}

// Puedes también ponerlos aquí fijo si no quieres env:
const STATIC_ALLOWED = {
  "200": [], // <-- si quieres fijo, pega aquí los 100
  "300": [],
  "500": [],
};

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
  // ✅ si NO configuraste lista, por seguridad NO bloqueamos (para no tumbarte producción)
  if (!set || set.size === 0) return true;

  if (!set.has(code)) {
    throw new Error(`ItemCode no permitido en bodega ${wh}: ${code}`);
  }
  return true;
}

/* =========================================================
   ✅ Helpers
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

function verifyAdmin(req, res, next) {
  const token = readBearer(req);
  if (!token) return safeJson(res, 401, { ok: false, message: "Missing Bearer token" });

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    if (decoded?.role !== "admin") return safeJson(res, 403, { ok: false, message: "Forbidden" });
    req.admin = decoded;
    next();
  } catch {
    return safeJson(res, 401, { ok: false, message: "Invalid token" });
  }
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

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function parseUserFromComments(comments) {
  const m = String(comments || "").match(/\[user:([^\]]+)\]/i);
  return m ? String(m[1]).trim() : "";
}
function parseWhFromComments(comments) {
  const m = String(comments || "").match(/\[wh:([^\]]+)\]/i);
  return m ? String(m[1]).trim() : "";
}
function isCancelledLike(q) {
  const cancelVal = q?.CancelStatus ?? q?.cancelStatus ?? q?.Cancelled ?? q?.cancelled ?? "";
  const cancelRaw = String(cancelVal).trim().toLowerCase();
  const commLower = String(q?.Comments || q?.comments || "").toLowerCase();
  return (
    cancelRaw === "csyes" ||
    cancelRaw === "yes" ||
    cancelRaw === "true" ||
    cancelRaw.includes("csyes") ||
    cancelRaw.includes("cancel") ||
    commLower.includes("[cancel") ||
    commLower.includes("cancelad")
  );
}

/* =========================================================
   ✅ NUEVO: CATÁLOGO + STOCK LOCAL (SIN SAP)
   - Catálogo TSV: ItemCode<TAB>Description<...><TAB>PriceCaja(última columna)
   - Stock TSV: ItemCode<TAB>Warehouse<TAB>Available
========================================================= */
function normStr(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim();
}

// Pega aquí tu MISMO TSV de catálogo (el mismo que en el HTML)
const LOCAL_CATALOG_TSV = `
${""}
`.trim();

// Pega aquí tu TSV de stock local
const LOCAL_STOCK_TSV = `
${""}
`.trim();

function buildCatalogMapFromTSV(tsv) {
  const map = new Map();
  const lines = String(tsv || "")
    .split("\n")
    .map((x) => x.trim())
    .filter(Boolean);

  for (const row of lines) {
    const cols = row.split("\t").map((x) => x.trim());
    if (cols.length < 2) continue;

    const code = String(cols[0] || "").trim();
    const desc = String(cols[1] || "").trim();
    const last = cols[cols.length - 1];
    const priceCaja = Number(last);

    if (!code) continue;
    map.set(code, {
      code,
      desc,
      priceCaja: Number.isFinite(priceCaja) ? priceCaja : 0,
      _n_desc: normStr(desc),
      _n_code: normStr(code),
    });
  }
  return map;
}

function buildStockMapFromTSV(tsv) {
  const map = new Map();
  const lines = String(tsv || "")
    .split("\n")
    .map((x) => x.trim())
    .filter(Boolean);

  for (const row of lines) {
    const cols = row.split("\t").map((x) => x.trim());
    if (cols.length < 3) continue;
    const code = String(cols[0] || "").trim();
    const wh = String(cols[1] || "").trim();
    const avail = Number(cols[2]);
    if (!code || !wh) continue;
    map.set(`${wh}::${code}`, Number.isFinite(avail) ? avail : 0);
  }
  return map;
}

const LOCAL_CATALOG_MAP = buildCatalogMapFromTSV(LOCAL_CATALOG_TSV);
const LOCAL_STOCK_MAP = buildStockMapFromTSV(LOCAL_STOCK_TSV);

function getLocalCatalogItem(code) {
  return LOCAL_CATALOG_MAP.get(String(code || "").trim()) || null;
}
function getLocalAvailable(wh, code) {
  const k = `${String(wh || "").trim()}::${String(code || "").trim()}`;
  if (!LOCAL_STOCK_MAP.has(k)) return null;
  return Number(LOCAL_STOCK_MAP.get(k));
}

/* =========================================================
   ✅ HEALTH
========================================================= */
app.get("/api/health", async (req, res) => {
  safeJson(res, 200, {
    ok: true,
    message: "✅ PRODIMA API activa",
    yappy: YAPPY_ALIAS,
    warehouse_default: SAP_WAREHOUSE,
    priceList: SAP_PRICE_LIST,
    db: hasDb() ? "on" : "off",
    allowed_wh_200: (ALLOWED_BY_WH["200"] || []).length,
    allowed_wh_300: (ALLOWED_BY_WH["300"] || []).length,
    allowed_wh_500: (ALLOWED_BY_WH["500"] || []).length,
    local_catalog_count: LOCAL_CATALOG_MAP.size,
    local_stock_count: LOCAL_STOCK_MAP.size,
  });
});

/* =========================================================
   ✅ SAP Service Layer (Session cookie)
   (NO TOCADO)
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
   ✅ SAP helpers (NO TOCADO)
========================================================= */
async function sapGetFirstByDocNum(entity, docNum, select) {
  const n = Number(docNum);
  if (!Number.isFinite(n) || n <= 0) throw new Error("DocNum inválido");

  const parts = [];
  if (select) parts.push(`$select=${encodeURIComponent(select)}`);
  parts.push(`$filter=${encodeURIComponent(`DocNum eq ${n}`)}`);
  parts.push(`$top=1`);

  const path = `/${entity}?${parts.join("&")}`;
  const r = await slFetch(path);
  const arr = Array.isArray(r?.value) ? r.value : [];
  return arr[0] || null;
}

async function sapGetByDocEntry(entity, docEntry, select) {
  const n = Number(docEntry);
  if (!Number.isFinite(n) || n <= 0) throw new Error("DocEntry inválido");

  let path = `/${entity}(${n})`;
  if (select) path += `?$select=${encodeURIComponent(select)}`;
  return slFetch(path);
}

/* =========================================================
   ✅ TRACE logic + cache (NO TOCADO)
========================================================= */
const TRACE_CACHE = new Map();
const TRACE_TTL_MS = 6 * 60 * 60 * 1000;

function cacheGet(key) {
  const it = TRACE_CACHE.get(key);
  if (!it) return null;
  if (Date.now() - it.at > TRACE_TTL_MS) {
    TRACE_CACHE.delete(key);
    return null;
  }
  return it.data;
}
function cacheSet(key, data) {
  TRACE_CACHE.set(key, { at: Date.now(), data });
}

async function traceQuote(quoteDocNum, fromOverride, toOverride) {
  const cacheKey = `QDOCNUM:${quoteDocNum}`;
  const cached = cacheGet(cacheKey);
  if (cached) return cached;

  const quoteHead = await sapGetFirstByDocNum(
    "Quotations",
    quoteDocNum,
    "DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments"
  );
  if (!quoteHead) {
    const out = { ok: false, message: "Cotización no encontrada" };
    cacheSet(cacheKey, out);
    return out;
  }

  const quote = await sapGetByDocEntry("Quotations", quoteHead.DocEntry);
  const quoteDocEntry = Number(quote.DocEntry);
  const cardCode = String(quote.CardCode || "").trim();
  const quoteDate = String(quote.DocDate || "").slice(0, 10);

  const from = /^\d{4}-\d{2}-\d{2}$/.test(String(fromOverride || ""))
    ? String(fromOverride)
    : addDaysISO(quoteDate, -7);

  const to = /^\d{4}-\d{2}-\d{2}$/.test(String(toOverride || ""))
    ? String(toOverride)
    : addDaysISO(quoteDate, 30);

  const toPlus1 = addDaysISO(to, 1);

  const ordersList = await slFetch(
    `/Orders?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
      `&$filter=${encodeURIComponent(
        `CardCode eq '${cardCode.replace(/'/g, "''")}' and DocDate ge '${from}' and DocDate lt '${toPlus1}'`
      )}` +
      `&$orderby=DocDate desc,DocEntry desc&$top=200`
  );
  const orderCandidates = Array.isArray(ordersList?.value) ? ordersList.value : [];

  const orders = [];
  for (const o of orderCandidates) {
    const od = await sapGetByDocEntry("Orders", o.DocEntry);
    const lines = Array.isArray(od?.DocumentLines) ? od.DocumentLines : [];
    const linked = lines.some(
      (l) => Number(l?.BaseType) === 23 && Number(l?.BaseEntry) === quoteDocEntry
    );
    if (linked) orders.push(od);
    await sleep(30);
  }

  const deliveries = [];
  const orderDocEntrySet = new Set(orders.map((x) => Number(x.DocEntry)));

  if (orders.length) {
    const delList = await slFetch(
      `/DeliveryNotes?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
        `&$filter=${encodeURIComponent(
          `CardCode eq '${cardCode.replace(/'/g, "''")}' and DocDate ge '${from}' and DocDate lt '${toPlus1}'`
        )}` +
        `&$orderby=DocDate desc,DocEntry desc&$top=300`
    );
    const delCandidates = Array.isArray(delList?.value) ? delList.value : [];

    const seen = new Set();
    for (const d of delCandidates) {
      const dd = await sapGetByDocEntry("DeliveryNotes", d.DocEntry);
      const lines = Array.isArray(dd?.DocumentLines) ? dd.DocumentLines : [];
      const linked = lines.some(
        (l) => Number(l?.BaseType) === 17 && orderDocEntrySet.has(Number(l?.BaseEntry))
      );
      if (linked) {
        const de = Number(dd.DocEntry);
        if (!seen.has(de)) {
          seen.add(de);
          deliveries.push(dd);
        }
      }
      await sleep(30);
    }
  }

  const totalCotizado = Number(quote.DocTotal || 0);
  const totalEntregado = deliveries.reduce((a, d) => a + Number(d?.DocTotal || 0), 0);
  const pendiente = Number((totalCotizado - totalEntregado).toFixed(2));

  const out = {
    ok: true,
    quote,
    orders,
    deliveries,
    totals: { totalCotizado, totalEntregado, pendiente },
  };

  cacheSet(cacheKey, out);
  cacheSet(`QDOCENTRY:${quoteDocEntry}`, out);
  return out;
}

/* =========================================================
   ✅ USER LOGIN (unificado)  (NO TOCADO)
========================================================= */
async function handleUserLogin(req, res) {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const username = String(req.body?.username || req.body?.user || "").trim().toLowerCase();
    const pin = String(req.body?.pin || req.body?.pass || "").trim();

    if (!username || !pin)
      return safeJson(res, 400, { ok: false, message: "username y pin requeridos" });

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

app.get("/api/me", verifyUser, async (req, res) =>
  safeJson(res, 200, { ok: true, user: req.user })
);
app.get("/api/auth/me", verifyUser, async (req, res) =>
  safeJson(res, 200, { ok: true, user: req.user })
);

/* =========================================================
   ✅ ADMIN LOGIN (NO TOCADO)
========================================================= */
app.post("/api/admin/login", async (req, res) => {
  const user = String(req.body?.user || "").trim();
  const pass = String(req.body?.pass || "").trim();

  if (user !== ADMIN_USER || pass !== ADMIN_PASS) {
    return safeJson(res, 401, { ok: false, message: "Credenciales inválidas" });
  }
  const token = signToken({ role: "admin", user }, "12h");
  return safeJson(res, 200, { ok: true, token });
});

/* =========================================================
   ✅ ADMIN USERS (GET/POST/PATCH/DELETE) (NO TOCADO)
========================================================= */
app.get("/api/admin/users", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });
    const r = await dbQuery(
      `SELECT id, username, full_name, province, warehouse_code, is_active, created_at
       FROM app_users
       ORDER BY id DESC`
    );
    return safeJson(res, 200, { ok: true, users: r.rows });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

function normalizeUsername(u) {
  const s = String(u || "").trim().toLowerCase();
  if (!s) return "";
  if (!/^[a-z0-9._-]{2,50}$/.test(s)) return "__INVALID__";
  return s;
}
function toIntId(x) {
  const n = Number(x);
  return Number.isFinite(n) && n > 0 ? Math.trunc(n) : null;
}

app.post("/api/admin/users", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const username = normalizeUsername(req.body?.username);
    const full_name = String(req.body?.full_name || req.body?.fullName || "").trim();
    const province = String(req.body?.province || "").trim();
    const pin = String(req.body?.pin || "").trim();
    const warehouse_code_in = String(req.body?.warehouse_code || req.body?.warehouse || "").trim();

    if (!username || username === "__INVALID__") {
      return safeJson(res, 400, {
        ok: false,
        message: "Username inválido. Usa letras/números y . _ - (mín 2).",
      });
    }
    if (!pin || pin.length < 4) {
      return safeJson(res, 400, { ok: false, message: "PIN mínimo 4" });
    }

    const warehouse_code = warehouse_code_in || provinceToWarehouse(province || "");
    const pin_hash = await bcrypt.hash(pin, 10);

    const q = await dbQuery(
      `INSERT INTO app_users (username, full_name, pin_hash, province, warehouse_code, is_active)
       VALUES ($1,$2,$3,$4,$5,TRUE)
       RETURNING id, username, full_name, province, warehouse_code, is_active, created_at`,
      [username, full_name, pin_hash, province, warehouse_code]
    );

    return safeJson(res, 200, { ok: true, user: q.rows[0] });
  } catch (e) {
    if (String(e?.code) === "23505") {
      return safeJson(res, 409, { ok: false, message: "Ese username ya existe" });
    }
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.patch("/api/admin/users/:id/pin", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const id = toIntId(req.params.id);
    if (!id) return safeJson(res, 400, { ok: false, message: "ID inválido" });

    const pin = String(req.body?.pin || "").trim();
    if (!pin || pin.length < 4) {
      return safeJson(res, 400, { ok: false, message: "PIN mínimo 4" });
    }

    const pin_hash = await bcrypt.hash(pin, 10);

    const r = await dbQuery(
      `UPDATE app_users SET pin_hash=$1 WHERE id=$2
       RETURNING id, username, full_name, province, warehouse_code, is_active, created_at`,
      [pin_hash, id]
    );

    if (!r.rows?.length) return safeJson(res, 404, { ok: false, message: "Usuario no existe" });
    return safeJson(res, 200, { ok: true, user: r.rows[0] });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.patch("/api/admin/users/:id/toggle", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const id = toIntId(req.params.id);
    if (!id) return safeJson(res, 400, { ok: false, message: "ID inválido" });

    const r = await dbQuery(
      `UPDATE app_users
       SET is_active = NOT is_active
       WHERE id=$1
       RETURNING id, username, full_name, province, warehouse_code, is_active, created_at`,
      [id]
    );

    if (!r.rows?.length) return safeJson(res, 404, { ok: false, message: "Usuario no existe" });
    return safeJson(res, 200, { ok: true, user: r.rows[0] });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.delete("/api/admin/users/:id", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada" });

    const id = toIntId(req.params.id);
    if (!id) return safeJson(res, 400, { ok: false, message: "ID inválido" });

    const r = await dbQuery(
      `DELETE FROM app_users WHERE id=$1
       RETURNING id`,
      [id]
    );

    if (!r.rows?.length) return safeJson(res, 404, { ok: false, message: "Usuario no existe" });
    return safeJson(res, 200, { ok: true });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ NUEVO: endpoint para que el frontend sepa la lista (opcional)
========================================================= */
app.get("/api/sap/allowed-items", verifyUser, async (req, res) => {
  const wh = getWarehouseFromReq(req);
  const set = getAllowedSetForWh(wh);
  const list = set ? Array.from(set) : [];
  return res.json({ ok: true, warehouse: wh, allowedCount: list.length, allowed: list });
});

/* =========================================================
   ✅ CAMBIO CLAVE: /api/sap/item/:code -> SOLO LOCAL
========================================================= */
app.get("/api/sap/item/:code", verifyUser, async (req, res) => {
  try {
    const code = String(req.params.code || "").trim();
    if (!code) return res.status(400).json({ ok: false, message: "ItemCode vacío." });

    const warehouseCode = getWarehouseFromReq(req);

    // ✅ BLOQUEO por bodega (igual que antes)
    assertItemAllowedOrThrow(warehouseCode, code);

    // ✅ SOLO LOCAL
    const it = getLocalCatalogItem(code);
    if (!it) return res.status(404).json({ ok: false, message: `No existe en catálogo local: ${code}` });

    const available = getLocalAvailable(warehouseCode, code);

    return res.json({
      ok: true,
      item: {
        ItemCode: code,
        ItemName: it.desc || `Producto ${code}`,
        SalesUnit: "Caja",
      },
      warehouse: warehouseCode,
      bodega: warehouseCode,
      priceList: "LOCAL",
      priceListNo: null,
      price: Number(it.priceCaja || 0), // precio CAJA local
      priceUnit: null,
      factorCaja: null,
      uom: "Caja",
      stock: {
        warehouse: warehouseCode,
        onHand: null,
        committed: null,
        ordered: null,
        available: available,
        hasStock: available != null ? available > 0 : null,
      },
      disponible: available,
      enStock: available != null ? available > 0 : null,
    });
  } catch (err) {
    return res.status(400).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ CAMBIO CLAVE: /api/sap/items/search -> SOLO LOCAL
========================================================= */
app.get("/api/sap/items/search", verifyUser, async (req, res) => {
  try {
    const q = String(req.query?.q || "").trim();
    const top = Math.min(Math.max(Number(req.query?.top || 15), 5), 50);

    if (q.length < 2) return res.json({ ok: true, q, results: [] });

    const warehouseCode = getWarehouseFromReq(req);
    const allowedSet = getAllowedSetForWh(warehouseCode);

    const nq = normStr(q);

    const out = [];
    for (const v of LOCAL_CATALOG_MAP.values()) {
      if (!v?.code) continue;

      if (allowedSet && allowedSet.size > 0) {
        if (!allowedSet.has(String(v.code).trim())) continue;
      }

      const hay = `${v._n_code} ${v._n_desc}`;
      if (hay.includes(nq)) {
        out.push({
          ItemCode: v.code,
          ItemName: v.desc,
          SalesUnit: "Caja",
        });
      }
    }

    // orden simple: primero los que empiezan por código
    out.sort((a, b) => {
      const aStart = normStr(a.ItemCode).startsWith(nq) ? 0 : 1;
      const bStart = normStr(b.ItemCode).startsWith(nq) ? 0 : 1;
      if (aStart !== bStart) return aStart - bStart;
      return String(a.ItemName || "").localeCompare(String(b.ItemName || ""));
    });

    return res.json({ ok: true, q, warehouse: warehouseCode, results: out.slice(0, top) });
  } catch (err) {
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ ADMIN QUOTES (HISTÓRICO + DASHBOARD)
   (NO TOCADO)
========================================================= */
async function scanQuotes({
  f,
  t,
  wantSkip,
  wantLimit,
  userFilter,
  clientFilter,
  includeTotal,
}) {
  const toPlus1 = addDaysISO(t, 1);
  const batchTop = 200;

  let skipSap = 0;
  let totalFiltered = 0;
  const pageRows = [];

  const uFilter = String(userFilter || "").trim().toLowerCase();
  const cFilter = String(clientFilter || "").trim().toLowerCase();

  const maxSapPages = includeTotal ? 200 : 50;

  const seenDocEntry = new Set();

  for (let page = 0; page < maxSapPages; page++) {
    const raw = await slFetch(
      `/Quotations?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
        `&$filter=${encodeURIComponent(`DocDate ge '${f}' and DocDate lt '${toPlus1}'`)}` +
        `&$orderby=DocDate desc,DocEntry desc&$top=${batchTop}&$skip=${skipSap}`
    );

    const rows = Array.isArray(raw?.value) ? raw.value : [];
    if (!rows.length) break;

    skipSap += rows.length;

    for (const q of rows) {
      const de = Number(q?.DocEntry);
      if (Number.isFinite(de)) {
        if (seenDocEntry.has(de)) continue;
        seenDocEntry.add(de);
      }

      if (isCancelledLike(q)) continue;

      const usuario = parseUserFromComments(q.Comments || "") || "sin_user";
      const wh = parseWhFromComments(q.Comments || "") || "sin_wh";

      if (uFilter && !String(usuario).toLowerCase().includes(uFilter)) continue;

      if (cFilter) {
        const cc = String(q.CardCode || "").toLowerCase();
        const cn = String(q.CardName || "").toLowerCase();
        if (!cc.includes(cFilter) && !cn.includes(cFilter)) continue;
      }

      const idx = totalFiltered;
      totalFiltered++;

      if (idx >= wantSkip && pageRows.length < wantLimit) {
        pageRows.push({
          docEntry: q.DocEntry,
          docNum: q.DocNum,
          cardCode: q.CardCode,
          cardName: q.CardName,
          fecha: String(q.DocDate || "").slice(0, 10),
          estado: q.DocumentStatus || "",
          cancelStatus: q.CancelStatus ?? "",
          comments: q.Comments || "",
          usuario,
          warehouse: wh,
          montoCotizacion: Number(q.DocTotal || 0),
          montoEntregado: 0,
          pendiente: Number(q.DocTotal || 0),
        });
      }
    }

    if (!includeTotal && pageRows.length >= wantLimit) break;
  }

  return { pageRows, totalFiltered };
}

app.get("/api/admin/quotes", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const from = String(req.query?.from || "");
    const to = String(req.query?.to || "");

    const withDelivered = String(req.query?.withDelivered || "0") === "1";

    const limitRaw =
      req.query?.limit != null
        ? Number(req.query.limit)
        : req.query?.top != null
        ? Number(req.query.top)
        : 20;

    const limit = Math.max(1, Math.min(200, Number.isFinite(limitRaw) ? limitRaw : 20));

    const pageRaw = req.query?.page != null ? Number(req.query.page) : 1;
    const page = Math.max(1, Number.isFinite(pageRaw) ? pageRaw : 1);
    const skip = (page - 1) * limit;

    const includeTotal = String(req.query?.includeTotal || "0") === "1";

    const userFilter = String(req.query?.user || "");
    const clientFilter = String(req.query?.client || "");

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const defaultFrom = addDaysISO(today, -30);

    const f = /^\d{4}-\d{2}-\d{2}$/.test(from) ? from : defaultFrom;
    const t = /^\d{4}-\d{2}-\d{2}$/.test(to) ? to : today;

    const { pageRows, totalFiltered } = await scanQuotes({
      f,
      t,
      wantSkip: skip,
      wantLimit: limit,
      userFilter,
      clientFilter,
      includeTotal,
    });

    if (withDelivered && pageRows.length) {
      const CONC = 2;
      let idx = 0;

      async function worker() {
        while (idx < pageRows.length) {
          const i = idx++;
          const q = pageRows[i];
          try {
            const tr = await traceQuote(q.docNum, f, t);
            if (tr.ok) {
              q.montoEntregado = Number(tr.totals?.totalEntregado || 0);
              q.pendiente = Number(tr.totals?.pendiente || 0);
            }
          } catch {}
          await sleep(25);
        }
      }

      await Promise.all(Array.from({ length: CONC }, () => worker()));
    }

    return safeJson(res, 200, {
      ok: true,
      quotes: pageRows,
      from: f,
      to: t,
      page,
      limit,
      total: includeTotal ? totalFiltered : null,
      pageCount: includeTotal ? Math.max(1, Math.ceil(totalFiltered / limit)) : null,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ CUSTOMERS search / customer (NO TOCADO)
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
   ✅ QUOTE (NO TOCADO) - sigue en SAP
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

    const cleanLines = lines
      .map((l) => ({
        ItemCode: String(l.itemCode || "").trim(),
        Quantity: Number(l.qty || 0),
      }))
      .filter((x) => x.ItemCode && x.Quantity > 0);

    if (!cleanLines.length)
      return res.status(400).json({ ok: false, message: "No hay líneas válidas (qty>0)." });

    for (const ln of cleanLines) {
      assertItemAllowedOrThrow(warehouseCode, ln.ItemCode);
    }

    const DocumentLines = cleanLines.map((ln) => ({
      ItemCode: ln.ItemCode,
      Quantity: ln.Quantity,
      WarehouseCode: warehouseCode,
    }));

    const docDate = getDateISOInOffset(TZ_OFFSET_MIN);
    const creator = req.user?.username || "unknown";
    const province = String(req.user?.province || "").trim();

    const baseComments = [
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
      Comments: baseComments,
      JournalMemo: "Cotización web mercaderistas",
      DocumentLines,
    };

    try {
      const created = await slFetch(`/Quotations`, {
        method: "POST",
        body: JSON.stringify(payload),
      });

      return res.json({
        ok: true,
        message: "Cotización creada",
        docEntry: created.DocEntry,
        docNum: created.DocNum,
        warehouse: warehouseCode,
        bodega: warehouseCode,
        fallback: false,
      });
    } catch (err1) {
      const msg1 = String(err1?.message || err1);

      const isNoMatch = msg1.includes("ODBC -2028") || msg1.toLowerCase().includes("no matching records found");

      if (!isNoMatch) throw err1;

      const payloadFallback = {
        ...payload,
        Comments: `${baseComments} [wh_fallback:1]`,
        DocumentLines: cleanLines.map((ln) => ({
          ItemCode: ln.ItemCode,
          Quantity: ln.Quantity,
        })),
      };

      const created2 = await slFetch(`/Quotations`, {
        method: "POST",
        body: JSON.stringify(payloadFallback),
      });

      return res.json({
        ok: true,
        message: "Cotización creada (fallback sin WarehouseCode por -2028)",
        docEntry: created2.DocEntry,
        docNum: created2.DocNum,
        warehouse: warehouseCode,
        bodega: warehouseCode,
        fallback: true,
      });
    }
  } catch (err) {
    const msg = String(err?.message || err);
    const isAllow = msg.toLowerCase().includes("no permitido");
    return res.status(isAllow ? 400 : 500).json({ ok: false, message: msg });
  }
});

/* =========================================================
   ✅ START
========================================================= */
(async () => {
  try {
    await ensureDb();
    console.log(hasDb() ? "DB ready ✅" : "DB not configured (skipped init) ⚠️");
  } catch (e) {
    console.error("DB init error:", e.message);
  }

  app.listen(Number(PORT), () => {
    console.log(`Server listening on :${PORT}`);
  });
})();
