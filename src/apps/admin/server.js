import express from "express";
import pg from "pg";
import jwt from "jsonwebtoken";

const { Pool } = pg;

const app = express();
app.use(express.json({ limit: "2mb" }));

/* =========================================================
   ✅ ENV
========================================================= */
const {
  PORT = 3000,
  DATABASE_URL = "",
  JWT_SECRET = "change_me",
  ADMIN_USER = "PRODIMA",
  ADMIN_PASS = "ADMINISTRADOR",

  SAP_BASE_URL = "",
  SAP_COMPANYDB = "",
  SAP_USER = "",
  SAP_PASS = "",

  CORS_ORIGIN = "",
} = process.env;

/* =========================================================
   ✅ CORS ROBUSTO
========================================================= */
const ALLOWED_ORIGINS = new Set(
  String(CORS_ORIGIN || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)
);
const allowAll = ALLOWED_ORIGINS.size === 0;

app.use((req, res, next) => {
  const origin = req.headers.origin;

  if (allowAll && origin) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
  } else if (origin && ALLOWED_ORIGINS.has(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
  }

  res.setHeader("Access-Control-Allow-Methods", "GET,POST,PATCH,DELETE,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.setHeader("Access-Control-Max-Age", "86400");

  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

/* =========================================================
   ✅ DB (solo para app_users scope)
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
      pin_hash TEXT NOT NULL DEFAULT '',
      province TEXT DEFAULT '',
      warehouse_code TEXT DEFAULT '',
      is_active BOOLEAN DEFAULT TRUE,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);
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

function missingSapEnv() {
  return !SAP_BASE_URL || !SAP_COMPANYDB || !SAP_USER || !SAP_PASS;
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
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
const TZ_OFFSET_MIN = -300;
function getDateISOInOffset(offsetMin = 0) {
  const now = new Date();
  const ms = now.getTime() + now.getTimezoneOffset() * 60000 + Number(offsetMin) * 60000;
  const d = new Date(ms);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
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

function wantsGroups(req) {
  const q = req.query || {};
  const v = (x) => String(x ?? "").trim().toLowerCase();
  return (
    v(q.withGroups) === "1" ||
    v(q.groups) === "1" ||
    v(q.includeGroups) === "1" ||
    v(q.dashboard) === "1" ||
    v(q.mode) === "dashboard"
  );
}

/* =========================================================
   ✅ HEALTH
========================================================= */
app.get("/api/health", async (req, res) => {
  safeJson(res, 200, {
    ok: true,
    message: "✅ PRODIMA API activa",
    db: hasDb() ? "on" : "off",
    sap: missingSapEnv() ? "missing" : "ok",
  });
});

/* =========================================================
   ✅ fetch wrapper (Node16/18)
========================================================= */
let _fetch = globalThis.fetch || null;
async function httpFetch(url, options) {
  if (_fetch) return _fetch(url, options);
  const mod = await import("node-fetch");
  _fetch = mod.default;
  return _fetch(url, options);
}

/* =========================================================
   ✅ SAP Service Layer (cookie + timeout)
========================================================= */
let SL_COOKIE = "";
let SL_COOKIE_AT = 0;

async function slLogin() {
  const url = `${SAP_BASE_URL.replace(/\/$/, "")}/Login`;
  const body = { CompanyDB: SAP_COMPANYDB, UserName: SAP_USER, Password: SAP_PASS };

  const r = await httpFetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  const txt = await r.text();
  let data = {};
  try {
    data = JSON.parse(txt);
  } catch {}

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

  const controller = new AbortController();
  const timeoutMs = Number(options.timeoutMs || 12000);
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const r = await httpFetch(url, {
      method: String(options.method || "GET").toUpperCase(),
      signal: controller.signal,
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
  } catch (e) {
    if (String(e?.name) === "AbortError") throw new Error(`SAP timeout (${timeoutMs}ms) en slFetch`);
    throw e;
  } finally {
    clearTimeout(timeout);
  }
}

/* =========================================================
   ✅ ADMIN LOGIN
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
   ✅ Created users scope (si lo usas)
========================================================= */
let CREATED_USERS_CACHE = { ts: 0, set: new Set() };
const CREATED_USERS_TTL_MS = 5 * 60 * 1000;

async function getCreatedUsersSetCached() {
  if (!hasDb()) return new Set();
  const now = Date.now();
  if (CREATED_USERS_CACHE.ts && now - CREATED_USERS_CACHE.ts < CREATED_USERS_TTL_MS) return CREATED_USERS_CACHE.set;

  const r = await dbQuery(`SELECT username FROM app_users WHERE is_active=TRUE`);
  const set = new Set((r.rows || []).map((x) => String(x.username || "").trim().toLowerCase()).filter(Boolean));
  CREATED_USERS_CACHE = { ts: now, set };
  return set;
}

/* =========================================================
   ✅ Item Group cache + helpers (ESTO ES LO QUE TE FALTA)
========================================================= */
const ITEM_GROUP_CODE_TO_NAME = new Map(); // groupCode -> groupName
const ITEM_CODE_TO_GROUP_NAME = new Map(); // itemCode -> groupName
const GROUP_TTL_MS = 24 * 60 * 60 * 1000;
const GROUP_CACHE_AT = new Map();

function cacheFresh(key, ttl) {
  const ts = GROUP_CACHE_AT.get(key);
  return ts && Date.now() - ts < ttl;
}
function cacheStamp(key) {
  GROUP_CACHE_AT.set(key, Date.now());
}

async function getGroupNameByGroupCode(groupCode) {
  const code = Number(groupCode);
  if (!Number.isFinite(code)) return "";

  const key = `G:${code}`;
  if (ITEM_GROUP_CODE_TO_NAME.has(code) && cacheFresh(key, GROUP_TTL_MS)) {
    return ITEM_GROUP_CODE_TO_NAME.get(code) || "";
  }

  const r = await slFetch(
    `/ItemGroups?$select=GroupCode,GroupName&$filter=${encodeURIComponent(`GroupCode eq ${code}`)}&$top=1`,
    { timeoutMs: 12000 }
  );
  const arr = Array.isArray(r?.value) ? r.value : [];
  const name = String(arr?.[0]?.GroupName || "").trim();

  ITEM_GROUP_CODE_TO_NAME.set(code, name);
  cacheStamp(key);
  return name;
}

async function getGroupNameByItemCode(itemCode) {
  const code = String(itemCode || "").trim();
  if (!code) return "";

  const key = `I:${code}`;
  if (ITEM_CODE_TO_GROUP_NAME.has(code) && cacheFresh(key, GROUP_TTL_MS)) {
    return ITEM_CODE_TO_GROUP_NAME.get(code) || "";
  }

  const it = await slFetch(`/Items('${encodeURIComponent(code)}')?$select=ItemCode,ItemsGroupCode`, {
    timeoutMs: 12000,
  });

  const gcode = it?.ItemsGroupCode;
  const gname = await getGroupNameByGroupCode(gcode);

  ITEM_CODE_TO_GROUP_NAME.set(code, gname);
  cacheStamp(key);
  return gname;
}

async function resolveGroupsForItemCodes(itemCodes) {
  const unique = Array.from(new Set(itemCodes.map((x) => String(x || "").trim()).filter(Boolean)));
  if (!unique.length) return new Map();

  const out = new Map();
  const CONC = 8;
  let idx = 0;

  async function worker() {
    while (idx < unique.length) {
      const i = idx++;
      const code = unique[i];
      try {
        const g = await getGroupNameByItemCode(code);
        out.set(code, g || "");
      } catch {
        out.set(code, "");
      }
      await sleep(5);
    }
  }

  await Promise.all(Array.from({ length: CONC }, worker));
  return out;
}

/* =========================================================
   ✅ QUOTES scan (rápido)
========================================================= */
async function scanQuotes({ f, t, wantSkip, wantLimit, userFilter, clientFilter, onlyCreated }) {
  const toPlus1 = addDaysISO(t, 1);
  const batchTop = 200;

  let skipSap = 0;
  let totalFiltered = 0;
  const pageRows = [];

  const uFilter = String(userFilter || "").trim().toLowerCase();
  const cFilter = String(clientFilter || "").trim().toLowerCase();

  const maxSapPages = 60;
  const seenDocEntry = new Set();

  const createdSet = onlyCreated ? await getCreatedUsersSetCached() : null;

  for (let page = 0; page < maxSapPages; page++) {
    const raw = await slFetch(
      `/Quotations?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
        `&$filter=${encodeURIComponent(`DocDate ge '${f}' and DocDate lt '${toPlus1}'`)}` +
        `&$orderby=DocDate desc,DocEntry desc&$top=${batchTop}&$skip=${skipSap}`,
      { timeoutMs: 12000 }
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

      if (createdSet) {
        const u = String(usuario || "").trim().toLowerCase();
        if (!u || !createdSet.has(u)) continue;
      }

      if (uFilter && !String(usuario).toLowerCase().includes(uFilter)) continue;

      if (cFilter) {
        const cc = String(q.CardCode || "").toLowerCase();
        const cn = String(q.CardName || "").toLowerCase();
        if (!cc.includes(cFilter) && !cn.includes(cFilter)) continue;
      }

      const idx = totalFiltered++;
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

          // delivered lo maneja tu lógica aparte (no afecta grupos)
          montoEntregado: Number(q.montoEntregado || 0) || 0,
          pendiente: Number(q.DocTotal || 0),
        });
      }
    }

    if (pageRows.length >= wantLimit) break;
  }

  return { pageRows, totalFiltered };
}

/* =========================================================
   ✅ /api/admin/quotes (con withGroups)
========================================================= */
app.get("/api/admin/quotes", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const from = String(req.query?.from || "");
    const to = String(req.query?.to || "");

    const withDelivered = String(req.query?.withDelivered || "0") === "1";
    const withGroups = wantsGroups(req);

    const onlyCreated = String(req.query?.onlyCreated || "0") === "1";

    const limitRaw =
      req.query?.limit != null
        ? Number(req.query.limit)
        : req.query?.top != null
        ? Number(req.query.top)
        : 20;

    let limit = Math.max(1, Math.min(200, Number.isFinite(limitRaw) ? limitRaw : 20));
    const skip = req.query?.skip != null ? Math.max(0, Number(req.query.skip) || 0) : 0;

    // caps para estabilidad
    if (withDelivered) limit = Math.min(limit, 60);
    if (withGroups) limit = Math.min(limit, 80);

    const userFilter = String(req.query?.user || "");
    const clientFilter = String(req.query?.client || "");

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const defaultFrom = addDaysISO(today, -30);

    const f = /^\d{4}-\d{2}-\d{2}$/.test(from) ? from : defaultFrom;
    const tt = /^\d{4}-\d{2}-\d{2}$/.test(to) ? to : today;

    const { pageRows, totalFiltered } = await scanQuotes({
      f,
      t: tt,
      wantSkip: skip,
      wantLimit: limit,
      userFilter,
      clientFilter,
      onlyCreated,
    });

    // ✅ GRUPOS (esto es lo que necesitas para tus 2 tablas)
    if (withGroups && pageRows.length) {
      const CONC = 3;
      let idx = 0;

      async function workerGroups() {
        while (idx < pageRows.length) {
          const i = idx++;
          const q = pageRows[i];

          try {
            const full = await slFetch(
              `/Quotations(${Number(q.docEntry)})?$select=DocEntry&$expand=DocumentLines($select=ItemCode,LineTotal)`,
              { timeoutMs: 15000 }
            );

            const lines = Array.isArray(full?.DocumentLines) ? full.DocumentLines : [];
            const codes = lines.map((ln) => String(ln?.ItemCode || "").trim()).filter(Boolean);

            const mapGroups = await resolveGroupsForItemCodes(codes);

            const outLines = [];
            for (const ln of lines) {
              const code = String(ln?.ItemCode || "").trim();
              if (!code) continue;

              outLines.push({
                ItemCode: code,
                LineTotal: Number(ln?.LineTotal || 0),
                ItmsGrpNam: mapGroups.get(code) || "",
              });
            }

            q.lines = outLines;

            const uniq = new Set(outLines.map((x) => x.ItmsGrpNam).filter(Boolean));
            if (uniq.size === 1) q.itemGroup = Array.from(uniq)[0];
          } catch {
            // best-effort
          }

          await sleep(15);
        }
      }

      await Promise.all(Array.from({ length: CONC }, () => workerGroups()));
    }

    // ✅ (si no usas withDelivered aquí, no pasa nada; tu HTML ya lo maneja)
    return safeJson(res, 200, {
      ok: true,
      quotes: pageRows,
      from: f,
      to: tt,
      limit,
      skip,
      total: totalFiltered,
      withGroups,
      withDelivered,
      scope: { onlyCreated },
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
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
    console.log(hasDb() ? "DB ready ✅" : "DB not configured ⚠️");
  } catch (e) {
    console.error("DB init error:", e.message);
  }

  app.listen(Number(PORT), () => console.log(`Server listening on :${PORT}`));
})();
