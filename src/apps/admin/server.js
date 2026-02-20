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
const PORT = Number(process.env.PORT || 10000);

const CORS_ORIGIN = process.env.CORS_ORIGIN || "*";
const DATABASE_URL = process.env.DATABASE_URL || "";

const ADMIN_USER = process.env.ADMIN_USER || "PRODIMA";
const ADMIN_PASS = process.env.ADMIN_PASS || "ADMINISTRADOR";
const JWT_SECRET = process.env.JWT_SECRET || "prodima_change_this_secret";

// SAP
const SAP_BASE_URL = process.env.SAP_BASE_URL || "";
const SAP_COMPANYDB = process.env.SAP_COMPANYDB || "";
const SAP_USER = process.env.SAP_USER || "";
const SAP_PASS = process.env.SAP_PASS || "";

// Panamá
const TZ_OFFSET_MIN = Number(process.env.TZ_OFFSET_MIN || -300);

/* =========================================================
   ✅ CORS
========================================================= */
app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? true : [CORS_ORIGIN],
    methods: ["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  })
);
app.options("*", cors());

/* =========================================================
   ✅ DB
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
  console.log("✅ DB Schema OK");
}

/* =========================================================
   ✅ JWT
========================================================= */
function signAdminToken() {
  return jwt.sign({ typ: "admin" }, JWT_SECRET, { expiresIn: "2h" });
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

/* =========================================================
   ✅ Time helpers
========================================================= */
function getDateISOInOffset(offsetMinutes = -300) {
  const now = new Date();
  const utcMs = now.getTime() + now.getTimezoneOffset() * 60000;
  const localMs = utcMs + offsetMinutes * 60000;
  const local = new Date(localMs);
  return local.toISOString().slice(0, 10);
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
   ✅ SAP Service Layer (cookie + timeout)
========================================================= */
function missingSapEnv() {
  return !SAP_BASE_URL || !SAP_COMPANYDB || !SAP_USER || !SAP_PASS;
}

let SL_COOKIE = null;
let SL_COOKIE_TIME = 0;

async function slLogin() {
  if (missingSapEnv()) throw new Error("Faltan variables SAP");

  const payload = {
    CompanyDB: SAP_COMPANYDB,
    UserName: SAP_USER,
    Password: SAP_PASS,
  };

  const res = await fetch(`${SAP_BASE_URL.replace(/\/$/, "")}/Login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  const t = await res.text();
  if (!res.ok) throw new Error(`Login SAP falló (${res.status}): ${t}`);

  const setCookie = res.headers.get("set-cookie");
  if (!setCookie) throw new Error("No se recibió cookie del Service Layer.");

  SL_COOKIE = setCookie
    .split(",")
    .map((s) => s.split(";")[0])
    .join("; ");

  SL_COOKIE_TIME = Date.now();
}

async function slFetch(path, options = {}, timeoutMs = 20000) {
  if (!SL_COOKIE || Date.now() - SL_COOKIE_TIME > 25 * 60 * 1000) {
    await slLogin();
  }

  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(`timeout ${timeoutMs}ms`), timeoutMs);

  const url = `${SAP_BASE_URL.replace(/\/$/, "")}${path.startsWith("/") ? path : `/${path}`}`;

  try {
    const res = await fetch(url, {
      ...options,
      signal: controller.signal,
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
      return slFetch(path, options, timeoutMs);
    }

    let json;
    try {
      json = text ? JSON.parse(text) : {};
    } catch {
      json = { raw: text };
    }

    if (!res.ok) throw new Error(`SAP error ${res.status}: ${json?.error?.message?.value || text}`);
    return json;
  } finally {
    clearTimeout(t);
  }
}

/* =========================================================
   ✅ Parse helpers
========================================================= */
function parseUserFromComments(comments = "") {
  const m = String(comments).match(/\[user:([^\]]+)\]/i);
  return m ? String(m[1]).trim() : "";
}
function parseWhFromComments(comments = "") {
  const m = String(comments).match(/\[wh:([^\]]+)\]/i);
  return m ? String(m[1]).trim() : "";
}
function isCancelledLike(q) {
  const cancelVal = q?.CancelStatus ?? q?.cancelStatus ?? q?.Cancelled ?? q?.cancelled ?? "";
  const cancelRaw = String(cancelVal).trim().toLowerCase();
  const commLower = String(q?.Comments || q?.comments || "").toLowerCase();
  const stLower = String(q?.DocumentStatus || q?.estado || "").toLowerCase();

  return (
    cancelRaw === "csyes" ||
    cancelRaw === "yes" ||
    cancelRaw === "true" ||
    cancelRaw.includes("csyes") ||
    cancelRaw.includes("cancel") ||
    stLower.includes("cancel") ||
    commLower.includes("[cancel") ||
    commLower.includes("cancelad")
  );
}

/* =========================================================
   ✅ TRACE delivered helpers (por docNum)
========================================================= */
const TRACE_CACHE = new Map();
const TRACE_TTL_MS = 2 * 60 * 60 * 1000;

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

async function sapGetFirstByDocNum(entity, docNum, select) {
  const n = Number(docNum);
  if (!Number.isFinite(n) || n <= 0) throw new Error("DocNum inválido");

  const parts = [];
  if (select) parts.push(`$select=${encodeURIComponent(select)}`);
  parts.push(`$filter=${encodeURIComponent(`DocNum eq ${n}`)}`);
  parts.push(`$top=1`);

  const path = `/${entity}?${parts.join("&")}`;
  const r = await slFetch(path, {}, 15000);
  const arr = Array.isArray(r?.value) ? r.value : [];
  return arr[0] || null;
}

async function sapGetByDocEntry(entity, docEntry) {
  const n = Number(docEntry);
  if (!Number.isFinite(n) || n <= 0) throw new Error("DocEntry inválido");
  return slFetch(`/${entity}(${n})`, {}, 20000);
}

async function traceDeliveredForQuote(docNum, from, to) {
  const cacheKey = `DEL:${docNum}:${from || ""}:${to || ""}`;
  const cached = cacheGet(cacheKey);
  if (cached) return cached;

  const head = await sapGetFirstByDocNum(
    "Quotations",
    docNum,
    "DocEntry,DocNum,DocDate,DocTotal,CardCode"
  );
  if (!head) {
    const out = { ok: false, totalEntregado: 0, pendiente: 0 };
    cacheSet(cacheKey, out);
    return out;
  }

  const quote = await sapGetByDocEntry("Quotations", head.DocEntry);
  const quoteDocEntry = Number(quote.DocEntry);
  const cardCode = String(quote.CardCode || "").trim();

  const quoteDate = String(quote.DocDate || "").slice(0, 10);
  const f = /^\d{4}-\d{2}-\d{2}$/.test(String(from || "")) ? String(from) : addDaysISO(quoteDate, -7);
  const t = /^\d{4}-\d{2}-\d{2}$/.test(String(to || "")) ? String(to) : addDaysISO(quoteDate, 30);
  const toPlus1 = addDaysISO(t, 1);

  // Orders del cliente
  const ordersList = await slFetch(
    `/Orders?$select=DocEntry,DocDate,CardCode&` +
      `$filter=${encodeURIComponent(
        `CardCode eq '${cardCode.replace(/'/g, "''")}' and DocDate ge '${f}' and DocDate lt '${toPlus1}'`
      )}` +
      `&$orderby=DocDate desc,DocEntry desc&$top=120`,
    {},
    20000
  );
  const orderCandidates = Array.isArray(ordersList?.value) ? ordersList.value : [];

  const orderDocEntries = [];
  const MAX_ORDER_CHECK = 35;

  for (let i = 0; i < Math.min(orderCandidates.length, MAX_ORDER_CHECK); i++) {
    const o = orderCandidates[i];
    const od = await sapGetByDocEntry("Orders", o.DocEntry);
    const lines = Array.isArray(od?.DocumentLines) ? od.DocumentLines : [];
    const linked = lines.some((l) => Number(l?.BaseType) === 23 && Number(l?.BaseEntry) === quoteDocEntry);
    if (linked) orderDocEntries.push(Number(od.DocEntry));
  }

  let totalEntregado = 0;

  if (orderDocEntries.length) {
    const orderSet = new Set(orderDocEntries);

    const delList = await slFetch(
      `/DeliveryNotes?$select=DocEntry,DocDate,DocTotal,CardCode&` +
        `$filter=${encodeURIComponent(
          `CardCode eq '${cardCode.replace(/'/g, "''")}' and DocDate ge '${f}' and DocDate lt '${toPlus1}'`
        )}` +
        `&$orderby=DocDate desc,DocEntry desc&$top=160`,
      {},
      25000
    );

    const delCandidates = Array.isArray(delList?.value) ? delList.value : [];
    const MAX_DEL_CHECK = 45;

    const seen = new Set();
    for (let i = 0; i < Math.min(delCandidates.length, MAX_DEL_CHECK); i++) {
      const d = delCandidates[i];
      const dd = await sapGetByDocEntry("DeliveryNotes", d.DocEntry);
      const lines = Array.isArray(dd?.DocumentLines) ? dd.DocumentLines : [];
      const linked = lines.some((l) => Number(l?.BaseType) === 17 && orderSet.has(Number(l?.BaseEntry)));
      if (linked) {
        const de = Number(dd.DocEntry);
        if (!seen.has(de)) {
          seen.add(de);
          totalEntregado += Number(dd?.DocTotal || 0);
        }
      }
    }
  }

  const totalCotizado = Number(quote?.DocTotal || 0);
  const pendiente = Number((totalCotizado - totalEntregado).toFixed(2));

  const out = { ok: true, totalEntregado: Number(totalEntregado.toFixed(2)), pendiente };
  cacheSet(cacheKey, out);
  return out;
}

/* =========================================================
   ✅ HEALTH
========================================================= */
app.get("/api/health", async (req, res) => {
  res.json({
    ok: true,
    message: "✅ PRODIMA API activa (ADMIN)",
    db: hasDb() ? "on" : "off",
    sap: missingSapEnv() ? "off" : "on",
    today: getDateISOInOffset(TZ_OFFSET_MIN),
  });
});

/* =========================================================
   ✅ ADMIN LOGIN
========================================================= */
app.post("/api/admin/login", async (req, res) => {
  const user = String(req.body?.user || "").trim();
  const pass = String(req.body?.pass || "").trim();

  if (!user || !pass) return res.status(400).json({ ok: false, message: "user y pass requeridos" });
  if (user !== ADMIN_USER || pass !== ADMIN_PASS)
    return res.status(401).json({ ok: false, message: "Credenciales inválidas" });

  return res.json({ ok: true, token: signAdminToken() });
});

/* =========================================================
   ✅ ADMIN USERS
========================================================= */
app.get("/api/admin/users", verifyAdmin, async (req, res) => {
  if (!hasDb()) return res.status(500).json({ ok: false, message: "DB no configurada" });
  const r = await dbQuery(`
    SELECT id, username, full_name, is_active, province, warehouse_code, created_at
    FROM app_users
    ORDER BY created_at DESC;
  `);
  return res.json({ ok: true, users: r.rows || [] });
});

app.post("/api/admin/users", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return res.status(500).json({ ok: false, message: "DB no configurada" });

    const username = String(req.body?.username || "").trim().toLowerCase();
    const fullName = String(req.body?.fullName || req.body?.full_name || "").trim();
    const pin = String(req.body?.pin || "").trim();
    const province = String(req.body?.province || "").trim();
    const warehouse_code = String(req.body?.warehouse_code || "").trim();

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

    return res.json({ ok: true, user: ins.rows[0] });
  } catch (e) {
    const msg = String(e.message || e);
    if (msg.includes("duplicate") || msg.includes("unique"))
      return res.status(400).json({ ok: false, message: "Ese username ya existe" });
    return res.status(500).json({ ok: false, message: msg });
  }
});

app.patch("/api/admin/users/:id/toggle", verifyAdmin, async (req, res) => {
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
  return res.json({ ok: true, user: r.rows[0] });
});

app.patch("/api/admin/users/:id/pin", verifyAdmin, async (req, res) => {
  if (!hasDb()) return res.status(500).json({ ok: false, message: "DB no configurada" });

  const id = Number(req.params.id || 0);
  if (!id) return res.status(400).json({ ok: false, message: "id inválido" });

  const pin = String(req.body?.pin || "").trim();
  if (!pin || pin.length < 4) return res.status(400).json({ ok: false, message: "PIN mínimo 4" });

  const pin_hash = await bcrypt.hash(pin, 10);
  const r = await dbQuery(`UPDATE app_users SET pin_hash=$1 WHERE id=$2 RETURNING id`, [pin_hash, id]);

  if (!r.rowCount) return res.status(404).json({ ok: false, message: "Usuario no encontrado" });
  return res.json({ ok: true });
});

app.delete("/api/admin/users/:id", verifyAdmin, async (req, res) => {
  if (!hasDb()) return res.status(500).json({ ok: false, message: "DB no configurada" });

  const id = Number(req.params.id || 0);
  if (!id) return res.status(400).json({ ok: false, message: "id inválido" });

  const r = await dbQuery(`DELETE FROM app_users WHERE id = $1 RETURNING id, username;`, [id]);
  if (!r.rowCount) return res.status(404).json({ ok: false, message: "Usuario no encontrado" });

  return res.json({ ok: true });
});

/* =========================================================
   ✅ ADMIN QUOTES (RÁPIDO, NO ENTREGADO)
   - Esto te deja volver a ver 80/104 sin que se trabe.
========================================================= */
app.get("/api/admin/quotes", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const limit = Math.min(Math.max(Number(req.query?.limit || req.query?.top || 80), 1), 500);
    const skip = Math.max(Number(req.query?.skip || 0), 0);

    const from = String(req.query?.from || "").trim();
    const to = String(req.query?.to || "").trim();

    const filterParts = [];
    if (from) filterParts.push(`DocDate ge '${from}'`);
    if (to) filterParts.push(`DocDate lt '${addDaysISO(to, 1)}'`);
    const sapFilter = filterParts.length ? `&$filter=${encodeURIComponent(filterParts.join(" and "))}` : "";

    const SELECT =
      `DocEntry,DocNum,CardCode,CardName,DocTotal,DocDate,DocumentStatus,CancelStatus,Comments`;

    const sap = await slFetch(
      `/Quotations?$select=${SELECT}` +
        `&$orderby=DocDate desc,DocEntry desc&$top=${limit}&$skip=${skip}${sapFilter}`,
      {},
      20000
    );

    const values = Array.isArray(sap?.value) ? sap.value : [];

    // NO filtramos canceladas: solo marcamos isCancelled
    const quotes = values.map((q) => {
      const fechaISO = String(q.DocDate || "").slice(0, 10);
      const cancelStatus = String(q.CancelStatus || "").trim();
      const isCancelled = isCancelledLike(q) || cancelStatus.toLowerCase() === "csyes";

      const estado = isCancelled
        ? "Cancelled"
        : q.DocumentStatus === "bost_Open"
        ? "Open"
        : q.DocumentStatus === "bost_Close"
        ? "Close"
        : String(q.DocumentStatus || "");

      const usuario = parseUserFromComments(q.Comments || "") || "sin_user";
      const wh = parseWhFromComments(q.Comments || "") || "sin_wh";

      const monto = Number(q.DocTotal || 0);

      return {
        docEntry: q.DocEntry,
        docNum: q.DocNum,
        cardCode: String(q.CardCode || "").trim(),
        cardName: String(q.CardName || "").trim(),
        montoCotizacion: monto,
        fecha: fechaISO,
        estado,
        cancelStatus,
        isCancelled,
        usuario,
        warehouse: wh,
        comments: q.Comments || "",
        // delivered se llenará con el batch
        montoEntregado: 0,
        pendiente: monto,
      };
    });

    return res.json({ ok: true, limit, skip, count: quotes.length, quotes });
  } catch (err) {
    console.error("❌ /api/admin/quotes:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ NUEVO: BATCH ENTREGADO (para lista visible)
   Body: { docNums: number[], from?: "YYYY-MM-DD", to?: "YYYY-MM-DD" }
   Response: { ok:true, delivered: { [docNum]: { montoEntregado, pendiente } } }
========================================================= */
app.post("/api/admin/quotes/delivered", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const docNums = Array.isArray(req.body?.docNums) ? req.body.docNums : [];
    const from = String(req.body?.from || "").trim();
    const to = String(req.body?.to || "").trim();

    const clean = Array.from(new Set(docNums.map((x) => Number(x)).filter((n) => Number.isFinite(n) && n > 0)));

    // 🔒 para no matar Render
    const MAX = 80;
    const list = clean.slice(0, MAX);

    const delivered = {};
    const CONC = 2;
    let idx = 0;

    async function worker() {
      while (idx < list.length) {
        const i = idx++;
        const docNum = list[i];

        try {
          const r = await traceDeliveredForQuote(docNum, from, to);
          if (r?.ok) {
            delivered[String(docNum)] = {
              montoEntregado: Number(r.totalEntregado || 0),
              pendiente: Number(r.pendiente || 0),
            };
          } else {
            delivered[String(docNum)] = { montoEntregado: 0, pendiente: 0 };
          }
        } catch {
          delivered[String(docNum)] = { montoEntregado: 0, pendiente: 0 };
        }
      }
    }

    await Promise.all(Array.from({ length: CONC }, () => worker()));

    return res.json({ ok: true, count: list.length, delivered });
  } catch (err) {
    console.error("❌ /api/admin/quotes/delivered:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ ADMIN DASHBOARD (compat)
========================================================= */
app.get("/api/admin/dashboard", verifyAdmin, async (req, res) => {
  return res.json({ ok: true, message: "ok" });
});

/* =========================================================
   ✅ START
========================================================= */
ensureSchema()
  .then(() => app.listen(PORT, () => console.log("✅ Server listo en puerto", PORT)))
  .catch(() => app.listen(PORT, () => console.log("✅ Server listo en puerto", PORT, "(sin DB)")));
