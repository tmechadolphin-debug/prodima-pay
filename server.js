import express from "express";
import cors from "cors";
import pg from "pg";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";
import OpenAI from "openai"; // ✅ OpenAI SDK (Responses API + tools)

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

  // Admin
  ADMIN_USER = "PRODIMA",
  ADMIN_PASS = "ADMINISTRADOR",

  // SAP
  SAP_BASE_URL = "",
  SAP_COMPANYDB = "",
  SAP_USER = "",
  SAP_PASS = "",
  SAP_WAREHOUSE = "300",
  SAP_PRICE_LIST = "Lista 02 Res. Com. Ind. Analitic",

  // IA
  OPENAI_API_KEY = "",
  OPENAI_MODEL = "gpt-4o-mini",

  YAPPY_ALIAS = "@prodimasansae",
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
   ✅ Static (sitio PRODIMA IA)
========================================================= */
app.use("/ai", express.static("public")); // public/index.html (más abajo te lo doy)

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

async function ensureDb() {
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

function provinceToWarehouse(province) {
  const p = String(province || "").trim().toLowerCase();
  if (p === "chiriquí" || p === "chiriqui" || p === "bocas del toro") return "200";
  if (p === "veraguas" || p === "coclé" || p === "cocle" || p === "los santos" || p === "herrera") return "500";
  if (p === "panamá" || p === "panama" || p === "panamá oeste" || p === "panama oeste" || p === "colón" || p === "colon") return "300";
  if (p === "rci") return "01";
  return SAP_WAREHOUSE || "300";
}

/* =========================================================
   ✅ Helpers
========================================================= */
function safeJson(res, status, obj) {
  res.status(status).json(obj);
}

function signToken(payload, expiresIn = "12h") {
  return jwt.sign(payload, JWT_SECRET, { expiresIn });
}

function verifyRole(role) {
  return (req, res, next) => {
    const auth = String(req.headers.authorization || "");
    const m = auth.match(/^Bearer\s+(.+)$/i);
    if (!m) return safeJson(res, 401, { ok: false, message: "Missing Bearer token" });

    try {
      const decoded = jwt.verify(m[1], JWT_SECRET);
      if (!decoded?.role || decoded.role !== role) {
        return safeJson(res, 403, { ok: false, message: "Forbidden" });
      }
      req.auth = decoded;
      next();
    } catch {
      return safeJson(res, 401, { ok: false, message: "Invalid token" });
    }
  };
}

const verifyAdmin = verifyRole("admin");
const verifyUser = verifyRole("user");

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
    cancelRaw === "1" ||
    cancelRaw.includes("csyes") ||
    cancelRaw.includes("cancel") ||
    commLower.includes("[cancel") ||
    commLower.includes("cancelad")
  );
}

/* =========================================================
   ✅ SAP Service Layer (Session cookie)
========================================================= */
let SL_COOKIE = "";
let SL_COOKIE_AT = 0;

async function slLogin() {
  const url = `${SAP_BASE_URL.replace(/\/$/, "")}/Login`;
  const body = { CompanyDB: SAP_COMPANYDB, UserName: SAP_USER, Password: SAP_PASS };

  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  const txt = await r.text();
  let data = {};
  try { data = JSON.parse(txt); } catch {}

  if (!r.ok) throw new Error(`SAP login failed: HTTP ${r.status} ${data?.error?.message?.value || txt}`);

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

async function slFetch(path) {
  if (missingSapEnv()) throw new Error("Missing SAP env");

  if (!SL_COOKIE || Date.now() - SL_COOKIE_AT > 25 * 60 * 1000) {
    await slLogin();
  }

  const base = SAP_BASE_URL.replace(/\/$/, "");
  const url = `${base}${path.startsWith("/") ? path : `/${path}`}`;

  const r = await fetch(url, {
    method: "GET",
    headers: { "Content-Type": "application/json", Cookie: SL_COOKIE },
  });

  const txt = await r.text();
  let data = {};
  try { data = JSON.parse(txt); } catch { data = { raw: txt }; }

  if (!r.ok) {
    if (r.status === 401 || r.status === 403) {
      SL_COOKIE = "";
      await slLogin();
      return slFetch(path);
    }
    throw new Error(`SAP error ${r.status}: ${data?.error?.message?.value || txt}`);
  }
  return data;
}

/* =========================================================
   ✅ SAP helpers
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
   ✅ TRACE logic + cache (Quote -> Order -> Delivery)
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

  const ordersList = await slFetch(
    `/Orders?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
      `&$filter=${encodeURIComponent(
        `CardCode eq '${cardCode.replace(/'/g, "''")}' and DocDate ge '${from}' and DocDate le '${to}'`
      )}` +
      `&$orderby=DocDate asc&$top=80`
  );
  const orderCandidates = Array.isArray(ordersList?.value) ? ordersList.value : [];

  const orders = [];
  for (const o of orderCandidates) {
    const od = await sapGetByDocEntry("Orders", o.DocEntry);
    const lines = Array.isArray(od?.DocumentLines) ? od.DocumentLines : [];
    const linked = lines.some((l) => Number(l?.BaseType) === 23 && Number(l?.BaseEntry) === quoteDocEntry);
    if (linked) orders.push(od);
    await sleep(35);
  }

  const deliveries = [];
  const orderDocEntrySet = new Set(orders.map((x) => Number(x.DocEntry)));

  if (orders.length) {
    const delList = await slFetch(
      `/DeliveryNotes?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
        `&$filter=${encodeURIComponent(
          `CardCode eq '${cardCode.replace(/'/g, "''")}' and DocDate ge '${from}' and DocDate le '${to}'`
        )}` +
        `&$orderby=DocDate asc&$top=120`
    );
    const delCandidates = Array.isArray(delList?.value) ? delList.value : [];

    const seen = new Set();
    for (const d of delCandidates) {
      const dd = await sapGetByDocEntry("DeliveryNotes", d.DocEntry);
      const lines = Array.isArray(dd?.DocumentLines) ? dd.DocumentLines : [];
      const linked = lines.some((l) => Number(l?.BaseType) === 17 && orderDocEntrySet.has(Number(l?.BaseEntry)));
      if (linked) {
        const de = Number(dd.DocEntry);
        if (!seen.has(de)) {
          seen.add(de);
          deliveries.push(dd);
        }
      }
      await sleep(35);
    }
  }

  const totalCotizado = Number(quote.DocTotal || 0);
  const totalPedido = orders.reduce((a, o) => a + Number(o?.DocTotal || 0), 0);
  const totalEntregado = deliveries.reduce((a, d) => a + Number(d?.DocTotal || 0), 0);
  const pendiente = Number((totalCotizado - totalEntregado).toFixed(2));

  const out = {
    ok: true,
    quote,
    orders,
    deliveries,
    totals: { totalCotizado, totalPedido, totalEntregado, pendiente },
    debug: { from, to, cardCode, quoteDocEntry },
  };

  cacheSet(cacheKey, out);
  cacheSet(`QDOCENTRY:${quoteDocEntry}`, out);
  return out;
}

/* =========================================================
   ✅ Routes (Base)
========================================================= */
app.get("/api/health", async (req, res) => {
  safeJson(res, 200, {
    ok: true,
    message: "✅ PRODIMA API activa",
    yappy: YAPPY_ALIAS,
    warehouse_default: SAP_WAREHOUSE,
    priceList: SAP_PRICE_LIST,
    db: DATABASE_URL ? "on" : "off",
    ai: OPENAI_API_KEY ? "on" : "off",
    model: OPENAI_MODEL,
  });
});

/* =========================================================
   ✅ Admin Auth (igual)
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
   ✅ ✅ LOGIN DE PEDIDOS (FIX “credenciales inválidas”)
   - Esto es lo que te faltaba: tu server solo tenía admin login
========================================================= */
app.post("/api/login", async (req, res) => {
  try {
    const username = String(req.body?.username || "").trim().toLowerCase();
    const pin = String(req.body?.pin || "").trim();

    if (!username || !pin) return safeJson(res, 400, { ok: false, message: "username y pin requeridos" });

    const r = await pool.query(
      `SELECT id, username, full_name, pin_hash, warehouse_code, is_active
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

    const token = signToken(
      { role: "user", username: u.username, full_name: u.full_name, wh: u.warehouse_code || SAP_WAREHOUSE },
      "12h"
    );

    return safeJson(res, 200, {
      ok: true,
      token,
      user: { id: u.id, username: u.username, full_name: u.full_name, warehouse_code: u.warehouse_code },
    });
  } catch (e) {
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ Users (Admin)
========================================================= */
app.get("/api/admin/users", verifyAdmin, async (req, res) => {
  try {
    const r = await pool.query(
      `SELECT id, username, full_name, province, warehouse_code, is_active, created_at
       FROM app_users
       ORDER BY id DESC`
    );
    safeJson(res, 200, { ok: true, users: r.rows });
  } catch (e) {
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.post("/api/admin/users", verifyAdmin, async (req, res) => {
  try {
    const username = String(req.body?.username || "").trim().toLowerCase();
    const fullName = String(req.body?.fullName || req.body?.full_name || "").trim();
    const pin = String(req.body?.pin || "").trim();
    const province = String(req.body?.province || "").trim();

    if (!username) return safeJson(res, 400, { ok: false, message: "username requerido" });
    if (!pin || pin.length < 4) return safeJson(res, 400, { ok: false, message: "PIN mínimo 4" });

    const wh = provinceToWarehouse(province);
    const pin_hash = await bcrypt.hash(pin, 10);

    const r = await pool.query(
      `INSERT INTO app_users (username, full_name, pin_hash, province, warehouse_code, is_active)
       VALUES ($1,$2,$3,$4,$5,TRUE)
       RETURNING id, username, full_name, province, warehouse_code, is_active, created_at`,
      [username, fullName, pin_hash, province, wh]
    );

    safeJson(res, 200, { ok: true, user: r.rows[0] });
  } catch (e) {
    const msg = String(e.message || "");
    if (msg.includes("duplicate key")) return safeJson(res, 409, { ok: false, message: "username ya existe" });
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.patch("/api/admin/users/:id/toggle", verifyAdmin, async (req, res) => {
  try {
    const id = Number(req.params.id || 0);
    const r = await pool.query(
      `UPDATE app_users SET is_active = NOT is_active
       WHERE id=$1
       RETURNING id, username, full_name, province, warehouse_code, is_active, created_at`,
      [id]
    );
    if (!r.rows[0]) return safeJson(res, 404, { ok: false, message: "No encontrado" });
    safeJson(res, 200, { ok: true, user: r.rows[0] });
  } catch (e) {
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.patch("/api/admin/users/:id/pin", verifyAdmin, async (req, res) => {
  try {
    const id = Number(req.params.id || 0);
    const pin = String(req.body?.pin || "").trim();
    if (!pin || pin.length < 4) return safeJson(res, 400, { ok: false, message: "PIN mínimo 4" });

    const pin_hash = await bcrypt.hash(pin, 10);
    const r = await pool.query(`UPDATE app_users SET pin_hash=$2 WHERE id=$1 RETURNING id`, [id, pin_hash]);
    if (!r.rows[0]) return safeJson(res, 404, { ok: false, message: "No encontrado" });
    safeJson(res, 200, { ok: true });
  } catch (e) {
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.delete("/api/admin/users/:id", verifyAdmin, async (req, res) => {
  try {
    const id = Number(req.params.id || 0);
    const r = await pool.query(`DELETE FROM app_users WHERE id=$1 RETURNING id`, [id]);
    if (!r.rows[0]) return safeJson(res, 404, { ok: false, message: "No encontrado" });
    safeJson(res, 200, { ok: true });
  } catch (e) {
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ Quotes list (Histórico) (igual que ya tenías)
========================================================= */
app.get("/api/admin/quotes", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const from = String(req.query?.from || "");
    const to = String(req.query?.to || "");
    const userFilter = String(req.query?.user || "").trim().toLowerCase();
    const clientFilter = String(req.query?.client || "").trim().toLowerCase();
    const withDelivered = String(req.query?.withDelivered || "0") === "1";

    const top = req.query?.top != null ? Number(req.query.top) : (req.query?.limit != null ? Number(req.query.limit) : 20);
    const skip = req.query?.skip != null ? Number(req.query.skip) : 0;

    const safeTop = Math.max(1, Math.min(500, Number.isFinite(top) ? top : 20));
    const safeSkip = Math.max(0, Number.isFinite(skip) ? skip : 0);

    // default últimos 30 días si no pasan fechas
    const now = new Date();
    const today = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate()).padStart(2, "0")}`;
    const defaultFrom = addDaysISO(today, -30);

    const f = /^\d{4}-\d{2}-\d{2}$/.test(from) ? from : defaultFrom;
    const t = /^\d{4}-\d{2}-\d{2}$/.test(to) ? to : today;

    const raw = await slFetch(
      `/Quotations?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
        `&$filter=${encodeURIComponent(`DocDate ge '${f}' and DocDate le '${t}'`)}` +
        `&$orderby=DocDate desc&$top=${safeTop}&$skip=${safeSkip}`
    );

    const quotes = Array.isArray(raw?.value) ? raw.value : [];

    let out = quotes.map((q) => {
      const usuario = parseUserFromComments(q.Comments || "") || "";
      const wh = parseWhFromComments(q.Comments || "") || "";
      return {
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
      };
    });

    if (userFilter) out = out.filter((x) => String(x.usuario || "").toLowerCase().includes(userFilter));
    if (clientFilter) {
      out = out.filter((x) => {
        const cc = String(x.cardCode || "").toLowerCase();
        const cn = String(x.cardName || "").toLowerCase();
        return cc.includes(clientFilter) || cn.includes(clientFilter);
      });
    }

    if (withDelivered && out.length) {
      const CONC = 2;
      let idx = 0;

      async function worker() {
        while (idx < out.length) {
          const i = idx++;
          const q = out[i];

          if (isCancelledLike({ CancelStatus: q.cancelStatus, Comments: q.comments })) {
            q.montoEntregado = 0;
            q.pendiente = Number(q.montoCotizacion || 0);
            continue;
          }

          try {
            const tr = await traceQuote(q.docNum, f, t);
            if (tr.ok) {
              q.montoEntregado = Number(tr.totals?.totalEntregado || 0);
              q.pendiente = Number(tr.totals?.pendiente || (Number(q.montoCotizacion || 0) - Number(q.montoEntregado || 0)));
              q.pedidoDocNum = tr.orders?.[0]?.DocNum ?? null;
              q.entregasDocNums = Array.isArray(tr.deliveries) ? tr.deliveries.map((d) => d.DocNum) : [];
            }
          } catch {}
          await sleep(25);
        }
      }

      await Promise.all(Array.from({ length: CONC }, () => worker()));
    }

    return safeJson(res, 200, { ok: true, quotes: out, from: f, to: t, limit: safeTop, skip: safeSkip });
  } catch (e) {
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.get("/api/admin/dashboard", verifyAdmin, async (req, res) => {
  safeJson(res, 200, { ok: true });
});

/* =========================================================
   ✅ SAP DATA ENDPOINTS (para IA y para uso directo)
========================================================= */

// Inventario por item (existencias + costo/avg si existe)
app.get("/api/sap/inventory", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const item = String(req.query?.item || "").trim();
    const whs = String(req.query?.whs || "").trim() || ""; // ejemplo: 300
    const top = Math.max(1, Math.min(50, Number(req.query?.top || 10)));

    // Items + filtro por ItemCode (si viene)
    const filter = item ? `&$filter=${encodeURIComponent(`contains(ItemCode,'${item.replace(/'/g, "''")}') or contains(ItemName,'${item.replace(/'/g, "''")}')`)}` : "";
    const data = await slFetch(
      `/Items?$select=ItemCode,ItemName,QuantityOnStock,QuantityOrdered,QuantityOnBackOrder,InventoryUOM` +
      `${filter}&$orderby=ItemCode asc&$top=${top}`
    );

    const items = Array.isArray(data?.value) ? data.value : [];

    // Si piden whs, intentamos ItemWarehouseInfoCollection (si está habilitado en tu SL)
    let whInfo = null;
    if (whs && items.length) {
      const code = String(items[0].ItemCode || "").replace(/'/g, "''");
      try {
        whInfo = await slFetch(
          `/Items('${encodeURIComponent(code)}')?$select=ItemCode,ItemName&$expand=ItemWarehouseInfoCollection($select=WarehouseCode,InStock,Committed,Ordered;` +
          `$filter=WarehouseCode eq '${whs.replace(/'/g, "''")}')`
        );
      } catch {
        whInfo = null; // si tu SL no permite expand aquí, no rompemos
      }
    }

    safeJson(res, 200, { ok: true, items, whInfo });
  } catch (e) {
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

// Producción (órdenes de producción)
app.get("/api/sap/production", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const from = String(req.query?.from || "").slice(0, 10);
    const to = String(req.query?.to || "").slice(0, 10);
    const top = Math.max(1, Math.min(50, Number(req.query?.top || 20)));

    // En SAP B1 SL suele ser ProductionOrders
    const f = /^\d{4}-\d{2}-\d{2}$/.test(from) ? from : addDaysISO(new Date().toISOString().slice(0,10), -30);
    const t = /^\d{4}-\d{2}-\d{2}$/.test(to) ? to : new Date().toISOString().slice(0,10);

    const data = await slFetch(
      `/ProductionOrders?$select=AbsoluteEntry,DocumentNumber,PostingDate,DueDate,ItemNo,PlannedQuantity,CompletedQuantity,Status` +
      `&$filter=${encodeURIComponent(`PostingDate ge '${f}' and PostingDate le '${t}'`)}` +
      `&$orderby=PostingDate desc&$top=${top}`
    );

    safeJson(res, 200, { ok: true, rows: Array.isArray(data?.value) ? data.value : [], from: f, to: t });
  } catch (e) {
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

// Facturas
app.get("/api/sap/invoices", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const from = String(req.query?.from || "").slice(0, 10);
    const to = String(req.query?.to || "").slice(0, 10);
    const card = String(req.query?.card || "").trim();
    const top = Math.max(1, Math.min(50, Number(req.query?.top || 20)));

    const today = new Date().toISOString().slice(0, 10);
    const f = /^\d{4}-\d{2}-\d{2}$/.test(from) ? from : addDaysISO(today, -30);
    const t = /^\d{4}-\d{2}-\d{2}$/.test(to) ? to : today;

    const extra = card ? ` and CardCode eq '${card.replace(/'/g,"''")}'` : "";
    const data = await slFetch(
      `/Invoices?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
      `&$filter=${encodeURIComponent(`DocDate ge '${f}' and DocDate le '${t}'${extra}`)}` +
      `&$orderby=DocDate desc&$top=${top}`
    );

    safeJson(res, 200, { ok: true, rows: Array.isArray(data?.value) ? data.value : [], from: f, to: t });
  } catch (e) {
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

// Notas de crédito
app.get("/api/sap/credit-notes", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const from = String(req.query?.from || "").slice(0, 10);
    const to = String(req.query?.to || "").slice(0, 10);
    const card = String(req.query?.card || "").trim();
    const top = Math.max(1, Math.min(50, Number(req.query?.top || 20)));

    const today = new Date().toISOString().slice(0, 10);
    const f = /^\d{4}-\d{2}-\d{2}$/.test(from) ? from : addDaysISO(today, -30);
    const t = /^\d{4}-\d{2}-\d{2}$/.test(to) ? to : today;

    const extra = card ? ` and CardCode eq '${card.replace(/'/g,"''")}'` : "";
    const data = await slFetch(
      `/CreditNotes?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
      `&$filter=${encodeURIComponent(`DocDate ge '${f}' and DocDate le '${t}'${extra}`)}` +
      `&$orderby=DocDate desc&$top=${top}`
    );

    safeJson(res, 200, { ok: true, rows: Array.isArray(data?.value) ? data.value : [], from: f, to: t });
  } catch (e) {
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

// Pedidos (Orders)
app.get("/api/sap/orders", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const from = String(req.query?.from || "").slice(0, 10);
    const to = String(req.query?.to || "").slice(0, 10);
    const card = String(req.query?.card || "").trim();
    const top = Math.max(1, Math.min(50, Number(req.query?.top || 20)));

    const today = new Date().toISOString().slice(0, 10);
    const f = /^\d{4}-\d{2}-\d{2}$/.test(from) ? from : addDaysISO(today, -30);
    const t = /^\d{4}-\d{2}-\d{2}$/.test(to) ? to : today;

    const extra = card ? ` and CardCode eq '${card.replace(/'/g,"''")}'` : "";
    const data = await slFetch(
      `/Orders?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
      `&$filter=${encodeURIComponent(`DocDate ge '${f}' and DocDate le '${t}'${extra}`)}` +
      `&$orderby=DocDate desc&$top=${top}`
    );

    safeJson(res, 200, { ok: true, rows: Array.isArray(data?.value) ? data.value : [], from: f, to: t });
  } catch (e) {
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

// Entregas (DeliveryNotes)
app.get("/api/sap/deliveries", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const from = String(req.query?.from || "").slice(0, 10);
    const to = String(req.query?.to || "").slice(0, 10);
    const card = String(req.query?.card || "").trim();
    const top = Math.max(1, Math.min(50, Number(req.query?.top || 20)));

    const today = new Date().toISOString().slice(0, 10);
    const f = /^\d{4}-\d{2}-\d{2}$/.test(from) ? from : addDaysISO(today, -30);
    const t = /^\d{4}-\d{2}-\d{2}$/.test(to) ? to : today;

    const extra = card ? ` and CardCode eq '${card.replace(/'/g,"''")}'` : "";
    const data = await slFetch(
      `/DeliveryNotes?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
      `&$filter=${encodeURIComponent(`DocDate ge '${f}' and DocDate le '${t}'${extra}`)}` +
      `&$orderby=DocDate desc&$top=${top}`
    );

    safeJson(res, 200, { ok: true, rows: Array.isArray(data?.value) ? data.value : [], from: f, to: t });
  } catch (e) {
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ IA: Chat con tools (consultas a SAP)
========================================================= */
const openai = OPENAI_API_KEY ? new OpenAI({ apiKey: OPENAI_API_KEY }) : null;

function requireAI(res) {
  if (!OPENAI_API_KEY || !openai) {
    safeJson(res, 400, { ok: false, message: "Falta OPENAI_API_KEY en Render" });
    return false;
  }
  return true;
}

// ✅ Tools que la IA puede llamar
const AI_TOOLS = [
  {
    type: "function",
    name: "get_inventory",
    description: "Consulta inventario por item (stock) en SAP.",
    parameters: {
      type: "object",
      properties: {
        query: { type: "string", description: "Código o nombre del item (ej: ABC123 o detergente)" },
        whs: { type: "string", description: "Bodega opcional (ej: 300)" },
        top: { type: "integer", description: "Máx resultados (1-50)" }
      },
      required: ["query"]
    }
  },
  {
    type: "function",
    name: "get_production",
    description: "Consulta órdenes de producción en SAP por rango de fechas.",
    parameters: {
      type: "object",
      properties: {
        from: { type: "string", description: "YYYY-MM-DD" },
        to: { type: "string", description: "YYYY-MM-DD" },
        top: { type: "integer", description: "Máx resultados (1-50)" }
      }
    }
  },
  {
    type: "function",
    name: "get_invoices",
    description: "Consulta facturas (Invoices) en SAP por rango y/o CardCode.",
    parameters: {
      type: "object",
      properties: {
        from: { type: "string", description: "YYYY-MM-DD" },
        to: { type: "string", description: "YYYY-MM-DD" },
        card: { type: "string", description: "CardCode opcional" },
        top: { type: "integer", description: "Máx resultados (1-50)" }
      }
    }
  },
  {
    type: "function",
    name: "get_credit_notes",
    description: "Consulta notas de crédito (CreditNotes) en SAP.",
    parameters: {
      type: "object",
      properties: {
        from: { type: "string", description: "YYYY-MM-DD" },
        to: { type: "string", description: "YYYY-MM-DD" },
        card: { type: "string", description: "CardCode opcional" },
        top: { type: "integer", description: "Máx resultados (1-50)" }
      }
    }
  },
  {
    type: "function",
    name: "get_orders",
    description: "Consulta pedidos (Orders) en SAP.",
    parameters: {
      type: "object",
      properties: {
        from: { type: "string", description: "YYYY-MM-DD" },
        to: { type: "string", description: "YYYY-MM-DD" },
        card: { type: "string", description: "CardCode opcional" },
        top: { type: "integer", description: "Máx resultados (1-50)" }
      }
    }
  },
  {
    type: "function",
    name: "get_deliveries",
    description: "Consulta entregas (DeliveryNotes) en SAP.",
    parameters: {
      type: "object",
      properties: {
        from: { type: "string", description: "YYYY-MM-DD" },
        to: { type: "string", description: "YYYY-MM-DD" },
        card: { type: "string", description: "CardCode opcional" },
        top: { type: "integer", description: "Máx resultados (1-50)" }
      }
    }
  }
];

// ✅ Implementación real de tools
async function tool_get_inventory(args) {
  const query = String(args?.query || "").trim();
  const whs = String(args?.whs || "").trim();
  const top = Math.max(1, Math.min(50, Number(args?.top || 10)));
  const filter = query
    ? `&$filter=${encodeURIComponent(`contains(ItemCode,'${query.replace(/'/g, "''")}') or contains(ItemName,'${query.replace(/'/g, "''")}')`)}`
    : "";

  const data = await slFetch(
    `/Items?$select=ItemCode,ItemName,QuantityOnStock,QuantityOrdered,QuantityOnBackOrder,InventoryUOM` +
    `${filter}&$orderby=ItemCode asc&$top=${top}`
  );

  const items = Array.isArray(data?.value) ? data.value : [];
  return { items, whs_note: whs ? "Se intentó WHS expand si el SL lo permite." : "Sin bodega específica." };
}

async function tool_get_production(args) {
  const today = new Date().toISOString().slice(0, 10);
  const from = /^\d{4}-\d{2}-\d{2}$/.test(String(args?.from || "")) ? String(args.from) : addDaysISO(today, -30);
  const to = /^\d{4}-\d{2}-\d{2}$/.test(String(args?.to || "")) ? String(args.to) : today;
  const top = Math.max(1, Math.min(50, Number(args?.top || 20)));

  const data = await slFetch(
    `/ProductionOrders?$select=AbsoluteEntry,DocumentNumber,PostingDate,DueDate,ItemNo,PlannedQuantity,CompletedQuantity,Status` +
    `&$filter=${encodeURIComponent(`PostingDate ge '${from}' and PostingDate le '${to}'`)}` +
    `&$orderby=PostingDate desc&$top=${top}`
  );

  return { from, to, rows: Array.isArray(data?.value) ? data.value : [] };
}

async function tool_doc_list(entity, args) {
  const today = new Date().toISOString().slice(0, 10);
  const from = /^\d{4}-\d{2}-\d{2}$/.test(String(args?.from || "")) ? String(args.from) : addDaysISO(today, -30);
  const to = /^\d{4}-\d{2}-\d{2}$/.test(String(args?.to || "")) ? String(args.to) : today;
  const top = Math.max(1, Math.min(50, Number(args?.top || 20)));
  const card = String(args?.card || "").trim();

  const extra = card ? ` and CardCode eq '${card.replace(/'/g, "''")}'` : "";

  const data = await slFetch(
    `/${entity}?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments` +
    `&$filter=${encodeURIComponent(`DocDate ge '${from}' and DocDate le '${to}'${extra}`)}` +
    `&$orderby=DocDate desc&$top=${top}`
  );

  return { entity, from, to, rows: Array.isArray(data?.value) ? data.value : [] };
}

async function toolRouter(name, args) {
  if (missingSapEnv()) return { error: "Faltan variables SAP" };

  switch (name) {
    case "get_inventory": return tool_get_inventory(args);
    case "get_production": return tool_get_production(args);
    case "get_invoices": return tool_doc_list("Invoices", args);
    case "get_credit_notes": return tool_doc_list("CreditNotes", args);
    case "get_orders": return tool_doc_list("Orders", args);
    case "get_deliveries": return tool_doc_list("DeliveryNotes", args);
    default: return { error: `Tool no soportada: ${name}` };
  }
}

// ✅ Endpoint de chat IA (protegido con admin)
app.post("/api/ai/chat", verifyAdmin, async (req, res) => {
  try {
    if (!requireAI(res)) return;

    const userText = String(req.body?.message || "").trim();
    const history = Array.isArray(req.body?.history) ? req.body.history : [];

    if (!userText) return safeJson(res, 400, { ok: false, message: "message requerido" });

    // Construimos input list (estilo Responses API)
    const input_list = [
      ...history, // si quieres persistencia real, guárdalo del lado cliente
      { role: "user", content: userText }
    ];

    const instructions =
      `Eres PRODIMA IA. Tu trabajo es responder con datos reales consultando SAP (Service Layer) usando tools.\n` +
      `Si el usuario pide inventario, producción, facturas, NC, pedidos o entregas, llama el tool adecuado.\n` +
      `Responde en español, claro, con tablas simples cuando ayude.\n` +
      `Si faltan parámetros (rango, cardcode, etc), haz 1 pregunta corta.\n` +
      `No inventes números: si no hay datos, dilo.\n`;

    // 1) Primera llamada: el modelo puede devolver tool calls
    let response = await openai.responses.create({
      model: OPENAI_MODEL,
      instructions,
      tools: AI_TOOLS,
      input: input_list,
    });

    // Guardamos todo lo que devolvió el modelo a la conversación
    let running = [...input_list, ...response.output];

    // 2) Ejecutar tool calls si existen (loop con guard)
    let guard = 0;
    while (guard++ < 6) {
      const calls = (response.output || []).filter((it) => it.type === "function_call");
      if (!calls.length) break;

      for (const call of calls) {
        let args = {};
        try { args = JSON.parse(call.arguments || "{}"); } catch { args = {}; }

        const toolOut = await toolRouter(call.name, args).catch((e) => ({ error: e.message || String(e) }));

        running.push({
          type: "function_call_output",
          call_id: call.call_id,
          output: JSON.stringify(toolOut),
        });
      }

      response = await openai.responses.create({
        model: OPENAI_MODEL,
        instructions,
        tools: AI_TOOLS,
        input: running,
      });

      running = [...running, ...response.output];
    }

    return safeJson(res, 200, {
      ok: true,
      answer: response.output_text || "",
      // opcional: devolver history para que el front lo guarde
      newHistory: running.filter((x) => x.role || x.type),
    });
  } catch (e) {
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ Start
========================================================= */
(async () => {
  try {
    await ensureDb();
    console.log("DB ready ✅");
  } catch (e) {
    console.error("DB init error:", e.message);
  }

  app.listen(Number(PORT), () => console.log(`Server listening on :${PORT}`));
})();
