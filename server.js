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
  ssl: DATABASE_URL && DATABASE_URL.includes("sslmode")
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
  if (
    p === "panamá" ||
    p === "panama" ||
    p === "panamá oeste" ||
    p === "panama oeste" ||
    p === "colón" ||
    p === "colon"
  ) return "300";
  if (p === "rci") return "01";
  return SAP_WAREHOUSE || "300";
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

/* ✅ NUEVO: middleware para Pedidos (usuarios app_users) */
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
  try { data = JSON.parse(txt); } catch {}

  if (!r.ok) {
    throw new Error(`SAP login failed: HTTP ${r.status} ${data?.error?.message?.value || txt}`);
  }

  const setCookie = r.headers.get("set-cookie") || "";
  const cookies = [];
  for (const part of setCookie.split(",")) {
    const s = part.trim();
    if (s.startsWith("B1SESSION=") || s.startsWith("ROUTEID=")) {
      cookies.push(s.split(";")[0]);
    }
  }
  SL_COOKIE = cookies.join("; ");
  SL_COOKIE_AT = Date.now();
  return true;
}

async function slFetch(path) {
  if (missingSapEnv()) throw new Error("Missing SAP env");

  if (!SL_COOKIE || (Date.now() - SL_COOKIE_AT) > 25 * 60 * 1000) {
    await slLogin();
  }

  const base = SAP_BASE_URL.replace(/\/$/, "");
  const url = `${base}${path.startsWith("/") ? path : `/${path}`}`;

  const r = await fetch(url, {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      Cookie: SL_COOKIE,
    },
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
   ✅ SAP helpers (SIN $expand DocumentLines)
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
   ✅ TRACE logic + cache
========================================================= */
const TRACE_CACHE = new Map();
const TRACE_TTL_MS = 6 * 60 * 60 * 1000;

function cacheGet(key) {
  const it = TRACE_CACHE.get(key);
  if (!it) return null;
  if ((Date.now() - it.at) > TRACE_TTL_MS) {
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
    const linked = lines.some(
      (l) => Number(l?.BaseType) === 23 && Number(l?.BaseEntry) === quoteDocEntry
    );
    if (linked) orders.push(od);
    await sleep(50);
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
      await sleep(50);
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
   ✅ Routes
========================================================= */
app.get("/api/health", async (req, res) => {
  safeJson(res, 200, {
    ok: true,
    message: "✅ PRODIMA API activa",
    yappy: YAPPY_ALIAS,
    warehouse_default: SAP_WAREHOUSE,
    priceList: SAP_PRICE_LIST,
    db: DATABASE_URL ? "on" : "off",
  });
});

/* ------------------ ✅ LOGIN PEDIDOS (NUEVO) ------------------ */
/*
  Pedidos debe llamar:
  POST /api/login
  body: { username, pin }
*/
app.post("/api/login", async (req, res) => {
  try {
    const username = String(req.body?.username || req.body?.user || "").trim().toLowerCase();
    const pin = String(req.body?.pin || req.body?.pass || "").trim();

    if (!username || !pin) {
      return safeJson(res, 400, { ok: false, message: "username y pin requeridos" });
    }

    const r = await pool.query(
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

    const token = signToken(
      {
        role: "user",
        id: u.id,
        username: u.username,
        full_name: u.full_name || "",
        province: u.province || "",
        warehouse_code: u.warehouse_code || "",
      },
      "30d"
    );

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
});

/* ✅ opcional: para validar sesión desde Pedidos */
app.get("/api/me", verifyUser, async (req, res) => {
  safeJson(res, 200, { ok: true, user: req.user });
});

/* ------------------ Admin Auth ------------------ */
app.post("/api/admin/login", async (req, res) => {
  const user = String(req.body?.user || "").trim();
  const pass = String(req.body?.pass || "").trim();

  if (user !== ADMIN_USER || pass !== ADMIN_PASS) {
    return safeJson(res, 401, { ok: false, message: "Credenciales inválidas" });
  }

  const token = signToken({ role: "admin", user }, "12h");
  return safeJson(res, 200, { ok: true, token });
});

/* ------------------ Users ------------------ */
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
    if (msg.includes("duplicate key")) {
      return safeJson(res, 409, { ok: false, message: "username ya existe" });
    }
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.patch("/api/admin/users/:id/toggle", verifyAdmin, async (req, res) => {
  try {
    const id = Number(req.params.id || 0);
    const r = await pool.query(
      `UPDATE app_users
       SET is_active = NOT is_active
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
    const r = await pool.query(
      `UPDATE app_users SET pin_hash=$2 WHERE id=$1 RETURNING id`,
      [id, pin_hash]
    );
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

/* ------------------ SAP basic ------------------ */
app.get("/api/admin/sap/order/:docNum", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const docNum = Number(req.params.docNum || 0);
    const head = await sapGetFirstByDocNum(
      "Orders",
      docNum,
      "DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments"
    );

    if (!head) return safeJson(res, 404, { ok: false, message: "Pedido no encontrado" });

    const order = await sapGetByDocEntry("Orders", head.DocEntry);
    safeJson(res, 200, { ok: true, order });
  } catch (e) {
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.get("/api/admin/sap/delivery/:docNum", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const docNum = Number(req.params.docNum || 0);
    const head = await sapGetFirstByDocNum(
      "DeliveryNotes",
      docNum,
      "DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName,DocumentStatus,CancelStatus,Comments"
    );

    if (!head) return safeJson(res, 404, { ok: false, message: "Entrega no encontrada" });

    const delivery = await sapGetByDocEntry("DeliveryNotes", head.DocEntry);
    safeJson(res, 200, { ok: true, delivery });
  } catch (e) {
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* ------------------ TRACE quote ------------------ */
app.get("/api/admin/trace/quote/:docNum", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const quoteDocNum = Number(req.params.docNum || 0);
    if (!quoteDocNum) return safeJson(res, 400, { ok: false, message: "docNum inválido" });

    const from = String(req.query?.from || "");
    const to = String(req.query?.to || "");

    const out = await traceQuote(quoteDocNum, from, to);
    if (!out.ok) return safeJson(res, 404, out);
    safeJson(res, 200, out);
  } catch (e) {
    safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* ------------------ Quotes list (Histórico) ------------------ */
app.get("/api/admin/quotes", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const from = String(req.query?.from || "");
    const to = String(req.query?.to || "");

    const userFilter = String(req.query?.user || "").trim().toLowerCase();
    const clientFilter = String(req.query?.client || "").trim().toLowerCase();

    const withDelivered = String(req.query?.withDelivered || "0") === "1";

    const top = req.query?.top != null
      ? Number(req.query.top)
      : (req.query?.limit != null ? Number(req.query.limit) : 20);

    const skip = req.query?.skip != null ? Number(req.query.skip) : 0;

    const safeTop = Math.max(1, Math.min(500, Number.isFinite(top) ? top : 20));
    const safeSkip = Math.max(0, Number.isFinite(skip) ? skip : 0);

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

    let quotes = Array.isArray(raw?.value) ? raw.value : [];

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

    if (userFilter) {
      out = out.filter((x) => String(x.usuario || "").toLowerCase().includes(userFilter));
    }
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
              q.pendiente = Number(
                tr.totals?.pendiente ||
                (Number(q.montoCotizacion || 0) - Number(q.montoEntregado || 0))
              );
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

/* ------------------ Dashboard (simple) ------------------ */
app.get("/api/admin/dashboard", verifyAdmin, async (req, res) => {
  safeJson(res, 200, { ok: true });
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

  app.listen(Number(PORT), () => {
    console.log(`Server listening on :${PORT}`);
  });
})();
