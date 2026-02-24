import express from "express";
import jwt from "jsonwebtoken";
import pg from "pg";

const { Pool } = pg;

const app = express();
app.use(express.json({ limit: "3mb" }));

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

  // CORS_ORIGIN=https://prodima.com.pa,https://www.prodima.com.pa
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
function isISO(s) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(s || ""));
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
const TZ_OFFSET_MIN = -300; // Panamá
function getDateISOInOffset(offsetMin = 0) {
  const now = new Date();
  const ms = now.getTime() + now.getTimezoneOffset() * 60000 + Number(offsetMin) * 60000;
  const d = new Date(ms);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

/* =========================================================
   ✅ Postgres (Supabase)
========================================================= */
const pool = new Pool({
  connectionString: DATABASE_URL || undefined,
  ssl: DATABASE_URL ? { rejectUnauthorized: false } : undefined,
});

function hasDb() {
  return Boolean(DATABASE_URL);
}
async function dbQuery(text, params = []) {
  return pool.query(text, params);
}

async function ensureDb() {
  if (!hasDb()) return;

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS fact_invoice_lines (
      doc_entry      INTEGER NOT NULL,
      line_num       INTEGER NOT NULL,
      doc_num        INTEGER NOT NULL,
      doc_date       DATE    NOT NULL,
      card_code      TEXT    NOT NULL,
      card_name      TEXT    NOT NULL,
      warehouse_code TEXT    NOT NULL,
      line_total     NUMERIC(18,2) NOT NULL,
      updated_at     TIMESTAMP DEFAULT NOW(),
      PRIMARY KEY (doc_entry, line_num)
    );
  `);

  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_fact_doc_date ON fact_invoice_lines(doc_date);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_fact_wh ON fact_invoice_lines(warehouse_code);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_fact_card ON fact_invoice_lines(card_code);`);

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS sync_state (
      k TEXT PRIMARY KEY,
      v TEXT NOT NULL,
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);
}

async function setState(k, v) {
  if (!hasDb()) return;
  await dbQuery(
    `INSERT INTO sync_state(k,v,updated_at) VALUES($1,$2,NOW())
     ON CONFLICT(k) DO UPDATE SET v=EXCLUDED.v, updated_at=NOW()`,
    [k, String(v)]
  );
}
async function getState(k) {
  if (!hasDb()) return "";
  const r = await dbQuery(`SELECT v FROM sync_state WHERE k=$1 LIMIT 1`, [k]);
  return r.rows?.[0]?.v || "";
}

/* =========================================================
   ✅ SAP Service Layer (cookie + timeout)
========================================================= */
let _fetch = globalThis.fetch || null;
async function httpFetch(url, options) {
  if (_fetch) return _fetch(url, options);
  const mod = await import("node-fetch");
  _fetch = mod.default;
  return _fetch(url, options);
}

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
}

async function slFetch(path, options = {}) {
  if (missingSapEnv()) throw new Error("Missing SAP env");
  if (!SL_COOKIE || Date.now() - SL_COOKIE_AT > 25 * 60 * 1000) await slLogin();

  const base = SAP_BASE_URL.replace(/\/$/, "");
  const url = `${base}${path.startsWith("/") ? path : `/${path}`}`;

  const controller = new AbortController();
  const timeoutMs = Number(options.timeoutMs || 60000); // ✅ 60s default aquí
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
    try { data = JSON.parse(txt); } catch { data = { raw: txt }; }

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
   ✅ SAP: scan invoices headers
========================================================= */
async function scanInvoicesHeaders({ f, t, maxDocs = 900 }) {
  const toPlus1 = addDaysISO(t, 1);
  const batchTop = 200;
  let skipSap = 0;
  const out = [];

  for (let page = 0; page < 200; page++) {
    const raw = await slFetch(
      `/Invoices?$select=DocEntry,DocNum,DocDate,DocTotal,CardCode,CardName` +
        `&$filter=${encodeURIComponent(`DocDate ge '${f}' and DocDate lt '${toPlus1}'`)}` +
        `&$orderby=DocDate desc,DocEntry desc&$top=${batchTop}&$skip=${skipSap}`,
      { timeoutMs: 60000 }
    );

    const rows = Array.isArray(raw?.value) ? raw.value : [];
    if (!rows.length) break;

    skipSap += rows.length;

    for (const r of rows) {
      out.push({
        DocEntry: Number(r.DocEntry),
        DocNum: Number(r.DocNum),
        DocDate: String(r.DocDate || "").slice(0, 10),
        DocTotal: Number(r.DocTotal || 0),
        CardCode: String(r.CardCode || ""),
        CardName: String(r.CardName || ""),
      });
      if (out.length >= maxDocs) return out;
    }
  }
  return out;
}

async function getInvoiceDoc(docEntry) {
  const de = Number(docEntry);
  if (!Number.isFinite(de) || de <= 0) return null;
  return slFetch(`/Invoices(${de})`, { timeoutMs: 90000 }); // ✅ 90s solo aquí
}

/* =========================================================
   ✅ Sync: upsert invoice lines into Postgres
========================================================= */
async function upsertLinesToDb(invHeader, docFull) {
  const lines = Array.isArray(docFull?.DocumentLines) ? docFull.DocumentLines : [];
  if (!lines.length) return 0;

  const docEntry = Number(invHeader.DocEntry);
  const docNum = Number(invHeader.DocNum);
  const docDate = String(invHeader.DocDate || "").slice(0, 10);
  const cardCode = String(invHeader.CardCode || "");
  const cardName = String(invHeader.CardName || "");

  // batch insert values
  const values = [];
  const params = [];
  let p = 1;

  for (const ln of lines) {
    const lineNum = Number(ln.LineNum);
    if (!Number.isFinite(lineNum)) continue;

    const wh = String(ln.WarehouseCode || "SIN_WH").trim() || "SIN_WH";
    const lt = Number(ln.LineTotal || 0);

    params.push(docEntry, lineNum, docNum, docDate, cardCode, cardName, wh, lt);
    values.push(`($${p++},$${p++},$${p++},$${p++}::date,$${p++},$${p++},$${p++},$${p++})`);
  }

  if (!values.length) return 0;

  await dbQuery(
    `
    INSERT INTO fact_invoice_lines
      (doc_entry,line_num,doc_num,doc_date,card_code,card_name,warehouse_code,line_total)
    VALUES ${values.join(",")}
    ON CONFLICT (doc_entry,line_num)
    DO UPDATE SET
      doc_num=EXCLUDED.doc_num,
      doc_date=EXCLUDED.doc_date,
      card_code=EXCLUDED.card_code,
      card_name=EXCLUDED.card_name,
      warehouse_code=EXCLUDED.warehouse_code,
      line_total=EXCLUDED.line_total,
      updated_at=NOW()
    `,
    params
  );

  return values.length;
}

async function syncRangeToDb({ from, to, maxDocs = 900 }) {
  if (!hasDb()) throw new Error("DB no configurada (DATABASE_URL)");

  const headers = await scanInvoicesHeaders({ f: from, t: to, maxDocs });
  if (!headers.length) return { headers: 0, lines: 0 };

  // Concurrencia baja para no matar SL
  const CONC = 1;
  let idx = 0;
  let totalLines = 0;

  async function worker() {
    while (idx < headers.length) {
      const i = idx++;
      const h = headers[i];
      try {
        const full = await getInvoiceDoc(h.DocEntry);
        const inserted = await upsertLinesToDb(h, full);
        totalLines += inserted;
      } catch (e) {
        // no romper todo por 1 doc
      }
      await sleep(20);
    }
  }

  await Promise.all(Array.from({ length: CONC }, () => worker()));

  await setState("last_sync_from", from);
  await setState("last_sync_to", to);
  await setState("last_sync_at", new Date().toISOString());

  return { headers: headers.length, lines: totalLines };
}

/* =========================================================
   ✅ Dashboard from DB (rápido)
========================================================= */
async function dashboardFromDb(from, to) {
  const tableQ = await dbQuery(
    `
    SELECT
      (card_code || ' · ' || card_name) AS customer,
      warehouse_code AS warehouse,
      SUM(line_total)::numeric(18,2) AS dollars,
      COUNT(DISTINCT doc_entry) AS invoices
    FROM fact_invoice_lines
    WHERE doc_date >= $1::date AND doc_date <= $2::date
    GROUP BY 1,2
    ORDER BY dollars DESC
    `,
    [from, to]
  );

  const totalsQ = await dbQuery(
    `
    SELECT
      COUNT(DISTINCT doc_entry) AS invoices,
      COALESCE(SUM(line_total),0)::numeric(18,2) AS dollars
    FROM fact_invoice_lines
    WHERE doc_date >= $1::date AND doc_date <= $2::date
    `,
    [from, to]
  );

  const byMonthQ = await dbQuery(
    `
    SELECT
      to_char(date_trunc('month', doc_date), 'YYYY-MM') AS month,
      COUNT(DISTINCT doc_entry) AS invoices,
      COALESCE(SUM(line_total),0)::numeric(18,2) AS dollars
    FROM fact_invoice_lines
    WHERE doc_date >= $1::date AND doc_date <= $2::date
    GROUP BY 1
    ORDER BY 1
    `,
    [from, to]
  );

  return {
    ok: true,
    from,
    to,
    totals: {
      invoices: Number(totalsQ.rows?.[0]?.invoices || 0),
      dollars: Number(totalsQ.rows?.[0]?.dollars || 0),
    },
    byMonth: (byMonthQ.rows || []).map((r) => ({
      month: r.month,
      invoices: Number(r.invoices || 0),
      dollars: Number(r.dollars || 0),
    })),
    table: (tableQ.rows || []).map((r) => ({
      customer: r.customer,
      warehouse: r.warehouse,
      dollars: Number(r.dollars || 0),
      invoices: Number(r.invoices || 0),
    })),
  };
}

/* =========================================================
   ✅ Routes
========================================================= */
app.get("/api/health", async (req, res) => {
  return safeJson(res, 200, {
    ok: true,
    message: "✅ PRODIMA INVOICES API activa",
    sap: missingSapEnv() ? "missing" : "ok",
    db: hasDb() ? "on" : "off",
    last_sync_at: await getState("last_sync_at"),
  });
});

/* ✅ Admin login */
app.post("/api/admin/login", async (req, res) => {
  const user = String(req.body?.user || "").trim();
  const pass = String(req.body?.pass || "").trim();

  if (user !== ADMIN_USER || pass !== ADMIN_PASS) {
    return safeJson(res, 401, { ok: false, message: "Credenciales inválidas" });
  }
  const token = signToken({ role: "admin", user }, "12h");
  return safeJson(res, 200, { ok: true, token });
});

/* ✅ Dashboard: SOLO DB (rápido y sin timeouts) */
app.get("/api/admin/invoices/dashboard", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const fromQ = String(req.query?.from || "");
    const toQ = String(req.query?.to || "");

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const defaultFrom = addDaysISO(today, -30);

    const from = isISO(fromQ) ? fromQ : defaultFrom;
    const to = isISO(toQ) ? toQ : today;

    const data = await dashboardFromDb(from, to);
    return safeJson(res, 200, data);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* ✅ Sync rango (backfill): POST /sync?from=YYYY-MM-DD&to=YYYY-MM-DD&maxDocs=900 */
app.post("/api/admin/invoices/sync", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const fromQ = String(req.query?.from || "");
    const toQ = String(req.query?.to || "");
    if (!isISO(fromQ) || !isISO(toQ)) {
      return safeJson(res, 400, { ok: false, message: "Requiere from y to (YYYY-MM-DD)" });
    }

    const maxDocsRaw = Number(req.query?.maxDocs || 900);
    const maxDocs = Math.max(50, Math.min(2000, Number.isFinite(maxDocsRaw) ? Math.trunc(maxDocsRaw) : 900));

    const out = await syncRangeToDb({ from: fromQ, to: toQ, maxDocs });
    return safeJson(res, 200, { ok: true, ...out, from: fromQ, to: toQ, maxDocs });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

/* ✅ Sync reciente: POST /sync/recent?days=14&maxDocs=1200 */
app.post("/api/admin/invoices/sync/recent", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const daysRaw = Number(req.query?.days || 14);
    const days = Math.max(1, Math.min(120, Number.isFinite(daysRaw) ? Math.trunc(daysRaw) : 14));

    const maxDocsRaw = Number(req.query?.maxDocs || 1200);
    const maxDocs = Math.max(50, Math.min(3000, Number.isFinite(maxDocsRaw) ? Math.trunc(maxDocsRaw) : 1200));

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = addDaysISO(today, -days);

    const out = await syncRangeToDb({ from, to: today, maxDocs });
    return safeJson(res, 200, { ok: true, ...out, from, to: today, days, maxDocs });
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
