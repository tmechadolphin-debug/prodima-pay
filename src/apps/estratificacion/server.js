// prodima-pay/src/apps/estratificacion/server.js
import express from "express";
import pg from "pg";
import jwt from "jsonwebtoken";

const { Pool } = pg;

const app = express();
app.use(express.json({ limit: "6mb" }));

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
   ✅ Áreas por grupo (fallback)
========================================================= */
const GROUPS_CONS = new Set([
  "Prod. De Limpieza",
  "Químicos Trata. Agua",
  "Sazonadores",
  "Químicos Piscina",
  "Vinagres",
  "Especialidades y GMT",
]);

const GROUPS_RCI = new Set([
  "Equip. Y Acces. Agua",
  "Cuidado De La Ropa",
  "Servicios",
  "Art. De Limpieza",
  "Equip. Y Acces. Pis",
  "M.P.Res.Comer.ind.",
]);

function normalizeGroup(s) {
  return String(s || "").trim();
}

function inferAreaFromGroup(grupo) {
  const g = normalizeGroup(grupo);
  if (!g) return "";
  if (GROUPS_CONS.has(g)) return "CONS";
  if (GROUPS_RCI.has(g)) return "RCI";
  return "";
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

/**
 * Tablas (DB):
 * - item_group_cache: mapeo item_code -> grupo/area (ya la tienes llenándose)
 * - inv_item_cache: inventario actual (stock/min/max/committed/ordered/available)
 * - sales_item_lines: líneas netas (INV positivo, CRN negativo)
 * - sync_state
 */
async function ensureDb() {
  if (!hasDb()) return;

  // item_group_cache (por si no existe o le faltan columnas)
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS item_group_cache (
      item_code TEXT PRIMARY KEY,
      group_name TEXT DEFAULT '',
      updated_at TIMESTAMP DEFAULT NOW(),
      area TEXT DEFAULT '',
      grupo TEXT DEFAULT '',
      item_desc TEXT DEFAULT ''
    );
  `);

  await dbQuery(`ALTER TABLE item_group_cache ADD COLUMN IF NOT EXISTS group_name TEXT DEFAULT '';`);
  await dbQuery(`ALTER TABLE item_group_cache ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();`);
  await dbQuery(`ALTER TABLE item_group_cache ADD COLUMN IF NOT EXISTS area TEXT DEFAULT '';`);
  await dbQuery(`ALTER TABLE item_group_cache ADD COLUMN IF NOT EXISTS grupo TEXT DEFAULT '';`);
  await dbQuery(`ALTER TABLE item_group_cache ADD COLUMN IF NOT EXISTS item_desc TEXT DEFAULT '';`);

  // inventario
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS inv_item_cache (
      item_code TEXT PRIMARY KEY,
      item_desc TEXT NOT NULL DEFAULT '',
      stock NUMERIC(18,4) NOT NULL DEFAULT 0,
      stock_min NUMERIC(18,4) NOT NULL DEFAULT 0,
      stock_max NUMERIC(18,4) NOT NULL DEFAULT 0,
      committed NUMERIC(18,4) NOT NULL DEFAULT 0,
      ordered NUMERIC(18,4) NOT NULL DEFAULT 0,
      available NUMERIC(18,4) NOT NULL DEFAULT 0,
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await dbQuery(`ALTER TABLE inv_item_cache ADD COLUMN IF NOT EXISTS committed NUMERIC(18,4) NOT NULL DEFAULT 0;`);
  await dbQuery(`ALTER TABLE inv_item_cache ADD COLUMN IF NOT EXISTS ordered NUMERIC(18,4) NOT NULL DEFAULT 0;`);
  await dbQuery(`ALTER TABLE inv_item_cache ADD COLUMN IF NOT EXISTS available NUMERIC(18,4) NOT NULL DEFAULT 0;`);

  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_inv_item_code ON inv_item_cache(item_code);`);

  // ventas netas (inv/crn)
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS sales_item_lines (
      doc_entry INTEGER NOT NULL,
      line_num  INTEGER NOT NULL,
      doc_type  TEXT NOT NULL, -- 'INV' o 'CRN'
      doc_date  DATE NOT NULL,
      card_code TEXT NOT NULL DEFAULT '',
      warehouse TEXT NOT NULL DEFAULT '',
      item_code TEXT NOT NULL DEFAULT '',
      item_desc TEXT NOT NULL DEFAULT '',
      item_group TEXT NOT NULL DEFAULT '',
      quantity  NUMERIC(18,4) NOT NULL DEFAULT 0,
      revenue   NUMERIC(18,2) NOT NULL DEFAULT 0, -- neto
      gross_profit NUMERIC(18,2) NOT NULL DEFAULT 0, -- neto
      updated_at TIMESTAMP DEFAULT NOW(),
      PRIMARY KEY (doc_entry, line_num, doc_type)
    );
  `);

  // migraciones suaves
  await dbQuery(`ALTER TABLE sales_item_lines ADD COLUMN IF NOT EXISTS card_code TEXT NOT NULL DEFAULT '';`);
  await dbQuery(`ALTER TABLE sales_item_lines ADD COLUMN IF NOT EXISTS warehouse TEXT NOT NULL DEFAULT '';`);
  await dbQuery(`ALTER TABLE sales_item_lines ADD COLUMN IF NOT EXISTS item_group TEXT NOT NULL DEFAULT '';`);

  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_sales_item_date ON sales_item_lines(doc_date);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_sales_item_code ON sales_item_lines(item_code);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_sales_item_group ON sales_item_lines(item_group);`);

  // estado
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
   ✅ fetch wrapper
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
  const base = SAP_BASE_URL.replace(/\/$/, "");
  const url = `${base}/Login`;
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
  const timeoutMs = Number(options.timeoutMs || 60000);
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
   ✅ Sync: Inventario (BINGO method)
   /Items('CODE')?$select=ItemCode,ItemName,ItemWarehouseInfoCollection
========================================================= */
function pickNum(obj, keys) {
  for (const k of keys) {
    const n = Number(obj?.[k]);
    if (Number.isFinite(n)) return n;
  }
  return 0;
}

async function syncInventoryItems({ maxItems = 5000 } = {}) {
  const batchTop = 200;
  let skipSap = 0;
  let saved = 0;

  for (let page = 0; page < 400; page++) {
    const raw = await slFetch(
      `/Items?$select=ItemCode,ItemName&$orderby=ItemCode asc&$top=${batchTop}&$skip=${skipSap}`,
      { timeoutMs: 90000 }
    );

    const rows = Array.isArray(raw?.value) ? raw.value : [];
    if (!rows.length) break;
    skipSap += rows.length;

    for (const it of rows) {
      if (saved >= maxItems) return saved;

      const code = String(it.ItemCode || "").trim();
      if (!code) continue;

      const safe = code.replace(/'/g, "''");

      let full = null;
      try {
        full = await slFetch(
          `/Items('${safe}')?$select=ItemCode,ItemName,ItemWarehouseInfoCollection`,
          { timeoutMs: 90000 }
        );
      } catch {
        await sleep(5);
        continue;
      }

      const whRows = Array.isArray(full?.ItemWarehouseInfoCollection)
        ? full.ItemWarehouseInfoCollection
        : [];

      let stock = 0;
      let committed = 0;
      let ordered = 0;
      let mn = 0;
      let mx = 0;

      for (const w of whRows) {
        // stocks
        stock += pickNum(w, ["InStock", "OnHand"]);
        committed += pickNum(w, ["Committed", "IsCommited"]);
        ordered += pickNum(w, ["Ordered", "OnOrder"]);

        // min/max (tu respuesta trae MinimalStock/MaximalStock)
        mn += pickNum(w, ["MinimalStock", "MinStock"]);
        mx += pickNum(w, ["MaximalStock", "MaxStock"]);
      }

      const available = stock - committed + ordered;

      await dbQuery(
        `
        INSERT INTO inv_item_cache(item_code,item_desc,stock,stock_min,stock_max,committed,ordered,available,updated_at)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,NOW())
        ON CONFLICT(item_code) DO UPDATE SET
          item_desc=EXCLUDED.item_desc,
          stock=EXCLUDED.stock,
          stock_min=EXCLUDED.stock_min,
          stock_max=EXCLUDED.stock_max,
          committed=EXCLUDED.committed,
          ordered=EXCLUDED.ordered,
          available=EXCLUDED.available,
          updated_at=NOW()
        `,
        [
          code,
          String(full?.ItemName || it.ItemName || ""),
          stock,
          mn,
          mx,
          committed,
          ordered,
          available,
        ]
      );

      saved++;
      if (saved % 50 === 0) await sleep(10);
    }
  }

  return saved;
}

/* =========================================================
   ✅ Sync: Ventas netas por item (INV - CRN)
========================================================= */
function pickGrossProfit(ln) {
  const candidates = [ln?.GrossProfit, ln?.GrossProfitTotal, ln?.GrossProfitFC, ln?.GrossProfitSC];
  for (const c of candidates) {
    const n = Number(c);
    if (Number.isFinite(n)) return n;
  }
  return 0;
}

async function scanDocHeaders(entity, { from, to, maxDocs = 2500 }) {
  const toPlus1 = addDaysISO(to, 1);
  const batchTop = 200;
  let skipSap = 0;
  const out = [];

  for (let page = 0; page < 300; page++) {
    const raw = await slFetch(
      `/${entity}?$select=DocEntry,DocNum,DocDate&` +
        `$filter=${encodeURIComponent(`DocDate ge '${from}' and DocDate lt '${toPlus1}'`)}&` +
        `$orderby=DocDate asc,DocEntry asc&$top=${batchTop}&$skip=${skipSap}`,
      { timeoutMs: 90000 }
    );

    const rows = Array.isArray(raw?.value) ? raw.value : [];
    if (!rows.length) break;
    skipSap += rows.length;

    for (const r of rows) {
      out.push({ DocEntry: Number(r.DocEntry), DocDate: String(r.DocDate || "").slice(0, 10) });
      if (out.length >= maxDocs) return out;
    }
  }
  return out;
}

async function getDoc(entity, docEntry) {
  const de = Number(docEntry);
  if (!Number.isFinite(de) || de <= 0) return null;
  return slFetch(`/${entity}(${de})`, { timeoutMs: 120000 });
}

async function upsertSalesLines(docType, docDate, docFull, sign) {
  const docEntry = Number(docFull?.DocEntry || 0);
  const lines = Array.isArray(docFull?.DocumentLines) ? docFull.DocumentLines : [];
  if (!docEntry || !lines.length) return 0;

  const cardCode = String(docFull?.CardCode || "");
  let inserted = 0;

  for (const ln of lines) {
    const lineNum = Number(ln?.LineNum);
    if (!Number.isFinite(lineNum)) continue;

    const itemCode = String(ln?.ItemCode || "").trim();
    const itemDesc = String(ln?.ItemDescription || ln?.ItemName || "").trim();
    const wh = String(ln?.WarehouseCode || "").trim(); // puede venir vacío en algunos docs
    const group = String(ln?.ItemGroup ?? ln?.ItemsGroupCode ?? "").trim();

    const qty = Number(ln?.Quantity || 0) * sign;
    const rev = Number(ln?.LineTotal || 0) * sign;
    const gp = pickGrossProfit(ln) * sign;

    await dbQuery(
      `
      INSERT INTO sales_item_lines(doc_entry,line_num,doc_type,doc_date,card_code,warehouse,item_code,item_desc,item_group,quantity,revenue,gross_profit,updated_at)
      VALUES($1,$2,$3,$4::date,$5,$6,$7,$8,$9,$10,$11,$12,NOW())
      ON CONFLICT(doc_entry,line_num,doc_type) DO UPDATE SET
        doc_date=EXCLUDED.doc_date,
        card_code=EXCLUDED.card_code,
        warehouse=EXCLUDED.warehouse,
        item_code=EXCLUDED.item_code,
        item_desc=EXCLUDED.item_desc,
        item_group=EXCLUDED.item_group,
        quantity=EXCLUDED.quantity,
        revenue=EXCLUDED.revenue,
        gross_profit=EXCLUDED.gross_profit,
        updated_at=NOW()
      `,
      [docEntry, lineNum, docType, docDate, cardCode, wh, itemCode, itemDesc, group, qty, rev, gp]
    );

    inserted++;
    if (inserted % 150 === 0) await sleep(10);
  }

  return inserted;
}

async function syncSales({ from, to, maxDocs = 2500 }) {
  let saved = 0;

  const invHeaders = await scanDocHeaders("Invoices", { from, to, maxDocs });
  for (const h of invHeaders) {
    try {
      const full = await getDoc("Invoices", h.DocEntry);
      saved += await upsertSalesLines("INV", h.DocDate, full, +1);
    } catch {}
    await sleep(10);
  }

  const crnHeaders = await scanDocHeaders("CreditNotes", { from, to, maxDocs });
  for (const h of crnHeaders) {
    try {
      const full = await getDoc("CreditNotes", h.DocEntry);
      saved += await upsertSalesLines("CRN", h.DocDate, full, -1);
    } catch {}
    await sleep(10);
  }

  return saved;
}

/* =========================================================
   ✅ ABC helpers (A/B/C por contribución acumulada)
========================================================= */
function abcByMetric(rows, metricKey) {
  const arr = rows
    .map((r) => ({ key: r.itemCode, v: Math.max(0, Number(r[metricKey] || 0)) }))
    .sort((a, b) => b.v - a.v);

  const total = arr.reduce((a, x) => a + x.v, 0) || 0;
  let acc = 0;

  const out = new Map();
  for (const x of arr) {
    acc += x.v;
    const share = total > 0 ? acc / total : 1;
    const letter = share <= 0.8 ? "A" : share <= 0.95 ? "B" : "C";
    out.set(x.key, letter);
  }
  return out;
}

function totalLabelFromABC(a1, a2, a3) {
  const score = (l) => (l === "A" ? 3 : l === "B" ? 2 : 1);
  const avg = (score(a1) + score(a2) + score(a3)) / 3;

  if (avg >= 2.5) return { label: "AB Crítico", cls: "bad" };
  if (avg >= 1.8) return { label: "C Importante", cls: "warn" };
  return { label: "D", cls: "ok" };
}

/* =========================================================
   ✅ Dashboard (DB)  -> SIN item_master
   Usa: item_group_cache + sales_item_lines + inv_item_cache
========================================================= */
async function dashboardFromDb({ from, to, area, grupo, q }) {
  // 1) traer item_group_cache (catálogo)
  const groupWhere = [];
  const groupParams = [];
  let gp = 1;

  if (area && area !== "__ALL__") {
    groupWhere.push(`(COALESCE(NULLIF(area,''), '') = $${gp++})`);
    groupParams.push(area);
  }
  if (grupo && grupo !== "__ALL__") {
    groupWhere.push(`(COALESCE(NULLIF(grupo,''), COALESCE(NULLIF(group_name,''),'')) = $${gp++})`);
    groupParams.push(grupo);
  }
  if (q && String(q).trim()) {
    const qq = `%${String(q).trim().toLowerCase()}%`;
    groupWhere.push(`(LOWER(item_code) LIKE $${gp++} OR LOWER(COALESCE(item_desc,'')) LIKE $${gp++})`);
    groupParams.push(qq, qq);
  }

  const groupSql = `
    SELECT
      item_code,
      COALESCE(NULLIF(item_desc,''), '') AS item_desc,
      COALESCE(NULLIF(grupo,''), NULLIF(group_name,''), '') AS grupo,
      COALESCE(NULLIF(area,''), '') AS area
    FROM item_group_cache
    ${groupWhere.length ? `WHERE ${groupWhere.join(" AND ")}` : ""}
  `;
  const groupR = await dbQuery(groupSql, groupParams);

  // 2) ventas agregadas por item en rango
  const salesAgg = await dbQuery(
    `
    SELECT
      item_code,
      COALESCE(NULLIF(MAX(item_desc),''),'') AS item_desc,
      COALESCE(SUM(revenue),0)::numeric(18,2) AS revenue,
      COALESCE(SUM(gross_profit),0)::numeric(18,2) AS gp
    FROM sales_item_lines
    WHERE doc_date >= $1::date AND doc_date <= $2::date
    GROUP BY 1
    `,
    [from, to]
  );
  const salesMap = new Map(salesAgg.rows.map((r) => [String(r.item_code), r]));

  // 3) inventario actual
  const invR = await dbQuery(
    `SELECT item_code, stock::float AS stock, stock_min::float AS stock_min, stock_max::float AS stock_max,
            committed::float AS committed, ordered::float AS ordered, available::float AS available
     FROM inv_item_cache`,
    []
  );
  const invMap = new Map(invR.rows.map((r) => [String(r.item_code), r]));

  // 4) construir items: catálogo (groups) como base
  const items = (groupR.rows || []).map((g) => {
    const code = String(g.item_code || "");
    const s = salesMap.get(code) || { revenue: 0, gp: 0, item_desc: "" };
    const rev = Number(s.revenue || 0);
    const gpVal = Number(s.gp || 0);
    const pct = rev > 0 ? (gpVal / rev) * 100 : 0;

    const inv = invMap.get(code) || {
      stock: 0,
      stock_min: 0,
      stock_max: 0,
      committed: 0,
      ordered: 0,
      available: 0,
    };

    const grupoFinal = String(g.grupo || "Sin grupo") || "Sin grupo";
    const areaFinal = String(g.area || inferAreaFromGroup(grupoFinal) || "CONS");

    return {
      itemCode: code,
      itemDesc: String(g.item_desc || s.item_desc || ""),
      area: areaFinal,
      grupo: grupoFinal,
      revenue: rev,
      gp: gpVal,
      gpPct: Number(pct.toFixed(2)),
      stock: Number(inv.stock || 0),
      stockMin: Number(inv.stock_min || 0),
      stockMax: Number(inv.stock_max || 0),
      committed: Number(inv.committed || 0),
      ordered: Number(inv.ordered || 0),
      available: Number(inv.available || 0),
    };
  });

  // si filtraste por área pero hay registros con area vacío: re-filtrar por inferencia
  const filteredItems =
    area && area !== "__ALL__" ? items.filter((x) => x.area === area) : items;

  // grupos disponibles
  const groupSet = new Set(filteredItems.map((x) => x.grupo).filter(Boolean));
  const availableGroups = Array.from(groupSet).sort((a, b) => a.localeCompare(b));

  // ranking por grupo (rank área) basado en revenue
  const groupAggMap = new Map();
  for (const it of filteredItems) {
    const gname = it.grupo || "Sin grupo";
    const cur = groupAggMap.get(gname) || { grupo: gname, revenue: 0, gp: 0 };
    cur.revenue += it.revenue;
    cur.gp += it.gp;
    groupAggMap.set(gname, cur);
  }
  const groupAgg = Array.from(groupAggMap.values())
    .map((g) => ({
      ...g,
      gpPct: g.revenue > 0 ? Number(((g.gp / g.revenue) * 100).toFixed(2)) : 0,
    }))
    .sort((a, b) => b.revenue - a.revenue);

  const groupRank = new Map();
  groupAgg.forEach((g, idx) => groupRank.set(g.grupo, idx + 1));

  // ABC
  const abcRev = abcByMetric(filteredItems, "revenue");
  const abcGP = abcByMetric(filteredItems, "gp");
  const abcPct = abcByMetric(filteredItems, "gpPct");

  const outItems = filteredItems.map((it) => {
    const a1 = abcRev.get(it.itemCode) || "C";
    const a2 = abcGP.get(it.itemCode) || "C";
    const a3 = abcPct.get(it.itemCode) || "C";
    const total = totalLabelFromABC(a1, a2, a3);
    return {
      ...it,
      abcRevenue: a1,
      abcGP: a2,
      abcGPPct: a3,
      totalLabel: total.label,
      totalTagClass: total.cls,
      rankArea: groupRank.get(it.grupo) || 9999,
    };
  });

  const totals = outItems.reduce(
    (a, x) => {
      a.revenue += Number(x.revenue || 0);
      a.gp += Number(x.gp || 0);
      return a;
    },
    { revenue: 0, gp: 0 }
  );
  const gpPctTotal = totals.revenue > 0 ? Number(((totals.gp / totals.revenue) * 100).toFixed(2)) : 0;

  return {
    ok: true,
    from,
    to,
    area,
    grupo,
    q,
    lastSyncAt: await getState("last_sync_at"),
    totals: { revenue: totals.revenue, gp: totals.gp, gpPct: gpPctTotal },
    availableGroups,
    groupAgg,
    items: outItems.sort((a, b) => (a.rankArea - b.rankArea) || (b.revenue - a.revenue)),
  };
}

/* =========================================================
   ✅ Health + Auth
========================================================= */
app.get("/api/health", async (req, res) => {
  return safeJson(res, 200, {
    ok: true,
    message: "✅ PRODIMA ESTRATIFICACION API activa",
    sap: missingSapEnv() ? "missing" : "ok",
    db: hasDb() ? "on" : "off",
    last_sync_at: await getState("last_sync_at"),
  });
});

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
   ✅ Dashboard endpoint (DB)
========================================================= */
app.get("/api/admin/estratificacion/dashboard", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const fromQ = String(req.query?.from || "");
    const toQ = String(req.query?.to || "");
    const area = String(req.query?.area || "__ALL__");
    const grupo = String(req.query?.grupo || "__ALL__");
    const q = String(req.query?.q || "");

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = isISO(fromQ) ? fromQ : "2024-01-01";
    const to = isISO(toQ) ? toQ : today;

    const data = await dashboardFromDb({ from, to, area, grupo, q });
    return safeJson(res, 200, data);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================================================
   ✅ Debug counts (para consola)
========================================================= */
app.get("/api/admin/estratificacion/debug-counts", verifyAdmin, async (req, res) => {
  try {
    const a = await dbQuery(`SELECT COUNT(*)::int AS n FROM item_group_cache`);
    const b = await dbQuery(`SELECT COUNT(*)::int AS n FROM inv_item_cache`);
    const c = await dbQuery(`SELECT COUNT(*)::int AS n FROM sales_item_lines`);
    const last = await getState("last_sync_at");

    return safeJson(res, 200, {
      ok: true,
      item_group_cache: a.rows?.[0]?.n || 0,
      inv_item_cache: b.rows?.[0]?.n || 0,
      sales_item_lines: c.rows?.[0]?.n || 0,
      last_sync_at: last || null,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================================================
   ✅ Sync endpoint (SAP -> DB)
   GET /api/admin/estratificacion/sync?mode=days&n=5&maxDocs=2500
========================================================= */
app.get("/api/admin/estratificacion/sync", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const mode = String(req.query?.mode || "days").toLowerCase();
    const nRaw = Number(req.query?.n || 5);
    const n = Math.max(1, Math.min(120, Number.isFinite(nRaw) ? Math.trunc(nRaw) : 5));

    const maxDocsRaw = Number(req.query?.maxDocs || 2500);
    const maxDocs = Math.max(50, Math.min(20000, Number.isFinite(maxDocsRaw) ? Math.trunc(maxDocsRaw) : 2500));

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = addDaysISO(today, -n);

    // 1) inventory (actual)
    const invSaved = await syncInventoryItems({ maxItems: 999999 });

    // 2) sales recent (INV + CRN)
    const salesSaved = await syncSales({ from, to: today, maxDocs });

    await setState("last_sync_at", new Date().toISOString());
    await setState("last_sync_from", from);
    await setState("last_sync_to", today);

    return safeJson(res, 200, {
      ok: true,
      mode,
      n,
      maxDocs,
      invSaved,
      salesSaved,
      from,
      to: today,
      lastSyncAt: await getState("last_sync_at"),
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
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
