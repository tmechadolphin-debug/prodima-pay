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
function normText(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

/* =========================================================
   ✅ Grupos por área (fallback)
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

function inferAreaFromGroup(grupo) {
  const g = String(grupo || "").trim();
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

async function ensureDb() {
  if (!hasDb()) return;

  // tabla base (ya la tienes)
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

  // inventario agregado
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

  // ventas netas por línea
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS sales_item_lines (
      doc_entry INTEGER NOT NULL,
      line_num  INTEGER NOT NULL,
      doc_type  TEXT NOT NULL, -- INV o CRN
      doc_date  DATE NOT NULL,
      card_code TEXT NOT NULL DEFAULT '',
      item_code TEXT NOT NULL DEFAULT '',
      item_desc TEXT NOT NULL DEFAULT '',
      quantity  NUMERIC(18,4) NOT NULL DEFAULT 0,
      revenue   NUMERIC(18,2) NOT NULL DEFAULT 0,
      gross_profit NUMERIC(18,2) NOT NULL DEFAULT 0,
      updated_at TIMESTAMP DEFAULT NOW(),
      PRIMARY KEY (doc_entry, line_num, doc_type)
    );
  `);

  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_sales_item_date ON sales_item_lines(doc_date);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_sales_item_code ON sales_item_lines(item_code);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_sales_card_code ON sales_item_lines(card_code);`);

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
      `/${entity}?$select=DocEntry,DocDate&` +
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
  const cardCode = String(docFull?.CardCode || "").trim();
  const lines = Array.isArray(docFull?.DocumentLines) ? docFull.DocumentLines : [];
  if (!docEntry || !lines.length) return 0;

  let inserted = 0;

  for (const ln of lines) {
    const lineNum = Number(ln?.LineNum);
    if (!Number.isFinite(lineNum)) continue;

    const itemCode = String(ln?.ItemCode || "").trim();
    if (!itemCode) continue;

    const itemDesc = String(ln?.ItemDescription || ln?.ItemName || "").trim();
    const qty = Number(ln?.Quantity || 0) * sign;
    const rev = Number(ln?.LineTotal || 0) * sign;
    const gp = pickGrossProfit(ln) * sign;

    await dbQuery(
      `
      INSERT INTO sales_item_lines(doc_entry,line_num,doc_type,doc_date,card_code,item_code,item_desc,quantity,revenue,gross_profit,updated_at)
      VALUES($1,$2,$3,$4::date,$5,$6,$7,$8,$9,$10,NOW())
      ON CONFLICT(doc_entry,line_num,doc_type) DO UPDATE SET
        doc_date=EXCLUDED.doc_date,
        card_code=EXCLUDED.card_code,
        item_code=EXCLUDED.item_code,
        item_desc=EXCLUDED.item_desc,
        quantity=EXCLUDED.quantity,
        revenue=EXCLUDED.revenue,
        gross_profit=EXCLUDED.gross_profit,
        updated_at=NOW()
      `,
      [docEntry, lineNum, docType, docDate, cardCode, itemCode, itemDesc, qty, rev, gp]
    );

    inserted++;
    if (inserted % 200 === 0) await sleep(10);
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
   ✅ Sync Inventario REAL (corrigiendo encoding + nombres)
   - Solo items relevantes: item_group_cache
========================================================= */
function odataKey(code) {
  // muy importante: esto evita que ItemCode con puntos, espacios, etc rompa el endpoint
  return encodeURIComponent(String(code || "").trim());
}

function n0(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : 0;
}

function pickField(obj, keys) {
  for (const k of keys) {
    if (obj && obj[k] != null) return obj[k];
  }
  return null;
}

async function getRelevantItemCodesForInventory(limit = 8000) {
  const r = await dbQuery(
    `SELECT item_code
     FROM item_group_cache
     WHERE COALESCE(NULLIF(item_code,''),'') <> ''
     ORDER BY updated_at DESC
     LIMIT $1`,
    [Math.max(10, Math.min(20000, Number(limit) || 8000))]
  );
  return (r.rows || []).map((x) => String(x.item_code || "").trim()).filter(Boolean);
}

async function fetchItemInventoryTotals(itemCode) {
  const code = String(itemCode || "").trim();
  if (!code) return null;

  // 1) name (ItemName)
  let itemName = "";
  try {
    const head = await slFetch(`/Items('${odataKey(code)}')?$select=ItemCode,ItemName`, { timeoutMs: 60000 });
    itemName = String(head?.ItemName || "").trim();
  } catch {
    // si ni siquiera se puede leer el item, no insistimos
    return null;
  }

  // 2) colección por warehouse
  let whRows = [];
  try {
    const col = await slFetch(
      `/Items('${odataKey(code)}')/ItemWarehouseInfoCollection` +
        `?$select=WarehouseCode,InStock,OnHand,Committed,IsCommited,Ordered,OnOrder,MinStock,MaxStock,MinimumStock,MaximumStock`,
      { timeoutMs: 90000 }
    );
    whRows = Array.isArray(col?.value) ? col.value : [];
  } catch {
    // fallback: algunos SL exponen otro nombre de entidad
    whRows = [];
  }

  // si no hay filas, devolvemos stock 0 pero guardamos desc
  let stock = 0,
    committed = 0,
    ordered = 0,
    stockMin = 0,
    stockMax = 0;

  for (const w of whRows) {
    stock += n0(pickField(w, ["InStock", "OnHand"]));
    committed += n0(pickField(w, ["Committed", "IsCommited"]));
    ordered += n0(pickField(w, ["Ordered", "OnOrder"]));
    stockMin += n0(pickField(w, ["MinStock", "MinimumStock"]));
    stockMax += n0(pickField(w, ["MaxStock", "MaximumStock"]));
  }

  const available = stock - committed + ordered;

  return {
    itemCode: code,
    itemDesc: itemName,
    stock,
    stockMin,
    stockMax,
    committed,
    ordered,
    available,
  };
}

async function upsertInventoryRow(row) {
  await dbQuery(
    `
    INSERT INTO inv_item_cache(item_code,item_desc,stock,stock_min,stock_max,committed,ordered,available,updated_at)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,NOW())
    ON CONFLICT(item_code) DO UPDATE SET
      item_desc=COALESCE(NULLIF(EXCLUDED.item_desc,''), inv_item_cache.item_desc),
      stock=EXCLUDED.stock,
      stock_min=EXCLUDED.stock_min,
      stock_max=EXCLUDED.stock_max,
      committed=EXCLUDED.committed,
      ordered=EXCLUDED.ordered,
      available=EXCLUDED.available,
      updated_at=NOW()
    `,
    [
      row.itemCode,
      row.itemDesc || "",
      Number(row.stock || 0),
      Number(row.stockMin || 0),
      Number(row.stockMax || 0),
      Number(row.committed || 0),
      Number(row.ordered || 0),
      Number(row.available || 0),
    ]
  );
}

async function syncInventoryForRelevantItems({ maxItems = 8000 }) {
  const codes = await getRelevantItemCodesForInventory(maxItems);
  if (!codes.length) return { scanned: 0, saved: 0, note: "No hay item_group_cache para tomar items." };

  let saved = 0;
  let scanned = 0;

  const CONC = 3;
  let i = 0;

  async function worker() {
    while (i < codes.length) {
      const idx = i++;
      const code = codes[idx];
      scanned++;

      try {
        const row = await fetchItemInventoryTotals(code);
        if (row) {
          await upsertInventoryRow(row);
          saved++;
        }
      } catch {}

      if (scanned % 40 === 0) await sleep(30);
    }
  }

  await Promise.all(Array.from({ length: CONC }, worker));
  return { scanned, saved };
}

/* =========================================================
   ✅ ABC helpers
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
   ✅ Dashboard (DB) usando item_group_cache + inv_item_cache
========================================================= */
async function dashboardFromDb({ from, to, area, grupo, q }) {
  const params = [from, to];
  let p = 3;

  let whereG = "1=1";
  if (area && area !== "__ALL__") {
    params.push(area);
    whereG += ` AND g.area = $${p++}`;
  }
  if (grupo && grupo !== "__ALL__") {
    params.push(grupo);
    whereG += ` AND g.grupo = $${p++}`;
  }
  if (q && String(q).trim()) {
    const qq = `%${normText(q)}%`;
    params.push(qq);
    params.push(qq);
    whereG += ` AND (LOWER(g.item_code) LIKE $${p++} OR LOWER(g.item_desc) LIKE $${p++})`;
  }

  const salesAgg = await dbQuery(
    `
    SELECT
      g.item_code AS item_code,
      COALESCE(NULLIF(g.item_desc,''), '') AS item_desc,
      COALESCE(NULLIF(g.area,''), '') AS area,
      COALESCE(NULLIF(g.grupo,''), COALESCE(NULLIF(g.group_name,''), 'Sin grupo')) AS grupo,
      COALESCE(SUM(s.revenue),0)::numeric(18,2) AS revenue,
      COALESCE(SUM(s.gross_profit),0)::numeric(18,2) AS gp
    FROM item_group_cache g
    LEFT JOIN sales_item_lines s
      ON s.item_code = g.item_code
      AND s.doc_date >= $1::date AND s.doc_date <= $2::date
    WHERE ${whereG}
    GROUP BY 1,2,3,4
    ORDER BY revenue DESC
    `,
    params
  );

  const inv = await dbQuery(
    `
    SELECT item_code,
           stock::float AS stock,
           stock_min::float AS stock_min,
           stock_max::float AS stock_max,
           committed::float AS committed,
           ordered::float AS ordered,
           available::float AS available
    FROM inv_item_cache
    `,
    []
  );
  const invMap = new Map(inv.rows.map((r) => [String(r.item_code), r]));

  const items = (salesAgg.rows || []).map((r) => {
    const rev = Number(r.revenue || 0);
    const gp = Number(r.gp || 0);
    const pct = rev > 0 ? (gp / rev) * 100 : 0;

    const rawGrupo = String(r.grupo || "Sin grupo").trim() || "Sin grupo";
    let a = String(r.area || "").trim();
    if (!a) a = inferAreaFromGroup(rawGrupo) || "CONS";

    const invRow = invMap.get(String(r.item_code)) || {
      stock: 0,
      stock_min: 0,
      stock_max: 0,
      committed: 0,
      ordered: 0,
      available: 0,
    };

    return {
      itemCode: r.item_code,
      itemDesc: r.item_desc || "",
      area: a,
      grupo: rawGrupo,
      revenue: rev,
      gp: gp,
      gpPct: Number(pct.toFixed(2)),
      stock: Number(invRow.stock || 0),
      stockMin: Number(invRow.stock_min || 0),
      stockMax: Number(invRow.stock_max || 0),
      committed: Number(invRow.committed || 0),
      ordered: Number(invRow.ordered || 0),
      available: Number(invRow.available || 0),
    };
  });

  const groupSet = new Set(items.map((x) => x.grupo).filter(Boolean));
  const availableGroups = Array.from(groupSet).sort((a, b) => a.localeCompare(b));

  const groupAggMap = new Map();
  for (const it of items) {
    const g = it.grupo || "Sin grupo";
    const cur = groupAggMap.get(g) || { grupo: g, revenue: 0, gp: 0 };
    cur.revenue += it.revenue;
    cur.gp += it.gp;
    groupAggMap.set(g, cur);
  }
  const groupAgg = Array.from(groupAggMap.values())
    .map((g) => ({
      ...g,
      gpPct: g.revenue > 0 ? Number(((g.gp / g.revenue) * 100).toFixed(2)) : 0,
    }))
    .sort((a, b) => b.revenue - a.revenue);

  const groupRank = new Map();
  groupAgg.forEach((g, idx) => groupRank.set(g.grupo, idx + 1));

  const abcRev = abcByMetric(items, "revenue");
  const abcGP = abcByMetric(items, "gp");
  const abcPct = abcByMetric(items, "gpPct");

  const outItems = items.map((it) => {
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
    items: outItems.sort((a, b) => a.rankArea - b.rankArea || b.revenue - a.revenue),
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

    const salesSaved = await syncSales({ from, to: today, maxDocs });
    const invRes = await syncInventoryForRelevantItems({ maxItems: 8000 });

    await setState("last_sync_at", new Date().toISOString());
    await setState("last_sync_from", from);
    await setState("last_sync_to", today);

    return safeJson(res, 200, {
      ok: true,
      mode,
      n,
      maxDocs,
      salesSaved,
      invScanned: invRes.scanned,
      invSaved: invRes.saved,
      invNote: invRes.note || null,
      from,
      to: today,
      lastSyncAt: await getState("last_sync_at"),
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================================================
   ✅ Debug counts
========================================================= */
app.get("/api/admin/estratificacion/debug-counts", verifyAdmin, async (req, res) => {
  try {
    const r1 = await dbQuery(`SELECT COUNT(*)::int AS n FROM item_group_cache`);
    const r2 = await dbQuery(`SELECT COUNT(*)::int AS n FROM inv_item_cache`);
    const r3 = await dbQuery(`SELECT COUNT(*)::int AS n FROM sales_item_lines`);
    return safeJson(res, 200, {
      ok: true,
      item_group_cache: r1.rows?.[0]?.n || 0,
      inv_item_cache: r2.rows?.[0]?.n || 0,
      sales_item_lines: r3.rows?.[0]?.n || 0,
      last_sync_at: await getState("last_sync_at"),
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message });
  }
});

app.get("/api/admin/estratificacion/debug-sap-inv", verifyAdmin, async (req, res) => {
  try {
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const code = String(req.query?.code || "").trim();
    if (!code) return safeJson(res, 400, { ok: false, message: "code requerido" });

    const safe = code.replace(/'/g, "''");

    // 1) intenta por sub-recurso
    let raw1 = null;
    try {
      raw1 = await slFetch(
        `/Items('${safe}')/ItemWarehouseInfoCollection?$select=WarehouseCode,InStock,Committed,Ordered,MinStock,MaxStock,OnHand,IsCommited,OnOrder`,
        { timeoutMs: 90000 }
      );
    } catch (e) {
      raw1 = { error: String(e.message || e) };
    }

    // 2) intenta por expand (a veces funciona cuando el sub-recurso no)
    let raw2 = null;
    try {
      raw2 = await slFetch(
        `/Items('${safe}')?$select=ItemCode,ItemName&$expand=ItemWarehouseInfoCollection($select=WarehouseCode,InStock,Committed,Ordered,MinStock,MaxStock,OnHand,IsCommited,OnOrder)`,
        { timeoutMs: 90000 }
      );
    } catch (e) {
      raw2 = { error: String(e.message || e) };
    }

    return safeJson(res, 200, { ok: true, code, raw1, raw2 });
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
