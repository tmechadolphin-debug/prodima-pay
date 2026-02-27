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
   ✅ GRUPOS (TU REGLA DE NEGOCIO)
========================================================= */
const GROUPS_CONS = new Set([
  "Prod. De Limpieza",
  "Cuidado De La Ropa",
  "Sazonadores",
  "Art. De Limpieza",
  "Vinagres",
  "Especialidades y GMT",
]);

const GROUPS_RCI = new Set([
  "Equip. Y Acces. Agua",
  "Químicos Piscina",
  "Servicios",
  "Químicos Trat. Agua",
  "Equip. Y Acces. Pisc",
  "M.P.Res.Comer.Ind.",
]);

/* =========================================================
   ✅ NORMALIZACIÓN + CANONICALIZACIÓN (+ HEURÍSTICA)
========================================================= */
function normGroupName(s) {
  return String(s || "")
    .trim()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "") // quita acentos
    .replace(/\s+/g, " ")
    .toUpperCase();
}

const GROUPS_CONS_N = new Set(Array.from(GROUPS_CONS).map(normGroupName));
const GROUPS_RCI_N = new Set(Array.from(GROUPS_RCI).map(normGroupName));

const CANON_GROUP = new Map(
  [...Array.from(GROUPS_CONS), ...Array.from(GROUPS_RCI)].map((g) => [normGroupName(g), g])
);

function canonicalGroupName(groupName) {
  const n = normGroupName(groupName);
  return CANON_GROUP.get(n) || String(groupName || "").trim();
}

/**
 * ✅ HEURÍSTICA: fuerza variantes del SAP a tus 12 grupos
 * (resuelve la mayoría de "Sin grupo" por abreviaturas/puntos/mayúsculas)
 */
function guessCanonicalGroupName(groupNameRaw) {
  const n = normGroupName(groupNameRaw);
  if (!n) return "";

  // ---- RCI ----
  if (n.includes("EQUIP") && n.includes("AGUA")) return "Equip. Y Acces. Agua";
  if (n.includes("EQUIP") && (n.includes("PISC") || n.includes("PISCINA"))) return "Equip. Y Acces. Pisc";

  if ((n.includes("QUIM") || n.includes("QUIMIC")) && (n.includes("PISC") || n.includes("PISCINA")))
    return "Químicos Piscina";

  if ((n.includes("QUIM") || n.includes("QUIMIC")) && n.includes("TRAT") && n.includes("AGUA"))
    return "Químicos Trat. Agua";

  if (n.includes("SERV")) return "Servicios";

  if (n.includes("M.P") || (n.includes("MP") && (n.includes("COMER") || n.includes("IND"))))
    return "M.P.Res.Comer.Ind.";

  // ---- CONS ----
  if (n.includes("SAZON")) return "Sazonadores";
  if (n.includes("VINAGR")) return "Vinagres";
  if (n.includes("ROPA")) return "Cuidado De La Ropa";

  if (n.includes("LIMPIEZ") || n.includes("LIMPIEZA") || n.includes("CLEAN")) {
    if (n.includes("ART") || n.includes("ESPON") || n.includes("PANO") || n.includes("PAÑO")) return "Art. De Limpieza";
    return "Prod. De Limpieza";
  }

  if (n.includes("ESPECIAL") || n.includes("GMT")) return "Especialidades y GMT";

  return "";
}

function normalizeGrupoFinal(grupoRaw) {
  const raw = String(grupoRaw || "").trim();
  if (!raw) return "Sin grupo";

  const canon = canonicalGroupName(raw);
  const canonN = normGroupName(canon);

  // si ya está en tus listas, perfecto
  if (GROUPS_CONS_N.has(canonN) || GROUPS_RCI_N.has(canonN)) return canon;

  // si no, prueba heurística
  const guessed = guessCanonicalGroupName(raw);
  if (guessed) return guessed;

  return canon || "Sin grupo";
}

function inferAreaFromGroup(groupName) {
  const g = normGroupName(groupName);
  if (!g) return "";
  if (GROUPS_CONS_N.has(g)) return "CONS";
  if (GROUPS_RCI_N.has(g)) return "RCI";
  return "";
}

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

/**
 * ✅ Tablas usadas por Estratificación
 */
async function ensureDb() {
  if (!hasDb()) return;

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS sales_item_lines (
      doc_entry INTEGER NOT NULL,
      line_num  INTEGER NOT NULL,
      doc_type  TEXT NOT NULL, -- 'INV' o 'CRN'
      doc_date  DATE NOT NULL,
      item_code TEXT NOT NULL DEFAULT '',
      item_desc TEXT NOT NULL DEFAULT '',
      quantity  NUMERIC(18,4) NOT NULL DEFAULT 0,
      revenue   NUMERIC(18,2) NOT NULL DEFAULT 0,
      gross_profit NUMERIC(18,2) NOT NULL DEFAULT 0,
      item_group TEXT DEFAULT '',
      area TEXT DEFAULT '',
      warehouse TEXT DEFAULT '',
      updated_at TIMESTAMP DEFAULT NOW(),
      PRIMARY KEY (doc_entry, line_num, doc_type)
    );
  `);

  await dbQuery(`ALTER TABLE sales_item_lines ADD COLUMN IF NOT EXISTS doc_num INTEGER;`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_sales_item_date ON sales_item_lines(doc_date);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_sales_item_code ON sales_item_lines(item_code);`);

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

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS item_group_cache (
      item_code TEXT PRIMARY KEY,
      group_name TEXT NOT NULL DEFAULT '',
      area TEXT NOT NULL DEFAULT '',
      grupo TEXT NOT NULL DEFAULT '',
      item_desc TEXT NOT NULL DEFAULT '',
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await dbQuery(`ALTER TABLE item_group_cache ADD COLUMN IF NOT EXISTS group_name TEXT NOT NULL DEFAULT '';`);
  await dbQuery(`ALTER TABLE item_group_cache ADD COLUMN IF NOT EXISTS area TEXT NOT NULL DEFAULT '';`);
  await dbQuery(`ALTER TABLE item_group_cache ADD COLUMN IF NOT EXISTS grupo TEXT NOT NULL DEFAULT '';`);
  await dbQuery(`ALTER TABLE item_group_cache ADD COLUMN IF NOT EXISTS item_desc TEXT NOT NULL DEFAULT '';`);
  await dbQuery(`ALTER TABLE item_group_cache ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();`);

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
      `/${entity}?$select=DocEntry,DocNum,DocDate&` +
        `$filter=${encodeURIComponent(`DocDate ge '${from}' and DocDate lt '${toPlus1}'`)}&` +
        `$orderby=DocDate asc,DocEntry asc&$top=${batchTop}&$skip=${skipSap}`,
      { timeoutMs: 120000 }
    );

    const rows = Array.isArray(raw?.value) ? raw.value : [];
    if (!rows.length) break;
    skipSap += rows.length;

    for (const r of rows) {
      out.push({
        DocEntry: Number(r.DocEntry),
        DocNum: r.DocNum != null ? Number(r.DocNum) : null,
        DocDate: String(r.DocDate || "").slice(0, 10),
      });
      if (out.length >= maxDocs) return out;
    }
  }
  return out;
}

async function getDoc(entity, docEntry) {
  const de = Number(docEntry);
  if (!Number.isFinite(de) || de <= 0) return null;
  return slFetch(`/${entity}(${de})`, { timeoutMs: 180000 });
}

async function upsertSalesLines(docType, docDate, docFull, sign) {
  const docEntry = Number(docFull?.DocEntry || 0);
  const docNum = docFull?.DocNum != null ? Number(docFull.DocNum) : null;
  const lines = Array.isArray(docFull?.DocumentLines) ? docFull.DocumentLines : [];
  if (!docEntry || !lines.length) return 0;

  let inserted = 0;

  for (const ln of lines) {
    const lineNum = Number(ln?.LineNum);
    if (!Number.isFinite(lineNum)) continue;

    const itemCode = String(ln?.ItemCode || "").trim();
    if (!itemCode) continue;

    const itemDesc = String(ln?.ItemDescription || ln?.ItemName || "").trim();

    // ✅ FIX NC: ABS + signo para que CRN siempre reste
    const qtyRaw = Number(ln?.Quantity || 0);
    const revRaw = Number(ln?.LineTotal || 0);
    const gpRaw = Number(pickGrossProfit(ln) || 0);

    const qty = Math.abs(qtyRaw) * sign;
    const rev = Math.abs(revRaw) * sign;
    const gp = Math.abs(gpRaw) * sign;

    await dbQuery(
      `
      INSERT INTO sales_item_lines(
        doc_entry,line_num,doc_type,doc_date,doc_num,item_code,item_desc,quantity,revenue,gross_profit,updated_at
      )
      VALUES($1,$2,$3,$4::date,$5,$6,$7,$8,$9,$10,NOW())
      ON CONFLICT(doc_entry,line_num,doc_type) DO UPDATE SET
        doc_date=EXCLUDED.doc_date,
        doc_num=EXCLUDED.doc_num,
        item_code=EXCLUDED.item_code,
        item_desc=EXCLUDED.item_desc,
        quantity=EXCLUDED.quantity,
        revenue=EXCLUDED.revenue,
        gross_profit=EXCLUDED.gross_profit,
        updated_at=NOW()
      `,
      [docEntry, lineNum, docType, docDate, docNum, itemCode, itemDesc, qty, rev, gp]
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
   ✅ Inventario (solo bodegas 300,200,500,01)
========================================================= */
const INV_WH_ALLOWED = new Set(["300", "200", "500", "01"]);

function sumInvFromWarehouseInfo(infoArr) {
  const rowsAll = Array.isArray(infoArr) ? infoArr : [];
  const rows = rowsAll.filter((w) => INV_WH_ALLOWED.has(String(w?.WarehouseCode ?? w?.WhsCode ?? "").trim()));

  let stock = 0;
  let committed = 0;
  let ordered = 0;

  let stockMin = 0;
  let stockMax = 0;

  for (const w of rows) {
    stock += Number(w?.InStock ?? w?.OnHand ?? 0);
    committed += Number(w?.Committed ?? w?.IsCommited ?? 0);
    ordered += Number(w?.Ordered ?? w?.OnOrder ?? 0);

    const mn = Number(w?.MinimalStock ?? w?.MinStock ?? 0);
    const mx = Number(w?.MaximalStock ?? w?.MaxStock ?? 0);
    if (Number.isFinite(mn) && mn > stockMin) stockMin = mn;
    if (Number.isFinite(mx) && mx > stockMax) stockMax = mx;
  }

  const available = stock - committed + ordered;

  return { stock, committed, ordered, available, stockMin, stockMax };
}

async function getInventoryForItemCode(code) {
  const itemCode = String(code || "").trim();
  if (!itemCode) return null;

  const safe = itemCode.replace(/'/g, "''");
  const a = await slFetch(`/Items('${safe}')?$select=ItemCode,ItemName,ItemWarehouseInfoCollection`, { timeoutMs: 120000 });

  const itemName = String(a?.ItemName || "").trim();
  const inv = sumInvFromWarehouseInfo(a?.ItemWarehouseInfoCollection);

  return { itemCode, itemDesc: itemName, ...inv };
}

async function syncInventoryForSalesItems({ from, to, maxItems = 1200 }) {
  const r = await dbQuery(
    `
    SELECT DISTINCT item_code
    FROM sales_item_lines
    WHERE doc_date >= $1::date AND doc_date <= $2::date
      AND item_code <> ''
    LIMIT $3
    `,
    [from, to, Math.max(100, Math.min(5000, Number(maxItems || 1200)))]
  );

  const codes = (r.rows || []).map((x) => String(x.item_code || "").trim()).filter(Boolean);
  let saved = 0;

  for (let i = 0; i < codes.length; i++) {
    const code = codes[i];
    try {
      const inv = await getInventoryForItemCode(code);
      if (!inv) continue;

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
          inv.itemCode,
          inv.itemDesc || "",
          Number(inv.stock || 0),
          Number(inv.stockMin || 0),
          Number(inv.stockMax || 0),
          Number(inv.committed || 0),
          Number(inv.ordered || 0),
          Number(inv.available || 0),
        ]
      );

      saved++;
    } catch {}
    if ((i + 1) % 25 === 0) await sleep(15);
  }

  return saved;
}

/* =========================================================
   ✅ Sync: Grupos por Item (ItemGroups)
========================================================= */
async function getItemGroupNameFromSap(itemCode) {
  const code = String(itemCode || "").trim();
  if (!code) return { groupName: "", itemName: "", itmsGrpCod: null };

  const safe = code.replace(/'/g, "''");
  const item = await slFetch(`/Items('${safe}')?$select=ItemCode,ItemName,ItemsGroupCode`, { timeoutMs: 90000 });

  const itmsGrpCod = item?.ItemsGroupCode != null ? Number(item.ItemsGroupCode) : null;
  const itemName = String(item?.ItemName || "").trim();

  if (!Number.isFinite(itmsGrpCod) || itmsGrpCod == null) {
    return { groupName: "", itemName, itmsGrpCod: null };
  }

  try {
    const g = await slFetch(`/ItemGroups(${itmsGrpCod})?$select=GroupName`, { timeoutMs: 90000 });
    const groupName = String(g?.GroupName || "").trim();
    return { groupName, itemName, itmsGrpCod };
  } catch {
    return { groupName: "", itemName, itmsGrpCod };
  }
}

async function syncItemGroupsForSalesItems({ from, to, maxItems = 1500 }) {
  const r = await dbQuery(
    `
    SELECT DISTINCT item_code, MAX(NULLIF(item_desc,'')) AS item_desc
    FROM sales_item_lines
    WHERE doc_date >= $1::date AND doc_date <= $2::date
      AND item_code <> ''
    GROUP BY item_code
    LIMIT $3
    `,
    [from, to, Math.max(100, Math.min(8000, Number(maxItems || 1500)))]
  );

  const rows = r.rows || [];
  let saved = 0;

  for (let i = 0; i < rows.length; i++) {
    const code = String(rows[i].item_code || "").trim();
    const descFromSales = String(rows[i].item_desc || "").trim();
    if (!code) continue;

    try {
      const sap = await getItemGroupNameFromSap(code);
      const groupNameRaw = String(sap.groupName || "").trim();

      // ✅ FIX: normaliza (canon + heurística) para evitar "Sin grupo"
      const grupo = normalizeGrupoFinal(groupNameRaw || "");
      const area = inferAreaFromGroup(grupo) || inferAreaFromGroup(groupNameRaw) || "";

      const itemDesc = (String(sap.itemName || "").trim() || descFromSales || "");

      await dbQuery(
        `
        INSERT INTO item_group_cache(item_code,group_name,area,grupo,item_desc,updated_at)
        VALUES($1,$2,$3,$4,$5,NOW())
        ON CONFLICT(item_code) DO UPDATE SET
          group_name=EXCLUDED.group_name,
          area=CASE WHEN EXCLUDED.area <> '' THEN EXCLUDED.area ELSE item_group_cache.area END,
          grupo=CASE WHEN EXCLUDED.grupo <> '' THEN EXCLUDED.grupo ELSE item_group_cache.grupo END,
          item_desc=CASE WHEN EXCLUDED.item_desc <> '' THEN EXCLUDED.item_desc ELSE item_group_cache.item_desc END,
          updated_at=NOW()
        `,
        [code, groupNameRaw, area, grupo, itemDesc]
      );

      saved++;
    } catch {}
    if ((i + 1) % 25 === 0) await sleep(15);
  }

  return saved;
}

/* =========================================================
   ✅ ABC helpers (AHORA A/B/C/D como Excel)
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
    const letter = share <= 0.8 ? "A" : share <= 0.95 ? "B" : share <= 0.99 ? "C" : "D";
    out.set(x.key, letter);
  }
  return out;
}

function letterScore(l) {
  const L = String(l || "").toUpperCase();
  if (L === "A") return 4;
  if (L === "B") return 3;
  if (L === "C") return 2;
  return 1; // D o vacío
}

function totalFromLetters(a1, a2, a3) {
  const r = letterScore(a1);
  const g = letterScore(a2);
  const p = letterScore(a3);
  const avg = (r + g + p) / 3;
  const t = Math.round(avg * 10) / 10; // 1 decimal como Excel

  if (t >= 3.5) return { label: "AB Crítico", cls: "bad", r, g, p, t };
  if (t >= 2.0) return { label: "C Importante", cls: "warn", r, g, p, t };
  return { label: "D", cls: "ok", r, g, p, t };
}

/* =========================================================
   ✅ Dashboard (DB) con universo ABC como Excel + orden
   ✅ FIX: normaliza grupos (canon + heurística) en lectura también
========================================================= */
async function dashboardFromDb({ from, to, area, grupo, q }) {
  const salesAgg = await dbQuery(
    `
    WITH s AS (
      SELECT
        item_code,
        MAX(NULLIF(item_desc,'')) AS item_desc,
        COALESCE(SUM(revenue),0)::numeric(18,2) AS revenue,
        COALESCE(SUM(gross_profit),0)::numeric(18,2) AS gp,
        MAX(NULLIF(area,'')) AS area_s,
        MAX(NULLIF(item_group,'')) AS grupo_s
      FROM sales_item_lines
      WHERE doc_date >= $1::date AND doc_date <= $2::date
      GROUP BY item_code
    )
    SELECT
      s.item_code AS item_code,
      COALESCE(NULLIF(s.item_desc,''), NULLIF(g.item_desc,''), '') AS item_desc,
      COALESCE(NULLIF(s.grupo_s,''), NULLIF(g.grupo,''), NULLIF(g.group_name,''), 'Sin grupo') AS grupo,
      COALESCE(NULLIF(s.area_s,''), NULLIF(g.area,''), '') AS area,
      s.revenue AS revenue,
      s.gp AS gp,
      COALESCE(i.stock,0)::float AS stock,
      COALESCE(i.stock_min,0)::float AS stock_min,
      COALESCE(i.stock_max,0)::float AS stock_max,
      COALESCE(i.committed,0)::float AS committed,
      COALESCE(i.ordered,0)::float AS ordered,
      COALESCE(i.available,0)::float AS available
    FROM s
    LEFT JOIN item_group_cache g ON g.item_code = s.item_code
    LEFT JOIN inv_item_cache i   ON i.item_code = s.item_code
    ORDER BY s.revenue DESC
    `,
    [from, to]
  );

  let items = (salesAgg.rows || []).map((r) => {
    const rev = Number(r.revenue || 0);
    const gp = Number(r.gp || 0);
    const pct = rev > 0 ? (gp / rev) * 100 : 0;

    const grupoTxtRaw = String(r.grupo || "Sin grupo");
    const grupoTxt = normalizeGrupoFinal(grupoTxtRaw);

    const areaDb = String(r.area || "");
    const areaFinal = areaDb || inferAreaFromGroup(grupoTxt) || inferAreaFromGroup(grupoTxtRaw) || "CONS";

    return {
      itemCode: String(r.item_code || ""),
      itemDesc: String(r.item_desc || ""),
      area: areaFinal,
      grupo: grupoTxt,
      revenue: rev,
      gp: gp,
      gpPct: Number(pct.toFixed(2)),
      stock: Number(r.stock || 0),
      stockMin: Number(r.stock_min || 0),
      stockMax: Number(r.stock_max || 0),
      committed: Number(r.committed || 0),
      ordered: Number(r.ordered || 0),
      available: Number(r.available || 0),
    };
  });

  const areaSel = String(area || "__ALL__");
  const grupoSel = String(grupo || "__ALL__");
  const qq = String(q || "").trim().toLowerCase();

  // availableGroups (según filtro área)
  let availableGroups = [];
  if (areaSel === "CONS") availableGroups = Array.from(GROUPS_CONS);
  else if (areaSel === "RCI") availableGroups = Array.from(GROUPS_RCI);
  else availableGroups = Array.from(new Set([...GROUPS_CONS, ...GROUPS_RCI]));
  availableGroups.sort((a, b) => a.localeCompare(b));

  /* =========================
     ✅ UNIVERSO ABC (NO depende de q)
  ========================= */
  let universe = items.slice();

  if (areaSel !== "__ALL__") {
    universe = universe.filter((x) => String(x.area || "") === areaSel);
  }
  if (grupoSel !== "__ALL__") {
    const gSelN = normGroupName(grupoSel);
    universe = universe.filter((x) => normGroupName(x.grupo) === gSelN);
  }

  const abcRev = abcByMetric(universe, "revenue");
  const abcGP = abcByMetric(universe, "gp");
  const abcPct = abcByMetric(universe, "gpPct");

  let outItems = items.map((it) => {
    const a1 = abcRev.get(it.itemCode) || "D";
    const a2 = abcGP.get(it.itemCode) || "D";
    const a3 = abcPct.get(it.itemCode) || "D";
    const total = totalFromLetters(a1, a2, a3);

    return {
      ...it,
      abcRevenue: a1,
      abcGP: a2,
      abcGPPct: a3,
      totalLabel: total.label,
      totalTagClass: total.cls,
      R: total.r,
      G: total.g,
      "%": total.p,
      T: total.t,
    };
  });

  // filtros de vista (incluye q)
  if (areaSel !== "__ALL__") outItems = outItems.filter((x) => String(x.area || "") === areaSel);
  if (grupoSel !== "__ALL__") {
    const gSelN = normGroupName(grupoSel);
    outItems = outItems.filter((x) => normGroupName(x.grupo) === gSelN);
  }
  if (qq) {
    outItems = outItems.filter(
      (x) => x.itemCode.toLowerCase().includes(qq) || x.itemDesc.toLowerCase().includes(qq)
    );
  }

  // groupAgg / rank (sobre lo mostrado)
  const groupAggMap = new Map();
  for (const it of outItems) {
    const g = it.grupo || "Sin grupo";
    const cur = groupAggMap.get(g) || { grupo: g, revenue: 0, gp: 0 };
    cur.revenue += it.revenue;
    cur.gp += it.gp;
    groupAggMap.set(g, cur);
  }
  const groupAgg = Array.from(groupAggMap.values())
    .map((g) => ({ ...g, gpPct: g.revenue > 0 ? Number(((g.gp / g.revenue) * 100).toFixed(2)) : 0 }))
    .sort((a, b) => b.revenue - a.revenue);

  const groupRank = new Map();
  groupAgg.forEach((g, idx) => groupRank.set(g.grupo, idx + 1));
  outItems = outItems.map((x) => ({ ...x, rankArea: groupRank.get(x.grupo) || 9999 }));

  // ✅ orden solicitado: Revenue -> GM$ -> GM% -> TOTAL
  outItems.sort((a, b) => {
    const dr = (b.revenue || 0) - (a.revenue || 0);
    if (dr) return dr;

    const dg = (b.gp || 0) - (a.gp || 0);
    if (dg) return dg;

    const dp = (b.gpPct || 0) - (a.gpPct || 0);
    if (dp) return dp;

    const dt = (b.T || 0) - (a.T || 0);
    if (dt) return dt;

    const rankLabel = (lab) => (lab === "AB Crítico" ? 3 : lab === "C Importante" ? 2 : 1);
    return rankLabel(b.totalLabel) - rankLabel(a.totalLabel);
  });

  const totals = outItems.reduce(
    (acc, x) => {
      acc.revenue += Number(x.revenue || 0);
      acc.gp += Number(x.gp || 0);
      return acc;
    },
    { revenue: 0, gp: 0 }
  );
  const gpPctTotal = totals.revenue > 0 ? Number(((totals.gp / totals.revenue) * 100).toFixed(2)) : 0;

  return {
    ok: true,
    from,
    to,
    area: areaSel,
    grupo: grupoSel,
    q: qq,
    lastSyncAt: await getState("last_sync_at"),
    totals: { revenue: totals.revenue, gp: totals.gp, gpPct: gpPctTotal },
    availableGroups,
    groupAgg,
    items: outItems,
  };
}

/* =========================================================
   ✅ Item docs endpoint (modal)
========================================================= */
app.get("/api/admin/estratificacion/item-docs", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const itemCode = String(req.query?.itemCode || "").trim();
    if (!itemCode) return safeJson(res, 400, { ok: false, message: "Falta itemCode" });

    const fromQ = String(req.query?.from || "");
    const toQ = String(req.query?.to || "");
    const areaSel = String(req.query?.area || "__ALL__");
    const grupoSel = String(req.query?.grupo || "__ALL__");

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = isISO(fromQ) ? fromQ : "2024-01-01";
    const to = isISO(toQ) ? toQ : today;

    const q1 = await dbQuery(
      `
      SELECT
        s.doc_type,
        s.doc_date,
        s.doc_entry,
        s.doc_num,
        s.item_code,
        s.item_desc,
        s.quantity,
        s.revenue,
        s.gross_profit,
        COALESCE(NULLIF(s.area,''), NULLIF(g.area,''), '') AS area,
        COALESCE(NULLIF(s.item_group,''), NULLIF(g.grupo,''), NULLIF(g.group_name,''), 'Sin grupo') AS grupo
      FROM sales_item_lines s
      LEFT JOIN item_group_cache g ON g.item_code = s.item_code
      WHERE s.item_code = $1
        AND s.doc_date >= $2::date
        AND s.doc_date <= $3::date
      ORDER BY s.doc_date DESC, s.doc_entry DESC, s.line_num ASC
      LIMIT 500
      `,
      [itemCode, from, to]
    );

    let rows = (q1.rows || []).map((r) => {
      const grupoTxtRaw = String(r.grupo || "Sin grupo");
      const grupoTxt = normalizeGrupoFinal(grupoTxtRaw);

      const areaDb = String(r.area || "");
      const areaFinal = areaDb || inferAreaFromGroup(grupoTxt) || inferAreaFromGroup(grupoTxtRaw) || "CONS";

      return {
        docType: String(r.doc_type || ""),
        docDate: String(r.doc_date || "").slice(0, 10),
        docEntry: Number(r.doc_entry || 0),
        docNum: r.doc_num != null ? Number(r.doc_num) : null,
        itemCode: String(r.item_code || ""),
        itemDesc: String(r.item_desc || ""),
        quantity: Number(r.quantity || 0),
        total: Number(r.revenue || 0),
        gp: Number(r.gross_profit || 0),
        area: areaFinal,
        grupo: grupoTxt,
      };
    });

    if (areaSel !== "__ALL__") rows = rows.filter((x) => String(x.area || "") === areaSel);
    if (grupoSel !== "__ALL__") {
      const gSelN = normGroupName(grupoSel);
      rows = rows.filter((x) => normGroupName(x.grupo) === gSelN);
    }

    return safeJson(res, 200, { ok: true, itemCode, from, to, rows });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

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
   ✅ Dashboard endpoint
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
========================================================= */
app.get("/api/admin/estratificacion/sync", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const mode = String(req.query?.mode || "days").toLowerCase();

    const maxDocsRaw = Number(req.query?.maxDocs || 2500);
    const maxDocs = Math.max(50, Math.min(20000, Number.isFinite(maxDocsRaw) ? Math.trunc(maxDocsRaw) : 2500));

    const today = getDateISOInOffset(TZ_OFFSET_MIN);

    let from = "";
    let to = today;

    if (mode === "range") {
      const fromQ = String(req.query?.from || "");
      const toQ = String(req.query?.to || "");
      from = isISO(fromQ) ? fromQ : "2024-01-01";
      to = isISO(toQ) ? toQ : today;
    } else {
      const nRaw = Number(req.query?.n || 5);
      const n =
        mode === "days"
          ? Math.max(1, Math.min(120, Number.isFinite(nRaw) ? Math.trunc(nRaw) : 5))
          : Math.max(1, Math.min(30, Number.isFinite(nRaw) ? Math.trunc(nRaw) : 5));
      from = addDaysISO(today, -n);
      to = today;
    }

    // ventas netas
    const salesSaved = await syncSales({ from, to, maxDocs });

    // grupos + inventario para items del rango
    const groupsSaved = await syncItemGroupsForSalesItems({ from, to, maxItems: 2500 });
    const invSaved = await syncInventoryForSalesItems({ from, to, maxItems: 2500 });

    await setState("last_sync_at", new Date().toISOString());
    await setState("last_sync_from", from);
    await setState("last_sync_to", to);

    return safeJson(res, 200, {
      ok: true,
      mode,
      maxDocs,
      from,
      to,
      salesSaved,
      groupsSaved,
      invSaved,
      lastSyncAt: await getState("last_sync_at"),
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================================================
   ✅ DEBUG: counts
========================================================= */
app.get("/api/admin/estratificacion/debug-counts", verifyAdmin, async (req, res) => {
  try {
    const r1 = await dbQuery(`SELECT COUNT(*)::int AS c FROM sales_item_lines`);
    const r2 = await dbQuery(`SELECT COUNT(*)::int AS c FROM inv_item_cache`);
    const r3 = await dbQuery(`SELECT COUNT(*)::int AS c FROM item_group_cache`);
    return safeJson(res, 200, {
      ok: true,
      sales_item_lines: r1.rows?.[0]?.c || 0,
      inv_item_cache: r2.rows?.[0]?.c || 0,
      item_group_cache: r3.rows?.[0]?.c || 0,
      last_sync_at: await getState("last_sync_at"),
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
  app.listen(Number(PORT), () => console.log(`Server listening on :${PORT}
