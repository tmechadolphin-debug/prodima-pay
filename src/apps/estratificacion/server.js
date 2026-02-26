// server.js (estratificación)
// DB-first: guarda ventas (INV/CRN) + item_master + inventario + grupos en Supabase.

import express from "express";
import pg from "pg";
import jwt from "jsonwebtoken";

const { Pool } = pg;

const app = express();
app.use(express.json({ limit: "6mb" }));

/* =========================
   ENV
========================= */
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

/* =========================
   CORS ROBUSTO
========================= */
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
  // 👇 IMPORTANTE: permitir Authorization siempre
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.setHeader("Access-Control-Max-Age", "86400");

  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

/* =========================
   Helpers
========================= */
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

/* =========================
   Postgres (Supabase)
========================= */
const pool = new Pool({
  connectionString: DATABASE_URL || undefined,
  ssl: DATABASE_URL ? { rejectUnauthorized: false } : undefined,
  max: 3,
});

function hasDb() {
  return Boolean(DATABASE_URL);
}
async function dbQuery(text, params = []) {
  return pool.query(text, params);
}

async function ensureDb() {
  if (!hasDb()) return;

  // ✅ Ventas por línea (INV + CRN)
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS sales_item_lines (
      doc_entry   INTEGER NOT NULL,
      line_num    INTEGER NOT NULL,
      doc_type    TEXT    NOT NULL,         -- 'INV' | 'CRN'
      doc_num     INTEGER NOT NULL,
      doc_date    DATE    NOT NULL,
      card_code   TEXT    NOT NULL DEFAULT '',
      card_name   TEXT    NOT NULL DEFAULT '',
      item_code   TEXT    NOT NULL DEFAULT '',
      item_desc   TEXT    NOT NULL DEFAULT '',
      quantity    NUMERIC(18,4) NOT NULL DEFAULT 0,
      revenue     NUMERIC(18,2) NOT NULL DEFAULT 0,
      gross_profit NUMERIC(18,2) NOT NULL DEFAULT 0,
      updated_at  TIMESTAMP DEFAULT NOW(),
      PRIMARY KEY (doc_entry, doc_type, line_num)
    );
  `);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_sales_date ON sales_item_lines(doc_date);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_sales_item ON sales_item_lines(item_code);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_sales_card ON sales_item_lines(card_code);`);

  // ✅ Maestro de artículos (mín/max desde SAP si existen)
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS item_master (
      item_code TEXT PRIMARY KEY,
      item_desc TEXT NOT NULL DEFAULT '',
      items_group_code INTEGER,
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  // ✅ Inventario cache
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS inv_item_cache (
      item_code TEXT PRIMARY KEY,
      item_desc TEXT NOT NULL DEFAULT '',
      min_stock NUMERIC(18,4) NOT NULL DEFAULT 0,
      max_stock NUMERIC(18,4) NOT NULL DEFAULT 0,
      on_hand   NUMERIC(18,4) NOT NULL DEFAULT 0,   -- existencia total (sum bodegas)
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  // ✅ Grupo + Área (para filtros)
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS item_group_cache (
      item_code TEXT PRIMARY KEY,
      item_desc TEXT NOT NULL DEFAULT '',
      area      TEXT NOT NULL DEFAULT 'Cons',  -- 'Cons'|'RCI' (o 'Todas' en UI)
      grupo     TEXT NOT NULL DEFAULT '',
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  // ✅ Estado sync
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

/* =========================
   fetch wrapper
========================= */
let _fetch = globalThis.fetch || null;
async function httpFetch(url, options) {
  if (_fetch) return _fetch(url, options);
  const mod = await import("node-fetch");
  _fetch = mod.default;
  return _fetch(url, options);
}

/* =========================
   SAP Service Layer (cookie + timeout)
========================= */
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

/* =========================
   SAP helpers: headers + documents
========================= */
function pickGrossProfit(ln) {
  const candidates = [ln?.GrossProfit, ln?.GrossProfitTotal, ln?.GrossProfitFC, ln?.GrossProfitSC];
  for (const c of candidates) {
    const n = Number(c);
    if (Number.isFinite(n)) return n;
  }
  return 0;
}

async function scanDocHeaders(entity, { from, to, maxDocs = 2000 }) {
  const toPlus1 = addDaysISO(to, 1);
  const batchTop = 200;
  let skipSap = 0;
  const out = [];

  for (let page = 0; page < 400; page++) {
    const raw = await slFetch(
      `/${entity}?$select=DocEntry,DocNum,DocDate,CardCode,CardName` +
        `&$filter=${encodeURIComponent(`DocDate ge '${from}' and DocDate lt '${toPlus1}'`)}` +
        `&$orderby=DocDate asc,DocEntry asc&$top=${batchTop}&$skip=${skipSap}`,
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
        CardCode: String(r.CardCode || ""),
        CardName: String(r.CardName || ""),
      });
      if (out.length >= maxDocs) return out;
    }
  }
  return out;
}

async function getDoc(entity, docEntry) {
  const de = Number(docEntry);
  if (!Number.isFinite(de) || de <= 0) return null;
  return slFetch(`/${entity}(${de})`, { timeoutMs: 90000 });
}

/* =========================
   DB upserts
========================= */
async function upsertSalesLines(docType, header, fullDoc) {
  const lines = Array.isArray(fullDoc?.DocumentLines) ? fullDoc.DocumentLines : [];
  if (!lines.length) return 0;

  const docEntry = Number(header.DocEntry);
  const docNum = Number(header.DocNum);
  const docDate = String(header.DocDate || "").slice(0, 10);
  const cardCode = String(header.CardCode || "");
  const cardName = String(header.CardName || "");

  const values = [];
  const params = [];
  let p = 1;

  for (const ln of lines) {
    const lineNum = Number(ln.LineNum);
    if (!Number.isFinite(lineNum)) continue;

    const itemCode = String(ln.ItemCode || "").trim();
    const itemDesc = String(ln.ItemDescription || ln.ItemName || "").trim();
    const qty = Number(ln.Quantity || 0);
    const revenue = Number(ln.LineTotal ?? ln.RowTotal ?? 0);
    const gp = pickGrossProfit(ln);

    params.push(docEntry, lineNum, docType, docNum, docDate, cardCode, cardName, itemCode, itemDesc, qty, revenue, gp);
    values.push(
      `($${p++},$${p++},$${p++},$${p++},$${p++}::date,$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`
    );
  }

  if (!values.length) return 0;

  await dbQuery(
    `
    INSERT INTO sales_item_lines
      (doc_entry,line_num,doc_type,doc_num,doc_date,card_code,card_name,item_code,item_desc,quantity,revenue,gross_profit)
    VALUES ${values.join(",")}
    ON CONFLICT (doc_entry, doc_type, line_num)
    DO UPDATE SET
      doc_num=EXCLUDED.doc_num,
      doc_date=EXCLUDED.doc_date,
      card_code=EXCLUDED.card_code,
      card_name=EXCLUDED.card_name,
      item_code=EXCLUDED.item_code,
      item_desc=EXCLUDED.item_desc,
      quantity=EXCLUDED.quantity,
      revenue=EXCLUDED.revenue,
      gross_profit=EXCLUDED.gross_profit,
      updated_at=NOW()
    `,
    params
  );

  return values.length;
}

async function upsertItemMaster(itemCode, itemDesc, itemsGroupCode = null) {
  await dbQuery(
    `
    INSERT INTO item_master(item_code,item_desc,items_group_code,updated_at)
    VALUES($1,$2,$3,NOW())
    ON CONFLICT (item_code)
    DO UPDATE SET
      item_desc=EXCLUDED.item_desc,
      items_group_code=COALESCE(EXCLUDED.items_group_code, item_master.items_group_code),
      updated_at=NOW()
    `,
    [String(itemCode || ""), String(itemDesc || ""), itemsGroupCode != null ? Number(itemsGroupCode) : null]
  );
}

async function upsertInvCache(itemCode, itemDesc, minStock, maxStock, onHand) {
  await dbQuery(
    `
    INSERT INTO inv_item_cache(item_code,item_desc,min_stock,max_stock,on_hand,updated_at)
    VALUES($1,$2,$3,$4,$5,NOW())
    ON CONFLICT (item_code)
    DO UPDATE SET
      item_desc=EXCLUDED.item_desc,
      min_stock=EXCLUDED.min_stock,
      max_stock=EXCLUDED.max_stock,
      on_hand=EXCLUDED.on_hand,
      updated_at=NOW()
    `,
    [
      String(itemCode || ""),
      String(itemDesc || ""),
      Number(minStock || 0),
      Number(maxStock || 0),
      Number(onHand || 0),
    ]
  );
}

async function upsertGroupCache(itemCode, itemDesc, area, grupo) {
  await dbQuery(
    `
    INSERT INTO item_group_cache(item_code,item_desc,area,grupo,updated_at)
    VALUES($1,$2,$3,$4,NOW())
    ON CONFLICT (item_code)
    DO UPDATE SET
      item_desc=EXCLUDED.item_desc,
      area=EXCLUDED.area,
      grupo=EXCLUDED.grupo,
      updated_at=NOW()
    `,
    [String(itemCode || ""), String(itemDesc || ""), String(area || "Cons"), String(grupo || "")]
  );
}

/* =========================
   Item master + inv + groups (from SAP Items)
========================= */

// Mapeo simple a “Área” según nombre (ajústalo si quieres)
function inferAreaFromGroupName(groupName) {
  const s = String(groupName || "").toLowerCase();
  if (s.includes("rci") || s.includes("res.") || s.includes("ind")) return "RCI";
  return "Cons";
}

// Mapeo a grupos “bonitos” (si viene otro, lo dejamos tal cual)
function normalizeGrupo(groupName) {
  const g = String(groupName || "").trim();
  if (!g) return "";
  return g;
}

async function fetchItemFromSAP(itemCode) {
  const code = String(itemCode || "").trim();
  if (!code) return null;

  const safe = code.replace(/'/g, "''");

  // Trae info general + warehouses
  // OJO: ItemWarehouseInfoCollection suele venir bien sin expand adicional.
  const it = await slFetch(
    `/Items('${safe}')?$select=ItemCode,ItemName,ItemsGroupCode,MinInventory,MaxInventory,ItemWarehouseInfoCollection`,
    { timeoutMs: 60000 }
  );

  return it || null;
}

async function fetchGroupName(itemsGroupCode) {
  const n = Number(itemsGroupCode);
  if (!Number.isFinite(n) || n <= 0) return "";
  try {
    const g = await slFetch(`/ItemGroups(${n})?$select=GroupName`, { timeoutMs: 30000 });
    return String(g?.GroupName || "").trim();
  } catch {
    return "";
  }
}

function sumOnHandFromItem(it) {
  const coll = Array.isArray(it?.ItemWarehouseInfoCollection) ? it.ItemWarehouseInfoCollection : [];
  let onHand = 0;
  for (const w of coll) {
    const s = Number(w?.InStock ?? w?.OnHand ?? 0);
    if (Number.isFinite(s)) onHand += s;
  }
  return Number(onHand.toFixed(4));
}

async function ensureItemMasterAndInventoryForItemCodes(itemCodes) {
  const uniq = Array.from(new Set((itemCodes || []).map((x) => String(x || "").trim()).filter(Boolean)));

  let okItems = 0;
  let okInv = 0;
  let okGroups = 0;

  // Concurrency baja para no matar SL
  const CONC = 3;
  let idx = 0;

  async function worker() {
    while (idx < uniq.length) {
      const i = idx++;
      const code = uniq[i];
      try {
        const it = await fetchItemFromSAP(code);
        if (!it) continue;

        const itemDesc = String(it?.ItemName || "").trim();
        const igc = it?.ItemsGroupCode != null ? Number(it.ItemsGroupCode) : null;

        await upsertItemMaster(code, itemDesc, igc);
        okItems++;

        const minStock = Number(it?.MinInventory || 0);
        const maxStock = Number(it?.MaxInventory || 0);
        const onHand = sumOnHandFromItem(it);

        await upsertInvCache(code, itemDesc, minStock, maxStock, onHand);
        okInv++;

        const groupName = await fetchGroupName(igc);
        const area = inferAreaFromGroupName(groupName);
        const grupo = normalizeGrupo(groupName);

        await upsertGroupCache(code, itemDesc, area, grupo);
        okGroups++;
      } catch {
        // no rompemos el sync por un item malo
      }
      await sleep(20);
    }
  }

  await Promise.all(Array.from({ length: CONC }, () => worker()));
  return { okItems, okInv, okGroups, total: uniq.length };
}

/* =========================
   Sync (last N days) => DB
========================= */
async function syncSalesRange({ from, to, maxDocs = 2000 }) {
  if (!hasDb()) throw new Error("DB no configurada (DATABASE_URL)");
  if (missingSapEnv()) throw new Error("Faltan variables SAP");

  // 1) headers INV + CRN
  const invHeaders = await scanDocHeaders("Invoices", { from, to, maxDocs });
  const crnHeaders = await scanDocHeaders("CreditNotes", { from, to, maxDocs });

  let lines = 0;
  let docs = 0;

  // Concurrency baja para estabilidad
  const CONC = 1;

  async function processHeaders(entity, docType, headersArr) {
    let idx = 0;
    async function worker() {
      while (idx < headersArr.length) {
        const h = headersArr[idx++];
        try {
          const full = await getDoc(entity, h.DocEntry);
          const inserted = await upsertSalesLines(docType, h, full);
          lines += inserted;
          docs += 1;
        } catch {
          // skip doc
        }
        await sleep(25);
      }
    }
    await Promise.all(Array.from({ length: CONC }, () => worker()));
  }

  await processHeaders("Invoices", "INV", invHeaders);
  await processHeaders("CreditNotes", "CRN", crnHeaders);

  return { docs, lines, invDocs: invHeaders.length, crnDocs: crnHeaders.length };
}

/* =========================
   Dashboard (DB)
========================= */
async function dashboardFromDb({ from, to, area = "__ALL__", grupo = "__ALL__", q = "" }) {
  // filtramos por joins contra caches para área/grupo
  const params = [from, to];
  let p = 3;

  let where = `s.doc_date >= $1::date AND s.doc_date <= $2::date AND s.item_code <> ''`;

  // búsqueda por código/desc
  if (q) {
    params.push(`%${q.toLowerCase()}%`);
    where += ` AND (LOWER(s.item_code) LIKE $${p} OR LOWER(s.item_desc) LIKE $${p})`;
    p++;
  }

  // join a item_group_cache para filtrar área/grupo
  let join = `LEFT JOIN item_group_cache g ON g.item_code = s.item_code
              LEFT JOIN inv_item_cache i ON i.item_code = s.item_code`;

  if (area && area !== "__ALL__") {
    params.push(area);
    where += ` AND COALESCE(g.area,'Cons') = $${p++}`;
  }
  if (grupo && grupo !== "__ALL__") {
    params.push(grupo);
    where += ` AND COALESCE(g.grupo,'') = $${p++}`;
  }

  const rowsQ = await dbQuery(
    `
    SELECT
      s.item_code AS item_code,
      COALESCE(NULLIF(s.item_desc,''), COALESCE(m.item_desc,'')) AS item_desc,

      COALESCE(SUM(s.revenue),0)::numeric(18,2) AS revenue,
      COALESCE(SUM(s.gross_profit),0)::numeric(18,2) AS gross_margin,

      COALESCE(g.area,'Cons') AS area,
      COALESCE(g.grupo,'') AS grupo,

      COALESCE(i.min_stock,0)::numeric(18,4) AS min_stock,
      COALESCE(i.max_stock,0)::numeric(18,4) AS max_stock,
      COALESCE(i.on_hand,0)::numeric(18,4)   AS stock

    FROM sales_item_lines s
    ${join}
    LEFT JOIN item_master m ON m.item_code = s.item_code
    WHERE ${where}
    GROUP BY 1,2,5,6,7,8,9
    ORDER BY revenue DESC
    LIMIT 5000
    `,
    params
  );

  const items = (rowsQ.rows || []).map((r) => {
    const rev = Number(r.revenue || 0);
    const gm = Number(r.gross_margin || 0);
    const gmp = rev !== 0 ? (gm / rev) * 100 : 0;

    // TOTAL = promedio (normalizado) de revenue/gm/gmp: aquí lo dejamos simple como promedio de rangos relativos no (lo hará el front)
    return {
      itemCode: r.item_code,
      itemDesc: r.item_desc,
      revenue: rev,
      grossMargin: gm,
      grossMarginPct: Number(gmp.toFixed(4)),
      area: r.area,
      grupo: r.grupo,
      min: Number(r.min_stock || 0),
      max: Number(r.max_stock || 0),
      stock: Number(r.stock || 0),
    };
  });

  // resumen por grupo
  const groupAgg = new Map();
  for (const it of items) {
    const key = it.grupo || "Sin grupo";
    const prev = groupAgg.get(key) || { grupo: key, revenue: 0, grossMargin: 0 };
    prev.revenue += it.revenue;
    prev.grossMargin += it.grossMargin;
    groupAgg.set(key, prev);
  }

  const byGroup = Array.from(groupAgg.values())
    .map((x) => ({
      grupo: x.grupo,
      revenue: Number(x.revenue.toFixed(2)),
      grossMargin: Number(x.grossMargin.toFixed(2)),
      grossMarginPct: x.revenue !== 0 ? Number(((x.grossMargin / x.revenue) * 100).toFixed(4)) : 0,
    }))
    .sort((a, b) => b.revenue - a.revenue);

  return { ok: true, from, to, items, byGroup };
}

/* =========================
   Routes
========================= */
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

// ✅ Sync (mode=days, n=5|1) => llena ventas + hidrata master/inv/grupos
app.get("/api/admin/estratificacion/sync", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });
    if (missingSapEnv()) return safeJson(res, 400, { ok: false, message: "Faltan variables SAP" });

    const mode = String(req.query?.mode || "days").toLowerCase();
    const nRaw = Number(req.query?.n || 5);
    const n = Math.max(1, Math.min(120, Number.isFinite(nRaw) ? Math.trunc(nRaw) : 5));

    // rango
    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const from = mode === "days" ? addDaysISO(today, -n) : addDaysISO(today, -5);
    const to = today;

    await setState("last_sync_mode", mode);
    await setState("last_sync_n", String(n));
    await setState("last_sync_from", from);
    await setState("last_sync_to", to);

    // 1) sales lines
    const r1 = await syncSalesRange({ from, to, maxDocs: 2000 });

    // 2) tomar item codes recientes desde DB (solo los que aparecieron en el rango)
    const itemQ = await dbQuery(
      `
      SELECT DISTINCT item_code
      FROM sales_item_lines
      WHERE doc_date >= $1::date AND doc_date <= $2::date
        AND item_code <> ''
      `,
      [from, to]
    );
    const itemCodes = (itemQ.rows || []).map((x) => x.item_code).filter(Boolean);

    // 3) hidratar item_master + inv + group para esos itemCodes
    const r2 = await ensureItemMasterAndInventoryForItemCodes(itemCodes);

    const stamp = new Date().toISOString();
    await setState("last_sync_at", stamp);

    return safeJson(res, 200, {
      ok: true,
      from,
      to,
      synced: {
        sales_docs: r1.docs,
        sales_lines: r1.lines,
        invDocs: r1.invDocs,
        crnDocs: r1.crnDocs,
      },
      hydrated: {
        items_seen: r2.total,
        item_master_upserts: r2.okItems,
        inv_cache_upserts: r2.okInv,
        group_cache_upserts: r2.okGroups,
      },
      last_sync_at: stamp,
    });
  } catch (e) {
    await setState("last_sync_error", e.message || String(e));
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

// ✅ Dashboard DB
app.get("/api/admin/estratificacion/dashboard", verifyAdmin, async (req, res) => {
  try {
    if (!hasDb()) return safeJson(res, 500, { ok: false, message: "DB no configurada (DATABASE_URL)" });

    const fromQ = String(req.query?.from || "");
    const toQ = String(req.query?.to || "");
    const area = String(req.query?.area || "__ALL__");   // 'Cons'|'RCI'|'__ALL__'
    const grupo = String(req.query?.grupo || "__ALL__"); // string o '__ALL__'
    const q = String(req.query?.q || "").trim();

    const today = getDateISOInOffset(TZ_OFFSET_MIN);
    const defaultFrom = "2024-01-01";

    const from = isISO(fromQ) ? fromQ : defaultFrom;
    const to = isISO(toQ) ? toQ : today;

    const data = await dashboardFromDb({ from, to, area, grupo, q });
    return safeJson(res, 200, data);
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

// ✅ Debug counts (para validar inserciones)
app.get("/api/admin/estratificacion/debug-counts", verifyAdmin, async (req, res) => {
  try {
    const tables = ["sales_item_lines", "item_master", "inv_item_cache", "item_group_cache"];
    const out = {};
    for (const t of tables) {
      const r = await dbQuery(`SELECT COUNT(*)::int AS c FROM ${t}`);
      out[t] = Number(r.rows?.[0]?.c || 0);
    }
    out.last_sync_at = await getState("last_sync_at");
    out.last_sync_error = await getState("last_sync_error");
    return safeJson(res, 200, { ok: true, counts: out });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
});

/* =========================
   START
========================= */
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
