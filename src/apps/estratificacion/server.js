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
   ✅ NORMALIZACIÓN + CANONICALIZACIÓN + HEURÍSTICAS
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

// ✅ NUEVO: mapeo por "patrones" para variantes raras del SAP
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

  // Limpieza (hogar / artículos) - preferimos Art. si dice ESPONJA/PAÑO/ART
  if (n.includes("LIMPIEZ") || n.includes("LIMPIEZA") || n.includes("CLEAN")) {
    if (n.includes("ART") || n.includes("ESPON") || n.includes("PANO") || n.includes("PAÑO")) return "Art. De Limpieza";
    return "Prod. De Limpieza";
  }

  if (n.includes("ESPECIAL") || n.includes("GMT")) return "Especialidades y GMT";

  return "";
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

/* =========================================================
   ✅ Sync: Grupos por Item (ItemGroups)
   ✅ FIX: aplica heurística para no caer en "Sin grupo"
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

      // ✅ primero intenta canonical exacto, luego heurística
      let grupo = canonicalGroupName(groupNameRaw || "");
      if (!grupo || normGroupName(grupo) === "SIN GRUPO") {
        const guessed = guessCanonicalGroupName(groupNameRaw);
        if (guessed) grupo = guessed;
      }

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
        [code, groupNameRaw, area, grupo || "Sin grupo", itemDesc]
      );

      saved++;
    } catch {}
    if ((i + 1) % 25 === 0) await sleep(15);
  }

  return saved;
}

/* =========================================================
   ✅ Dashboard: aplica heurística también en lectura
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
      s.gp AS gp
    FROM s
    LEFT JOIN item_group_cache g ON g.item_code = s.item_code
    ORDER BY s.revenue DESC
    `,
    [from, to]
  );

  let items = (salesAgg.rows || []).map((r) => {
    const rev = Number(r.revenue || 0);
    const gp = Number(r.gp || 0);
    const pct = rev > 0 ? (gp / rev) * 100 : 0;

    const grupoTxtRaw = String(r.grupo || "Sin grupo");
    let grupoTxt = canonicalGroupName(grupoTxtRaw);
    if (!grupoTxt || normGroupName(grupoTxt) === "SIN GRUPO") {
      const guessed = guessCanonicalGroupName(grupoTxtRaw);
      if (guessed) grupoTxt = guessed;
      else grupoTxt = grupoTxtRaw;
    }

    const areaDb = String(r.area || "");
    const areaFinal = areaDb || inferAreaFromGroup(grupoTxt) || inferAreaFromGroup(grupoTxtRaw) || "CONS";

    return {
      itemCode: String(r.item_code || ""),
      itemDesc: String(r.item_desc || ""),
      area: areaFinal,
      grupo: grupoTxt || "Sin grupo",
      revenue: rev,
      gp: gp,
      gpPct: Number(pct.toFixed(2)),
    };
  });

  // ... (el resto de tu lógica dashboard/ABC/orden queda igual que ya lo tienes)
  // ⚠️ Aquí solo te mostré el fix de grupo; pega tu bloque actual completo después de esta construcción.
  return { ok: true, items };
}

/* =========================================================
   ⚠️ NOTA IMPORTANTE
   Este archivo es largo y tu server.js actual tiene muchas más secciones:
   - sync de ventas, inventario, endpoints, ABC, etc.
   Para no romper nada, solo debes copiar estas funciones NUEVAS:
   - guessCanonicalGroupName()
   y aplicar sus usos en:
   - syncItemGroupsForSalesItems()
   - dashboardFromDb() donde arma grupoTxt
========================================================= */
