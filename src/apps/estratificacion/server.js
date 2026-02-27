// src/apps/estratificacion/server.js
import express from "express";
import jwt from "jsonwebtoken";
import { createClient } from "@supabase/supabase-js";

/* =========================================================
   ✅ ENV
========================================================= */
const {
  PORT = 3000,
  JWT_SECRET = "change_me",
  ADMIN_USER = "PRODIMA",
  ADMIN_PASS = "ADMINISTRADOR",

  CORS_ORIGIN = "",

  // Supabase (Render env)
  SUPABASE_URL = "",
  SUPABASE_SERVICE_ROLE = "",

  // Tablas (por si cambias nombres en el futuro)
  SALES_LINES_TABLE = "sales_item_lines",
  ITEM_GROUP_CACHE_TABLE = "item_group_cache",

  // Columnas (sales_item_lines ya confirmadas)
  SALES_COL_CODE = "item_code",
  SALES_COL_GROUP = "item_group",
  SALES_COL_DATE = "doc_date",

  // Columnas (cache) => AJUSTABLE
  ITEM_GROUP_CACHE_CODE_COL = "item_code",
  ITEM_GROUP_CACHE_GROUP_COL = "group_name", // si tu cache usa item_group/grupo, cámbialo en Render
} = process.env;

/* =========================================================
   ✅ APP
========================================================= */
const app = express();
app.use(express.json({ limit: "2mb" }));

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
  res.setHeader(
    "Access-Control-Allow-Headers",
    "Content-Type, Authorization, x-warehouse"
  );
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
function norm(s) {
  return String(s || "").trim();
}
function uniqClean(arr) {
  return Array.from(new Set((arr || []).map(norm).filter(Boolean)));
}

/* =========================================================
   ✅ Supabase client (NO crashea si env mala)
========================================================= */
const rawUrl = String(SUPABASE_URL || "").trim();
const rawKey = String(SUPABASE_SERVICE_ROLE || "").trim();

let sb = null;
let sbInitError = "";

try {
  if (rawUrl && rawKey && /^https?:\/\//i.test(rawUrl)) {
    sb = createClient(rawUrl, rawKey, { auth: { persistSession: false } });
  } else {
    sbInitError =
      "Supabase env inválida. SUPABASE_URL debe ser https://xxxxx.supabase.co y SUPABASE_SERVICE_ROLE debe ser sb_secret_...";
  }
} catch (e) {
  sbInitError = String(e?.message || e);
}

function hasSupabase() {
  return !!sb;
}

/* =========================================================
   ✅ HEALTH / VERSION / ROUTES
========================================================= */
app.get("/api/health", (req, res) => {
  safeJson(res, 200, {
    ok: true,
    message: "✅ PRODIMA ESTRATIFICACION API activa",
    supabase: hasSupabase() ? "ok" : "missing",
    supabaseError: hasSupabase() ? "" : sbInitError,
    tables: {
      salesLines: SALES_LINES_TABLE,
      cache: ITEM_GROUP_CACHE_TABLE,
    },
    cacheCols: {
      code: ITEM_GROUP_CACHE_CODE_COL,
      group: ITEM_GROUP_CACHE_GROUP_COL,
    },
  });
});

app.get("/api/version", (req, res) => {
  safeJson(res, 200, {
    ok: true,
    commit: process.env.RENDER_GIT_COMMIT || "unknown",
    node: process.version,
  });
});

// útil para confirmar paths en Render
app.get("/__routes", (req, res) => {
  const routes = [];
  try {
    app._router.stack.forEach((m) => {
      if (m.route && m.route.path) {
        const methods = Object.keys(m.route.methods)
          .filter(Boolean)
          .map((x) => x.toUpperCase())
          .join(",");
        routes.push(`${methods} ${m.route.path}`);
      }
    });
  } catch {}
  safeJson(res, 200, { ok: true, routes });
});

/* =========================================================
   ✅ Admin login
========================================================= */
app.post("/api/admin/login", async (req, res) => {
  const user = norm(req.body?.user).toUpperCase();
  const pass = norm(req.body?.pass);

  if (user !== String(ADMIN_USER || "").toUpperCase() || pass !== String(ADMIN_PASS || "")) {
    return safeJson(res, 401, { ok: false, message: "Credenciales inválidas" });
  }

  const token = signToken({ role: "admin", user }, "12h");
  return safeJson(res, 200, { ok: true, token });
});

/* =========================================================
   ✅ Core: obtener grupo más reciente desde sales_item_lines
========================================================= */
async function getLatestGroupsFromSalesLines(itemCodes) {
  // itemCodes: array de strings ya limpios
  // retorna Map(code -> group)
  const best = new Map();
  if (!itemCodes.length) return best;

  const { data: rows, error } = await sb
    .from(SALES_LINES_TABLE)
    .select(`${SALES_COL_CODE},${SALES_COL_GROUP},${SALES_COL_DATE}`)
    .in(SALES_COL_CODE, itemCodes)
    .not(SALES_COL_GROUP, "is", null)
    .order(SALES_COL_DATE, { ascending: false });

  if (error) throw new Error(error.message);

  for (const r of rows || []) {
    const code = norm(r?.[SALES_COL_CODE]);
    const grp = norm(r?.[SALES_COL_GROUP]);
    if (!code || !grp) continue;
    if (!best.has(code)) best.set(code, grp); // primer match es el más reciente por el order desc
  }
  return best;
}

async function upsertIntoCache(bestMap) {
  const payload = Array.from(bestMap.entries()).map(([code, grp]) => ({
    [ITEM_GROUP_CACHE_CODE_COL]: code,
    [ITEM_GROUP_CACHE_GROUP_COL]: grp,
    updated_at: new Date().toISOString(),
  }));

  const { error } = await sb
    .from(ITEM_GROUP_CACHE_TABLE)
    .upsert(payload, { onConflict: ITEM_GROUP_CACHE_CODE_COL });

  if (error) throw new Error(error.message);

  return payload.length;
}

/* =========================================================
   ✅ 1) FORZAR LISTA: sales_item_lines -> item_group_cache
   POST /api/admin/item-groups/force
   Body: { itemCodes: [...] }
========================================================= */
async function forceHandler(req, res) {
  try {
    if (!hasSupabase()) {
      return safeJson(res, 500, { ok: false, message: "Faltan variables Supabase", detail: sbInitError });
    }

    const clean = uniqClean(req.body?.itemCodes);

    if (!clean.length) {
      return safeJson(res, 400, { ok: false, message: "Envía itemCodes: [] con al menos 1 código." });
    }

    console.log("FORCE GROUPS: reading from sales_item_lines", clean.length);

    const best = await getLatestGroupsFromSalesLines(clean);
    if (best.size === 0) {
      return safeJson(res, 200, {
        ok: true,
        requested: clean.length,
        updated: 0,
        message: "No encontré item_group en sales_item_lines para esos códigos.",
      });
    }

    const updated = await upsertIntoCache(best);

    const missing = clean.filter((c) => !best.has(c));
    return safeJson(res, 200, {
      ok: true,
      requested: clean.length,
      updated,
      missingInSalesLinesCount: missing.length,
      missingInSalesLines: missing.slice(0, 200),
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
}

app.post("/api/admin/item-groups/force", verifyAdmin, forceHandler);

// ✅ alias por si sigues usando el prefijo /estratificacion
app.post("/api/admin/estratificacion/item-groups/force", verifyAdmin, forceHandler);

/* =========================================================
   ✅ 2) BACKFILL MASIVO (sin lista)
   POST /api/admin/item-groups/backfill-missing?limit=5000
   - toma item_codes vendidos, saca su item_group más reciente,
     y los mete a cache (solo lo que falte)
========================================================= */
async function backfillHandler(req, res) {
  try {
    if (!hasSupabase()) {
      return safeJson(res, 500, { ok: false, message: "Faltan variables Supabase", detail: sbInitError });
    }

    const limitRaw = Number(req.query?.limit ?? 5000);
    const limit = Math.max(100, Math.min(50000, Number.isFinite(limitRaw) ? Math.trunc(limitRaw) : 5000));

    // 1) obtener codes desde sales_item_lines (distintos)
    // Supabase no tiene "distinct on" directo en JS client,
    // así que tomamos filas recientes y construimos set.
    const { data: rows, error } = await sb
      .from(SALES_LINES_TABLE)
      .select(`${SALES_COL_CODE},${SALES_COL_GROUP},${SALES_COL_DATE}`)
      .not(SALES_COL_GROUP, "is", null)
      .order(SALES_COL_DATE, { ascending: false })
      .limit(limit);

    if (error) throw new Error(error.message);

    const best = new Map();
    for (const r of rows || []) {
      const code = norm(r?.[SALES_COL_CODE]);
      const grp = norm(r?.[SALES_COL_GROUP]);
      if (!code || !grp) continue;
      if (!best.has(code)) best.set(code, grp);
    }

    if (best.size === 0) {
      return safeJson(res, 200, { ok: true, scanned: (rows || []).length, updated: 0, message: "No data found." });
    }

    // 2) filtrar los que YA existen en cache
    const codes = Array.from(best.keys());

    const { data: existing, error: e2 } = await sb
      .from(ITEM_GROUP_CACHE_TABLE)
      .select(ITEM_GROUP_CACHE_CODE_COL)
      .in(ITEM_GROUP_CACHE_CODE_COL, codes);

    if (e2) throw new Error(e2.message);

    const existsSet = new Set((existing || []).map((x) => norm(x?.[ITEM_GROUP_CACHE_CODE_COL])));
    const missing = codes.filter((c) => !existsSet.has(norm(c)));

    const bestMissing = new Map();
    for (const c of missing) bestMissing.set(c, best.get(c));

    const updated = bestMissing.size ? await upsertIntoCache(bestMissing) : 0;

    return safeJson(res, 200, {
      ok: true,
      scannedRows: (rows || []).length,
      uniqueCodesFound: best.size,
      alreadyInCache: existsSet.size,
      insertedNow: updated,
      note: "Esto llena el cache solo con lo vendido (sales_item_lines).",
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
}

app.post("/api/admin/item-groups/backfill-missing", verifyAdmin, backfillHandler);
app.post("/api/admin/estratificacion/item-groups/backfill-missing", verifyAdmin, backfillHandler);

/* =========================================================
   ✅ START
========================================================= */
process.on("unhandledRejection", (e) => console.error("unhandledRejection:", e));
process.on("uncaughtException", (e) => console.error("uncaughtException:", e));

app.listen(Number(PORT), () => console.log(`Estratificacion server listening on :${PORT}`));
