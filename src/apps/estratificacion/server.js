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

  // Supabase (Render)
  SUPABASE_URL = "",
  SUPABASE_SERVICE_ROLE = "",

  // Tablas
  SALES_LINES_TABLE = "sales_item_lines",
  CACHE_TABLE = "item_group_cache",
} = process.env;

/* =========================================================
   ✅ APP
========================================================= */
const app = express();
app.use(express.json({ limit: "3mb" }));

/* =========================================================
   ✅ CORS
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
function norm(x) {
  return String(x || "").trim();
}
function uniqClean(arr) {
  return Array.from(new Set((arr || []).map(norm).filter(Boolean)));
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

/* =========================================================
   ✅ Supabase client (no crashea)
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
    tables: { salesLines: SALES_LINES_TABLE, cache: CACHE_TABLE },
  });
});

app.get("/api/version", (req, res) => {
  safeJson(res, 200, {
    ok: true,
    commit: process.env.RENDER_GIT_COMMIT || "unknown",
    node: process.version,
  });
});

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
   ✅ ADMIN LOGIN
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
   ✅ 1) LISTAR "SIN GRUPO" DESDE CACHE
   GET /api/admin/item-groups/missing?limit=200
========================================================= */
async function listMissingHandler(req, res) {
  try {
    if (!hasSupabase()) return safeJson(res, 500, { ok: false, message: "Faltan variables Supabase", detail: sbInitError });

    const limitRaw = Number(req.query?.limit ?? 200);
    const limit = Math.max(10, Math.min(5000, Number.isFinite(limitRaw) ? Math.trunc(limitRaw) : 200));

    const { data, error } = await sb
      .from(CACHE_TABLE)
      .select("item_code,item_desc,area,group_name,grupo,updated_at")
      .or("group_name.eq.Sin grupo,grupo.eq.Sin grupo,group_name.is.null,grupo.is.null")
      .order("updated_at", { ascending: false })
      .limit(limit);

    if (error) throw new Error(error.message);

    return safeJson(res, 200, { ok: true, count: (data || []).length, rows: data || [] });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
}

app.get("/api/admin/item-groups/missing", verifyAdmin, listMissingHandler);
app.get("/api/admin/estratificacion/item-groups/missing", verifyAdmin, listMissingHandler);

/* =========================================================
   ✅ 2) CREAR FALTANTES EN CACHE DESDE VENTAS (sales_item_lines)
   - Inserta registros en item_group_cache con group_name/grupo="Sin grupo"
   POST /api/admin/item-groups/create-missing
   Body opcional:
     { itemCodes:[...], limitSalesRows: 5000 }
========================================================= */
async function createMissingHandler(req, res) {
  try {
    if (!hasSupabase()) return safeJson(res, 500, { ok: false, message: "Faltan variables Supabase", detail: sbInitError });

    const itemCodes = Array.isArray(req.body?.itemCodes) ? req.body.itemCodes : [];
    const clean = uniqClean(itemCodes);

    const limitSalesRowsRaw = Number(req.body?.limitSalesRows ?? 5000);
    const limitSalesRows = Math.max(100, Math.min(50000, Number.isFinite(limitSalesRowsRaw) ? Math.trunc(limitSalesRowsRaw) : 5000));

    // 1) candidatos: lista o últimos vendidos
    let candidates = clean;
    let salesRows = [];

    if (!candidates.length) {
      const { data, error } = await sb
        .from(SALES_LINES_TABLE)
        .select("item_code,item_desc,area,doc_date")
        .order("doc_date", { ascending: false })
        .limit(limitSalesRows);

      if (error) throw new Error(error.message);
      salesRows = data || [];
      candidates = Array.from(new Set(salesRows.map(r => norm(r.item_code)).filter(Boolean)));
    }

    if (!candidates.length) {
      return safeJson(res, 200, { ok: true, inserted: 0, message: "No hay candidatos." });
    }

    // 2) cuáles ya existen en cache
    const { data: existing, error: e2 } = await sb
      .from(CACHE_TABLE)
      .select("item_code")
      .in("item_code", candidates);

    if (e2) throw new Error(e2.message);

    const existsSet = new Set((existing || []).map(x => norm(x.item_code)));
    const missing = candidates.filter(c => !existsSet.has(norm(c)));

    if (!missing.length) {
      return safeJson(res, 200, {
        ok: true,
        requested: candidates.length,
        existed: existsSet.size,
        inserted: 0,
        message: "No hay faltantes. Ya todos existen en item_group_cache.",
      });
    }

    // 3) buscar info de esos missing en ventas (último doc_date por item)
    if (!salesRows.length) {
      const { data, error } = await sb
        .from(SALES_LINES_TABLE)
        .select("item_code,item_desc,area,doc_date")
        .in("item_code", missing)
        .order("doc_date", { ascending: false });

      if (error) throw new Error(error.message);
      salesRows = data || [];
    }

    const latest = new Map();
    for (const r of salesRows) {
      const code = norm(r.item_code);
      if (!code) continue;
      if (!latest.has(code)) latest.set(code, r); // viene ordenado desc
    }

    const now = new Date().toISOString();
    const payload = missing.map(code => {
      const r = latest.get(code) || {};
      return {
        item_code: code,
        item_desc: r.item_desc ? String(r.item_desc) : null,
        area: (r.area && String(r.area).trim()) ? String(r.area).trim() : "EMPTY",
        group_name: "Sin grupo",
        grupo: "Sin grupo",
        updated_at: now,
      };
    });

    const { error: e3 } = await sb
      .from(CACHE_TABLE)
      .upsert(payload, { onConflict: "item_code" });

    if (e3) throw new Error(e3.message);

    return safeJson(res, 200, {
      ok: true,
      requested: candidates.length,
      existed: existsSet.size,
      inserted: payload.length,
      sampleInserted: payload.slice(0, 20).map(x => x.item_code),
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
}

app.post("/api/admin/item-groups/create-missing", verifyAdmin, createMissingHandler);
app.post("/api/admin/estratificacion/item-groups/create-missing", verifyAdmin, createMissingHandler);

/* =========================================================
   ✅ 3) ASIGNAR GRUPO MANUAL
   POST /api/admin/item-groups/set
   Body: { itemCodes:[...], group:"EQUIP. Y ACCES. AGUA", area:"RCI"(opcional) }
========================================================= */
async function setGroupHandler(req, res) {
  try {
    if (!hasSupabase()) return safeJson(res, 500, { ok: false, message: "Faltan variables Supabase", detail: sbInitError });

    const codes = uniqClean(req.body?.itemCodes);
    const group = norm(req.body?.group);
    const area = req.body?.area != null ? norm(req.body.area) : null;

    if (!codes.length) return safeJson(res, 400, { ok: false, message: "Envía itemCodes." });
    if (!group) return safeJson(res, 400, { ok: false, message: "Envía group." });

    const now = new Date().toISOString();
    const payload = codes.map(code => ({
      item_code: code,
      group_name: group,
      grupo: group,
      ...(area ? { area } : {}),
      updated_at: now,
    }));

    const { error } = await sb
      .from(CACHE_TABLE)
      .upsert(payload, { onConflict: "item_code" });

    if (error) throw new Error(error.message);

    return safeJson(res, 200, { ok: true, updated: payload.length, group });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: e.message || String(e) });
  }
}

app.post("/api/admin/item-groups/set", verifyAdmin, setGroupHandler);
app.post("/api/admin/estratificacion/item-groups/set", verifyAdmin, setGroupHandler);

/* =========================================================
   ✅ START
========================================================= */
process.on("unhandledRejection", (e) => console.error("unhandledRejection:", e));
process.on("uncaughtException", (e) => console.error("uncaughtException:", e));

app.listen(Number(PORT), () => console.log(`Estratificacion server listening on :${PORT}`));
