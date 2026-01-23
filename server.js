import express from "express";
import cors from "cors";
import jwt from "jsonwebtoken";
import crypto from "crypto";
import pg from "pg";

const { Pool } = pg;

const app = express();
app.use(express.json({ limit: "2mb" }));

/* ========= ENV ========= */
const SAP_BASE_URL = process.env.SAP_BASE_URL || "";
const SAP_COMPANYDB = process.env.SAP_COMPANYDB || "";
const SAP_USER = process.env.SAP_USER || "";
const SAP_PASS = process.env.SAP_PASS || "";
const SAP_WAREHOUSE = process.env.SAP_WAREHOUSE || "01";
const SAP_PRICE_LIST = process.env.SAP_PRICE_LIST || "Lista Distribuidor";

const YAPPY_ALIAS = process.env.YAPPY_ALIAS || "@prodimasansae";
const CORS_ORIGIN = process.env.CORS_ORIGIN || "*";

/* ========= AUTH ========= */
const AUTH_SECRET = process.env.AUTH_SECRET || "";
const MERCH_USERS_RAW = process.env.MERCH_USERS || "[]";
let MERCH_USERS = [];
try {
  MERCH_USERS = JSON.parse(MERCH_USERS_RAW);
} catch {
  MERCH_USERS = [];
}

/* ========= DB (Render Postgres) ========= */
const DATABASE_URL = process.env.DATABASE_URL || "";
const pool = DATABASE_URL
  ? new Pool({
      connectionString: DATABASE_URL,
      ssl: { rejectUnauthorized: false },
    })
  : null;

async function dbEnsure() {
  if (!pool) return;
  await pool.query(`
    CREATE TABLE IF NOT EXISTS audit_events (
      id SERIAL PRIMARY KEY,
      type TEXT NOT NULL,
      username TEXT,
      fullname TEXT,
      ip TEXT,
      user_agent TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      payload JSONB
    )
  `);
}

async function audit(type, req, payload = {}, user = null) {
  const ip =
    req.headers["x-forwarded-for"]?.toString()?.split(",")[0]?.trim() ||
    req.socket?.remoteAddress ||
    "";
  const ua = req.headers["user-agent"] || "";

  const username = user?.user || null;
  const fullname = user?.name || null;

  // Si hay BD → guardar persistente
  if (pool) {
    await pool.query(
      `
      INSERT INTO audit_events(type, username, fullname, ip, user_agent, payload)
      VALUES ($1,$2,$3,$4,$5,$6)
    `,
      [type, username, fullname, ip, ua, payload]
    );
    return;
  }

  // Fallback si no hay BD
  console.log("AUDIT:", {
    type,
    username,
    fullname,
    ip,
    ua,
    payload,
    at: new Date().toISOString(),
  });
}

/* ========= CORS ========= */
app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? "*" : [CORS_ORIGIN],
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  })
);

/* ========= Helpers ========= */
let SL_COOKIE = null;
let SL_COOKIE_TIME = 0;

function missingSapEnv() {
  return !SAP_BASE_URL || !SAP_COMPANYDB || !SAP_USER || !SAP_PASS;
}

async function slLogin() {
  if (missingSapEnv()) {
    console.log("⚠️ Faltan variables SAP en Render > Environment");
    return;
  }

  const payload = {
    CompanyDB: SAP_COMPANYDB,
    UserName: SAP_USER,
    Password: SAP_PASS,
  };

  const res = await fetch(`${SAP_BASE_URL}/Login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const t = await res.text();
    throw new Error(`Login SAP falló (${res.status}): ${t}`);
  }

  const setCookie = res.headers.get("set-cookie");
  if (!setCookie) throw new Error("No se recibió cookie del Service Layer.");

  SL_COOKIE = setCookie
    .split(",")
    .map((s) => s.split(";")[0])
    .join("; ");

  SL_COOKIE_TIME = Date.now();
  console.log("✅ Login SAP OK (cookie guardada)");
}

async function slFetch(path, options = {}) {
  if (!SL_COOKIE || Date.now() - SL_COOKIE_TIME > 25 * 60 * 1000) {
    await slLogin();
  }

  const res = await fetch(`${SAP_BASE_URL}${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      Cookie: SL_COOKIE,
      ...(options.headers || {}),
    },
  });

  const text = await res.text();

  if (res.status === 401 || res.status === 403) {
    SL_COOKIE = null;
    await slLogin();
    return slFetch(path, options);
  }

  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }

  if (!res.ok) {
    throw new Error(`SAP error ${res.status}: ${text}`);
  }

  return json;
}

/* ========= AUTH Middleware ========= */
function requireAuth(req, res, next) {
  try {
    const h = req.headers.authorization || "";
    const token = h.startsWith("Bearer ") ? h.slice(7) : "";
    if (!token) return res.status(401).json({ ok: false, message: "No auth token" });

    const decoded = jwt.verify(token, AUTH_SECRET);
    req.auth = decoded; // { user, name, iat, exp }
    return next();
  } catch (e) {
    return res.status(401).json({ ok: false, message: "Token inválido o expirado" });
  }
}

/* ========= HEALTH ========= */
app.get("/api/health", (req, res) => {
  res.json({
    ok: true,
    message: "✅ PRODIMA API activa",
    yappy: YAPPY_ALIAS,
    warehouse: SAP_WAREHOUSE,
    priceList: SAP_PRICE_LIST,
    auth: Boolean(AUTH_SECRET && MERCH_USERS.length),
    db: Boolean(pool),
  });
});

/* ========= AUTH Routes ========= */
app.post("/api/auth/login", async (req, res) => {
  try {
    if (!AUTH_SECRET) {
      return res.status(500).json({ ok: false, message: "AUTH_SECRET no configurado" });
    }

    const user = String(req.body?.user || "").trim().toLowerCase();
    const pin = String(req.body?.pin || "").trim();

    if (!user || !pin) {
      return res.status(400).json({ ok: false, message: "user y pin son requeridos" });
    }

    const found = MERCH_USERS.find(
      (u) => String(u.user || "").toLowerCase() === user && String(u.pin || "") === pin
    );
    if (!found) {
      await audit("login_failed", req, { user });
      return res.status(401).json({ ok: false, message: "Credenciales incorrectas" });
    }

    const payload = {
      user: found.user,
      name: found.name || found.user,
    };

    const token = jwt.sign(payload, AUTH_SECRET, { expiresIn: "12h" });

    await audit("login", req, { ok: true }, payload);

    return res.json({
      ok: true,
      token,
      user: payload,
      loginAt: new Date().toISOString(),
    });
  } catch (err) {
    return res.status(500).json({ ok: false, message: err.message });
  }
});

app.get("/api/auth/me", requireAuth, (req, res) => {
  res.json({ ok: true, user: req.auth });
});

/* ========= Obtener PriceListNo por nombre ========= */
async function getPriceListNoByName(name) {
  const safe = name.replace(/'/g, "''");

  try {
    const r1 = await slFetch(
      `/PriceLists?$select=PriceListNo,PriceListName&$filter=PriceListName eq '${safe}'`
    );
    if (r1?.value?.length) return r1.value[0].PriceListNo;
  } catch {}

  try {
    const r2 = await slFetch(
      `/PriceLists?$select=PriceListNo,ListName&$filter=ListName eq '${safe}'`
    );
    if (r2?.value?.length) return r2.value[0].PriceListNo;
  } catch {}

  return null;
}

/* =========================================================
   ✅ SAP Item (protegido)
========================================================= */
app.get("/api/sap/item/:code", requireAuth, async (req, res) => {
  try {
    if (missingSapEnv()) {
      return res.status(400).json({
        ok: false,
        message: "Faltan variables SAP en Render > Environment",
      });
    }

    const code = String(req.params.code || "").trim();
    if (!code) return res.status(400).json({ ok: false, message: "ItemCode vacío." });

    const priceListNo = await getPriceListNoByName(SAP_PRICE_LIST);

    const itemFull = await slFetch(`/Items('${encodeURIComponent(code)}')`);

    const item = {
      ItemCode: itemFull.ItemCode,
      ItemName: itemFull.ItemName,
      SalesUnit: itemFull.SalesUnit,
      InventoryItem: itemFull.InventoryItem,
    };

    let price = null;
    if (priceListNo !== null && Array.isArray(itemFull.ItemPrices)) {
      const p = itemFull.ItemPrices.find((x) => Number(x.PriceList) === Number(priceListNo));
      if (p && p.Price != null) price = Number(p.Price);
    }

    let wh = null;
    if (Array.isArray(itemFull.ItemWarehouseInfoCollection)) {
      wh = itemFull.ItemWarehouseInfoCollection.find(
        (x) => String(x.WarehouseCode) === String(SAP_WAREHOUSE)
      );
    }

    const onHand = wh?.InStock ?? wh?.OnHand ?? wh?.QuantityOnStock ?? null;
    const committed = wh?.Committed ?? 0;
    const available = onHand !== null ? Number(onHand) - Number(committed) : null;

    return res.json({
      ok: true,
      item,
      warehouse: SAP_WAREHOUSE,
      priceList: SAP_PRICE_LIST,
      priceListNo,
      price,
      stock: {
        onHand,
        committed,
        available,
        hasStock: available !== null ? available > 0 : null,
      },
    });
  } catch (err) {
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ SAP Customer (protegido)
   Nota: Ajusta select según tu versión SAP
========================================================= */
app.get("/api/sap/customer/:cardCode", requireAuth, async (req, res) => {
  try {
    const cardCode = String(req.params.cardCode || "").trim();
    if (!cardCode) return res.status(400).json({ ok: false, message: "CardCode vacío" });

    const c = await slFetch(`/BusinessPartners('${encodeURIComponent(cardCode)}')?$select=CardCode,CardName,Phone1,Phone2,Cellular,EmailAddress,Address`);

    return res.json({ ok: true, customer: c });
  } catch (err) {
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ Crear Cotización en SAP (protegido)
========================================================= */
app.post("/api/sap/quote", requireAuth, async (req, res) => {
  try {
    const cardCode = String(req.body?.cardCode || "").trim();
    const comments = String(req.body?.comments || "").trim();
    const lines = Array.isArray(req.body?.lines) ? req.body.lines : [];

    if (!cardCode) return res.status(400).json({ ok: false, message: "cardCode requerido" });
    if (!lines.length) return res.status(400).json({ ok: false, message: "lines vacío" });

    const docLines = lines
      .map((l) => ({
        ItemCode: String(l.itemCode || "").trim(),
        Quantity: Number(l.qty || 0),
      }))
      .filter((x) => x.ItemCode && x.Quantity > 0);

    if (!docLines.length) {
      return res.status(400).json({ ok: false, message: "No hay líneas válidas" });
    }

    // 🔥 Importante: no validamos inventario aquí (son cotizaciones)
    const payload = {
      CardCode: cardCode,
      Comments: `[WEB MERCADERISTA: ${req.auth.user}] ${comments || ""}`.trim(),
      DocumentLines: docLines,
    };

    const created = await slFetch("/Quotations", {
      method: "POST",
      body: JSON.stringify(payload),
    });

    // SAP normalmente devuelve DocEntry, y se puede consultar DocNum con fetch adicional,
    // pero muchas veces viene en la respuesta:
    const docEntry = created?.DocEntry ?? null;
    const docNum = created?.DocNum ?? null;

    await audit(
      "quote_created",
      req,
      {
        cardCode,
        docEntry,
        docNum,
        lines: docLines.length,
      },
      req.auth
    );

    return res.json({ ok: true, docEntry, docNum, created });
  } catch (err) {
    await audit("quote_failed", req, { error: err.message }, req.auth);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* ========= START ========= */
const PORT = process.env.PORT || 10000;

(async () => {
  await dbEnsure();
  app.listen(PORT, () => console.log("✅ Server listo en puerto", PORT));
})();
