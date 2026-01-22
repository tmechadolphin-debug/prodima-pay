import express from "express";
import cors from "cors";

/**
 * PRODIMA API (Render)
 * - SAP Business One Service Layer (TopManage cloud)
 * - Endpoints para precio + stock por ItemCode
 *
 * IMPORTANTE:
 *  - NO uses $expand con ItemPrices o ItemWarehouseInfoCollection (da error en algunos entornos)
 *  - Usamos $select y luego filtramos en el backend
 */

const app = express();

/* ===========================
   ENV
=========================== */
const {
  PORT = 3000,

  // CORS
  CORS_ORIGIN = "*",

  // SAP (Service Layer)
  SAP_BASE_URL = "",
  SAP_COMPANYDB = "",
  SAP_USER = "",
  SAP_PASS = "",

  // Preferencias
  SAP_WAREHOUSE = "01",
  SAP_PRICE_LIST = "Lista Distribuidor",

  // Yappy (solo informativo por ahora)
  YAPPY_ALIAS = "@prodimasansae",
} = process.env;

/* ===========================
   MIDDLEWARE
=========================== */
app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? "*" : CORS_ORIGIN.split(",").map((s) => s.trim()),
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  })
);
app.use(express.json({ limit: "2mb" }));

/* ===========================
   HELPERS
=========================== */
function missingSapEnv() {
  return !SAP_BASE_URL || !SAP_COMPANYDB || !SAP_USER || !SAP_PASS;
}

function safeJsonParse(text) {
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

// Para poner un ItemCode seguro en OData: Items('...')
function odataQuote(value = "") {
  // En OData el escape de comilla simple es duplicándola
  return String(value).replace(/'/g, "''");
}

// Build URL base asegurando que termine sin slash
const SAP = SAP_BASE_URL ? SAP_BASE_URL.replace(/\/+$/, "") : "";

/* ===========================
   SAP SESSION CACHE
=========================== */
let sapCookie = "";
let sapCookieAt = 0;
const SAP_COOKIE_TTL_MS = 1000 * 60 * 20; // 20 min

async function sapLogin() {
  if (missingSapEnv()) {
    throw new Error("Faltan variables de entorno SAP.");
  }

  const now = Date.now();
  if (sapCookie && now - sapCookieAt < SAP_COOKIE_TTL_MS) {
    return sapCookie;
  }

  const url = `${SAP}/Login`;
  const body = {
    CompanyDB: SAP_COMPANYDB,
    UserName: SAP_USER,
    Password: SAP_PASS,
  };

  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`SAP Login error ${resp.status}: ${text}`);
  }

  const setCookie = resp.headers.get("set-cookie");
  if (!setCookie) {
    throw new Error("SAP Login: no se recibió cookie.");
  }

  sapCookie = setCookie;
  sapCookieAt = now;
  return sapCookie;
}

async function sapFetch(path, { method = "GET", body } = {}) {
  const cookie = await sapLogin();
  const url = `${SAP}${path.startsWith("/") ? "" : "/"}${path}`;

  const resp = await fetch(url, {
    method,
    headers: {
      "Content-Type": "application/json",
      Cookie: cookie,
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  const text = await resp.text();
  const json = safeJsonParse(text);

  if (!resp.ok) {
    // retry once on auth
    if (resp.status === 401 || resp.status === 403) {
      sapCookie = "";
      const cookie2 = await sapLogin();
      const resp2 = await fetch(url, {
        method,
        headers: { "Content-Type": "application/json", Cookie: cookie2 },
        body: body ? JSON.stringify(body) : undefined,
      });
      const text2 = await resp2.text();
      const json2 = safeJsonParse(text2);
      if (!resp2.ok) throw new Error(`SAP error ${resp2.status}: ${text2}`);
      return json2 ?? text2;
    }
    throw new Error(`SAP error ${resp.status}: ${text}`);
  }

  return json ?? text;
}

/* ===========================
   PRICE LIST RESOLUTION
=========================== */
let cachedPriceListNo = null;
let cachedPriceListName = null;
let priceListFetchedAt = 0;
const PRICE_LIST_TTL_MS = 1000 * 60 * 60;

async function resolvePriceListNo() {
  const now = Date.now();

  // allow numeric value
  const asNum = Number(SAP_PRICE_LIST);
  if (!Number.isNaN(asNum) && String(SAP_PRICE_LIST).trim() !== "") {
    cachedPriceListNo = asNum;
    cachedPriceListName = `#${asNum}`;
    return asNum;
  }

  if (cachedPriceListNo && now - priceListFetchedAt < PRICE_LIST_TTL_MS) {
    return cachedPriceListNo;
  }

  const data = await sapFetch(`/PriceLists?$select=PriceListNo,PriceListName`);
  const list = Array.isArray(data?.value) ? data.value : [];
  const found = list.find(
    (x) => String(x.PriceListName || "").trim().toLowerCase() === String(SAP_PRICE_LIST).trim().toLowerCase()
  );

  if (!found) {
    cachedPriceListNo = 1;
    cachedPriceListName = "1";
    priceListFetchedAt = now;
    return 1;
  }

  cachedPriceListNo = Number(found.PriceListNo);
  cachedPriceListName = found.PriceListName;
  priceListFetchedAt = now;
  return cachedPriceListNo;
}

/* ===========================
   ROUTES
=========================== */
app.get("/api/health", (req, res) => {
  res.json({
    ok: true,
    message: "✅ PRODIMA API activa",
    yappy: YAPPY_ALIAS,
    warehouse: SAP_WAREHOUSE,
    priceList: SAP_PRICE_LIST,
  });
});

/**
 * ✅ ITEM COMPLETO (precio + stock)
 * GET /api/sap/item/:code
 */
app.get("/api/sap/item/:code", async (req, res) => {
  try {
    if (missingSapEnv()) {
      return res.status(400).json({ ok: false, message: "⚠️ Faltan variables de entorno SAP en Render." });
    }

    const code = req.params.code?.trim();
    if (!code) return res.status(400).json({ ok: false, message: "ItemCode inválido." });

    const priceListNo = await resolvePriceListNo();

    // ⚠️ NO $expand — SOLO $select
    const select = encodeURIComponent("ItemCode,ItemName,SalesUnit,ItemPrices,ItemWarehouseInfoCollection");
    const item = await sapFetch(`/Items('${encodeURIComponent(odataQuote(code))}')?$select=${select}`);

    let price = null;
    let currency = null;
    if (Array.isArray(item?.ItemPrices)) {
      const p = item.ItemPrices.find((x) => Number(x.PriceList) === Number(priceListNo));
      if (p) {
        price = Number(p.Price);
        currency = p.Currency || null;
      }
    }

    let stock = null;
    if (Array.isArray(item?.ItemWarehouseInfoCollection)) {
      const w = item.ItemWarehouseInfoCollection.find((x) => String(x.WarehouseCode) === String(SAP_WAREHOUSE));
      if (w) stock = w.InStock ?? w.OnHand ?? null;
    }

    return res.json({
      ok: true,
      item: { ItemCode: item.ItemCode, ItemName: item.ItemName, SalesUnit: item.SalesUnit ?? null },
      priceList: { requested: SAP_PRICE_LIST, resolvedNo: priceListNo, resolvedName: cachedPriceListName || null },
      warehouse: SAP_WAREHOUSE,
      price,
      currency,
      stock,
      hasStock: stock !== null ? Number(stock) > 0 : null,
      raw: {
        hasItemPrices: Array.isArray(item?.ItemPrices),
        hasWarehouseInfo: Array.isArray(item?.ItemWarehouseInfoCollection),
      },
    });
  } catch (err) {
    console.error("❌ /api/sap/item/:code", err);
    return res.status(500).json({ ok: false, message: String(err?.message || err) });
  }
});

/* ===========================
   START
=========================== */
app.listen(PORT, () => {
  if (missingSapEnv()) console.log("⚠️ Faltan variables de entorno SAP. Revisa Render > Environment.");
  console.log("✅ Server listo en puerto", PORT);
});
