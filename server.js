import express from "express";
import cors from "cors";

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

/* ========= CORS ========= */
app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? "*" : [CORS_ORIGIN],
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type"],
  })
);

/* ========= Helpers ========= */
let SL_COOKIE = null;
let SL_COOKIE_TIME = 0;

/** Cache para PriceListNo (evita buscarlo para cada item) */
let PRICE_LIST_CACHE = { name: "", no: null, ts: 0 };
const PRICE_LIST_TTL_MS = 6 * 60 * 60 * 1000; // 6 horas

/** Cache corta para items (evita que refresh repita todo) */
const ITEM_CACHE = new Map(); // code -> { ts, data }
const ITEM_TTL_MS = 20 * 1000; // 20 segundos (puedes subir a 60s si quieres)

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

  // Reintento si expiró
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

/* ========= API Health ========= */
app.get("/api/health", (req, res) => {
  res.json({
    ok: true,
    message: "✅ PRODIMA API activa",
    yappy: YAPPY_ALIAS,
    warehouse: SAP_WAREHOUSE,
    priceList: SAP_PRICE_LIST,
  });
});

/* ========= Obtener PriceListNo por nombre (CACHEADO) ========= */
async function getPriceListNoByNameCached(name) {
  const now = Date.now();

  if (
    PRICE_LIST_CACHE.name === name &&
    PRICE_LIST_CACHE.no !== null &&
    now - PRICE_LIST_CACHE.ts < PRICE_LIST_TTL_MS
  ) {
    return PRICE_LIST_CACHE.no;
  }

  const safe = name.replace(/'/g, "''");
  let no = null;

  // Intento A: PriceListName
  try {
    const r1 = await slFetch(
      `/PriceLists?$select=PriceListNo,PriceListName&$filter=PriceListName eq '${safe}'`
    );
    if (r1?.value?.length) no = r1.value[0].PriceListNo;
  } catch {}

  // Intento B: ListName
  if (no === null) {
    try {
      const r2 = await slFetch(
        `/PriceLists?$select=PriceListNo,ListName&$filter=ListName eq '${safe}'`
      );
      if (r2?.value?.length) no = r2.value[0].PriceListNo;
    } catch {}
  }

  PRICE_LIST_CACHE = { name, no, ts: now };
  return no;
}

/* ========= Construir respuesta desde el Item FULL ========= */
function buildItemResponse(itemFull, code, priceListNo) {
  const item = {
    ItemCode: itemFull.ItemCode ?? code,
    ItemName: itemFull.ItemName ?? `Producto ${code}`,
    SalesUnit: itemFull.SalesUnit ?? "",
    InventoryItem: itemFull.InventoryItem ?? null,
  };

  // Precio
  let price = null;
  if (priceListNo !== null && Array.isArray(itemFull.ItemPrices)) {
    const p = itemFull.ItemPrices.find(
      (x) => Number(x.PriceList) === Number(priceListNo)
    );
    if (p && p.Price != null) price = Number(p.Price);
  }

  // Stock
  let wh = null;
  if (Array.isArray(itemFull.ItemWarehouseInfoCollection)) {
    wh = itemFull.ItemWarehouseInfoCollection.find(
      (x) => String(x.WarehouseCode) === String(SAP_WAREHOUSE)
    );
  }

  const onHand = wh?.InStock ?? wh?.OnHand ?? wh?.QuantityOnStock ?? null;
  const committed = wh?.Committed ?? 0;
  const available = onHand !== null ? Number(onHand) - Number(committed) : null;

  return {
    ok: true,
    item,
    price,
    stock: {
      onHand,
      committed,
      available,
      hasStock: available !== null ? available > 0 : null,
    },
  };
}

/* ========= Obtener 1 item (con cache corto) ========= */
async function getOneItem(code, priceListNo) {
  const now = Date.now();
  const cached = ITEM_CACHE.get(code);
  if (cached && now - cached.ts < ITEM_TTL_MS) {
    return cached.data;
  }

  // ✅ Truco: pedimos solo lo necesario (si SAP lo soporta)
  // Si tu SAP no respeta $select para arrays, igual cae al fallback FULL.
  let itemFull;
  try {
    itemFull = await slFetch(
      `/Items('${encodeURIComponent(code)}')?$select=ItemCode,ItemName,SalesUnit,InventoryItem,ItemPrices,ItemWarehouseInfoCollection`
    );
  } catch {
    // Fallback FULL (tu método actual)
    itemFull = await slFetch(`/Items('${encodeURIComponent(code)}')`);
  }

  const data = buildItemResponse(itemFull, code, priceListNo);

  ITEM_CACHE.set(code, { ts: now, data });
  return data;
}

/* =========================================================
   ✅ Endpoint seguro 1 item
   GET /api/sap/item/0110
========================================================= */
app.get("/api/sap/item/:code", async (req, res) => {
  try {
    if (missingSapEnv()) {
      return res.status(400).json({
        ok: false,
        message: "Faltan variables SAP en Render > Environment",
      });
    }

    const code = String(req.params.code || "").trim();
    if (!code) {
      return res.status(400).json({ ok: false, message: "ItemCode vacío." });
    }

    const priceListNo = await getPriceListNoByNameCached(SAP_PRICE_LIST);
    const r = await getOneItem(code, priceListNo);

    return res.json({
      ok: true,
      item: r.item,
      warehouse: SAP_WAREHOUSE,
      priceList: SAP_PRICE_LIST,
      priceListNo,
      price: r.price,
      stock: r.stock,
    });
  } catch (err) {
    console.error("❌ /api/sap/item error:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ Endpoint MULTI-ITEM RÁPIDO (1 llamada desde frontend)
   GET /api/sap/items?codes=0110,0105,0124
========================================================= */
app.get("/api/sap/items", async (req, res) => {
  try {
    if (missingSapEnv()) {
      return res.status(400).json({
        ok: false,
        message: "Faltan variables SAP en Render > Environment",
      });
    }

    const codes = String(req.query.codes || "")
      .split(",")
      .map((x) => x.trim())
      .filter(Boolean);

    if (!codes.length) {
      return res.status(400).json({ ok: false, message: "codes vacío" });
    }

    const priceListNo = await getPriceListNoByNameCached(SAP_PRICE_LIST);

    // ✅ Paralelo CON LÍMITE (para no saturar SAP)
    const CONCURRENCY = 5;
    const items = {};
    let i = 0;

    async function worker() {
      while (i < codes.length) {
        const idx = i++;
        const code = codes[idx];
        try {
          const r = await getOneItem(code, priceListNo);
          items[code] = {
            ok: true,
            name: r.item.ItemName,
            unit: r.item.SalesUnit,
            price: r.price,
            stock: r.stock,
          };
        } catch (e) {
          items[code] = { ok: false, message: String(e.message || e) };
        }
      }
    }

    await Promise.all(Array.from({ length: CONCURRENCY }, worker));

    return res.json({
      ok: true,
      warehouse: SAP_WAREHOUSE,
      priceList: SAP_PRICE_LIST,
      priceListNo,
      items,
    });
  } catch (err) {
    console.error("❌ /api/sap/items error:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ CLIENTE: trae info del Business Partner
   GET /api/sap/customer/C12345
========================================================= */
app.get("/api/sap/customer/:code", async (req, res) => {
  try {
    if (missingSapEnv()) {
      return res.status(400).json({ ok: false, message: "Faltan variables SAP" });
    }

    const code = String(req.params.code || "").trim();
    if (!code) return res.status(400).json({ ok: false, message: "CardCode vacío." });

    // BusinessPartners('Cxxxx')
    const bp = await slFetch(
      `/BusinessPartners('${encodeURIComponent(code)}')?$select=CardCode,CardName,Phone1,Phone2,EmailAddress,Address,City,Country,ZipCode`
    );

    // armamos una dirección bonita
    const addrParts = [
      bp.Address,
      bp.City,
      bp.ZipCode,
      bp.Country
    ].filter(Boolean).join(", ");

    return res.json({
      ok: true,
      customer: {
        CardCode: bp.CardCode,
        CardName: bp.CardName,
        Phone1: bp.Phone1,
        Phone2: bp.Phone2,
        EmailAddress: bp.EmailAddress,
        Address: addrParts || bp.Address || "",
      },
    });
  } catch (err) {
    console.error("❌ /api/sap/customer error:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});


/* =========================================================
   ✅ CREAR COTIZACIÓN (Quotation) en SAP
   POST /api/sap/quote
   body:
   {
     "cardCode":"C12345",
     "comments":"...",
     "paymentMethod":"CONTRA_ENTREGA",
     "lines":[ {"itemCode":"0110","qty":2}, ... ]
   }
========================================================= */
app.post("/api/sap/quote", async (req, res) => {
  try {
    if (missingSapEnv()) {
      return res.status(400).json({ ok: false, message: "Faltan variables SAP" });
    }

    const cardCode = String(req.body?.cardCode || "").trim();
    const comments = String(req.body?.comments || "").trim();
    const lines = Array.isArray(req.body?.lines) ? req.body.lines : [];

    if (!cardCode) return res.status(400).json({ ok: false, message: "cardCode requerido." });
    if (!lines.length) return res.status(400).json({ ok: false, message: "lines requerido." });

    const DocumentLines = lines
      .map((l) => ({
        ItemCode: String(l.itemCode || "").trim(),
        Quantity: Number(l.qty || 0),
      }))
      .filter((x) => x.ItemCode && x.Quantity > 0);

    if (!DocumentLines.length) {
      return res.status(400).json({ ok: false, message: "No hay líneas válidas (qty>0)." });
    }

    const today = new Date();
    const docDate = today.toISOString().slice(0, 10); // YYYY-MM-DD

    // ✅ IMPORTANTE: NO seteamos PriceListNum
    // SAP usará automáticamente la lista de precios del cliente (CardCode)
    const payload = {
      CardCode: cardCode,
      DocDate: docDate,
      DocDueDate: docDate,
      Comments: comments
        ? `[WEB PEDIDOS] ${comments}`
        : "[WEB PEDIDOS] Cotización mercaderista",
      JournalMemo: "Cotización web mercaderistas",
      DocumentLines,
    };

    const created = await slFetch(`/Quotations`, {
      method: "POST",
      body: JSON.stringify(payload),
    });

    return res.json({
      ok: true,
      message: "Cotización creada",
      docEntry: created.DocEntry,
      docNum: created.DocNum,
    });
  } catch (err) {
    console.error("❌ /api/sap/quote error:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});


/* ========= START ========= */
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log("✅ Server listo en puerto", PORT));
