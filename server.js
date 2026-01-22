import express from "express";
import cors from "cors";

const app = express();
app.use(express.json({ limit: "2mb" }));

/* ========= ENV ========= */
const SAP_BASE_URL = (process.env.SAP_BASE_URL || "").replace(/\/$/, ""); // sin / final
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

function missingSapEnv() {
  return !SAP_BASE_URL || !SAP_COMPANYDB || !SAP_USER || !SAP_PASS;
}

// Escapa strings para OData: ' -> ''
function odataEscape(str) {
  return String(str || "").replace(/'/g, "''");
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

  // Guardamos solo lo necesario (B1SESSION + ROUTEID normalmente)
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
      Cookie: SL_COOKIE || "",
      ...(options.headers || {}),
    },
  });

  const text = await res.text();

  // si cookie expira, reintentar 1 vez
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

/* ========= Obtener PriceListNo por nombre ========= */
async function getPriceListNoByName(name) {
  const safeName = odataEscape(name);

  // Intento 1: PriceListName
  try {
    const r1 = await slFetch(
      `/PriceLists?$select=PriceListNo,PriceListName&$filter=PriceListName eq '${safeName}'`
    );
    if (r1?.value?.length) return r1.value[0].PriceListNo;
  } catch {}

  // Intento 2: ListName
  try {
    const r2 = await slFetch(
      `/PriceLists?$select=PriceListNo,ListName&$filter=ListName eq '${safeName}'`
    );
    if (r2?.value?.length) return r2.value[0].PriceListNo;
  } catch {}

  return null;
}

/* ========= Endpoint: item + precio + stock por bodega =========
   GET /api/sap/item/0110
*/
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

    const safeCode = odataEscape(code);
    const safeWH = odataEscape(SAP_WAREHOUSE);

    // 1) Info básica del item (SIN encodeURIComponent dentro del string)
    const item = await slFetch(
      `/Items('${safeCode}')?$select=ItemCode,ItemName,SalesUnit,InventoryItem`
    );

    // 2) Stock por bodega (ItemWarehouseInfoCollection)
    // Campos correctos: InStock, Committed, Ordered
    let whRow = null;
    try {
      const whRes = await slFetch(
        `/ItemWarehouseInfoCollection?$select=ItemCode,WarehouseCode,InStock,Committed,Ordered&$filter=ItemCode eq '${safeCode}' and WarehouseCode eq '${safeWH}'`
      );
      if (whRes?.value?.length) whRow = whRes.value[0];
    } catch {
      whRow = null;
    }

    // 3) Precio por lista (opcional)
    let price = null;
    const priceListNo = await getPriceListNoByName(SAP_PRICE_LIST);

    if (priceListNo !== null) {
      try {
        const pRes = await slFetch(
          `/ItemPrices?$select=ItemCode,PriceList,Price&$filter=ItemCode eq '${safeCode}' and PriceList eq ${priceListNo}`
        );
        if (pRes?.value?.length) price = Number(pRes.value[0].Price);
      } catch {
        price = null;
      }
    }

    // 4) Cálculo stock disponible
    const onHand = whRow ? Number(whRow.InStock || 0) : null;
    const committed = whRow ? Number(whRow.Committed || 0) : 0;
    const available = whRow ? onHand - committed : null;

    return res.json({
      ok: true,
      item: {
        ItemCode: item.ItemCode,
        ItemName: item.ItemName,
        SalesUnit: item.SalesUnit,
        InventoryItem: item.InventoryItem,
      },
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
    console.error("❌ /api/sap/item error:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* ========= START ========= */
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log("✅ Server listo en puerto", PORT));
