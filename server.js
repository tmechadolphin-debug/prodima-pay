import express from "express";
import cors from "cors";

const app = express();
app.use(express.json({ limit: "2mb" }));

/**
 * =========================
 * ENV VARS (Render)
 * =========================
 */
const SAP_BASE_URL   = process.env.SAP_BASE_URL;      // https://india.pa2.sap.topmanage.cloud/b1s/v1
const SAP_COMPANYDB  = process.env.SAP_COMPANYDB;     // C7357449_PRDMA_PRD
const SAP_USER       = process.env.SAP_USER;          // adm-red@prodima.com.pa
const SAP_PASS       = process.env.SAP_PASS;          // tu password
const SAP_WAREHOUSE  = process.env.SAP_WAREHOUSE || "01";
const SAP_PRICE_LIST = process.env.SAP_PRICE_LIST || "Lista Distribuidor";

const YAPPY_ALIAS    = process.env.YAPPY_ALIAS || "@prodimasansae";
const CORS_ORIGIN    = process.env.CORS_ORIGIN || "*";

/**
 * =========================
 * CORS (solo tu dominio)
 * =========================
 */
app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? "*" : [CORS_ORIGIN],
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type"],
  })
);

if (!SAP_BASE_URL || !SAP_COMPANYDB || !SAP_USER || !SAP_PASS) {
  console.warn("⚠️ Faltan variables de entorno SAP. Revisa Render > Environment.");
}

/**
 * =========================
 * SAP SESSION COOKIE EN MEMORIA
 * =========================
 */
let sapCookie = "";

// toma el set-cookie completo
function extractCookie(headers) {
  return headers.get("set-cookie") || "";
}

async function sapLogin() {
  if (!SAP_BASE_URL) throw new Error("Falta SAP_BASE_URL");
  if (!SAP_COMPANYDB) throw new Error("Falta SAP_COMPANYDB");
  if (!SAP_USER) throw new Error("Falta SAP_USER");
  if (!SAP_PASS) throw new Error("Falta SAP_PASS");

  const payload = {
    CompanyDB: SAP_COMPANYDB,
    UserName: SAP_USER,
    Password: SAP_PASS,
  };

  const resp = await fetch(`${SAP_BASE_URL}/Login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!resp.ok) {
    const txt = await resp.text();
    throw new Error(`SAP Login falló: ${resp.status} - ${txt}`);
  }

  sapCookie = extractCookie(resp.headers);
  if (!sapCookie) {
    console.warn("⚠️ Login OK pero no recibí set-cookie. Verifica SAP Service Layer.");
  }
}

async function sapFetch(path, { method = "GET", body } = {}) {
  if (!sapCookie) await sapLogin();

  const headers = {
    "Content-Type": "application/json",
    Cookie: sapCookie,
  };

  let resp = await fetch(`${SAP_BASE_URL}${path}`, {
    method,
    headers,
    body: body ? JSON.stringify(body) : undefined,
  });

  // si expiró sesión, re-login 1 vez
  if (resp.status === 401) {
    sapCookie = "";
    await sapLogin();

    resp = await fetch(`${SAP_BASE_URL}${path}`, {
      method,
      headers: {
        "Content-Type": "application/json",
        Cookie: sapCookie,
      },
      body: body ? JSON.stringify(body) : undefined,
    });
  }

  if (!resp.ok) {
    const txt = await resp.text();
    throw new Error(`SAP error ${resp.status}: ${txt}`);
  }

  return resp.json();
}

/**
 * =========================
 * HEALTH
 * =========================
 */
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
 * =========================
 * BUSCAR PriceListNo por nombre exacto
 * =========================
 */
async function getPriceListNoByName(priceListName) {
  const data = await sapFetch(
    `/PriceLists?$select=PriceListNo,PriceListName&$filter=PriceListName eq '${priceListName}'`
  );
  const list = data?.value || [];
  if (!list.length) return null;
  return list[0].PriceListNo;
}

/**
 * =========================
 * STOCK + PRECIO POR SKU
 * =========================
 * GET /api/sap/items?skus=E-0100,E-0101
 */
app.get("/api/sap/items", async (req, res) => {
  try {
    const warehouse = (req.query.warehouse || SAP_WAREHOUSE).toString();
    const priceListName = (req.query.pricelist || SAP_PRICE_LIST).toString();

    const skusRaw = (req.query.skus || "").toString().trim();
    const skus = skusRaw
      ? skusRaw.split(",").map((x) => x.trim()).filter(Boolean)
      : [];

    const priceListNo = await getPriceListNoByName(priceListName);
    if (priceListNo === null) {
      return res.status(400).json({
        ok: false,
        message: `No encontré la lista de precios: "${priceListName}". Revisa el nombre exacto en SAP.`,
      });
    }

    if (!skus.length) {
      return res.status(400).json({
        ok: false,
        message: "Debes enviar skus=SKU1,SKU2,...",
      });
    }

    const results = [];

    for (const sku of skus) {
      const item = await sapFetch(
        `/Items('${encodeURIComponent(sku)}')?$select=ItemCode,ItemName,ItemPrices,ItemWarehouseInfoCollection&$expand=ItemPrices,ItemWarehouseInfoCollection`
      );

      const wh = (item.ItemWarehouseInfoCollection || []).find(
        (w) => (w.WarehouseCode || "").toString() === warehouse
      );

      // En SAP puede venir InStock o OnHand (depende versión)
      const stock = Number(wh?.InStock ?? wh?.OnHand ?? 0);

      const priceRow = (item.ItemPrices || []).find(
        (p) => Number(p.PriceList) === Number(priceListNo)
      );
      const price = Number(priceRow?.Price ?? 0);

      results.push({
        sku: item.ItemCode,
        name: item.ItemName,
        price,
        stock,
        available: stock > 0,
      });
    }

    return res.json({ ok: true, warehouse, priceListName, results });
  } catch (err) {
    console.error("❌ /api/sap/items error:", err);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/**
 * =========================
 * YAPPY ORDER (REGISTRO)
 * POST /api/yappy-order
 * =========================
 */
const yappyOrders = [];

app.post("/api/yappy-order", (req, res) => {
  try {
    const order = req.body;

    if (!order?.customer?.nombre || !order?.customer?.tel || !order?.customer?.mail || !order?.customer?.dir) {
      return res.status(400).send("Pedido incompleto: faltan datos del cliente.");
    }
    if (!order?.items?.length) {
      return res.status(400).send("Pedido incompleto: carrito vacío.");
    }
    if (!order?.amount || Number(order.amount) <= 0) {
      return res.status(400).send("Pedido incompleto: monto inválido.");
    }

    const orderId = "YP-" + Date.now();

    const payload = {
      orderId,
      createdAt: new Date().toISOString(),
      paymentMethod: "Yappy",
      yappyAlias: YAPPY_ALIAS,
      amount: Number(order.amount),
      reference: order?.reference || "",
      comments: order?.comments || "",
      customer: order.customer,
      retiroPorTerceros: order?.retiroPorTerceros || null,
      items: order.items,
      status: "PENDIENTE_VALIDACION",
      origin: order?.origin || "",
    };

    yappyOrders.push(payload);

    console.log("✅ NUEVO PEDIDO YAPPY:", payload);

    return res.json({ ok: true, orderId, yappyAlias: YAPPY_ALIAS });
  } catch (err) {
    console.error("❌ Error en /api/yappy-order:", err);
    return res.status(500).send("Error interno registrando pedido.");
  }
});

/**
 * (opcional) ver pedidos rápido
 */
app.get("/api/yappy-orders", (req, res) => {
  res.json({ ok: true, count: yappyOrders.length, orders: yappyOrders });
});

/**
 * =========================
 * START SERVER
 * =========================
 */
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("✅ Server listo en puerto", PORT));
