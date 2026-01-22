import express from "express";
import cors from "cors";
import Stripe from "stripe";

const app = express();

/* ===========================
   ENV
=========================== */
const {
  SAP_BASE_URL = "",
  SAP_COMPANYDB = "",
  SAP_USER = "",
  SAP_PASS = "",
  SAP_WAREHOUSE = "01",
  SAP_PRICE_LIST = "Lista Distribuidor",
  YAPPY_ALIAS = "@prodimasansae",
  CORS_ORIGIN = "*",
  STRIPE_SECRET_KEY = "",
} = process.env;

/* ===========================
   MIDDLEWARES
=========================== */
app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? "*" : CORS_ORIGIN,
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type"],
  })
);
app.use(express.json({ limit: "2mb" }));

/* ===========================
   HEALTH
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

/* ===========================
   YAPPY (REGISTRA ORDEN)
=========================== */
const yappyOrders = [];

app.post("/api/yappy-order", (req, res) => {
  try {
    const order = req.body;

    if (
      !order?.customer?.nombre ||
      !order?.customer?.tel ||
      !order?.customer?.mail ||
      !order?.customer?.dir
    ) {
      return res
        .status(400)
        .send("Pedido incompleto: faltan datos del cliente.");
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
      yappyAlias: order?.yappyAlias || YAPPY_ALIAS,
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

    return res.json({ ok: true, orderId });
  } catch (err) {
    console.error("❌ Error en /api/yappy-order:", err);
    return res.status(500).send("Error interno registrando pedido.");
  }
});

app.get("/api/yappy-orders", (req, res) => {
  res.json({ ok: true, count: yappyOrders.length, orders: yappyOrders });
});

/* ===========================
   SAP BUSINESS ONE (SERVICE LAYER)
=========================== */

// Cache de sesión SAP (cookies) + priceListId
let sapCookieHeader = "";
let sapCookieAt = 0;
let cachedPriceListId = null;
let cachedPriceListAt = 0;

function missingSapEnv() {
  return !SAP_BASE_URL || !SAP_COMPANYDB || !SAP_USER || !SAP_PASS;
}

function normalizeBaseUrl(url) {
  // Acepta con o sin / al final
  return url.endsWith("/") ? url.slice(0, -1) : url;
}

function parseSetCookieToHeader(setCookieArr) {
  // set-cookie puede venir como array
  if (!setCookieArr) return "";
  const arr = Array.isArray(setCookieArr) ? setCookieArr : [setCookieArr];
  // Tomamos solo nombre=valor
  return arr
    .map((c) => String(c).split(";")[0])
    .filter(Boolean)
    .join("; ");
}

async function sapLogin() {
  if (missingSapEnv()) {
    throw new Error("Faltan variables de entorno SAP.");
  }

  const base = normalizeBaseUrl(SAP_BASE_URL);
  const url = `${base}/Login`;

  const body = {
    CompanyDB: SAP_COMPANYDB,
    UserName: SAP_USER,
    Password: SAP_PASS,
  };

  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  if (!r.ok) {
    const txt = await r.text();
    throw new Error(`SAP Login error ${r.status}: ${txt}`);
  }

  // Capturamos cookies
  const setCookie = r.headers.getSetCookie
    ? r.headers.getSetCookie()
    : r.headers.get("set-cookie");

  sapCookieHeader = parseSetCookieToHeader(setCookie);
  sapCookieAt = Date.now();

  return true;
}

async function sapFetch(path, { method = "GET", body } = {}) {
  const base = normalizeBaseUrl(SAP_BASE_URL);
  const url = `${base}${path.startsWith("/") ? "" : "/"}${path}`;

  // Re-login si no hay cookie o si pasó mucho tiempo (20 min)
  const ageMs = Date.now() - sapCookieAt;
  if (!sapCookieHeader || ageMs > 20 * 60 * 1000) {
    await sapLogin();
  }

  const r = await fetch(url, {
    method,
    headers: {
      "Content-Type": "application/json",
      Cookie: sapCookieHeader,
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  // Si expira sesión, reintentar una vez
  if (r.status === 401 || r.status === 403) {
    await sapLogin();

    const r2 = await fetch(url, {
      method,
      headers: {
        "Content-Type": "application/json",
        Cookie: sapCookieHeader,
      },
      body: body ? JSON.stringify(body) : undefined,
    });

    if (!r2.ok) {
      const txt = await r2.text();
      throw new Error(`SAP error ${r2.status}: ${txt}`);
    }

    return r2;
  }

  if (!r.ok) {
    const txt = await r.text();
    throw new Error(`SAP error ${r.status}: ${txt}`);
  }

  return r;
}

async function getPriceListIdByName() {
  // cache 6 horas
  if (cachedPriceListId && Date.now() - cachedPriceListAt < 6 * 60 * 60 * 1000) {
    return cachedPriceListId;
  }

  // PriceLists?$filter=PriceListName eq 'Lista Distribuidor'&$select=PriceListNo,PriceListName
  const filter = encodeURIComponent(`PriceListName eq '${SAP_PRICE_LIST}'`);
  const r = await sapFetch(
    `/PriceLists?$select=PriceListNo,PriceListName&$filter=${filter}`
  );
  const json = await r.json();
  const row = json?.value?.[0];
  if (!row?.PriceListNo) {
    // Si no lo encuentra, devolvemos null (no bloquea stock)
    cachedPriceListId = null;
    cachedPriceListAt = Date.now();
    return null;
  }

  cachedPriceListId = Number(row.PriceListNo);
  cachedPriceListAt = Date.now();
  return cachedPriceListId;
}

// ✅ Probar conexión SAP
app.get("/api/sap/ping", async (req, res) => {
  try {
    const r = await sapFetch("/CompanyService_GetAdminInfo");
    const json = await r.json();
    res.json({ ok: true, adminInfo: json });
  } catch (e) {
    res.status(500).json({ ok: false, message: String(e.message || e) });
  }
});

// ✅ Obtener item por código con stock y precio
// Ej: /api/sap/item/0110
app.get("/api/sap/item/:code", async (req, res) => {
  try {
    const code = String(req.params.code || "").trim();
    if (!code) return res.status(400).json({ ok: false, message: "ItemCode requerido" });

    // 1) Traer item + warehouses (expand) + ItemPrices (SIN expand)
    // Nota: el error que viste era por usar $expand=ItemPrices, eso NO es válido.
    const select =
      "ItemCode,ItemName,SalesUnit,InventoryUOM,QuantityOnStock,ItemPrices,ItemWarehouseInfoCollection";

    const r = await sapFetch(
      `/Items('${encodeURIComponent(code)}')?$select=${select}&$expand=ItemWarehouseInfoCollection($select=WarehouseCode,InStock,Committed,Ordered)`
    );

    const item = await r.json();

    // 2) Stock por warehouse
    const wh = (item.ItemWarehouseInfoCollection || []).find(
      (w) => String(w.WarehouseCode) === String(SAP_WAREHOUSE)
    );

    const stock = wh ? Number(wh.InStock || 0) : null;

    // 3) Precio por lista
    let price = null;
    let priceListId = await getPriceListIdByName();

    if (priceListId && Array.isArray(item.ItemPrices)) {
      const p = item.ItemPrices.find((x) => Number(x.PriceList) === Number(priceListId));
      if (p && p.Price != null) price = Number(p.Price);
    }

    res.json({
      ok: true,
      itemCode: item.ItemCode,
      itemName: item.ItemName,
      uom: item.InventoryUOM || item.SalesUnit || "",
      warehouse: SAP_WAREHOUSE,
      stock,
      priceListName: SAP_PRICE_LIST,
      priceListId,
      price,
      raw: {
        QuantityOnStock: item.QuantityOnStock,
      },
    });
  } catch (e) {
    res.status(500).json({ ok: false, message: String(e.message || e) });
  }
});

/* ===========================
   STRIPE (opcional)
=========================== */
const stripe = STRIPE_SECRET_KEY ? new Stripe(STRIPE_SECRET_KEY) : null;

app.post("/api/pay/stripe/create-checkout-session", async (req, res) => {
  try {
    if (!stripe) {
      return res.status(400).send("Stripe no está configurado. Falta STRIPE_SECRET_KEY.");
    }

    const { customer, items, currency, success_url, cancel_url } = req.body;

    if (!items?.length) {
      return res.status(400).send("No hay items para pagar.");
    }

    const line_items = items.map((it) => ({
      quantity: Number(it.qty || 1),
      price_data: {
        currency: currency || "usd",
        unit_amount: Number(it.unit_amount),
        product_data: { name: `${it.name} (${it.sku})` },
      },
    }));

    const session = await stripe.checkout.sessions.create({
      mode: "payment",
      payment_method_types: ["card"],
      line_items,
      customer_email: customer?.mail || undefined,
      success_url,
      cancel_url,
      metadata: {
        nombre: customer?.nombre || "",
        telefono: customer?.tel || "",
        direccion: customer?.dir || "",
      },
    });

    res.json({ url: session.url });
  } catch (err) {
    console.error("❌ Stripe error:", err);
    res.status(500).send(err.message);
  }
});

/* ===========================
   START SERVER
=========================== */
if (missingSapEnv()) {
  console.warn("⚠️ Faltan variables de entorno SAP. Revisa Render > Environment.");
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("✅ Server listo en puerto", PORT));
