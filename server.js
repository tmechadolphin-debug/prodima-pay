import express from "express";
import cors from "cors";
import Stripe from "stripe";
import path from "path";
import { fileURLToPath } from "url";

/**
 * PRODIMA PAY API (Render)
 * - Health
 * - SAP B1 Service Layer: Item (stock + precio por lista)
 * - Yappy: registrar pedido (sin pasarela oficial todavía)
 * - Stripe: opcional (solo si existe STRIPE_SECRET_KEY)
 */

const app = express();

/* ===========================
   ENV
=========================== */
const PORT = process.env.PORT || 3000;

const CORS_ORIGIN = process.env.CORS_ORIGIN || "*";

const SAP_BASE_URL   = (process.env.SAP_BASE_URL || "").replace(/\/+$/, ""); // sin trailing /
const SAP_COMPANYDB  = process.env.SAP_COMPANYDB || "";
const SAP_USER       = process.env.SAP_USER || "";
const SAP_PASS       = process.env.SAP_PASS || "";
const SAP_WAREHOUSE  = process.env.SAP_WAREHOUSE || "01";
const SAP_PRICE_LIST = process.env.SAP_PRICE_LIST || "Lista Distribuidor";

const YAPPY_ALIAS    = process.env.YAPPY_ALIAS || "@prodimasansae";

// Stripe opcional
const STRIPE_KEY = process.env.STRIPE_SECRET_KEY || "";
const stripe = STRIPE_KEY ? new Stripe(STRIPE_KEY) : null;

/* ===========================
   MIDDLEWARES
=========================== */
app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? "*" : CORS_ORIGIN.split(",").map(s => s.trim()),
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type"],
  })
);

app.use(express.json({ limit: "2mb" }));

/* ===========================
   STATIC (opcional)
   - Si en algún momento montas el frontend aquí mismo,
     Render puede servirlo desde el mismo dominio.
=========================== */
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
app.use(express.static(__dirname));

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
   ✅ YAPPY (registro de pedido)
   Nota: no existe confirmación automática sin API oficial.
=========================== */
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
      yappyAlias: order?.yappyAlias || YAPPY_ALIAS,
      amount: Number(order.amount),
      reference: order?.reference || "", // opcional
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
   ✅ SAP B1 Service Layer helpers
   - Mantiene cookie en memoria para no loguearse en cada request
=========================== */

let sapSession = {
  cookie: "",
  createdAt: 0,
};

let cachedPriceList = {
  name: "",
  listNum: null,
  cachedAt: 0,
};

// Renueva login cada ~25 min (la sesión suele durar más, pero así es seguro)
const SAP_SESSION_TTL_MS = 25 * 60 * 1000;

function haveSapEnv() {
  return Boolean(SAP_BASE_URL && SAP_COMPANYDB && SAP_USER && SAP_PASS);
}

async function sapLoginIfNeeded() {
  if (!haveSapEnv()) {
    throw new Error("Faltan variables SAP (SAP_BASE_URL, SAP_COMPANYDB, SAP_USER, SAP_PASS).");
  }

  const now = Date.now();
  if (sapSession.cookie && (now - sapSession.createdAt) < SAP_SESSION_TTL_MS) {
    return sapSession.cookie;
  }

  const loginUrl = `${SAP_BASE_URL}/Login`;

  const resp = await fetch(loginUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      CompanyDB: SAP_COMPANYDB,
      UserName: SAP_USER,
      Password: SAP_PASS,
    }),
  });

  if (!resp.ok) {
    const t = await resp.text();
    throw new Error(`SAP login error ${resp.status}: ${t}`);
  }

  // Node/undici: getSetCookie() devuelve array de Set-Cookie
  const setCookies = resp.headers.getSetCookie?.() || [];
  const cookie = setCookies
    .map((c) => c.split(";")[0])
    .filter(Boolean)
    .join("; ");

  if (!cookie) {
    throw new Error("No se recibieron cookies de SAP (B1SESSION/ROUTEID).");
  }

  sapSession.cookie = cookie;
  sapSession.createdAt = now;

  return cookie;
}

async function sapFetch(pathWithQuery) {
  const cookie = await sapLoginIfNeeded();

  const url = `${SAP_BASE_URL}${pathWithQuery.startsWith("/") ? "" : "/"}${pathWithQuery}`;

  const resp = await fetch(url, {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      "Cookie": cookie,
    },
  });

  if (!resp.ok) {
    const t = await resp.text();
    throw new Error(`SAP error ${resp.status}: ${t}`);
  }

  return resp.json();
}

function escapeODataString(s) {
  // OData usa comillas simples dentro de '...': duplicar
  return String(s).replace(/'/g, "''");
}

async function getPriceListNumByName(listName) {
  const now = Date.now();

  // cache 1 hora
  if (
    cachedPriceList.name === listName &&
    cachedPriceList.listNum !== null &&
    (now - cachedPriceList.cachedAt) < (60 * 60 * 1000)
  ) {
    return cachedPriceList.listNum;
  }

  const safeName = escapeODataString(listName);
  const filter = encodeURIComponent(`ListName eq '${safeName}'`);
  const data = await sapFetch(`/PriceLists?$select=ListNum,ListName&$filter=${filter}`);

  const row = data?.value?.[0];
  if (!row) {
    throw new Error(`No encontré la lista de precios en SAP: "${listName}"`);
  }

  cachedPriceList = {
    name: listName,
    listNum: Number(row.ListNum),
    cachedAt: now,
  };

  return cachedPriceList.listNum;
}

/* ===========================
   ✅ SAP: Item (precio + stock)
   GET /api/sap/item/0110
=========================== */
app.get("/api/sap/item/:code", async (req, res) => {
  try {
    if (!haveSapEnv()) {
      return res.status(400).json({
        ok: false,
        message: "Faltan variables de entorno SAP. Revisa Render > Environment.",
      });
    }

    const code = String(req.params.code || "").trim();
    if (!code) return res.status(400).json({ ok: false, message: "ItemCode inválido." });

    // 1) Traemos el item completo (SIN $expand) para evitar errores de navegación
    const item = await sapFetch(`/Items('${encodeURIComponent(code)}')`);

    // 2) Sacamos precio por lista
    let price = null;
    try {
      const listNum = await getPriceListNumByName(SAP_PRICE_LIST);

      if (Array.isArray(item?.ItemPrices)) {
        const pRow = item.ItemPrices.find((p) => Number(p.PriceList) === Number(listNum));
        if (pRow && pRow.Price !== undefined) price = Number(pRow.Price);
      }
    } catch (e) {
      // Si falla lista/precio, igual devolvemos item + stock
      console.warn("⚠️ No pude obtener precio por lista:", e.message);
    }

    // 3) Sacamos stock por bodega
    let stock = null;
    if (Array.isArray(item?.ItemWarehouseInfoCollection)) {
      const w = item.ItemWarehouseInfoCollection.find((x) => String(x.WarehouseCode) === String(SAP_WAREHOUSE));
      if (w) stock = Number(w.InStock ?? w.OnHand ?? w.InStockQuantity ?? 0);
    }

    return res.json({
      ok: true,
      item: {
        ItemCode: item.ItemCode,
        ItemName: item.ItemName,
        UoMGroupEntry: item.UoMGroupEntry,
      },
      warehouse: SAP_WAREHOUSE,
      priceList: SAP_PRICE_LIST,
      price,
      stock,
      raw: {
        // Campos útiles para debug (puedes eliminar luego)
        hasItemPrices: Array.isArray(item?.ItemPrices),
        hasWarehouseInfo: Array.isArray(item?.ItemWarehouseInfoCollection),
      },
    });
  } catch (err) {
    console.error("❌ /api/sap/item/:code", err);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* ===========================
   ✅ SAP: buscar items por texto (opcional)
   GET /api/sap/search?q=cloro
=========================== */
app.get("/api/sap/search", async (req, res) => {
  try {
    const q = String(req.query.q || "").trim();
    if (!q) return res.status(400).json({ ok: false, message: "Falta query ?q=" });

    // Búsqueda simple por ItemName
    const safe = escapeODataString(q);
    const filter = encodeURIComponent(`contains(ItemName,'${safe}')`);
    const data = await sapFetch(`/Items?$select=ItemCode,ItemName&$filter=${filter}&$top=25`);

    return res.json({ ok: true, count: data?.value?.length || 0, items: data?.value || [] });
  } catch (err) {
    console.error("❌ /api/sap/search", err);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* ===========================
   STRIPE (opcional)
=========================== */
app.post("/api/pay/stripe/create-checkout-session", async (req, res) => {
  try {
    if (!stripe) return res.status(400).send("Stripe no está configurado (falta STRIPE_SECRET_KEY).");

    const { customer, items, currency, success_url, cancel_url } = req.body;
    if (!items?.length) return res.status(400).send("No hay items para pagar.");

    const line_items = items.map((it) => ({
      quantity: Number(it.qty || 1),
      price_data: {
        currency: currency || "usd",
        unit_amount: Number(it.unit_amount), // centavos
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
   START
=========================== */
app.listen(PORT, () => {
  if (!haveSapEnv()) {
    console.warn("⚠️ Faltan variables de entorno SAP. Revisa Render > Environment.");
  }
  console.log("✅ Server listo en puerto", PORT);
});
