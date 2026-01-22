import express from "express";
import cors from "cors";
import Stripe from "stripe";

const app = express();

/* ===========================
   ENV (Render)
=========================== */
const PORT = process.env.PORT || 3000;

const CORS_ORIGIN = process.env.CORS_ORIGIN || "*"; // ej: https://prodima.com.pa
const YAPPY_ALIAS = process.env.YAPPY_ALIAS || "@prodimasansae";

// SAP (Service Layer)
const SAP_BASE_URL  = (process.env.SAP_BASE_URL || "").replace(/\/+$/, ""); // ej: https://.../b1s/v1
const SAP_COMPANYDB = process.env.SAP_COMPANYDB || "";
const SAP_USER      = process.env.SAP_USER || "";
const SAP_PASS      = process.env.SAP_PASS || "";
const SAP_WAREHOUSE = process.env.SAP_WAREHOUSE || "01"; // ej: 01
const SAP_PRICE_LIST_NAME = process.env.SAP_PRICE_LIST || "Lista Distribuidor";

// Stripe (opcional)
const STRIPE_KEY = process.env.STRIPE_SECRET_KEY || "";
const stripe = STRIPE_KEY ? new Stripe(STRIPE_KEY) : null;

/* ===========================
   MIDDLEWARES
=========================== */
app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? "*" : [CORS_ORIGIN],
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
    priceList: SAP_PRICE_LIST_NAME,
  });
});

/* =========================================================
   ✅ YAPPY (REGISTRO DE ORDEN)
   IMPORTANTE:
   - Yappy NO ofrece (públicamente) una API para “confirmación automática”
     al estilo Stripe, por lo tanto aquí registramos la orden
     y luego el equipo valida el pago.
========================================================= */
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

    return res.json({ ok: true, orderId });
  } catch (err) {
    console.error("❌ Error en /api/yappy-order:", err);
    return res.status(500).send("Error interno registrando pedido.");
  }
});

app.get("/api/yappy-orders", (req, res) => {
  res.json({ ok: true, count: yappyOrders.length, orders: yappyOrders });
});

/* =========================================================
   ✅ SAP BUSINESS ONE (Service Layer)
   /api/sap/item/:code  -> devuelve precio + stock
========================================================= */

// Cache simple de cookie de sesión
let sapSession = { cookie: "", expiresAt: 0 };

function missingSapEnv() {
  return !SAP_BASE_URL || !SAP_COMPANYDB || !SAP_USER || !SAP_PASS;
}

async function sapLogin() {
  if (missingSapEnv()) return null;

  // Reusa sesión por ~25 minutos
  const now = Date.now();
  if (sapSession.cookie && now < sapSession.expiresAt) return sapSession.cookie;

  const loginUrl = `${SAP_BASE_URL}/Login`;
  const body = {
    CompanyDB: SAP_COMPANYDB,
    UserName: SAP_USER,
    Password: SAP_PASS,
  };

  const r = await fetch(loginUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  if (!r.ok) {
    const txt = await r.text();
    throw new Error(`SAP Login error ${r.status}: ${txt}`);
  }

  // Tomamos SOLO la cookie B1SESSION (suficiente para llamadas)
  const setCookie = r.headers.get("set-cookie") || "";
  const b1session = setCookie.split(",").find(x => x.includes("B1SESSION=")) || setCookie;
  const cookie = b1session.split(";")[0].trim();

  sapSession = { cookie, expiresAt: Date.now() + 25 * 60 * 1000 };
  return cookie;
}

async function sapGet(path) {
  const cookie = await sapLogin();
  if (!cookie) throw new Error("Faltan variables de entorno SAP.");

  const url = `${SAP_BASE_URL}${path.startsWith("/") ? "" : "/"}${path}`;

  const r = await fetch(url, {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      Cookie: cookie,
    },
  });

  const txt = await r.text();
  if (!r.ok) {
    throw new Error(`SAP error ${r.status}: ${txt}`);
  }

  try {
    return JSON.parse(txt);
  } catch {
    return txt;
  }
}

async function getPriceListNumberByName(priceListName) {
  const name = priceListName.replace(/'/g, "''"); // escapar
  const data = await sapGet(`/PriceLists?$filter=PriceListName eq '${name}'&$select=PriceListNo,PriceListName,Currency`);

  const row = data?.value?.[0];
  if (!row) return null;

  const listNum = row.PriceListNo ?? row.ListNum ?? null;
  return {
    listNum,
    listName: row.PriceListName,
    currency: row.Currency || null,
  };
}

async function getItemPriceByList(itemCode, listNum) {
  // ✅ Evita $expand ItemPrices (en tu SAP falla con error 201)
  try {
    const code = itemCode.replace(/'/g, "''");
    const q = `/ItemPrices?$filter=ItemCode eq '${code}' and PriceList eq ${Number(listNum)}&$select=ItemCode,PriceList,Price,Currency`;
    const data = await sapGet(q);
    const row = data?.value?.[0];
    if (!row) return null;
    return {
      value: Number(row.Price),
      currency: row.Currency || null,
    };
  } catch (e) {
    console.warn("⚠️ No se pudo obtener precio por ItemPrices:", e.message);
    return null;
  }
}

async function getItemStock(itemCode, warehouseCode) {
  const code = itemCode.replace(/'/g, "''");
  const data = await sapGet(
    `/Items('${code}')/ItemWarehouseInfoCollection?$select=WarehouseCode,InStock,Committed,Ordered`
  );
  const rows = data?.value || [];
  const wh = rows.find(r => String(r.WarehouseCode).trim() === String(warehouseCode).trim());
  if (!wh) return null;

  const inStock = Number(wh.InStock || 0);
  const committed = Number(wh.Committed || 0);
  const ordered = Number(wh.Ordered || 0);
  const available = inStock - committed + ordered;

  return { warehouse: warehouseCode, inStock, committed, ordered, available };
}

app.get("/api/sap/item/:code", async (req, res) => {
  try {
    if (missingSapEnv()) {
      return res.status(400).json({
        ok: false,
        message: "Faltan variables de entorno SAP. Revisa Render > Environment.",
      });
    }

    const itemCode = String(req.params.code || "").trim();
    if (!itemCode) {
      return res.status(400).json({ ok: false, message: "ItemCode requerido." });
    }

    // Info básica del ítem
    const item = await sapGet(`/Items('${itemCode.replace(/'/g, "''")}')?$select=ItemCode,ItemName,SalesUnit`);
    const basic = {
      code: item.ItemCode,
      name: item.ItemName,
      uom: item.SalesUnit || null,
    };

    // Stock por warehouse
    const stock = await getItemStock(itemCode, SAP_WAREHOUSE);

    // Precio por lista (por nombre)
    const pl = await getPriceListNumberByName(SAP_PRICE_LIST_NAME);
    let price = null;
    if (pl?.listNum != null) {
      price = await getItemPriceByList(itemCode, pl.listNum);
    }

    return res.json({
      ok: true,
      item: basic,
      warehouse: SAP_WAREHOUSE,
      stock,
      priceList: pl || null,
      price, // puede ser null si no hay soporte
    });
  } catch (err) {
    console.error("❌ /api/sap/item error:", err);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   STRIPE (OPCIONAL)
========================================================= */
app.post("/api/pay/stripe/create-checkout-session", async (req, res) => {
  try {
    if (!stripe) {
      return res.status(400).send("Stripe no está configurado. Falta STRIPE_SECRET_KEY.");
    }

    const { customer, items, currency, success_url, cancel_url } = req.body;
    if (!items?.length) return res.status(400).send("No hay items para pagar.");

    const line_items = items.map((it) => ({
      quantity: Number(it.qty || 1),
      price_data: {
        currency: currency || "usd",
        unit_amount: Number(it.unit_amount), // en centavos
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
  if (missingSapEnv()) {
    console.warn("⚠️ Faltan variables de entorno SAP. Revisa Render > Environment.");
  }
  console.log("✅ Server listo en puerto", PORT);
});
