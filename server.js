import express from "express";
import cors from "cors";
import Stripe from "stripe";

const app = express();
app.use(express.json({ limit: "2mb" }));

/* ======================================================
   ✅ ENV VARIABLES (Render > Environment)
====================================================== */
const SAP_BASE_URL = process.env.SAP_BASE_URL || ""; // Ej: https://india.pa2.sap.topmanage.cloud/b1s/v1
const SAP_COMPANYDB = process.env.SAP_COMPANYDB || "";
const SAP_USER = process.env.SAP_USER || "";
const SAP_PASS = process.env.SAP_PASS || "";
const SAP_WAREHOUSE = process.env.SAP_WAREHOUSE || "01";
const SAP_PRICE_LIST = process.env.SAP_PRICE_LIST || "Lista Distribuidor";

const YAPPY_ALIAS = process.env.YAPPY_ALIAS || "@prodimasansae";
const CORS_ORIGIN = process.env.CORS_ORIGIN || "*";

const STRIPE_KEY = process.env.STRIPE_SECRET_KEY || ""; // opcional
const stripe = STRIPE_KEY ? new Stripe(STRIPE_KEY) : null;

/* ======================================================
   ✅ CORS
====================================================== */
app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? "*" : [CORS_ORIGIN],
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type"],
  })
);

/* ======================================================
   ✅ HELPERS SAP SERVICE LAYER
====================================================== */
let SL_COOKIE = null; // cookie del service layer
let SL_COOKIE_TIME = 0;

// Cache simple para PriceListNo (evitar pedirlo cada vez)
let PRICE_LIST_NO_CACHE = null;
let PRICE_LIST_NO_CACHE_TIME = 0;

function missingSapEnv() {
  return !SAP_BASE_URL || !SAP_COMPANYDB || !SAP_USER || !SAP_PASS;
}

async function slLogin() {
  if (missingSapEnv()) {
    console.log("⚠️ Faltan variables SAP. Revisa Render > Environment.");
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

  // Guardamos solo lo importante: B1SESSION + ROUTEID (y otras si vinieran)
  SL_COOKIE = setCookie
    .split(",")
    .map((s) => s.split(";")[0])
    .join("; ");

  SL_COOKIE_TIME = Date.now();

  console.log("✅ Login SAP OK (cookie guardada)");
}

async function slFetch(path, options = {}) {
  // refrescar cookie si no existe o expiró (25 minutos)
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

  // si expiró cookie, reintenta 1 vez
  if (res.status === 401 || res.status === 403) {
    SL_COOKIE = null;
    await slLogin();

    const res2 = await fetch(`${SAP_BASE_URL}${path}`, {
      ...options,
      headers: {
        "Content-Type": "application/json",
        Cookie: SL_COOKIE,
        ...(options.headers || {}),
      },
    });

    const text2 = await res2.text();
    if (!res2.ok) throw new Error(`SAP error ${res2.status}: ${text2}`);
    return text2 ? JSON.parse(text2) : {};
  }

  if (!res.ok) {
    throw new Error(`SAP error ${res.status}: ${text}`);
  }

  return text ? JSON.parse(text) : {};
}

/* ======================================================
   ✅ HEALTH
====================================================== */
app.get("/api/health", (req, res) => {
  res.json({
    ok: true,
    message: "✅ PRODIMA API activa",
    yappy: YAPPY_ALIAS,
    warehouse: SAP_WAREHOUSE,
    priceList: SAP_PRICE_LIST,
  });
});

/* ======================================================
   ✅ PRICE LIST NO (por nombre)
====================================================== */
async function getPriceListNoByName(name) {
  // cache 10 minutos
  if (
    PRICE_LIST_NO_CACHE != null &&
    Date.now() - PRICE_LIST_NO_CACHE_TIME < 10 * 60 * 1000
  ) {
    return PRICE_LIST_NO_CACHE;
  }

  const safeName = name.replace(/'/g, "''");

  // Intento 1: PriceListName
  try {
    const r1 = await slFetch(
      `/PriceLists?$select=PriceListNo,PriceListName&$filter=PriceListName eq '${safeName}'`
    );
    if (r1?.value?.length) {
      PRICE_LIST_NO_CACHE = r1.value[0].PriceListNo;
      PRICE_LIST_NO_CACHE_TIME = Date.now();
      return PRICE_LIST_NO_CACHE;
    }
  } catch {}

  // Intento 2: ListName
  try {
    const r2 = await slFetch(
      `/PriceLists?$select=PriceListNo,ListName&$filter=ListName eq '${safeName}'`
    );
    if (r2?.value?.length) {
      PRICE_LIST_NO_CACHE = r2.value[0].PriceListNo;
      PRICE_LIST_NO_CACHE_TIME = Date.now();
      return PRICE_LIST_NO_CACHE;
    }
  } catch {}

  return null;
}

/* ======================================================
   ✅ SAP ITEM INDIVIDUAL
   GET /api/sap/item/0110
====================================================== */
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

    // 1) Item básico
    const item = await slFetch(
      `/Items('${encodeURIComponent(code)}')?$select=ItemCode,ItemName,SalesUnit,InventoryItem`
    );

    // 2) Stock por bodega (collection)
    let wh = null;
    try {
      const whRes = await slFetch(
        `/ItemWarehouseInfoCollection?$select=ItemCode,WarehouseCode,OnHand,InStock,Committed&$filter=ItemCode eq '${code}' and WarehouseCode eq '${SAP_WAREHOUSE}'`
      );
      if (whRes?.value?.length) wh = whRes.value[0];
    } catch {
      wh = null;
    }

    // 3) Precio por lista (ItemPrices)
    let price = null;
    const priceListNo = await getPriceListNoByName(SAP_PRICE_LIST);

    if (priceListNo !== null) {
      try {
        const pRes = await slFetch(
          `/ItemPrices?$select=ItemCode,PriceList,Price&$filter=ItemCode eq '${code}' and PriceList eq ${priceListNo}`
        );
        if (pRes?.value?.length) price = Number(pRes.value[0].Price);
      } catch {
        price = null;
      }
    }

    const onHand = wh?.OnHand ?? wh?.InStock ?? null;
    const committed = wh?.Committed ?? 0;
    const available = onHand !== null ? Number(onHand) - Number(committed) : null;

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

/* ======================================================
   ✅ SAP ITEMS BATCH (MUY RÁPIDO)
   GET /api/sap/items?codes=0110,0105,0124
   ✅ solo 2-3 llamadas totales a SAP (no 15 o 40)
====================================================== */
app.get("/api/sap/items", async (req, res) => {
  try {
    if (missingSapEnv()) {
      return res.status(400).json({
        ok: false,
        message: "Faltan variables SAP en Render > Environment",
      });
    }

    const codesRaw = String(req.query.codes || "").trim();
    if (!codesRaw) {
      return res.status(400).json({ ok: false, message: "Faltan códigos." });
    }

    const codes = codesRaw
      .split(",")
      .map((x) => x.trim())
      .filter(Boolean)
      .slice(0, 60);

    if (!codes.length) {
      return res.status(400).json({ ok: false, message: "Códigos inválidos." });
    }

    // OR filter: ItemCode eq '0110' or ItemCode eq '0105'
    const orFilter = codes.map((c) => `ItemCode eq '${c.replace(/'/g, "''")}'`).join(" or ");

    // OR para stock con bodega fija
    const orFilterWarehouse = codes
      .map((c) => `(ItemCode eq '${c.replace(/'/g, "''")}' and WarehouseCode eq '${SAP_WAREHOUSE}')`)
      .join(" or ");

    // PriceListNo una sola vez
    const priceListNo = await getPriceListNoByName(SAP_PRICE_LIST);

    // ✅ 1) Items (una llamada)
    const itemsRes = await slFetch(
      `/Items?$select=ItemCode,ItemName,SalesUnit,InventoryItem&$filter=${encodeURIComponent(orFilter)}`
    );

    // ✅ 2) Stock por bodega (una llamada)
    const stockRes = await slFetch(
      `/ItemWarehouseInfoCollection?$select=ItemCode,WarehouseCode,OnHand,InStock,Committed&$filter=${encodeURIComponent(
        orFilterWarehouse
      )}`
    );

    // ✅ 3) Precios por lista (una llamada)
    let pricesRes = { value: [] };
    if (priceListNo != null) {
      const orFilterPrices = codes.map((c) => `ItemCode eq '${c.replace(/'/g, "''")}'`).join(" or ");
      pricesRes = await slFetch(
        `/ItemPrices?$select=ItemCode,PriceList,Price&$filter=PriceList eq ${priceListNo} and (${encodeURIComponent(
          orFilterPrices
        )})`
      );
    }

    // Map base de items
    const itemsMap = {};
    (itemsRes?.value || []).forEach((it) => {
      itemsMap[it.ItemCode] = {
        code: it.ItemCode,
        name: it.ItemName,
        unit: it.SalesUnit || "",
        inventoryItem: it.InventoryItem,
        price: null,
        stock: { onHand: null, committed: 0, available: null, hasStock: null },
      };
    });

    // Map stock
    const stockMap = {};
    (stockRes?.value || []).forEach((wh) => {
      stockMap[wh.ItemCode] = wh;
    });

    // Map precios
    const priceMap = {};
    (pricesRes?.value || []).forEach((p) => {
      // PriceList debería ser el número de lista
      priceMap[p.ItemCode] = Number(p.Price);
    });

    // unir todo
    codes.forEach((code) => {
      if (!itemsMap[code]) {
        itemsMap[code] = {
          code,
          name: `Producto ${code}`,
          unit: "",
          inventoryItem: null,
          price: null,
          stock: { onHand: null, committed: 0, available: null, hasStock: null },
        };
      }

      const wh = stockMap[code];
      const onHand = wh?.OnHand ?? wh?.InStock ?? null;
      const committed = wh?.Committed ?? 0;
      const available = onHand != null ? Number(onHand) - Number(committed) : null;

      itemsMap[code].price = priceMap[code] ?? null;
      itemsMap[code].stock = {
        onHand,
        committed,
        available,
        hasStock: available != null ? available > 0 : null,
      };
    });

    return res.json({
      ok: true,
      warehouse: SAP_WAREHOUSE,
      priceList: SAP_PRICE_LIST,
      priceListNo,
      items: itemsMap,
    });
  } catch (err) {
    console.error("❌ /api/sap/items error:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* ======================================================
   ✅ YAPPY ORDER (PAGO CONTRA ENTREGA / VALIDACIÓN)
====================================================== */
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
      return res.status(400).send("Pedido incompleto: faltan datos del cliente.");
    }
    if (!order?.items?.length) {
      return res.status(400).send("Pedido incompleto: carrito vacío.");
    }

    const orderId = "YP-" + Date.now();

    const payload = {
      orderId,
      createdAt: new Date().toISOString(),
      paymentMethod: "Yappy",
      yappyAlias: order?.yappyAlias || YAPPY_ALIAS,
      amount: Number(order.amount || 0),
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

/* ======================================================
   ✅ STRIPE (OPCIONAL)
====================================================== */
app.post("/api/pay/stripe/create-checkout-session", async (req, res) => {
  try {
    if (!stripe) {
      return res
        .status(400)
        .send("Stripe no está configurado. Falta STRIPE_SECRET_KEY.");
    }

    const { customer, items, currency, success_url, cancel_url } = req.body;

    if (!items?.length) {
      return res.status(400).send("No hay items para pagar.");
    }

    const line_items = items.map((it) => ({
      quantity: Number(it.qty || 1),
      price_data: {
        currency: currency || "usd",
        unit_amount: Number(it.unit_amount), // en centavos
        product_data: {
          name: `${it.name} (${it.sku})`,
        },
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
    console.error("❌ Stripe error:", err.message);
    res.status(500).send(err.message);
  }
});

/* ======================================================
   ✅ START
====================================================== */
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log("✅ Server listo en puerto", PORT));
