import express from "express";
import cors from "cors";
import Stripe from "stripe";
import path from "path";
import { fileURLToPath } from "url";

const app = express();

/* ===========================
   MIDDLEWARES
=========================== */

// ✅ Si tu frontend y backend están en el mismo dominio, CORS no es necesario.
// Pero si lo estás probando desde otro dominio, esto ayuda.
app.use(
  cors({
    origin: "*", // en producción puedes restringirlo a tu dominio
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type"],
  })
);

app.use(express.json({ limit: "2mb" }));

/* ===========================
   SERVIR SITIO ESTÁTICO (OPCIONAL)
   - Si subes tu website a Render junto a server.js,
     podrás abrir tu index y checkout desde el mismo dominio.
=========================== */
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ✅ Cambia "public" por tu carpeta real si quieres servir la web desde aquí
// Ejemplo: app.use(express.static(path.join(__dirname, "public")));
app.use(express.static(__dirname));

/* ===========================
   ENDPOINT DE PRUEBA
=========================== */
app.get("/api/health", (req, res) => {
  res.json({ ok: true, message: "✅ API PRODIMA activa" });
});

/* ===========================
   ✅ YAPPY (FUNCIONAL)
   Endpoint que usa tu checkout:
   POST /api/yappy-order
=========================== */

// ⚠️ Esto es memoria temporal (se borra si el server reinicia).
// Si quieres histórico, luego lo guardamos en DB (Render Postgres / Mongo / Sheets)
const yappyOrders = [];

app.post("/api/yappy-order", (req, res) => {
  try {
    const order = req.body;

    // Validación mínima
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
      yappyAlias: order?.yappyAlias || "@prodimasansae",
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

    // ✅ Aquí luego podemos:
    // - enviar email automático
    // - guardar en base de datos
    // - mandar a WhatsApp interno
    // - integrar con SAP

    return res.json({ ok: true, orderId });
  } catch (err) {
    console.error("❌ Error en /api/yappy-order:", err);
    return res.status(500).send("Error interno registrando pedido.");
  }
});

/* ===========================
   ✅ LISTAR PEDIDOS (opcional)
   Solo para que tú puedas verlos rápido:
=========================== */
app.get("/api/yappy-orders", (req, res) => {
  res.json({ ok: true, count: yappyOrders.length, orders: yappyOrders });
});

/* ===========================
   STRIPE (OPCIONAL)
   - Solo se activa si existe STRIPE_SECRET_KEY
=========================== */
const STRIPE_KEY = process.env.STRIPE_SECRET_KEY || "";
const stripe = STRIPE_KEY ? new Stripe(STRIPE_KEY) : null;

// Endpoint Stripe (si lo llegas a usar después)
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
   START SERVER
=========================== */
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("✅ Server listo en puerto", PORT));
