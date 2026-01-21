import express from "express";
import cors from "cors";
import Stripe from "stripe";

const app = express();
app.use(cors());
app.use(express.json());

// ✅ Configura tu Stripe Secret Key como variable de entorno:
// Windows (PowerShell):  $env:STRIPE_SECRET_KEY="sk_live_..."
// Windows (CMD):        set STRIPE_SECRET_KEY=sk_live_...
// Linux/Mac:            export STRIPE_SECRET_KEY="sk_live_..."
const stripe = new Stripe(process.env.STRIPE_SECRET_KEY);

// Endpoint: crea una sesión de Stripe Checkout
app.post("/api/pay/stripe/create-checkout-session", async (req, res) => {
  try {
    const { customer, items, currency, success_url, cancel_url } = req.body;

    if (!items?.length) {
      return res.status(400).send("No hay items para pagar.");
    }

    // line_items para Stripe Checkout
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
    console.error(err);
    res.status(500).send(err.message);
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("✅ Server listo en puerto", PORT));
