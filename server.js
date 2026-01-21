import express from "express";
import cors from "cors";

const app = express();
app.use(cors());
app.use(express.json({ limit: "2mb" }));

/**
 * =========================
 * CONFIG SAP (ENV VARS)
 * =========================
 * Render -> Environment Variables:
 *
 * SAP_SL_URL       = https://india.pa2.sap.topmanage.cloud/b1s/v1
 * SAP_COMPANY_DB   = C7357449_PRDMA_PRD
 * SAP_USERNAME     = adm-red@prodima.com.pa
 * SAP_PASSWORD     = (tu password)
 */
const SAP_SL_URL = process.env.SAP_SL_URL;
const SAP_COMPANY_DB = process.env.SAP_COMPANY_DB;
const SAP_USERNAME = process.env.SAP_USERNAME;
const SAP_PASSWORD = process.env.SAP_PASSWORD;

if (!SAP_SL_URL || !SAP_COMPANY_DB || !SAP_USERNAME || !SAP_PASSWORD) {
  console.warn("⚠️ Faltan variables de entorno SAP. Revisa Render > Environment.");
}

/**
 * =========================
 * COOKIE SESSION EN MEMORIA
 * =========================
 */
let sapCookie = "";

/**
 * Extrae la cookie de sesión desde el header set-cookie.
 * Normalmente incluye B1SESSION y/o ROUTEID.
 */
function extractCookie(headers) {
  return headers.get("set-cookie") || "";
}

async function sapLogin() {
  const payload = {
    CompanyDB: SAP_COMPANY_DB,
    UserName: SAP_USERNAME,
    Password: SAP_PASSWORD,
  };

  const resp = await fetch(`${SAP_SL_URL}/Login`, {
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
    console.warn("⚠️ Login OK pero no recibí set-cookie. Verifica el host/proxy de SAP.");
  }
  return true;
}

async function sapFetch(path, { method = "GET", body } = {}) {
  if (!sapCookie) {
    await sapLogin();
  }

  const headers = {
    "Content-Type": "application/json",
    Cookie: sapCookie,
  };

  let resp = await fetch(`${SAP_SL_URL}${path}`, {
    method,
    headers,
    body: body ? JSON.stringify(body) : undefined,
  });

  // Si expiró la sesión, reintenta
  if (resp.status === 401) {
    sapCookie = "";
    await sapLogin();

    resp = await fetch(`${SAP_SL_URL}${path}`, {
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
 * BUSCAR PriceListNo por nombre (exacto)
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
 * HEALTH
 * =========================
 */
app.get("/api/health", (req, res) => {
  res.json({ ok: true, message: "✅ PRODIMA API activa" });
});

/**
 * =========================
 * ITEMS + STOCK + PRICE
 * =========================
 * GET /api/sap/items?warehouse=01&pricelist=Lista%20Distribuidor&skus=E-0100,E-0101
 */
app.get("/api/sap/items", async (req, res) => {
  try {
    const warehouse = (req.query.warehouse || "01").toString();
    const priceListName = (req.query.pricelist || "Lista Distribuidor").toString();

    // Lista de SKUs opcional (separados por coma)
    const skusRaw = (req.query.skus || "").toString().trim();
    const skus = skusRaw ? skusRaw.split(",").map((x) => x.trim()).filter(Boolean) : [];

    const priceListNo = await getPriceListNoByName(priceListName);
    if (priceListNo === null) {
      return res.status(400).json({
        ok: false,
        message: `No encontré la lista de precios: "${priceListName}". Revisa el nombre exacto en SAP.`,
      });
    }

    let results = [];

    if (skus.length) {
      // Consulta por SKU para asegurar stock y precio por warehouse y lista
      for (const sku of skus) {
        const item = await sapFetch(
          `/Items('${encodeURIComponent(sku)}')?$select=ItemCode,ItemName,ItemPrices,ItemWarehouseInfoCollection&$expand=ItemPrices,ItemWarehouseInfoCollection`
        );

        // Stock en warehouse
        const wh = (item.ItemWarehouseInfoCollection || []).find(
          (w) => (w.WarehouseCode || "").toString() === warehouse
        );

        // Dependiendo de la versión puede venir InStock u OnHand
        const stock = Number(wh?.InStock ?? wh?.OnHand ?? 0);

        // Precio en lista
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
    } else {
      // Si no pasan SKUs, devuelve los primeros 50 como demo (sin precios/stock)
      const data = await sapFetch(`/Items?$select=ItemCode,ItemName&$orderby=ItemCode asc&$top=50`);
      results = (data.value || []).map((x) => ({
        sku: x.ItemCode,
        name: x.ItemName,
        price: 0,
        stock: 0,
        available: false,
      }));
    }

    return res.json({ ok: true, warehouse, priceListName, results });
  } catch (err) {
    console.error("❌ /api/sap/items error:", err);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/**
 * =========================
 * START
 * =========================
 */
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("✅ Server listo en puerto", PORT));
