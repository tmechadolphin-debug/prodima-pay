import express from "express";
import cors from "cors";
import crypto from "crypto";
import { Pool } from "pg";

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

/** ✅ Supabase Postgres */
const DATABASE_URL = process.env.DATABASE_URL || "";
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || "";

/* ========= DB ========= */
const pool = DATABASE_URL
  ? new Pool({
      connectionString: DATABASE_URL,
      ssl: { rejectUnauthorized: false },
    })
  : null;

function mustHaveDb(res) {
  if (!pool) {
    res.status(500).json({
      ok: false,
      message: "DATABASE_URL no configurado en Render > Environment",
    });
    return false;
  }
  return true;
}

function sha256(x) {
  return crypto.createHash("sha256").update(String(x)).digest("hex");
}

/* ========= CORS ========= */
app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? "*" : [CORS_ORIGIN],
    methods: ["GET", "POST", "PATCH", "OPTIONS"],
    allowedHeaders: ["Content-Type", "x-admin-token"],
  })
);

/* ========= Helpers SAP ========= */
let SL_COOKIE = null;
let SL_COOKIE_TIME = 0;

/** Cache para PriceListNo (evita buscarlo para cada item) */
let PRICE_LIST_CACHE = { name: "", no: null, ts: 0 };
const PRICE_LIST_TTL_MS = 6 * 60 * 60 * 1000; // 6 horas

/** Cache corta para items (evita que refresh repita todo) */
const ITEM_CACHE = new Map(); // code -> { ts, data }
const ITEM_TTL_MS = 20 * 1000; // 20 segundos

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

/* ========= AUDIT ========= */
async function audit(req, eventType, username = null, details = {}) {
  try {
    if (!pool) return;

    const ip =
      req.headers["x-forwarded-for"]?.split(",")[0]?.trim() ||
      req.socket?.remoteAddress ||
      "";
    const userAgent = req.headers["user-agent"] || "";

    await pool.query(
      `insert into audit_events(event_type, username, ip, user_agent, details)
       values($1,$2,$3,$4,$5)`,
      [eventType, username, ip, userAgent, details]
    );
  } catch (e) {
    console.log("audit error:", e.message);
  }
}

/* ========= Admin Middleware ========= */
function requireAdmin(req, res, next) {
  const tok = req.headers["x-admin-token"];

  if (!ADMIN_TOKEN) {
    return res.status(500).json({
      ok: false,
      message: "ADMIN_TOKEN no configurado en Render > Environment",
    });
  }

  if (!tok || tok !== ADMIN_TOKEN) {
    return res.status(403).json({ ok: false, message: "Token admin inválido" });
  }

  next();
}

/* ========= API Health ========= */
app.get("/api/health", async (req, res) => {
  const dbOk = !!pool;
  res.json({
    ok: true,
    message: "✅ PRODIMA API activa",
    yappy: YAPPY_ALIAS,
    warehouse: SAP_WAREHOUSE,
    priceList: SAP_PRICE_LIST,
    db: dbOk ? "OK" : "OFF",
  });
});

/* =========================================================
   ✅ AUTH MERCADERISTA (login)
   POST /api/auth/login
   body: { username, pin }
========================================================= */
app.post("/api/auth/login", async (req, res) => {
  try {
    if (!mustHaveDb(res)) return;

    const username = String(req.body?.username || "")
      .trim()
      .toLowerCase();
    const pin = String(req.body?.pin || "").trim();

    if (!username || !pin) {
      return res
        .status(400)
        .json({ ok: false, message: "username y pin requeridos" });
    }

    const pinHash = sha256(pin);

    const r = await pool.query(
      `select username, full_name, is_active
       from users
       where username=$1 and pin_hash=$2
       limit 1`,
      [username, pinHash]
    );

    if (!r.rows.length) {
      await audit(req, "LOGIN_FAIL", username, {});
      return res.status(401).json({ ok: false, message: "Usuario o PIN inválido" });
    }

    const user = r.rows[0];
    if (!user.is_active) {
      await audit(req, "LOGIN_BLOCKED", username, {});
      return res.status(403).json({ ok: false, message: "Usuario desactivado" });
    }

    await audit(req, "LOGIN_OK", username, { fullName: user.full_name });

    return res.json({
      ok: true,
      user: {
        username: user.username,
        fullName: user.full_name,
      },
    });
  } catch (err) {
    console.error("❌ /api/auth/login error:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ ADMIN USERS
   GET /api/admin/users
   POST /api/admin/users
   PATCH /api/admin/users/:username
========================================================= */
app.get("/api/admin/users", requireAdmin, async (req, res) => {
  try {
    if (!mustHaveDb(res)) return;

    const r = await pool.query(
      `select id, username, full_name, is_active, created_at
       from users
       order by created_at desc`
    );

    return res.json({ ok: true, users: r.rows });
  } catch (err) {
    console.error("❌ /api/admin/users GET error:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

app.post("/api/admin/users", requireAdmin, async (req, res) => {
  try {
    if (!mustHaveDb(res)) return;

    const username = String(req.body?.username || "")
      .trim()
      .toLowerCase();
    const fullName = String(req.body?.fullName || "").trim();
    const pin = String(req.body?.pin || "").trim();

    if (!username || !pin) {
      return res
        .status(400)
        .json({ ok: false, message: "username y pin son requeridos" });
    }

    const pinHash = sha256(pin);

    const r = await pool.query(
      `insert into users(username, full_name, pin_hash, is_active)
       values($1,$2,$3,true)
       returning id, username, full_name, is_active, created_at`,
      [username, fullName, pinHash]
    );

    await audit(req, "ADMIN_CREATE_USER", "ADMIN", { username });

    return res.json({ ok: true, user: r.rows[0] });
  } catch (err) {
    console.error("❌ /api/admin/users POST error:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

app.patch("/api/admin/users/:username", requireAdmin, async (req, res) => {
  try {
    if (!mustHaveDb(res)) return;

    const username = String(req.params.username || "")
      .trim()
      .toLowerCase();
    const isActive = !!req.body?.isActive;

    const r = await pool.query(
      `update users set is_active=$1 where username=$2
       returning id, username, full_name, is_active, created_at`,
      [isActive, username]
    );

    await audit(req, "ADMIN_UPDATE_USER", "ADMIN", { username, isActive });

    return res.json({ ok: true, user: r.rows[0] || null });
  } catch (err) {
    console.error("❌ /api/admin/users PATCH error:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
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

  let itemFull;
  try {
    itemFull = await slFetch(
      `/Items('${encodeURIComponent(
        code
      )}')?$select=ItemCode,ItemName,SalesUnit,InventoryItem,ItemPrices,ItemWarehouseInfoCollection`
    );
  } catch {
    itemFull = await slFetch(`/Items('${encodeURIComponent(code)}')`);
  }

  const data = buildItemResponse(itemFull, code, priceListNo);

  ITEM_CACHE.set(code, { ts: now, data });
  return data;
}

/* =========================================================
   ✅ Endpoint 1 item
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
   ✅ Endpoint MULTI-ITEM RÁPIDO
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

    // Paralelo con límite
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
   ✅ CLIENTE: Business Partner
   GET /api/sap/customer/C12345
========================================================= */
app.get("/api/sap/customer/:code", async (req, res) => {
  try {
    if (missingSapEnv()) {
      return res.status(400).json({ ok: false, message: "Faltan variables SAP" });
    }

    const code = String(req.params.code || "").trim();
    if (!code) {
      return res.status(400).json({ ok: false, message: "CardCode vacío." });
    }

    const bp = await slFetch(
      `/BusinessPartners('${encodeURIComponent(
        code
      )}')?$select=CardCode,CardName,Phone1,Phone2,EmailAddress,Address,City,Country,ZipCode`
    );

    const addrParts = [bp.Address, bp.City, bp.ZipCode, bp.Country]
      .filter(Boolean)
      .join(", ");

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
     "lines":[ {"itemCode":"0110","qty":2}, ... ],
     "username":"mer01"   <-- (nuevo, opcional)
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

    // ✅ Nuevo: quién creó la cotización (desde el web)
    const username = String(req.body?.username || "").trim().toLowerCase();

    if (!cardCode)
      return res.status(400).json({ ok: false, message: "cardCode requerido." });
    if (!lines.length)
      return res.status(400).json({ ok: false, message: "lines requerido." });

    const DocumentLines = lines
      .map((l) => ({
        ItemCode: String(l.itemCode || "").trim(),
        Quantity: Number(l.qty || 0),
      }))
      .filter((x) => x.ItemCode && x.Quantity > 0);

    if (!DocumentLines.length) {
      return res
        .status(400)
        .json({ ok: false, message: "No hay líneas válidas (qty>0)." });
    }

    const today = new Date();
    const docDate = today.toISOString().slice(0, 10); // YYYY-MM-DD

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

    // ✅ Auditoría en Supabase
    await audit(req, "CREATE_QUOTE_OK", username || null, {
      cardCode,
      docEntry: created.DocEntry,
      docNum: created.DocNum,
      lines: DocumentLines.length,
    });

    return res.json({
      ok: true,
      message: "Cotización creada",
      docEntry: created.DocEntry,
      docNum: created.DocNum,
    });
  } catch (err) {
    console.error("❌ /api/sap/quote error:", err.message);

    // audit fail
    try {
      const username = String(req.body?.username || "").trim().toLowerCase();
      await audit(req, "CREATE_QUOTE_FAIL", username || null, {
        message: err.message,
      });
    } catch {}

    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* ========= START ========= */
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log("✅ Server listo en puerto", PORT));
