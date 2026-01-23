import express from "express";
import cors from "cors";
import pg from "pg";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";

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

const DATABASE_URL = process.env.DATABASE_URL || "";
const ADMIN_USER = process.env.ADMIN_USER || "PRODIMA";
const ADMIN_PASS = process.env.ADMIN_PASS || "ADMINISTRADOR";
const JWT_SECRET = process.env.JWT_SECRET || "CHANGE_ME_PLEASE";

/* ========= CORS =========
   IMPORTANTE: permitir Authorization para el login de mercaderistas
*/
app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? "*" : [CORS_ORIGIN],
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  })
);

/* ========= DB (Supabase Postgres) ========= */
const { Pool } = pg;
const pool = DATABASE_URL
  ? new Pool({
      connectionString: DATABASE_URL,
      ssl: { rejectUnauthorized: false }, // Supabase requiere SSL
      max: 3,
    })
  : null;

async function dbQuery(text, params = []) {
  if (!pool) throw new Error("DATABASE_URL no está configurado");
  const r = await pool.query(text, params);
  return r;
}

/* ========= AUDIT ========= */
function getIP(req) {
  const xf = req.headers["x-forwarded-for"];
  if (xf) return String(xf).split(",")[0].trim();
  return req.socket?.remoteAddress || "";
}

async function audit(req, event_type, username = null, meta = {}) {
  try {
    const ip = getIP(req);
    const user_agent = String(req.headers["user-agent"] || "");
    await dbQuery(
      `insert into public.audit_events (event_type, username, ip, user_agent, meta)
       values ($1, $2, $3, $4, $5::jsonb)`,
      [event_type, username, ip, user_agent, JSON.stringify(meta || {})]
    );
  } catch (e) {
    // no rompemos el sistema si auditoría falla
    console.log("⚠️ audit error:", e.message);
  }
}

/* ========= AUTH MERCADERISTAS ========= */
function signMercToken(username) {
  return jwt.sign(
    { typ: "merc", username },
    JWT_SECRET,
    { expiresIn: "12h" }
  );
}

function authMerc(req, res, next) {
  try {
    const header = String(req.headers.authorization || "");
    const token = header.startsWith("Bearer ") ? header.slice(7) : "";
    if (!token) {
      return res.status(401).json({ ok: false, message: "Falta Authorization Bearer token" });
    }
    const decoded = jwt.verify(token, JWT_SECRET);
    if (!decoded || decoded.typ !== "merc" || !decoded.username) {
      return res.status(401).json({ ok: false, message: "Token inválido" });
    }
    req.mercUser = decoded.username;
    next();
  } catch (e) {
    return res.status(401).json({ ok: false, message: "Token inválido o expirado" });
  }
}

/* ========= AUTH ADMIN ========= */
function signAdminToken() {
  return jwt.sign({ typ: "admin" }, JWT_SECRET, { expiresIn: "2h" });
}

function authAdmin(req, res, next) {
  try {
    const header = String(req.headers.authorization || "");
    const token = header.startsWith("Bearer ") ? header.slice(7) : "";
    if (!token) return res.status(401).json({ ok: false, message: "Falta Bearer token admin" });

    const decoded = jwt.verify(token, JWT_SECRET);
    if (!decoded || decoded.typ !== "admin") {
      return res.status(401).json({ ok: false, message: "Token admin inválido" });
    }
    next();
  } catch {
    return res.status(401).json({ ok: false, message: "Token admin inválido o expirado" });
  }
}

/* ========= SAP Helpers ========= */
let SL_COOKIE = null;
let SL_COOKIE_TIME = 0;

let PRICE_LIST_CACHE = { name: "", no: null, ts: 0 };
const PRICE_LIST_TTL_MS = 6 * 60 * 60 * 1000;

const ITEM_CACHE = new Map();
const ITEM_TTL_MS = 20 * 1000;

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

  try {
    const r1 = await slFetch(
      `/PriceLists?$select=PriceListNo,PriceListName&$filter=PriceListName eq '${safe}'`
    );
    if (r1?.value?.length) no = r1.value[0].PriceListNo;
  } catch {}

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

function buildItemResponse(itemFull, code, priceListNo) {
  const item = {
    ItemCode: itemFull.ItemCode ?? code,
    ItemName: itemFull.ItemName ?? `Producto ${code}`,
    SalesUnit: itemFull.SalesUnit ?? "",
    InventoryItem: itemFull.InventoryItem ?? null,
  };

  let price = null;
  if (priceListNo !== null && Array.isArray(itemFull.ItemPrices)) {
    const p = itemFull.ItemPrices.find(
      (x) => Number(x.PriceList) === Number(priceListNo)
    );
    if (p && p.Price != null) price = Number(p.Price);
  }

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

async function getOneItem(code, priceListNo) {
  const now = Date.now();
  const cached = ITEM_CACHE.get(code);
  if (cached && now - cached.ts < ITEM_TTL_MS) return cached.data;

  let itemFull;
  try {
    itemFull = await slFetch(
      `/Items('${encodeURIComponent(code)}')?$select=ItemCode,ItemName,SalesUnit,InventoryItem,ItemPrices,ItemWarehouseInfoCollection`
    );
  } catch {
    itemFull = await slFetch(`/Items('${encodeURIComponent(code)}')`);
  }

  const data = buildItemResponse(itemFull, code, priceListNo);
  ITEM_CACHE.set(code, { ts: now, data });
  return data;
}

/* =========================================================
   HEALTH
========================================================= */
app.get("/api/health", (req, res) => {
  res.json({
    ok: true,
    message: "✅ PRODIMA API activa",
    yappy: YAPPY_ALIAS,
    warehouse: SAP_WAREHOUSE,
    priceList: SAP_PRICE_LIST,
    db: !!DATABASE_URL,
  });
});

/* =========================================================
   ✅ ADMIN LOGIN (usuario fijo PRODIMA / ADMINISTRADOR)
   POST /api/admin/login
   body: { "user":"PRODIMA", "pass":"ADMINISTRADOR" }
========================================================= */
app.post("/api/admin/login", async (req, res) => {
  try {
    const user = String(req.body?.user || "");
    const pass = String(req.body?.pass || "");

    if (user !== ADMIN_USER || pass !== ADMIN_PASS) {
      await audit(req, "ADMIN_LOGIN_FAIL", user, {});
      return res.status(401).json({ ok: false, message: "Credenciales admin inválidas" });
    }

    await audit(req, "ADMIN_LOGIN_OK", user, {});
    const token = signAdminToken();
    return res.json({ ok: true, token });
  } catch (e) {
    return res.status(500).json({ ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ ADMIN: LISTAR USUARIOS
   GET /api/admin/users
========================================================= */
app.get("/api/admin/users", authAdmin, async (req, res) => {
  try {
    const r = await dbQuery(
      `select id, username, full_name, is_active, created_at
       from public.merc_users
       order by created_at desc`
    );
    return res.json({ ok: true, users: r.rows });
  } catch (e) {
    return res.status(500).json({ ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ ADMIN: CREAR USUARIO MERCADERISTA
   POST /api/admin/users
   body: { "username":"mer01", "fullName":"Mercaderista 01", "pin":"1234" }
========================================================= */
app.post("/api/admin/users", authAdmin, async (req, res) => {
  try {
    const username = String(req.body?.username || "").trim().toLowerCase();
    const fullName = String(req.body?.fullName || "").trim();
    const pin = String(req.body?.pin || "").trim();

    if (!username || !fullName || !pin) {
      return res.status(400).json({ ok: false, message: "Faltan campos: username, fullName, pin" });
    }
    if (pin.length < 4) {
      return res.status(400).json({ ok: false, message: "PIN mínimo 4 dígitos" });
    }

    const pin_hash = await bcrypt.hash(pin, 10);

    await dbQuery(
      `insert into public.merc_users (username, full_name, pin_hash)
       values ($1, $2, $3)`,
      [username, fullName, pin_hash]
    );

    await audit(req, "ADMIN_CREATE_USER", username, { fullName });

    return res.json({ ok: true, message: "Usuario creado" });
  } catch (e) {
    // username duplicado
    if (String(e.message || "").includes("duplicate key")) {
      return res.status(409).json({ ok: false, message: "Ese username ya existe" });
    }
    return res.status(500).json({ ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ ADMIN: ACTIVAR/DESACTIVAR
   POST /api/admin/users/:username/toggle
========================================================= */
app.post("/api/admin/users/:username/toggle", authAdmin, async (req, res) => {
  try {
    const u = String(req.params.username || "").trim().toLowerCase();
    if (!u) return res.status(400).json({ ok: false, message: "username requerido" });

    const r = await dbQuery(
      `update public.merc_users
       set is_active = not is_active
       where username = $1
       returning username, is_active`,
      [u]
    );

    if (!r.rowCount) return res.status(404).json({ ok: false, message: "No existe ese usuario" });

    await audit(req, "ADMIN_TOGGLE_USER", u, { is_active: r.rows[0].is_active });
    return res.json({ ok: true, user: r.rows[0] });
  } catch (e) {
    return res.status(500).json({ ok: false, message: e.message });
  }
});

/* =========================================================
   ✅ MERCADERISTA LOGIN
   POST /api/auth/login
   body: { "username":"mer01", "pin":"1234" }
========================================================= */
app.post("/api/auth/login", async (req, res) => {
  try {
    const username = String(req.body?.username || "").trim().toLowerCase();
    const pin = String(req.body?.pin || "").trim();

    if (!username || !pin) {
      await audit(req, "LOGIN_FAIL", username, { reason: "missing_fields" });
      return res.status(400).json({ ok: false, message: "username y pin requeridos" });
    }

    const r = await dbQuery(
      `select username, full_name, pin_hash, is_active
       from public.merc_users
       where username = $1
       limit 1`,
      [username]
    );

    if (!r.rowCount) {
      await audit(req, "LOGIN_FAIL", username, { reason: "not_found" });
      return res.status(401).json({ ok: false, message: "Usuario o PIN incorrecto" });
    }

    const user = r.rows[0];
    if (!user.is_active) {
      await audit(req, "LOGIN_FAIL", username, { reason: "inactive" });
      return res.status(403).json({ ok: false, message: "Usuario inactivo" });
    }

    const ok = await bcrypt.compare(pin, user.pin_hash);
    if (!ok) {
      await audit(req, "LOGIN_FAIL", username, { reason: "bad_pin" });
      return res.status(401).json({ ok: false, message: "Usuario o PIN incorrecto" });
    }

    await audit(req, "LOGIN_OK", username, { fullName: user.full_name });

    const token = signMercToken(username);

    return res.json({
      ok: true,
      token,
      user: { username: user.username, fullName: user.full_name },
    });
  } catch (e) {
    return res.status(500).json({ ok: false, message: e.message });
  }
});

/* =========================================================
   SAP: ITEM
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
    if (!code) return res.status(400).json({ ok: false, message: "ItemCode vacío." });

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
   SAP: MULTI ITEMS
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

    if (!codes.length) return res.status(400).json({ ok: false, message: "codes vacío" });

    const priceListNo = await getPriceListNoByNameCached(SAP_PRICE_LIST);

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
   SAP: CUSTOMER
========================================================= */
app.get("/api/sap/customer/:code", async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const code = String(req.params.code || "").trim();
    if (!code) return res.status(400).json({ ok: false, message: "CardCode vacío." });

    const bp = await slFetch(
      `/BusinessPartners('${encodeURIComponent(code)}')?$select=CardCode,CardName,Phone1,Phone2,EmailAddress,Address,City,Country,ZipCode`
    );

    const addrParts = [bp.Address, bp.City, bp.ZipCode, bp.Country].filter(Boolean).join(", ");

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
   ✅ CREAR COTIZACIÓN (PROTEGIDA POR LOGIN)
   POST /api/sap/quote
========================================================= */
app.post("/api/sap/quote", authMerc, async (req, res) => {
  try {
    if (missingSapEnv()) return res.status(400).json({ ok: false, message: "Faltan variables SAP" });

    const cardCode = String(req.body?.cardCode || "").trim();
    const comments = String(req.body?.comments || "").trim();
    const lines = Array.isArray(req.body?.lines) ? req.body.lines : [];

    if (!cardCode) return res.status(400).json({ ok: false, message: "cardCode requerido." });
    if (!lines.length) return res.status(400).json({ ok: false, message: "lines requerido." });

    const DocumentLines = lines
      .map((l) => ({
        ItemCode: String(l.itemCode || "").trim(),
        Quantity: Number(l.qty || 0),
      }))
      .filter((x) => x.ItemCode && x.Quantity > 0);

    if (!DocumentLines.length) {
      return res.status(400).json({ ok: false, message: "No hay líneas válidas (qty>0)." });
    }

    const today = new Date();
    const docDate = today.toISOString().slice(0, 10);

    // ✅ Nota: Cotización = NO valida stock como un Delivery/Invoice
    // Igual, SAP puede fallar si ItemCode no existe o está inactivo.
    const payload = {
      CardCode: cardCode,
      DocDate: docDate,
      DocDueDate: docDate,
      Comments: comments
        ? `[WEB PEDIDOS][${req.mercUser}] ${comments}`
        : `[WEB PEDIDOS][${req.mercUser}] Cotización mercaderista`,
      JournalMemo: "Cotización web mercaderistas",
      DocumentLines,
    };

    const created = await slFetch(`/Quotations`, {
      method: "POST",
      body: JSON.stringify(payload),
    });

    await audit(req, "CREATE_QUOTE", req.mercUser, {
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
    await audit(req, "CREATE_QUOTE_FAIL", req.mercUser || null, { error: err.message });
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* ========= START ========= */
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log("✅ Server listo en puerto", PORT));
