import express from "express";
import cors from "cors";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";
import pg from "pg";

const { Pool } = pg;

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

/* ✅ DB (Supabase Postgres) */
const DATABASE_URL = process.env.DATABASE_URL || "";
const JWT_SECRET = process.env.JWT_SECRET || "CHANGE_ME_NOW_SUPER_SECRET";
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || "";

/* ========= CORS ========= */
app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? "*" : [CORS_ORIGIN],
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  })
);

/* ========= DB Pool ========= */
const pool = DATABASE_URL
  ? new Pool({
      connectionString: DATABASE_URL,
      ssl: { rejectUnauthorized: false },
    })
  : null;

async function dbQuery(sql, params = []) {
  if (!pool) throw new Error("DATABASE_URL no configurado en Render.");
  const r = await pool.query(sql, params);
  return r;
}

async function auditLog({ eventType, username = null, req = null, details = {} }) {
  try {
    const ip =
      req?.headers["x-forwarded-for"]?.split(",")[0]?.trim() ||
      req?.socket?.remoteAddress ||
      null;

    const ua = req?.headers["user-agent"] || null;

    await dbQuery(
      `insert into public.audit_events(event_type, username, ip, user_agent, details)
       values($1,$2,$3,$4,$5)`,
      [eventType, username, ip, ua, JSON.stringify(details || {})]
    );
  } catch (e) {
    console.log("⚠️ auditLog falló:", e.message);
  }
}

/* ========= Helpers SAP ========= */
let SL_COOKIE = null;
let SL_COOKIE_TIME = 0;

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

/* ========= AUTH ========= */
function signToken(user) {
  return jwt.sign(
    {
      username: user.username,
      fullName: user.full_name,
      uid: user.id,
    },
    JWT_SECRET,
    { expiresIn: "12h" }
  );
}

function authRequired(req, res, next) {
  const h = req.headers.authorization || "";
  const token = h.startsWith("Bearer ") ? h.slice(7) : null;
  if (!token) return res.status(401).json({ ok: false, message: "No autorizado (sin token)." });

  try {
    const payload = jwt.verify(token, JWT_SECRET);
    req.user = payload;
    return next();
  } catch (e) {
    return res.status(401).json({ ok: false, message: "Token inválido o expirado." });
  }
}

/* ========= API Health ========= */
app.get("/api/health", async (req, res) => {
  res.json({
    ok: true,
    message: "✅ PRODIMA API activa",
    yappy: YAPPY_ALIAS,
    warehouse: SAP_WAREHOUSE,
    priceList: SAP_PRICE_LIST,
    db: pool ? "OK" : "OFF",
  });
});

/* ========= AUTH: LOGIN ========= */
app.post("/api/auth/login", async (req, res) => {
  try {
    const username = String(req.body.username || "").trim().toLowerCase();
    const pin = String(req.body.pin || "").trim();

    if (!username || !pin) {
      await auditLog({ eventType: "LOGIN_FAIL", username, req, details: { reason: "missing_fields" } });
      return res.status(400).json({ ok: false, message: "Username y PIN son obligatorios." });
    }

    const r = await dbQuery(
      `select id, username, full_name, pin_hash, is_active
       from public.app_users
       where username=$1`,
      [username]
    );

    if (!r.rows.length) {
      await auditLog({ eventType: "LOGIN_FAIL", username, req, details: { reason: "user_not_found" } });
      return res.status(401).json({ ok: false, message: "Credenciales inválidas." });
    }

    const user = r.rows[0];
    if (!user.is_active) {
      await auditLog({ eventType: "LOGIN_FAIL", username, req, details: { reason: "inactive_user" } });
      return res.status(401).json({ ok: false, message: "Usuario inactivo." });
    }

    const ok = await bcrypt.compare(pin, user.pin_hash);
    if (!ok) {
      await auditLog({ eventType: "LOGIN_FAIL", username, req, details: { reason: "bad_pin" } });
      return res.status(401).json({ ok: false, message: "Credenciales inválidas." });
    }

    await dbQuery(`update public.app_users set last_login_at=now() where id=$1`, [user.id]);

    await auditLog({ eventType: "LOGIN_SUCCESS", username, req, details: { uid: user.id } });

    const token = signToken(user);
    return res.json({
      ok: true,
      token,
      user: { username: user.username, fullName: user.full_name },
    });
  } catch (err) {
    console.error("❌ /api/auth/login:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* ========= ADMIN: CREATE USER ========= */
app.post("/api/admin/users", async (req, res) => {
  try {
    if (!ADMIN_TOKEN) {
      return res.status(500).json({ ok: false, message: "ADMIN_TOKEN no configurado en Render." });
    }

    const auth = String(req.headers["x-admin-token"] || "").trim();
    if (auth !== ADMIN_TOKEN) {
      return res.status(401).json({ ok: false, message: "No autorizado (admin)." });
    }

    const username = String(req.body.username || "").trim().toLowerCase();
    const fullName = String(req.body.fullName || "").trim();
    const pin = String(req.body.pin || "").trim();

    if (!username || !fullName || !pin) {
      return res.status(400).json({ ok: false, message: "username, fullName y pin son obligatorios." });
    }

    const pinHash = await bcrypt.hash(pin, 10);

    const r = await dbQuery(
      `insert into public.app_users(username, full_name, pin_hash)
       values($1,$2,$3)
       on conflict (username) do update set full_name=excluded.full_name, pin_hash=excluded.pin_hash
       returning id, username, full_name, created_at`,
      [username, fullName, pinHash]
    );

    await auditLog({
      eventType: "ADMIN_CREATE_USER",
      username: "ADMIN",
      req,
      details: { created: username },
    });

    return res.json({ ok: true, user: r.rows[0] });
  } catch (err) {
    console.error("❌ /api/admin/users:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* ========= Obtener PriceListNo por nombre ========= */
async function getPriceListNoByName(name) {
  const safe = name.replace(/'/g, "''");

  try {
    const r1 = await slFetch(
      `/PriceLists?$select=PriceListNo,PriceListName&$filter=PriceListName eq '${safe}'`
    );
    if (r1?.value?.length) return r1.value[0].PriceListNo;
  } catch {}

  try {
    const r2 = await slFetch(
      `/PriceLists?$select=PriceListNo,ListName&$filter=ListName eq '${safe}'`
    );
    if (r2?.value?.length) return r2.value[0].PriceListNo;
  } catch {}

  return null;
}

/* =========================================================
   ✅ ITEM (puede ser protegido o público)
========================================================= */
app.get("/api/sap/item/:code", authRequired, async (req, res) => {
  try {
    if (missingSapEnv()) {
      return res.status(400).json({ ok: false, message: "Faltan variables SAP en Render > Environment" });
    }

    const code = String(req.params.code || "").trim();
    if (!code) return res.status(400).json({ ok: false, message: "ItemCode vacío." });

    const priceListNo = await getPriceListNoByName(SAP_PRICE_LIST);
    const itemFull = await slFetch(`/Items('${encodeURIComponent(code)}')`);

    const item = {
      ItemCode: itemFull.ItemCode,
      ItemName: itemFull.ItemName,
      SalesUnit: itemFull.SalesUnit,
      InventoryItem: itemFull.InventoryItem,
    };

    let price = null;
    if (priceListNo !== null && Array.isArray(itemFull.ItemPrices)) {
      const p = itemFull.ItemPrices.find((x) => Number(x.PriceList) === Number(priceListNo));
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

    return res.json({
      ok: true,
      item,
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

/* =========================================================
   ✅ CUSTOMER (protegido)
========================================================= */
app.get("/api/sap/customer/:cardCode", authRequired, async (req, res) => {
  try {
    if (missingSapEnv()) {
      return res.status(400).json({ ok: false, message: "Faltan variables SAP en Render > Environment" });
    }

    const cardCode = String(req.params.cardCode || "").trim();
    if (!cardCode) return res.status(400).json({ ok: false, message: "CardCode vacío." });

    const c = await slFetch(`/BusinessPartners('${encodeURIComponent(cardCode)}')`);

    // Campos útiles
    const customer = {
      CardCode: c.CardCode,
      CardName: c.CardName,
      Phone1: c.Phone1,
      Phone2: c.Phone2,
      EmailAddress: c.EmailAddress,
      Address: c.Address,
    };

    return res.json({ ok: true, customer });
  } catch (err) {
    console.error("❌ /api/sap/customer error:", err.message);
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ CREAR COTIZACIÓN (protegido)  ✅ IGNORA INVENTARIO
========================================================= */
app.post("/api/sap/quote", authRequired, async (req, res) => {
  try {
    if (missingSapEnv()) {
      return res.status(400).json({ ok: false, message: "Faltan variables SAP en Render > Environment" });
    }

    const cardCode = String(req.body.cardCode || "").trim();
    const comments = String(req.body.comments || "").trim();
    const lines = Array.isArray(req.body.lines) ? req.body.lines : [];

    if (!cardCode) return res.status(400).json({ ok: false, message: "cardCode vacío." });
    if (!lines.length) return res.status(400).json({ ok: false, message: "No hay líneas." });

    // ⚠️ Cotización en SAP: Quotations = /Quotations
    // Lineas: { ItemCode, Quantity }
    const doc = {
      CardCode: cardCode,
      Comments: comments,
      DocumentLines: lines.map((l) => ({
        ItemCode: String(l.itemCode || "").trim(),
        Quantity: Number(l.qty || 0),
      })),
    };

    // ✅ Crear en SAP
    const created = await slFetch(`/Quotations`, {
      method: "POST",
      body: JSON.stringify(doc),
    });

    // Traer DocNum con GET (a veces POST devuelve DocEntry)
    const docEntry = created?.DocEntry;
    let docNum = null;

    if (docEntry != null) {
      const q = await slFetch(`/Quotations(${docEntry})?$select=DocNum,DocEntry`);
      docNum = q?.DocNum ?? null;
    }

    await auditLog({
      eventType: "CREATE_QUOTE",
      username: req.user?.username || null,
      req,
      details: { cardCode, docEntry, docNum, linesCount: lines.length },
    });

    return res.json({
      ok: true,
      docEntry: docEntry ?? null,
      docNum: docNum ?? null,
      created,
    });
  } catch (err) {
    console.error("❌ /api/sap/quote:", err.message);

    await auditLog({
      eventType: "CREATE_QUOTE_FAIL",
      username: req.user?.username || null,
      req,
      details: { error: err.message },
    });

    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* ========= START ========= */
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log("✅ Server listo en puerto", PORT));
