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

/* === Seguridad Login/Admin === */
const DATABASE_URL = process.env.DATABASE_URL || "";
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || ""; // <-- este lo creas en Render
const SESSION_TTL_HOURS = Number(process.env.SESSION_TTL_HOURS || 12);

/* ========= CORS ========= */
app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? "*" : [CORS_ORIGIN],
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization", "x-admin-token"],
  })
);

/* ========= DB ========= */
let pool = null;

function dbEnabled() {
  return Boolean(DATABASE_URL);
}

function getPool() {
  if (!pool) {
    pool = new Pool({
      connectionString: DATABASE_URL,
      ssl: { rejectUnauthorized: false },
    });
  }
  return pool;
}

/* ========= Helpers / Crypto ========= */
function nowPlusHours(h) {
  return new Date(Date.now() + h * 60 * 60 * 1000);
}

function hashPin(pin) {
  // guarda: salt:hash (hex)
  const salt = crypto.randomBytes(16);
  const hash = crypto.scryptSync(String(pin), salt, 32);
  return `${salt.toString("hex")}:${hash.toString("hex")}`;
}

function verifyPin(pin, stored) {
  try {
    const [saltHex, hashHex] = String(stored).split(":");
    const salt = Buffer.from(saltHex, "hex");
    const hash = Buffer.from(hashHex, "hex");
    const test = crypto.scryptSync(String(pin), salt, 32);
    return crypto.timingSafeEqual(hash, test);
  } catch {
    return false;
  }
}

function randomToken() {
  return crypto.randomBytes(32).toString("hex");
}

async function audit(event_type, req, user, meta = {}) {
  if (!dbEnabled()) return;

  const ip =
    req.headers["x-forwarded-for"]?.toString().split(",")[0]?.trim() ||
    req.socket?.remoteAddress ||
    null;

  const ua = req.headers["user-agent"] || null;

  const payload = {
    event_type,
    user_id: user?.id ?? null,
    username: user?.username ?? null,
    ip,
    user_agent: ua,
    meta: meta ?? {},
  };

  try {
    await getPool().query(
      `insert into public.audit_events(event_type, user_id, username, ip, user_agent, meta)
       values ($1,$2,$3,$4,$5,$6::jsonb)`,
      [
        payload.event_type,
        payload.user_id,
        payload.username,
        payload.ip,
        payload.user_agent,
        JSON.stringify(payload.meta),
      ]
    );
  } catch (e) {
    console.log("⚠️ No pude guardar audit:", e.message);
  }
}

/* ========= Auth Middleware ========= */
async function requireAuth(req, res, next) {
  try {
    if (!dbEnabled()) {
      return res.status(500).json({
        ok: false,
        message: "DB no configurada. Falta DATABASE_URL en Render.",
      });
    }

    const auth = req.headers.authorization || "";
    const token = auth.startsWith("Bearer ") ? auth.slice(7).trim() : "";

    if (!token) {
      return res.status(401).json({ ok: false, message: "No autorizado (sin token)" });
    }

    const r = await getPool().query(
      `select s.id as session_id, s.expires_at, s.user_id,
              u.username, u.full_name, u.is_active
       from public.sessions s
       join public.users u on u.id = s.user_id
       where s.token = $1
       limit 1`,
      [token]
    );

    if (!r.rows.length) {
      return res.status(401).json({ ok: false, message: "Sesión inválida" });
    }

    const row = r.rows[0];
    if (!row.is_active) {
      return res.status(403).json({ ok: false, message: "Usuario inactivo" });
    }

    const exp = new Date(row.expires_at).getTime();
    if (Date.now() > exp) {
      return res.status(401).json({ ok: false, message: "Sesión expirada" });
    }

    // actualizar last_seen
    await getPool().query(`update public.sessions set last_seen_at = now() where id = $1`, [
      row.session_id,
    ]);

    req.user = {
      id: row.user_id,
      username: row.username,
      full_name: row.full_name,
      session_id: row.session_id,
      token,
    };

    next();
  } catch (e) {
    return res.status(500).json({ ok: false, message: e.message });
  }
}

function requireAdmin(req, res, next) {
  const t = req.headers["x-admin-token"] || "";
  if (!ADMIN_TOKEN) {
    return res.status(500).json({ ok: false, message: "ADMIN_TOKEN no configurado en Render" });
  }
  if (String(t) !== String(ADMIN_TOKEN)) {
    return res.status(403).json({ ok: false, message: "Admin token inválido" });
  }
  next();
}

/* ========= SAP Helpers ========= */
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

/* ========= API Health ========= */
app.get("/api/health", async (req, res) => {
  res.json({
    ok: true,
    message: "✅ PRODIMA API activa",
    yappy: YAPPY_ALIAS,
    warehouse: SAP_WAREHOUSE,
    priceList: SAP_PRICE_LIST,
    db: dbEnabled() ? "OK" : "OFF",
  });
});

/* ========= AUTH ========= */
/* Login mercaderista */
app.post("/api/auth/login", async (req, res) => {
  try {
    if (!dbEnabled()) {
      return res.status(500).json({
        ok: false,
        message: "DB no configurada. Falta DATABASE_URL en Render.",
      });
    }

    const { username, pin } = req.body || {};
    const u = String(username || "").trim().toLowerCase();
    const p = String(pin || "").trim();

    if (!u || !p) {
      return res.status(400).json({ ok: false, message: "username y pin son requeridos" });
    }

    const r = await getPool().query(
      `select id, username, full_name, pin_hash, is_active
       from public.users
       where username = $1
       limit 1`,
      [u]
    );

    if (!r.rows.length) {
      await audit("LOGIN_FAIL", req, null, { username: u, reason: "user_not_found" });
      return res.status(401).json({ ok: false, message: "Credenciales inválidas" });
    }

    const user = r.rows[0];
    if (!user.is_active) {
      await audit("LOGIN_FAIL", req, { id: user.id, username: user.username }, { reason: "inactive" });
      return res.status(403).json({ ok: false, message: "Usuario inactivo" });
    }

    if (!verifyPin(p, user.pin_hash)) {
      await audit("LOGIN_FAIL", req, { id: user.id, username: user.username }, { reason: "bad_pin" });
      return res.status(401).json({ ok: false, message: "Credenciales inválidas" });
    }

    // crear sesión
    const token = randomToken();
    const expiresAt = nowPlusHours(SESSION_TTL_HOURS);

    await getPool().query(
      `insert into public.sessions(user_id, token, expires_at)
       values ($1,$2,$3)`,
      [user.id, token, expiresAt]
    );

    await audit("LOGIN_OK", req, { id: user.id, username: user.username }, { ttl_hours: SESSION_TTL_HOURS });

    return res.json({
      ok: true,
      token,
      user: { id: user.id, username: user.username, fullName: user.full_name },
      expiresAt,
    });
  } catch (e) {
    return res.status(500).json({ ok: false, message: e.message });
  }
});

/* Logout */
app.post("/api/auth/logout", requireAuth, async (req, res) => {
  try {
    await getPool().query(`delete from public.sessions where id = $1`, [req.user.session_id]);
    await audit("LOGOUT", req, req.user, {});
    return res.json({ ok: true });
  } catch (e) {
    return res.status(500).json({ ok: false, message: e.message });
  }
});

/* ========= ADMIN USERS ========= */
/* Crear usuario mercaderista */
app.post("/api/admin/users", requireAdmin, async (req, res) => {
  try {
    if (!dbEnabled()) {
      return res.status(500).json({ ok: false, message: "DB no configurada. Falta DATABASE_URL" });
    }

    const { username, fullName, pin } = req.body || {};
    const u = String(username || "").trim().toLowerCase();
    const fn = String(fullName || "").trim();
    const p = String(pin || "").trim();

    if (!u || !fn || !p) {
      return res.status(400).json({ ok: false, message: "username, fullName y pin son requeridos" });
    }

    const pin_hash = hashPin(p);

    const r = await getPool().query(
      `insert into public.users(username, full_name, pin_hash)
       values ($1,$2,$3)
       returning id, username, full_name, created_at`,
      [u, fn, pin_hash]
    );

    return res.json({ ok: true, user: r.rows[0] });
  } catch (e) {
    if (String(e.message || "").includes("duplicate key")) {
      return res.status(409).json({ ok: false, message: "Ese username ya existe" });
    }
    return res.status(500).json({ ok: false, message: e.message });
  }
});

/* Listar usuarios */
app.get("/api/admin/users", requireAdmin, async (req, res) => {
  try {
    const r = await getPool().query(
      `select id, username, full_name, is_active, created_at
       from public.users
       order by created_at desc`
    );
    return res.json({ ok: true, users: r.rows });
  } catch (e) {
    return res.status(500).json({ ok: false, message: e.message });
  }
});

/* ========= AUDIT ========= */
app.get("/api/admin/audit_events", requireAdmin, async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit || 100), 500);
    const r = await getPool().query(
      `select id, event_type, username, ip, created_at, meta
       from public.audit_events
       order by created_at desc
       limit $1`,
      [limit]
    );
    return res.json({ ok: true, events: r.rows });
  } catch (e) {
    return res.status(500).json({ ok: false, message: e.message });
  }
});

/* ========= PRICE LIST NO ========= */
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
   ✅ ITEM (para formulario)
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
   ✅ CUSTOMER
========================================================= */
app.get("/api/sap/customer/:cardCode", async (req, res) => {
  try {
    const cardCode = String(req.params.cardCode || "").trim();
    if (!cardCode) return res.status(400).json({ ok: false, message: "CardCode vacío." });

    const r = await slFetch(`/BusinessPartners('${encodeURIComponent(cardCode)}')`);

    const customer = {
      CardCode: r.CardCode,
      CardName: r.CardName,
      Phone1: r.Phone1,
      Phone2: r.Phone2,
      EmailAddress: r.EmailAddress,
      Address: r.Address || r.BPAddresses?.[0]?.Street || "",
    };

    return res.json({ ok: true, customer });
  } catch (err) {
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* =========================================================
   ✅ CREATE QUOTE (PROTEGIDO POR LOGIN)
========================================================= */
app.post("/api/sap/quote", requireAuth, async (req, res) => {
  try {
    if (missingSapEnv()) {
      return res.status(400).json({ ok: false, message: "Faltan variables SAP" });
    }

    const { cardCode, comments, lines } = req.body || {};
    const cc = String(cardCode || "").trim();

    if (!cc) return res.status(400).json({ ok: false, message: "cardCode requerido" });
    if (!Array.isArray(lines) || !lines.length) {
      return res.status(400).json({ ok: false, message: "lines requerido" });
    }

    // ✅ Cotización se debe poder crear aunque no haya inventario
    // Solo mandamos ItemCode + Quantity
    const DocumentLines = lines.map((l) => ({
      ItemCode: String(l.itemCode).trim(),
      Quantity: Number(l.qty || 0),
    }));

    const payload = {
      CardCode: cc,
      Comments: comments ? String(comments) : "",
      DocumentLines,
    };

    const r = await slFetch(`/Quotations`, {
      method: "POST",
      body: JSON.stringify(payload),
    });

    await audit("QUOTE_CREATED", req, req.user, {
      cardCode: cc,
      docEntry: r?.DocEntry,
      docNum: r?.DocNum,
      linesCount: DocumentLines.length,
    });

    return res.json({ ok: true, docEntry: r.DocEntry, docNum: r.DocNum });
  } catch (err) {
    return res.status(500).json({ ok: false, message: err.message });
  }
});

/* ========= START ========= */
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log("✅ Server listo en puerto", PORT));
