import express from "express";
import pg from "pg";
import jwt from "jsonwebtoken";
import bcrypt from "bcryptjs";

const { Pool } = pg;

const app = express();
app.use(express.json({ limit: "2mb" }));

const {
  PORT = 3000,
  DATABASE_URL = "",
  JWT_SECRET = "change_me",
  ADMIN_USER = "PRODIMA",
  ADMIN_PASS = "ADMINISTRADOR",
  CORS_ORIGIN = "",
} = process.env;

const ALLOWED_ORIGINS = new Set(
  String(CORS_ORIGIN || "")
    .split(",")
    .map(s => s.trim())
    .filter(Boolean)
);
const allowAll = ALLOWED_ORIGINS.size === 0;

app.use((req, res, next) => {
  const origin = req.headers.origin;

  if (allowAll && origin) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
  } else if (origin && ALLOWED_ORIGINS.has(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
  }

  res.setHeader("Access-Control-Allow-Methods", "GET,POST,PATCH,DELETE,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.setHeader("Access-Control-Max-Age", "86400");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

const pool = new Pool({
  connectionString: DATABASE_URL || undefined,
  ssl: DATABASE_URL && DATABASE_URL.includes("sslmode")
    ? { rejectUnauthorized:false }
    : undefined,
});

function hasDb(){
  return Boolean(DATABASE_URL);
}

async function dbQuery(text, params = []){
  return pool.query(text, params);
}

function safeJson(res, status, obj){
  res.status(status).json(obj);
}

function signToken(payload, ttl = "12h"){
  return jwt.sign(payload, JWT_SECRET, { expiresIn: ttl });
}

function readBearer(req){
  const auth = String(req.headers.authorization || "");
  const m = auth.match(/^Bearer\s+(.+)$/i);
  return m ? m[1] : "";
}

function verifyAuth(req, res, next){
  const token = readBearer(req);
  if(!token) return safeJson(res, 401, { ok:false, message:"Missing Bearer token" });

  try{
    const decoded = jwt.verify(token, JWT_SECRET);
    req.user = decoded;
    next();
  }catch{
    return safeJson(res, 401, { ok:false, message:"Invalid token" });
  }
}

function verifyAdmin(req, res, next){
  const token = readBearer(req);
  if(!token) return safeJson(res, 401, { ok:false, message:"Missing Bearer token" });

  try{
    const decoded = jwt.verify(token, JWT_SECRET);
    if(decoded?.role !== "admin") {
      return safeJson(res, 403, { ok:false, message:"Forbidden" });
    }
    req.user = decoded;
    next();
  }catch{
    return safeJson(res, 401, { ok:false, message:"Invalid token" });
  }
}

async function hashPin(pin){
  const saltRounds = 10;
  return bcrypt.hash(String(pin), saltRounds);
}

async function comparePin(pin, pinHash){
  return bcrypt.compare(String(pin), String(pinHash || ""));
}

async function ensureDb(){
  if(!hasDb()) return;

  await dbQuery(`
    CREATE TABLE IF NOT EXISTS portal_users (
      id SERIAL PRIMARY KEY,
      username TEXT UNIQUE NOT NULL,
      full_name TEXT DEFAULT '',
      pin_hash TEXT NOT NULL,
      role TEXT NOT NULL DEFAULT 'user',
      is_active BOOLEAN DEFAULT TRUE,
      permissions_json JSONB NOT NULL DEFAULT '[]'::jsonb,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await dbQuery(`
    CREATE INDEX IF NOT EXISTS idx_portal_users_username ON portal_users(username);
  `);
}

app.get("/api/health", async (req, res) => {
  return safeJson(res, 200, {
    ok:true,
    message:"✅ PRODIMA Portal API activa",
    db: hasDb() ? "on" : "off"
  });
});

app.post("/api/auth/login", async (req, res) => {
  try{
    const username = String(req.body?.username || "").trim();
    const pass = String(req.body?.pass || "").trim();

    if(!username || !pass){
      return safeJson(res, 400, { ok:false, message:"Usuario y contraseña requeridos" });
    }

    if(username === ADMIN_USER && pass === ADMIN_PASS){
      const user = {
        id:0,
        username:ADMIN_USER,
        full_name:"Administrador PRODIMA",
        role:"admin",
        permissions:["*"]
      };

      const token = signToken(user, "12h");
      return safeJson(res, 200, { ok:true, token, user });
    }

    if(!hasDb()) return safeJson(res, 500, { ok:false, message:"DB no configurada" });

    const r = await dbQuery(
      `SELECT id, username, full_name, pin_hash, role, is_active, permissions_json
       FROM portal_users
       WHERE LOWER(username)=LOWER($1)
       LIMIT 1`,
      [username]
    );

    const u = r.rows?.[0];
    if(!u) return safeJson(res, 401, { ok:false, message:"Credenciales inválidas" });
    if(!u.is_active) return safeJson(res, 403, { ok:false, message:"Usuario inactivo" });

    const okPass = await comparePin(pass, u.pin_hash);
    if(!okPass) return safeJson(res, 401, { ok:false, message:"Credenciales inválidas" });

    const user = {
      id: u.id,
      username: u.username,
      full_name: u.full_name,
      role: u.role || "user",
      permissions: Array.isArray(u.permissions_json) ? u.permissions_json : []
    };

    const token = signToken(user, "12h");
    return safeJson(res, 200, { ok:true, token, user });
  }catch(e){
    return safeJson(res, 500, { ok:false, message:e.message || String(e) });
  }
});

app.get("/api/auth/me", verifyAuth, async (req, res) => {
  return safeJson(res, 200, { ok:true, user:req.user });
});

app.get("/api/admin/users", verifyAdmin, async (req, res) => {
  try{
    if(!hasDb()) return safeJson(res, 500, { ok:false, message:"DB no configurada" });

    const r = await dbQuery(
      `SELECT id, username, full_name, role, is_active, permissions_json, created_at
       FROM portal_users
       ORDER BY id DESC`
    );

    const users = (r.rows || []).map(x => ({
      id:x.id,
      username:x.username,
      full_name:x.full_name,
      role:x.role,
      is_active:x.is_active,
      permissions:Array.isArray(x.permissions_json) ? x.permissions_json : [],
      created_at:x.created_at,
    }));

    return safeJson(res, 200, { ok:true, users });
  }catch(e){
    return safeJson(res, 500, { ok:false, message:e.message || String(e) });
  }
});

app.post("/api/admin/users", verifyAdmin, async (req, res) => {
  try{
    if(!hasDb()) return safeJson(res, 500, { ok:false, message:"DB no configurada" });

    const username = String(req.body?.username || "").trim().toLowerCase();
    const full_name = String(req.body?.full_name || "").trim();
    const pin = String(req.body?.pin || "").trim();
    const role = String(req.body?.role || "user").trim().toLowerCase();
    const permissions = Array.isArray(req.body?.permissions) ? req.body.permissions.map(x => String(x).trim()) : [];

    if(!username) return safeJson(res, 400, { ok:false, message:"Username requerido" });
    if(!full_name) return safeJson(res, 400, { ok:false, message:"Nombre requerido" });
    if(!pin || pin.length < 4) return safeJson(res, 400, { ok:false, message:"PIN mínimo 4" });
    if(!["user","admin"].includes(role)) return safeJson(res, 400, { ok:false, message:"Rol inválido" });

    const pin_hash = await hashPin(pin);
    const finalPermissions = role === "admin" ? ["*"] : permissions;

    const r = await dbQuery(
      `INSERT INTO portal_users(username, full_name, pin_hash, role, is_active, permissions_json)
       VALUES ($1,$2,$3,$4,TRUE,$5::jsonb)
       RETURNING id, username, full_name, role, is_active, permissions_json, created_at`,
      [username, full_name, pin_hash, role, JSON.stringify(finalPermissions)]
    );

    const user = r.rows?.[0];
    return safeJson(res, 200, {
      ok:true,
      user:{
        id:user.id,
        username:user.username,
        full_name:user.full_name,
        role:user.role,
        is_active:user.is_active,
        permissions:user.permissions_json,
        created_at:user.created_at,
      }
    });
  }catch(e){
    if(String(e?.code) === "23505"){
      return safeJson(res, 409, { ok:false, message:"Ese username ya existe" });
    }
    return safeJson(res, 500, { ok:false, message:e.message || String(e) });
  }
});

app.patch("/api/admin/users/:id", verifyAdmin, async (req, res) => {
  try{
    if (!hasDb()) return safeJson(res, 500, { ok:false, message:"DB no configurada" });

    const id = Number(req.params.id || 0);
    const full_name = String(req.body?.full_name || "").trim();
    const role = String(req.body?.role || "user").trim().toLowerCase();
    const permissions = Array.isArray(req.body?.permissions) ? req.body.permissions : [];

    if(!Number.isFinite(id) || id <= 0) return safeJson(res, 400, { ok:false, message:"ID inválido" });
    if(!full_name) return safeJson(res, 400, { ok:false, message:"Nombre requerido" });
    if(!["user","admin"].includes(role)) return safeJson(res, 400, { ok:false, message:"Rol inválido" });

    const finalPermissions = role === "admin" ? ["*"] : permissions;

    const r = await dbQuery(
      `UPDATE portal_users
       SET full_name=$2,
           role=$3,
           permissions_json=$4::jsonb
       WHERE id=$1
       RETURNING id, username, full_name, role, is_active, permissions_json, created_at`,
      [id, full_name, role, JSON.stringify(finalPermissions)]
    );

    if(!r.rowCount) return safeJson(res, 404, { ok:false, message:"Usuario no encontrado" });

    return safeJson(res, 200, {
      ok:true,
      user:{
        ...r.rows[0],
        permissions: r.rows[0].permissions_json
      }
    });
  }catch(e){
    return safeJson(res, 500, { ok:false, message:e.message || String(e) });
  }
});

app.patch("/api/admin/users/:id/toggle", verifyAdmin, async (req, res) => {
  try{
    if(!hasDb()) return safeJson(res, 500, { ok:false, message:"DB no configurada" });

    const id = Number(req.params.id || 0);
    if(!Number.isFinite(id) || id <= 0) return safeJson(res, 400, { ok:false, message:"ID inválido" });

    const r = await dbQuery(
      `UPDATE portal_users
       SET is_active = NOT is_active
       WHERE id = $1
       RETURNING id, username, full_name, role, is_active, permissions_json, created_at`,
      [id]
    );

    if(!r.rowCount) return safeJson(res, 404, { ok:false, message:"Usuario no encontrado" });

    const user = r.rows[0];
    return safeJson(res, 200, {
      ok:true,
      user:{
        id:user.id,
        username:user.username,
        full_name:user.full_name,
        role:user.role,
        is_active:user.is_active,
        permissions:user.permissions_json,
        created_at:user.created_at,
      }
    });
  }catch(e){
    return safeJson(res, 500, { ok:false, message:e.message || String(e) });
  }
});

app.delete("/api/admin/users/:id", verifyAdmin, async (req, res) => {
  try{
    if(!hasDb()) return safeJson(res, 500, { ok:false, message:"DB no configurada" });

    const id = Number(req.params.id || 0);
    if(!Number.isFinite(id) || id <= 0) return safeJson(res, 400, { ok:false, message:"ID inválido" });

    const r = await dbQuery(`DELETE FROM portal_users WHERE id=$1`, [id]);
    if(!r.rowCount) return safeJson(res, 404, { ok:false, message:"Usuario no encontrado" });

    return safeJson(res, 200, { ok:true });
  }catch(e){
    return safeJson(res, 500, { ok:false, message:e.message || String(e) });
  }
});

process.on("unhandledRejection", e => console.error("unhandledRejection:", e));
process.on("uncaughtException", e => console.error("uncaughtException:", e));

(async ()=>{
  try{
    await ensureDb();
    console.log(hasDb() ? "DB ready ✅" : "DB not configured ⚠️");
  }catch(e){
    console.error("DB init error:", e.message);
  }

  app.listen(Number(PORT), ()=> console.log(`Server listening on :${PORT}`));
})();
