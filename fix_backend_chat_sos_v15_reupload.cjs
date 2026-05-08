#!/usr/bin/env node
/**
 * PTYDrive Backend V15 REUPLOAD - Chat/SOS unificado para app + admin web
 *
 * Corrige:
 * - Admin no ve chats.
 * - Admin no recibe/lista SOS.
 * - SOS no crea hilo de soporte.
 * - Respuesta admin desde SOS no llega a hilo de soporte.
 *
 * Agrega/normaliza endpoints:
 *   POST /api/support/messages
 *   GET  /api/support/messages
 *   POST /api/admin/support/messages
 *   GET  /api/admin/chats
 *   GET  /api/admin/support/threads
 *   POST /api/sos
 *   POST /api/security/sos
 *   GET  /api/admin/sos
 *   GET  /api/admin/security/sos
 *   POST /api/admin/sos/:id/message
 *   PATCH /api/admin/sos/:id/status
 *
 * NO toca:
 * - /api/routes/drive
 * - aceptar carrera
 * - Google routes
 *
 * Uso en Codespaces:
 *   cd /workspaces/prodima-pay
 *   node fix_backend_chat_sos_v15_reupload.cjs
 *   git add ptydrive/server.js
 *   git commit -m "Fix PTYDrive chat and SOS admin pipeline"
 *   git push
 */

const fs = require("fs");
const path = require("path");

const root = process.cwd();
const serverPath = path.join(root, "ptydrive", "server.js");

function fail(msg){ console.error("ERROR:", msg); process.exit(1); }
function stamp(){ return new Date().toISOString().replace(/[:.]/g, "-"); }
function backup(file, tag){ const bak=`${file}.bak_${tag}_${stamp()}`; fs.copyFileSync(file,bak); console.log("Backup:", path.relative(root,bak)); return bak; }

if (!fs.existsSync(serverPath)) fail(`No encontré ${serverPath}`);

backup(serverPath, "chat_sos_v15_reupload");

let src = fs.readFileSync(serverPath, "utf8");
let changes = 0;
const marker = "/* PTY BACKEND CHAT SOS V15 REUPLOAD */";

if (!src.includes(marker) && !src.includes("/* PTY BACKEND CHAT SOS V15 */")) {
  const insertCandidates = [
    "/* PTY BACKEND FAST CORE V9 - LIGHTWEIGHT ROUTES */",
    "/* PTY BACKEND FAST PROFILE REVIEWS V8 - NO ROUTE TOUCH */",
    "/* PTY BACKEND CORE SERVICES V7 - NO ROUTE TOUCH */",
    'app.get("/api/rides/active"',
  ];
  let idx = -1;
  for (const item of insertCandidates) {
    idx = src.indexOf(item);
    if (idx !== -1) break;
  }
  if (idx === -1) fail("No encontré punto para insertar V15 antes de rutas existentes.");

  const block = `
${marker}
const ptyV15AuthOptional = typeof authOptional === "function" ? authOptional : ((req, _res, next) => next());

function ptyV15Text(v = "") { return String(v ?? "").trim(); }
function ptyV15Json(v, fallback = {}) {
  if (!v) return fallback;
  if (typeof v === "object") return v;
  try { return JSON.parse(v); } catch { return fallback; }
}
function ptyV15Point(v = {}) {
  const raw = ptyV15Json(v, v || {});
  const lat = Number(raw.lat ?? raw.latitude);
  const lng = Number(raw.lng ?? raw.lon ?? raw.longitude);
  return Number.isFinite(lat) && Number.isFinite(lng) ? { ...raw, lat, lng } : raw;
}
function ptyV15SafeJson(res, status, payload) {
  try { if (typeof safeJson === "function") return safeJson(res, status, payload); } catch {}
  return res.status(status).json(payload);
}
function ptyV15ThreadKey({ userId = "", userEmail = "", rideId = "", role = "" } = {}) {
  return [ptyV15Text(userId || userEmail || "anon").toLowerCase(), ptyV15Text(rideId || "general").toLowerCase(), ptyV15Text(role || "user").toLowerCase()].join(":");
}

async function ptyV15EnsureTables() {
  if (globalThis.__PTY_V15_CHAT_SOS_READY__) return;

  await db(\`
    CREATE TABLE IF NOT EXISTS ride_support_threads (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      thread_key TEXT UNIQUE,
      user_id TEXT DEFAULT '',
      user_email TEXT DEFAULT '',
      user_name TEXT DEFAULT '',
      user_phone TEXT DEFAULT '',
      role TEXT DEFAULT '',
      ride_id TEXT DEFAULT '',
      status TEXT DEFAULT 'open',
      title TEXT DEFAULT '',
      last_message TEXT DEFAULT '',
      last_at TIMESTAMPTZ DEFAULT NOW(),
      meta JSONB DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  \`);
  await db(\`
    CREATE TABLE IF NOT EXISTS ride_support_messages (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      thread_id UUID REFERENCES ride_support_threads(id) ON DELETE CASCADE,
      user_id TEXT DEFAULT '',
      sender_id TEXT DEFAULT '',
      sender_role TEXT DEFAULT '',
      sender_name TEXT DEFAULT '',
      author TEXT DEFAULT '',
      text TEXT DEFAULT '',
      meta JSONB DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  \`);
  await db(\`CREATE INDEX IF NOT EXISTS idx_pty_v15_support_messages_thread ON ride_support_messages(thread_id, created_at);\`);
  await db(\`CREATE INDEX IF NOT EXISTS idx_pty_v15_support_threads_user ON ride_support_threads(user_id, ride_id, updated_at DESC);\`);
  await db(\`CREATE INDEX IF NOT EXISTS idx_pty_v15_support_threads_status ON ride_support_threads(status, updated_at DESC);\`);

  await db(\`
    CREATE TABLE IF NOT EXISTS ride_sos_alerts (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      user_id TEXT DEFAULT '',
      user_email TEXT DEFAULT '',
      user_name TEXT DEFAULT '',
      user_phone TEXT DEFAULT '',
      role TEXT DEFAULT '',
      ride_id TEXT DEFAULT '',
      message TEXT DEFAULT '',
      status TEXT DEFAULT 'open',
      location JSONB DEFAULT '{}'::jsonb,
      trusted_contact JSONB DEFAULT '{}'::jsonb,
      ride JSONB DEFAULT '{}'::jsonb,
      thread_id TEXT DEFAULT '',
      meta JSONB DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  \`);
  await db(\`ALTER TABLE ride_sos_alerts ADD COLUMN IF NOT EXISTS thread_id TEXT DEFAULT '';\`).catch(() => null);
  await db(\`ALTER TABLE ride_sos_alerts ADD COLUMN IF NOT EXISTS meta JSONB DEFAULT '{}'::jsonb;\`).catch(() => null);
  await db(\`CREATE INDEX IF NOT EXISTS idx_pty_v15_sos_status ON ride_sos_alerts(status, created_at DESC);\`);

  globalThis.__PTY_V15_CHAT_SOS_READY__ = true;
}

async function ptyV15UpsertThread({ userId = "", userEmail = "", userName = "", userPhone = "", role = "", rideId = "", title = "", lastMessage = "", meta = {} } = {}) {
  await ptyV15EnsureTables();
  const key = ptyV15ThreadKey({ userId, userEmail, rideId, role });
  const r = await db(\`
    INSERT INTO ride_support_threads(thread_key,user_id,user_email,user_name,user_phone,role,ride_id,title,last_message,last_at,meta,updated_at)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW(),$10::jsonb,NOW())
    ON CONFLICT(thread_key) DO UPDATE SET
      user_id=COALESCE(NULLIF(EXCLUDED.user_id,''), ride_support_threads.user_id),
      user_email=COALESCE(NULLIF(EXCLUDED.user_email,''), ride_support_threads.user_email),
      user_name=COALESCE(NULLIF(EXCLUDED.user_name,''), ride_support_threads.user_name),
      user_phone=COALESCE(NULLIF(EXCLUDED.user_phone,''), ride_support_threads.user_phone),
      role=COALESCE(NULLIF(EXCLUDED.role,''), ride_support_threads.role),
      ride_id=COALESCE(NULLIF(EXCLUDED.ride_id,''), ride_support_threads.ride_id),
      title=COALESCE(NULLIF(EXCLUDED.title,''), ride_support_threads.title),
      last_message=COALESCE(NULLIF(EXCLUDED.last_message,''), ride_support_threads.last_message),
      last_at=NOW(),
      updated_at=NOW(),
      meta=ride_support_threads.meta || EXCLUDED.meta
    RETURNING *\`,
    [key, userId, userEmail, userName, userPhone, role, rideId, title || "Soporte PTY", lastMessage, JSON.stringify(meta || {})]
  );
  return r.rows[0];
}

function ptyV15NormalizeSupportThread(row = {}) {
  return {
    id: String(row.id || ""),
    threadId: String(row.id || ""),
    type: "support",
    title: row.title || row.user_name || row.user_email || "Soporte",
    userId: row.user_id || "",
    userEmail: row.user_email || "",
    userName: row.user_name || "",
    userPhone: row.user_phone || "",
    role: row.role || "",
    rideId: row.ride_id || "",
    status: row.status || "open",
    lastMessage: row.last_message || "",
    updatedAt: row.updated_at || row.last_at || row.created_at,
    createdAt: row.created_at,
    unread: true,
  };
}

function ptyV15NormalizeSupportMessage(row = {}, thread = {}) {
  return {
    id: String(row.id || ""),
    threadId: String(row.thread_id || thread.id || ""),
    userId: row.user_id || thread.user_id || "",
    senderId: row.sender_id || "",
    senderRole: row.sender_role || "",
    senderName: row.sender_name || "",
    author: row.author || row.sender_name || "Usuario",
    text: row.text || "",
    message: row.text || "",
    createdAt: row.created_at,
    at: row.created_at,
  };
}

async function ptyV15AddSupportMessage({ thread, userId = "", senderId = "", senderRole = "", senderName = "", author = "", text = "", meta = {} } = {}) {
  const clean = ptyV15Text(text);
  if (!clean || !thread?.id) return null;
  const r = await db(\`
    INSERT INTO ride_support_messages(thread_id,user_id,sender_id,sender_role,sender_name,author,text,meta)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
    RETURNING *\`,
    [thread.id, userId || thread.user_id || "", senderId, senderRole, senderName, author || senderName || senderRole || "Usuario", clean, JSON.stringify(meta || {})]
  );
  await db(\`UPDATE ride_support_threads SET last_message=$2,last_at=NOW(),updated_at=NOW() WHERE id=$1\`, [thread.id, clean]).catch(() => null);
  const msg = ptyV15NormalizeSupportMessage(r.rows[0], thread);
  try {
    io.to("admins").emit("support.message", { thread: ptyV15NormalizeSupportThread(thread), message: msg });
    if (thread.user_id) io.to(\`user:\${thread.user_id}\`).emit("support.message", { thread: ptyV15NormalizeSupportThread(thread), message: msg });
    io.emit("support.message", { thread: ptyV15NormalizeSupportThread(thread), message: msg });
  } catch {}
  return msg;
}

app.post("/api/support/messages", ptyV15AuthOptional, async (req, res) => {
  try {
    const user = req.user || {};
    const body = req.body || {};
    const userId = ptyV15Text(body.userId || user.id || "");
    const userEmail = ptyV15Text(body.userEmail || user.email || "");
    const role = ptyV15Text(body.role || user.role || "");
    const text = ptyV15Text(body.message || body.text || body.detail || "");
    const thread = await ptyV15UpsertThread({
      userId,
      userEmail,
      userName: body.userName || user.name || "",
      userPhone: body.userPhone || user.phone || "",
      role,
      rideId: body.rideId || "",
      title: body.title || "Soporte PTY",
      lastMessage: text,
      meta: body,
    });
    const message = await ptyV15AddSupportMessage({
      thread,
      userId,
      senderId: userId,
      senderRole: role || "user",
      senderName: body.userName || user.name || (role === "driver" ? "Conductor" : "Usuario"),
      author: body.author || body.userName || user.name || (role === "driver" ? "Conductor" : "Usuario"),
      text,
      meta: body,
    });
    return ptyV15SafeJson(res, 201, { ok: true, thread: ptyV15NormalizeSupportThread(thread), message });
  } catch (e) {
    return ptyV15SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/support/messages", ptyV15AuthOptional, async (req, res) => {
  try {
    await ptyV15EnsureTables();
    const userId = ptyV15Text(req.query.userId || req.user?.id || "");
    const rideId = ptyV15Text(req.query.rideId || "");
    const params = [userId];
    let where = \`WHERE (t.user_id::text=$1::text OR $1::text='')\`;
    if (rideId) { params.push(rideId); where += \` AND t.ride_id::text=$2::text\`; }
    const r = await db(\`
      SELECT m.*, t.user_id AS t_user_id
      FROM ride_support_messages m
      JOIN ride_support_threads t ON t.id=m.thread_id
      \${where}
      ORDER BY m.created_at ASC
      LIMIT 500\`, params);
    const messages = r.rows.map((row) => ptyV15NormalizeSupportMessage(row, { user_id: row.t_user_id }));
    return ptyV15SafeJson(res, 200, { ok: true, messages, data: messages });
  } catch (e) {
    return ptyV15SafeJson(res, 500, { ok: false, message: String(e?.message || e), messages: [] });
  }
});

app.post("/api/admin/support/messages", ptyV15AuthOptional, async (req, res) => {
  try {
    const body = req.body || {};
    const text = ptyV15Text(body.message || body.text || body.detail || "");
    const thread = await ptyV15UpsertThread({
      userId: body.userId || "",
      userEmail: body.userEmail || "",
      userName: body.userName || "",
      userPhone: body.userPhone || "",
      role: body.role || "",
      rideId: body.rideId || "",
      title: body.title || "Soporte PTY",
      lastMessage: text,
      meta: body,
    });
    const message = await ptyV15AddSupportMessage({
      thread,
      userId: body.userId || "",
      senderId: body.senderId || "admin",
      senderRole: body.senderRole || "admin",
      senderName: body.senderName || "PTY Drive",
      author: body.author || "PTY Drive",
      text,
      meta: body,
    });
    return ptyV15SafeJson(res, 201, { ok: true, thread: ptyV15NormalizeSupportThread(thread), message });
  } catch (e) {
    return ptyV15SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/admin/support/threads", ptyV15AuthOptional, async (_req, res) => {
  try {
    await ptyV15EnsureTables();
    const r = await db(\`SELECT * FROM ride_support_threads ORDER BY updated_at DESC LIMIT 300\`);
    const threads = r.rows.map(ptyV15NormalizeSupportThread);
    return ptyV15SafeJson(res, 200, { ok: true, threads, reports: threads });
  } catch (e) {
    return ptyV15SafeJson(res, 500, { ok: false, message: String(e?.message || e), threads: [] });
  }
});

app.get("/api/admin/chats", ptyV15AuthOptional, async (_req, res) => {
  try {
    await ptyV15EnsureTables();
    const supportR = await db(\`SELECT * FROM ride_support_threads ORDER BY updated_at DESC LIMIT 200\`).catch(() => ({ rows: [] }));
    const rideR = await db(\`
      SELECT ride_id,
             MAX(created_at) AS updated_at,
             COUNT(*) AS count,
             (ARRAY_AGG(text ORDER BY created_at DESC))[1] AS last_message
      FROM ride_chat_messages
      GROUP BY ride_id
      ORDER BY updated_at DESC
      LIMIT 200\`).catch(() => ({ rows: [] }));

    const supportChats = supportR.rows.map((row) => ({ ...ptyV15NormalizeSupportThread(row), id: \`support:\${row.id}\`, chatType: "support" }));
    const rideChats = rideR.rows.map((row) => ({
      id: \`ride:\${row.ride_id}\`,
      rideId: row.ride_id,
      chatType: "ride",
      title: \`Viaje \${String(row.ride_id).slice(-6)}\`,
      lastMessage: row.last_message || "",
      updatedAt: row.updated_at,
      count: Number(row.count || 0),
      unread: true,
    }));
    const chats = [...supportChats, ...rideChats].sort((a,b)=> new Date(b.updatedAt || 0) - new Date(a.updatedAt || 0));
    return ptyV15SafeJson(res, 200, { ok: true, chats, threads: chats });
  } catch (e) {
    return ptyV15SafeJson(res, 500, { ok: false, message: String(e?.message || e), chats: [] });
  }
});

async function ptyV15CreateSos(req, res) {
  try {
    await ptyV15EnsureTables();
    const user = req.user || {};
    const body = req.body || {};
    const userId = ptyV15Text(body.userId || user.id || "");
    const role = ptyV15Text(body.role || user.role || "");
    const msg = ptyV15Text(body.message || "SOS activado desde la app");
    const thread = await ptyV15UpsertThread({
      userId,
      userEmail: body.userEmail || user.email || "",
      userName: body.userName || user.name || "",
      userPhone: body.userPhone || user.phone || "",
      role,
      rideId: body.rideId || "",
      title: "🚨 SOS",
      lastMessage: msg,
      meta: body,
    });
    await ptyV15AddSupportMessage({
      thread,
      userId,
      senderId: userId,
      senderRole: role || "user",
      senderName: body.userName || user.name || "Usuario",
      author: body.userName || user.name || "Usuario",
      text: msg,
      meta: { sos: true, ...body },
    });
    const r = await db(\`
      INSERT INTO ride_sos_alerts(user_id,user_email,user_name,user_phone,role,ride_id,message,status,location,trusted_contact,ride,thread_id,meta)
      VALUES($1,$2,$3,$4,$5,$6,$7,'open',$8::jsonb,$9::jsonb,$10::jsonb,$11,$12::jsonb)
      RETURNING *\`,
      [
        userId,
        body.userEmail || user.email || "",
        body.userName || user.name || "",
        body.userPhone || user.phone || "",
        role,
        body.rideId || "",
        msg,
        JSON.stringify(ptyV15Point(body.location || body || {})),
        JSON.stringify(body.trustedContact || {}),
        JSON.stringify(body.ride || {}),
        String(thread.id),
        JSON.stringify(body),
      ]
    );
    const alert = { ...r.rows[0], threadId: String(thread.id), location: ptyV15Json(r.rows[0].location, {}) };
    try { io.to("admins").emit("sos.alert", { alert }); io.emit("sos.alert", { alert }); } catch {}
    return ptyV15SafeJson(res, 201, { ok: true, alert, thread: ptyV15NormalizeSupportThread(thread) });
  } catch (e) {
    return ptyV15SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
}
app.post("/api/sos", ptyV15AuthOptional, ptyV15CreateSos);
app.post("/api/security/sos", ptyV15AuthOptional, ptyV15CreateSos);

async function ptyV15ListSos(_req, res) {
  try {
    await ptyV15EnsureTables();
    const r = await db(\`SELECT * FROM ride_sos_alerts ORDER BY created_at DESC LIMIT 200\`);
    const alerts = r.rows.map((a) => ({ ...a, threadId: a.thread_id || "", location: ptyV15Json(a.location, {}) }));
    return ptyV15SafeJson(res, 200, { ok: true, alerts, sosAlerts: alerts });
  } catch (e) {
    return ptyV15SafeJson(res, 500, { ok: false, message: String(e?.message || e), alerts: [] });
  }
}
app.get("/api/admin/sos", ptyV15AuthOptional, ptyV15ListSos);
app.get("/api/admin/security/sos", ptyV15AuthOptional, ptyV15ListSos);

app.patch("/api/admin/sos/:id/status", ptyV15AuthOptional, async (req, res) => {
  try {
    await ptyV15EnsureTables();
    const status = ptyV15Text(req.body?.status || "resolved");
    const r = await db(\`UPDATE ride_sos_alerts SET status=$2, updated_at=NOW() WHERE id::text=$1::text RETURNING *\`, [req.params.id, status]);
    return ptyV15SafeJson(res, 200, { ok: true, alert: r.rows[0] || null });
  } catch (e) {
    return ptyV15SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.post("/api/admin/sos/:id/message", ptyV15AuthOptional, async (req, res) => {
  try {
    await ptyV15EnsureTables();
    const alertR = await db(\`SELECT * FROM ride_sos_alerts WHERE id::text=$1::text LIMIT 1\`, [req.params.id]);
    const alert = alertR.rows[0];
    if (!alert) return ptyV15SafeJson(res, 404, { ok: false, message: "SOS no encontrado" });
    let thread = null;
    if (alert.thread_id) {
      const tr = await db(\`SELECT * FROM ride_support_threads WHERE id::text=$1::text LIMIT 1\`, [alert.thread_id]).catch(() => ({ rows: [] }));
      thread = tr.rows[0] || null;
    }
    if (!thread) {
      thread = await ptyV15UpsertThread({
        userId: alert.user_id || "",
        userEmail: alert.user_email || "",
        userName: alert.user_name || "",
        userPhone: alert.user_phone || "",
        role: alert.role || "",
        rideId: alert.ride_id || "",
        title: "🚨 SOS",
        lastMessage: req.body?.message || "",
        meta: { sosId: req.params.id },
      });
      await db(\`UPDATE ride_sos_alerts SET thread_id=$2 WHERE id::text=$1::text\`, [req.params.id, thread.id]).catch(() => null);
    }
    const message = await ptyV15AddSupportMessage({
      thread,
      userId: alert.user_id || "",
      senderId: "admin",
      senderRole: "admin",
      senderName: "PTY Drive",
      author: "PTY Drive",
      text: req.body?.message || "",
      meta: { sosId: req.params.id, fromAdmin: true },
    });
    return ptyV15SafeJson(res, 201, { ok: true, thread: ptyV15NormalizeSupportThread(thread), message });
  } catch (e) {
    return ptyV15SafeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

`;

  src = src.slice(0, idx) + block + src.slice(idx);
  changes++;
  console.log("OK: bloque V15 Chat/SOS insertado");
} else {
  console.log("OK ya existe: bloque V15 Chat/SOS");
}

fs.writeFileSync(serverPath, src, "utf8");

console.log("");
console.log(`Listo. Cambios aplicados: ${changes}`);
console.log("");
console.log("Ahora ejecuta:");
console.log("git add ptydrive/server.js");
console.log('git commit -m "Fix PTYDrive chat and SOS admin pipeline"');
console.log("git push");
