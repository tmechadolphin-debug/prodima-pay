#!/usr/bin/env node
/**
 * PTYDrive FIX V4 - documentos rápidos, aprobación persistente y estado correcto.
 *
 * Corrige backend Render:
 * 1. Aprobar/rechazar documento responde rápido y no devuelve base64 innecesario.
 * 2. Calcula aprobación por tipo aceptando duplicados: si una foto de "identity" está aprobada,
 *    identity cuenta como aprobado aunque haya otra pendiente.
 * 3. Actualiza ride_users.document_status y flags para app:
 *    riderVerified, identityVerified, driverDocumentsApproved, canAcceptRides.
 * 4. Agrega /api/rider/verification/status para que rider no vuelva a pedir foto tras aprobar.
 * 5. Agrega /api/rider/documents/status y /api/driver/documents/status seguros.
 *
 * Uso:
 *   cd /workspaces/prodima-pay
 *   node fix_ptydrive_documents_v4_backend_fast_status.cjs
 *   git add ptydrive/server.js
 *   git commit -m "Fix PTYDrive document approval persistence and fast status"
 *   git push
 */

const fs = require("fs");
const path = require("path");

const root = process.cwd();
const file = path.join(root, "ptydrive", "server.js");

function fail(msg) {
  console.error("ERROR:", msg);
  process.exit(1);
}

function backup(file) {
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const bak = `${file}.bak_docs_v4_${stamp}`;
  fs.copyFileSync(file, bak);
  console.log("Backup:", path.relative(root, bak));
}

if (!fs.existsSync(file)) fail(`No encontré ${file}. Ejecuta desde la raíz prodima-pay.`);

backup(file);
let src = fs.readFileSync(file, "utf8");
let changes = 0;

const blockMarker = "/* PTY DOCS V4 FAST STATUS ROUTES */";

if (!src.includes(blockMarker)) {
  const insertBeforeCandidates = [
    'app.patch("/api/admin/documents/:id/status"',
    'app.get("/api/driver/documents/status"',
    'app.get("/api/driver/documents/approved"',
    '/* =========================================================\n   Documents / verification\n========================================================= */'
  ];

  let idx = -1;
  for (const c of insertBeforeCandidates) {
    idx = src.indexOf(c);
    if (idx !== -1) break;
  }
  if (idx === -1) fail("No encontré dónde insertar rutas V4 de documentos.");

  const block = `
${blockMarker}
const PTY_DOCS_V4_DRIVER_TYPES = [
  "fotoPerfilConductor",
  "selfieConLicencia",
  "cedulaFoto",
  "licenciaFoto",
  "recordPolicivo",
  "registroVehicular",
  "seguroVehicular",
  "inspeccionVehicular",
  "vehiculoFrontal",
  "vehiculoTrasero",
  "vehiculoLateral",
];

function ptyDocsV4Required(role = "driver") {
  const r = String(role || "driver").toLowerCase();
  if (typeof requiredDocumentTypesForRole === "function") {
    try { return requiredDocumentTypesForRole(r); } catch {}
  }
  return r === "rider" ? ["identity"] : PTY_DOCS_V4_DRIVER_TYPES;
}

function ptyDocsV4PublicLean(row = {}) {
  const d = typeof documentToPublic === "function" ? documentToPublic(row) : {
    id: row.id,
    userId: row.user_id,
    role: row.role,
    type: row.type,
    status: row.status,
    url: row.url,
    filename: row.filename,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
  return d;
}

function ptyDocsV4StatusSummary(documents = [], role = "driver") {
  const required = ptyDocsV4Required(role);
  const byType = {};
  for (const doc of documents || []) {
    const type = String(doc.type || doc.documentType || doc.key || "").trim();
    if (!type) continue;
    const status = String(doc.status || "pending").toLowerCase();
    const prev = byType[type] || { type, total: 0, approved: 0, pending: 0, rejected: 0, status: "missing" };
    prev.total += 1;
    if (status === "approved") prev.approved += 1;
    else if (status === "rejected") prev.rejected += 1;
    else prev.pending += 1;

    // Regla: un tipo queda aprobado si al menos una versión enviada está aprobada.
    if (prev.approved > 0) prev.status = "approved";
    else if (prev.rejected > 0 && prev.pending === 0) prev.status = "rejected";
    else prev.status = "pending";
    byType[type] = prev;
  }

  const statusMap = {};
  for (const type of Object.keys(byType)) statusMap[type] = byType[type].status;

  const missing = required.filter((type) => statusMap[type] !== "approved");
  const anyRejected = Object.values(byType).some((it) => it.status === "rejected");
  const anyPending = Object.values(byType).some((it) => it.status === "pending");
  const approved = missing.length === 0;
  const documentStatus = approved ? "approved" : anyRejected ? "rejected" : anyPending ? "pending" : "missing";

  return { approved, missing, statusMap, byType, documentStatus, required };
}

async function ptyDocsV4LoadDocuments(userId, role = "") {
  const params = [String(userId || "")];
  let where = "user_id::text=$1::text";
  if (role) {
    params.push(String(role || ""));
    where += \` AND role::text=$\${params.length}::text\`;
  }
  const r = await db(
    \`SELECT * FROM ride_documents
     WHERE \${where}
     ORDER BY created_at DESC\`,
    params
  );
  return r.rows.map(ptyDocsV4PublicLean);
}

async function ptyDocsV4SetUserStatus(userId, role, summary) {
  const finalStatus = summary.documentStatus || (summary.approved ? "approved" : "pending");
  const payload = {
    documentStatus: finalStatus,
    verificationStatus: finalStatus,
    statusMap: summary.statusMap || {},
    driverDocumentsApproved: role === "driver" ? Boolean(summary.approved) : undefined,
    canAcceptRides: role === "driver" ? Boolean(summary.approved) : undefined,
    riderVerified: role === "rider" ? Boolean(summary.approved) : undefined,
    identityVerified: role === "rider" ? Boolean(summary.approved) : undefined,
  };
  const r = await db(
    \`UPDATE ride_users
     SET document_status=$2::text,
         driver_docs=COALESCE(driver_docs,'{}'::jsonb) || $3::jsonb,
         updated_at=NOW()
     WHERE id::text=$1::text
     RETURNING *\`,
    [String(userId || ""), finalStatus, JSON.stringify(payload)]
  );
  return r.rows[0] || null;
}

function ptyDocsV4UserPayload(userRow, role, summary) {
  const base = typeof publicUser === "function" ? publicUser(userRow || {}) : userRow || {};
  return {
    ...base,
    documentStatus: summary.documentStatus,
    verificationStatus: summary.documentStatus,
    driverDocumentsApproved: role === "driver" ? Boolean(summary.approved) : Boolean(base.driverDocumentsApproved),
    canAcceptRides: role === "driver" ? Boolean(summary.approved) : Boolean(base.canAcceptRides),
    riderVerified: role === "rider" ? Boolean(summary.approved) : Boolean(base.riderVerified),
    identityVerified: role === "rider" ? Boolean(summary.approved) : Boolean(base.identityVerified),
  };
}

async function ptyDocsV4StatusResponse(req, res, roleOverride = "") {
  try {
    const role = String(roleOverride || req.query.role || req.body?.role || req.user?.role || "driver").toLowerCase() === "rider" ? "rider" : "driver";
    const userId = req.user?.id || asText(req.query.userId || req.body?.userId);
    if (!userId) return safeJson(res, 400, { ok: false, message: "userId requerido" });

    const documents = await ptyDocsV4LoadDocuments(userId, role);
    const summary = ptyDocsV4StatusSummary(documents, role);
    const userR = await db(\`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1\`, [String(userId)]);
    const user = ptyDocsV4UserPayload(userR.rows[0], role, summary);

    return safeJson(res, 200, {
      ok: true,
      approved: summary.approved,
      verified: summary.approved,
      status: summary.documentStatus,
      driverDocumentsApproved: role === "driver" ? summary.approved : undefined,
      canAcceptRides: role === "driver" ? summary.approved : undefined,
      riderVerified: role === "rider" ? summary.approved : undefined,
      identityVerified: role === "rider" ? summary.approved : undefined,
      missing: summary.missing,
      statusMap: summary.statusMap,
      byType: summary.byType,
      documents,
      user,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
}

app.get("/api/rider/verification/status", authOptional, async (req, res) => {
  return ptyDocsV4StatusResponse(req, res, "rider");
});

app.get("/api/rider/documents/status", authOptional, async (req, res) => {
  return ptyDocsV4StatusResponse(req, res, "rider");
});

app.get("/api/driver/documents/status", authOptional, async (req, res) => {
  return ptyDocsV4StatusResponse(req, res, "driver");
});

app.get("/api/driver/documents/approved", authOptional, async (req, res) => {
  return ptyDocsV4StatusResponse(req, res, "driver");
});

app.get("/api/documents/status", authOptional, async (req, res) => {
  return ptyDocsV4StatusResponse(req, res, "");
});

app.patch("/api/admin/documents/:id/status", authRequired, requireAdmin, async (req, res) => {
  try {
    const started = Date.now();
    const docId = asText(req.params.id);
    const status = asText(req.body.status || "pending").toLowerCase();
    const reason = asText(req.body.reason || "");
    const allowed = new Set(["pending", "approved", "rejected"]);

    if (!docId) return safeJson(res, 400, { ok: false, message: "Documento requerido" });
    if (!allowed.has(status)) return safeJson(res, 400, { ok: false, message: "status inválido" });

    const updated = await db(
      \`UPDATE ride_documents
       SET status=$2::text,
           reason=$3::text,
           updated_at=NOW()
       WHERE id::text=$1::text
       RETURNING id, user_id, role, type, status, reason, filename, mime_type, size_bytes, meta, created_at, updated_at\`,
      [String(docId), status, reason]
    );

    if (!updated.rows.length) {
      return safeJson(res, 404, { ok: false, message: "Documento no encontrado" });
    }

    const rawDoc = updated.rows[0];
    const document = ptyDocsV4PublicLean(rawDoc);
    const userId = String(document.userId || rawDoc.user_id || "");
    const role = String(document.role || rawDoc.role || "driver").toLowerCase() === "rider" ? "rider" : "driver";

    const documents = await ptyDocsV4LoadDocuments(userId, role);
    const summary = ptyDocsV4StatusSummary(documents, role);
    const userRow = await ptyDocsV4SetUserStatus(userId, role, summary);
    const user = ptyDocsV4UserPayload(userRow, role, summary);

    const payload = {
      ok: true,
      document,
      status,
      approved: summary.approved,
      documentStatus: summary.documentStatus,
      verificationStatus: summary.documentStatus,
      missing: summary.missing,
      statusMap: summary.statusMap,
      byType: summary.byType,
      user,
      elapsedMs: Date.now() - started,
      title: summary.approved ? "Documentos aprobados" : status === "approved" ? "Documento aprobado" : status === "rejected" ? "Documento rechazado" : "Documento pendiente",
      message: summary.approved
        ? "Tus documentos han sido aprobados exitosamente."
        : status === "approved"
          ? "Documento aprobado. Aún pueden quedar documentos pendientes."
          : status === "rejected"
            ? "Documento rechazado. Revisa el motivo en la app."
            : "Documento marcado como pendiente.",
    };

    try {
      emitToUser(userId, "documents:status", payload);
      emitToUser(userId, "documents:approved", payload);
      io.to("admins").emit("documents:status", payload);
    } catch {}

    return safeJson(res, 200, payload);
  } catch (e) {
    console.error("[PTY_DOCS_V4_STATUS_ERROR]", e);
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

`;

  src = src.slice(0, idx) + block + src.slice(idx);
  changes++;
  console.log("OK: rutas V4 insertadas antes de las rutas antiguas");
} else {
  console.log("OK ya existe: bloque V4");
}

fs.writeFileSync(file, src, "utf8");

console.log("");
console.log(`Listo. Cambios aplicados: ${changes}`);
console.log("");
console.log("Ahora ejecuta:");
console.log("git add ptydrive/server.js");
console.log('git commit -m "Fix PTYDrive document approval persistence and fast status"');
console.log("git push");
console.log("");
console.log("Luego Render hará deploy automático. Si no, usa Manual Deploy > Deploy latest commit.");
