#!/usr/bin/env node
/**
 * FIX PTYDrive V3 - aprobación de documentos + estado aprobado para app.
 *
 * Corrige:
 * 1) PATCH /api/admin/documents/:id/status devuelve estado aprobado real.
 * 2) GET /api/driver/documents/status para que la app detecte aprobación.
 * 3) GET /api/rider/documents/status para que rider vea aprobado.
 * 4) Respuesta incluye flags:
 *    driverDocumentsApproved, canAcceptRides, riderVerified, identityVerified.
 *
 * Uso en Codespaces:
 *   cd /workspaces/prodima-pay
 *   node fix_ptydrive_doc_approval_backend_v3.cjs
 *   git add ptydrive/server.js
 *   git commit -m "Fix document approval status and app notification backend"
 *   git push
 */

const fs = require("fs");
const path = require("path");

const root = process.cwd();
const serverPath = path.join(root, "ptydrive", "server.js");

function fail(msg) {
  console.error("ERROR:", msg);
  process.exit(1);
}

function backup(file) {
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const bak = `${file}.bak_doc_notify_${stamp}`;
  fs.copyFileSync(file, bak);
  console.log("Backup:", path.relative(root, bak));
}

if (!fs.existsSync(serverPath)) {
  fail(`No encontré ${serverPath}. Ejecuta desde la raíz del repo prodima-pay.`);
}

backup(serverPath);

let src = fs.readFileSync(serverPath, "utf8");
let changes = 0;

if (!src.includes("function buildDocumentUserPayload(")) {
  const needle = `function areDocumentsApproved(documents = [], role = "driver") {
  const required = requiredDocumentTypesForRole(role);
  const statusMap = buildStatusMap(documents);
  const missing = required.filter((type) => statusMap[type] !== "approved");
  return { approved: missing.length === 0, missing, statusMap };
}`;
  if (!src.includes(needle)) {
    console.log("AVISO: no encontré areDocumentsApproved exacto; insertaré helpers antes de las rutas de documentos.");
    const marker = '/* =========================================================\n   Documents upload / driver verification compatibility\n========================================================= */';
    if (!src.includes(marker)) fail("No encontré marcador de rutas de documentos.");
    const helperBlock = `
function buildDocumentUserPayload(userRow, role, approved, extra = {}) {
  const base = publicUser(userRow || {});
  const normalizedRole = String(role || base?.role || "").toLowerCase();
  return {
    ...base,
    ...extra,
    driverDocumentsApproved: normalizedRole === "driver" ? Boolean(approved) : Boolean(extra.driverDocumentsApproved),
    canAcceptRides: normalizedRole === "driver" ? Boolean(approved) : Boolean(extra.canAcceptRides),
    riderVerified: normalizedRole === "rider" ? Boolean(approved) : Boolean(extra.riderVerified),
    identityVerified: normalizedRole === "rider" ? Boolean(approved) : Boolean(extra.identityVerified),
    verificationStatus: approved ? "approved" : (base?.documentStatus || "pending"),
  };
}

`;
    src = src.replace(marker, helperBlock + marker);
  } else {
    const helperBlock = `${needle}

function buildDocumentUserPayload(userRow, role, approved, extra = {}) {
  const base = publicUser(userRow || {});
  const normalizedRole = String(role || base?.role || "").toLowerCase();
  return {
    ...base,
    ...extra,
    driverDocumentsApproved: normalizedRole === "driver" ? Boolean(approved) : Boolean(extra.driverDocumentsApproved),
    canAcceptRides: normalizedRole === "driver" ? Boolean(approved) : Boolean(extra.canAcceptRides),
    riderVerified: normalizedRole === "rider" ? Boolean(approved) : Boolean(extra.riderVerified),
    identityVerified: normalizedRole === "rider" ? Boolean(approved) : Boolean(extra.identityVerified),
    verificationStatus: approved ? "approved" : (base?.documentStatus || "pending"),
  };
}`;
    src = src.replace(needle, helperBlock);
  }
  changes++;
  console.log("OK: helper buildDocumentUserPayload agregado");
} else {
  console.log("OK ya existe: buildDocumentUserPayload");
}

/* Inserta status endpoints para app */
if (!src.includes('app.get("/api/driver/documents/status"')) {
  const marker = 'app.get("/api/driver/documents/approved"';
  const idx = src.indexOf(marker);
  if (idx === -1) fail('No encontré app.get("/api/driver/documents/approved"...');

  const statusRoutes = `
/* DOCUMENT STATUS ROUTES FOR MOBILE APP V3 */
app.get("/api/driver/documents/status", authOptional, async (req, res) => {
  try {
    const userId = req.user?.id || asText(req.query.userId || req.body?.userId);
    if (!userId) return safeJson(res, 400, { ok: false, message: "userId requerido" });

    const docsR = await db(
      \`SELECT * FROM ride_documents
       WHERE user_id::text=$1::text
         AND role::text='driver'
       ORDER BY created_at DESC\`,
      [String(userId)]
    );

    const documents = docsR.rows.map(documentToPublic);
    const result = areDocumentsApproved(documents, "driver");
    const userR = await db(\`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1\`, [String(userId)]);
    const user = buildDocumentUserPayload(userR.rows[0], "driver", result.approved);

    return safeJson(res, 200, {
      ok: true,
      approved: result.approved,
      status: result.approved ? "approved" : (documents.some((d) => d.status === "rejected") ? "rejected" : documents.length ? "pending" : "missing"),
      driverDocumentsApproved: result.approved,
      canAcceptRides: result.approved,
      missing: result.missing,
      statusMap: result.statusMap,
      documents,
      user,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/rider/documents/status", authOptional, async (req, res) => {
  try {
    const userId = req.user?.id || asText(req.query.userId || req.body?.userId);
    if (!userId) return safeJson(res, 400, { ok: false, message: "userId requerido" });

    const docsR = await db(
      \`SELECT * FROM ride_documents
       WHERE user_id::text=$1::text
         AND role::text='rider'
       ORDER BY created_at DESC\`,
      [String(userId)]
    );

    const documents = docsR.rows.map(documentToPublic);
    const result = areDocumentsApproved(documents, "rider");
    const status = result.approved
      ? "approved"
      : documents.some((d) => d.status === "rejected")
        ? "rejected"
        : documents.length
          ? "pending"
          : "missing";
    const userR = await db(\`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1\`, [String(userId)]);
    const user = buildDocumentUserPayload(userR.rows[0], "rider", result.approved, {
      riderVerified: result.approved,
      identityVerified: result.approved,
      verificationStatus: status,
    });

    return safeJson(res, 200, {
      ok: true,
      verified: result.approved,
      approved: result.approved,
      riderVerified: result.approved,
      identityVerified: result.approved,
      status,
      missing: result.missing,
      statusMap: result.statusMap,
      documents,
      user,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/documents/status", authOptional, async (req, res) => {
  try {
    const role = asText(req.query.role || req.body?.role || req.user?.role || "driver").toLowerCase() === "rider" ? "rider" : "driver";
    const userId = req.user?.id || asText(req.query.userId || req.body?.userId);
    if (!userId) return safeJson(res, 400, { ok: false, message: "userId requerido" });

    const docsR = await db(
      \`SELECT * FROM ride_documents
       WHERE user_id::text=$1::text
         AND role::text=$2::text
       ORDER BY created_at DESC\`,
      [String(userId), role]
    );

    const documents = docsR.rows.map(documentToPublic);
    const result = areDocumentsApproved(documents, role);
    const status = result.approved
      ? "approved"
      : documents.some((d) => d.status === "rejected")
        ? "rejected"
        : documents.length
          ? "pending"
          : "missing";
    const userR = await db(\`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1\`, [String(userId)]);
    const user = buildDocumentUserPayload(userR.rows[0], role, result.approved);

    return safeJson(res, 200, {
      ok: true,
      approved: result.approved,
      verified: result.approved,
      status,
      missing: result.missing,
      statusMap: result.statusMap,
      documents,
      user,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

`;
  src = src.slice(0, idx) + statusRoutes + src.slice(idx);
  changes++;
  console.log("OK: rutas /api/driver/documents/status y /api/rider/documents/status agregadas");
} else {
  console.log("OK ya existe: /api/driver/documents/status");
}

/* Inserta ruta de aprobación V3 antes de las anteriores */
const v3Marker = "/* SAFE DOC STATUS ROUTE V3 APPROVAL NOTIFY */";
if (!src.includes(v3Marker)) {
  const needle = 'app.patch("/api/admin/documents/:id/status"';
  const idx = src.indexOf(needle);
  if (idx === -1) fail('No encontré ruta app.patch("/api/admin/documents/:id/status"...');

  const route = `
${v3Marker}
app.patch("/api/admin/documents/:id/status", authRequired, requireAdmin, async (req, res) => {
  try {
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
       RETURNING *\`,
      [String(docId), status, reason]
    );

    if (!updated.rows.length) {
      return safeJson(res, 404, { ok: false, message: "Documento no encontrado" });
    }

    const rawDoc = updated.rows[0];
    const document = documentToPublic(rawDoc);
    const userId = String(document.userId || rawDoc.user_id || "");
    const role = String(document.role || rawDoc.role || "driver").toLowerCase() === "rider" ? "rider" : "driver";

    const docsR = await db(
      \`SELECT * FROM ride_documents
       WHERE user_id::text=$1::text
         AND role::text=$2::text
       ORDER BY created_at DESC\`,
      [userId, role]
    );

    const documents = docsR.rows.map(documentToPublic);
    const result = areDocumentsApproved(documents, role);
    const finalStatus = result.approved
      ? "approved"
      : documents.some((d) => String(d.status || "").toLowerCase() === "rejected")
        ? "rejected"
        : documents.length
          ? "pending"
          : "missing";

    await db(
      \`UPDATE ride_users
       SET document_status=$2::text,
           driver_docs=COALESCE(driver_docs,'{}'::jsonb) || $3::jsonb,
           updated_at=NOW()
       WHERE id::text=$1::text\`,
      [
        userId,
        finalStatus,
        JSON.stringify({
          [\`\${document.type}Status\`]: status,
          documentStatus: finalStatus,
          verificationStatus: finalStatus,
          driverDocumentsApproved: role === "driver" ? result.approved : undefined,
          canAcceptRides: role === "driver" ? result.approved : undefined,
          riderVerified: role === "rider" ? result.approved : undefined,
          identityVerified: role === "rider" ? result.approved : undefined,
        }),
      ]
    );

    const userR = await db(\`SELECT * FROM ride_users WHERE id::text=$1::text LIMIT 1\`, [userId]);
    const user = buildDocumentUserPayload(userR.rows[0], role, result.approved, {
      verificationStatus: finalStatus,
      riderVerified: role === "rider" ? result.approved : false,
      identityVerified: role === "rider" ? result.approved : false,
      driverDocumentsApproved: role === "driver" ? result.approved : false,
      canAcceptRides: role === "driver" ? result.approved : false,
    });

    const payload = {
      ok: true,
      document,
      documents,
      status,
      documentStatus: finalStatus,
      approved: result.approved,
      missing: result.missing,
      statusMap: result.statusMap,
      user,
      title: result.approved ? "Documentos aprobados" : status === "approved" ? "Documento aprobado" : status === "rejected" ? "Documento rechazado" : "Documento pendiente",
      message: result.approved
        ? "Tus documentos han sido aprobados exitosamente."
        : status === "approved"
          ? "Uno de tus documentos fue aprobado. Aún pueden quedar documentos pendientes."
          : status === "rejected"
            ? "Uno de tus documentos fue rechazado. Revisa el panel de verificación."
            : "Tu documento quedó pendiente de revisión.",
    };

    emitToUser(userId, "documents:status", payload);
    emitToUser(userId, "documents:approved", payload);
    io.to("admins").emit("documents:status", payload);

    return safeJson(res, 200, payload);
  } catch (e) {
    console.error("[ADMIN_DOCUMENT_STATUS_V3_ERROR]", e);
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

`;
  src = src.slice(0, idx) + route + src.slice(idx);
  changes++;
  console.log("OK: ruta V3 de aprobación insertada antes de la anterior");
} else {
  console.log("OK ya existe: ruta V3 de aprobación");
}

fs.writeFileSync(serverPath, src, "utf8");

console.log("");
console.log(`Listo. Cambios aplicados: ${changes}`);
console.log("");
console.log("Ahora ejecuta:");
console.log("git add ptydrive/server.js");
console.log('git commit -m "Fix document approval status and app notification backend"');
console.log("git push");
console.log("");
console.log("Luego Render hará deploy automático. Si no, usa Manual Deploy > Deploy latest commit.");
