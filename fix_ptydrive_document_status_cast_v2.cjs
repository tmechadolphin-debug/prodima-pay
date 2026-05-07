#!/usr/bin/env node
/**
 * FIX PTYDrive - aprobar/rechazar documentos:
 * error: operator does not exist: text = integer
 *
 * Causa:
 *   En Supabase puede haber columnas text/uuid/serial según la tabla existente.
 *   Al aprobar, una consulta compara id/user_id sin casteo y Postgres falla.
 *
 * Solución:
 *   - Agrega una ruta segura ANTES de la ruta actual:
 *     PATCH /api/admin/documents/:id/status
 *   - Usa id::text=$1::text y user_id::text=$1::text.
 *   - También ajusta listUserDocuments para comparar con casteo.
 *
 * Uso:
 *   cd /workspaces/prodima-pay
 *   node fix_ptydrive_document_status_cast_v2.cjs
 *   git add ptydrive/server.js
 *   git commit -m "Fix document approval type cast"
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
  const bak = `${file}.bak_doc_cast_${stamp}`;
  fs.copyFileSync(file, bak);
  console.log("Backup:", path.relative(root, bak));
}

if (!fs.existsSync(serverPath)) {
  fail(`No encontré ${serverPath}. Ejecuta desde la raíz del repo prodima-pay.`);
}

backup(serverPath);
let src = fs.readFileSync(serverPath, "utf8");
let changes = 0;

/* 1) Ajusta helper listUserDocuments si existe */
if (src.includes('let where = "user_id=$1";')) {
  src = src.replace('let where = "user_id=$1";', 'let where = "user_id::text=$1::text";');
  changes++;
  console.log("OK: listUserDocuments usa user_id::text");
} else {
  console.log("AVISO: no encontré let where = user_id=$1; puede estar ya corregido.");
}

if (src.includes('where += ` AND role=$${params.length}`;')) {
  src = src.replace('where += ` AND role=$${params.length}`;', 'where += ` AND role::text=$${params.length}::text`;');
  changes++;
  console.log("OK: listUserDocuments usa role::text");
}

/* 2) Inserta ruta segura antes de cualquier ruta de status existente */
const marker = '/* SAFE DOC STATUS ROUTE CAST FIX V2 */';

if (!src.includes(marker)) {
  const routeNeedle = 'app.patch("/api/admin/documents/:id/status"';
  const idx = src.indexOf(routeNeedle);
  if (idx === -1) {
    fail('No encontré app.patch("/api/admin/documents/:id/status"... en ptydrive/server.js');
  }

  const safeRoute = `
${marker}
app.patch("/api/admin/documents/:id/status", authRequired, requireAdmin, async (req, res) => {
  try {
    const docId = asText(req.params.id);
    const status = asText(req.body.status || "pending").toLowerCase();
    const reason = asText(req.body.reason || "");
    const allowed = new Set(["pending", "approved", "rejected"]);

    if (!docId) return safeJson(res, 400, { ok: false, message: "Documento requerido" });
    if (!allowed.has(status)) return safeJson(res, 400, { ok: false, message: "status inválido" });

    const r = await db(
      \`UPDATE ride_documents
       SET status=$2::text,
           reason=$3::text,
           updated_at=NOW()
       WHERE id::text=$1::text
       RETURNING *\`,
      [String(docId), status, reason]
    );

    if (!r.rows.length) {
      return safeJson(res, 404, { ok: false, message: "Documento no encontrado" });
    }

    const rawDoc = r.rows[0];
    const doc = documentToPublic(rawDoc);
    const userId = String(doc.userId || rawDoc.user_id || "");
    const role = String(doc.role || rawDoc.role || "driver").toLowerCase();

    const docsR = await db(
      \`SELECT * FROM ride_documents
       WHERE user_id::text=$1::text
         AND role::text=$2::text
       ORDER BY created_at DESC\`,
      [userId, role]
    );

    const allDocs = docsR.rows.map(documentToPublic);
    const result = areDocumentsApproved(allDocs, role);
    const nextUserStatus = result.approved
      ? "approved"
      : allDocs.some((d) => String(d.status || "").toLowerCase() === "rejected")
        ? "rejected"
        : "pending";

    if (userId) {
      await db(
        \`UPDATE ride_users
         SET document_status=$2::text,
             driver_docs=COALESCE(driver_docs,'{}'::jsonb) || $3::jsonb,
             updated_at=NOW()
         WHERE id::text=$1::text\`,
        [
          userId,
          nextUserStatus,
          JSON.stringify({
            [\`\${doc.type}Status\`]: status,
            driverDocumentsApproved: role === "driver" ? result.approved : undefined,
            canAcceptRides: role === "driver" ? result.approved : undefined,
            riderVerified: role === "rider" ? result.approved : undefined,
            identityVerified: role === "rider" ? result.approved : undefined,
          }),
        ]
      );
    }

    emitToUser(userId, "documents:status", {
      document: doc,
      status,
      approved: result.approved,
      missing: result.missing,
    });

    io.to("admins").emit("documents:status", {
      document: doc,
      status,
      approved: result.approved,
      missing: result.missing,
    });

    return safeJson(res, 200, {
      ok: true,
      document: doc,
      status,
      approved: result.approved,
      missing: result.missing,
      userDocumentStatus: nextUserStatus,
    });
  } catch (e) {
    console.error("[ADMIN_DOCUMENT_STATUS_ERROR]", e);
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

`;

  src = src.slice(0, idx) + safeRoute + src.slice(idx);
  changes++;
  console.log("OK: ruta segura de aprobar/rechazar documentos insertada antes de la original");
} else {
  console.log("OK ya existe: ruta segura V2");
}

/* 3) Corrige consultas exactas viejas si existen */
const replacements = [
  [
    "WHERE id=$1\\n       RETURNING *",
    "WHERE id::text=$1::text\\n       RETURNING *"
  ],
  [
    "WHERE id=$1 RETURNING *",
    "WHERE id::text=$1::text RETURNING *"
  ],
  [
    "WHERE id=$1",
    "WHERE id::text=$1::text"
  ],
];

for (const [from, to] of replacements) {
  if (src.includes(from)) {
    src = src.replaceAll(from, to);
    changes++;
    console.log("OK: reemplazo defensivo:", from);
  }
}

fs.writeFileSync(serverPath, src, "utf8");

console.log("");
console.log(`Listo. Cambios aplicados: ${changes}`);
console.log("");
console.log("Ahora ejecuta:");
console.log("git add ptydrive/server.js");
console.log('git commit -m "Fix document approval type cast"');
console.log("git push");
console.log("");
console.log("Luego Render hará deploy automático. Si no: Manual Deploy > Deploy latest commit.");
