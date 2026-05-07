#!/usr/bin/env node
/**
 * FIX PTYDrive documentos Render/Supabase
 *
 * Corrige en backend:
 * - POST /api/documents/upload               (subida FormData desde Expo)
 * - POST /api/driver/documents               (metadata de documentos driver)
 * - GET  /api/driver/documents/approved      (estatus aprobado/pendiente)
 * - GET  /api/admin/documents                (admin-web lista documentos)
 * - PATCH /api/admin/documents/:id/status    (aprobar/rechazar desde admin)
 * - PATCH /api/users/me/profile              (perfil/teléfono/icono/contacto)
 *
 * También agrega tabla ride_documents y dependencia multer.
 *
 * Uso en GitHub Codespaces o repo local:
 *   cd /workspaces/prodima-pay
 *   node fix_ptydrive_documents_backend_v1.cjs
 *   git add ptydrive/server.js ptydrive/package.json
 *   git commit -m "Fix PTYDrive document upload endpoints"
 *   git push
 */

const fs = require("fs");
const path = require("path");

const root = process.cwd();
const serverPath = path.join(root, "ptydrive", "server.js");
const pkgPath = path.join(root, "ptydrive", "package.json");

function fail(msg) {
  console.error("ERROR:", msg);
  process.exit(1);
}

function backup(file) {
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const bak = `${file}.bak_docs_${stamp}`;
  fs.copyFileSync(file, bak);
  console.log("Backup:", path.relative(root, bak));
}

if (!fs.existsSync(serverPath)) fail(`No encontré ${serverPath}. Ejecuta desde la raíz del repo prodima-pay.`);
if (!fs.existsSync(pkgPath)) fail(`No encontré ${pkgPath}.`);

backup(serverPath);
backup(pkgPath);

let server = fs.readFileSync(serverPath, "utf8");
let changes = 0;

/* package.json: multer */
const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf8"));
pkg.dependencies = pkg.dependencies || {};
if (!pkg.dependencies.multer) {
  pkg.dependencies.multer = "^1.4.5-lts.1";
  fs.writeFileSync(pkgPath, JSON.stringify(pkg, null, 2) + "\n", "utf8");
  changes++;
  console.log("OK: agregado multer en package.json");
} else {
  console.log("OK ya existe: multer en package.json");
}

/* import multer */
if (!server.includes('from "multer"') && !server.includes("from 'multer'")) {
  const importNeedle = `import crypto from "crypto";`;
  if (!server.includes(importNeedle)) fail("No encontré import crypto para insertar multer.");
  server = server.replace(importNeedle, `${importNeedle}\nimport multer from "multer";`);
  changes++;
  console.log("OK: import multer agregado");
} else {
  console.log("OK ya existe: import multer");
}

/* upload middleware */
if (!server.includes("const documentUpload = multer(")) {
  const appUseNeedle = `app.use(express.json({ limit: "20mb" }));`;
  if (!server.includes(appUseNeedle)) fail("No encontré app.use(express.json({ limit: \"20mb\" })).");
  const uploadBlock = `${appUseNeedle}

const documentUpload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: Math.max(1, Number(process.env.MAX_DOCUMENT_UPLOAD_MB || 25)) * 1024 * 1024,
  },
});`;
  server = server.replace(appUseNeedle, uploadBlock);
  changes++;
  console.log("OK: middleware documentUpload agregado");
} else {
  console.log("OK ya existe: documentUpload");
}

/* ride_documents table */
if (!server.includes("CREATE TABLE IF NOT EXISTS ride_documents")) {
  const marker = `  await db(\`CREATE TABLE IF NOT EXISTS pgcrypto;\`);`;
  // The generated server uses CREATE EXTENSION, so we insert after ride_users table instead.
  const afterUsers = `  await db(\`
    CREATE TABLE IF NOT EXISTS ride_rides`;
  const tableBlock = `  await db(\`
    CREATE TABLE IF NOT EXISTS ride_documents (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      user_id UUID REFERENCES ride_users(id) ON DELETE CASCADE,
      role TEXT DEFAULT 'driver',
      type TEXT NOT NULL,
      status TEXT DEFAULT 'pending',
      reason TEXT DEFAULT '',
      url TEXT DEFAULT '',
      filename TEXT DEFAULT '',
      mime_type TEXT DEFAULT '',
      size_bytes INTEGER DEFAULT 0,
      meta JSONB DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  \`);

`;
  if (!server.includes(afterUsers)) {
    fail("No encontré el bloque de creación ride_rides para insertar ride_documents.");
  }
  server = server.replace(afterUsers, tableBlock + afterUsers);
  changes++;
  console.log("OK: tabla ride_documents agregada a ensureDb");
} else {
  console.log("OK ya existe: tabla ride_documents");
}

/* indexes */
if (!server.includes("idx_ride_documents_user")) {
  const indexNeedle = `  await db(\`CREATE INDEX IF NOT EXISTS idx_ride_chat_ride ON ride_chat_messages(ride_id, created_at);\`);`;
  if (server.includes(indexNeedle)) {
    server = server.replace(
      indexNeedle,
      `${indexNeedle}
  await db(\`CREATE INDEX IF NOT EXISTS idx_ride_documents_user ON ride_documents(user_id, role, type);\`);
  await db(\`CREATE INDEX IF NOT EXISTS idx_ride_documents_status ON ride_documents(status, created_at DESC);\`);`
    );
    changes++;
    console.log("OK: índices ride_documents agregados");
  } else {
    console.log("AVISO: no encontré punto exacto para índices ride_documents; la tabla igual funciona.");
  }
}

/* helpers */
if (!server.includes("function documentToPublic(row")) {
  const helperNeedle = `function publicUser(u = {}) {
  if (!u) return null;
  return {
    id: u.id,
    email: u.email,
    name: u.name || "",
    role: u.role || "rider",
    phone: u.phone || "",
    markerIcon: u.marker_icon || "📍",
    documentStatus: u.document_status || "pending",
    trustedContact: u.trusted_contact || {},
    createdAt: u.created_at,
    updatedAt: u.updated_at,
  };
}`;
  if (!server.includes(helperNeedle)) fail("No encontré publicUser exacto para insertar helpers.");
  const helperBlock = `${helperNeedle}

function documentToPublic(row = {}) {
  if (!row) return null;
  return {
    id: row.id,
    userId: row.user_id,
    role: row.role || "driver",
    type: row.type || "",
    key: row.type || "",
    documentType: row.type || "",
    status: row.status || "pending",
    reason: row.reason || "",
    url: row.url || "",
    fileUrl: row.url || "",
    previewUrl: row.url || "",
    filename: row.filename || "",
    fileName: row.filename || "",
    mimeType: row.mime_type || "",
    sizeBytes: Number(row.size_bytes || 0),
    meta: row.meta || {},
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

const DRIVER_REQUIRED_DOCUMENT_TYPES = [
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

function requiredDocumentTypesForRole(role = "driver") {
  return String(role || "driver").toLowerCase() === "rider"
    ? ["identity"]
    : DRIVER_REQUIRED_DOCUMENT_TYPES;
}

async function listUserDocuments(userId, role = "") {
  const params = [userId];
  let where = "user_id=$1";
  if (role) {
    params.push(role);
    where += \` AND role=$\${params.length}\`;
  }
  const r = await db(
    \`SELECT * FROM ride_documents WHERE \${where} ORDER BY created_at DESC\`,
    params
  );
  return r.rows.map(documentToPublic);
}

function buildStatusMap(documents = []) {
  return documents.reduce((acc, doc) => {
    const type = String(doc.type || doc.documentType || doc.key || "").trim();
    if (type) acc[type] = String(doc.status || "pending").toLowerCase();
    return acc;
  }, {});
}

function areDocumentsApproved(documents = [], role = "driver") {
  const required = requiredDocumentTypesForRole(role);
  const statusMap = buildStatusMap(documents);
  const missing = required.filter((type) => statusMap[type] !== "approved");
  return { approved: missing.length === 0, missing, statusMap };
}`;
  server = server.replace(helperNeedle, helperBlock);
  changes++;
  console.log("OK: helpers de documentos agregados");
} else {
  console.log("OK ya existe: helpers documentos");
}

/* profile + document routes */
if (!server.includes('app.post("/api/documents/upload"')) {
  const documentsMarker = `/* =========================================================
   Documents / verification
========================================================= */`;
  if (!server.includes(documentsMarker)) fail("No encontré marcador Documents / verification para insertar rutas.");
  const routesBlock = `/* =========================================================
   Profile compatibility
========================================================= */
app.patch("/api/users/me/profile", authOptional, async (req, res) => {
  try {
    const userId = req.user?.id || asText(req.body.userId);
    if (!userId) return safeJson(res, 401, { ok: false, message: "Usuario requerido" });

    const phone = req.body.phone !== undefined ? cleanPhone(req.body.phone) : undefined;
    const markerIcon = req.body.markerIcon !== undefined
      ? asText(req.body.markerIcon)
      : req.body.riderMarkerIcon !== undefined
        ? asText(req.body.riderMarkerIcon)
        : req.body.userMarkerIcon !== undefined
          ? asText(req.body.userMarkerIcon)
          : undefined;
    const trustedContact = req.body.trustedContact !== undefined ? req.body.trustedContact : undefined;
    const name = req.body.name !== undefined ? asText(req.body.name) : undefined;

    const r = await db(
      \`UPDATE ride_users SET
        name=COALESCE($2, name),
        phone=COALESCE($3, phone),
        marker_icon=COALESCE($4, marker_icon),
        trusted_contact=COALESCE($5::jsonb, trusted_contact),
        updated_at=NOW()
       WHERE id=$1
       RETURNING *\`,
      [
        userId,
        name ?? null,
        phone ?? null,
        markerIcon ?? null,
        trustedContact !== undefined ? JSON.stringify(trustedContact || {}) : null,
      ]
    );

    if (!r.rows.length) return safeJson(res, 404, { ok: false, message: "Usuario no encontrado" });
    return safeJson(res, 200, { ok: true, user: publicUser(r.rows[0]) });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

/* =========================================================
   Documents upload / driver verification compatibility
========================================================= */
app.post("/api/documents/upload", authOptional, documentUpload.single("file"), async (req, res) => {
  try {
    const userId = req.user?.id || asText(req.body.userId);
    const type = asText(req.body.type || "documento");
    const role = asText(req.body.role || req.user?.role || "driver").toLowerCase();
    if (!userId) return safeJson(res, 400, { ok: false, message: "userId requerido" });
    if (!type) return safeJson(res, 400, { ok: false, message: "type requerido" });
    if (!req.file) return safeJson(res, 400, { ok: false, message: "file requerido" });

    const mime = req.file.mimetype || "image/jpeg";
    const dataUrl = \`data:\${mime};base64,\${req.file.buffer.toString("base64")}\`;

    const meta = { ...req.body };
    delete meta.userId;
    delete meta.type;
    delete meta.role;

    const inserted = await db(
      \`INSERT INTO ride_documents(user_id, role, type, status, url, filename, mime_type, size_bytes, meta)
       VALUES($1,$2,$3,'pending',$4,$5,$6,$7,$8::jsonb)
       RETURNING *\`,
      [
        userId,
        role,
        type,
        dataUrl,
        req.file.originalname || \`\${type}.jpg\`,
        mime,
        Number(req.file.size || 0),
        JSON.stringify(meta || {}),
      ]
    );

    const document = documentToPublic(inserted.rows[0]);

    await db(
      \`UPDATE ride_users
       SET document_status='pending',
           driver_docs=COALESCE(driver_docs,'{}'::jsonb) || $2::jsonb,
           updated_at=NOW()
       WHERE id=$1\`,
      [
        userId,
        JSON.stringify({
          [type]: dataUrl,
          [\`\${type}Status\`]: "pending",
          ...meta,
        }),
      ]
    );

    const userR = await db(\`SELECT * FROM ride_users WHERE id=$1 LIMIT 1\`, [userId]);
    io.to("admins").emit("documents:pending", { document, user: publicUser(userR.rows[0]) });

    return safeJson(res, 201, { ok: true, document, user: publicUser(userR.rows[0]) });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.post("/api/driver/documents", authOptional, async (req, res) => {
  try {
    const userId = req.user?.id || asText(req.body.driverId || req.body.userId);
    const docs = req.body.documents || req.body.docs || {};
    if (!userId) return safeJson(res, 400, { ok: false, message: "driverId requerido" });

    const vehicle = {
      brand: docs.marcaVehiculo || docs.brand || docs.make || "",
      make: docs.marcaVehiculo || docs.make || docs.brand || "",
      model: docs.modeloVehiculo || docs.model || "",
      plate: docs.placa || docs.plate || "",
      color: docs.colorVehiculo || docs.color || "",
      year: docs.anioVehiculo || docs.year || "",
      serviceTier: docs.serviceTier || docs.enrollmentType || "viaje",
      vehicleType: docs.vehicleType || (docs.enrollmentType === "moto" ? "moto" : "car"),
    };

    const r = await db(
      \`UPDATE ride_users
       SET document_status='pending',
           phone=COALESCE(NULLIF($2,''), phone),
           driver_docs=COALESCE(driver_docs,'{}'::jsonb) || $3::jsonb,
           updated_at=NOW()
       WHERE id=$1
       RETURNING *\`,
      [
        userId,
        cleanPhone(docs.telefonoContacto || ""),
        JSON.stringify({
          ...docs,
          vehicle,
          driverDocumentsApproved: false,
          canAcceptRides: false,
        }),
      ]
    );

    const documents = await listUserDocuments(userId, "driver");
    io.to("admins").emit("documents:pending", { user: publicUser(r.rows[0]), documents });
    return safeJson(res, 200, { ok: true, user: publicUser(r.rows[0]), documents });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/driver/documents/approved", authOptional, async (req, res) => {
  try {
    const userId = req.user?.id || asText(req.query.userId || req.body?.userId);
    if (!userId) return safeJson(res, 400, { ok: false, message: "userId requerido" });
    const documents = await listUserDocuments(userId, "driver");
    const result = areDocumentsApproved(documents, "driver");
    const userR = await db(\`SELECT * FROM ride_users WHERE id=$1 LIMIT 1\`, [userId]);
    return safeJson(res, 200, {
      ok: true,
      approved: result.approved,
      driverDocumentsApproved: result.approved,
      canAcceptRides: result.approved,
      missing: result.missing,
      statusMap: result.statusMap,
      documents,
      user: userR.rows[0] ? publicUser(userR.rows[0]) : null,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/rider/documents/approved", authOptional, async (req, res) => {
  try {
    const userId = req.user?.id || asText(req.query.userId || req.body?.userId);
    if (!userId) return safeJson(res, 400, { ok: false, message: "userId requerido" });
    const documents = await listUserDocuments(userId, "rider");
    const result = areDocumentsApproved(documents, "rider");
    const userR = await db(\`SELECT * FROM ride_users WHERE id=$1 LIMIT 1\`, [userId]);
    return safeJson(res, 200, {
      ok: true,
      verified: result.approved,
      approved: result.approved,
      status: result.approved ? "approved" : (documents[0]?.status || "pending"),
      missing: result.missing,
      statusMap: result.statusMap,
      documents,
      user: userR.rows[0] ? publicUser(userR.rows[0]) : null,
    });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/admin/documents", authRequired, requireAdmin, async (_req, res) => {
  try {
    const r = await db(
      \`SELECT d.*, u.email AS user_email, u.name AS user_name, u.role AS user_role
       FROM ride_documents d
       LEFT JOIN ride_users u ON u.id=d.user_id
       ORDER BY d.created_at DESC
       LIMIT 1000\`
    );
    const documents = r.rows.map((row) => ({
      ...documentToPublic(row),
      userEmail: row.user_email || "",
      userName: row.user_name || "",
      userRole: row.user_role || row.role || "",
      user: { id: row.user_id, email: row.user_email || "", name: row.user_name || "", role: row.user_role || "" },
    }));
    return safeJson(res, 200, { ok: true, documents });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.patch("/api/admin/documents/:id/status", authRequired, requireAdmin, async (req, res) => {
  try {
    const status = asText(req.body.status || "pending").toLowerCase();
    const reason = asText(req.body.reason || "");
    const allowed = new Set(["pending", "approved", "rejected"]);
    if (!allowed.has(status)) return safeJson(res, 400, { ok: false, message: "status inválido" });

    const r = await db(
      \`UPDATE ride_documents
       SET status=$2, reason=$3, updated_at=NOW()
       WHERE id=$1
       RETURNING *\`,
      [req.params.id, status, reason]
    );
    if (!r.rows.length) return safeJson(res, 404, { ok: false, message: "Documento no encontrado" });

    const doc = documentToPublic(r.rows[0]);
    const allDocs = await listUserDocuments(doc.userId, doc.role);
    const result = areDocumentsApproved(allDocs, doc.role);
    await db(
      \`UPDATE ride_users
       SET document_status=$2,
           driver_docs=COALESCE(driver_docs,'{}'::jsonb) || $3::jsonb,
           updated_at=NOW()
       WHERE id=$1\`,
      [
        doc.userId,
        result.approved ? "approved" : (allDocs.some((d) => d.status === "rejected") ? "rejected" : "pending"),
        JSON.stringify({
          [\`\${doc.type}Status\`]: status,
          driverDocumentsApproved: doc.role === "driver" ? result.approved : undefined,
          canAcceptRides: doc.role === "driver" ? result.approved : undefined,
        }),
      ]
    );

    emitToUser(doc.userId, "documents:status", { document: doc, status, approved: result.approved, missing: result.missing });
    io.to("admins").emit("documents:status", { document: doc, status, approved: result.approved });

    return safeJson(res, 200, { ok: true, document: doc, approved: result.approved, missing: result.missing });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

`;
  server = server.replace(documentsMarker, routesBlock + documentsMarker);
  changes++;
  console.log("OK: rutas documentos/perfil agregadas");
} else {
  console.log("OK ya existe: /api/documents/upload");
}

/* Avoid duplicate broken app.patch("/api/profile" handler using app._router.handle? Keep harmless.
   Write server. */
fs.writeFileSync(serverPath, server, "utf8");

console.log("");
console.log(`Listo. Cambios aplicados: ${changes}`);
console.log("");
console.log("Ahora ejecuta:");
console.log("git add ptydrive/server.js ptydrive/package.json");
console.log('git commit -m "Fix PTYDrive document upload endpoints"');
console.log("git push");
console.log("");
console.log("Luego Render hará deploy. Si no lo hace: Manual Deploy > Deploy latest commit.");
