#!/usr/bin/env node
/**
 * PTYDrive FIX - Google Places + Reverse Geocoding
 *
 * Backend Render:
 * - GET /api/places/search?q=&lat=&lng=&limit=
 *   Usa Google Places Autocomplete + Place Details para devolver nombre, dirección y coordenadas.
 * - GET /api/places/autocomplete?q=&lat=&lng=&limit=
 *   Alias compatible para autocomplete.
 * - GET /api/places/details?placeId=
 *   Devuelve detalle de un Place ID.
 * - GET /api/places/reverse?lat=&lng=
 *   Reverse geocoding Google para convertir coordenadas a nombre/dirección.
 * - GET /api/geocode/reverse?lat=&lng=
 *   Alias.
 *
 * Requiere variable en Render:
 *   GOOGLE_MAPS_API_KEY=TU_API_KEY_DE_GOOGLE
 *
 * Uso en Codespaces:
 *   cd /workspaces/prodima-pay
 *   node fix_ptydrive_google_places_backend_v1.cjs
 *   git add ptydrive/server.js
 *   git commit -m "Add Google Places search and reverse geocoding"
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
  const bak = `${file}.bak_google_places_${stamp}`;
  fs.copyFileSync(file, bak);
  console.log("Backup:", path.relative(root, bak));
}

if (!fs.existsSync(serverPath)) {
  fail(`No encontré ${serverPath}. Ejecuta desde la raíz del repo prodima-pay.`);
}

backup(serverPath);

let src = fs.readFileSync(serverPath, "utf8");
let changes = 0;

const marker = "/* PTY GOOGLE PLACES + GEOCODING V1 */";

if (!src.includes(marker)) {
  const insertBeforeCandidates = [
    '/* =========================================================\n   Rides',
    'app.post("/api/rides"',
    'app.get("/api/rides"'
  ];

  let idx = -1;
  for (const c of insertBeforeCandidates) {
    idx = src.indexOf(c);
    if (idx !== -1) break;
  }
  if (idx === -1) fail("No encontré punto de inserción antes de Rides.");

  const block = `
${marker}
function ptyGoogleKey() {
  return String(process.env.GOOGLE_MAPS_API_KEY || process.env.GOOGLE_API_KEY || "").trim();
}

function ptyCompactPlaceLabel(value = "", fallback = "") {
  const text = String(value || "").trim();
  if (!text) return String(fallback || "").trim();
  return text
    .replace(/,\\s*Panamá\\s*,\\s*Panamá$/i, ", Panamá")
    .replace(/,\\s*Panama\\s*,\\s*Panama$/i, ", Panamá")
    .replace(/\\s+/g, " ")
    .trim();
}

function ptyAsNum(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function ptyGooglePlaceFromDetails(result = {}, fallbackName = "") {
  const loc = result?.geometry?.location || {};
  const lat = ptyAsNum(loc.lat);
  const lng = ptyAsNum(loc.lng);
  const name = ptyCompactPlaceLabel(result.name || fallbackName || result.formatted_address || "Destino");
  const address = ptyCompactPlaceLabel(result.formatted_address || name, name);
  return {
    id: result.place_id || \`google_\${lat}_\${lng}\`,
    placeId: result.place_id || "",
    googlePlaceId: result.place_id || "",
    source: "google_places",
    title: name,
    short: name,
    name,
    placeName: name,
    exactName: name,
    destinationName: name,
    destinationLabel: name,
    destinationTitle: name,
    lat,
    lng,
    address,
    fullAddress: address,
    display_name: address,
    searchSubtitle: address,
    subtitle: address,
    types: result.types || [],
    businessStatus: result.business_status || "",
  };
}

async function ptyGoogleJson(url) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 8500);
  try {
    const r = await fetch(url, {
      signal: controller.signal,
      headers: { Accept: "application/json" },
    });
    const json = await r.json().catch(() => ({}));
    return { ok: r.ok, status: r.status, json };
  } finally {
    clearTimeout(timeout);
  }
}

async function ptyGooglePlaceDetails(placeId = "") {
  const key = ptyGoogleKey();
  if (!key) throw new Error("GOOGLE_MAPS_API_KEY no configurada en Render");
  if (!placeId) throw new Error("placeId requerido");

  const url = new URL("https://maps.googleapis.com/maps/api/place/details/json");
  url.searchParams.set("place_id", placeId);
  url.searchParams.set("language", "es");
  url.searchParams.set("fields", "place_id,name,formatted_address,geometry,types,business_status");
  url.searchParams.set("key", key);

  const { json } = await ptyGoogleJson(url);
  if (json.status && !["OK"].includes(json.status)) {
    throw new Error(json.error_message || \`Google details: \${json.status}\`);
  }
  return ptyGooglePlaceFromDetails(json.result || {});
}

async function ptyGoogleAutocompleteWithDetails({ q, lat, lng, limit }) {
  const key = ptyGoogleKey();
  if (!key) throw new Error("GOOGLE_MAPS_API_KEY no configurada en Render");
  const cleanQ = String(q || "").trim();
  if (!cleanQ) return [];

  const max = Math.max(1, Math.min(12, Number(limit || 8)));
  const url = new URL("https://maps.googleapis.com/maps/api/place/autocomplete/json");
  url.searchParams.set("input", cleanQ);
  url.searchParams.set("language", "es");
  url.searchParams.set("components", "country:pa");
  url.searchParams.set("types", "establishment|geocode");
  url.searchParams.set("key", key);
  const latN = ptyAsNum(lat);
  const lngN = ptyAsNum(lng);
  if (latN !== null && lngN !== null) {
    url.searchParams.set("location", \`\${latN},\${lngN}\`);
    url.searchParams.set("radius", "60000");
  }

  const { json } = await ptyGoogleJson(url);
  if (json.status && !["OK", "ZERO_RESULTS"].includes(json.status)) {
    throw new Error(json.error_message || \`Google autocomplete: \${json.status}\`);
  }

  const predictions = Array.isArray(json.predictions) ? json.predictions.slice(0, max) : [];
  const places = [];
  for (const p of predictions) {
    try {
      const details = await ptyGooglePlaceDetails(p.place_id);
      const name = ptyCompactPlaceLabel(details.name || p.structured_formatting?.main_text || p.description || cleanQ, cleanQ);
      places.push({
        ...details,
        title: name,
        short: name,
        name,
        placeName: name,
        exactName: name,
        predictionDescription: p.description || "",
        searchSubtitle: details.fullAddress || p.structured_formatting?.secondary_text || p.description || "",
      });
    } catch {}
  }

  return places.filter((p) => Number.isFinite(Number(p.lat)) && Number.isFinite(Number(p.lng)));
}

async function ptyGoogleTextSearch({ q, lat, lng, limit }) {
  const key = ptyGoogleKey();
  if (!key) throw new Error("GOOGLE_MAPS_API_KEY no configurada en Render");
  const cleanQ = String(q || "").trim();
  if (!cleanQ) return [];

  const max = Math.max(1, Math.min(12, Number(limit || 8)));
  const url = new URL("https://maps.googleapis.com/maps/api/place/textsearch/json");
  url.searchParams.set("query", cleanQ.toLowerCase().includes("panama") || cleanQ.toLowerCase().includes("panamá") ? cleanQ : \`\${cleanQ}, Panamá\`);
  url.searchParams.set("language", "es");
  url.searchParams.set("region", "pa");
  url.searchParams.set("key", key);
  const latN = ptyAsNum(lat);
  const lngN = ptyAsNum(lng);
  if (latN !== null && lngN !== null) {
    url.searchParams.set("location", \`\${latN},\${lngN}\`);
    url.searchParams.set("radius", "60000");
  }

  const { json } = await ptyGoogleJson(url);
  if (json.status && !["OK", "ZERO_RESULTS"].includes(json.status)) {
    throw new Error(json.error_message || \`Google text search: \${json.status}\`);
  }

  return (Array.isArray(json.results) ? json.results : [])
    .slice(0, max)
    .map((r) => ptyGooglePlaceFromDetails(r, cleanQ))
    .filter((p) => Number.isFinite(Number(p.lat)) && Number.isFinite(Number(p.lng)));
}

async function ptyGoogleReverse({ lat, lng }) {
  const key = ptyGoogleKey();
  if (!key) throw new Error("GOOGLE_MAPS_API_KEY no configurada en Render");
  const latN = ptyAsNum(lat);
  const lngN = ptyAsNum(lng);
  if (latN === null || lngN === null) throw new Error("lat/lng requeridos");

  const url = new URL("https://maps.googleapis.com/maps/api/geocode/json");
  url.searchParams.set("latlng", \`\${latN},\${lngN}\`);
  url.searchParams.set("language", "es");
  url.searchParams.set("region", "pa");
  url.searchParams.set("key", key);

  const { json } = await ptyGoogleJson(url);
  if (json.status && !["OK", "ZERO_RESULTS"].includes(json.status)) {
    throw new Error(json.error_message || \`Google geocode: \${json.status}\`);
  }

  const results = Array.isArray(json.results) ? json.results : [];
  const best =
    results.find((r) => (r.types || []).includes("premise")) ||
    results.find((r) => (r.types || []).includes("establishment")) ||
    results.find((r) => (r.types || []).includes("street_address")) ||
    results.find((r) => (r.types || []).includes("route")) ||
    results[0] ||
    {};

  const address = ptyCompactPlaceLabel(best.formatted_address, \`\${latN.toFixed(5)}, \${lngN.toFixed(5)}\`);
  const nameComponent =
    (best.address_components || []).find((c) => (c.types || []).includes("premise")) ||
    (best.address_components || []).find((c) => (c.types || []).includes("establishment")) ||
    (best.address_components || [])[0] ||
    {};
  const name = ptyCompactPlaceLabel(nameComponent.long_name || address, address);

  const place = {
    id: best.place_id || \`reverse_\${latN}_\${lngN}\`,
    placeId: best.place_id || "",
    googlePlaceId: best.place_id || "",
    source: "google_geocode",
    title: name,
    short: name,
    name,
    placeName: name,
    exactName: name,
    destinationName: name,
    destinationLabel: name,
    destinationTitle: name,
    lat: latN,
    lng: lngN,
    address,
    fullAddress: address,
    display_name: address,
    searchSubtitle: address,
    subtitle: address,
    types: best.types || [],
  };

  return {
    label: name,
    title: name,
    name,
    address,
    display_name: address,
    place,
    results: results.slice(0, 5),
  };
}

async function ptyPlacesSearchHandler(req, res) {
  try {
    const q = asText(req.query.q || req.query.input || req.query.query || "");
    const lat = req.query.lat;
    const lng = req.query.lng;
    const limit = req.query.limit || 8;
    if (!q) return safeJson(res, 200, { ok: true, places: [], results: [] });

    let places = [];
    try {
      places = await ptyGoogleAutocompleteWithDetails({ q, lat, lng, limit });
    } catch (e) {
      console.warn("[GOOGLE_AUTOCOMPLETE_WARN]", e?.message || e);
    }

    if (!places.length) {
      places = await ptyGoogleTextSearch({ q, lat, lng, limit });
    }

    const seen = new Set();
    const clean = places.filter((p) => {
      const key = String(p.placeId || \`\${p.title}_\${Number(p.lat).toFixed(5)}_\${Number(p.lng).toFixed(5)}\`).toLowerCase();
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });

    return safeJson(res, 200, { ok: true, provider: "google", places: clean, results: clean });
  } catch (e) {
    console.error("[GOOGLE_PLACES_SEARCH_ERROR]", e);
    return safeJson(res, 500, { ok: false, message: String(e?.message || e), places: [], results: [] });
  }
}

app.get("/api/places/search", authOptional, ptyPlacesSearchHandler);
app.get("/api/places/autocomplete", authOptional, ptyPlacesSearchHandler);
app.get("/api/google/places/autocomplete", authOptional, ptyPlacesSearchHandler);

app.get("/api/places/details", authOptional, async (req, res) => {
  try {
    const placeId = asText(req.query.placeId || req.query.place_id || "");
    const place = await ptyGooglePlaceDetails(placeId);
    return safeJson(res, 200, { ok: true, place, result: place });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/google/places/details", authOptional, async (req, res) => {
  try {
    const placeId = asText(req.query.placeId || req.query.place_id || "");
    const place = await ptyGooglePlaceDetails(placeId);
    return safeJson(res, 200, { ok: true, place, result: place });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

async function ptyReverseHandler(req, res) {
  try {
    const out = await ptyGoogleReverse({ lat: req.query.lat, lng: req.query.lng });
    return safeJson(res, 200, { ok: true, ...out });
  } catch (e) {
    console.error("[GOOGLE_REVERSE_ERROR]", e);
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
}

app.get("/api/places/reverse", authOptional, ptyReverseHandler);
app.get("/api/geocode/reverse", authOptional, ptyReverseHandler);
app.get("/api/google/geocode/reverse", authOptional, ptyReverseHandler);

`;

  src = src.slice(0, idx) + block + src.slice(idx);
  changes++;
  console.log("OK: endpoints Google Places/Geocoding agregados");
} else {
  console.log("OK ya existe: bloque Google Places V1");
}

fs.writeFileSync(serverPath, src, "utf8");

console.log("");
console.log(`Listo. Cambios aplicados: ${changes}`);
console.log("");
console.log("IMPORTANTE: en Render agrega:");
console.log("GOOGLE_MAPS_API_KEY=TU_API_KEY");
console.log("");
console.log("Luego ejecuta:");
console.log("git add ptydrive/server.js");
console.log('git commit -m "Add Google Places search and reverse geocoding"');
console.log("git push");
console.log("");
console.log("Luego Render hará deploy. Si no, Manual Deploy > Deploy latest commit.");
