#!/usr/bin/env node
/**
 * PTYDrive FIX Google Places V2:
 * - Evita que el backend devuelva Plus Codes / códigos tipo "XFQM+C22" o "114375-3".
 * - Prioriza nombres reales de Google Places: establishment, point_of_interest, premise.
 * - Reverse geocoding primero busca POIs cercanos con Nearby Search y luego usa Geocoding.
 *
 * Uso en Codespaces:
 *   cd /workspaces/prodima-pay
 *   node fix_ptydrive_google_places_backend_v2_clean_names.cjs
 *   git add ptydrive/server.js
 *   git commit -m "Clean Google Places labels and reverse geocoding names"
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
  const bak = `${file}.bak_google_places_v2_${stamp}`;
  fs.copyFileSync(file, bak);
  console.log("Backup:", path.relative(root, bak));
}

if (!fs.existsSync(serverPath)) fail(`No encontré ${serverPath}. Ejecuta desde la raíz del repo prodima-pay.`);
backup(serverPath);

let src = fs.readFileSync(serverPath, "utf8");
let changes = 0;

const marker = "/* PTY GOOGLE PLACES + GEOCODING V2 CLEAN HUMAN NAMES */";

if (!src.includes(marker)) {
  let idx = src.indexOf("/* PTY GOOGLE PLACES + GEOCODING V1 */");
  if (idx === -1) idx = src.indexOf('app.get("/api/places/search"');
  if (idx === -1) idx = src.indexOf('app.get("/api/places/reverse"');
  if (idx === -1) fail("No encontré rutas Google Places V1 para insertar V2 antes.");

  const block = `
${marker}
function ptyGv2Key() {
  return String(process.env.GOOGLE_MAPS_API_KEY || process.env.GOOGLE_API_KEY || "").trim();
}

function ptyGv2Num(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function ptyGv2LooksLikeCode(value = "") {
  const text = String(value || "").trim();
  if (!text) return true;
  if (/^[23456789CFGHJMPQRVWX]{4,}\\+[23456789CFGHJMPQRVWX]{2,}/i.test(text)) return true; // Plus Code
  if (/^[A-Z0-9]{3,}\\+[A-Z0-9]{2,}/i.test(text)) return true; // XFQM+C22
  if (/^\\d{3,}[-–]\\d{1,}$/i.test(text)) return true; // 114375-3
  if (/^\\d+\\.\\d+\\s*,\\s*-?\\d+\\.\\d+/.test(text)) return true; // coordenadas
  if (/^(destino|recogida)\\s+-?\\d+\\.\\d+\\s*,\\s*-?\\d+\\.\\d+/i.test(text)) return true;
  return false;
}

function ptyGv2CleanAddress(value = "", fallback = "") {
  let text = String(value || "").trim();
  text = text
    .replace(/^[A-Z0-9]{3,}\\+[A-Z0-9]{2,}\\s*,\\s*/i, "")
    .replace(/^\\d{3,}[-–]\\d{1,}\\s*,\\s*/i, "")
    .replace(/,\\s*Panamá\\s*,\\s*Panamá$/i, ", Panamá")
    .replace(/,\\s*Panama\\s*,\\s*Panama$/i, ", Panamá")
    .replace(/,\\s*Provincia de Panamá\\s*,\\s*Panamá$/i, ", Panamá")
    .replace(/\\s+/g, " ")
    .trim();
  if (!text || ptyGv2LooksLikeCode(text)) return String(fallback || "").trim();
  return text;
}

function ptyGv2PickComponent(components = []) {
  const preferredTypes = [
    "establishment",
    "point_of_interest",
    "premise",
    "subpremise",
    "street_address",
    "route",
    "neighborhood",
    "sublocality",
    "locality",
  ];
  for (const type of preferredTypes) {
    const found = components.find((c) => Array.isArray(c.types) && c.types.includes(type));
    const name = ptyGv2CleanAddress(found?.long_name || found?.short_name || "");
    if (name && !ptyGv2LooksLikeCode(name)) return name;
  }
  return "";
}

function ptyGv2ScorePlace(place = {}) {
  const types = Array.isArray(place.types) ? place.types : [];
  const name = ptyGv2CleanAddress(place.name || place.title || place.formatted_address || "");
  let score = 0;
  if (name && !ptyGv2LooksLikeCode(name)) score += 1000;
  if (types.includes("establishment")) score += 280;
  if (types.includes("point_of_interest")) score += 260;
  if (types.includes("premise")) score += 240;
  if (types.includes("shopping_mall")) score += 220;
  if (types.includes("store")) score += 120;
  if (types.includes("parking")) score -= 70;
  if (types.includes("route")) score -= 180;
  if (types.includes("plus_code")) score -= 500;
  if (String(place.business_status || "").toUpperCase() === "OPERATIONAL") score += 40;
  if (ptyGv2LooksLikeCode(name)) score -= 1000;
  return score;
}

function ptyGv2PlaceFromGoogle(result = {}, fallbackName = "") {
  const loc = result?.geometry?.location || {};
  const lat = ptyGv2Num(typeof loc.lat === "function" ? loc.lat() : loc.lat);
  const lng = ptyGv2Num(typeof loc.lng === "function" ? loc.lng() : loc.lng);
  const address = ptyGv2CleanAddress(result.formatted_address || result.vicinity || result.description || "", fallbackName);
  let name = ptyGv2CleanAddress(result.name || result.structured_formatting?.main_text || fallbackName || "", "");
  if (!name || ptyGv2LooksLikeCode(name)) name = ptyGv2PickComponent(result.address_components || []);
  if (!name || ptyGv2LooksLikeCode(name)) name = address || fallbackName || "Destino";
  if (ptyGv2LooksLikeCode(name) && address) name = address;

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
    address: address || name,
    fullAddress: address || name,
    display_name: address || name,
    searchSubtitle: address || name,
    subtitle: address || name,
    types: result.types || [],
    businessStatus: result.business_status || "",
    _score: ptyGv2ScorePlace({ ...result, name }),
  };
}

async function ptyGv2Json(url) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 8500);
  try {
    const r = await fetch(url, { signal: controller.signal, headers: { Accept: "application/json" } });
    const json = await r.json().catch(() => ({}));
    return { ok: r.ok, status: r.status, json };
  } finally {
    clearTimeout(timeout);
  }
}

async function ptyGv2Details(placeId = "", fallbackName = "") {
  const key = ptyGv2Key();
  if (!key) throw new Error("GOOGLE_MAPS_API_KEY no configurada en Render");
  if (!placeId) throw new Error("placeId requerido");
  const url = new URL("https://maps.googleapis.com/maps/api/place/details/json");
  url.searchParams.set("place_id", placeId);
  url.searchParams.set("language", "es");
  url.searchParams.set("fields", "place_id,name,formatted_address,geometry,types,business_status,address_components,vicinity");
  url.searchParams.set("key", key);
  const { json } = await ptyGv2Json(url);
  if (json.status && json.status !== "OK") throw new Error(json.error_message || \`Google details: \${json.status}\`);
  return ptyGv2PlaceFromGoogle(json.result || {}, fallbackName);
}

async function ptyGv2Autocomplete({ q, lat, lng, limit }) {
  const key = ptyGv2Key();
  if (!key) throw new Error("GOOGLE_MAPS_API_KEY no configurada en Render");
  const cleanQ = String(q || "").trim();
  if (!cleanQ) return [];

  const max = Math.max(1, Math.min(12, Number(limit || 8)));
  const url = new URL("https://maps.googleapis.com/maps/api/place/autocomplete/json");
  url.searchParams.set("input", cleanQ);
  url.searchParams.set("language", "es");
  url.searchParams.set("components", "country:pa");
  url.searchParams.set("key", key);
  const latN = ptyGv2Num(lat);
  const lngN = ptyGv2Num(lng);
  if (latN !== null && lngN !== null) {
    url.searchParams.set("location", \`\${latN},\${lngN}\`);
    url.searchParams.set("radius", "60000");
  }

  const { json } = await ptyGv2Json(url);
  if (json.status && !["OK", "ZERO_RESULTS"].includes(json.status)) throw new Error(json.error_message || \`Google autocomplete: \${json.status}\`);
  const predictions = Array.isArray(json.predictions) ? json.predictions.slice(0, max) : [];
  const places = [];
  for (const p of predictions) {
    try {
      places.push(await ptyGv2Details(p.place_id, p.structured_formatting?.main_text || p.description || cleanQ));
    } catch {}
  }
  return places;
}

async function ptyGv2TextSearch({ q, lat, lng, limit }) {
  const key = ptyGv2Key();
  if (!key) throw new Error("GOOGLE_MAPS_API_KEY no configurada en Render");
  const cleanQ = String(q || "").trim();
  if (!cleanQ) return [];

  const max = Math.max(1, Math.min(12, Number(limit || 8)));
  const url = new URL("https://maps.googleapis.com/maps/api/place/textsearch/json");
  url.searchParams.set("query", /panam[áa]/i.test(cleanQ) ? cleanQ : \`\${cleanQ}, Panamá\`);
  url.searchParams.set("language", "es");
  url.searchParams.set("region", "pa");
  url.searchParams.set("key", key);
  const latN = ptyGv2Num(lat);
  const lngN = ptyGv2Num(lng);
  if (latN !== null && lngN !== null) {
    url.searchParams.set("location", \`\${latN},\${lngN}\`);
    url.searchParams.set("radius", "60000");
  }
  const { json } = await ptyGv2Json(url);
  if (json.status && !["OK", "ZERO_RESULTS"].includes(json.status)) throw new Error(json.error_message || \`Google text search: \${json.status}\`);
  return (Array.isArray(json.results) ? json.results : []).slice(0, max).map((r) => ptyGv2PlaceFromGoogle(r, cleanQ));
}

async function ptyGv2Nearby(lat, lng, radius = 90) {
  const key = ptyGv2Key();
  if (!key) throw new Error("GOOGLE_MAPS_API_KEY no configurada en Render");
  const latN = ptyGv2Num(lat);
  const lngN = ptyGv2Num(lng);
  if (latN === null || lngN === null) return [];

  const url = new URL("https://maps.googleapis.com/maps/api/place/nearbysearch/json");
  url.searchParams.set("location", \`\${latN},\${lngN}\`);
  url.searchParams.set("radius", String(Math.max(25, Math.min(180, Number(radius || 90)))));
  url.searchParams.set("language", "es");
  url.searchParams.set("key", key);
  const { json } = await ptyGv2Json(url);
  if (json.status && !["OK", "ZERO_RESULTS"].includes(json.status)) throw new Error(json.error_message || \`Google nearby: \${json.status}\`);
  return (Array.isArray(json.results) ? json.results : [])
    .map((r) => ptyGv2PlaceFromGoogle(r))
    .filter((p) => p.title && !ptyGv2LooksLikeCode(p.title))
    .sort((a, b) => Number(b._score || 0) - Number(a._score || 0))
    .slice(0, 5);
}

async function ptyGv2Reverse(lat, lng) {
  const key = ptyGv2Key();
  if (!key) throw new Error("GOOGLE_MAPS_API_KEY no configurada en Render");
  const latN = ptyGv2Num(lat);
  const lngN = ptyGv2Num(lng);
  if (latN === null || lngN === null) throw new Error("lat/lng requeridos");

  // Primero intentamos POI cercano; esto evita códigos tipo XFQM+C22 cuando el pin cae sobre un mall, hospital, local, etc.
  let nearby = [];
  try {
    nearby = await ptyGv2Nearby(latN, lngN, 100);
  } catch (e) {
    console.warn("[GOOGLE_NEARBY_WARN]", e?.message || e);
  }
  if (nearby.length) {
    const best = nearby[0];
    return {
      label: best.title,
      title: best.title,
      name: best.title,
      address: best.address || best.title,
      display_name: best.address || best.title,
      place: { ...best, lat: latN, lng: lngN },
      results: nearby,
      provider: "google_nearby",
    };
  }

  const url = new URL("https://maps.googleapis.com/maps/api/geocode/json");
  url.searchParams.set("latlng", \`\${latN},\${lngN}\`);
  url.searchParams.set("language", "es");
  url.searchParams.set("region", "pa");
  url.searchParams.set("key", key);
  const { json } = await ptyGv2Json(url);
  if (json.status && !["OK", "ZERO_RESULTS"].includes(json.status)) throw new Error(json.error_message || \`Google geocode: \${json.status}\`);

  const results = Array.isArray(json.results) ? json.results : [];
  const filtered = results
    .map((r) => ({ raw: r, place: ptyGv2PlaceFromGoogle(r) }))
    .filter((x) => x.place.address || x.place.title)
    .sort((a, b) => Number(b.place._score || 0) - Number(a.place._score || 0));

  const selected = filtered[0]?.place || {
    title: "Punto seleccionado",
    short: "Punto seleccionado",
    name: "Punto seleccionado",
    address: "Punto seleccionado",
    fullAddress: "Punto seleccionado",
    display_name: "Punto seleccionado",
    lat: latN,
    lng: lngN,
    source: "google_geocode",
  };

  if (ptyGv2LooksLikeCode(selected.title) && selected.address) {
    selected.title = selected.address;
    selected.short = selected.address;
    selected.name = selected.address;
  }

  return {
    label: selected.title || selected.address || "Punto seleccionado",
    title: selected.title || selected.address || "Punto seleccionado",
    name: selected.title || selected.address || "Punto seleccionado",
    address: selected.address || selected.title || "Punto seleccionado",
    display_name: selected.address || selected.title || "Punto seleccionado",
    place: { ...selected, lat: latN, lng: lngN },
    results: filtered.slice(0, 5).map((x) => x.place),
    provider: "google_geocode",
  };
}

async function ptyGv2SearchHandler(req, res) {
  try {
    const q = asText(req.query.q || req.query.input || req.query.query || "");
    if (!q) return safeJson(res, 200, { ok: true, provider: "google_v2", places: [], results: [] });
    let places = [];
    try {
      places = await ptyGv2Autocomplete({ q, lat: req.query.lat, lng: req.query.lng, limit: req.query.limit || 8 });
    } catch (e) {
      console.warn("[GOOGLE_V2_AUTOCOMPLETE_WARN]", e?.message || e);
    }
    if (!places.length) places = await ptyGv2TextSearch({ q, lat: req.query.lat, lng: req.query.lng, limit: req.query.limit || 8 });

    const seen = new Set();
    const clean = places
      .filter((p) => p && Number.isFinite(Number(p.lat)) && Number.isFinite(Number(p.lng)))
      .filter((p) => p.title && !ptyGv2LooksLikeCode(p.title))
      .sort((a, b) => Number(b._score || 0) - Number(a._score || 0))
      .filter((p) => {
        const key = String(p.placeId || \`\${p.title}_\${Number(p.lat).toFixed(5)}_\${Number(p.lng).toFixed(5)}\`).toLowerCase();
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      })
      .map(({ _score, ...p }) => p);

    return safeJson(res, 200, { ok: true, provider: "google_v2", places: clean, results: clean });
  } catch (e) {
    console.error("[GOOGLE_V2_PLACES_ERROR]", e);
    return safeJson(res, 500, { ok: false, message: String(e?.message || e), places: [], results: [] });
  }
}

async function ptyGv2ReverseHandler(req, res) {
  try {
    const out = await ptyGv2Reverse(req.query.lat, req.query.lng);
    return safeJson(res, 200, { ok: true, ...out });
  } catch (e) {
    console.error("[GOOGLE_V2_REVERSE_ERROR]", e);
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
}

app.get("/api/places/search", authOptional, ptyGv2SearchHandler);
app.get("/api/places/autocomplete", authOptional, ptyGv2SearchHandler);
app.get("/api/google/places/autocomplete", authOptional, ptyGv2SearchHandler);

app.get("/api/places/details", authOptional, async (req, res) => {
  try {
    const placeId = asText(req.query.placeId || req.query.place_id || "");
    const place = await ptyGv2Details(placeId);
    return safeJson(res, 200, { ok: true, place, result: place });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/google/places/details", authOptional, async (req, res) => {
  try {
    const placeId = asText(req.query.placeId || req.query.place_id || "");
    const place = await ptyGv2Details(placeId);
    return safeJson(res, 200, { ok: true, place, result: place });
  } catch (e) {
    return safeJson(res, 500, { ok: false, message: String(e?.message || e) });
  }
});

app.get("/api/places/reverse", authOptional, ptyGv2ReverseHandler);
app.get("/api/geocode/reverse", authOptional, ptyGv2ReverseHandler);
app.get("/api/google/geocode/reverse", authOptional, ptyGv2ReverseHandler);

`;

  src = src.slice(0, idx) + block + src.slice(idx);
  changes++;
  console.log("OK: Google Places V2 insertado antes de V1");
} else {
  console.log("OK ya existe: Google Places V2");
}

fs.writeFileSync(serverPath, src, "utf8");

console.log("");
console.log(`Listo. Cambios aplicados: ${changes}`);
console.log("");
console.log("Ahora ejecuta:");
console.log("git add ptydrive/server.js");
console.log('git commit -m "Clean Google Places labels and reverse geocoding names"');
console.log("git push");
console.log("");
console.log("Luego Render hará deploy automático. Si no: Manual Deploy > Deploy latest commit.");
