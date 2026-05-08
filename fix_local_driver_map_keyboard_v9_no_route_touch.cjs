#!/usr/bin/env node
/**
 * PTYDrive Local V9 - driver map performance + keyboard lift
 *
 * NO TOCA:
 * - fetchRoadRoute
 * - showDriverRideOnMap
 * - /api/routes/drive
 * - la línea/ruta del driver que ya está funcionando
 *
 * Corrige:
 * 1) Mapa driver lento:
 *    - Desactiva capas pesadas por defecto: gasolineras, zonas calientes, tráfico.
 *    - Baja polling del driver.
 *    - Agrega tracksViewChanges={false} a Markers.
 *
 * 2) Inputs tapados por teclado:
 *    - Agrega KeyboardAvoidingView a modales de chat/soporte/incidentes/objetos/rating.
 *    - Agrega keyboardShouldPersistTaps y keyboardDismissMode.
 *    - Configura app.json android.softwareKeyboardLayoutMode="pan".
 *
 * Uso:
 *   cd "C:\\Users\\Prodima S.A\\Downloads\\ride-platform-mvp-09-05-2026-FIX3"
 *   node fix_local_driver_map_keyboard_v9_no_route_touch.cjs
 *
 * Luego:
 *   cd rider-app
 *   npx expo start --lan -c
 */

const fs = require("fs");
const path = require("path");

const root = process.cwd();
const appPath = path.join(root, "rider-app", "App.js");
const appJsonPath = path.join(root, "rider-app", "app.json");

function fail(msg) {
  console.error("ERROR:", msg);
  process.exit(1);
}

function backup(file, tag) {
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const bak = `${file}.bak_${tag}_${stamp}`;
  fs.copyFileSync(file, bak);
  console.log("Backup:", path.relative(root, bak));
}

function replaceOnce(src, from, to, label) {
  if (src.includes(to)) {
    console.log("OK ya existe:", label);
    return { src, changed: false };
  }
  if (!src.includes(from)) {
    console.log("AVISO no encontré:", label);
    return { src, changed: false };
  }
  console.log("OK:", label);
  return { src: src.replace(from, to), changed: true };
}

function findModalBounds(src, visibleName) {
  const vis = src.indexOf(`visible={${visibleName}}`);
  if (vis === -1) return null;
  const start = src.lastIndexOf("<Modal", vis);
  if (start === -1) return null;
  const end = src.indexOf("</Modal>", vis);
  if (end === -1) return null;
  return { start, end: end + "</Modal>".length };
}

function patchKeyboardModal(src, visibleName, cardStyleNames = []) {
  const bounds = findModalBounds(src, visibleName);
  if (!bounds) {
    console.log("AVISO: no encontré modal", visibleName);
    return { src, changed: false };
  }

  let block = src.slice(bounds.start, bounds.end);
  if (block.includes("PTY V9 KEYBOARD LIFT")) {
    console.log("OK ya existe: keyboard lift", visibleName);
    return { src, changed: false };
  }

  const original = block;
  const hasInput = block.includes("TextInput") || ["chatOpen","supportChatOpen","incidentOpen","lostItemOpen","ratingOpen","surveyOpen","helpOpen"].includes(visibleName);
  if (!hasInput) {
    console.log("AVISO: modal sin TextInput aparente, no se parchea", visibleName);
    return { src, changed: false };
  }

  block = block.replace(
    /(<Modal[\s\S]*?>\s*)/,
    `$1
        <KeyboardAvoidingView
          // PTY V9 KEYBOARD LIFT
          style={styles.ptyKeyboardAvoidRoot}
          behavior={Platform.OS === "ios" ? "padding" : "height"}
          keyboardVerticalOffset={Platform.OS === "ios" ? 18 : 0}
        >`
  );

  block = block.replace(/(\s*)<\/Modal>\s*$/, `$1</KeyboardAvoidingView>$1</Modal>`);

  for (const name of cardStyleNames) {
    const exact = `style={styles.${name}}`;
    if (block.includes(exact)) {
      block = block.replace(exact, `style={[styles.${name}, styles.ptyKeyboardModalCard]}`);
    }
  }

  block = block.replace(/<ScrollView(\s+)(?![^>]*keyboardShouldPersistTaps=)/g, `<ScrollView$1keyboardShouldPersistTaps="handled"\n              `);

  if (block === original) {
    console.log("AVISO: no se pudo parchear", visibleName);
    return { src, changed: false };
  }

  console.log("OK: keyboard lift aplicado a", visibleName);
  return { src: src.slice(0, bounds.start) + block + src.slice(bounds.end), changed: true };
}

if (!fs.existsSync(appPath)) {
  fail(`No encontré ${appPath}`);
}

backup(appPath, "driver_map_keyboard_v9");

let src = fs.readFileSync(appPath, "utf8");
let changes = 0;

// Importar KeyboardAvoidingView.
if (!src.includes("KeyboardAvoidingView")) {
  const r = replaceOnce(
    src,
    `  Image,
  Modal,`,
    `  Image,
  KeyboardAvoidingView,
  Modal,`,
    "import KeyboardAvoidingView"
  );
  src = r.src; if (r.changed) changes++;
} else {
  console.log("OK ya existe: KeyboardAvoidingView importado");
}

// Polling menos agresivo.
let before = src;
src = src.replace(/const POLL_MS = 2500;/g, "const POLL_MS = 4500;");
if (src !== before) {
  changes++;
  console.log("OK: POLL_MS subido a 4500ms");
}

// Capas pesadas OFF por defecto.
const bools = [
  [/const \[driverFuelStationsVisible,\s*setDriverFuelStationsVisible\]\s*=\s*useState\(true\);/g, "const [driverFuelStationsVisible, setDriverFuelStationsVisible] = useState(false);", "driverFuelStationsVisible=false"],
  [/const \[driverDemandMapVisible,\s*setDriverDemandMapVisible\]\s*=\s*useState\(true\);/g, "const [driverDemandMapVisible, setDriverDemandMapVisible] = useState(false);", "driverDemandMapVisible=false"],
  [/const \[driverTrafficVisible,\s*setDriverTrafficVisible\]\s*=\s*useState\(true\);/g, "const [driverTrafficVisible, setDriverTrafficVisible] = useState(false);", "driverTrafficVisible=false"],
];

for (const [re, to, label] of bools) {
  const b = src;
  src = src.replace(re, to);
  if (src !== b) {
    changes++;
    console.log("OK:", label);
  }
}

// Reducir intervalos de driver/polling.
before = src;
src = src
  .replace(/setInterval\(loadDriverPendingFromAdmin,\s*2500\)/g, "setInterval(loadDriverPendingFromAdmin, 6500)")
  .replace(/setInterval\(loadDriverPendingFromAdmin,\s*5000\)/g, "setInterval(loadDriverPendingFromAdmin, 6500)")
  .replace(/setInterval\(loadStatuses,\s*5000\)/g, "setInterval(loadStatuses, 25000)")
  .replace(/setInterval\(loadStatuses,\s*20000\)/g, "setInterval(loadStatuses, 25000)")
  .replace(/setInterval\(load,\s*10000\)/g, "setInterval(load, 25000)")
  .replace(/setInterval\(load,\s*20000\)/g, "setInterval(load, 25000)")
  .replace(/},\s*2500\);/g, "}, 6500);");
if (src !== before) {
  changes++;
  console.log("OK: polling reducido");
}

// Optimizar Marker.
before = src;
src = src.replace(/<Marker(\s+)(?![^>]*tracksViewChanges=)/g, `<Marker$1tracksViewChanges={false}\n            `);
if (src !== before) {
  changes++;
  console.log("OK: tracksViewChanges={false} agregado a Markers");
}

// Tráfico OFF.
before = src;
src = src.replace(/showsTraffic=\{[^}]+\}/g, "showsTraffic={false}");
if (src !== before) {
  changes++;
  console.log("OK: showsTraffic desactivado");
}

// Keyboard lift modales con inputs.
const modals = [
  ["chatOpen", ["chatHubModalCard"]],
  ["supportChatOpen", ["supportChatModal", "supportChatCard", "detailCardTall"]],
  ["incidentOpen", ["incidentModal", "detailCardTall", "supportChatModal"]],
  ["lostItemOpen", ["lostItemModal", "detailCardTall", "supportChatModal"]],
  ["ratingOpen", ["ratingModal", "detailCardTall", "supportChatModal"]],
  ["surveyOpen", ["surveyModal", "detailCardTall", "supportChatModal"]],
  ["helpOpen", ["helpModal", "detailCardTall", "supportChatModal"]],
];

for (const [visible, cards] of modals) {
  const r = patchKeyboardModal(src, visible, cards);
  src = r.src; if (r.changed) changes++;
}

// ScrollView de chats.
const scrolls = [
  [`<ScrollView
              style={styles.chatScroll}`, `<ScrollView
              keyboardShouldPersistTaps="handled"
              style={styles.chatScroll}`, "chatScroll keyboardShouldPersistTaps"],
  [`<ScrollView
              style={styles.supportChatScroll}`, `<ScrollView
              keyboardShouldPersistTaps="handled"
              style={styles.supportChatScroll}`, "supportChatScroll keyboardShouldPersistTaps"],
  [`contentContainerStyle={styles.chatScrollContent}`, `keyboardDismissMode="interactive"
              contentContainerStyle={styles.chatScrollContent}`, "chatScroll keyboardDismissMode"],
  [`contentContainerStyle={styles.supportChatScrollContent}`, `keyboardDismissMode="interactive"
              contentContainerStyle={styles.supportChatScrollContent}`, "supportChatScroll keyboardDismissMode"],
];

for (const [from, to, label] of scrolls) {
  if (!src.includes(to)) {
    const r = replaceOnce(src, from, to, label);
    src = r.src; if (r.changed) changes++;
  }
}

// Estilos.
if (!src.includes("ptyKeyboardAvoidRoot:")) {
  const idx = src.indexOf("const styles = StyleSheet.create({");
  if (idx !== -1) {
    const openIdx = src.indexOf("{", idx);
    const styles = `
  ptyKeyboardAvoidRoot: {
    flex: 1,
    justifyContent: "flex-end",
  },
  ptyKeyboardModalCard: {
    maxHeight: Platform.OS === "android" ? height * 0.78 : height * 0.88,
  },
`;
    src = src.slice(0, openIdx + 1) + "\n" + styles + src.slice(openIdx + 1);
    changes++;
    console.log("OK: estilos KeyboardAvoiding agregados");
  } else {
    console.log("AVISO: no encontré StyleSheet.create");
  }
} else {
  console.log("OK ya existe: estilos KeyboardAvoiding");
}

// Al entrar driver, apagar capas pesadas. Usar try para no romper si algún estado no existe.
if (!src.includes("PTY V9 DRIVER PERF DEFAULTS")) {
  const needle = `    setMode("driver");`;
  if (src.includes(needle)) {
    src = src.replace(
      needle,
      `    // PTY V9 DRIVER PERF DEFAULTS
    try { setDriverFuelStationsVisible(false); } catch {}
    try { setDriverDemandMapVisible(false); } catch {}
    try { setDriverTrafficVisible(false); } catch {}
${needle}`
    );
    changes++;
    console.log("OK: entrar driver apaga capas pesadas");
  } else {
    console.log("AVISO: no encontré setMode(\"driver\")");
  }
}

fs.writeFileSync(appPath, src, "utf8");

// app.json: android keyboard pan.
if (fs.existsSync(appJsonPath)) {
  backup(appJsonPath, "keyboard_pan_v9");
  try {
    const json = JSON.parse(fs.readFileSync(appJsonPath, "utf8"));
    json.expo = json.expo || {};
    json.expo.android = json.expo.android || {};
    if (json.expo.android.softwareKeyboardLayoutMode !== "pan") {
      json.expo.android.softwareKeyboardLayoutMode = "pan";
      fs.writeFileSync(appJsonPath, JSON.stringify(json, null, 2) + "\n", "utf8");
      changes++;
      console.log("OK: app.json android.softwareKeyboardLayoutMode=pan");
    } else {
      console.log("OK ya existe: app.json softwareKeyboardLayoutMode=pan");
    }
  } catch (e) {
    console.log("AVISO: app.json no se pudo parsear:", e.message);
  }
} else {
  console.log("AVISO: no encontré rider-app/app.json");
}

console.log("");
console.log(`Listo. Cambios aplicados: ${changes}`);
console.log("");
console.log("Reinicia Expo limpio:");
console.log(`cd "${path.join(root, "rider-app")}"`);
console.log("npx expo start --lan -c");
console.log("");
console.log("Nota: en Expo Go algunos cambios de app.json aplican solo en build, pero KeyboardAvoidingView sí aplica directo.");
