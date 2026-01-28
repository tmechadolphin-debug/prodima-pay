<!doctype html>
<html lang="es">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <meta name="robots" content="noindex,nofollow,noarchive" />
  <title>Prodima · Pedidos Mercaderistas</title>
  <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;600;700;800;900&display=swap" rel="stylesheet">

  <style>
    :root{
      --brand:#c31b1c;
      --brand-dark:#8f1214;
      --accent:#ffbf24;
      --accent-dark:#f29a00;
      --ink:#222;
      --muted:#666;
      --bd:#e6e8ec;
      --bg:#fff7e8;
      --ok:#0c8c6a;
      --bad:#c31b1c;
      --card:#ffffff;
      --shadow: 0 18px 50px rgba(0,0,0,.10);
    }
    *{box-sizing:border-box;margin:0;padding:0}
    body{
      font-family:'Montserrat',system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
      color:var(--ink);
      background:linear-gradient(120deg,#fff3db 0%, #ffffff 55%, #fff3db 100%);
      min-height:100vh;
      touch-action: manipulation; /* ayuda a evitar zoom raro por doble tap */
    }

    .topbar{
      background:linear-gradient(90deg,var(--brand) 0%, #e0341d 45%, var(--accent) 100%);
      color:#fff;
      padding:12px 16px;
      font-weight:900;
      letter-spacing:.3px;
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:12px;
    }
    .topbar .left{
      display:flex;
      align-items:center;
      gap:10px;
      flex-wrap:wrap;
    }
    .topbar small{
      opacity:.95;
      font-weight:800;
    }
    .pillStatus{
      background:#fff;
      color:#7b1a01;
      border:1px solid #ffd27f;
      border-radius:999px;
      padding:6px 10px;
      font-size:12px;
      font-weight:900;
      box-shadow:0 10px 18px rgba(0,0,0,.06);
      white-space:nowrap;
    }
    .pillStatus.ok{ color:#0c8c6a; border-color:#b7f0db; }
    .pillStatus.bad{ color:#b30000; border-color:#ffd27f; }

    .wrap{max-width:1200px;margin:18px auto 50px;padding:0 16px}

    .hero{
      background:
        radial-gradient(1000px 420px at 20% -10%, rgba(255,191,36,.55), transparent 60%),
        radial-gradient(900px 420px at 95% 0%, rgba(195,21,28,.22), transparent 62%),
        #fff;
      border:1px solid #f3d6a5;
      border-radius:22px;
      box-shadow: var(--shadow);
      padding:18px 18px 14px;
    }
    .hero h1{
      font-size:24px;
      font-weight:900;
      color:var(--brand);
      margin-bottom:4px;
    }
    .hero p{
      color:#6a3b1b;
      font-weight:700;
      font-size:13px;
      line-height:1.35;
      max-width:900px;
    }

    .grid{
      display:grid;
      grid-template-columns: 1.1fr .9fr;
      gap:18px;
      margin-top:18px;
    }
    @media (max-width:1050px){
      .grid{grid-template-columns:1fr}
    }

    .card{
      background:var(--card);
      border:1px solid #f3d6a5;
      border-radius:18px;
      box-shadow: var(--shadow);
      overflow:hidden;
    }
    .card-h{
      background:linear-gradient(90deg, rgba(195,21,28,.08), rgba(255,191,36,.25));
      border-bottom:1px solid #f3d6a5;
      padding:12px 14px;
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:12px;
    }
    .card-h strong{
      color:var(--brand);
      font-weight:900;
      letter-spacing:.2px;
    }
    .badge{
      background:#fff;
      border:1px solid #ffd27f;
      color:#7b1a01;
      font-weight:900;
      border-radius:999px;
      padding:6px 10px;
      font-size:12px;
      box-shadow:0 10px 18px rgba(0,0,0,.06);
      white-space:nowrap;
    }
    .card-b{padding:14px}

    .row{display:grid;grid-template-columns: 1fr 1fr; gap:10px}
    @media (max-width:560px){ .row{grid-template-columns:1fr} }

    label{
      display:block;
      font-weight:900;
      color:#6a3b1b;
      font-size:12px;
      margin-bottom:6px;
      letter-spacing:.2px;
    }
    .input{
      width:100%;
      height:42px;
      border-radius:14px;
      border:1px solid #ffd27f;
      padding:0 12px;
      outline:none;
      background:#fffdf6;
      font-weight:800;
      color:#2b1c16;
      font-size:16px; /* evita zoom iOS por inputs pequeños */
    }
    .input::placeholder{color:#c08a40;font-weight:700}

    .btn{
      height:42px;
      border-radius:14px;
      font-weight:900;
      border:0;
      cursor:pointer;
      padding:0 14px;
      display:inline-flex;
      align-items:center;
      justify-content:center;
      gap:8px;
      letter-spacing:.2px;
      font-size:14px;
    }
    .btn-primary{
      background:linear-gradient(90deg,var(--brand) 0%, var(--accent) 100%);
      color:#fff;
      box-shadow:0 12px 22px rgba(195,21,28,.25);
    }
    .btn-outline{
      background:#fff;
      color:var(--brand);
      border:1px solid #ffd27f;
    }
    .btn-danger{
      background:linear-gradient(90deg,#a40b0d 0%, #ff7a00 100%);
      color:#fff;
    }
    .btn:disabled{opacity:.6;cursor:not-allowed}

    .note{
      margin-top:10px;
      background:#fff7e8;
      border:1px dashed #f3c776;
      border-radius:14px;
      padding:10px 12px;
      color:#70421c;
      font-weight:700;
      font-size:12px;
      line-height:1.35;
    }

    .clientBox{
      margin-top:12px;
      border:1px solid #f3d6a5;
      border-radius:16px;
      padding:12px;
      background:linear-gradient(180deg,#fffef8 0%, #fff7e8 100%);
    }
    .clientBox .title{
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:10px;
      margin-bottom:8px;
    }
    .pill{
      display:inline-flex;
      align-items:center;
      gap:8px;
      padding:6px 10px;
      border-radius:999px;
      font-size:12px;
      font-weight:900;
      border:1px solid #ffd27f;
      background:#fff;
      color:#7b1a01;
    }

    .kv{
      display:grid;
      grid-template-columns: 140px 1fr;
      gap:6px 10px;
      font-size:12px;
      align-items:center;
    }
    .k{color:#7a4a1a;font-weight:900}
    .v{color:#2b1c16;font-weight:800}

    table{
      width:100%;
      border-collapse:separate;
      border-spacing:0;
      overflow:hidden;
      border:1px solid #f3d6a5;
      border-radius:16px;
    }
    thead th{
      text-align:left;
      padding:10px 10px;
      font-size:12px;
      color:#6a3b1b;
      font-weight:900;
      background:linear-gradient(90deg, rgba(195,21,28,.06), rgba(255,191,36,.20));
      border-bottom:1px solid #f3d6a5;
    }
    tbody td{
      padding:10px 10px;
      border-bottom:1px dashed #f3d6a5;
      vertical-align:middle;
      background:#fff;
      font-size:12px;
      font-weight:800;
      color:#2b1c16;
    }
    tbody tr:last-child td{border-bottom:0}

    .t-input{
      width:100%;
      height:38px;
      border-radius:12px;
      border:1px solid #ffd27f;
      background:#fffdf6;
      padding:0 10px;
      outline:none;
      font-weight:900;
      font-size:16px; /* evita zoom iOS */
    }
    .small{font-size:11px;color:#7a4a1a;font-weight:900}
    .muted{color:#777;font-weight:800}

    .stock-ok{color:var(--ok)}
    .stock-bad{color:var(--bad)}

    .footerActions{
      display:flex;
      gap:10px;
      flex-wrap:wrap;
      align-items:center;
      justify-content:space-between;
      margin-top:12px;
    }
    .totals{
      background:#fff;
      border:1px solid #ffd27f;
      border-radius:16px;
      padding:10px 12px;
      display:flex;
      gap:16px;
      align-items:center;
      flex-wrap:wrap;
      font-weight:900;
      color:#6a3b1b;
    }
    .totals span{
      color:#111;
      font-weight:900;
    }

    .toast{
      position:fixed;
      right:18px;
      bottom:18px;
      background:#111;
      color:#fff;
      padding:12px 14px;
      border-radius:14px;
      box-shadow:0 20px 50px rgba(0,0,0,.25);
      display:none;
      max-width:420px;
      z-index:999;
      font-weight:800;
      line-height:1.35;
    }
    .toast.ok{background:linear-gradient(90deg,#0c8c6a,#1bb88a)}
    .toast.bad{background:linear-gradient(90deg,#a40b0d,#ff7a00)}
    .copy{
      text-align:center;
      margin-top:16px;
      color:#7a4a1a;
      font-weight:800;
      font-size:12px;
      opacity:.95;
    }

    /* MODAL LOGIN */
    .overlay{
      position:fixed; inset:0;
      background:rgba(0,0,0,.55);
      display:none;
      align-items:center; justify-content:center;
      padding:18px;
      z-index:1000;
    }
    .modal{
      width:min(520px, 96vw);
      background:#fff;
      border:1px solid #f3d6a5;
      border-radius:18px;
      box-shadow:0 30px 80px rgba(0,0,0,.28);
      overflow:hidden;
    }
    .modal-h{
      padding:12px 14px;
      background:linear-gradient(90deg,var(--brand) 0%, var(--accent) 100%);
      color:#fff;
      font-weight:900;
      display:flex;
      justify-content:space-between;
      align-items:center;
      gap:10px;
    }
    .modal-b{ padding:14px; }
    .modal-b .row2{
      display:grid; grid-template-columns:1fr 1fr; gap:10px;
    }
    @media (max-width:560px){ .modal-b .row2{ grid-template-columns:1fr; } }
    .modal-f{
      padding:14px;
      display:flex;
      justify-content:flex-end;
      gap:10px;
      border-top:1px solid #f3d6a5;
      background:#fffef8;
    }
    .chip{
      display:inline-flex;
      align-items:center;
      gap:6px;
      padding:5px 10px;
      border-radius:999px;
      font-size:11px;
      font-weight:900;
      border:1px solid #ffd27f;
      background:#fff;
      color:#7b1a01;
      white-space:nowrap;
    }
    .chip.ok{border-color:#b7f0db;color:#0c8c6a}
    .chip.bad{border-color:#ffd27f;color:#b30000}

    /* PC: mantiene igual */
    @media (min-width: 1025px){
      body{ zoom: 1; }
    }

    /* TELÉFONO: agrandar UI */
    @media (max-width: 600px){
      html{
        font-size: 18px;
        -webkit-text-size-adjust: 100%;
      }
      body{
        transform: scale(1.18);
        transform-origin: top left;
        width: calc(100% / 1.18);
      }
      .input, .btn{
        height: 52px;
        border-radius: 16px;
        font-size: 16px;
      }
      .wrap{ padding: 0 10px; }
      .tableWrap, table{ overflow-x: auto; display:block; }
    }

    /* ✅ Sugerencias (dropdown propio móvil/PC) */
    .suggestWrap{
      display:none;
      position:relative;
      margin-top:6px;
    }
    .sugBox{
      background:#fff;
      border:1px solid #ffd27f;
      border-radius:14px;
      box-shadow:0 18px 50px rgba(0,0,0,.12);
      overflow:hidden;
      max-height:260px;
      overflow-y:auto;
    }
    .sugItem{
      padding:10px 12px;
      font-weight:800;
      font-size:13px;
      border-bottom:1px dashed #f3d6a5;
      cursor:pointer;
    }
    .sugItem:last-child{border-bottom:0}
    .sugItem:active{background:#fff3db}
    .sugCode{font-weight:900;color:#c31b1c}
    .sugName{color:#2b1c16}
    .sugMini{margin-top:4px;font-size:11px;color:#777;font-weight:800}

  </style>
</head>

<body>

  <div class="topbar">
    <div class="left">
      <div>📦 PRODIMA · Pedidos Mercaderistas</div>
      <span class="pillStatus bad" id="apiStatus">API: verificando...</span>
      <span class="pillStatus bad" id="loginStatus">Login: no</span>
    </div>

    <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap">
      <small>uso interno · no público</small>
      <button class="btn btn-outline" id="btnLogout" type="button" style="display:none;height:34px;border-radius:12px;padding:0 10px;font-size:12px">
        🚪 Salir
      </button>
    </div>
  </div>

  <div class="wrap">
    <section class="hero">
      <h1>Crear cotización (SAP)</h1>
      <p>
        ✅ Ingresa el <b>código de cliente</b> o escribe el <b>nombre</b> (ej: “Ricamar”) para autocompletar.
        <br/>
        ✅ Agrega productos por <b>código de artículo</b> o <b>descripción</b> y cantidad.
      </p>
      <div class="note">
        ⚡ Tip: cliente por <b>código</b> (C01133) o <b>nombre</b> (Importadora Ricamar).
        <br/>
        ⚡ Tip: artículos por <b>código</b> (0110) o <b>texto</b> (Low salsa china).
      </div>
    </section>

    <div class="grid">

      <!-- CLIENTE -->
      <section class="card">
        <div class="card-h">
          <strong>1) Cliente</strong>
          <span class="badge" id="whoami">Usuario: --</span>
        </div>
        <div class="card-b">

          <div class="row">
            <div>
              <label for="cardCode">Código o nombre de cliente (SAP)</label>
              <input id="cardCode" class="input" list="clientList" placeholder="Ej: C01133 o Ricamar" />
              <datalist id="clientList"></datalist>

              <!-- dropdown móvil/extra -->
              <div id="clientSuggest" class="suggestWrap"></div>

              <div class="small muted" style="margin-top:6px" id="clientHint">
                Escribe 2+ letras para ver sugerencias.
              </div>

              <div style="display:flex; gap:10px; flex-wrap:wrap; margin-top:10px">
                <button class="btn btn-primary" id="btnLoadClient" type="button">🔎 Buscar cliente</button>
                <button class="btn btn-outline" id="btnClearClient" type="button">🧹 Limpiar</button>
              </div>

              <div id="clientBox" class="clientBox" style="display:none">
                <div class="title">
                  <div class="pill">👤 Cliente cargado</div>
                  <div class="pill" id="clientCredit">💳 OK</div>
                </div>

                <div class="kv">
                  <div class="k">Código</div><div class="v" id="c_code">--</div>
                  <div class="k">Nombre</div><div class="v" id="c_name">--</div>
                  <div class="k">Teléfono</div><div class="v" id="c_phone">--</div>
                  <div class="k">Email</div><div class="v" id="c_email">--</div>
                  <div class="k">Dirección</div><div class="v" id="c_addr">--</div>
                </div>
              </div>
            </div>
            <div></div>
          </div>

        </div>
      </section>

      <!-- CONFIRMACIÓN -->
      <section class="card">
        <div class="card-h">
          <strong>3) Confirmación</strong>
          <span class="badge">Cotizar: Ingreso en SAP</span>
        </div>
        <div class="card-b">

          <label for="comments">Comentarios / Nota (opcional)</label>
          <input id="comments" class="input" placeholder="Ej: Entregar martes / Observaciones..." />

          <label style="margin-top:12px">Adjuntos (foto o PDF)</label>
          <input id="files" class="input" type="file" multiple accept="image/*,application/pdf" capture="environment"/>
          <div id="filesList" class="note" style="display:none;margin-top:10px"></div>

          <div class="footerActions">
            <div class="totals">
              Total líneas: <span id="t_lines">0</span>
              Total unidades: <span id="t_qty">0</span>
              Total estimado: <span id="t_total">$ 0.00</span>
            </div>

            <div style="display:flex;gap:10px;flex-wrap:wrap">
              <button id="btnAddRow" class="btn btn-outline" type="button">➕ Agregar línea</button>
              <button id="btnCreateQuote" class="btn btn-primary" type="button">✅ Crear cotización</button>
            </div>
          </div>

          <div class="note" style="margin-top:12px">
            🧾 Esto crea una <b>cotización</b> en SAP. Inventario en rojo NO bloquea la cotización.
          </div>

        </div>
      </section>

    </div>

    <!-- PRODUCTOS -->
    <section class="card" style="margin-top:18px">
      <div class="card-h">
        <strong>2) Productos</strong>
        <span class="badge">Agrega ItemCode o Descripción + Cantidad</span>
      </div>
      <div class="card-b">

        <table>
          <thead>
            <tr>
              <th style="width:190px">Código / Búsqueda</th>
              <th>Descripción</th>
              <th style="width:110px">Precio</th>
              <th style="width:130px">Disponible</th>
              <th style="width:110px">Cantidad</th>
              <th style="width:110px">Subtotal</th>
              <th style="width:86px">Acción</th>
            </tr>
          </thead>
          <tbody id="linesBody"></tbody>
        </table>

        <div class="note" style="margin-top:12px">
          ✅ Si sale <b>Sin stock</b>, igual puedes cotizar. Es informativo.
        </div>

      </div>
    </section>

    <div class="copy">©️ 2026 PRODIMA · Pedidos internos</div>
  </div>

  <div id="toast" class="toast"></div>

  <!-- LOGIN MODAL -->
  <div class="overlay" id="overlay">
    <div class="modal">
      <div class="modal-h">
        <div>🔐 Login Mercaderista</div>
        <div class="chip bad" id="loginState">🔒 Bloqueado</div>
      </div>

      <div class="modal-b">
        <div class="row2">
          <div>
            <label for="mUser">Usuario</label>
            <input id="mUser" class="input" placeholder="Ej: vane15" autocomplete="username"/>
          </div>
          <div>
            <label for="mPin">PIN</label>
            <input id="mPin" class="input" type="password" placeholder="Ej: 1234" autocomplete="current-password"/>
          </div>
        </div>
        <div class="note" style="margin-top:10px">
          ✅ Debes iniciar sesión para enviar cotizaciones.
        </div>
      </div>

      <div class="modal-f">
        <button class="btn btn-primary" id="btnLogin" type="button">Entrar</button>
      </div>
    </div>
  </div>

<script>
/* =========================================
   ✅ CONFIG API (Render backend)
========================================= */
const API_BASE = "https://prodima-pay.onrender.com";

/* =========================================
   AUTH TOKEN (Mercaderistas)
========================================= */
const TOKEN_KEY = "prodima_merc_token";
const USER_KEY  = "prodima_merc_user";

function getToken(){ return localStorage.getItem(TOKEN_KEY) || ""; }
function setToken(t){ localStorage.setItem(TOKEN_KEY, t); }
function clearToken(){ localStorage.removeItem(TOKEN_KEY); }

function setUser(u){ localStorage.setItem(USER_KEY, JSON.stringify(u || {})); }
function getUser(){
  try{ return JSON.parse(localStorage.getItem(USER_KEY) || "{}"); }
  catch{ return {}; }
}
function clearUser(){ localStorage.removeItem(USER_KEY); }

function authHeaders(){
  const t = getToken();
  return {
    "Content-Type":"application/json",
    ...(t ? {"Authorization":"Bearer " + t} : {})
  };
}

/* =========================================
   UI Helpers
========================================= */
function showToast(msg, type="ok"){
  const t = document.getElementById("toast");
  t.className = "toast " + (type==="ok" ? "ok" : "bad");
  t.textContent = msg;
  t.style.display = "block";
  setTimeout(()=> t.style.display = "none", 4500);
}
function money(n){
  if(n == null || Number.isNaN(Number(n))) return "--";
  return "$ " + Number(n).toFixed(2);
}
function safeStr(x){
  if(x == null) return "--";
  const s = String(x).trim();
  return s ? s : "--";
}

function setApiStatus(ok){
  const el = document.getElementById("apiStatus");
  el.className = "pillStatus " + (ok ? "ok" : "bad");
  el.textContent = ok ? "API: OK ✅" : "API: ERROR";
}
function setLoginStatus(ok){
  const el = document.getElementById("loginStatus");
  el.className = "pillStatus " + (ok ? "ok" : "bad");
  el.textContent = ok ? "Login: sí ✅" : "Login: no";
}

/* =========================================
   API Calls
========================================= */
async function apiHealth(){
  const res = await fetch(`${API_BASE}/api/health`);
  const data = await res.json().catch(()=>({}));
  return { ok: res.ok && data.ok, data };
}

async function apiMercLogin(username, pin){
  const res = await fetch(`${API_BASE}/api/auth/login`, {
    method:"POST",
    headers:{ "Content-Type":"application/json" },
    body: JSON.stringify({ username, pin })
  });
  const data = await res.json().catch(()=>({}));
  if(!res.ok || !data.ok) throw new Error(data?.message || "Credenciales inválidas");
  return data;
}

async function apiGetCustomer(cardCode){
  const res = await fetch(`${API_BASE}/api/sap/customer/${encodeURIComponent(cardCode)}`, {
    headers: authHeaders()
  });
  const data = await res.json().catch(()=>({}));
  if(!res.ok || !data.ok) throw new Error(data?.message || "No se pudo cargar cliente");
  return data;
}

async function apiSearchCustomers(q){
  const res = await fetch(`${API_BASE}/api/sap/customers/search?q=${encodeURIComponent(q)}&top=20`, {
    headers: authHeaders()
  });
  const data = await res.json().catch(()=>({}));
  if(!res.ok || !data.ok) throw new Error(data?.message || "No se pudo buscar clientes");
  return Array.isArray(data.results) ? data.results : [];
}

async function apiGetItem(itemCode, cardCode){
  const qs = cardCode ? `?cardCode=${encodeURIComponent(cardCode)}` : "";
  const res = await fetch(`${API_BASE}/api/sap/item/${encodeURIComponent(itemCode)}${qs}`, {
    headers: authHeaders()
  });
  const data = await res.json().catch(()=>({}));
  if(!res.ok || !data.ok) throw new Error(data?.message || "No se pudo cargar item");
  return data;
}

/* ✅ NUEVO: buscar artículos por código o descripción
   Requiere endpoint backend:
   GET /api/sap/items/search?q=...&top=...
   -> { ok:true, results:[{ItemCode, ItemName, SalesUnit}] }
*/
async function apiSearchItems(q){
  const res = await fetch(`${API_BASE}/api/sap/items/search?q=${encodeURIComponent(q)}&top=20`, {
    headers: authHeaders()
  });
  const data = await res.json().catch(()=>({}));
  if(!res.ok || !data.ok) throw new Error(data?.message || "No se pudo buscar artículos");
  return Array.isArray(data.results) ? data.results : [];
}

async function apiCreateQuote(payload){
  const u = getUser();
  const createdBy = String(u?.username || "").trim().toLowerCase();

  const res = await fetch(`${API_BASE}/api/sap/quote`, {
    method: "POST",
    headers: authHeaders(),
    body: JSON.stringify({ ...payload, createdBy })
  });

  const data = await res.json().catch(()=>({}));
  if(!res.ok || !data.ok) throw new Error(data?.message || "No se pudo crear cotización");
  return data;
}

/* =========================================
   State
========================================= */
let CLIENT = null;
const LINES = [];
let lineSeq = 1;

/* =========================================
   Cliente Autocomplete
========================================= */
let clientSearchTimer = null;

function renderClientOptions(list){
  const dl = document.getElementById("clientList");
  dl.innerHTML = (list || []).map(r=>{
    const code = String(r.CardCode || "").trim();
    const name = String(r.CardName || "").trim();
    if(!code) return "";
    return `<option value="${code}">${name}</option>`;
  }).join("");
}

document.getElementById("cardCode").addEventListener("input", (e)=>{
  const q = String(e.target.value || "").trim();
  clearTimeout(clientSearchTimer);

  clientSearchTimer = setTimeout(async ()=>{
    if(!getToken()) return;
    if(q.length < 2) return;

    try{
      const results = await apiSearchCustomers(q);
      renderClientOptions(results);
      const hint = document.getElementById("clientHint");
      if(hint) hint.textContent = results.length ? `Sugerencias: ${results.length}` : "Sin sugerencias.";
    }catch(err){
      console.log(err.message || err);
    }
  }, 250);
});

document.getElementById("cardCode").addEventListener("keydown", (e)=>{
  if(e.key === "Enter"){
    e.preventDefault();
    loadClient();
  }
});

/* =========================================
   Adjuntos
========================================= */
const FILES = [];

function renderFiles(){
  const box = document.getElementById("filesList");
  if(!FILES.length){
    box.style.display = "none";
    box.innerHTML = "";
    return;
  }
  box.style.display = "block";

  box.innerHTML = `
    <b>📎 Archivos adjuntos:</b><br/>
    ${FILES.map((f, i)=> `
      • ${f.name} (${Math.round(f.size/1024)} KB)
      <button type="button" class="btn btn-outline" style="height:26px;border-radius:10px;padding:0 8px;font-size:11px;margin-left:8px"
        onclick="removeFile(${i})">Quitar</button>
    `).join("<br/>")}
  `;
}

window.removeFile = function(idx){
  FILES.splice(idx, 1);
  renderFiles();
};

document.getElementById("files").addEventListener("change", (e)=>{
  const incoming = Array.from(e.target.files || []);

  const MAX_FILES = 5;
  const MAX_MB_EACH = 10;

  for(const f of incoming){
    if(FILES.length >= MAX_FILES){
      showToast(`Máximo ${MAX_FILES} archivos.`, "bad");
      break;
    }
    if(f.size > MAX_MB_EACH * 1024 * 1024){
      showToast(`Archivo muy grande (${f.name}). Máx ${MAX_MB_EACH}MB`, "bad");
      continue;
    }
    FILES.push(f);
  }

  e.target.value = "";
  renderFiles();
});

/* =========================================
   Totals + Render
========================================= */
function calcTotals(){
  const lines = LINES.filter(l => l.itemCode && Number(l.qty) > 0);
  const totalLines = lines.length;
  const totalQty = lines.reduce((a,b)=> a + Number(b.qty||0), 0);
  const total = lines.reduce((a,b)=> a + (Number(b.qty||0) * (Number(b.price)||0)), 0);

  document.getElementById("t_lines").textContent = String(totalLines);
  document.getElementById("t_qty").textContent = String(totalQty);
  document.getElementById("t_total").textContent = money(total);
}

/* =========================================
   ✅ Items Suggest (global)
========================================= */
const ITEM_SUGGEST_CACHE = new Map(); // q -> results
async function getItemSuggestions(q){
  const key = q.toLowerCase();
  if(ITEM_SUGGEST_CACHE.has(key)) return ITEM_SUGGEST_CACHE.get(key);
  const res = await apiSearchItems(q);
  ITEM_SUGGEST_CACHE.set(key, res);
  return res;
}

function hideSuggest(wrap){
  if(!wrap) return;
  wrap.style.display = "none";
  wrap.innerHTML = "";
}

function showSuggest(wrap, list, onPick){
  if(!wrap) return;
  if(!list || !list.length){
    hideSuggest(wrap);
    return;
  }

  wrap.innerHTML = `
    <div class="sugBox">
      ${list.slice(0,20).map(it => `
        <div class="sugItem" data-code="${it.ItemCode}">
          <div><span class="sugCode">${it.ItemCode}</span> — <span class="sugName">${it.ItemName || ""}</span></div>
          <div class="sugMini">${it.SalesUnit ? ("Unidad: " + it.SalesUnit) : ""}</div>
        </div>
      `).join("")}
    </div>
  `;
  wrap.style.display = "block";

  wrap.querySelectorAll(".sugItem").forEach(el=>{
    el.addEventListener("click", ()=>{
      const code = el.getAttribute("data-code");
      if(code) onPick(code);
    });
  });
}

/* =========================================
   Lines UI
========================================= */
function renderLines(){
  const tbody = document.getElementById("linesBody");
  tbody.innerHTML = LINES.map(l => {
    const price = l.price;
    const avail = l.available;

    const stockClass = (avail == null) ? "" : (avail > 0 ? "stock-ok" : "stock-bad");
    const stockText  = (avail == null) ? "<span class='muted'>--</span>" : `<span class="${stockClass}">${avail}</span>`;
    const sub = (l.qty && l.price != null) ? money(Number(l.qty) * Number(l.price)) : "--";

    return `
      <tr data-id="${l.id}">
        <td>
          <input class="t-input js-itemInput" placeholder="Ej: 0110 o Low salsa..." value="${l.itemCodeRaw||l.itemCode||""}" data-field="itemCode" />
          <div class="small muted" style="margin-top:6px">${l.unit ? ("Unidad: " + l.unit) : ""}</div>

          <!-- dropdown sugerencias por línea -->
          <div class="suggestWrap js-itemSuggest"></div>
        </td>
        <td>
          <div style="font-weight:900">${l.name ? l.name : "<span class='muted'>Escribe el código o descripción...</span>"}</div>
          <div class="small muted" style="margin-top:4px">${l.err ? ("⚠️ " + l.err) : ""}</div>
        </td>
        <td><span style="font-weight:900">${money(price)}</span></td>
        <td>${stockText}</td>
        <td>
          <input class="t-input" style="text-align:center" type="number" min="0" step="1" value="${l.qty||""}" data-field="qty" />
        </td>
        <td><span style="font-weight:900">${sub}</span></td>
        <td>
          <button class="btn btn-danger" style="height:38px;border-radius:12px;padding:0 10px" data-action="remove" type="button">🗑️</button>
        </td>
      </tr>
    `;
  }).join("");

  // item input live suggestions
  tbody.querySelectorAll("tr").forEach(tr=>{
    const id = Number(tr.dataset.id);
    const inp = tr.querySelector(".js-itemInput");
    const wrap = tr.querySelector(".js-itemSuggest");

    let tmr = null;
    let last = [];

    function doHide(){ hideSuggest(wrap); }
    function doPick(code){
      doHide();
      inp.value = code;
      onItemCodeChanged(id, code);
    }

    if(inp){
      inp.addEventListener("input", (e)=>{
        const q = String(e.target.value||"").trim();
        const line = LINES.find(x=>x.id===id);
        if(line) line.itemCodeRaw = q;

        clearTimeout(tmr);

        if(!getToken() || q.length < 2){
          doHide();
          return;
        }

        tmr = setTimeout(async ()=>{
          try{
            last = await getItemSuggestions(q);
            showSuggest(wrap, last, doPick);
          }catch{
            doHide();
          }
        }, 220);
      });

      // Enter: si hay sugerencia toma la primera, si no intenta directo
      inp.addEventListener("keydown", async (e)=>{
        if(e.key === "Enter"){
          e.preventDefault();
          const q = String(inp.value||"").trim();
          if(q.length < 1) return;

          try{
            if(last && last.length){
              doPick(last[0].ItemCode);
            }else{
              // intenta resolver por búsqueda (por si todavía no llegó)
              const rs = await getItemSuggestions(q);
              if(rs.length) doPick(rs[0].ItemCode);
              else await onItemCodeChanged(id, q); // caerá a validación
            }
          }catch{
            await onItemCodeChanged(id, q);
          }
        }
        if(e.key === "Escape"){
          doHide();
        }
      });

      // blur: oculta con delay pequeño para permitir click en lista
      inp.addEventListener("blur", ()=>{
        setTimeout(()=> doHide(), 180);
      });
      inp.addEventListener("focus", ()=>{
        // si ya hay texto y last resultados, re-muestra
        const q = String(inp.value||"").trim();
        if(q.length >= 2 && last && last.length){
          showSuggest(wrap, last, doPick);
        }
      });
    }
  });

  // qty input
  tbody.querySelectorAll("input[data-field='qty']").forEach(inp=>{
    inp.addEventListener("input", (e)=>{
      const tr = e.target.closest("tr");
      const id = Number(tr.dataset.id);
      const qty = Number(e.target.value||0);
      const line = LINES.find(x=>x.id===id);
      if(line){
        line.qty = qty;
      }
      calcTotals();
      renderLines();
    });
  });

  // remove
  tbody.querySelectorAll("button[data-action='remove']").forEach(btn=>{
    btn.addEventListener("click", (e)=>{
      const tr = e.target.closest("tr");
      const id = Number(tr.dataset.id);
      const idx = LINES.findIndex(x=>x.id===id);
      if(idx>=0){
        LINES.splice(idx,1);
        renderLines();
        calcTotals();
      }
    });
  });
}

async function onItemCodeChanged(lineId, raw){
  const line = LINES.find(x=>x.id===lineId);
  if(!line) return;

  const codeOrText = String(raw||"").trim();

  line.itemCodeRaw = codeOrText;
  line.itemCode = "";
  line.name = "";
  line.price = null;
  line.available = null;
  line.unit = "";
  line.err = "";

  renderLines();

  if(!codeOrText){
    calcTotals();
    return;
  }

  try{
    if(!getToken()) throw new Error("Debes iniciar sesión.");
    if(!CLIENT?.CardCode) throw new Error("Selecciona un cliente primero.");

    // ✅ Si no parece ItemCode, lo resolvemos por búsqueda y usamos el ItemCode real
    const looksLikeItemCode = /^[A-Za-z0-9._-]+$/.test(codeOrText);
    let itemCode = codeOrText;

    if(!looksLikeItemCode){
      const results = await apiSearchItems(codeOrText);
      if(!results.length) throw new Error("No encontré artículos con esa descripción.");
      itemCode = results[0].ItemCode;
    }

    const cardCode = CLIENT?.CardCode || "";
    const r = await apiGetItem(itemCode, cardCode);

    line.itemCode = itemCode;
    line.name = r?.item?.ItemName || `Producto ${itemCode}`;
    line.price = (r?.price != null) ? Number(r.price) : null;
    line.available = (r?.stock?.available != null) ? Number(r.stock.available) : null;
    line.unit = r?.item?.SalesUnit || r?.uom || "";
    line.err = "";

    if(line.available != null && line.available <= 0){
      line.err = "Sin stock (solo informativo, puedes cotizar).";
    }
  }catch(err){
    line.err = String(err.message || err);
  }

  renderLines();
  calcTotals();
}

/* =========================================
   Cliente
========================================= */
function clearClient(){
  CLIENT = null;
  document.getElementById("clientBox").style.display = "none";
  document.getElementById("c_code").textContent = "--";
  document.getElementById("c_name").textContent = "--";
  document.getElementById("c_phone").textContent = "--";
  document.getElementById("c_email").textContent = "--";
  document.getElementById("c_addr").textContent = "--";
  document.getElementById("clientCredit").textContent = "💳 OK";
}

function extractCardCodeFromInput(raw){
  const s = String(raw || "").trim();
  const m = s.match(/\(([^)]+)\)\s*$/);
  if(m && m[1]) return String(m[1]).trim();
  return s;
}

async function loadClient(){
  if(!getToken()){
    showToast("Debes iniciar sesión primero.", "bad");
    openLogin();
    return;
  }

  const raw = String(document.getElementById("cardCode").value||"").trim();
  const code = extractCardCodeFromInput(raw);

  if(!code){
    showToast("Escribe el código o nombre del cliente.", "bad");
    return;
  }

  try{
    let cardCode = code;

    if(code.length >= 2 && !/^[A-Za-z0-9]+$/.test(code)){
      const results = await apiSearchCustomers(code);
      if(results.length){
        cardCode = results[0].CardCode;
        document.getElementById("cardCode").value = cardCode;
      }
    }

    const r = await apiGetCustomer(cardCode);
    CLIENT = r.customer;

    document.getElementById("clientBox").style.display = "block";
    document.getElementById("c_code").textContent = safeStr(CLIENT.CardCode);
    document.getElementById("c_name").textContent = safeStr(CLIENT.CardName);
    document.getElementById("c_phone").textContent = safeStr(CLIENT.Phone1 || CLIENT.Phone2);
    document.getElementById("c_email").textContent = safeStr(CLIENT.EmailAddress);
    document.getElementById("c_addr").textContent = safeStr(CLIENT.Address);

    showToast("Cliente cargado correctamente ✅", "ok");
  }catch(err){
    clearClient();
    showToast("No pude cargar el cliente: " + (err.message||err), "bad");
  }
}

/* =========================================
   Crear cotización
========================================= */
async function createQuote(){
  if(!getToken()){
    showToast("Debes iniciar sesión para cotizar.", "bad");
    openLogin();
    return;
  }

  if(!CLIENT || !CLIENT.CardCode){
    showToast("Primero selecciona un cliente.", "bad");
    return;
  }

  const lines = LINES
    .filter(l => l.itemCode && Number(l.qty) > 0)
    .map(l => ({ itemCode: l.itemCode, qty: Number(l.qty) }));

  if(!lines.length){
    showToast("Agrega al menos 1 producto con cantidad.", "bad");
    return;
  }

  const comments = String(document.getElementById("comments").value||"").trim();

  const payload = {
    cardCode: CLIENT.CardCode,
    comments,
    paymentMethod: "CONTRA_ENTREGA",
    lines
  };

  const btn = document.getElementById("btnCreateQuote");
  btn.disabled = true;
  btn.textContent = "⏳ Creando cotización...";

  try{
    const r = await apiCreateQuote(payload);
    showToast(`✅ Cotización creada: #${r.docNum} (DocEntry ${r.docEntry})`, "ok");
  }catch(err){
    showToast("Error creando cotización: " + (err.message||err), "bad");
  }finally{
    btn.disabled = false;
    btn.textContent = "✅ Crear cotización";
  }
}

/* =========================================
   Login Mercaderista
========================================= */
function openLogin(){
  document.getElementById("overlay").style.display = "flex";
  document.getElementById("loginState").textContent = "🔒 Bloqueado";
  document.getElementById("loginState").className = "chip bad";
}
function closeLogin(){
  document.getElementById("overlay").style.display = "none";
}

async function doLogin(){
  const username = String(document.getElementById("mUser").value||"").trim().toLowerCase();
  const pin = String(document.getElementById("mPin").value||"").trim();

  if(!username){ showToast("Escribe tu usuario.", "bad"); return; }
  if(!pin){ showToast("Escribe tu PIN.", "bad"); return; }

  const btn = document.getElementById("btnLogin");
  btn.disabled = true;
  btn.textContent = "⏳ Entrando...";

  try{
    const r = await apiMercLogin(username, pin);
    setToken(r.token);
    setUser(r.user || { username });

    document.getElementById("loginState").textContent = "✅ Acceso OK";
    document.getElementById("loginState").className = "chip ok";

    closeLogin();
    setLoginStatus(true);
    document.getElementById("btnLogout").style.display = "inline-flex";

    const u = getUser();
    document.getElementById("whoami").textContent = "Usuario: " + (u.full_name || u.username || "--");

    showToast("✅ Sesión iniciada", "ok");
  }catch(err){
    document.getElementById("loginState").textContent = "⛔ Login falló";
    document.getElementById("loginState").className = "chip bad";
    showToast(err.message || err, "bad");
  }finally{
    btn.disabled = false;
    btn.textContent = "Entrar";
  }
}

function doLogout(){
  clearToken();
  clearUser();
  setLoginStatus(false);
  document.getElementById("btnLogout").style.display = "none";
  document.getElementById("whoami").textContent = "Usuario: --";
  showToast("Sesión cerrada.", "ok");
  openLogin();
}

/* =========================================
   Init
========================================= */
function addRow(){
  LINES.push({
    id: lineSeq++,
    itemCode:"",
    itemCodeRaw:"",
    name:"",
    price:null,
    available:null,
    unit:"",
    qty:"",
    err:""
  });
  renderLines();
  calcTotals();
}

(async function init(){
  try{
    const r = await apiHealth();
    setApiStatus(r.ok);
  }catch{
    setApiStatus(false);
  }

  addRow(); addRow();

  document.getElementById("btnAddRow").addEventListener("click", addRow);
  document.getElementById("btnLoadClient").addEventListener("click", loadClient);
  document.getElementById("btnClearClient").addEventListener("click", ()=>{
    document.getElementById("cardCode").value = "";
    const dl = document.getElementById("clientList");
    dl.innerHTML = "";
    const hint = document.getElementById("clientHint");
    if(hint) hint.textContent = "Escribe 2+ letras para ver sugerencias.";
    clearClient();
  });
  document.getElementById("btnCreateQuote").addEventListener("click", createQuote);

  document.getElementById("btnLogin").addEventListener("click", doLogin);
  document.getElementById("mPin").addEventListener("keydown", (e)=>{
    if(e.key === "Enter") doLogin();
  });

  document.getElementById("btnLogout").addEventListener("click", doLogout);

  if(getToken()){
    setLoginStatus(true);
    document.getElementById("btnLogout").style.display = "inline-flex";
    const u = getUser();
    document.getElementById("whoami").textContent = "Usuario: " + (u.full_name || u.username || "--");
  }else{
    setLoginStatus(false);
    openLogin();
  }
})();

/* =========================================
   ✅ Cliente Suggest Móvil (mantengo lo que ya te funcionó)
========================================= */
(function enableMobileClientSuggest(){
  const input = document.getElementById("cardCode");
  const dl = document.getElementById("clientList");
  const wrap = document.getElementById("clientSuggest");

  if(!input || !dl || !wrap) return;

  const IS_MOBILE = /Android|iPhone|iPad|iPod/i.test(navigator.userAgent)
    || (navigator.maxTouchPoints && navigator.maxTouchPoints > 1);

  let timer = null;
  let lastResults = [];

  function hideMobileBox(){
    wrap.style.display = "none";
    wrap.innerHTML = "";
  }

  function showMobileBox(customers){
    if(!IS_MOBILE) return;
    if(!customers || !customers.length){
      hideMobileBox();
      return;
    }

    const html = `
      <div class="sugBox">
        ${customers.slice(0,20).map(c => `
          <div class="sugItem" data-code="${c.CardCode}">
            <div><span class="sugCode">${c.CardCode}</span> — <span class="sugName">${c.CardName}</span></div>
          </div>
        `).join("")}
      </div>
    `;

    wrap.innerHTML = html;
    wrap.style.display = "block";

    wrap.querySelectorAll(".sugItem").forEach(el=>{
      el.addEventListener("click", ()=>{
        const code = el.getAttribute("data-code");
        input.value = code;
        hideMobileBox();
        const btn = document.getElementById("btnLoadClient");
        if(btn) btn.click();
      });
    });
  }

  async function doSearch(q){
    try{
      const results = await apiSearchCustomers(q);
      lastResults = Array.isArray(results) ? results : [];

      dl.innerHTML = lastResults.map(r =>
        `<option value="${r.CardCode}">${r.CardName}</option>`
      ).join("");

      showMobileBox(lastResults);

    }catch(e){
      dl.innerHTML = "";
      hideMobileBox();
      console.log(e.message || e);
    }
  }

  input.addEventListener("input", (e)=>{
    const q = String(e.target.value || "").trim();
    clearTimeout(timer);

    if(q.length < 2){
      dl.innerHTML = "";
      hideMobileBox();
      return;
    }

    timer = setTimeout(()=> doSearch(q), 220);
  });

  document.addEventListener("click", (e)=>{
    if(!IS_MOBILE) return;
    if(e.target === input) return;
    if(wrap.contains(e.target)) return;
    hideMobileBox();
  });

  const btnLoad = document.getElementById("btnLoadClient");
  if(btnLoad){
    btnLoad.addEventListener("click", async ()=>{
      const v = String(input.value||"").trim();
      if(!v) return;
      if(/^C\d+/i.test(v)) return;

      try{
        const results = lastResults.length ? lastResults : await apiSearchCustomers(v);
        if(results && results.length){
          input.value = results[0].CardCode;
          hideMobileBox();
        }
      }catch{}
    }, true);
  }
})();
</script>

</body>
</html>
