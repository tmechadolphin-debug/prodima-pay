<!doctype html>
<html lang="es">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.5, maximum-scale=0.5">
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
      font-size: 20px;
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
      font-size:20px;
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
      font-size:20px;
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
      position:relative;
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
      font-size:20px;
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
      font-size:20px;
      font-weight:900;
      border:1px solid #ffd27f;
      background:#fff;
      color:#7b1a01;
      white-space:nowrap;
    }
    .chip.ok{border-color:#b7f0db;color:#0c8c6a}
    .chip.bad{border-color:#ffd27f;color:#b30000}

    @media (min-width: 1025px){
      body{ zoom: 1; }
    }

    @media (max-width: 600px){
      html{
        font-size: 25px;
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
        font-size: 25px;
      }
      .wrap{ padding: 0 10px; }
      .tableWrap, table{
        overflow-x: auto;
        display:block;
      }
    }

    .sugWrap{
      display:none;
      margin-top:6px;
      position:relative;
      z-index:10;
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
      font-size:20px;
      border-bottom:1px dashed #f3d6a5;
      cursor:pointer;
      user-select:none;
      -webkit-tap-highlight-color: transparent;
      touch-action: manipulation;
    }
    .sugItem:last-child{ border-bottom:0; }
    .sugItem:active{ background:#fff3db; }
    .sugCode{ font-weight:900; color:#c31b1c; }
    .sugName{ color:#2b1c16; }
    .sugMini{
      margin-top:4px;
      font-size:20px;
      color:#777;
      font-weight:800;
    }

    .itemSug{
      display:none;
      position:absolute;
      left:10px;
      right:10px;
      top:54px;
      z-index:30;
    }

    /* ✅ AUMENTO DE LETRA */
    label{ font-size:16px !important; }
    .badge{ font-size:14px !important; }
    .pill{ font-size:14px !important; }
    .note{ font-size:14px !important; }

    .input{ font-size:20px !important; }
    .kv{ font-size:18px !important; grid-template-columns: 160px 1fr; }
    .k{ font-size:18px !important; }
    .v{ font-size:18px !important; }

    thead th{ font-size:16px !important; }
    tbody td{ font-size:18px !important; }
    .t-input{ font-size:20px !important; height:44px; }
    .small{ font-size:15px !important; }
    .totals{ font-size:16px !important; }

    /* “Última cotización” fija al lado del botón */
    .lastQuotePill{
      display:none;
      align-items:center;
      gap:8px;
      padding:8px 12px;
      border-radius:999px;
      border:1px solid #ffd27f;
      background:#fff;
      color:#7b1a01;
      font-weight:900;
      box-shadow:0 10px 18px rgba(0,0,0,.06);
      white-space:nowrap;
    }
    .lastQuotePill b{ color:#c31b1c; }
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
        ✅ Agrega productos por <b>código o descripción</b> (ej: “011..” o “Salsa China”).
      </p>
      <div class="note">
        📦 Importante: en este módulo la <b>cantidad es en CAJAS</b> y el <b>precio mostrado es por CAJA</b> (sin conversiones).
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

              <input id="cardCode" class="input" list="clientList" placeholder="Ej: C01133 o Ricamar" autocomplete="off"/>
              <datalist id="clientList"></datalist>

              <div id="clientSuggest" class="sugWrap"></div>

              <div class="small muted" style="margin-top:6px" id="clientHint">
                Escribe 2+ letras para ver sugerencias.
              </div>
            </div>

            <div style="display:flex;gap:10px;align-items:end;justify-content:flex-end;flex-wrap:wrap">
              <button id="btnLoadClient" class="btn btn-primary" type="button">🔎 Buscar cliente</button>
              <button id="btnClearClient" class="btn btn-outline" type="button">Limpiar</button>
            </div>
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
      </section>

      <!-- CONFIRMACIÓN -->
      <section class="card">
        <div class="card-h">
          <strong>3) Confirmación</strong>
          <span class="badge">Cotizar: Ingreso en SAP</span>
        </div>
        <div class="card-b">

          <!-- ✅ NUEVO: selector de bodega (admins pueden cambiar cualquiera) -->
          <label for="whsCode">Bodega</label>
          <select id="whsCode" class="input"></select>
          <div class="small muted" style="margin-top:6px" id="whsHint">--</div>

          <label for="comments" style="margin-top:12px">Comentarios / Nota (opcional)</label>
          <input id="comments" class="input" placeholder="Ej: Entregar martes / Observaciones..." />

          <label style="margin-top:12px">Adjuntos (foto o PDF)</label>
          <input id="files" class="input" type="file" multiple accept="image/*,application/pdf" capture="environment"/>
          <div id="filesList" class="note" style="display:none;margin-top:10px"></div>

          <div class="footerActions">
            <div class="totals">
              Total líneas: <span id="t_lines">0</span>
              Total cajas: <span id="t_qty">0</span>
              Total estimado: <span id="t_total">$ 0.00</span>
            </div>

            <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center">
              <button id="btnAddRow" class="btn btn-outline" type="button">➕ Agregar línea</button>
              <button id="btnCreateQuote" class="btn btn-primary" type="button">✅ Crear cotización</button>

              <!-- ✅ FIJO: última cotización creada -->
              <div class="lastQuotePill" id="lastQuotePill">
                Última: <b id="lastQuoteNum">#--</b>
              </div>
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
        <span class="badge">Código/Descripción + Cantidad (Cajas)</span>
      </div>
      <div class="card-b">

        <table>
          <thead>
            <tr>
              <th style="width:1200px">Código</th>
              <th style="width:1400pxpx">Descripción</th>
              <th style="width:220px">Precio</th>
              <th style="width:130px">Disp</th>
              <th style="width:130px">Cajas</th>
              <th style="width:130px">Subtotal</th>
              <th style="width:86px">Eliminar</th>
            </tr>
          </thead>
          <tbody id="linesBody"></tbody>
        </table>

        <div class="note" style="margin-top:12px">
          ✅ Sugerencias rápidas: incluye los <b>TOP 100 artículos</b> (offline). Si sale <b>Sin stock</b>, igual puedes cotizar. Es informativo.
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
   ✅ CONFIG API
========================================= */
const API_BASE = "https://prodima-pay.onrender.com";

/* =========================================
   ✅ ADMIN USERS CON BODEGA LIBRE
========================================= */
const ADMIN_FREE_WHS = new Set(["soto","liliana","daniel11","respinosa","test"]);

/* =========================================
   ✅ TOP 100 ARTÍCULOS (SUGERENCIAS OFFLINE)
========================================= */
const TOP100_ITEMS = [
  {"code":"0105","name":"Salsa China Low Sodium 48/5.5","barcode":"025433001059"},
  {"code":"0110","name":"Salsa China Low Sodium 24/10.5","barcode":"025433001103"},
  {"code":"0124","name":"Salsa China Low Sodium 24 oz. 12 unidades/caja.","barcode":"025433001240"},
  {"code":"0205","name":"Salsa China Concentrada. 48/5.5 oz","barcode":"025433002056"},
  {"code":"0210","name":"Salsa China Concentrada  24/10.5 oz","barcode":"025433002100"},
  {"code":"0224","name":"Salsa China Concentrada 24 oz. 12 unidades/caja.","barcode":"025433002247"},
  {"code":"0294","name":"Salsa China  Food Service 4/1 gal.","barcode":"025433002940"},
  {"code":"0305","name":"Salsa Inglesa 48/5.5 oz","barcode":"025433003053"},
  {"code":"0310","name":"Salsa Inglesa 24/10.5 oz","barcode":"025433003107"},
  {"code":"0324","name":"Salsa Inglesa 24 oz. 12 unidades/caja.","barcode":"025433003244"},
  {"code":"0391","name":"Salsa Inglesa 4/ 1 Galon","barcode":"025433003916"},
  {"code":"0405","name":"Salsa Condimentada 48/5.5 oz","barcode":"025433004050"},
  {"code":"0410","name":"Salsa Condimentada 24/10.5 oz","barcode":"025433004104"},
  {"code":"0424","name":"Salsa Condimentada 24 oz. 12 unidades/caja.","barcode":"025433004241"},
  {"code":"0505","name":"Vainilla 48/5.5 oz","barcode":"025433005057-6"},
  {"code":"0506.S","name":"Trio Pack Salsa Sansae 5.5 Onz. 16 por caja.","barcode":"025433005064"},
  {"code":"0510","name":"Vainilla 24/ 10.5 oz.","barcode":"025433005101"},
  {"code":"05908","name":"Pastillas de doble accion para piscinas 5/1","barcode":"025433059081"},
  {"code":"0591","name":"Vainilla  4/1 Gal.","barcode":"025433005910"},
  {"code":"0605","name":"Recao Criollo 48/ 5.5 Oz.","barcode":"025433006054"},
  {"code":"0610","name":"Recao Criollo 24/10.5 Oz.","barcode":"025433006108"},
  {"code":"0624","name":"Recao Criollo 24oz. 12 unidades/caja.","barcode":"025433006245"},
  {"code":"0640","name":"Duo condimentada / Recao de 10.5","barcode":"025433006405"},
  {"code":"0645","name":"Tri-Pk Condi/Criollo/ Low Sodium 5.5 oz. 16 por caja.","barcode":"025433006450"},
  {"code":"1010","name":"Salsa de Ajo Sansae 12/ 10 onzas","barcode":"025433010105"},
  {"code":"1102","name":"Picante Caribeno Original Sansae  36/2onz.","barcode":"025433011027-6"},
  {"code":"1105","name":"Picante Caribeno Original Sansae  12/5 onz.","barcode":"025433011058"},
  {"code":"1110","name":"Picante Caribeno Original Sansae  12/ 10 onz.","barcode":"025433011102"},
  {"code":"1202","name":"Picante Habanero Crushed Pepper Sansae 36/ 2 onz.","barcode":"025433012024"},
  {"code":"1205","name":"Picante Habanero Crushed Pepper Sansae 12/ 5 onz.","barcode":"025433012055"},
  {"code":"1210","name":"Picante Habanero Crushed Pepper Sansae  12/ 10 onz.","barcode":"025433012109"},
  {"code":"1305","name":"Picante Jalapeno Crushed Pepper Sansae 12/ 5 onz.","barcode":"025433013052"},
  {"code":"1410","name":"Picante Chipotle  Sansae Gourmet 12/ 10 onz.","barcode":"025433014103"},
  {"code":"1510","name":"Salsa Tropical Mora Chipotle Sansae Gourmet 12/10.5oz","barcode":"025433015100"},
  {"code":"1610","name":"Salsa Tropical Mango Coco Sansae Gourmet 12/10.5oz","barcode":"025433016107"},
  {"code":"1710","name":"Salsa Tropical Pina Chipotle Sansae Gourmet 12/10.5 onz","barcode":"025433017104"},
  {"code":"1810","name":"Salsa Tropical Manzana Chipotle Sansae Gourmet 12/10.5 onz","barcode":"025433018101"},
  {"code":"1908","name":"Vinagre Blanco Sansae 12/8 oz.","barcode":"025433019085-6"},
  {"code":"1910","name":"Salsa BBQ Original Sansae 12/10.5 oz.","barcode":"025433019108"},
  {"code":"1916","name":"Vinagre Blanco Sansae 24/16 onz","barcode":"025433019160"},
  {"code":"1932","name":"Vinagre Blanco Sansae 12/32 onz","barcode":"025433019320"},
  {"code":"1991","name":"Vinagre Blanco Sansae 4/1 gal","barcode":"025433019917"},
  {"code":"2010","name":"Salsa BBQ Chipotle Sansae 12/10.5 onz.","barcode":"025433020104"},
  {"code":"2016","name":"Vinagre de Sidra de Manzana Sansae 24/16 onz","barcode":"025433020166"},
  {"code":"2032","name":"Vinagre de Sidra de Manzana Sansae 12/32 onz.","barcode":"025433020326"},
  {"code":"2112","name":"Salsa Chimichurri Sansae 24/ 8 onzas","barcode":"025433021125"},
  {"code":"50007","name":"Fibra C/Acerina 10cm 150/1u","barcode":"749968500077"},
  {"code":"50007-1","name":"Fibra C/Acerina  CHICA 10cm 50/1u","barcode":"749968500077"},
  {"code":"51245","name":"Multi Cleaner Atomizador. 12/20 oz (585ML)","barcode":"025433051245"},
  {"code":"51252","name":"Multi Cleaner Repuesto. 12/20 oz.(585 ML)","barcode":"025433051252"},
  {"code":"51320","name":"Multi Cleaner Atomizador. 12/29oz (870 ML)","barcode":"025433051320"},
  {"code":"51337","name":"Multi Cleaner Repuesto. 12/29 oz (870ML)","barcode":"025433051337"},
  {"code":"51344","name":"Multi Cleaner DoyPack 12/500 ml","barcode":"025433051344"},
  {"code":"52143","name":"Pinoclin 12/16 onzas","barcode":"025433052143"},
  {"code":"52242","name":"Pinoclin 12/20 oz (585ml)","barcode":"025433052242"},
  {"code":"52327","name":"Pinoclin 12/29 oz (870 ml)","barcode":"025433052327"},
  {"code":"52334","name":"Pinoclin DoyPack 12/500 ml","barcode":"025433052334"},
  {"code":"62142","name":"Windo Cleaner atomizador 12/16 onz.","barcode":"025433062142"},
  {"code":"62159","name":"Windo Cleaner repuesto 12/16 onzas","barcode":"025433062159"},
  {"code":"62241","name":"Windo Cleaner atomizador 12/20 onz. (585ml)","barcode":"025433062241"},
  {"code":"62258","name":"Windo Cleaner repuesto 12/20 onz. (585ml)","barcode":"025433062258"},
  {"code":"62340","name":"Windo Cleaner atomizador 12/29 onz. (870ml)","barcode":"025433062340"},
  {"code":"62357","name":"Windo Cleaner Repuesto 12/29 onz. (870ml)","barcode":"025433062357"},
  {"code":"62364","name":"Windo Cleaner DoyPack 12/500 ml","barcode":"025433062364"},
  {"code":"6424","name":"Cloro Granular Ocean Blue 65% 7 lbs.","barcode":"025433064245"},
  {"code":"6538","name":"Paño Abrasivo 10x15 100/1u","barcode":"025433065389"},
  {"code":"6539","name":"Esponja con Paño 8x12 48/1","barcode":"025433065396"},
  {"code":"6540","name":"Abrasivo Triple 48/1u","barcode":"025433065402"},
  {"code":"6541","name":"Esponja y Paño 48/1u","barcode":"025433065419"},
  {"code":"6542","name":"Paño Con Esponja 100/1u","barcode":"025433065426"},
  {"code":"6543","name":"Esponja Glowy para vajillas y ollas 48/1u","barcode":"025433065433"},
  {"code":"6544","name":"Esponja Biselada 7x9 50/1u","barcode":"025433065440"},
  {"code":"6560","name":"Trío de Esponjas 18/1","barcode":"025433065389"},
  {"code":"68205","name":"Refil Primaveral Facil Planchado 24/450 ml","barcode":"025433068205"},
  {"code":"68243","name":"Multi Cleaner bano Atomizador 24/20oz","barcode":"025433068243-PA"},
  {"code":"68250","name":"Multi Cleaner bano  Repuesto  24/20oz","barcode":"025433068250"},
  {"code":"68304","name":"Refil Primaveral Facil planchado 12/800 ml.","barcode":"025433068304"},
  {"code":"68311","name":"Refil Primaveral Doble Fragancia con Micro Capsulas. Jumbo Size  4 / 3.1 litros","barcode":"025433068311"},
  {"code":"68328","name":"Multi Cleaner Bano Atomizador 29 oz. (12/29 oz)","barcode":"025433068328"},
  {"code":"68359","name":"Multi Cleaner Bano Repuesto 29 oz. (12/29oz)","barcode":"025433068359"},
  {"code":"6837","name":"Duo MCL. Bano AT + Mcl. Cocina At. 6 / 29 onz.","barcode":"025433068373"},
  {"code":"68403","name":"Refil Lavanda Facil Planchado 24/450 ml.","barcode":"025433068403"},
  {"code":"68410","name":"Refil Lavanda Facil Planchado 12/800 ml.","barcode":"025433068410"},
  {"code":"68434","name":"Multi Cleaner Bano DoyPack 12/500 ml","barcode":"025433068434"},
  {"code":"68503","name":"Refil Fresas Con Chocolate 12/600 ml + 200 ml.","barcode":"025433068854"},
  {"code":"68519","name":"Refil Bebe Facil Planchado 12/800 ML","barcode":"025433068519"},
  {"code":"68717","name":"Refil Bouquet de Primavera Facil Planchado 12/ 800 ML","barcode":"025433068717"},
  {"code":"68809","name":"Refil Blossom Doble Fragancia con Micro Capsulas. Jumbo Size  4 / 3.1 litros","barcode":"025433068809"},
  {"code":"68861","name":"Refil Caricias de Petalos 12/600 ml + 200 ml.","barcode":"025433068861"},
  {"code":"68878","name":"Refil Delicada Vainilla 12/600 ml + 200 ml.","barcode":"025433068878"},
  {"code":"68939","name":"Baking Soda Multi Cleaner 12/500 g","barcode":"025433068939"},
  {"code":"7020","name":"Multi Cleaner Cocina Antibact.Atom. 12/20 onz. ( 585 ml )","barcode":"025433070208"},
  {"code":"7021","name":"Multi Cleaner Cocina Antibact.Rep. 12/20 onz. (585ml)","barcode":"025433070215"},
  {"code":"7029","name":"Multi Cleaner Cocina Antibact.Atom. 12/29 onz. ( 870 ml)","barcode":"025433070291"},
  {"code":"7030","name":"Multi Cleaner Cocina Antibact.Rep. 12/29 onz. ( 870 ml)","barcode":"025433070307"},
  {"code":"70321","name":"Multi Cleaner Cocina Antibact DoyPack 12/500 ml.","barcode":"025433070321"},
  {"code":"70581","name":"Trio Limpieza Del Hogar 20 oz.","barcode":"025433070581"},
  {"code":"7270","name":"Potasa Drain Flush 24/240 Gr.","barcode":"025433072707"},
  {"code":"M0205","name":"Salsa Sansae Mini China Concentrada. 48/5.5 oz","barcode":"025433022054-6"},
  {"code":"M0305","name":"Salsa Sansae Mini Inglesa 48/5.5 oz","barcode":"025433023051-6"},
  {"code":"M0405","name":"Salsa Sansae Mini Condimentada 48/5.5 oz","barcode":"025433024058-6"}
];

function normalize(s){
  return String(s||"")
    .toLowerCase()
    .normalize("NFD").replace(/[\u0300-\u036f]/g,"")
    .trim();
}
function searchLocalItems(q){
  const nq = normalize(q);
  if(nq.length < 2) return [];
  const out = [];
  for(const it of TOP100_ITEMS){
    const code = String(it.code||"");
    const name = String(it.name||"");
    const hay = normalize(code + " " + name);
    if(hay.includes(nq)){
      out.push({
        ItemCode: code,
        ItemName: name,
        SalesUnit: "Caja",
        Barcode: it.barcode || ""
      });
    }
  }
  out.sort((a,b)=>{
    const aStart = normalize(a.ItemCode).startsWith(nq) ? 0 : 1;
    const bStart = normalize(b.ItemCode).startsWith(nq) ? 0 : 1;
    if(aStart !== bStart) return aStart - bStart;
    return a.ItemName.localeCompare(b.ItemName);
  });
  return out.slice(0, 20);
}
function mergeItemResults(local, remote){
  const map = new Map();
  for(const x of (local||[])){
    if(x?.ItemCode) map.set(String(x.ItemCode), x);
  }
  for(const x of (remote||[])){
    if(x?.ItemCode && !map.has(String(x.ItemCode))) map.set(String(x.ItemCode), x);
  }
  return Array.from(map.values()).slice(0, 20);
}

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
   ✅ Anti-duplicado cotización (1 minuto)
========================================= */
const LAST_QUOTE_KEY = "prodima_last_quote";
const QUOTE_COOLDOWN_MS = 60 * 1000;

function getLastQuote(){
  try{
    const raw = localStorage.getItem(LAST_QUOTE_KEY);
    if(!raw) return null;
    const obj = JSON.parse(raw);
    if(!obj || !obj.ts) return null;
    return obj;
  }catch{
    return null;
  }
}
function setLastQuote(docNum, docEntry){
  const obj = { docNum: String(docNum || ""), docEntry: String(docEntry || ""), ts: Date.now() };
  localStorage.setItem(LAST_QUOTE_KEY, JSON.stringify(obj));
  return obj;
}
function updateLastQuoteUI(){
  const pill = document.getElementById("lastQuotePill");
  const numEl = document.getElementById("lastQuoteNum");
  if(!pill || !numEl) return;

  const last = getLastQuote();
  if(last && last.docNum){
    numEl.textContent = "#" + last.docNum;
    pill.style.display = "inline-flex";
  }else{
    pill.style.display = "none";
    numEl.textContent = "#--";
  }
}
function getRemainingCooldownMs(){
  const last = getLastQuote();
  if(!last) return 0;
  const delta = Date.now() - Number(last.ts || 0);
  const remaining = QUOTE_COOLDOWN_MS - delta;
  return remaining > 0 ? remaining : 0;
}

/* =========================================
   UI Helpers
========================================= */
function showToast(msg, type="ok"){
  const t = document.getElementById("toast");
  t.className = "toast " + (type==="ok" ? "ok" : "bad");
  t.textContent = msg;
  t.style.display = "block";
  setTimeout(()=> t.style.display = "none", 30000);
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
   ✅ BODEGAS (selector)
========================================= */
let WAREHOUSES = ["200","300","500","01"]; // fallback

function getIsFreeWhsUser(){
  const u = getUser();
  const username = String(u?.username || "").trim().toLowerCase();
  return ADMIN_FREE_WHS.has(username);
}
function getUserWarehouse(){
  const u = getUser();
  // intenta varios nombres comunes sin romper nada
  return String(u?.warehouse || u?.whsCode || u?.WhsCode || u?.warehouse_code || u?.defaultWarehouse || "300").trim() || "300";
}
function setWarehouseOptions(list){
  const sel = document.getElementById("whsCode");
  if(!sel) return;

  const uniq = Array.from(new Set((list || []).map(x=>String(x).trim()).filter(Boolean)));
  const finalList = uniq.length ? uniq : ["300"];

  sel.innerHTML = finalList.map(w => `<option value="${w}">${w}</option>`).join("");
}
function applyWarehouseLock(){
  const sel = document.getElementById("whsCode");
  const hint = document.getElementById("whsHint");
  if(!sel) return;

  const isFree = getIsFreeWhsUser();
  const uWhs = getUserWarehouse();

  // asegura opciones
  setWarehouseOptions(WAREHOUSES);

  if(isFree){
    sel.disabled = false;
    if(hint) hint.textContent = "✅ Usuario administrador: puedes elegir cualquier bodega.";
    // si no hay valor, asigna la del usuario como default
    if(!sel.value) sel.value = uWhs;
  }else{
    sel.value = uWhs;
    sel.disabled = true;
    if(hint) hint.textContent = `🔒 Bodega fija por usuario: ${uWhs}`;
  }
}

// intenta traer bodegas del API (si existe). Si no existe, se queda con fallback.
async function apiGetWarehouses(){
  try{
    const res = await fetch(`${API_BASE}/api/sap/warehouses`, { headers: authHeaders() });
    const data = await res.json().catch(()=>({}));
    if(res.status === 401){ doLogout(true); throw new Error("Sesión expirada."); }
    if(!res.ok || !data.ok) return null;

    // soporta varias formas
    const arr = data.warehouses || data.results || data.data || data.list;
    if(Array.isArray(arr)){
      // intenta mapear
      const codes = arr.map(x => x?.WhsCode || x?.whsCode || x?.code || x).map(x=>String(x||"").trim()).filter(Boolean);
      return codes.length ? codes : null;
    }
    return null;
  }catch{
    return null;
  }
}

function updateAuthUI(){
  const ok = !!getToken();
  setLoginStatus(ok);

  const btnLogout = document.getElementById("btnLogout");
  if(btnLogout) btnLogout.style.display = ok ? "inline-flex" : "none";

  const who = document.getElementById("whoami");
  if(who){
    const u = getUser();
    who.textContent = ok ? ("Usuario: " + (u.full_name || u.username || "--")) : "Usuario: --";
  }

  // ✅ aplica lock/unlock de bodega según usuario
  applyWarehouseLock();

  if(!ok){
    const overlay = document.getElementById("overlay");
    if(overlay && overlay.style.display !== "flex"){
      openLogin();
    }
  }
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
  if(res.status === 401){ doLogout(true); throw new Error("Sesión expirada. Inicia sesión."); }
  if(!res.ok || !data.ok) throw new Error(data?.message || "No se pudo cargar cliente");
  return data;
}

async function apiSearchCustomers(q){
  const res = await fetch(`${API_BASE}/api/sap/customers/search?q=${encodeURIComponent(q)}&top=20`, {
    headers: authHeaders()
  });
  const data = await res.json().catch(()=>({}));
  if(res.status === 401){ doLogout(true); throw new Error("Sesión expirada. Inicia sesión."); }
  if(!res.ok || !data.ok) throw new Error(data?.message || "No se pudo buscar clientes");
  return Array.isArray(data.results) ? data.results : [];
}

async function apiGetItem(itemCode, cardCode){
  const qs = new URLSearchParams();
  if(cardCode) qs.set("cardCode", cardCode);
  qs.set("uom", "CAJA");
  const suffix = qs.toString() ? `?${qs.toString()}` : "";

  const res = await fetch(`${API_BASE}/api/sap/item/${encodeURIComponent(itemCode)}${suffix}`, {
    headers: authHeaders()
  });
  const data = await res.json().catch(()=>({}));
  if(res.status === 401){ doLogout(true); throw new Error("Sesión expirada. Inicia sesión."); }
  if(!res.ok || !data.ok) throw new Error(data?.message || "No se pudo cargar item");
  return data;
}

async function apiSearchItems(q, cardCode){
  try{
    const qs = new URLSearchParams();
    qs.set("q", q);
    qs.set("top", "20");
    if(cardCode) qs.set("cardCode", cardCode);

    const res = await fetch(`${API_BASE}/api/sap/items/search?${qs.toString()}`, {
      headers: authHeaders()
    });
    const data = await res.json().catch(()=>({}));
    if(res.status === 401){ doLogout(true); throw new Error("Sesión expirada. Inicia sesión."); }
    if(!res.ok || !data.ok) return [];
    return Array.isArray(data.results) ? data.results : [];
  }catch{
    return [];
  }
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
  if(res.status === 401){ doLogout(true); throw new Error("Sesión expirada. Inicia sesión."); }
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
   Cliente autocomplete
========================================= */
const clientInput = document.getElementById("cardCode");
const clientDatalist = document.getElementById("clientList");
const clientSuggestWrap = document.getElementById("clientSuggest");
const clientHint = document.getElementById("clientHint");

const IS_MOBILE = /Android|iPhone|iPad|iPod/i.test(navigator.userAgent)
  || (navigator.maxTouchPoints && navigator.maxTouchPoints > 1);

let clientTimer = null;
let clientLastResults = [];

function hideClientSuggest(){
  if(!clientSuggestWrap) return;
  clientSuggestWrap.style.display = "none";
  clientSuggestWrap.innerHTML = "";
}

function showClientSuggest(customers){
  if(!clientSuggestWrap) return;
  if(!IS_MOBILE){ hideClientSuggest(); return; }
  if(!customers || !customers.length){ hideClientSuggest(); return; }

  clientSuggestWrap.innerHTML = `
    <div class="sugBox">
      ${customers.slice(0,20).map(c => `
        <div class="sugItem" data-code="${c.CardCode}">
          <div><span class="sugCode">${c.CardCode}</span> — <span class="sugName">${c.CardName}</span></div>
        </div>
      `).join("")}
    </div>
  `;
  clientSuggestWrap.style.display = "block";

  clientSuggestWrap.querySelectorAll(".sugItem").forEach(el=>{
    el.addEventListener("click", ()=>{
      const code = el.getAttribute("data-code");
      clientInput.value = code;
      hideClientSuggest();
      const btn = document.getElementById("btnLoadClient");
      if(btn) btn.click();
    });
  });
}

async function doClientSearch(q){
  if(!getToken()) return;
  if(q.length < 2){
    if(clientDatalist) clientDatalist.innerHTML = "";
    hideClientSuggest();
    if(clientHint) clientHint.textContent = "Escribe 2+ letras para ver sugerencias.";
    return;
  }

  const results = await apiSearchCustomers(q);
  clientLastResults = Array.isArray(results) ? results : [];

  if(clientDatalist){
    clientDatalist.innerHTML = clientLastResults.map(r=>{
      const code = String(r.CardCode||"").trim();
      const name = String(r.CardName||"").trim();
      if(!code) return "";
      return `<option value="${code}">${name}</option>`;
    }).join("");
  }

  showClientSuggest(clientLastResults);

  if(clientHint){
    clientHint.textContent = clientLastResults.length ? `Sugerencias: ${clientLastResults.length}` : "Sin sugerencias.";
  }
}

if(clientInput){
  clientInput.addEventListener("input", (e)=>{
    const q = String(e.target.value || "").trim();
    clearTimeout(clientTimer);
    clientTimer = setTimeout(()=> doClientSearch(q), 220);
  });

  clientInput.addEventListener("keydown", (e)=>{
    if(e.key === "Enter"){
      e.preventDefault();
      loadClient();
    }
  });

  document.addEventListener("click", (e)=>{
    if(!IS_MOBILE) return;
    if(e.target === clientInput) return;
    if(clientSuggestWrap && clientSuggestWrap.contains(e.target)) return;
    hideClientSuggest();
  });
}

/* =========================================
   Adjuntos
========================================= */
const FILES = [];
function renderFiles(){
  const box = document.getElementById("filesList");
  if(!box) return;

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

const filesInput = document.getElementById("files");
if(filesInput){
  filesInput.addEventListener("change", (e)=>{
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
}

/* =========================================
   Totals
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
   Item Autocomplete
========================================= */
function ensureItemSuggestEl(tr){
  let el = tr.querySelector(".itemSug");
  if(el) return el;

  el = document.createElement("div");
  el.className = "itemSug";
  tr.querySelector("td")?.appendChild(el);
  return el;
}

function hideItemSuggest(tr){
  const el = tr.querySelector(".itemSug");
  if(!el) return;
  el.style.display = "none";
  el.innerHTML = "";
}

function showItemSuggest(tr, results){
  const el = ensureItemSuggestEl(tr);

  if(!IS_MOBILE){
    el.style.display = "none";
    el.innerHTML = "";
    return;
  }

  if(!results || !results.length){
    hideItemSuggest(tr);
    return;
  }

  el.innerHTML = `
    <div class="sugBox">
      ${results.slice(0,20).map(it => `
        <div class="sugItem" data-code="${it.ItemCode}">
          <div><span class="sugCode">${it.ItemCode}</span> — <span class="sugName">${it.ItemName || ""}</span></div>
          <div class="sugMini">${it.SalesUnit ? `Unidad: ${it.SalesUnit}` : ""}${it.Barcode ? ` · Barra: ${it.Barcode}` : ""}</div>
        </div>
      `).join("")}
    </div>
  `;
  el.style.display = "block";

  const pick = async (ev, node)=>{
    if(ev){
      ev.preventDefault();
      ev.stopPropagation();
    }
    const code = node.getAttribute("data-code") || "";
    const inp = tr.querySelector("input[data-field='itemCode']");
    if(inp){
      inp.value = code;
      hideItemSuggest(tr);
      await onItemCodeChanged(Number(tr.dataset.id), code);
    }
  };

  const MOVE_THRESHOLD = 8;

  el.querySelectorAll(".sugItem").forEach(node=>{
    let sx = 0, sy = 0, moved = false;

    node.addEventListener("touchstart", (ev)=>{
      const t = ev.touches && ev.touches[0];
      if(!t) return;
      sx = t.clientX; sy = t.clientY;
      moved = false;
    }, { passive:true });

    node.addEventListener("touchmove", (ev)=>{
      const t = ev.touches && ev.touches[0];
      if(!t) return;
      if(Math.abs(t.clientX - sx) > MOVE_THRESHOLD || Math.abs(t.clientY - sy) > MOVE_THRESHOLD){
        moved = true;
      }
    }, { passive:true });

    node.addEventListener("touchend", (ev)=>{
      if(moved) return;
      pick(ev, node);
    }, { passive:false });

    node.addEventListener("pointerdown", (ev)=>{
      sx = ev.clientX; sy = ev.clientY;
      moved = false;
    }, { passive:true });

    node.addEventListener("pointermove", (ev)=>{
      if(Math.abs(ev.clientX - sx) > MOVE_THRESHOLD || Math.abs(ev.clientY - sy) > MOVE_THRESHOLD){
        moved = true;
      }
    }, { passive:true });

    node.addEventListener("pointerup", (ev)=>{
      if(moved) return;
      pick(ev, node);
    }, { passive:false });

    node.addEventListener("click", (ev)=> pick(ev, node));
  });
}

async function doItemSearch(lineId, tr, q){
  if(q.length < 2){
    const dl = tr.querySelector("datalist");
    if(dl) dl.innerHTML = "";
    hideItemSuggest(tr);
    return;
  }

  const local = searchLocalItems(q);

  {
    const st0 = ITEM_SUG_STATE.get(lineId) || { timer:null, results:[] };
    st0.results = local;
    ITEM_SUG_STATE.set(lineId, st0);
  }

  const dl = tr.querySelector("datalist");
  if(dl){
    dl.innerHTML = local.map(r=>{
      const code = String(r.ItemCode||"").trim();
      const name = String(r.ItemName||"").trim();
      if(!code) return "";
      return `<option value="${code}">${name}</option>`;
    }).join("");
  }

  showItemSuggest(tr, local);

  if(!getToken()) return;
  const cardCode = CLIENT?.CardCode || "";
  const remote = await apiSearchItems(q, cardCode);
  const merged = mergeItemResults(local, remote);

  if(dl){
    dl.innerHTML = merged.map(r=>{
      const code = String(r.ItemCode||"").trim();
      const name = String(r.ItemName||"").trim();
      if(!code) return "";
      return `<option value="${code}">${name}</option>`;
    }).join("");
  }
  showItemSuggest(tr, merged);

  {
    const st = ITEM_SUG_STATE.get(lineId) || { timer:null, results:[] };
    st.results = merged;
    ITEM_SUG_STATE.set(lineId, st);
  }
}

/* =========================================
   Render Lines
========================================= */
function renderLines(){
  const tbody = document.getElementById("linesBody");
  tbody.innerHTML = LINES.map(l => {
    const price = l.price;
    const avail = l.available;

    const stockClass = (avail == null) ? "" : (avail > 0 ? "stock-ok" : "stock-bad");
    const stockText  = (avail == null) ? "<span class='muted'>--</span>" : `<span class="${stockClass}">${avail}</span>`;
    const sub = (l.qty && l.price != null) ? money(Number(l.qty) * Number(l.price)) : "--";

    const uomText = l.unit ? `Unidad: ${l.unit}` : "";
    const uomWarn = (l.unit && normalize(l.unit).includes("caja")) ? "" : (l.unit ? " · ⚠️ Verifica UoM" : "");

    return `
      <tr data-id="${l.id}">
        <td>
          <input class="t-input" placeholder="Ej: 0110 o Salsa..." value="${l.itemCode||""}"
                 data-field="itemCode" list="itemList_${l.id}" autocomplete="off"/>
          <datalist id="itemList_${l.id}"></datalist>

          <div class="small muted" style="margin-top:6px">${uomText}${uomWarn}</div>
          <div class="itemSug"></div>
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

  tbody.querySelectorAll("input[data-field='itemCode']").forEach(inp=>{
    inp.addEventListener("input", async (e)=>{
      const tr = e.target.closest("tr");
      const lineId = Number(tr.dataset.id);
      const q = String(e.target.value||"").trim();

      const state = ITEM_SUG_STATE.get(lineId) || { timer:null, results:[] };
      clearTimeout(state.timer);

      const exact = (state.results || []).find(x => String(x.ItemCode || "").trim() === q);
      if(exact){
        hideItemSuggest(tr);
        await onItemCodeChanged(lineId, q);
        return;
      }

      state.timer = setTimeout(()=> doItemSearch(lineId, tr, q), 180);
      ITEM_SUG_STATE.set(lineId, state);
    });

    inp.addEventListener("change", async (e)=>{
      const tr = e.target.closest("tr");
      const id = Number(tr.dataset.id);
      const code = String(e.target.value||"").trim();
      hideItemSuggest(tr);
      await onItemCodeChanged(id, code);
    });

    inp.addEventListener("keydown", async (e)=>{
      if(e.key === "Enter"){
        e.preventDefault();
        const tr = e.target.closest("tr");
        const id = Number(tr.dataset.id);
        const code = String(e.target.value||"").trim();
        hideItemSuggest(tr);
        await onItemCodeChanged(id, code);
      }
    });
  });

  tbody.querySelectorAll("input[data-field='qty']").forEach(inp=>{
    inp.addEventListener("input", (e)=>{
      const tr = e.target.closest("tr");
      const id = Number(tr.dataset.id);
      const qty = Number(e.target.value||0);
      const line = LINES.find(x=>x.id===id);
      if(line) line.qty = qty;

      calcTotals();
      autoAddRowIfNeeded();
    });
  });

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

const ITEM_SUG_STATE = new Map();

document.addEventListener("click", (e)=>{
  if(!IS_MOBILE) return;
  if(e.target.closest(".sugBox")) return;
  document.querySelectorAll(".itemSug").forEach(x=>{
    x.style.display="none";
    x.innerHTML="";
  });
});

async function onItemCodeChanged(lineId, code){
  const line = LINES.find(x=>x.id===lineId);
  if(!line) return;

  line.itemCode = code;
  line.name = "";
  line.price = null;
  line.available = null;
  line.unit = "Caja";
  line.err = "";

  renderLines();

  if(!code){
    calcTotals();
    return;
  }

  try{
    const cardCode = CLIENT?.CardCode || "";
    const r = await apiGetItem(code, cardCode);

    line.name = r?.item?.ItemName || `Producto ${code}`;
    line.price = (r?.price != null) ? Number(r.price) : null;
    line.available = (r?.stock?.available != null) ? Number(r.stock.available) : null;
    line.unit = r?.item?.SalesUnit || r?.uom || "Caja";

    line.err = "";

    if(line.available != null && line.available <= 0){
      line.err = "Sin stock (solo informativo, puedes cotizar).";
    }

    if(line.unit && !normalize(line.unit).includes("caja")){
      line.err = (line.err ? (line.err + " ") : "") + "Precio mostrado debe ser por CAJA. Verifica UoM en SAP.";
    }

  }catch(err){
    line.err = String(err.message || err);
  }

  renderLines();
  calcTotals();
  autoAddRowIfNeeded();
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
      const results = clientLastResults.length ? clientLastResults : await apiSearchCustomers(code);
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
   ✅ LIMPIAR TODO DESPUÉS DE CREAR COTIZACIÓN
========================================= */
function resetAllAfterQuote(){
  // Cliente
  document.getElementById("cardCode").value = "";
  if(clientDatalist) clientDatalist.innerHTML = "";
  hideClientSuggest();
  if(clientHint) clientHint.textContent = "Escribe 2+ letras para ver sugerencias.";
  clearClient();

  // Comentarios
  document.getElementById("comments").value = "";

  // Archivos
  FILES.splice(0, FILES.length);
  const fi = document.getElementById("files");
  if(fi) fi.value = "";
  renderFiles();

  // Líneas
  ITEM_SUG_STATE.clear();
  LINES.splice(0, LINES.length);
  lineSeq = 1;
  addRow(); addRow(); addRow();
  calcTotals();

  // Bodega: re-aplicar lock (por si cambia algo visual)
  applyWarehouseLock();
}

/* =========================================
   Crear cotización
========================================= */
let CREATE_IN_FLIGHT = false;

async function createQuote(){
  if(!getToken()){
    showToast("Debes iniciar sesión para cotizar.", "bad");
    openLogin();
    return;
  }

  const remaining = getRemainingCooldownMs();
  const last = getLastQuote();
  if(remaining > 0 && last?.docNum){
    const secs = Math.ceil(remaining / 1000);
    showToast(`La cotización ya fue creada y es la siguiente: #${last.docNum}. Intenta de nuevo en ${secs}s.`, "bad");
    updateLastQuoteUI();
    return;
  }

  if(CREATE_IN_FLIGHT){
    showToast("Ya se está creando una cotización. Espera un momento.", "bad");
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

  // ✅ bodega seleccionada (admins pueden cambiarla)
  const whsSel = document.getElementById("whsCode");
  const whsCode = String(whsSel?.value || getUserWarehouse() || "300").trim() || "300";

  const payload = {
    cardCode: CLIENT.CardCode,
    comments,
    paymentMethod: "CONTRA_ENTREGA",
    lines,

    // ✅ se envía con varios nombres (por compatibilidad con server)
    whsCode,
    WhsCode: whsCode,
    warehouse: whsCode
  };

  const btn = document.getElementById("btnCreateQuote");
  btn.disabled = true;
  btn.textContent = "⏳ Creando cotización...";
  CREATE_IN_FLIGHT = true;

  try{
    const r = await apiCreateQuote(payload);

    setLastQuote(r.docNum, r.docEntry);
    updateLastQuoteUI();

    showToast(`✅ Cotización creada: #${r.docNum} (DocEntry ${r.docEntry})`, "ok");

    // ✅ NUEVO: limpiar todo al crear
    resetAllAfterQuote();

  }catch(err){
    showToast("Error creando cotización: " + (err.message||err), "bad");
  }finally{
    CREATE_IN_FLIGHT = false;
    btn.disabled = false;
    btn.textContent = "✅ Crear cotización";
  }
}

/* =========================================
   Login
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

    // ✅ intenta cargar bodegas reales del API (si existe), luego aplica lock
    const codes = await apiGetWarehouses();
    if(codes && codes.length) WAREHOUSES = codes;
    applyWarehouseLock();

    document.getElementById("loginState").textContent = "✅ Acceso OK";
    document.getElementById("loginState").className = "chip ok";

    closeLogin();
    updateAuthUI();
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

function doLogout(silent=false){
  clearToken();
  clearUser();
  updateAuthUI();
  if(!silent) showToast("Sesión cerrada.", "ok");
}

/* =========================================
   ✅ AUTO-ADD LÍNEAS
========================================= */
function isLineComplete(line){
  const hasCode = !!String(line?.itemCode || "").trim();
  const hasQty  = Number(line?.qty || 0) > 0;
  return hasCode && hasQty;
}

function countTrailingEmptyLines(){
  let count = 0;
  for(let i = LINES.length - 1; i >= 0; i--){
    const l = LINES[i];
    const emptyCode = !String(l?.itemCode || "").trim();
    const emptyQty  = !(Number(l?.qty || 0) > 0);
    if(emptyCode && emptyQty){
      count++;
    }else{
      break;
    }
  }
  return count;
}

function ensureTrailingEmptyLines(minEmptyAtEnd = 2){
  let empties = countTrailingEmptyLines();
  while(empties < minEmptyAtEnd){
    addRow();
    empties++;
  }
}

function autoAddRowIfNeeded(){
  if(!LINES.length){
    ensureTrailingEmptyLines(2);
    return;
  }

  const anyComplete = LINES.some(isLineComplete);
  if(anyComplete){
    ensureTrailingEmptyLines(2);
  }
}

/* =========================================
   Init
========================================= */
function addRow(){
  LINES.push({
    id: lineSeq++,
    itemCode:"",
    name:"",
    price:null,
    available:null,
    unit:"Caja",
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

  // inicializa select bodega (aunque esté bloqueado)
  setWarehouseOptions(WAREHOUSES);
  applyWarehouseLock();

  addRow(); addRow(); addRow();

  const btnAddRow = document.getElementById("btnAddRow");
  if(btnAddRow) btnAddRow.addEventListener("click", addRow);

  const btnLoadClient = document.getElementById("btnLoadClient");
  if(btnLoadClient) btnLoadClient.addEventListener("click", loadClient);

  const btnClearClient = document.getElementById("btnClearClient");
  if(btnClearClient) btnClearClient.addEventListener("click", ()=>{
    document.getElementById("cardCode").value = "";
    if(clientDatalist) clientDatalist.innerHTML = "";
    hideClientSuggest();
    if(clientHint) clientHint.textContent = "Escribe 2+ letras para ver sugerencias.";
    clearClient();
  });

  const btnCreateQuote = document.getElementById("btnCreateQuote");
  if(btnCreateQuote) btnCreateQuote.addEventListener("click", createQuote);

  document.getElementById("btnLogin").addEventListener("click", doLogin);
  document.getElementById("mPin").addEventListener("keydown", (e)=>{
    if(e.key === "Enter") doLogin();
  });

  const btnLogout = document.getElementById("btnLogout");
  if(btnLogout) btnLogout.addEventListener("click", ()=>doLogout(false));

  updateAuthUI();

  updateLastQuoteUI();
})();
</script>

</body>
</html>
