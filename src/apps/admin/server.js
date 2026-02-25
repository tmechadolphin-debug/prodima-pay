<!doctype html>
<html lang="es">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <meta name="robots" content="noindex,nofollow,noarchive" />
  <title>PRODIMA · Admin (Usuarios · Histórico · Dashboard)</title>

  <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;600;700;800;900&display=swap" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/xlsx@0.19.3/dist/xlsx.full.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>

  <style>
    :root{
      --brand:#c31b1c; --accent:#ffbf24; --ink:#1f1f1f; --muted:#6b6b6b;
      --card:#ffffff; --bd:#f1d39f; --shadow: 0 18px 50px rgba(0,0,0,.10);
      --ok:#0c8c6a; --warn:#e67e22; --bad:#c31b1c;
    }
    *{box-sizing:border-box;margin:0;padding:0}
    body{
      font-family:'Montserrat',system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
      color:var(--ink);
      background:
        radial-gradient(1200px 520px at 10% -10%, rgba(255,191,36,.40), transparent 60%),
        radial-gradient(900px 420px at 95% 0%, rgba(195,21,28,.20), transparent 62%),
        linear-gradient(120deg,#fff3db 0%, #ffffff 55%, #fff3db 100%);
      min-height:100vh;
    }

    .topbar{
      background:linear-gradient(90deg,var(--brand) 0%, #e0341d 45%, var(--accent) 100%);
      color:#fff; padding:12px 16px;
      font-weight:900; letter-spacing:.3px;
      display:flex; align-items:center; justify-content:space-between;
      gap:12px; flex-wrap:wrap;
      position:sticky; top:0; z-index:50;
      box-shadow:0 12px 28px rgba(0,0,0,.18);
    }
    .topbar .left{display:flex;align-items:center;gap:10px;flex-wrap:wrap}

    .pill{
      background:#fff; color:#7b1a01;
      border:1px solid #ffd27f; border-radius:999px;
      padding:6px 10px; font-size:12px; font-weight:900;
      box-shadow:0 10px 18px rgba(0,0,0,.06);
      white-space:nowrap;
    }
    .pill.ok{ color:var(--ok); border-color:#b7f0db; }
    .pill.bad{ color:#b30000; border-color:#ffd27f; }
    .pill.warn{ color:#8a4b00; border-color:#ffe3a8; }

    .btn{
      height:40px;border-radius:14px;font-weight:900;border:0;cursor:pointer;
      padding:0 14px;display:inline-flex;align-items:center;justify-content:center;gap:8px;
      letter-spacing:.2px;user-select:none;
    }
    .btn-primary{
      background:linear-gradient(90deg,var(--brand) 0%, var(--accent) 100%);
      color:#fff;box-shadow:0 12px 22px rgba(195,21,28,.25);
    }
    .btn-outline{ background:#fff;color:var(--brand);border:1px solid #ffd27f; }
    .btn-danger{ background:linear-gradient(90deg,#a40b0d 0%, #ff7a00 100%); color:#fff; }
    .btn:disabled{opacity:.6;cursor:not-allowed}

    .wrap{max-width:1400px;margin:18px auto 60px;padding:0 16px}
    .hero{
      background:
        radial-gradient(1000px 420px at 20% -10%, rgba(255,191,36,.55), transparent 60%),
        radial-gradient(900px 420px at 95% 0%, rgba(195,21,28,.22), transparent 62%),
        #fff;
      border:1px solid var(--bd);
      border-radius:22px;
      box-shadow: var(--shadow);
      padding:16px 16px 14px;
    }
    .hero h1{font-size:22px;font-weight:900;color:var(--brand);margin-bottom:4px}
    .hero p{color:#6a3b1b;font-weight:700;font-size:13px;line-height:1.35;max-width:1200px}

    .tabs{display:flex;gap:10px;margin-top:14px;flex-wrap:wrap}
    .tab{
      background:#fff;border:1px solid #ffd27f;border-radius:999px;
      padding:10px 14px;font-weight:900;color:#7b1a01;cursor:pointer;
      box-shadow:0 10px 18px rgba(0,0,0,.06);
      user-select:none;
    }
    .tab.active{
      background:linear-gradient(90deg, rgba(195,21,28,.10), rgba(255,191,36,.35));
      border-color:var(--bd);
      color:var(--brand);
    }

    .section{
      margin-top:16px;background:var(--card);border:1px solid var(--bd);
      border-radius:18px;box-shadow: var(--shadow);overflow:hidden;
    }
    .section-h{
      background:linear-gradient(90deg, rgba(195,21,28,.08), rgba(255,191,36,.25));
      border-bottom:1px solid var(--bd);
      padding:12px 14px;
      display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;
    }
    .section-h strong{color:var(--brand);font-weight:900;letter-spacing:.2px}
    .section-b{padding:14px}

    .row{display:grid;grid-template-columns: 1fr 1fr 1fr 1fr;gap:10px}
    @media (max-width:1100px){ .row{grid-template-columns:1fr 1fr} }
    @media (max-width:560px){ .row{grid-template-columns:1fr} }

    label{display:block;font-weight:900;color:#6a3b1b;font-size:12px;margin-bottom:6px;letter-spacing:.2px}
    .input{
      width:100%;height:42px;border-radius:14px;border:1px solid #ffd27f;
      padding:0 12px;outline:none;background:#fffdf6;font-weight:800;color:#2b1c16;
    }
    select.input{ cursor:pointer; }
    .muted{color:#777;font-weight:800;font-size:12px}

    .cards{display:grid;grid-template-columns: repeat(4, 1fr);gap:12px;margin-top:10px}
    @media (max-width:1100px){ .cards{grid-template-columns: repeat(2, 1fr);} }
    @media (max-width:560px){ .cards{grid-template-columns: 1fr;} }

    .stat{
      background:linear-gradient(180deg,#fffef8 0%, #fff7e8 100%);
      border:1px solid var(--bd);
      border-radius:16px;
      padding:12px;
    }
    .stat .k{color:#7a4a1a;font-weight:900;font-size:12px}
    .stat .v{margin-top:6px;font-weight:900;font-size:20px;color:#111}
    .stat .s{margin-top:6px;font-weight:800;font-size:12px;color:#6a6a6a}

    .grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    @media (max-width:1000px){.grid2{grid-template-columns:1fr}}

    .chartCard{
      background:linear-gradient(180deg,#fffef8 0%, #fff7e8 100%);
      border:1px solid var(--bd);
      border-radius:16px;
      padding:12px;
    }
    .chartWrap{position:relative;width:100%;height:260px;margin-top:10px}

    table{width:100%;border-collapse:separate;border-spacing:0;border:1px solid var(--bd);border-radius:16px;overflow:hidden}
    thead th{
      text-align:left;padding:10px 10px;font-size:12px;color:#6a3b1b;font-weight:900;
      background:linear-gradient(90deg, rgba(195,21,28,.06), rgba(255,191,36,.20));
      border-bottom:1px solid var(--bd); white-space:nowrap;
    }
    tbody td{
      padding:10px 10px;border-bottom:1px dashed var(--bd);vertical-align:top;
      background:#fff;font-size:12px;font-weight:800;color:#2b1c16;
    }
    tbody tr:last-child td{border-bottom:0}
    .tableWrap{overflow:auto;border-radius:16px}

    .barRow{display:flex;gap:10px;align-items:center}
    .bar{flex:1;height:12px;border-radius:999px;background:#ffe7b7;border:1px solid #ffd27f;overflow:hidden}
    .bar>i{display:block;height:100%;width:0%;background:linear-gradient(90deg,var(--brand) 0%, var(--accent) 100%)}

    .tag{display:inline-flex;align-items:center;justify-content:center;border-radius:999px;padding:4px 8px;border:1px solid #ffd27f;font-weight:900;font-size:11px;background:#fff;white-space:nowrap;}
    .tag.ok{border-color:#b7f0db;color:var(--ok)}
    .tag.bad{border-color:#ffd27f;color:#b30000}
    .tag.warn{border-color:#ffe3a8;color:#8a4b00}

    .toast{
      position:fixed;right:18px;bottom:18px;background:#111;color:#fff;
      padding:12px 14px;border-radius:14px;box-shadow:0 20px 50px rgba(0,0,0,.25);
      display:none;max-width:560px;z-index:999;font-weight:800;line-height:1.35;
    }
    .toast.ok{background:linear-gradient(90deg,#0c8c6a,#1bb88a)}
    .toast.bad{background:linear-gradient(90deg,#a40b0d,#ff7a00)}
    .toast.warn{background:linear-gradient(90deg,#8a4b00,#ffbf24)}

    .overlay{
      position:fixed;inset:0;background:rgba(0,0,0,.55);
      display:none;align-items:center;justify-content:center;padding:18px;z-index:1000;
    }
    .modal{
      width:min(560px, 96vw);background:#fff;border:1px solid var(--bd);
      border-radius:18px;box-shadow:0 30px 80px rgba(0,0,0,.28);overflow:hidden;
    }
    .modal-h{
      padding:12px 14px;background:linear-gradient(90deg,var(--brand) 0%, var(--accent) 100%);
      color:#fff;font-weight:900;display:flex;justify-content:space-between;align-items:center;gap:10px;
    }
    .modal-b{padding:14px}
    .modal-b .row2{display:grid;grid-template-columns:1fr 1fr;gap:10px}
    @media (max-width:560px){ .modal-b .row2{grid-template-columns:1fr} }
    .modal-f{padding:14px;display:flex;justify-content:flex-end;gap:10px;border-top:1px solid var(--bd);background:#fffef8}
  </style>
</head>

<body>
  <div class="topbar">
    <div class="left">
      <div>🛠️ PRODIMA · Admin</div>
      <span class="pill bad" id="apiStatus">API: verificando...</span>
      <span class="pill bad" id="authStatus">Admin: no</span>
      <span class="pill warn" id="syncPill">Sync DB: —</span>
      <span class="pill bad" id="whoami">Usuario: --</span>
      <span class="pill warn" id="scopePill">Scope: Todos</span>
    </div>

    <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap">
      <button class="btn btn-outline" id="btnRefresh" type="button" style="height:34px;border-radius:12px;padding:0 10px;font-size:12px">
        🔄 Refrescar
      </button>
      <button class="btn btn-outline" id="btnLogout" type="button" style="display:none;height:34px;border-radius:12px;padding:0 10px;font-size:12px">
        🚪 Salir
      </button>
    </div>
  </div>

  <div class="wrap">
    <section class="hero">
      <h1>Panel Administrador</h1>
      <p>
        ✅ Dashboard (DB cache) con Sync · ✅ Histórico (SL) con entregado batch y paginación real · ✅ Categorías.
      </p>

      <div class="tabs">
        <div class="tab active" data-tab="dash">📊 Dashboard</div>
        <div class="tab" data-tab="quotes">🧾 Histórico</div>
        <div class="tab" data-tab="users">👥 Usuarios</div>
      </div>
    </section>

    <!-- Scope -->
    <section class="section">
      <div class="section-h">
        <strong>🎯 Scope de datos</strong>
        <span class="pill" id="scopeHint">—</span>
      </div>
      <div class="section-b">
        <div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap;padding:12px;border:1px solid #ffd27f;border-radius:16px;background:#fff;box-shadow:0 10px 18px rgba(0,0,0,.06)">
          <label style="display:flex;gap:10px;align-items:center;font-weight:900;color:#6a3b1b">
            <input id="scopeOnlyCreated" type="checkbox" checked style="transform:scale(1.2)">
            Solo usuarios creados (app_users)
          </label>
          <span class="pill ok" id="createdUsersChip">Usuarios creados: 0</span>
          <span class="pill warn" id="excludedChip">Excluidos: 0</span>
          <span class="muted" style="margin-left:auto">Afecta Dashboard DB + Histórico</span>
        </div>
      </div>
    </section>

    <!-- DASHBOARD -->
    <section class="section" id="tab_dash">
      <div class="section-h">
        <strong>📊 Dashboard (DB)</strong>
        <span class="pill" id="dashHint">Listo</span>
      </div>
      <div class="section-b">

        <div class="row">
          <div><label>Desde</label><input id="dashFrom" class="input" type="date"></div>
          <div><label>Hasta</label><input id="dashTo" class="input" type="date"></div>
          <div>
            <label>Categoría</label>
            <select id="dashCat" class="input">
              <option value="__ALL__">Todas (sin filtro)</option>
              <option value="Prod. De limpieza">Prod. De limpieza</option>
              <option value="Art. De limpieza">Art. De limpieza</option>
              <option value="Cuidado de la Ropa">Cuidado de la Ropa</option>
              <option value="Sazonadores">Sazonadores</option>
              <option value="Vinagres">Vinagres</option>
              <option value="Especialidades y GMT">Especialidades y GMT</option>
            </select>
          </div>
          <div><label>&nbsp;</label><button class="btn btn-primary" id="btnLoadDash" type="button" style="width:100%">✅ Cargar</button></div>
        </div>

        <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-top:10px">
          <button class="btn btn-outline" id="btnToday" type="button">Hoy</button>
          <button class="btn btn-outline" id="btnThisMonth" type="button">Este mes</button>
          <button class="btn btn-outline" id="btnThisYear" type="button">Este año</button>
          <span class="pill" id="dashRangePill">—</span>
        </div>

        <div class="cards">
          <div class="stat"><div class="k">Cotizaciones</div><div class="v" id="kpiQuotes">0</div><div class="s" id="kpiNote">DB cache</div></div>
          <div class="stat"><div class="k">Monto cotizado</div><div class="v" id="kpiCot">$ 0.00</div><div class="s">Σ DocTotal</div></div>
          <div class="stat"><div class="k">Monto entregado</div><div class="v" id="kpiEnt">$ 0.00</div><div class="s">Σ Delivery</div></div>
          <div class="stat"><div class="k">Fill rate</div><div class="v" id="kpiFill">0%</div><div class="s">Entregado / Cotizado</div></div>
        </div>

        <div class="grid2" style="margin-top:12px">
          <div class="chartCard">
            <div style="display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap;align-items:center">
              <div style="font-weight:900;color:#6a3b1b">👤 Top usuarios</div>
              <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap">
                <span class="pill" id="topUsersHint">—</span>
                <button class="btn btn-outline" id="btnUsersExpand" type="button" style="height:34px;border-radius:12px;padding:0 10px;font-size:12px">Ver todos</button>
              </div>
            </div>
            <div class="tableWrap">
              <table>
                <thead>
                  <tr>
                    <th>Usuario</th><th>Cant.</th><th>Cotizado</th><th>Entregado</th><th>%</th><th style="width:220px">Visual</th>
                  </tr>
                </thead>
                <tbody id="topUsersBody"></tbody>
              </table>
            </div>
          </div>

          <div class="chartCard">
            <div style="display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap;align-items:center">
              <div style="font-weight:900;color:#6a3b1b">🏬 Top bodegas</div>
              <span class="pill" id="topWhHint">—</span>
            </div>
            <div class="tableWrap">
              <table>
                <thead>
                  <tr>
                    <th>Bodega</th><th>Cant.</th><th>Cotizado</th><th>Entregado</th><th>%</th><th style="width:220px">Visual</th>
                  </tr>
                </thead>
                <tbody id="topWhBody"></tbody>
              </table>
            </div>
          </div>
        </div>

        <div class="grid2" style="margin-top:12px">
          <div class="chartCard">
            <div style="display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap;align-items:center">
              <div style="font-weight:900;color:#6a3b1b">🏷️ Top clientes</div>
              <span class="pill" id="topClientsHint">—</span>
            </div>
            <div class="tableWrap">
              <table>
                <thead><tr><th>Cliente</th><th>Cant.</th><th>$</th><th style="width:220px">Visual</th></tr></thead>
                <tbody id="topClientsBody"></tbody>
              </table>
            </div>
          </div>

          <div class="chartCard">
            <div style="display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap;align-items:center">
              <div style="font-weight:900;color:#6a3b1b">🧩 Categorías</div>
              <span class="pill" id="groupsHint">—</span>
            </div>
            <div class="tableWrap">
              <table>
                <thead><tr><th>Grupo</th><th>Cant.</th><th>$</th><th style="width:220px">Visual</th></tr></thead>
                <tbody id="groupsBody"></tbody>
              </table>
            </div>
            <div class="muted" style="margin-top:10px">
              Si una categoría te sale 0, casi siempre es porque el nombre viene diferente. Este HTML hace match tolerante (minúsculas + espacios).
            </div>
          </div>
        </div>
      </div>
    </section>

    <!-- HISTÓRICO -->
    <section class="section" id="tab_quotes" style="display:none">
      <div class="section-h">
        <strong>🧾 Histórico (SL)</strong>
        <span class="pill" id="quotesHint">—</span>
      </div>
      <div class="section-b">
        <div class="row">
          <div><label>Desde</label><input id="qFrom" class="input" type="date"></div>
          <div><label>Hasta</label><input id="qTo" class="input" type="date"></div>
          <div><label>Usuario</label><input id="qUser" class="input" placeholder="Ej: luis01"></div>
          <div><label>Cliente</label><input id="qClient" class="input" placeholder="Ej: Ricamar"></div>
        </div>

        <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:10px;align-items:center">
          <button class="btn btn-primary" id="btnLoadQuotes" type="button">✅ Buscar</button>
          <button class="btn btn-outline" id="btnExportQuotesXlsx" type="button">📄 Exportar Excel</button>
          <button class="btn btn-outline" id="btnQuotesDay" type="button">📅 Día</button>
          <button class="btn btn-outline" id="btnQuotesOpen" type="button">🟠 Abiertas</button>

          <span class="pill" id="quotesCount">0 registros</span>
          <span class="pill warn" id="quotesScopeInfo">Scope</span>
          <span class="pill" id="quotesFillPill">Fill rate: —</span>

          <div style="margin-left:auto;display:flex;gap:8px;align-items:center;flex-wrap:wrap">
            <button class="btn btn-outline" id="btnPrevPage" type="button" style="height:36px;border-radius:12px;padding:0 10px;font-size:12px">⬅️</button>
            <span class="pill" id="pageInfo">Página 1</span>
            <button class="btn btn-outline" id="btnNextPage" type="button" style="height:36px;border-radius:12px;padding:0 10px;font-size:12px">➡️</button>
            <span class="pill" id="pageMeta">20 por página</span>
          </div>
        </div>

        <div class="tableWrap" style="margin-top:10px">
          <table>
            <thead>
              <tr>
                <th>Fecha</th><th>DocNum</th><th>Cliente</th><th>Usuario</th><th>Bodega</th>
                <th>Estado</th><th>Cotizado</th><th>Entregado</th><th>Comentarios</th>
              </tr>
            </thead>
            <tbody id="quotesBody"></tbody>
          </table>
        </div>
        <div class="muted" style="margin-top:10px">
          Tip: “Entregado” se calcula por batch (20 docNums) vía /quotes/delivered al cambiar de página también.
        </div>
      </div>
    </section>

    <!-- USUARIOS -->
    <section class="section" id="tab_users" style="display:none">
      <div class="section-h">
        <strong>👥 Usuarios</strong>
        <span class="pill" id="usersHint">—</span>
      </div>
      <div class="section-b">

        <div class="row">
          <div><label>Username</label><input id="uUsername" class="input" placeholder="Ej: vane15"></div>
          <div><label>Nombre</label><input id="uFullName" class="input" placeholder="Ej: Vanessa Pérez"></div>
          <div><label>PIN</label><input id="uPin" class="input" type="password" placeholder="Mínimo 4"></div>
          <div>
            <label>Provincia</label>
            <input id="uProvince" class="input" placeholder="Ej: Panamá / Chiriquí / Veraguas">
            <div class="muted" style="margin-top:6px">Bodega sugerida: <b id="uWhPreview">--</b></div>
          </div>
        </div>

        <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:10px">
          <button class="btn btn-primary" id="btnCreateUser" type="button">➕ Crear usuario</button>
          <span class="pill" id="usersCount">0 usuarios</span>
        </div>

        <div class="tableWrap" style="margin-top:10px">
          <table>
            <thead>
              <tr><th>ID</th><th>Username</th><th>Nombre</th><th>Activo</th><th>Provincia</th><th>Bodega</th><th>Creado</th><th>Acción</th></tr>
            </thead>
            <tbody id="usersBody"></tbody>
          </table>
        </div>

      </div>
    </section>

    <div class="muted" style="text-align:center;margin-top:16px">©️ 2026 PRODIMA · Admin interno</div>
  </div>

  <div id="toast" class="toast"></div>

  <!-- LOGIN -->
  <div class="overlay" id="overlayLogin">
    <div class="modal">
      <div class="modal-h">
        <div>🔐 Login Administrador</div>
        <span class="pill" id="loginState">—</span>
      </div>
      <div class="modal-b">
        <div class="row2">
          <div><label>Usuario</label><input id="aUser" class="input" placeholder="ADMIN" autocomplete="username"></div>
          <div><label>Contraseña</label><input id="aPass" class="input" type="password" placeholder="********" autocomplete="current-password"></div>
        </div>
        <div class="muted" style="margin-top:10px">Debes iniciar sesión como admin.</div>
      </div>
      <div class="modal-f">
        <button class="btn btn-primary" id="btnLogin" type="button">Entrar</button>
      </div>
    </div>
  </div>

<script>
/* =========================
   CONFIG
========================= */
const API_BASE = "https://prodima-admin.onrender.com";
const ADMIN_TOKEN_KEY = "prodima_admin_token";
const PAGE_SIZE = 20;

/* =========================
   STATE
========================= */
let USERS_LIST = [];
let CREATED_USER_SET = new Set();

let DASH_RAW = null;
let USERS_EXPANDED = false;

let LAST_QUOTES = [];
let QUOTES_PAGE = 1;
let QUOTES_TOTAL = null;     // si backend lo devuelve
let QUOTES_HAS_MORE = true;
let QUOTES_SKIP = 0;         // skip real

/* =========================
   UI Helpers
========================= */
function showToast(msg, type="ok"){
  const t = document.getElementById("toast");
  t.className = "toast " + (type==="ok" ? "ok" : type==="warn" ? "warn" : "bad");
  t.textContent = msg;
  t.style.display = "block";
  setTimeout(()=> t.style.display = "none", 5200);
}
function money(n){
  const x = Number(n || 0);
  return "$ " + (Number.isFinite(x) ? x.toFixed(2) : "0.00");
}
function pct(n){
  const x = Number(n||0);
  return (Number.isFinite(x) ? x.toFixed(2) : "0.00") + "%";
}
function ymd(d){
  const dt = (d instanceof Date) ? d : new Date(d);
  const y = dt.getFullYear();
  const m = String(dt.getMonth()+1).padStart(2,'0');
  const dd = String(dt.getDate()).padStart(2,'0');
  return `${y}-${m}-${dd}`;
}
function fmtDateES(iso){
  const s = String(iso || "").slice(0,10);
  if(!/^\d{4}-\d{2}-\d{2}$/.test(s)) return s || "--";
  const [Y,M,D] = s.split("-");
  return `${D}/${M}/${Y}`;
}
function getAdminToken(){ return localStorage.getItem(ADMIN_TOKEN_KEY) || ""; }
function setAdminToken(t){ localStorage.setItem(ADMIN_TOKEN_KEY, t); }
function clearAdminToken(){ localStorage.removeItem(ADMIN_TOKEN_KEY); }
function authHeaders(){
  const t = getAdminToken();
  return { "Content-Type":"application/json", ...(t ? {"Authorization":"Bearer "+t} : {}) };
}
function setApiStatus(ok){
  const el = document.getElementById("apiStatus");
  el.className = "pill " + (ok ? "ok" : "bad");
  el.textContent = ok ? "API: OK ✅" : "API: ERROR";
}
function setAuthStatus(ok){
  const el = document.getElementById("authStatus");
  el.className = "pill " + (ok ? "ok" : "bad");
  el.textContent = ok ? "Admin: sí ✅" : "Admin: no";
}
function openLogin(){ document.getElementById("overlayLogin").style.display = "flex"; }
function closeLogin(){ document.getElementById("overlayLogin").style.display = "none"; }

function isScopeOnlyCreated(){ return !!document.getElementById("scopeOnlyCreated")?.checked; }
function setScopePill(){
  const el = document.getElementById("scopePill");
  const only = isScopeOnlyCreated();
  el.className = "pill " + (only ? "ok" : "warn");
  el.textContent = only ? "Scope: Usuarios creados" : "Scope: Todos";
}
function buildCreatedUserSet(){
  const set = new Set();
  for(const u of (USERS_LIST||[])){
    const un = String(u.username||"").trim().toLowerCase();
    if(un) set.add(un);
  }
  CREATED_USER_SET = set;
  document.getElementById("createdUsersChip").textContent = `Usuarios creados: ${set.size}`;
}
function applyScopeToQuotes(quotes){
  if(!isScopeOnlyCreated()) return quotes;
  return (quotes||[]).filter(q=>{
    const u = String(q.usuario||"").trim().toLowerCase();
    return u && CREATED_USER_SET.has(u);
  });
}
function normalizeText(s){
  return String(s||"")
    .trim()
    .toLowerCase()
    .normalize("NFD").replace(/[\u0300-\u036f]/g,"")
    .replace(/\s+/g," ");
}
function barCell(value,total){
  const v = Math.max(0, Number(value||0));
  const t = Math.max(0, Number(total||0));
  const p = t>0 ? Math.max(0, Math.min(100, (v/t)*100)) : 0;
  return `
    <div class="barRow">
      <div class="bar"><i style="width:${p.toFixed(0)}%"></i></div>
      <span class="muted" style="min-width:54px;text-align:right">${p.toFixed(1)}%</span>
    </div>
  `;
}

/* =========================
   API
========================= */
async function apiHealth(){
  const r = await fetch(`${API_BASE}/api/health`);
  const j = await r.json().catch(()=>({}));
  return { ok: r.ok && j.ok, data: j };
}
async function apiAdminLogin(user, pass){
  const res = await fetch(`${API_BASE}/api/admin/login`,{
    method:"POST",
    headers:{ "Content-Type":"application/json" },
    body: JSON.stringify({ user, pass })
  });
  const data = await res.json().catch(()=>({}));
  if(!res.ok || !data.ok) throw new Error(data?.message || "Login inválido");
  return data;
}
async function apiGetUsers(){
  const res = await fetch(`${API_BASE}/api/admin/users`, { headers: authHeaders() });
  const data = await res.json().catch(()=>({}));
  if(!res.ok || !data.ok) throw new Error(data?.message || "No se pudo cargar usuarios");
  return data.users || [];
}
async function apiDashDb(from,to){
  const qs = new URLSearchParams();
  if(from) qs.set("from", from);
  if(to) qs.set("to", to);
  if(isScopeOnlyCreated()) qs.set("onlyCreated","1");
  const res = await fetch(`${API_BASE}/api/admin/quotes/dashboard-db?${qs.toString()}`, { headers: authHeaders() });
  const data = await res.json().catch(()=>({}));
  if(!res.ok || !data.ok) throw new Error(data?.message || "No se pudo cargar dashboard DB");
  return data;
}
async function apiGetQuotes({from,to,user,client,limit=PAGE_SIZE,skip=0}){
  const qs = new URLSearchParams();
  if(from) qs.set("from", from);
  if(to) qs.set("to", to);
  if(user) qs.set("user", user);
  if(client) qs.set("client", client);
  qs.set("limit", String(limit));
  qs.set("skip", String(skip));
  if(isScopeOnlyCreated()) qs.set("onlyCreated","1");

  const res = await fetch(`${API_BASE}/api/admin/quotes?${qs.toString()}`, { headers: authHeaders() });
  const data = await res.json().catch(()=>({}));
  if(!res.ok || !data.ok) throw new Error(data?.message || "No se pudo cargar histórico");
  return data;
}
async function apiDeliveredBatch(docNums, from, to){
  const qs = new URLSearchParams();
  qs.set("docNums", docNums.join(","));
  if(from) qs.set("from", from);
  if(to) qs.set("to", to);
  const res = await fetch(`${API_BASE}/api/admin/quotes/delivered?${qs.toString()}`, { headers: authHeaders() });
  const data = await res.json().catch(()=>({}));
  if(!res.ok || !data.ok) throw new Error(data?.message || "No se pudo calcular entregado");
  return data.delivered || {};
}

/* =========================
   DASHBOARD: categoría (tolerante)
========================= */
function getSelectedCat(){ return String(document.getElementById("dashCat").value || "__ALL__"); }

function applyCategoryView(d0, cat){
  if(!d0 || !d0.ok) return d0;
  if(!cat || cat==="__ALL__") return d0;

  const target = normalizeText(cat);
  const byGroup = Array.isArray(d0.byGroup) ? d0.byGroup : [];
  const gTotal = byGroup.reduce((a,x)=> a + Number(x.cotizado||0), 0);

  // match tolerante
  const gCat = byGroup
    .filter(x => normalizeText(x.group) === target)
    .reduce((a,x)=> a + Number(x.cotizado||0), 0);

  const share = (gTotal > 0) ? (gCat / gTotal) : 0;

  const clone = JSON.parse(JSON.stringify(d0));

  // si no hay data de grupo para esa categoria, no escondemos todo: avisamos
  clone.__cat = cat;
  clone.__share = share;

  clone.totals.cotizado = Number((Number(d0.totals.cotizado||0) * share).toFixed(2));
  clone.totals.entregado = Number((Number(d0.totals.entregado||0) * share).toFixed(2));
  clone.totals.fillRatePct = clone.totals.cotizado > 0 ? Number(((clone.totals.entregado/clone.totals.cotizado)*100).toFixed(2)) : 0;
  clone.totals.quotes = Math.round(Number(d0.totals.quotes||0) * share);

  const scaleArr = (arr)=> (arr||[]).map(r=>({
    ...r,
    cnt: Math.round(Number(r.cnt||0) * share),
    cotizado: Number((Number(r.cotizado||0) * share).toFixed(2)),
    entregado: Number((Number(r.entregado||0) * share).toFixed(2)),
    fillRatePct: (Number(r.cotizado||0) * share) > 0 ? Number(((Number(r.entregado||0)*share)/(Number(r.cotizado||0)*share)*100).toFixed(2)) : 0
  }));

  clone.byUser = scaleArr(d0.byUser);
  clone.byWh = scaleArr(d0.byWh);
  clone.byClient = (d0.byClient||[]).map(r=>({
    ...r,
    cnt: Math.round(Number(r.cnt||0) * share),
    cotizado: Number((Number(r.cotizado||0)*share).toFixed(2))
  }));
  clone.byGroup = (d0.byGroup||[]).filter(x=> normalizeText(x.group) === target).map(x=>({
    ...x,
    cnt: Number(x.cnt||0),
    cotizado: Number(x.cotizado||0)
  }));

  return clone;
}

/* =========================
   DASHBOARD render
========================= */
function renderDashDb(d0){
  const cat = getSelectedCat();
  const d = applyCategoryView(d0, cat);

  document.getElementById("dashRangePill").textContent =
    `${d.from} → ${d.to}` + (cat!=="__ALL__" ? ` · Cat: ${cat}` : "");

  document.getElementById("syncPill").className = "pill " + (d.lastSyncAt ? "ok":"warn");
  document.getElementById("syncPill").textContent = d.lastSyncAt ? ("Sync DB: " + d.lastSyncAt.replace("T"," ").slice(0,19)) : "Sync DB: —";

  document.getElementById("kpiQuotes").textContent = String(d.totals.quotes||0);
  document.getElementById("kpiCot").textContent = money(d.totals.cotizado||0);
  document.getElementById("kpiEnt").textContent = money(d.totals.entregado||0);
  document.getElementById("kpiFill").textContent = pct(d.totals.fillRatePct||0);

  if(cat !== "__ALL__" && d.__share === 0){
    document.getElementById("kpiNote").textContent = `⚠️ Cat no encontrada en byGroup (revise nombres)`;
  }else if(cat !== "__ALL__"){
    document.getElementById("kpiNote").textContent = `Filtro cat (aprox) · share ${(d.__share*100).toFixed(1)}%`;
  }else{
    document.getElementById("kpiNote").textContent = "DB cache";
  }

  // Top usuarios (expandible)
  const users = d.byUser || [];
  document.getElementById("topUsersHint").textContent = `${users.length} usuarios`;
  const denomU = Math.max(1, Number(d.totals.cotizado||0));
  const sliceN = USERS_EXPANDED ? users.length : 12;

  document.getElementById("topUsersBody").innerHTML = users.slice(0,sliceN).map(r=>`
    <tr>
      <td>${r.usuario}</td>
      <td>${Number(r.cnt||0)}</td>
      <td>${money(r.cotizado)}</td>
      <td>${money(r.entregado)}</td>
      <td>${pct(r.fillRatePct)}</td>
      <td>${barCell(r.cotizado, denomU)}</td>
    </tr>
  `).join("") || `<tr><td colspan="6" class="muted">Sin datos</td></tr>`;

  // Top bodegas
  const wh = d.byWh || [];
  document.getElementById("topWhHint").textContent = `${wh.length} bodegas`;
  document.getElementById("topWhBody").innerHTML = wh.slice(0,12).map(r=>`
    <tr>
      <td>${r.warehouse}</td>
      <td>${Number(r.cnt||0)}</td>
      <td>${money(r.cotizado)}</td>
      <td>${money(r.entregado)}</td>
      <td>${pct(r.fillRatePct)}</td>
      <td>${barCell(r.cotizado, denomU)}</td>
    </tr>
  `).join("") || `<tr><td colspan="6" class="muted">Sin datos</td></tr>`;

  // Top clientes
  const cl = d.byClient || [];
  document.getElementById("topClientsHint").textContent = `${cl.length} clientes`;
  document.getElementById("topClientsBody").innerHTML = cl.slice(0,12).map(r=>`
    <tr>
      <td>${r.customer}</td>
      <td>${Number(r.cnt||0)}</td>
      <td>${money(r.cotizado)}</td>
      <td>${barCell(r.cotizado, denomU)}</td>
    </tr>
  `).join("") || `<tr><td colspan="4" class="muted">Sin datos</td></tr>`;

  // Categorías
  const gr = d0.byGroup || []; // acá usamos el total real para que el visual tenga sentido
  const denomG = Math.max(1, gr.reduce((a,x)=>a+Number(x.cotizado||0),0));
  document.getElementById("groupsHint").textContent = `${gr.length} grupos`;
  document.getElementById("groupsBody").innerHTML = gr.slice(0,25).map(r=>`
    <tr>
      <td>${r.group}</td>
      <td>${Number(r.cnt||0)}</td>
      <td>${money(r.cotizado)}</td>
      <td>${barCell(r.cotizado, denomG)}</td>
    </tr>
  `).join("") || `<tr><td colspan="4" class="muted">Sin datos</td></tr>`;
}

async function loadDashboardDb(){
  const from = document.getElementById("dashFrom").value || "";
  const to = document.getElementById("dashTo").value || "";
  document.getElementById("dashHint").textContent = "Cargando...";
  try{
    const d = await apiDashDb(from,to);
    DASH_RAW = d;
    renderDashDb(DASH_RAW);
    document.getElementById("dashHint").textContent = "Listo ✅";
  }catch(e){
    document.getElementById("dashHint").textContent = "Error";
    showToast(e.message || e, "bad");
  }
}

/* =========================
   HISTÓRICO: paginación real (FIX)
========================= */
function inferStatus(q){
  const cs = (q.cancelStatus ?? q.CancelStatus ?? q.Cancelled ?? q.cancelled);
  const csStr = String(cs ?? "").toLowerCase().trim();
  const comm = String(q.comments||"").toLowerCase();
  if(cs === true || csStr.includes("csyes") || csStr.includes("cancel") || comm.includes("cancel")){
    return { text:"Cancelled", cls:"tag warn" };
  }
  const st = String(q.estado||q.DocumentStatus||"").toLowerCase();
  if(st.includes("open")) return { text:"Open", cls:"tag bad" };
  if(st.includes("close")) return { text:"Close", cls:"tag ok" };
  return { text: String(q.estado||"--"), cls:"tag" };
}
function computeQuotesFill(quotes){
  let cot=0, ent=0;
  for(const q of (quotes||[])){
    cot += Number(q.montoCotizacion||0);
    ent += Number(q.montoEntregado||0);
  }
  const p = cot>0 ? (ent/cot)*100 : 0;
  return { cot, ent, pct: p };
}
function renderQuotesTable(){
  const body = document.getElementById("quotesBody");
  body.innerHTML = (LAST_QUOTES||[]).map(q=>{
    const cliente = `${q.cardCode||""} · ${q.cardName||""}`.trim();
    const st = inferStatus(q);
    return `
      <tr>
        <td>${fmtDateES(q.fecha)}</td>
        <td>${q.docNum||""}</td>
        <td>${cliente||"--"}</td>
        <td>${q.usuario||"--"}</td>
        <td>${q.warehouse||"--"}</td>
        <td><span class="${st.cls}">${st.text}</span></td>
        <td>${money(q.montoCotizacion||0)}</td>
        <td>${money(q.montoEntregado||0)}</td>
        <td>${String(q.comments||"").slice(0,200)}</td>
      </tr>
    `;
  }).join("") || `<tr><td colspan="9" class="muted">Sin resultados</td></tr>`;

  document.getElementById("quotesCount").textContent = `${LAST_QUOTES.length} registros`;
  document.getElementById("quotesScopeInfo").textContent = isScopeOnlyCreated() ? "Scope: Usuarios creados" : "Scope: Todos";

  const f = computeQuotesFill(LAST_QUOTES);
  document.getElementById("quotesFillPill").textContent = `Fill rate: ${pct(f.pct)} (Ent ${money(f.ent)} / Cot ${money(f.cot)})`;

  const pages = QUOTES_TOTAL != null ? Math.max(1, Math.ceil(QUOTES_TOTAL / PAGE_SIZE)) : (QUOTES_HAS_MORE ? "…" : QUOTES_PAGE);
  document.getElementById("pageInfo").textContent = `Página ${QUOTES_PAGE} / ${pages}`;
  document.getElementById("pageMeta").textContent = `${PAGE_SIZE} por página` + (QUOTES_TOTAL!=null ? ` · total ${QUOTES_TOTAL}` : "");

  document.getElementById("btnPrevPage").disabled = QUOTES_PAGE <= 1;
  // si no sabemos total, usamos hasMore
  document.getElementById("btnNextPage").disabled = (QUOTES_TOTAL!=null) ? (QUOTES_PAGE >= Math.ceil(QUOTES_TOTAL/PAGE_SIZE)) : (!QUOTES_HAS_MORE);
}

async function hydrateDelivered(from,to){
  const docNums = (LAST_QUOTES||[]).map(x=>Number(x.docNum)).filter(n=>Number.isFinite(n)&&n>0).slice(0,20);
  if(!docNums.length) return;

  try{
    const delivered = await apiDeliveredBatch(docNums, from, to);
    const map = new Map(Object.entries(delivered||{}));
    for(const q of LAST_QUOTES){
      const it = map.get(String(q.docNum||""));
      if(it?.ok){
        q.montoEntregado = Number(it.totalEntregado||0);
        q.pendiente = Number(it.pendiente||0);
      }
    }
  }catch(e){
    showToast("Entregado batch falló: " + (e.message||e), "warn");
  }
}

async function loadQuotesPage({page=1, openOnly=false}={}){
  const from = document.getElementById("qFrom").value || "";
  const to = document.getElementById("qTo").value || "";
  const user = String(document.getElementById("qUser").value||"").trim();
  const client = String(document.getElementById("qClient").value||"").trim();

  const skip = (page - 1) * PAGE_SIZE;
  document.getElementById("quotesHint").textContent = "Cargando...";

  try{
    const data = await apiGetQuotes({from,to,user,client,limit:PAGE_SIZE,skip});
    let rows = data.quotes || [];
    rows = applyScopeToQuotes(rows);

    if(openOnly){
      rows = rows.filter(q => inferStatus(q).text === "Open");
      // ojo: esto afecta el count del page, pero se deja porque tu objetivo es ver Open rápido
    }

    LAST_QUOTES = rows;
    QUOTES_PAGE = page;
    QUOTES_SKIP = skip;

    QUOTES_TOTAL = (data.total != null && Number.isFinite(Number(data.total))) ? Number(data.total) : null;
    QUOTES_HAS_MORE = (rows.length >= PAGE_SIZE) && (QUOTES_TOTAL==null);

    renderQuotesTable();
    await hydrateDelivered(from,to);
    renderQuotesTable();

    document.getElementById("quotesHint").textContent = openOnly ? "Listo ✅ (Open)" : "Listo ✅";
  }catch(e){
    document.getElementById("quotesHint").textContent = "Error";
    showToast(e.message || e, "bad");
  }
}

function exportQuotesXLSX(){
  const rows = LAST_QUOTES || [];
  if(!rows.length){ showToast("No hay datos para exportar.","bad"); return; }
  if(typeof XLSX === "undefined"){ showToast("No cargó XLSX (CDN).","bad"); return; }

  const data = rows.map(q=>({
    Fecha: String(q.fecha||"").slice(0,10),
    DocNum: q.docNum,
    CardCode: q.cardCode,
    CardName: q.cardName,
    Usuario: q.usuario,
    Bodega: q.warehouse,
    Estado: inferStatus(q).text,
    MontoCotizacion: Number(q.montoCotizacion||0),
    MontoEntregado: Number(q.montoEntregado||0),
    Comentarios: q.comments || ""
  }));

  const ws = XLSX.utils.json_to_sheet(data);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, "Historico");
  XLSX.writeFile(wb, `prodima_historico_${Date.now()}.xlsx`);
}

/* =========================
   USERS UI (mínimo)
========================= */
function provinceToWarehouseUI(province){
  const p = String(province || "").trim().toLowerCase();
  if (p === "chiriquí" || p === "chiriqui" || p === "bocas del toro") return "200";
  if (p === "veraguas" || p === "coclé" || p === "cocle" || p === "los santos" || p === "herrera") return "500";
  if (p === "panamá" || p === "panama" || p === "panamá oeste" || p === "panama oeste" || p === "colón" || p === "colon") return "300";
  return "--";
}
async function loadUsers(){
  document.getElementById("usersHint").textContent = "Cargando...";
  try{
    USERS_LIST = await apiGetUsers();
    buildCreatedUserSet();
    document.getElementById("usersCount").textContent = `${USERS_LIST.length} usuarios`;
    document.getElementById("usersHint").textContent = "Listo ✅";
  }catch(e){
    document.getElementById("usersHint").textContent = "Error";
    showToast(e.message||e,"bad");
  }
}

/* =========================
   AUTH
========================= */
async function doLogin(){
  const user = String(document.getElementById("aUser").value||"").trim();
  const pass = String(document.getElementById("aPass").value||"").trim();
  if(!user || !pass) return showToast("Completa usuario y contraseña","bad");

  const btn = document.getElementById("btnLogin");
  btn.disabled=true; btn.textContent="⏳ Entrando...";

  try{
    const r = await apiAdminLogin(user, pass);
    setAdminToken(r.token);
    setAuthStatus(true);
    document.getElementById("whoami").textContent = "Usuario: " + user;
    document.getElementById("btnLogout").style.display = "inline-flex";
    closeLogin();

    await refreshAll();
    showToast("Sesión admin iniciada ✅","ok");
  }catch(e){
    showToast(e.message||e,"bad");
  }finally{
    btn.disabled=false; btn.textContent="Entrar";
  }
}
function doLogout(){
  clearAdminToken();
  setAuthStatus(false);
  document.getElementById("btnLogout").style.display="none";
  document.getElementById("whoami").textContent="Usuario: --";
  openLogin();
}

/* =========================
   Refresh
========================= */
async function refreshAll(){
  await loadUsers();
  await loadDashboardDb();
  await loadQuotesPage({page:1, openOnly:false});
}

/* =========================
   EVENTS
========================= */
document.querySelectorAll(".tab").forEach(el=> el.addEventListener("click", ()=> {
  document.querySelectorAll(".tab").forEach(t=>t.classList.toggle("active", t===el));
  document.getElementById("tab_dash").style.display = el.dataset.tab==="dash" ? "" : "none";
  document.getElementById("tab_quotes").style.display = el.dataset.tab==="quotes" ? "" : "none";
  document.getElementById("tab_users").style.display = el.dataset.tab==="users" ? "" : "none";
}));

document.getElementById("btnLogin").addEventListener("click", doLogin);
document.getElementById("aPass").addEventListener("keydown",(e)=>{ if(e.key==="Enter") doLogin(); });
document.getElementById("btnLogout").addEventListener("click", doLogout);

document.getElementById("btnRefresh").addEventListener("click", async ()=>{
  if(!getAdminToken()) return openLogin();
  await refreshAll();
});

document.getElementById("btnLoadDash").addEventListener("click", loadDashboardDb);
document.getElementById("dashCat").addEventListener("change", ()=>{ if(DASH_RAW) renderDashDb(DASH_RAW); });

document.getElementById("btnUsersExpand").addEventListener("click", ()=>{
  USERS_EXPANDED = !USERS_EXPANDED;
  document.getElementById("btnUsersExpand").textContent = USERS_EXPANDED ? "Ver top 12" : "Ver todos";
  if(DASH_RAW) renderDashDb(DASH_RAW);
});

document.getElementById("btnThisMonth").addEventListener("click", ()=>{
  const now = new Date();
  document.getElementById("dashFrom").value = ymd(new Date(now.getFullYear(), now.getMonth(), 1));
  document.getElementById("dashTo").value = ymd(now);
  loadDashboardDb();
});
document.getElementById("btnThisYear").addEventListener("click", ()=>{
  const now = new Date();
  document.getElementById("dashFrom").value = ymd(new Date(now.getFullYear(), 0, 1));
  document.getElementById("dashTo").value = ymd(now);
  loadDashboardDb();
});
document.getElementById("btnToday").addEventListener("click", ()=>{
  const t = ymd(new Date());
  document.getElementById("dashFrom").value = t;
  document.getElementById("dashTo").value = t;
  loadDashboardDb();
});

document.getElementById("btnLoadQuotes").addEventListener("click", ()=> loadQuotesPage({page:1, openOnly:false}));
document.getElementById("btnQuotesOpen").addEventListener("click", ()=> loadQuotesPage({page:1, openOnly:true}));
document.getElementById("btnQuotesDay").addEventListener("click", ()=>{
  const t = ymd(new Date());
  document.getElementById("qFrom").value = t;
  document.getElementById("qTo").value = t;
  loadQuotesPage({page:1, openOnly:false});
});
document.getElementById("btnExportQuotesXlsx").addEventListener("click", exportQuotesXLSX);

document.getElementById("btnPrevPage").addEventListener("click", ()=>{
  const next = Math.max(1, QUOTES_PAGE - 1);
  loadQuotesPage({page:next, openOnly:false});
});
document.getElementById("btnNextPage").addEventListener("click", ()=>{
  // si tenemos total, validamos
  if(QUOTES_TOTAL != null){
    const maxPage = Math.max(1, Math.ceil(QUOTES_TOTAL / PAGE_SIZE));
    if(QUOTES_PAGE >= maxPage) return;
  }else{
    // si no tenemos total, dependemos de hasMore
    if(!QUOTES_HAS_MORE) return;
  }
  loadQuotesPage({page:QUOTES_PAGE + 1, openOnly:false});
});

document.getElementById("scopeOnlyCreated").addEventListener("change", async ()=>{
  setScopePill();
  await loadDashboardDb();
  await loadQuotesPage({page:1, openOnly:false});
});

/* =========================
   INIT
========================= */
(async function init(){
  try{
    const r = await apiHealth();
    setApiStatus(r.ok);
  }catch{ setApiStatus(false); }

  setScopePill();

  const now = new Date();
  document.getElementById("dashFrom").value = ymd(new Date(now.getFullYear(), now.getMonth(), 1));
  document.getElementById("dashTo").value = ymd(now);

  if(getAdminToken()){
    setAuthStatus(true);
    document.getElementById("btnLogout").style.display="inline-flex";
    document.getElementById("whoami").textContent="Usuario: Admin";
    try{ await refreshAll(); }catch{ doLogout(); }
  }else{
    setAuthStatus(false);
    openLogin();
  }
})();
</script>

</body>
</html>
