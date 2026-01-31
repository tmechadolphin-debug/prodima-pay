<!doctype html>
<html lang="es">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <meta name="robots" content="noindex,nofollow,noarchive" />
  <title>PRODIMA · Admin (Usuarios · Histórico · Dashboard)</title>

  <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;600;700;800;900&display=swap" rel="stylesheet">

  <!-- ✅ Excel export (XLSX) -->
  <script src="https://cdn.jsdelivr.net/npm/xlsx@0.19.3/dist/xlsx.full.min.js"></script>

  <!-- ✅ Charts -->
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>

  <style>
    /* === TU CSS SIN CAMBIOS === */
    :root{
      --brand:#c31b1c;
      --accent:#ffbf24;
      --ink:#1f1f1f;
      --muted:#6b6b6b;
      --card:#ffffff;
      --bd:#f1d39f;
      --shadow: 0 18px 50px rgba(0,0,0,.10);
      --ok:#0c8c6a;
      --warn:#e67e22;
      --bad:#c31b1c;
    }
    *{box-sizing:border-box;margin:0;padding:0}
    body{
      font-family:'Montserrat',system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
      color:var(--ink);
      background:linear-gradient(120deg,#fff3db 0%, #ffffff 55%, #fff3db 100%);
      min-height:100vh;
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
      flex-wrap:wrap;
    }
    .topbar .left{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
    .pill{
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
    .btn-outline{
      background:#fff;color:var(--brand);border:1px solid #ffd27f;
    }
    .btn-danger{
      background:linear-gradient(90deg,#a40b0d 0%, #ff7a00 100%);
      color:#fff;
    }
    .btn:disabled{opacity:.6;cursor:not-allowed}

    .wrap{max-width:1300px;margin:18px auto 60px;padding:0 16px}
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
    .hero p{color:#6a3b1b;font-weight:700;font-size:13px;line-height:1.35;max-width:1100px}

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

    .row{
      display:grid;
      grid-template-columns: 1fr 1fr 1fr 1fr;
      gap:10px;
    }
    @media (max-width:1050px){ .row{grid-template-columns:1fr 1fr} }
    @media (max-width:560px){ .row{grid-template-columns:1fr} }

    label{display:block;font-weight:900;color:#6a3b1b;font-size:12px;margin-bottom:6px;letter-spacing:.2px}
    .input{
      width:100%;height:42px;border-radius:14px;border:1px solid #ffd27f;
      padding:0 12px;outline:none;background:#fffdf6;font-weight:800;color:#2b1c16;
    }
    .input::placeholder{color:#c08a40;font-weight:700}
    .note{
      margin-top:10px;background:#fff7e8;border:1px dashed #f3c776;border-radius:14px;
      padding:10px 12px;color:#70421c;font-weight:700;font-size:12px;line-height:1.35;
    }

    .cards{
      display:grid;grid-template-columns: repeat(4, 1fr);gap:12px;margin-top:10px;
    }
    @media (max-width:1050px){ .cards{grid-template-columns: repeat(2, 1fr);} }
    @media (max-width:560px){ .cards{grid-template-columns: 1fr;} }

    .stat{
      background:linear-gradient(180deg,#fffef8 0%, #fff7e8 100%);
      border:1px solid var(--bd);
      border-radius:16px;
      padding:12px;
    }
    .stat .k{color:#7a4a1a;font-weight:900;font-size:12px}
    .stat .v{margin-top:6px;font-weight:900;font-size:20px;color:#111}
    .stat .s{margin-top:6px;font-weight:800;font-size:12px;color:#6b6b6b}

    table{
      width:100%;
      border-collapse:separate;border-spacing:0;
      border:1px solid var(--bd);
      border-radius:16px;
      overflow:hidden;
    }
    thead th{
      text-align:left;padding:10px 10px;font-size:12px;color:#6a3b1b;font-weight:900;
      background:linear-gradient(90deg, rgba(195,21,28,.06), rgba(255,191,36,.20));
      border-bottom:1px solid var(--bd);
      white-space:nowrap;
    }
    tbody td{
      padding:10px 10px;border-bottom:1px dashed var(--bd);vertical-align:top;
      background:#fff;font-size:12px;font-weight:800;color:#2b1c16;
    }
    tbody tr:last-child td{border-bottom:0}
    .tableWrap{overflow:auto;border-radius:16px}

    .tag{
      display:inline-flex;align-items:center;justify-content:center;
      border-radius:999px;padding:4px 8px;border:1px solid #ffd27f;
      font-weight:900;font-size:11px;background:#fff;white-space:nowrap;
    }
    .tag.ok{border-color:#b7f0db;color:var(--ok)}
    .tag.bad{border-color:#ffd27f;color:#b30000}
    .tag.warn{border-color:#ffe3a8;color:#8a4b00}

    .muted{color:#777;font-weight:800;font-size:12px}

    .barRow{display:flex;gap:10px;align-items:center}
    .bar{
      flex:1;height:12px;border-radius:999px;background:#ffe7b7;
      border:1px solid #ffd27f;overflow:hidden;
    }
    .bar > i{
      display:block;height:100%;
      width:0%;
      background:linear-gradient(90deg,var(--brand) 0%, var(--accent) 100%);
    }

    .toast{
      position:fixed;right:18px;bottom:18px;background:#111;color:#fff;
      padding:12px 14px;border-radius:14px;box-shadow:0 20px 50px rgba(0,0,0,.25);
      display:none;max-width:560px;z-index:999;font-weight:800;line-height:1.35;
    }
    .toast.ok{background:linear-gradient(90deg,#0c8c6a,#1bb88a)}
    .toast.bad{background:linear-gradient(90deg,#a40b0d,#ff7a00)}

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
    .modal-f{
      padding:14px;display:flex;justify-content:flex-end;gap:10px;
      border-top:1px solid var(--bd);background:#fffef8;
    }
    .chip{
      display:inline-flex;align-items:center;gap:6px;padding:5px 10px;border-radius:999px;
      font-size:11px;font-weight:900;border:1px solid #ffd27f;background:#fff;color:#7b1a01;white-space:nowrap;
    }
    .chip.ok{border-color:#b7f0db;color:#0c8c6a}
    .chip.bad{border-color:#ffd27f;color:#b30000}
    .chip.warn{border-color:#ffe3a8;color:#8a4b00}

    .chartCard{
      background:linear-gradient(180deg,#fffef8 0%, #fff7e8 100%);
      border:1px solid var(--bd);
      border-radius:16px;
      padding:12px;
    }
    .chartWrap{position:relative;width:100%;height:260px;margin-top:10px}

    .tog{
      display:flex;align-items:center;gap:10px;flex-wrap:wrap;
      padding:10px 12px;border:1px solid #ffd27f;border-radius:16px;background:#fff;
      box-shadow:0 10px 18px rgba(0,0,0,.06);
    }
    .switch{
      position:relative;display:inline-block;width:46px;height:26px;flex:0 0 auto;
    }
    .switch input{display:none}
    .slider{
      position:absolute;cursor:pointer;top:0;left:0;right:0;bottom:0;
      background:#ffe7b7;border:1px solid #ffd27f;border-radius:999px;transition:.2s;
    }
    .slider:before{
      position:absolute;content:"";height:20px;width:20px;left:3px;top:2px;
      background:white;border-radius:50%;transition:.2s;
      box-shadow:0 8px 18px rgba(0,0,0,.18);
    }
    .switch input:checked + .slider{
      background:linear-gradient(90deg, rgba(195,21,28,.20), rgba(255,191,36,.45));
    }
    .switch input:checked + .slider:before{transform:translateX(20px)}
  </style>
</head>

<body>
  <div class="topbar">
    <div class="left">
      <div>🛠️ PRODIMA · Admin</div>
      <span class="pill bad" id="apiStatus">API: verificando...</span>
      <span class="pill bad" id="authStatus">Admin: no</span>
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
        ✅ Dashboard (KPIs + gráficos) · ✅ Histórico con export Excel · ✅ Usuarios mercaderistas (provincia → bodega automático).<br>
        ✅ Nuevo: <b>Filtro Scope</b> para ver solo cotizaciones hechas por <b>usuarios creados en este sistema</b> (o ver todo).
      </p>

      <div class="tabs">
        <div class="tab active" data-tab="dash">📊 Dashboard</div>
        <div class="tab" data-tab="quotes">🧾 Histórico</div>
        <div class="tab" data-tab="users">👥 Usuarios</div>
      </div>
    </section>

    <!-- ✅ Scope Toggle (global) -->
    <section class="section">
      <div class="section-h">
        <strong>🎯 Scope de datos</strong>
        <span class="pill" id="scopeHint">—</span>
      </div>
      <div class="section-b">
        <div class="tog">
          <label class="switch" title="Filtrar por usuarios creados en app_users">
            <input id="scopeOnlyCreated" type="checkbox" checked>
            <span class="slider"></span>
          </label>

          <div style="display:flex;flex-direction:column;gap:4px">
            <div style="font-weight:900;color:#6a3b1b">
              Solo cotizaciones de <span style="color:var(--brand)">usuarios creados</span>
            </div>
            <div class="muted">
              Si lo apagas, verás cotizaciones “en general”. Esto también limpia “sin_user / sin_wh” en dashboard.
            </div>
          </div>

          <div style="margin-left:auto;display:flex;gap:8px;flex-wrap:wrap;align-items:center">
            <span class="chip ok" id="createdUsersChip">Usuarios creados: 0</span>
            <span class="chip warn" id="excludedChip">Excluidos: 0</span>
          </div>
        </div>

        <div class="note">
          ✅ Nota: el <b>Entregado</b> se calcula desde SAP con <code>withDelivered=1</code> (trace Quote → Order → Delivery).
          <br>✅ El Dashboard omite <b>Cancelled</b>: solo suma <b>Open</b> y <b>Close</b>.
        </div>
      </div>
    </section>

    <!-- DASHBOARD -->
    <section class="section" id="tab_dash">
      <!-- (tu dashboard igual, sin cambios) -->
      <!-- ... -->
      <!-- ✅ Por espacio: mantén exactamente tu sección dashboard/histórico igual -->
      <!-- (No la alteré, porque el fix era server) -->
      <!-- Pega aquí tu dashboard + histórico tal como lo tenías -->
      <!-- --- -->
      <!-- Para no duplicar 100% aquí (es larguísimo), la parte modificada fue SOLO en Usuarios + JS -->
      <!-- --- -->

      <!-- ⚠️ NOTA: En tu caso real pega tu dashboard completo tal cual ya lo tenías arriba. -->
    </section>

    <!-- HISTÓRICO -->
    <section class="section" id="tab_quotes" style="display:none">
      <!-- (tu histórico igual, sin cambios) -->
    </section>

    <!-- USUARIOS -->
    <section class="section" id="tab_users" style="display:none">
      <div class="section-h">
        <strong>👥 Usuarios mercaderistas</strong>
        <span class="pill" id="usersHint">—</span>
      </div>
      <div class="section-b">

        <div class="row">
          <div>
            <label for="uUsername">Username</label>
            <input id="uUsername" class="input" placeholder="Ej: vane15">
          </div>
          <div>
            <label for="uFullName">Nombre</label>
            <input id="uFullName" class="input" placeholder="Ej: Vanessa Pérez">
          </div>
          <div>
            <label for="uPin">PIN</label>
            <input id="uPin" class="input" placeholder="Mínimo 4" type="password">
          </div>
          <div>
            <label for="uProvince">Provincia (define la bodega)</label>
            <input id="uProvince" class="input" placeholder="Ej: Panamá / Chiriquí / Veraguas">
            <div class="muted" style="margin-top:6px">
              Bodega sugerida: <b id="uWhPreview">--</b>
            </div>
          </div>
        </div>

        <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:10px">
          <button class="btn btn-primary" id="btnCreateUser" type="button">➕ Crear usuario</button>

          <!-- ✅ NUEVO: Export users from server -->
          <button class="btn btn-outline" id="btnExportUsersXlsx" type="button">📄 Exportar Excel</button>

          <span class="pill" id="usersCount">0 usuarios</span>
        </div>

        <div class="tableWrap" style="margin-top:10px">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Username</th>
                <th>Nombre</th>
                <th>Activo</th>
                <th>Provincia</th>
                <th>Bodega</th>
                <th>Creado</th>
                <th>Acción</th>
              </tr>
            </thead>
            <tbody id="usersBody"></tbody>
          </table>
        </div>

        <div class="note">
          ✅ Botón <b>Cambiar PIN</b> por usuario.
          Esto llama a <b>PATCH /api/admin/users/:id/pin</b> con <code>{ pin }</code>.
        </div>
      </div>
    </section>

    <div class="muted" style="text-align:center;margin-top:16px">
      ©️ 2026 PRODIMA · Admin interno
    </div>
  </div>

  <div id="toast" class="toast"></div>

  <!-- LOGIN MODAL -->
  <div class="overlay" id="overlayLogin">
    <div class="modal">
      <div class="modal-h">
        <div>🔐 Login Administrador</div>
        <div class="chip bad" id="loginState">🔒 Bloqueado</div>
      </div>

      <div class="modal-b">
        <div class="row2">
          <div>
            <label for="aUser">Usuario</label>
            <input id="aUser" class="input" placeholder="ADMIN" autocomplete="username">
          </div>
          <div>
            <label for="aPass">Contraseña</label>
            <input id="aPass" class="input" type="password" placeholder="********" autocomplete="current-password">
          </div>
        </div>
        <div class="note" style="margin-top:10px">
          ✅ Debes iniciar sesión como admin para ver Dashboard / Histórico / Usuarios.
        </div>
      </div>

      <div class="modal-f">
        <button class="btn btn-primary" id="btnLogin" type="button">Entrar</button>
      </div>
    </div>
  </div>

  <!-- CHANGE PIN MODAL -->
  <div class="overlay" id="overlayPin">
    <div class="modal">
      <div class="modal-h">
        <div>🔑 Cambiar PIN</div>
        <div class="chip warn" id="pinUserChip">—</div>
      </div>

      <div class="modal-b">
        <div class="row2">
          <div>
            <label for="pinNew">Nuevo PIN</label>
            <input id="pinNew" class="input" type="password" placeholder="Mínimo 4">
          </div>
          <div>
            <label for="pinNew2">Confirmar PIN</label>
            <input id="pinNew2" class="input" type="password" placeholder="Repite PIN">
          </div>
        </div>
        <div class="note" style="margin-top:10px">
          Se actualizará el PIN del usuario seleccionado. Esto NO cambia la bodega, solo credenciales.
        </div>
      </div>

      <div class="modal-f">
        <button class="btn btn-outline" id="btnPinCancel" type="button">Cancelar</button>
        <button class="btn btn-primary" id="btnPinSave" type="button">Guardar</button>
      </div>
    </div>
  </div>

<script>
/* =========================================
   ✅ CONFIG
========================================= */
const API_BASE = "https://prodima-pay.onrender.com";
const ADMIN_TOKEN_KEY = "prodima_admin_token";

/* =========================================
   State
========================================= */
let USERS_LIST = [];
let CREATED_USER_SET = new Set();
let LAST_QUOTES_RAW = [];
let LAST_QUOTES = [];
let QUOTES_PAGE = 1;
const PAGE_SIZE = 20;
let QUOTES_HAS_MORE = true;
let PIN_TARGET = { id:null, username:"" };

/* === TODO TU JS ORIGINAL AQUÍ === */
/* (para mantenerlo corto en este mensaje: pega tu JS completo tal cual, y SOLO agrega este bloque nuevo abajo) */

/* ✅ NUEVO: Export Usuarios XLSX desde server (con token) */
async function exportUsersXlsxFromServer(){
  try{
    const res = await fetch(`${API_BASE}/api/admin/users.xlsx`, { headers: authHeaders() });
    if(!res.ok){
      const txt = await res.text().catch(()=> "");
      throw new Error(txt || "No se pudo exportar usuarios");
    }
    const blob = await res.blob();
    const url = URL.createObjectURL(blob);

    const a = document.createElement("a");
    a.href = url;
    a.download = `prodima_usuarios_${Date.now()}.xlsx`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);

    showToast("Excel de usuarios descargado ✅","ok");
  }catch(err){
    showToast(err.message || err, "bad");
  }
}

/* ✅ Hook del botón */
document.addEventListener("DOMContentLoaded", ()=>{
  const btn = document.getElementById("btnExportUsersXlsx");
  if(btn) btn.addEventListener("click", exportUsersXlsxFromServer);
});
</script>

</body>
</html>
