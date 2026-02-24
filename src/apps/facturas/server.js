<!doctype html>
<html lang="es">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <meta name="robots" content="noindex,nofollow,noarchive" />
  <title>PRODIMA · Facturación (Dashboard)</title>

  <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;600;700;800;900&display=swap" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/xlsx@0.19.3/dist/xlsx.full.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>

  <style>
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
      background:
        radial-gradient(1200px 520px at 10% -10%, rgba(255,191,36,.40), transparent 60%),
        radial-gradient(900px 420px at 95% 0%, rgba(195,21,28,.20), transparent 62%),
        linear-gradient(120deg,#fff3db 0%, #ffffff 55%, #fff3db 100%);
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
      position:sticky; top:0; z-index:50;
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
    .btn:disabled{opacity:.6;cursor:not-allowed}

    .wrap{max-width:1400px;margin:18px auto 70px;padding:0 16px}
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
    .hero p{color:#6a3b1b;font-weight:700;font-size:13px;line-height:1.35;max-width:1180px}

    .section{
      margin-top:14px;background:var(--card);border:1px solid var(--bd);
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
    @media (max-width:1100px){ .row{grid-template-columns:1fr 1fr} }
    @media (max-width:560px){ .row{grid-template-columns:1fr} }

    label{display:block;font-weight:900;color:#6a3b1b;font-size:12px;margin-bottom:6px;letter-spacing:.2px}
    .input{
      width:100%;height:42px;border-radius:14px;border:1px solid #ffd27f;
      padding:0 12px;outline:none;background:#fffdf6;font-weight:800;color:#2b1c16;
    }
    .input::placeholder{color:#c08a40;font-weight:700}

    .cards{
      display:grid;grid-template-columns: repeat(4, 1fr);gap:12px;margin-top:10px;
    }
    @media (max-width:1100px){ .cards{grid-template-columns: repeat(2, 1fr);} }
    @media (max-width:560px){ .cards{grid-template-columns: 1fr;} }

    .stat{
      background:linear-gradient(180deg,#fffef8 0%, #fff7e8 100%);
      border:1px solid var(--bd);
      border-radius:16px;
      padding:12px;
    }
    .stat .k{color:#7a4a1a;font-weight:900;font-size:12px}
    .stat .v{margin-top:6px;font-weight:900;font-size:22px;color:#111}
    .stat .s{margin-top:6px;font-weight:800;font-size:12px;color:#6b6b6b}

    .grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    @media (max-width:1000px){.grid2{grid-template-columns:1fr}}

    .chartCard{
      background:linear-gradient(180deg,#fffef8 0%, #fff7e8 100%);
      border:1px solid var(--bd);
      border-radius:16px;
      padding:12px;
    }
    .chartWrap{position:relative;width:100%;height:260px;margin-top:10px}

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
    .muted{color:#777;font-weight:800;font-size:12px}

    .seg{
      display:flex;gap:8px;flex-wrap:wrap;align-items:center;
      background:#fff;border:1px solid #ffd27f;border-radius:999px;padding:6px;
      box-shadow:0 10px 18px rgba(0,0,0,.06);
    }
    .seg button{
      height:34px;border-radius:999px;border:0;cursor:pointer;
      padding:0 12px;font-weight:900;
      background:transparent;color:#7b1a01;
    }
    .seg button.active{
      background:linear-gradient(90deg, rgba(195,21,28,.10), rgba(255,191,36,.35));
      border:1px solid var(--bd);
      color:var(--brand);
    }

    .toast{
      position:fixed;right:18px;bottom:18px;background:#111;color:#fff;
      padding:12px 14px;border-radius:14px;box-shadow:0 20px 50px rgba(0,0,0,.25);
      display:none;max-width:560px;z-index:999;font-weight:800;line-height:1.35;
    }
    .toast.ok{background:linear-gradient(90deg,#0c8c6a,#1bb88a)}
    .toast.bad{background:linear-gradient(90deg,#a40b0d,#ff7a00)}
    .toast.warn{background:linear-gradient(90deg,#8a4b00,#ffbf24)}

    /* Modal Login */
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
  </style>
</head>

<body>
  <div class="topbar">
    <div class="left">
      <div>📊 PRODIMA · Facturación</div>
      <span class="pill bad" id="apiPill">API: —</span>
      <span class="pill bad" id="authPill">Admin: no</span>
      <span class="pill warn" id="rangePill">—</span>
    </div>

    <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap">
      <button class="btn btn-outline" id="btnExportAll" type="button" disabled>📄 Export Excel</button>
      <button class="btn btn-outline" id="btnRefresh" type="button" disabled>🔄 Refrescar</button>
      <button class="btn btn-outline" id="btnLogout" type="button" style="display:none">🚪 Salir</button>
    </div>
  </div>

  <div class="wrap" id="appContent" style="display:none">
    <section class="hero">
      <h1>Dashboard de Facturación</h1>
      <p>
        Totales de facturas por <b>Cliente</b> y por <b>Bodega</b>.<br>
        ✅ Top por <b>$</b> y por <b># Facturas</b> · ✅ Visual = % del total.
      </p>
    </section>

    <section class="section">
      <div class="section-h">
        <strong>📅 Filtros</strong>
        <span class="pill" id="hint">Listo</span>
      </div>
      <div class="section-b">
        <div class="row">
          <div>
            <label>Desde</label>
            <input id="from" class="input" type="date">
          </div>
          <div>
            <label>Hasta</label>
            <input id="to" class="input" type="date">
          </div>
          <div>
            <label>Atajos</label>
            <div style="display:flex;gap:8px;flex-wrap:wrap">
              <button class="btn btn-outline" id="btnToday" type="button" style="height:42px">Hoy</button>
              <button class="btn btn-outline" id="btnThisMonth" type="button" style="height:42px">Este mes</button>
              <button class="btn btn-outline" id="btnThisYear" type="button" style="height:42px">Este año</button>
            </div>
          </div>
          <div>
            <label>&nbsp;</label>
            <button class="btn btn-primary" id="btnLoad" type="button" style="width:100%">✅ Cargar</button>
          </div>
        </div>

        <div class="muted" style="margin-top:10px">
          Tip: rangos grandes = más lento. Usa mes o trimestre para velocidad.
        </div>
      </div>
    </section>

    <section class="section">
      <div class="section-h">
        <strong>⭐ KPIs</strong>
        <span class="pill" id="kpiHint">—</span>
      </div>
      <div class="section-b">
        <div class="cards">
          <div class="stat">
            <div class="k">Facturas</div>
            <div class="v" id="kpiInvoices">0</div>
            <div class="s">Cantidad de documentos</div>
          </div>
          <div class="stat">
            <div class="k">Facturación total</div>
            <div class="v" id="kpiDollars">$ 0.00</div>
            <div class="s">Suma DocTotal</div>
          </div>
          <div class="stat">
            <div class="k">Top bodega</div>
            <div class="v" id="kpiTopWh">—</div>
            <div class="s" id="kpiTopWhVal">—</div>
          </div>
          <div class="stat">
            <div class="k">Top cliente</div>
            <div class="v" id="kpiTopCust">—</div>
            <div class="s" id="kpiTopCustVal">—</div>
          </div>
        </div>
      </div>
    </section>

    <section class="section">
      <div class="section-h">
        <strong>🏆 Top (con visual %)</strong>
        <div class="seg">
          <button id="segMoney" class="active" type="button">Por $</button>
          <button id="segInvoices" type="button">Por # Facturas</button>
        </div>
      </div>

      <div class="section-b">
        <div class="grid2">
          <div class="chartCard">
            <div style="display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap;align-items:center">
              <div style="font-weight:900;color:#6a3b1b">Top clientes</div>
              <span class="pill" id="topClientsHint">—</span>
            </div>
            <div class="chartWrap"><canvas id="chClients"></canvas></div>
            <div class="tableWrap">
              <table>
                <thead>
                  <tr><th>Cliente</th><th id="thClientMetric">$</th><th style="width:220px">Visual</th></tr>
                </thead>
                <tbody id="topClientsBody"></tbody>
              </table>
            </div>
          </div>

          <div class="chartCard">
            <div style="display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap;align-items:center">
              <div style="font-weight:900;color:#6a3b1b">Top bodegas</div>
              <span class="pill" id="topWhHint">—</span>
            </div>
            <div class="chartWrap"><canvas id="chWh"></canvas></div>
            <div class="tableWrap">
              <table>
                <thead>
                  <tr><th>Bodega</th><th id="thWhMetric">$</th><th style="width:220px">Visual</th></tr>
                </thead>
                <tbody id="topWhBody"></tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </section>

    <section class="section">
      <div class="section-h">
        <strong>🧾 Cliente × Bodega</strong>
        <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center">
          <input id="search" class="input" style="height:38px;border-radius:12px;width:280px" placeholder="Buscar cliente / bodega...">
          <span class="pill" id="rowsPill">0 filas</span>
        </div>
      </div>
      <div class="section-b">
        <div class="tableWrap">
          <table>
            <thead>
              <tr><th>Cliente</th><th>Bodega</th><th>$</th><th># Facturas</th><th style="width:220px">Visual</th></tr>
            </thead>
            <tbody id="mainBody"></tbody>
          </table>
        </div>
      </div>
    </section>
  </div>

  <div id="toast" class="toast"></div>

  <div class="overlay" id="loginOverlay">
    <div class="modal">
      <div class="modal-h">
        <div>🔐 Login Administrador</div>
        <span class="pill" id="loginState">—</span>
      </div>
      <div class="modal-b">
        <div class="row2">
          <div>
            <label>Usuario</label>
            <input id="aUser" class="input" placeholder="PRODIMA" autocomplete="username">
          </div>
          <div>
            <label>Contraseña</label>
            <input id="aPass" class="input" type="password" placeholder="********" autocomplete="current-password">
          </div>
        </div>
        <div class="muted" style="margin-top:10px">
          Se valida contra <b>/api/admin/login</b>. Si falla, verás el error exacto.
        </div>
      </div>
      <div class="modal-f">
        <button class="btn btn-primary" id="btnLogin" type="button">Entrar</button>
      </div>
    </div>
  </div>

<script>
/* =========================
   ✅ CONFIG
========================= */
const API_BASE = "https://prodima-sales-admin.onrender.com";
const TOKEN_KEY = "prodima_sales_admin_token";

/* ✅ Ajusta aquí si quieres más/menos carga (recomendado 300–800) */
const DASH_MAX_DOCS = 600;

/* =========================
   STATE
========================= */
let LAST = null;
let MODE = "money";
let chClients = null, chWh = null;

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
function ymd(d){
  const dt = d instanceof Date ? d : new Date(d);
  const y = dt.getFullYear();
  const m = String(dt.getMonth()+1).padStart(2,'0');
  const dd = String(dt.getDate()).padStart(2,'0');
  return `${y}-${m}-${dd}`;
}
function token(){ return localStorage.getItem(TOKEN_KEY) || ""; }
function setToken(t){ localStorage.setItem(TOKEN_KEY, t); }
function clearToken(){ localStorage.removeItem(TOKEN_KEY); }
function headers(){
  const t = token();
  return { "Content-Type":"application/json", ...(t ? {"Authorization":"Bearer "+t} : {}) };
}

function openLogin(){ document.getElementById("loginOverlay").style.display = "flex"; }
function closeLogin(){ document.getElementById("loginOverlay").style.display = "none"; }

function setAuthUI(ok){
  const ap = document.getElementById("authPill");
  ap.className = "pill " + (ok ? "ok" : "bad");
  ap.textContent = ok ? "Admin: sí ✅" : "Admin: no";

  document.getElementById("btnLogout").style.display = ok ? "inline-flex" : "none";
  document.getElementById("appContent").style.display = ok ? "" : "none";
  document.getElementById("btnRefresh").disabled = !ok;
}

function setRange(from, to){
  document.getElementById("from").value = from;
  document.getElementById("to").value = to;
  document.getElementById("rangePill").textContent = `${from} → ${to}`;
}

/* =========================
   API
========================= */
async function apiHealth(){
  const r = await fetch(`${API_BASE}/api/health`);
  const j = await r.json().catch(()=>({}));
  return { ok: r.ok && j.ok, data: j };
}
async function apiLogin(user, pass){
  const r = await fetch(`${API_BASE}/api/admin/login`,{
    method:"POST",
    headers:{ "Content-Type":"application/json" },
    body: JSON.stringify({ user, pass })
  });
  const j = await r.json().catch(()=>({}));
  if(!r.ok || !j.ok) throw new Error(j?.message || `HTTP ${r.status}`);
  return j.token;
}
async function apiLoadDashboard(from, to){
  const qs = new URLSearchParams();
  if(from) qs.set("from", from);
  if(to) qs.set("to", to);
  qs.set("maxDocs", String(DASH_MAX_DOCS));

  const r = await fetch(`${API_BASE}/api/admin/invoices/dashboard?${qs.toString()}`, { headers: headers() });
  const j = await r.json().catch(()=>({}));
  if(!r.ok || !j.ok) throw new Error(j?.message || `HTTP ${r.status} ${r.statusText}`);
  return j;
}

/* =========================
   Aggregations extra (top por # facturas)
========================= */
function buildAgg(table){
  const byCustomerDol = new Map();
  const byCustomerInv = new Map();
  const byWhDol = new Map();
  const byWhInv = new Map();

  let totalDol = 0;
  let totalInv = 0;

  for(const r of (table||[])){
    const cust = String(r.customer||"SIN_CLIENTE");
    const wh = String(r.warehouse||"SIN_WH");
    const dol = Number(r.dollars||0);
    const inv = Number(r.invoices||0);

    totalDol += dol;
    totalInv += inv;

    byCustomerDol.set(cust, (byCustomerDol.get(cust)||0) + dol);
    byCustomerInv.set(cust, (byCustomerInv.get(cust)||0) + inv);

    byWhDol.set(wh, (byWhDol.get(wh)||0) + dol);
    byWhInv.set(wh, (byWhInv.get(wh)||0) + inv);
  }

  const toSorted = (m) => Array.from(m.entries())
    .map(([k,v])=>({key:k, value:Number(v||0)}))
    .sort((a,b)=> b.value - a.value);

  return {
    totals: { dollars: totalDol, invoices: totalInv },
    topCustomersByMoney: toSorted(byCustomerDol),
    topCustomersByInv: toSorted(byCustomerInv),
    topWhByMoney: toSorted(byWhDol),
    topWhByInv: toSorted(byWhInv),
  };
}

function pct(value,total){
  const v = Math.max(0, Number(value||0));
  const t = Math.max(0, Number(total||0));
  return t>0 ? Math.max(0, Math.min(100, (v/t)*100)) : 0;
}
function barCell(p){
  return `
    <div class="barRow">
      <div class="bar"><i style="width:${p.toFixed(0)}%"></i></div>
      <span class="muted" style="min-width:54px;text-align:right">${p.toFixed(1)}%</span>
    </div>
  `;
}
function renderBarChart(existing, canvasId, labels, values){
  const ctx = document.getElementById(canvasId);
  if(!ctx) return existing;
  if(existing){ existing.destroy(); existing = null; }
  return new Chart(ctx, {
    type:"bar",
    data:{ labels, datasets:[{ data: values }] },
    options:{ responsive:true, maintainAspectRatio:false, plugins:{ legend:{display:false} } }
  });
}

/* =========================
   Render
========================= */
function renderKPIs(data, agg){
  const inv = Number(data?.totals?.invoices ?? agg?.totals?.invoices ?? 0);
  const dol = Number(data?.totals?.dollars ?? agg?.totals?.dollars ?? 0);

  document.getElementById("kpiInvoices").textContent = String(inv||0);
  document.getElementById("kpiDollars").textContent = money(dol);

  const topWh = (MODE==="money" ? agg.topWhByMoney : agg.topWhByInv)[0];
  const topCu = (MODE==="money" ? agg.topCustomersByMoney : agg.topCustomersByInv)[0];

  document.getElementById("kpiTopWh").textContent = topWh ? topWh.key : "—";
  document.getElementById("kpiTopWhVal").textContent = topWh ? (MODE==="money" ? money(topWh.value) : `${topWh.value} facturas`) : "—";

  document.getElementById("kpiTopCust").textContent = topCu ? (topCu.key.length>22 ? topCu.key.slice(0,22)+"…" : topCu.key) : "—";
  document.getElementById("kpiTopCustVal").textContent = topCu ? (MODE==="money" ? money(topCu.value) : `${topCu.value} facturas`) : "—";
}

function renderTops(agg){
  const isMoney = MODE==="money";
  document.getElementById("thClientMetric").textContent = isMoney ? "$" : "#";
  document.getElementById("thWhMetric").textContent = isMoney ? "$" : "#";

  const topClients = (isMoney ? agg.topCustomersByMoney : agg.topCustomersByInv).slice(0, 12);
  const topWh = (isMoney ? agg.topWhByMoney : agg.topWhByInv).slice(0, 12);

  const denomClients = isMoney ? agg.totals.dollars : agg.totals.invoices;
  const denomWh = isMoney ? agg.totals.dollars : agg.totals.invoices;

  document.getElementById("topClientsHint").textContent = `${topClients.length} clientes`;
  document.getElementById("topWhHint").textContent = `${topWh.length} bodegas`;

  document.getElementById("topClientsBody").innerHTML = topClients.map(x=>{
    const p = pct(x.value, denomClients);
    return `<tr><td>${x.key}</td><td>${isMoney ? money(x.value) : x.value}</td><td>${barCell(p)}</td></tr>`;
  }).join("") || `<tr><td colspan="3" class="muted">Sin datos</td></tr>`;

  document.getElementById("topWhBody").innerHTML = topWh.map(x=>{
    const p = pct(x.value, denomWh);
    return `<tr><td>${x.key}</td><td>${isMoney ? money(x.value) : x.value}</td><td>${barCell(p)}</td></tr>`;
  }).join("") || `<tr><td colspan="3" class="muted">Sin datos</td></tr>`;

  const cLabels = topClients.map(x=> x.key.length>18 ? x.key.slice(0,18)+"…" : x.key);
  const cVals = topClients.map(x=> Number(x.value||0));
  const wLabels = topWh.map(x=> x.key);
  const wVals = topWh.map(x=> Number(x.value||0));

  chClients = renderBarChart(chClients, "chClients", cLabels, cVals);
  chWh = renderBarChart(chWh, "chWh", wLabels, wVals);
}

function renderMainTable(data){
  const q = String(document.getElementById("search").value||"").trim().toLowerCase();
  const rows = (data?.table || []).filter(r=>{
    if(!q) return true;
    return String(r.customer||"").toLowerCase().includes(q) || String(r.warehouse||"").toLowerCase().includes(q);
  });

  document.getElementById("rowsPill").textContent = `${rows.length} filas`;

  const totalDol = Number(data?.totals?.dollars || 0);

  document.getElementById("mainBody").innerHTML = rows.slice(0, 1600).map(r=>{
    const p = pct(r.dollars, totalDol);
    return `
      <tr>
        <td>${r.customer}</td>
        <td>${r.warehouse}</td>
        <td>${money(r.dollars)}</td>
        <td>${r.invoices}</td>
        <td>${barCell(p)}</td>
      </tr>
    `;
  }).join("") || `<tr><td colspan="5" class="muted">Sin datos</td></tr>`;
}

/* =========================
   Export
========================= */
function exportAll(){
  if(!LAST || !LAST.ok) return showToast("No hay data para exportar", "bad");
  if(typeof XLSX === "undefined") return showToast("No cargó XLSX (CDN).", "bad");

  const wb = XLSX.utils.book_new();

  const main = (LAST.table||[]).map(r=>({
    Cliente: r.customer,
    Bodega: r.warehouse,
    Dolares: Number(r.dollars||0),
    Facturas: Number(r.invoices||0),
  }));
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(main), "Cliente_x_Bodega");

  const agg = buildAgg(LAST.table||[]);
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(agg.topCustomersByMoney.slice(0,200).map(x=>({Cliente:x.key, Dolares:x.value}))), "TopClientes_$");
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(agg.topCustomersByInv.slice(0,200).map(x=>({Cliente:x.key, Facturas:x.value}))), "TopClientes_#");
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(agg.topWhByMoney.slice(0,200).map(x=>({Bodega:x.key, Dolares:x.value}))), "TopBodegas_$");
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(agg.topWhByInv.slice(0,200).map(x=>({Bodega:x.key, Facturas:x.value}))), "TopBodegas_#");

  XLSX.writeFile(wb, `facturacion_dashboard_${Date.now()}.xlsx`);
}

/* =========================
   Load Dashboard
========================= */
async function loadDashboard(){
  const from = document.getElementById("from").value || "";
  const to = document.getElementById("to").value || "";

  if(from && to && from > to){
    showToast("La fecha 'Desde' no puede ser mayor que 'Hasta'.", "bad");
    return;
  }

  document.getElementById("hint").textContent = "Cargando...";
  document.getElementById("kpiHint").textContent = "Cargando...";
  document.getElementById("rangePill").textContent = (from||to) ? `${from||"—"} → ${to||"—"}` : "Sin filtro";

  try{
    const data = await apiLoadDashboard(from,to);
    LAST = data;

    const agg = buildAgg(data.table||[]);
    renderKPIs(data, agg);
    renderTops(agg);
    renderMainTable(data);

    document.getElementById("btnExportAll").disabled = !(data.table||[]).length;
    document.getElementById("hint").textContent = "Listo ✅";
    document.getElementById("kpiHint").textContent = "Listo ✅";
    showToast("Dashboard cargado ✅","ok");
  }catch(e){
    document.getElementById("hint").textContent = "Error";
    document.getElementById("kpiHint").textContent = "Error";
    showToast(e.message || e, "bad");
  }
}

/* =========================
   Auth
========================= */
async function doLogin(){
  const user = String(document.getElementById("aUser").value||"").trim();
  const pass = String(document.getElementById("aPass").value||"").trim();
  if(!user || !pass) return showToast("Completa usuario y contraseña.", "bad");

  const st = document.getElementById("loginState");
  st.className = "pill warn";
  st.textContent = "Validando...";

  const btn = document.getElementById("btnLogin");
  btn.disabled = true;
  btn.textContent = "⏳ Entrando...";

  try{
    const t = await apiLogin(user, pass);
    setToken(t);

    st.className = "pill ok";
    st.textContent = "OK ✅";

    closeLogin();
    setAuthUI(true);
    document.getElementById("btnRefresh").disabled = false;

    await loadDashboard();
  }catch(e){
    st.className = "pill bad";
    st.textContent = "Falló ⛔";
    showToast(e.message || e, "bad");
  }finally{
    btn.disabled = false;
    btn.textContent = "Entrar";
  }
}

function doLogout(){
  clearToken();
  setAuthUI(false);
  openLogin();
}

/* =========================
   Segmented control
========================= */
function setMode(m){
  MODE = m;
  document.getElementById("segMoney").classList.toggle("active", MODE==="money");
  document.getElementById("segInvoices").classList.toggle("active", MODE==="invoices");
  if(LAST && LAST.ok){
    const agg = buildAgg(LAST.table||[]);
    renderKPIs(LAST, agg);
    renderTops(agg);
  }
}

/* =========================
   Events
========================= */
document.getElementById("btnLogin").addEventListener("click", doLogin);
document.getElementById("aPass").addEventListener("keydown",(e)=>{ if(e.key==="Enter") doLogin(); });

document.getElementById("btnLoad").addEventListener("click", loadDashboard);
document.getElementById("btnRefresh").addEventListener("click", loadDashboard);
document.getElementById("btnExportAll").addEventListener("click", exportAll);
document.getElementById("btnLogout").addEventListener("click", doLogout);

document.getElementById("search").addEventListener("input", ()=>{
  if(LAST && LAST.ok) renderMainTable(LAST);
});

document.getElementById("segMoney").addEventListener("click", ()=> setMode("money"));
document.getElementById("segInvoices").addEventListener("click", ()=> setMode("invoices"));

/* ✅ FIX: Atajos ahora SÍ cargan */
document.getElementById("btnToday").addEventListener("click", async ()=>{
  const t = ymd(new Date());
  setRange(t, t);
  await loadDashboard();
});

document.getElementById("btnThisMonth").addEventListener("click", async ()=>{
  const now = new Date();
  const f = ymd(new Date(now.getFullYear(), now.getMonth(), 1));
  const t = ymd(now);
  setRange(f, t);
  await loadDashboard();
});

document.getElementById("btnThisYear").addEventListener("click", async ()=>{
  const now = new Date();
  const f = ymd(new Date(now.getFullYear(), 0, 1));
  const t = ymd(now);
  setRange(f, t);
  await loadDashboard();
});

/* ✅ FIX: cambiar fecha manual dispara carga (debounce) */
const debouncedLoad = (() => {
  let timer = null;
  return () => {
    clearTimeout(timer);
    timer = setTimeout(() => loadDashboard(), 350);
  };
})();
function maybeAutoLoad(){
  const from = document.getElementById("from").value || "";
  const to = document.getElementById("to").value || "";

  // ✅ Solo auto-cargar si ya están ambas fechas
  if(from && to){
    document.getElementById("rangePill").textContent = `${from} → ${to}`;
    debouncedLoad();
  } else {
    // solo actualiza pill para que el usuario vea el progreso
    document.getElementById("rangePill").textContent = from ? `${from} → —` : "—";
  }
}

document.getElementById("from").addEventListener("change", maybeAutoLoad);
document.getElementById("to").addEventListener("change", maybeAutoLoad);

/* =========================
   INIT (a prueba de balas)
   - SIEMPRE muestra login al inicio
========================= */
window.addEventListener("load", async ()=>{
  // 1) API health
  try{
    const r = await apiHealth();
    const p = document.getElementById("apiPill");
    p.className = "pill " + (r.ok ? "ok" : "bad");
    p.textContent = r.ok ? "API: OK ✅" : "API: ERROR";
  }catch{
    const p = document.getElementById("apiPill");
    p.className = "pill bad";
    p.textContent = "API: ERROR";
  }

  // 2) fechas default: este mes (solo set, no carga)
  const now = new Date();
  setRange(ymd(new Date(now.getFullYear(), now.getMonth(), 1)), ymd(now));

  // 3) SIEMPRE pedir login
  setAuthUI(false);
  openLogin();
});
</script>
</body>
</html>
