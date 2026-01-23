<!doctype html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=1024">
  <link rel="icon" href="/assets/favicon.png" type="image/png">
  <link rel="shortcut icon" href="/assets/favicon.png" type="image/png">
  <title>Prodima</title>
  <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;600;700;800;900&display=swap" rel="stylesheet">

  <style>
    /* ===== PALETA SANSÁE (rojo + amarillo) ===== */
    :root{
      --brand:#c31b1c;
      --brand-dark:#8f1214;
      --accent:#ffbf24;
      --accent-dark:#f29a00;
      --ink:#222;
      --muted:#666;
      --bd:#e6e8ec;
    }

    *{box-sizing:border-box;margin:0;padding:0}

    body{
      font-family:'Montserrat',system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
      color:var(--ink);
      background:#fff;
    }

    .top-bar{
      background:linear-gradient(90deg,var(--brand) 0%, #e0341d 45%, var(--accent) 100%);
      color:#fff;
      text-align:center;
      padding:10px;
      font-weight:900;
      letter-spacing:.3px;
    }

    header{
      background:var(--brand);
      color:#fff;
      position:sticky;
      top:0;
      z-index:60;
      box-shadow:0 3px 10px rgba(0,0,0,.1);
    }

    .nav{
      max-width:1200px;
      margin:0 auto;
      display:grid;
      grid-template-columns:auto 1fr auto;
      align-items:center;
      gap:16px;
      padding:2px 16px;
    }

    .brand-tile{
      display:inline-flex;
      align-items:center;
      justify-content:center;
      background:#fff;
      border-radius:8px;
      padding:4px;
      box-shadow:0 1px 0 rgba(0,0,0,.05);
    }

    .brand-tile img{
      height:60px;
      width:55px;
    }

    .mainnav{justify-self:center}

    .mainnav>ul{
      list-style:none;
      display:flex;
      gap:28px;
      align-items:center
    }

    .mainnav a,
    .mainnav button{
      color:#fff;
      font-weight:800;
      text-decoration:none;
      background:none;
      border:0;
      cursor:pointer;
      font-family:inherit;
    }

    .mainnav a{
      position:relative;
      padding:6px 2px;
    }

    .mainnav a::after{
      content:"";
      position:absolute;
      left:0;
      right:0;
      bottom:-6px;
      height:2px;
      background:#fff;
      transform:scaleX(0);
      transform-origin:center;
      transition:transform .22s ease;
      opacity:.95;
    }

    .mainnav a:hover::after{transform:scaleX(1)}

    .has-mega{position:relative}

    .mega__toggle{
      display:flex;
      align-items:center;
      gap:8px;
      padding:6px 10px;
      border-radius:8px;
    }

    .mega__toggle svg{transition:transform .2s ease}

    .has-mega.open .mega__toggle svg{transform:rotate(180deg)}

    .mega__panel{
      position:absolute;
      left:50%;
      transform:translateX(-50%);
      top:calc(100% + 12px);
      background:#fff;
      color:#111;
      border-radius:14px;
      border:1px solid #e6e8ec;
      box-shadow:0 16px 40px rgba(0,0,0,.18);
      display:none;
      min-width:680px;
    }

    .has-mega.open .mega__panel{display:block}

    .mega__cols{
      display:grid;
      grid-template-columns:280px 1fr;
      min-height:240px
    }

    .mega__left{
      background:linear-gradient(180deg,#fff6e2 0%, #ffe3b3 100%);
      border-right:1px solid #f3c776;
      padding:8px;
      display:flex;
      flex-direction:column;
      gap:6px;
    }

    .mega__tab{
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:8px;
      padding:14px;
      border-radius:10px;
      border:1px solid transparent;
      font-weight:800;
      cursor:pointer;
      background:transparent;
    }

    .mainnav .mega__tab{
      color:#a02b00 !important;
      font-weight:800;
    }

    .mainnav .mega__tab:hover{
      background:rgba(255,191,36,.16);
    }

    .mainnav .mega__tab.is-active{
      background:linear-gradient(90deg,var(--brand) 0%, var(--accent) 100%) !important;
      color:#fff !important;
    }

    .mega__right{padding:18px 22px}

    .mega__submenu{
      list-style:none;
      margin:0;
      padding:0;
      min-width:260px
    }

    .mega__submenu li+li{margin-top:14px}

    .mega__submenu a{
      color:#111;
      font-weight:700;
      text-decoration:none;
    }

    .mega__submenu a:hover{
      color:var(--brand);
      text-decoration:underline;
    }

    .chev{opacity:.35}

    .burger{
      display:none;
      flex-direction:column;
      gap:5px;
      cursor:pointer
    }

    .burger span{
      width:26px;
      height:3px;
      background:#fff;
      border-radius:3px
    }

    @media (max-width:960px){
      .nav{grid-template-columns:auto auto 1fr}
      .burger{display:flex;justify-self:end}

      .mainnav{
        position:fixed;
        inset:80px 0 auto 0;
        background:var(--brand);
        border-top:1px solid rgba(255,255,255,.18);
        transform:translateY(-10px);
        opacity:0;
        pointer-events:none;
        transition:.18s ease;
      }

      .mainnav.open{
        transform:translateY(0);
        opacity:1;
        pointer-events:auto
      }

      .mainnav>ul{
        flex-direction:column;
        gap:12px;
        padding:16px
      }

      .mega__panel{
        position:static;
        transform:none;
        min-width:0;
        border-radius:10px;
        border:1px solid rgba(255,255,255,.2);
        background:rgba(0,0,0,.12);
        color:#fff;
      }

      .mega__cols{grid-template-columns:1fr}
      .mega__left{background:transparent;border:0}
      .mega__tab{background:rgba(255,255,255,.12);color:#fff}
      .mega__tab.is-active{background:#fff;color:var(--brand)}
      .mega__right{padding:10px 6px 16px 6px}
      .mega__submenu a{color:#fff}
    }

    .hero-food{
      --overlay: linear-gradient(
        180deg,
        rgba(195,21,28,.80) 0%,
        rgba(242,154,0,.55) 40%,
        rgba(255,255,255,0) 100%
      );
      background:
        var(--overlay),
        url("assets/comestible/hero.jpg") center/cover no-repeat,
        linear-gradient(120deg,#fff8e6 0%, #ffe9b3 40%, #ffffff 100%);
      padding:clamp(42px, 8vw, 80px) 16px 28px;
      border-bottom:1px solid #f6d38c;
    }

    .hero-food__inner{
      max-width:1200px;
      margin:0 auto;
      text-align:center;
      color:#2a1220
    }

    .hero-food h1{
      color:var(--brand);
      font-weight:900;
      font-size:clamp(28px, 5vw, 44px);
    }

    .hero-food p{
      max-width:820px;
      margin:10px auto 16px;
      color:#5b3a2a;
      font-size:clamp(14px, 1.9vw, 18px);
    }

    .btn-food{
      display:inline-block;
      background:linear-gradient(90deg,var(--brand) 0%, var(--accent) 100%);
      color:#fff;
      font-weight:900;
      letter-spacing:.3px;
      padding:12px 22px;
      border-radius:14px;
      text-decoration:none;
      box-shadow:0 10px 22px rgba(195,21,28,.35);
    }

    .btn-food:hover{filter:brightness(1.05)}

    .section{
      max-width:1200px;
      margin:26px auto 40px;
      padding:0 16px
    }

    .controls{
      display:flex;
      gap:10px;
      flex-wrap:wrap;
      align-items:center;
      margin:14px 0
    }

    .chips{display:flex;gap:10px;flex-wrap:wrap}

    .chip{
      padding:.55rem .95rem;
      border-radius:999px;
      cursor:pointer;
      user-select:none;
      background:linear-gradient(180deg,#fff8e3 0%, #ffe6b0 100%);
      border:1px solid #ffd27f;
      color:#9b2a00;
      font-weight:800;
      transition:.18s ease;
    }

    .chip:hover{
      transform:translateY(-1px);
      box-shadow:0 8px 20px rgba(242,154,0,.25);
    }

    .chip.active{
      background:linear-gradient(180deg,var(--brand) 0%, #ff7a00 55%, var(--accent) 100%);
      border-color:var(--accent-dark);
      color:#fff;
      box-shadow:0 10px 24px rgba(195,21,28,.35);
    }

    .search{
      flex:1 1 260px;
      min-width:220px;
      height:40px;
      border-radius:999px;
      border:1px solid #ffd27f;
      padding:0 14px;
      outline:0;
      font-weight:700;
      color:#3a2a33;
      background:#fffdf6;
    }

    .search::placeholder{color:#bf8b47;font-weight:600}

    .grid.cards{
      display:grid;
      grid-template-columns:repeat(4,1fr);
      gap:20px
    }

    @media (max-width:1000px){
      .grid.cards{grid-template-columns:repeat(2,1fr)}
    }

    @media (max-width:560px){
      .grid.cards{grid-template-columns:1fr}
    }

    .card{
      border:1px solid #f3d6a5;
      border-radius:18px;
      overflow:hidden;
      background:#fff;
      transition:.15s ease;
      box-shadow:0 10px 24px rgba(0,0,0,.06);
    }

    .card:hover{
      transform:translateY(-4px);
      box-shadow:0 22px 50px rgba(195,21,28,.22);
      border-color:#f0b858;
    }

    .card--food .thumb{
      height:190px;
      position:relative;
      overflow:hidden
    }

    .card--food .thumb img{
      width:100%;
      height:100%;
      object-fit:cover;
      display:block
    }

    .card--food .tag-cat{
      position:absolute;
      left:12px;
      top:12px;
      padding:6px 10px;
      border-radius:999px;
      background:rgba(0,0,0,.55);
      color:#fff;
      font-size:12px;
      font-weight:800;
      letter-spacing:.3px;
      backdrop-filter:blur(3px);
    }

    .card--food .price-badge{
      position:absolute;
      right:12px;
      top:12px;
      background:var(--accent);
      color:#7b1a01;
      font-weight:900;
      padding:6px 10px;
      border-radius:999px;
      box-shadow:0 8px 20px rgba(0,0,0,.25);
      border:1px solid var(--accent-dark);
    }

    .stock-badge{
      position:absolute;
      right:12px;
      bottom:12px;
      padding:6px 10px;
      border-radius:999px;
      font-weight:900;
      font-size:12px;
      color:#fff;
      background:#999;
      box-shadow:0 8px 20px rgba(0,0,0,.25);
    }

    .stock-ok{ background:#0c8c6a; }
    .stock-no{ background:#c31b1c; }

    .card--food .body{padding:14px}
    .card--food .sku{color:#8a6678;font-size:12px;margin-bottom:6px}
    .card--food h3{margin:.2rem 0 .4rem;font-size:18px}
    .card--food .cat{color:var(--brand);font-weight:800;margin-bottom:6px}
    .card--food p{color:#5f4a56;min-height:44px}

    .card--food .row{
      display:flex;
      justify-content:space-between;
      align-items:center;
      gap:10px;
      margin-top:10px
    }

    .add-btn{
      display:inline-flex;
      align-items:center;
      gap:8px;
      color:var(--brand);
      font-weight:900;
      text-decoration:none;
      padding:8px 12px;
      border-radius:12px;
      border:1px solid #ffd27f;
      background:linear-gradient(180deg,#fff8e3 0%, #ffe6b0 100%);
      cursor:pointer;
    }

    .add-btn:hover{
      background:linear-gradient(180deg,var(--accent) 0%, #ffe08a 100%);
      border-color:var(--accent-dark);
    }

    .add-btn:disabled{
      opacity:.6;
      cursor:not-allowed;
    }

    .fab-cart{
      position:fixed;
      right:18px;
      bottom:18px;
      background:linear-gradient(90deg,var(--brand) 0%, var(--accent) 100%);
      color:#fff;
      border:0;
      border-radius:999px;
      padding:12px 16px;
      font-weight:900;
      box-shadow:0 10px 22px rgba(195,21,28,.45);
      display:flex;
      align-items:center;
      gap:10px;
      z-index:90;
    }

    .fab-cart small{
      background:#fff;
      color:var(--brand);
      border-radius:999px;
      padding:4px 8px;
      font-weight:900;
    }

    .cart-panel{
      position:fixed;
      right:18px;
      bottom:72px;
      background:#fffef7;
      border:1px solid #f3d6a5;
      border-radius:16px;
      box-shadow:0 16px 40px rgba(0,0,0,.18);
      width:320px;
      max-height:60vh;
      overflow:auto;
      display:none;
      z-index:91;
    }

    .cart-panel header{
      display:flex;
      justify-content:space-between;
      align-items:center;
      padding:10px 12px;
      border-bottom:1px solid #f3d6a5;
      font-weight:800;
    }

    .cart-items{padding:10px 12px}

    .cart-item{
      display:grid;
      grid-template-columns:56px 1fr auto;
      gap:10px;
      align-items:center;
      padding:8px 0;
      border-bottom:1px dashed #f3d6a5;
    }

    .cart-item img{
      width:56px;
      height:56px;
      object-fit:cover;
      border-radius:8px;
    }

    .cart-actions{
      padding:10px 12px;
      border-top:1px solid #f3d6a5;
      display:flex;
      gap:10px;
    }

    .btn{
      display:inline-flex;
      justify-content:center;
      align-items:center;
      gap:6px;
      padding:10px 14px;
      border-radius:12px;
      font-weight:900;
      text-decoration:none;
      border:1px solid #eee;
    }

    .btn-outline{
      background:#fff;
      color:var(--brand);
      border-color:#ffd27f;
    }

    .btn-primary{
      background:linear-gradient(90deg,var(--brand) 0%, var(--accent) 100%);
      color:#fff;
      border-color:var(--brand);
    }

    .copy{
      text-align:center;
      padding:18px;
      color:#7a4a1a;
      border-top:1px solid #f3d6a5;
      margin-top:28px;
    }
  </style>
</head>

<body>
  <div class="top-bar">OFERTA ESPECIAL · HASTA 30 % DE DESCUENTO</div>

  <header class="header-prodima">
    <div class="nav">
      <a class="brand-tile" href="index.html" aria-label="Inicio">
        <img src="assets/Prodima Logo.PNG" alt="Prodima">
      </a>

      <nav id="mainnav" class="mainnav" aria-label="Principal">
        <ul>
          <li class="has-mega">
            <button class="mega__toggle" style="font-size:16px;font-weight:900;letter-spacing:.3px" aria-expanded="false" aria-controls="megaProductos">
              Nuestros productos
              <svg width="25" height="25" viewBox="0 0 24 24" aria-hidden="true">
                <path fill="currentColor" d="M7 10l5 5 5-5z"/>
              </svg>
            </button>

            <div id="megaProductos" class="mega__panel" role="menu" aria-label="Nuestros productos">
              <div class="mega__cols">
                <aside class="mega__left" role="tablist" aria-orientation="vertical">
                  <button class="mega__tab is-active" role="tab" aria-selected="true" aria-controls="pane-hogar" data-target="pane-hogar">
                    Hogar <span class="chev">›</span>
                  </button>
                  <button class="mega__tab" role="tab" aria-selected="false" aria-controls="pane-agua" data-target="pane-agua">
                    Agua <span class="chev">›</span>
                  </button>
                </aside>

                <section class="mega__right">
                  <ul id="pane-hogar" class="mega__submenu is-active" role="tabpanel">
                    <li><a href="comestible.html">Comestible</a></li>
                    <li><a href="No Comestible.html#hogar-no-comestible">No Comestible</a></li>
                  </ul>

                  <ul id="pane-agua" class="mega__submenu" role="tabpanel" hidden>
                    <li><a href="tratamiento de agua.html#agua-tratamiento">Tratamiento de Agua</a></li>
                    <li><a href="piscina.html#agua-piscina">Piscina</a></li>
                  </ul>
                </section>
              </div>
            </div>
          </li>

          <li><a href="mision.html">Quienes somos</a></li>
          <li><a href="atencion.html">Atención al cliente</a></li>
          <a href="unete.html">Trabaja con nosotros</a>
          <li><a href="assets/catalogo.pdf" target="_blank">Catálogo</a></li>
        </ul>
      </nav>

      <button class="burger" id="burger" aria-label="Abrir menú">
        <span></span><span></span><span></span>
      </button>
    </div>
  </header>

  <section class="hero-food">
    <div class="hero-food__inner">
      <h1>Linea de aderezos para la comida</h1>
      <p>Sazonadores, vinagres y productos <strong>gourmet</strong> para darle vida a tus recetas.</p>
      <a class="btn-food" href="#grid">Conoce nuestra variedad de productos</a>
    </div>
  </section>

  <section class="section">
    <div class="controls">
      <div class="chips"></div>
      <input id="srch" class="search" placeholder="Buscar productos...">
    </div>

    <div id="grid" class="grid cards" style="margin-top:12px"></div>
  </section>

  <button id="fabCart" class="fab-cart" type="button">
    🛒 Carrito <small id="cartCount">0</small>
  </button>

  <div id="cartPanel" class="cart-panel" role="dialog" aria-label="Carrito">
    <header>
      <span>Tu carrito</span>
      <button id="closeCart" style="background:none;border:0;font-weight:900;cursor:pointer">✕</button>
    </header>

    <div class="cart-items" id="cartItems"></div>

    <div class="cart-actions">
      <a class="btn btn-outline" href="checkout.html">Ver carrito</a>
      <a class="btn btn-primary" href="checkout.html?step=pay">Finalizar compra</a>
    </div>
  </div>

  <div class="copy">© 2025 PRODIMA — Línea Comestible</div>

  <script>
    /* =====================================================
       ✅ CONFIG API (Render)
    ===================================================== */
    const API_BASE = "https://prodima-pay.onrender.com"; // <- Render

    /* =====================================================
       ✅ CATÁLOGO LOCAL (SOLO itemCode + imagen + categoria)
       TODO lo demás lo trae SAP
    ===================================================== */
    const PAGE_KIND = "Comestible";
    const PRODIMA_DATA = {
      "Comestible": [
        { itemCode:"0110", categoria:"Sazonadores", descripcion:"China Low Sodium original Sansae.", img:"assets/imagenes/Salsa china.PNG" },
        { itemCode:"0105", categoria:"Sazonadores", descripcion:"China Soy Sauce original Sansae.", img:"assets/imagenes/Salsa china soy sauce.PNG" },
        { itemCode:"0124", categoria:"Sazonadores", descripcion:"Salsa Inglesa.", img:"assets/imagenes/Salsa inglesa.PNG" },
        { itemCode:"0205", categoria:"Sazonadores", descripcion:"Salsa Inglesa premium.", img:"assets/imagenes/Salsa condimentada.PNG" },
        { itemCode:"0405", categoria:"Sazonadores", descripcion:"Recao Criollo premium.", img:"assets/imagenes/Salsa recao.PNG" },
        { itemCode:"0645", categoria:"Vinagres", descripcion:"Vinagre manzana de mesa.", img:"assets/imagenes/vinagres.PNG" },
        { itemCode:"0650", categoria:"Vinagres", descripcion:"Vinagre blanco filtrado.", img:"assets/imagenes/vinagre blanco.PNG" },
        { itemCode:"0605", categoria:"Gourmets", descripcion:"Salsa gourmet premium mango coco.", img:"assets/imagenes/Salsa mango coco.PNG" },
        { itemCode:"0610", categoria:"Gourmets", descripcion:"Salsa gourmet premium ajo.", img:"assets/imagenes/Salsa de ajo.PNG" },
        { itemCode:"0304", categoria:"Gourmets", descripcion:"Salsa gourmet premium mora.", img:"assets/imagenes/Salsa mora.PNG" },
        { itemCode:"1110", categoria:"Gourmets", descripcion:"Salsa gourmet premium BBQ Chipotle.", img:"assets/imagenes/BBQ.PNG" },
        { itemCode:"1205", categoria:"Gourmets", descripcion:"Picante habanero premium.", img:"assets/imagenes/Picante habanero.PNG" },
        { itemCode:"0307", categoria:"Gourmets", descripcion:"Picante caribeño premium.", img:"assets/imagenes/Picante caribeño.PNG" }
      ]
    };

    /* =====================================================
       ✅ SAP CACHE (ItemCode -> datos SAP)
    ===================================================== */
    const sapCache = {};
    // sapCache["0110"] = {name, price, available, hasStock, unit}

    /* traer item desde SAP (Render) */
    async function fetchSapItem(itemCode){
      const url = `${API_BASE}/api/sap/item/${encodeURIComponent(itemCode)}`;
      const res = await fetch(url);
      const data = await res.json();
      if(!res.ok || !data.ok) throw new Error(data?.message || "Error SAP");
      return data;
    }

    /* =====================================================
       ✅ CARRITO
    ===================================================== */
    const LS_KEY = "prodima_cart_v1";
    const getCart = () => JSON.parse(localStorage.getItem(LS_KEY) || "[]");
    const saveCart = (c) => localStorage.setItem(LS_KEY, JSON.stringify(c));

    const addToCart = (item) => {
      const cart = getCart();
      const idx = cart.findIndex(x => x.sku === item.sku);
      if (idx >= 0) cart[idx].qty += 1;
      else cart.push({...item, qty:1});
      saveCart(cart);
      renderCartBadge();
      renderCartPanel();
    };

    const removeFromCart = (sku) => {
      saveCart(getCart().filter(x => x.sku!==sku));
      renderCartBadge();
      renderCartPanel();
    };

    const renderCartBadge = () => {
      const count = getCart().reduce((a,b)=>a+b.qty,0);
      document.getElementById('cartCount').textContent = count;
    };

    /* =====================================================
       ✅ RENDER DE PRODUCTOS
    ===================================================== */
    (function(){
      const grid = document.getElementById('grid');
      const chipsBox = document.querySelector('.chips');
      const search = document.getElementById('srch');
      const data = PRODIMA_DATA[PAGE_KIND] || [];
      let currentCat = 'Todas';

      function money(n){
        if(n == null || Number.isNaN(Number(n))) return "--";
        return `$ ${Number(n).toFixed(2)}`;
      }

      function render(){
        const q = (search?.value || '').toLowerCase().trim();
        const filtered = data.filter(p =>
          (currentCat==='Todas' || p.categoria===currentCat) &&
          ((p.itemCode+' '+(sapCache[p.itemCode]?.name||'')+' '+p.descripcion).toLowerCase().includes(q))
        );

        grid.innerHTML = filtered.map(p => {
          const sap = sapCache[p.itemCode]; // puede ser undefined
          const title = sap?.name || `Producto ${p.itemCode}`;
          const price = sap?.price;
          const available = sap?.available;
          const hasStock = sap?.hasStock;

          return `
            <article class="card card--food" aria-label="${title}">
              <div class="thumb">
                <img src="${p.img}" alt="${title}">
                <span class="tag-cat">${p.categoria}</span>
                <span class="price-badge" id="price-${p.itemCode}">${money(price)}</span>
                <span class="stock-badge ${hasStock===true?'stock-ok':(hasStock===false?'stock-no':'')}" id="stock-${p.itemCode}">
                  ${hasStock===true ? `Stock: ${available}` : (hasStock===false ? `Sin stock` : `Cargando...`)}
                </span>
              </div>

              <div class="body">
                <div class="sku">Código: ${p.itemCode}</div>
                <h3 id="name-${p.itemCode}">${title}</h3>
                <div class="cat">${p.categoria}</div>
                <p>${p.descripcion}</p>

                <div class="row">
                  <button class="add-btn" type="button" data-code="${p.itemCode}" ${hasStock===false ? "disabled" : ""}>
                    Agregar →
                  </button>
                </div>
              </div>
            </article>
          `;
        }).join('');

        // Botones Agregar
        grid.querySelectorAll('.add-btn').forEach(btn=>{
          btn.addEventListener('click', ()=>{
            const code = btn.dataset.code;
            const sap = sapCache[code];

            if(!sap){
              alert("Todavía estamos cargando este producto. Intenta de nuevo.");
              return;
            }

            if(!sap.hasStock){
              alert("Este producto no tiene stock disponible.");
              return;
            }

            addToCart({
              sku: code,
              name: sap.name,
              price: sap.price ?? 0,
              img: data.find(x=>x.itemCode===code)?.img || ""
            });
          });
        });
      }

      // Chips
      const cats = [...new Set(data.map(x=>x.categoria))];
      chipsBox.innerHTML = ['Todas', ...cats]
        .map(c => `<span class="chip ${c==='Todas'?'active':''}" data-cat="${c}">${c}</span>`)
        .join('');

      chipsBox.querySelectorAll('.chip').forEach(ch => ch.addEventListener('click', () => {
        chipsBox.querySelectorAll('.chip').forEach(x=>x.classList.remove('active'));
        ch.classList.add('active');
        currentCat = ch.getAttribute('data-cat');
        render();
      }));

      search?.addEventListener('input', render);

      // ✅ Primero renderiza “cargando…”
      render();
      renderCartBadge();
      renderCartPanel();

      // ✅ Luego carga SAP en paralelo
      (async ()=>{
        await Promise.allSettled(
          data.map(async (p)=>{
            try{
              const r = await fetchSapItem(p.itemCode);
              sapCache[p.itemCode] = {
                name: r?.item?.ItemName || `Producto ${p.itemCode}`,
                unit: r?.item?.SalesUnit || "",
                price: (r?.price != null) ? Number(r.price) : null,
                available: (r?.stock?.available != null) ? Number(r.stock.available) : null,
                hasStock: (r?.stock?.hasStock === true)
              };
            }catch(e){
              // si falla SAP, dejamos algo “seguro”
              sapCache[p.itemCode] = {
                name: `Producto ${p.itemCode}`,
                unit: "",
                price: null,
                available: null,
                hasStock: null
              };
            }
          })
        );

        // ✅ Re-render ya con precios y stock reales
        render();
      })();
    })();

    /* =====================================================
       ✅ MINI CARRITO
    ===================================================== */
    const panel = document.getElementById('cartPanel');

    document.getElementById('fabCart').onclick = () =>
      panel.style.display = panel.style.display==='block' ? 'none' : 'block';

    document.getElementById('closeCart').onclick = () =>
      panel.style.display = 'none';

    function renderCartPanel(){
      const items = getCart();
      const box = document.getElementById('cartItems');

      if(!items.length){
        box.innerHTML = '<p style="padding:6px 0;color:#666">Tu carrito está vacío.</p>';
        return;
      }

      box.innerHTML = items.map(it=> `
        <div class="cart-item">
          <img src="${it.img}" alt="${it.name}">
          <div>
            <div style="font-weight:800">${it.name}</div>
            <div style="font-size:12px;color:#666">
              ${it.sku} · Cant: ${it.qty}
            </div>
            <div style="font-size:12px;color:#666">
              Precio: ${it.price != null ? "$ " + Number(it.price).toFixed(2) : "--"}
            </div>
          </div>
          <div style="text-align:right">
            <button style="background:none;border:0;color:#0c8c6a;cursor:pointer" onclick="removeFromCart('${it.sku}')">
              Eliminar
            </button>
          </div>
        </div>
      `).join('');
    }

    /* --------- Burger y Mega --------- */
    (function () {
      const burger = document.getElementById('burger');
      const mainnav = document.getElementById('mainnav');
      if (!burger || !mainnav) return;

      burger.addEventListener('click', () => {
        const open = mainnav.classList.toggle('open');
        document.body.style.overflow = open ? 'hidden' : '';
      });
    })();

    (function () {
      const hasMega = document.querySelector('.has-mega');
      if (!hasMega) return;

      const toggle = hasMega.querySelector('.mega__toggle');
      const tabs = hasMega.querySelectorAll('.mega__tab');
      const panes = hasMega.querySelectorAll('.mega__submenu');

      const setOpen = (o)=>{
        hasMega.classList.toggle('open',o);
        toggle.setAttribute('aria-expanded',o?'true':'false');
      };

      toggle.addEventListener('click', (e)=>{
        e.stopPropagation();
        setOpen(!hasMega.classList.contains('open'));
      });

      document.addEventListener('click',(e)=>{
        if(!hasMega.contains(e.target)) setOpen(false);
      });

      document.addEventListener('keydown',(e)=>{
        if(e.key==='Escape') setOpen(false);
      });

      const activate = (btn)=>{
        const id=btn.dataset.target;
        tabs.forEach(t=>t.classList.toggle('is-active',t===btn));
        panes.forEach(p=>{
          const s=p.id===id;
          p.hidden=!s;
          p.classList.toggle('is-active',s)
        });
      };

      tabs.forEach(b=>{
        b.addEventListener('mouseenter',()=>activate(b));
        b.addEventListener('click',()=>activate(b));
      });

      activate(hasMega.querySelector('.mega__tab.is-active')||tabs[0]);
    })();
  </script>
</body>
</html>
