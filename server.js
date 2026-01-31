(async () => {
  const API = "https://prodima-pay.onrender.com";
  const token = localStorage.getItem("prodima_admin_token");

  const from = "2026-01-01";
  const to   = "2026-01-31";

  let skip = 0;
  const limit = 200;

  while (true) {
    const url = `${API}/api/admin/quotes?from=${from}&to=${to}&limit=${limit}&skip=${skip}&withDelivered=1`;
    const r = await fetch(url, { headers: { Authorization: "Bearer " + token }});
    const d = await r.json();

    const quotes = Array.isArray(d?.quotes) ? d.quotes : [];
    if (!quotes.length) {
      console.error("❌ No encontré ninguna con entregado > 0 en ese rango.");
      return;
    }

    const firstWith = quotes.find(q => Number(q?.montoEntregado || 0) > 0);
    if (firstWith) {
      console.log("✅ PRIMERA CON ENTREGADO:", firstWith);
      return;
    }

    skip += quotes.length;
  }
})();
