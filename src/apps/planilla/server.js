// src/apps/planilla/server.js
import express from "express";
import cors from "cors";
import multer from "multer";
import * as XLSX from "xlsx";
import PDFDocument from "pdfkit";
import { createClient } from "@supabase/supabase-js";
import { Resend } from "resend";
import path from "path";
import { fileURLToPath } from "url";

/* =========================================================
   PATHS
========================================================= */
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/* =========================================================
   ENV
========================================================= */
const {
  PORT = 3000,
  CORS_ORIGIN = "",

  SUPABASE_URL = "",
  SUPABASE_SERVICE_ROLE = "",

  RESEND_API_KEY = "",
  MAIL_FROM = "Planilla <nomina@localhost>",

  VOUCHER_BUCKET = "payroll-vouchers",
} = process.env;

/* =========================================================
   APP
========================================================= */
const app = express();
app.use(express.json({ limit: "5mb" }));

app.use(
  cors({
    origin: CORS_ORIGIN ? CORS_ORIGIN.split(",").map((s) => s.trim()) : true,
    credentials: true,
  })
);

// Servir frontend mínimo
app.use("/", express.static(path.join(__dirname, "public")));

/* =========================================================
   Supabase + Email
========================================================= */
const sb =
  SUPABASE_URL && SUPABASE_SERVICE_ROLE
    ? createClient(SUPABASE_URL.trim(), SUPABASE_SERVICE_ROLE.trim(), {
        auth: { persistSession: false },
      })
    : null;

const resend = RESEND_API_KEY ? new Resend(RESEND_API_KEY) : null;

function requireSupabase(req, res, next) {
  if (!sb) return res.status(500).json({ ok: false, message: "Supabase env missing" });
  next();
}

/* =========================================================
   HEALTH
========================================================= */
app.get("/api/health", (req, res) => {
  res.json({
    ok: true,
    supabase: !!sb,
    mail: !!resend,
    bucket: VOUCHER_BUCKET,
  });
});

/* =========================================================
   EMPLOYEES (mínimo)
========================================================= */
app.get("/api/employees", requireSupabase, async (req, res) => {
  const { data, error } = await sb.from("employees").select("*").order("full_name", { ascending: true });
  if (error) return res.status(500).json({ ok: false, message: error.message });
  res.json({ ok: true, employees: data });
});

app.post("/api/employees", requireSupabase, async (req, res) => {
  const p = req.body || {};
  if (!p.emp_code || !p.full_name) {
    return res.status(400).json({ ok: false, message: "emp_code y full_name requeridos" });
  }

  const payload = {
    emp_code: String(p.emp_code).trim(),
    full_name: String(p.full_name).trim(),
    email: p.email ? String(p.email).trim() : null,
    employee_type: p.employee_type === "NO_CLOCK" ? "NO_CLOCK" : "CLOCKS_IN",
    salary_type: (p.salary_type || "MONTHLY").toUpperCase(),
    base_salary: Number(p.base_salary || 0),
    hourly_rate: Number(p.hourly_rate || 0),
    department: p.department || null,
    is_active: p.is_active !== false,
  };

  const { data, error } = await sb.from("employees").insert(payload).select("*").single();
  if (error) return res.status(500).json({ ok: false, message: error.message });
  res.json({ ok: true, employee: data });
});

/* =========================================================
   PAY PERIODS
========================================================= */
app.post("/api/pay-periods", requireSupabase, async (req, res) => {
  const { period_type, start_date, end_date, label } = req.body || {};
  if (!period_type || !start_date || !end_date || !label) {
    return res.status(400).json({ ok: false, message: "period_type, start_date, end_date, label requeridos" });
  }
  const payload = {
    period_type: period_type === "MONTHLY" ? "MONTHLY" : "BIWEEKLY",
    start_date,
    end_date,
    label,
  };
  const { data, error } = await sb.from("pay_periods").insert(payload).select("*").single();
  if (error) return res.status(500).json({ ok: false, message: error.message });
  res.json({ ok: true, period: data });
});

app.get("/api/pay-periods", requireSupabase, async (req, res) => {
  const { data, error } = await sb.from("pay_periods").select("*").order("start_date", { ascending: false });
  if (error) return res.status(500).json({ ok: false, message: error.message });
  res.json({ ok: true, periods: data });
});

/* =========================================================
   IMPORT TIME FILE (TXT / XLSX)
========================================================= */
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 15 * 1024 * 1024 } });

/**
 * TXT esperado (ejemplo):
 * emp_code|2026-03-01 08:01:00|IN
 * emp_code|2026-03-01 17:03:00|OUT
 *
 * XLSX: columnas emp_code, ts, event_type (opcional)
 */
app.post("/api/time/import", requireSupabase, upload.single("file"), async (req, res) => {
  try {
    const file = req.file;
    const { period_start, period_end, uploaded_by = "system" } = req.body || {};
    if (!file) return res.status(400).json({ ok: false, message: "Archivo requerido" });

    const filename = file.originalname || "upload";
    const lower = filename.toLowerCase();
    const isXlsx = lower.endsWith(".xlsx") || lower.endsWith(".xls");
    const source_type = isXlsx ? "XLSX" : "TXT";

    const { data: imp, error: e0 } = await sb
      .from("time_imports")
      .insert({
        uploaded_by,
        source_filename: filename,
        source_type,
        period_start: period_start || null,
        period_end: period_end || null,
      })
      .select("*")
      .single();

    if (e0) return res.status(500).json({ ok: false, message: e0.message });

    const rows = [];

    if (!isXlsx) {
      const txt = file.buffer.toString("utf8");
      const lines = txt.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);

      for (const line of lines) {
        const parts = line.includes("|") ? line.split("|") : line.split(/[\t,]/);
        const emp_code = String(parts[0] || "").trim();
        const tsRaw = String(parts[1] || "").trim();
        const event_type = parts[2] ? String(parts[2]).trim().toUpperCase() : null;

        if (!emp_code || !tsRaw) continue;
        const ts = new Date(tsRaw);
        if (Number.isNaN(ts.getTime())) continue;

        rows.push({
          import_id: imp.id,
          emp_code,
          ts: ts.toISOString(),
          event_type: event_type || null,
          raw_line: line,
        });
      }
    } else {
      const wb = XLSX.read(file.buffer, { type: "buffer" });
      const ws = wb.Sheets[wb.SheetNames[0]];
      const json = XLSX.utils.sheet_to_json(ws, { defval: "" });

      for (const r of json) {
        const emp_code = String(r.emp_code || r.EmpCode || r.codigo || r.code || "").trim();
        const tsRaw = String(r.ts || r.timestamp || r.fecha || r.datetime || "").trim();
        const event_type = r.event_type ? String(r.event_type).trim().toUpperCase() : null;

        if (!emp_code || !tsRaw) continue;
        const ts = new Date(tsRaw);
        if (Number.isNaN(ts.getTime())) continue;

        rows.push({
          import_id: imp.id,
          emp_code,
          ts: ts.toISOString(),
          event_type: event_type || null,
          raw_line: JSON.stringify(r),
        });
      }
    }

    if (!rows.length) return res.json({ ok: true, import: imp, inserted: 0, message: "No se detectaron filas válidas" });

    let inserted = 0;
    const CHUNK = 1000;
    for (let i = 0; i < rows.length; i += CHUNK) {
      const chunk = rows.slice(i, i + CHUNK);
      const { error } = await sb.from("time_raw").insert(chunk);
      if (error) return res.status(500).json({ ok: false, message: error.message });
      inserted += chunk.length;
    }

    res.json({ ok: true, import: imp, inserted });
  } catch (e) {
    res.status(500).json({ ok: false, message: e.message });
  }
});

/* =========================================================
   PAYROLL CALC (MVP)
   - NO_CLOCK: cobra base sin depender de marcaciones
   - CLOCKS_IN: MVP cobra base igual; luego agregas ausencias/tardanzas/extras
========================================================= */
app.post("/api/pay-runs", requireSupabase, async (req, res) => {
  const { period_id, created_by = "system" } = req.body || {};
  if (!period_id) return res.status(400).json({ ok: false, message: "period_id requerido" });

  const { data: period, error: e1 } = await sb.from("pay_periods").select("*").eq("id", period_id).single();
  if (e1) return res.status(500).json({ ok: false, message: e1.message });

  const { data: run, error: e2 } = await sb
    .from("pay_runs")
    .insert({ period_id, status: "DRAFT", created_by })
    .select("*")
    .single();

  if (e2) return res.status(500).json({ ok: false, message: e2.message });

  res.json({ ok: true, run, period });
});

app.post("/api/pay-runs/:id/calculate", requireSupabase, async (req, res) => {
  const payRunId = req.params.id;

  const { data: run, error: e0 } = await sb.from("pay_runs").select("*, pay_periods(*)").eq("id", payRunId).single();
  if (e0) return res.status(500).json({ ok: false, message: e0.message });

  const period = run.pay_periods;

  const { data: emps, error: e1 } = await sb.from("employees").select("*").eq("is_active", true);
  if (e1) return res.status(500).json({ ok: false, message: e1.message });

  // limpiar items/slips anteriores
  await sb.from("pay_items").delete().eq("pay_run_id", payRunId);
  await sb.from("pay_slips").delete().eq("pay_run_id", payRunId);

  const payItems = [];
  const slips = [];

  for (const emp of emps) {
    let base = Number(emp.base_salary || 0);

    // prorrateo simple: si salario mensual y periodo quincenal => /2
    if (String(emp.salary_type || "").toUpperCase() === "MONTHLY" && period.period_type === "BIWEEKLY") {
      base = base / 2;
    }

    base = Number(base.toFixed(2));

    payItems.push({
      pay_run_id: payRunId,
      emp_id: emp.id,
      concept: "BASE_SALARY",
      amount: base,
      meta: { employee_type: emp.employee_type, salary_type: emp.salary_type },
    });

    const gross = base;
    const deductions = 0;
    const net = Number((gross - deductions).toFixed(2));

    slips.push({
      pay_run_id: payRunId,
      emp_id: emp.id,
      gross,
      deductions,
      net,
      pdf_path: null,
    });
  }

  const { error: e2 } = await sb.from("pay_items").insert(payItems);
  if (e2) return res.status(500).json({ ok: false, message: e2.message });

  const { error: e3 } = await sb.from("pay_slips").insert(slips);
  if (e3) return res.status(500).json({ ok: false, message: e3.message });

  const { error: e4 } = await sb.from("pay_runs").update({ status: "CALCULATED" }).eq("id", payRunId);
  if (e4) return res.status(500).json({ ok: false, message: e4.message });

  res.json({ ok: true, payRunId, employees: emps.length });
});

/* =========================================================
   PDF Layout (tipo "Comprobante de pago" como tu ejemplo)
========================================================= */
function money(n) {
  const x = Number(n || 0);
  return x.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}
function line(doc, x1, y1, x2, y2) {
  doc.moveTo(x1, y1).lineTo(x2, y2).stroke();
}
function sectionTitle(doc, x, y, w, title) {
  doc.font("Helvetica-Bold").fontSize(10).text(title, x, y, { width: w, align: "center" });
  line(doc, x, y + 14, x + w, y + 14);
}
function kv(doc, x, y, label, value, gap = 70) {
  doc.font("Helvetica-Bold").fontSize(9).text(label, x, y);
  doc.font("Helvetica").fontSize(9).text(value ?? "", x + gap, y);
}

function buildSlipPdfBuffer(payload) {
  const {
    companyName = "PRODIMA, S.A.",
    page = 1,

    planillaTipo = "PLANILLA QUINCENAL",
    periodFrom = "",
    periodTo = "",
    planillaNo = "--",
    transaccionNo = "--",

    sucursal = "PMA - PANAMA",
    empCode = "",
    empName = "",
    cedula = "",
    cargo = "",
    cuenta = "",
    salarioHora = 0,

    horas = { diurna_h: 0, diurna_$: 0, mixta_h: 0, mixta_$: 0, noct_h: 0, noct_$: 0, ajust_h: 0, ajust_$: 0 },
    ausencias = { no_paga_h: 0, no_paga_$: 0, pagadas_h: 0, pagadas_$: 0, cmed_h: 0, cmed_$: 0 },

    descuentosLegales = [],
    ingresos = [],

    salarioBruto = 0,
    totalOtros = 0,
    otrosDescuentos = 0,
  } = payload || {};

  const totalDescLeg = (descuentosLegales || []).reduce((a, b) => a + Number(b.amount || 0), 0);
  const salarioNeto = Number(salarioBruto || 0) - totalDescLeg - Number(otrosDescuentos || 0);

  return new Promise((resolve) => {
    const doc = new PDFDocument({ size: "A4", margin: 36 });
    const chunks = [];
    doc.on("data", (c) => chunks.push(c));
    doc.on("end", () => resolve(Buffer.concat(chunks)));

    doc.font("Helvetica").fontSize(9);
    doc.lineWidth(1);

    const pageW = doc.page.width;
    const left = doc.page.margins.left;
    const right = pageW - doc.page.margins.right;
    let y = 28;

    // Header
    doc.font("Helvetica-Bold").fontSize(14).text("COMPROBANTE DE PAGO", left, y, { width: right - left, align: "center" });
    y += 16;
    doc.font("Helvetica-Bold").fontSize(10).text(companyName, left, y, { width: right - left, align: "center" });
    doc.font("Helvetica").fontSize(9).text(`Página: ${page}`, right - 80, 34, { width: 80, align: "right" });

    y += 16;
    line(doc, left, y, right, y);
    y += 8;

    doc
      .font("Helvetica-Bold")
      .fontSize(9)
      .text(`${planillaTipo}  DEL  ${periodFrom}  A  ${periodTo}`, left, y);

    doc.font("Helvetica-Bold").fontSize(9).text(`PLANILLA #:  ${planillaNo}`, right - 210, y, { width: 120, align: "left" });
    doc.font("Helvetica-Bold").fontSize(9).text(`TRANSACCIÓN #:  ${transaccionNo}`, right - 90, y, { width: 90, align: "right" });

    y += 14;
    line(doc, left, y, right, y);
    y += 12;

    // 3 columns
    const colGap = 14;
    const colW = Math.floor((right - left - colGap * 2) / 3);
    const c1 = left;
    const c2 = left + colW + colGap;
    const c3 = left + (colW + colGap) * 2;

    // Left: employee
    kv(doc, c1, y, "SUCURSAL:", sucursal, 62);
    y += 12;
    kv(doc, c1, y, "Código:", empCode, 62);
    doc.font("Helvetica-Bold").text(empName, c1 + 120, y, { width: colW - 120 });
    y += 12;
    kv(doc, c1, y, "Cédula:", cedula, 62);
    y += 12;
    kv(doc, c1, y, "Cargo:", cargo, 62);
    y += 16;
    kv(doc, c1, y, "SALARIO x HORA:", String(salarioHora), 90);

    // Middle: legal deductions
    sectionTitle(doc, c2, y - 28, colW, "DESCUENTOS LEGALES");
    let yMid = y;
    for (const d of descuentosLegales || []) {
      doc.font("Helvetica").fontSize(9).text(d.name, c2, yMid, { width: colW - 60, align: "left" });
      doc.text(money(d.amount), c2, yMid, { width: colW, align: "right" });
      yMid += 12;
    }
    line(doc, c2, yMid + 2, c2 + colW, yMid + 2);
    yMid += 6;
    doc.font("Helvetica-Bold").text(money(totalDescLeg), c2, yMid, { width: colW, align: "right" });

    // Right: incomes + totals
    let yR = y;
    for (const it of ingresos || []) {
      doc.font("Helvetica").fontSize(9).text(it.name, c3, yR, { width: colW - 70, align: "left" });
      doc.text(money(it.amount), c3, yR, { width: colW, align: "right" });
      yR += 12;
    }
    yR += 4;
    line(doc, c3, yR, c3 + colW, yR);
    yR += 8;

    doc.font("Helvetica-Bold").text("Salario Bruto", c3, yR, { width: colW - 70 });
    doc.text(money(salarioBruto), c3, yR, { width: colW, align: "right" });
    yR += 12;

    doc.font("Helvetica-Bold").text("Total Otros", c3, yR, { width: colW - 70 });
    doc.text(money(totalOtros), c3, yR, { width: colW, align: "right" });
    yR += 12;

    doc.font("Helvetica-Bold").text("Descuentos Legales", c3, yR, { width: colW - 70 });
    doc.text(money(totalDescLeg), c3, yR, { width: colW, align: "right" });
    yR += 12;

    doc.font("Helvetica-Bold").text("Otros Descuentos", c3, yR, { width: colW - 70 });
    doc.text(money(otrosDescuentos), c3, yR, { width: colW, align: "right" });
    yR += 12;

    yR += 2;
    line(doc, c3, yR, c3 + colW, yR);
    yR += 8;
    doc.font("Helvetica-Bold").fontSize(10).text("SALARIO NETO", c3, yR, { width: colW - 70 });
    doc.font("Helvetica-Bold").fontSize(10).text(`$${money(salarioNeto)}`, c3, yR, { width: colW, align: "right" });

    // Lower left tables
    let y2 = Math.max(yMid, yR) + 24;

    sectionTitle(doc, c1, y2, colW, "HORAS REGULARES");
    y2 += 20;

    const row = (name, h, amt) => {
      doc.font("Helvetica").fontSize(9).text(name, c1, y2, { width: colW - 120 });
      doc.text(money(h), c1 + colW - 120, y2, { width: 60, align: "right" });
      doc.text(money(amt), c1 + colW - 60, y2, { width: 60, align: "right" });
      y2 += 12;
    };

    row("Diurna", horas.diurna_h, horas.diurna_$);
    row("Mixta", horas.mixta_h, horas.mixta_$);
    row("Noct", horas.noct_h, horas.noct_$);
    row("H. AJUST", horas.ajust_h, horas.ajust_$);

    y2 += 8;
    sectionTitle(doc, c1, y2, colW, "AUSENCIAS - TARDANZA");
    y2 += 20;

    row("NO Paga", ausencias.no_paga_h, ausencias.no_paga_$);
    row("Pagadas", ausencias.pagadas_h, ausencias.pagadas_$);
    row("C. Med.", ausencias.cmed_h, ausencias.cmed_$);

    // Footer
    const yFoot = doc.page.height - 120;
    doc.font("Helvetica-Bold").fontSize(9).text("DEPOSITADO EN LA CUENTA No.", c1, yFoot);
    doc.font("Helvetica").fontSize(9).text(cuenta, c1, yFoot + 12);

    const sigY = doc.page.height - 70;
    doc.font("Helvetica-Bold").text("RECIBIDO POR:", c1 + 150, sigY);
    line(doc, c1 + 235, sigY + 10, c1 + 480, sigY + 10);

    doc.font("Helvetica-Bold").text("PREPARADO POR:", c2 + 120, sigY);
    line(doc, c2 + 215, sigY + 10, c3 + colW, sigY + 10);

    doc.end();
  });
}

/* =========================================================
   GENERATE VOUCHERS (PDF + Storage)
========================================================= */
function formatISODateToDDMMYYYY(isoDate) {
  const s = String(isoDate || "").slice(0, 10);
  if (!s || s.length !== 10) return s;
  const [y, m, d] = s.split("-");
  return `${d}/${m}/${y}`;
}

app.post("/api/pay-runs/:id/generate-vouchers", requireSupabase, async (req, res) => {
  const payRunId = req.params.id;
  const { companyName = "PRODIMA, S.A." } = req.body || {};

  const { data: run, error: e0 } = await sb
    .from("pay_runs")
    .select("*, pay_periods(*)")
    .eq("id", payRunId)
    .single();
  if (e0) return res.status(500).json({ ok: false, message: e0.message });

  const p = run.pay_periods;
  const periodLabel = p?.label || "Periodo";
  const periodFrom = formatISODateToDDMMYYYY(p?.start_date);
  const periodTo = formatISODateToDDMMYYYY(p?.end_date);
  const planillaTipo = p?.period_type === "MONTHLY" ? "PLANILLA MENSUAL" : "PLANILLA QUINCENAL";

  const { data: slips, error: e1 } = await sb
    .from("pay_slips")
    .select("*, employees(emp_code,full_name,email,hourly_rate)")
    .eq("pay_run_id", payRunId);
  if (e1) return res.status(500).json({ ok: false, message: e1.message });

  let generated = 0;

  for (const s of slips) {
    const emp = s.employees;

    // En MVP: solo salario regular
    const gross = Number(s.gross || 0);

    const pdf = await buildSlipPdfBuffer({
      companyName,
      planillaTipo,
      periodFrom,
      periodTo,
      planillaNo: String(payRunId).slice(0, 6),
      transaccionNo: String(s.id).slice(0, 6),

      sucursal: "PMA - PANAMA",
      empCode: emp.emp_code,
      empName: emp.full_name,
      cedula: "", // agrega en employees si quieres
      cargo: "",  // agrega en employees si quieres
      cuenta: "", // agrega en employees si quieres
      salarioHora: Number(emp.hourly_rate || 0),

      descuentosLegales: [], // en MVP vacío; luego lo calculas
      ingresos: [{ name: "Salario Regular", amount: gross }],
      salarioBruto: gross,
      totalOtros: 0,
      otrosDescuentos: 0,
    });

    const filePath = `payruns/${payRunId}/${emp.emp_code}.pdf`;

    const { error: upErr } = await sb.storage.from(VOUCHER_BUCKET).upload(filePath, pdf, {
      contentType: "application/pdf",
      upsert: true,
    });

    if (upErr) return res.status(500).json({ ok: false, message: upErr.message });

    const { error: u2 } = await sb.from("pay_slips").update({ pdf_path: filePath }).eq("id", s.id);
    if (u2) return res.status(500).json({ ok: false, message: u2.message });

    generated++;
  }

  res.json({ ok: true, payRunId, periodLabel, generated });
});

/* =========================================================
   SEND VOUCHERS (EMAIL)
========================================================= */
app.post("/api/pay-runs/:id/send-vouchers", requireSupabase, async (req, res) => {
  if (!resend) return res.status(400).json({ ok: false, message: "Email provider not configured (RESEND_API_KEY)" });

  const payRunId = req.params.id;
  const { subject = "Comprobante de pago" } = req.body || {};

  const { data: run, error: e0 } = await sb
    .from("pay_runs")
    .select("*, pay_periods(*)")
    .eq("id", payRunId)
    .single();
  if (e0) return res.status(500).json({ ok: false, message: e0.message });

  const periodLabel = run.pay_periods?.label || "Periodo";

  const { data: slips, error: e1 } = await sb
    .from("pay_slips")
    .select("*, employees(full_name,email,emp_code)")
    .eq("pay_run_id", payRunId);
  if (e1) return res.status(500).json({ ok: false, message: e1.message });

  let sent = 0;

  for (const s of slips) {
    const emp = s.employees;
    if (!emp?.email) continue;
    if (!s.pdf_path) continue;

    const { data: signed, error: se } = await sb.storage.from(VOUCHER_BUCKET).createSignedUrl(s.pdf_path, 60 * 60 * 24);
    if (se) continue;

    // ✅ Sin escapes raros: template literal normal
    const html = `
      <p>Hola ${emp.full_name},</p>
      <p>Aquí está tu comprobante de pago del período <b>${periodLabel}</b>:</p>
      <p><a href="${signed.signedUrl}">Descargar voucher</a></p>
      <p>— Nómina</p>
    `;

    await resend.emails.send({
      from: MAIL_FROM,
      to: [emp.email],
      subject: `${subject} - ${periodLabel}`,
      html,
    });

    await sb.from("pay_slips").update({ emailed_at: new Date().toISOString() }).eq("id", s.id);
    sent++;
  }

  await sb.from("pay_runs").update({ status: "SENT", sent_at: new Date().toISOString() }).eq("id", payRunId);

  res.json({ ok: true, payRunId, sent });
});

/* =========================================================
   START
========================================================= */
process.on("unhandledRejection", (e) => console.error("unhandledRejection:", e));
process.on("uncaughtException", (e) => console.error("uncaughtException:", e));

app.listen(Number(PORT), () => console.log(`Planilla server listening on :${PORT}`));
