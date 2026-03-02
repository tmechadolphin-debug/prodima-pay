import express from "express";
import cors from "cors";
import multer from "multer";
import * as XLSX from "xlsx";
import PDFDocument from "pdfkit";
import { createClient } from "@supabase/supabase-js";
import { Resend } from "resend";

const {
  PORT = 3000,
  CORS_ORIGIN = "",
  SUPABASE_URL = "",
  SUPABASE_SERVICE_ROLE = "",
  RESEND_API_KEY = "",
  MAIL_FROM = "Payroll <payroll@localhost>",
} = process.env;

const app = express();
app.use(express.json({ limit: "5mb" }));

app.use(
  cors({
    origin: CORS_ORIGIN ? CORS_ORIGIN.split(",").map((s) => s.trim()) : true,
    credentials: true,
  })
);

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

app.get("/api/health", (req, res) => {
  res.json({
    ok: true,
    supabase: !!sb,
    mail: !!resend,
  });
});

/* =========================================================
   EMPLOYEES CRUD (mínimo)
========================================================= */
app.get("/api/employees", requireSupabase, async (req, res) => {
  const { data, error } = await sb
    .from("employees")
    .select("*")
    .order("full_name", { ascending: true });

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
    salary_type: p.salary_type || "MONTHLY",
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
 * TXT expected example (editable):
 * emp_code|2026-03-01 08:01:00|IN
 * emp_code|2026-03-01 17:03:00|OUT
 *
 * XLSX expected columns:
 * emp_code, ts, event_type (optional)
 */
app.post("/api/time/import", requireSupabase, upload.single("file"), async (req, res) => {
  try {
    const file = req.file;
    const { period_start, period_end, uploaded_by = "system" } = req.body || {};
    if (!file) return res.status(400).json({ ok: false, message: "Archivo requerido" });

    const filename = file.originalname || "upload";
    const isXlsx = filename.toLowerCase().endsWith(".xlsx") || filename.toLowerCase().endsWith(".xls");
    const source_type = isXlsx ? "XLSX" : "TXT";

    // create import record
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
        // flexible split: | or , or tab
        const parts = line.includes("|") ? line.split("|") : line.split(/[,\t]/);
        const emp_code = String(parts[0] || "").trim();
        const tsRaw = String(parts[1] || "").trim();
        const event_type = parts[2] ? String(parts[2]).trim().toUpperCase() : null;

        if (!emp_code || !tsRaw) continue;
        const ts = new Date(tsRaw);
        if (isNaN(ts.getTime())) continue;

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
        if (isNaN(ts.getTime())) continue;

        rows.push({
          import_id: imp.id,
          emp_code,
          ts: ts.toISOString(),
          event_type: event_type || null,
          raw_line: JSON.stringify(r),
        });
      }
    }

    if (!rows.length) {
      return res.json({ ok: true, import: imp, inserted: 0, message: "No se detectaron filas válidas" });
    }

    // bulk insert in chunks
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
   - CLOCKS_IN: base_salary prorrateado por período (simplificado)
   - NO_CLOCK: base_salary directo (gerentes/mercaderistas)
   (Extras, ISR, SS, tardanzas etc se agregan después)
========================================================= */
function daysBetween(startDate, endDate) {
  const s = new Date(startDate + "T00:00:00Z");
  const e = new Date(endDate + "T00:00:00Z");
  const ms = e.getTime() - s.getTime();
  return Math.floor(ms / 86400000) + 1;
}

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
  const totalDays = daysBetween(period.start_date, period.end_date);

  const { data: emps, error: e1 } = await sb
    .from("employees")
    .select("*")
    .eq("is_active", true);

  if (e1) return res.status(500).json({ ok: false, message: e1.message });

  // clear old items/slips for this run
  await sb.from("pay_items").delete().eq("pay_run_id", payRunId);
  await sb.from("pay_slips").delete().eq("pay_run_id", payRunId);

  const payItems = [];
  const slips = [];

  for (const emp of emps) {
    // Rule: NO_CLOCK => siempre cobra base_salary sin depender de time_raw
    // CLOCKS_IN => en MVP cobra base_salary igual (luego: descuentos por ausencia)
    let base = Number(emp.base_salary || 0);

    // Si salary_type es MONTHLY pero el periodo es quincenal, prorrateo simple:
    if (String(emp.salary_type || "").toUpperCase() === "MONTHLY" && period.period_type === "BIWEEKLY") {
      base = base / 2;
    }

    payItems.push({
      pay_run_id: payRunId,
      emp_id: emp.id,
      concept: "BASE_SALARY",
      amount: Number(base.toFixed(2)),
      meta: { employee_type: emp.employee_type, salary_type: emp.salary_type },
    });

    const gross = Number(base.toFixed(2));
    const deductions = 0;
    const net = gross - deductions;

    slips.push({
      pay_run_id: payRunId,
      emp_id: emp.id,
      gross,
      deductions,
      net,
      pdf_path: null,
    });
  }

  // insert
  const { error: e2 } = await sb.from("pay_items").insert(payItems);
  if (e2) return res.status(500).json({ ok: false, message: e2.message });

  const { error: e3 } = await sb.from("pay_slips").insert(slips);
  if (e3) return res.status(500).json({ ok: false, message: e3.message });

  const { error: e4 } = await sb.from("pay_runs").update({ status: "CALCULATED" }).eq("id", payRunId);
  if (e4) return res.status(500).json({ ok: false, message: e4.message });

  res.json({ ok: true, payRunId, employees: emps.length, totalDays });
});

/* =========================================================
   VOUCHER PDF + Upload to Supabase Storage
========================================================= */
function buildSlipPdfBuffer({ companyName, periodLabel, empName, empCode, gross, deductions, net }) {
  return new Promise((resolve) => {
    const doc = new PDFDocument({ size: "A4", margin: 40 });
    const chunks = [];
    doc.on("data", (c) => chunks.push(c));
    doc.on("end", () => resolve(Buffer.concat(chunks)));

    doc.fontSize(18).text(companyName || "PRODIMA - Nómina", { align: "center" });
    doc.moveDown(0.5);
    doc.fontSize(12).text(`Comprobante de Pago: ${periodLabel}`, { align: "center" });
    doc.moveDown(1);

    doc.fontSize(12).text(`Empleado: ${empName}`);
    doc.text(`Código: ${empCode}`);
    doc.moveDown(1);

    doc.fontSize(12).text(`Devengado: $${gross.toFixed(2)}`);
    doc.text(`Deducciones: $${deductions.toFixed(2)}`);
    doc.moveDown(0.5);
    doc.fontSize(14).text(`NETO A PAGAR: $${net.toFixed(2)}`, { underline: true });

    doc.moveDown(2);
    doc.fontSize(10).text("Documento generado automáticamente.", { align: "left" });

    doc.end();
  });
}

app.post("/api/pay-runs/:id/generate-vouchers", requireSupabase, async (req, res) => {
  const payRunId = req.params.id;
  const { companyName = "PRODIMA", bucket = "payroll-vouchers" } = req.body || {};

  const { data: run, error: e0 } = await sb
    .from("pay_runs")
    .select("*, pay_periods(*)")
    .eq("id", payRunId)
    .single();
  if (e0) return res.status(500).json({ ok: false, message: e0.message });

  const periodLabel = run.pay_periods?.label || "Periodo";

  // get slips + employee info
  const { data: slips, error: e1 } = await sb
    .from("pay_slips")
    .select("*, employees(emp_code,full_name,email)")
    .eq("pay_run_id", payRunId);

  if (e1) return res.status(500).json({ ok: false, message: e1.message });

  let generated = 0;

  for (const s of slips) {
    const emp = s.employees;
    const pdf = await buildSlipPdfBuffer({
      companyName,
      periodLabel,
      empName: emp.full_name,
      empCode: emp.emp_code,
      gross: Number(s.gross || 0),
      deductions: Number(s.deductions || 0),
      net: Number(s.net || 0),
    });

    const path = `payruns/${payRunId}/${emp.emp_code}.pdf`;

    const { error: upErr } = await sb.storage.from(bucket).upload(path, pdf, {
      contentType: "application/pdf",
      upsert: true,
    });

    if (upErr) return res.status(500).json({ ok: false, message: upErr.message });

    const { error: u2 } = await sb.from("pay_slips").update({ pdf_path: path }).eq("id", s.id);
    if (u2) return res.status(500).json({ ok: false, message: u2.message });

    generated++;
  }

  res.json({ ok: true, payRunId, generated });
});

/* =========================================================
   EMAIL vouchers (Resend)
========================================================= */
app.post("/api/pay-runs/:id/send-vouchers", requireSupabase, async (req, res) => {
  if (!resend) return res.status(400).json({ ok: false, message: "Email provider not configured" });

  const payRunId = req.params.id;
  const { bucket = "payroll-vouchers", subject = "Comprobante de pago" } = req.body || {};

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

    // Signed URL (1 día)
    const { data: signed, error: se } = await sb.storage.from(bucket).createSignedUrl(s.pdf_path, 60 * 60 * 24);
    if (se) continue;

    await resend.emails.send({
      from: MAIL_FROM,
      to: [emp.email],
      subject: `${subject} - ${periodLabel}`,
      html: `
        <p>Hola ${emp.full_name},</p>
        <p>Aquí está tu comprobante de pago del período <b>${periodLabel}</b>:</p>
        <p><a href="${signed.signedUrl}">Descargar voucher</a></p>
        <p>— Nómina</p>
      `,
    });

    await sb.from("pay_slips").update({ emailed_at: new Date().toISOString() }).eq("id", s.id);
    sent++;
  }

  await sb.from("pay_runs").update({ status: "SENT", sent_at: new Date().toISOString() }).eq("id", payRunId);

  res.json({ ok: true, payRunId, sent });
});

app.listen(Number(PORT), () => console.log(`Payroll API listening on :${PORT}`));
