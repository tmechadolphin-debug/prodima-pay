// src/apps/planilla/server.js
import express from "express";
import cors from "cors";
import multer from "multer";
import * as XLSX from "xlsx";
import PDFDocument from "pdfkit";
import { createClient } from "@supabase/supabase-js";
import { Resend } from "resend";

const {
  PORT = 3000,
  CORS_ORIGIN = "https://prodima.com.pa,https://www.prodima.com.pa",

  SUPABASE_URL = "",
  SUPABASE_SERVICE_ROLE = "",

  RESEND_API_KEY = "",
  MAIL_FROM = "Planilla <nomina@prodima.com.pa>",

  VOUCHER_BUCKET = "payroll-vouchers",

  // Panamá UTC-5
  TZ_OFFSET_MIN = "-300",

  // Multiplicador horas extra (simple)
  OVERTIME_MULT = "1.5",
} = process.env;

const TZ_OFF = Number(TZ_OFFSET_MIN || -300);
const OT_MULT = Number(OVERTIME_MULT || 1.5);

const app = express();
app.use(express.json({ limit: "10mb" }));

app.use(
  cors({
    origin: CORS_ORIGIN
      ? CORS_ORIGIN.split(",").map((s) => s.trim()).filter(Boolean)
      : true,
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

function safeNum(x, d = 0) {
  const n = Number(x);
  return Number.isFinite(n) ? n : d;
}

function toLocalDateISO(tsIso) {
  // Convierte timestamptz a fecha local (Panamá) YYYY-MM-DD
  const d = new Date(tsIso);
  const ms = d.getTime() + d.getTimezoneOffset() * 60000 + TZ_OFF * 60000;
  const u = new Date(ms);
  const y = u.getUTCFullYear();
  const m = String(u.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(u.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

function minutesBetween(aIso, bIso) {
  const a = new Date(aIso).getTime();
  const b = new Date(bIso).getTime();
  if (!Number.isFinite(a) || !Number.isFinite(b)) return 0;
  return Math.max(0, Math.round((b - a) / 60000));
}

function addDays(dateISO, n) {
  const d = new Date(dateISO + "T00:00:00Z");
  d.setUTCDate(d.getUTCDate() + n);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

function dayOfWeekISO(dateISO) {
  // 1=Mon ... 7=Sun
  const d = new Date(dateISO + "T00:00:00Z");
  const js = d.getUTCDay(); // 0=Sun
  return js === 0 ? 7 : js;
}

function isWorkday(mask, dowISO) {
  // Mon=1->bit0, Tue=2->bit1 ... Sun=64->bit6
  const bit = 1 << (dowISO - 1);
  return (mask & bit) !== 0;
}

app.get("/api/health", (req, res) => {
  res.json({
    ok: true,
    supabase: !!sb,
    mail: !!resend,
    tzOffsetMin: TZ_OFF,
    overtimeMult: OT_MULT,
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
  if (!p.emp_code || !p.full_name) return res.status(400).json({ ok: false, message: "emp_code y full_name requeridos" });

  const payload = {
    emp_code: String(p.emp_code).trim(),
    full_name: String(p.full_name).trim(),
    email: p.email ? String(p.email).trim() : null,
    employee_type: p.employee_type === "NO_CLOCK" ? "NO_CLOCK" : "CLOCKS_IN",
    salary_type: String(p.salary_type || "BIWEEKLY").toUpperCase(), // BIWEEKLY | MONTHLY | HOURLY
    base_salary: safeNum(p.base_salary, 0),
    hourly_rate: safeNum(p.hourly_rate, 0),
    department: p.department || null,
    is_active: p.is_active !== false,
  };

  const { data, error } = await sb.from("employees").insert(payload).select("*").single();
  if (error) return res.status(500).json({ ok: false, message: error.message });
  res.json({ ok: true, employee: data });
});

/* =========================================================
   SHIFT TEMPLATES + ASSIGN
========================================================= */
app.get("/api/shifts/templates", requireSupabase, async (req, res) => {
  const { data, error } = await sb.from("shift_templates").select("*").order("created_at", { ascending: false });
  if (error) return res.status(500).json({ ok: false, message: error.message });
  res.json({ ok: true, templates: data });
});

app.post("/api/shifts/templates", requireSupabase, async (req, res) => {
  const b = req.body || {};
  if (!b.name || !b.start_time || !b.end_time) {
    return res.status(400).json({ ok: false, message: "name, start_time, end_time requeridos" });
  }
  const payload = {
    name: String(b.name),
    start_time: b.start_time,
    end_time: b.end_time,
    break_minutes: Math.max(0, Math.trunc(safeNum(b.break_minutes, 60))),
    grace_minutes: Math.max(0, Math.trunc(safeNum(b.grace_minutes, 5))),
    workdays_mask: Math.max(0, Math.trunc(safeNum(b.workdays_mask, 31))), // Mon-Fri default
  };
  const { data, error } = await sb.from("shift_templates").insert(payload).select("*").single();
  if (error) return res.status(500).json({ ok: false, message: error.message });
  res.json({ ok: true, template: data });
});

app.post("/api/employees/:id/shift", requireSupabase, async (req, res) => {
  const empId = req.params.id;
  const { shift_id, effective_from, effective_to } = req.body || {};
  if (!shift_id || !effective_from) return res.status(400).json({ ok: false, message: "shift_id y effective_from requeridos" });

  const payload = {
    emp_id: empId,
    shift_id,
    effective_from,
    effective_to: effective_to || null,
  };

  const { data, error } = await sb.from("employee_shift_assignments").insert(payload).select("*").single();
  if (error) return res.status(500).json({ ok: false, message: error.message });
  res.json({ ok: true, assignment: data });
});

/* =========================================================
   PAY PERIODS + RUNS
========================================================= */
app.get("/api/pay-periods", requireSupabase, async (req, res) => {
  const { data, error } = await sb.from("pay_periods").select("*").order("start_date", { ascending: false });
  if (error) return res.status(500).json({ ok: false, message: error.message });
  res.json({ ok: true, periods: data });
});

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

app.post("/api/pay-runs", requireSupabase, async (req, res) => {
  const { period_id, created_by = "admin" } = req.body || {};
  if (!period_id) return res.status(400).json({ ok: false, message: "period_id requerido" });

  const { data: run, error } = await sb
    .from("pay_runs")
    .insert({ period_id, status: "DRAFT", created_by })
    .select("*")
    .single();

  if (error) return res.status(500).json({ ok: false, message: error.message });
  res.json({ ok: true, run });
});

/* =========================================================
   IMPORT TIME FILE (TXT / XLSX) -> time_raw
========================================================= */
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 15 * 1024 * 1024 } });

app.post("/api/time/import", requireSupabase, upload.single("file"), async (req, res) => {
  try {
    const file = req.file;
    const { period_start, period_end, uploaded_by = "admin" } = req.body || {};
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

    // insert chunks
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
   BUILD SHIFTS: time_raw -> work_shifts (por pay_run)
========================================================= */
async function getEmployeeShiftForDate(empId, dateISO) {
  // Busca asignación vigente; si no hay, usa plantilla STD
  const { data: assigns } = await sb
    .from("employee_shift_assignments")
    .select("*, shift_templates(*)")
    .eq("emp_id", empId)
    .lte("effective_from", dateISO)
    .or(`effective_to.is.null,effective_to.gte.${dateISO}`)
    .order("effective_from", { ascending: false })
    .limit(1);

  if (assigns && assigns.length) return assigns[0].shift_templates;

  // fallback: plantilla STD
  const { data: t } = await sb
    .from("shift_templates")
    .select("*")
    .eq("name", "STD 07:30-17:00 (Mon-Fri)")
    .limit(1);
  return t && t.length ? t[0] : null;
}

app.post("/api/pay-runs/:id/build-shifts", requireSupabase, async (req, res) => {
  try {
    const payRunId = req.params.id;

    const { data: run, error: e0 } = await sb
      .from("pay_runs")
      .select("*, pay_periods(*)")
      .eq("id", payRunId)
      .single();
    if (e0) return res.status(500).json({ ok: false, message: e0.message });

    const p = run.pay_periods;
    const start = p.start_date;
    const end = p.end_date;

    const { data: emps, error: e1 } = await sb.from("employees").select("*").eq("is_active", true);
    if (e1) return res.status(500).json({ ok: false, message: e1.message });

    // Traer todas las marcaciones del rango (para todos)
    const { data: marks, error: e2 } = await sb
      .from("time_raw")
      .select("emp_code,ts,event_type")
      .gte("ts", start + "T00:00:00Z")
      .lte("ts", end + "T23:59:59Z")
      .order("ts", { ascending: true });

    if (e2) return res.status(500).json({ ok: false, message: e2.message });

    // Agrupar marcaciones por emp_code + date
    const byEmpDate = new Map(); // key => [ts...]
    for (const m of (marks || [])) {
      const code = String(m.emp_code || "").trim();
      if (!code) continue;
      const d = toLocalDateISO(m.ts);
      const key = code + "||" + d;
      if (!byEmpDate.has(key)) byEmpDate.set(key, []);
      byEmpDate.get(key).push(m);
    }

    // borrar shifts previos de este run (rebuild)
    await sb.from("work_shifts").delete().eq("pay_run_id", payRunId);

    const payload = [];
    let created = 0;

    for (const emp of emps) {
      const empCode = String(emp.emp_code || "").trim();
      if (!empCode) continue;

      // si NO_CLOCK, igual creamos rows (para reportes), pero sin in/out
      for (let d = start; d <= end; d = addDays(d, 1)) {
        const tmpl = await getEmployeeShiftForDate(emp.id, d);
        if (!tmpl) continue;

        const dow = dayOfWeekISO(d);
        const laborable = isWorkday(Number(tmpl.workdays_mask || 31), dow);

        // si no es laborable, no creamos jornada (por defecto sábados NO)
        if (!laborable) continue;

        const key = empCode + "||" + d;
        const dayMarks = byEmpDate.get(key) || [];

        const scheduled_start = tmpl.start_time;
        const scheduled_end = tmpl.end_time;
        const break_minutes = Number(tmpl.break_minutes || 60);
        const grace_minutes = Number(tmpl.grace_minutes || 5);

        let in_ts = null;
        let out_ts = null;

        if (emp.employee_type === "CLOCKS_IN") {
          // regla simple: primera marca del día = entrada, última marca = salida
          if (dayMarks.length) {
            in_ts = dayMarks[0].ts;
            out_ts = dayMarks[dayMarks.length - 1].ts;
          }
        }

        let status = "OK";
        let absent = false;

        if (emp.employee_type === "CLOCKS_IN" && !in_ts && !out_ts) {
          status = "NO_MARKS";
          absent = true;
        } else if (emp.employee_type === "CLOCKS_IN" && (!in_ts || !out_ts || in_ts === out_ts)) {
          status = "INCOMPLETE";
        }

        // cálculo minutos
        let worked_minutes = 0;
        let late_minutes = 0;
        let overtime_minutes = 0;

        if (in_ts && out_ts && in_ts !== out_ts) {
          worked_minutes = Math.max(0, minutesBetween(in_ts, out_ts) - break_minutes);

          // tardanza: comparar hora local de in_ts con start+grace
          // Convertimos in_ts a hora local (Panamá)
          const inLocalDate = toLocalDateISO(in_ts);
          // construimos ISO local "fecha + start_time", luego pasamos a UTC aproximado con offset
          const graceStart = tmpl.start_time; // time string
          const [hh, mm] = String(graceStart).split(":").map(Number);
          const graceTotal = hh * 60 + mm + grace_minutes;

          const inD = new Date(in_ts);
          const ms = inD.getTime() + inD.getTimezoneOffset() * 60000 + TZ_OFF * 60000;
          const loc = new Date(ms);
          const inMin = loc.getUTCHours() * 60 + loc.getUTCMinutes();

          late_minutes = Math.max(0, inMin - graceTotal);

          // overtime si salida > scheduled_end
          const [eh, em] = String(tmpl.end_time).split(":").map(Number);
          const endMin = eh * 60 + em;

          const outD = new Date(out_ts);
          const ms2 = outD.getTime() + outD.getTimezoneOffset() * 60000 + TZ_OFF * 60000;
          const loc2 = new Date(ms2);
          const outMin = loc2.getUTCHours() * 60 + loc2.getUTCMinutes();

          overtime_minutes = Math.max(0, outMin - endMin);
        }

        payload.push({
          pay_run_id: payRunId,
          emp_id: emp.id,
          emp_code: empCode,
          work_date: d,
          scheduled_start,
          scheduled_end,
          break_minutes,
          grace_minutes,
          in_ts,
          out_ts,
          worked_minutes,
          late_minutes,
          overtime_minutes,
          absent,
          status,
          notes: null,
        });

        created++;
      }
    }

    // upsert en chunks
    const CH = 1000;
    for (let i = 0; i < payload.length; i += CH) {
      const chunk = payload.slice(i, i + CH);
      const { error } = await sb.from("work_shifts").insert(chunk);
      if (error) return res.status(500).json({ ok: false, message: error.message });
    }

    return res.json({ ok: true, payRunId, daysCreated: created, rows: payload.length });
  } catch (e) {
    return res.status(500).json({ ok: false, message: e.message || String(e) });
  }
});

/* =========================================================
   CALCULATE DETAILED: work_shifts -> pay_items + pay_slips + summary
========================================================= */
app.post("/api/pay-runs/:id/calculate-detailed", requireSupabase, async (req, res) => {
  try {
    const payRunId = req.params.id;

    const { data: run, error: e0 } = await sb
      .from("pay_runs")
      .select("*, pay_periods(*)")
      .eq("id", payRunId)
      .single();
    if (e0) return res.status(500).json({ ok: false, message: e0.message });

    const period = run.pay_periods;

    const { data: emps, error: e1 } = await sb.from("employees").select("*").eq("is_active", true);
    if (e1) return res.status(500).json({ ok: false, message: e1.message });

    // asegurarnos de que existan work_shifts
    const { data: wsCount } = await sb.from("work_shifts").select("id", { count: "exact", head: true }).eq("pay_run_id", payRunId);

    // limpiar cálculos previos
    await sb.from("pay_items").delete().eq("pay_run_id", payRunId);
    await sb.from("pay_slips").delete().eq("pay_run_id", payRunId);
    await sb.from("pay_run_employee_summary").delete().eq("pay_run_id", payRunId);

    const items = [];
    const slips = [];
    const summaries = [];

    for (const emp of emps) {
      // base periodo
      let periodBase = safeNum(emp.base_salary, 0);
      if (String(emp.salary_type || "").toUpperCase() === "MONTHLY" && period.period_type === "BIWEEKLY") {
        periodBase = periodBase / 2;
      }
      periodBase = Number(periodBase.toFixed(2));

      if (emp.employee_type === "NO_CLOCK") {
        // paga fijo
        items.push({
          pay_run_id: payRunId,
          emp_id: emp.id,
          concept: "BASE_SALARY",
          amount: periodBase,
          meta: { mode: "NO_CLOCK" },
        });

        const gross = periodBase;
        const deductions = 0;
        const net = gross;

        slips.push({ pay_run_id: payRunId, emp_id: emp.id, gross, deductions, net, pdf_path: null });

        summaries.push({
          pay_run_id: payRunId,
          emp_id: emp.id,
          emp_code: emp.emp_code,
          full_name: emp.full_name,
          expected_minutes: 0,
          worked_minutes: 0,
          late_minutes: 0,
          absent_days: 0,
          overtime_minutes: 0,
          gross,
          deductions,
          net,
        });
        continue;
      }

      // CLOCKS_IN -> basado en work_shifts
      const { data: shifts, error: eS } = await sb
        .from("work_shifts")
        .select("*")
        .eq("pay_run_id", payRunId)
        .eq("emp_id", emp.id)
        .order("work_date", { ascending: true });

      if (eS) return res.status(500).json({ ok: false, message: eS.message });

      // expected minutes por día: (end-start)-break
      let expected = 0;
      let worked = 0;
      let late = 0;
      let absentDays = 0;
      let ot = 0;

      for (const s of (shifts || [])) {
        const [sh, sm] = String(s.scheduled_start).split(":").map(Number);
        const [eh, em] = String(s.scheduled_end).split(":").map(Number);
        const sched = Math.max(0, (eh * 60 + em) - (sh * 60 + sm) - Number(s.break_minutes || 60));
        expected += sched;
        worked += Number(s.worked_minutes || 0);
        late += Number(s.late_minutes || 0);
        ot += Number(s.overtime_minutes || 0);
        if (s.absent) absentDays += 1;
      }

      // rate por minuto en el periodo (solo basado en expected para que ausencias descuenten)
      // si no hay expected (raro), paga fijo
      let gross = 0;
      if (expected > 0) {
        const ratePerMin = periodBase / expected;
        gross = worked * ratePerMin;

        // overtime (simple): paga extra a OT_MULT sobre rate normal
        gross += ot * ratePerMin * (OT_MULT - 1);
      } else {
        gross = periodBase;
      }

      gross = Number(gross.toFixed(2));
      const deductions = 0;
      const net = Number((gross - deductions).toFixed(2));

      items.push({
        pay_run_id: payRunId,
        emp_id: emp.id,
        concept: "BASE_SALARY_BY_MINUTES",
        amount: gross,
        meta: { expected_minutes: expected, worked_minutes: worked, overtime_minutes: ot, late_minutes: late, absent_days: absentDays },
      });

      slips.push({ pay_run_id: payRunId, emp_id: emp.id, gross, deductions, net, pdf_path: null });

      summaries.push({
        pay_run_id: payRunId,
        emp_id: emp.id,
        emp_code: emp.emp_code,
        full_name: emp.full_name,
        expected_minutes: expected,
        worked_minutes: worked,
        late_minutes: late,
        absent_days: absentDays,
        overtime_minutes: ot,
        gross,
        deductions,
        net,
      });
    }

    // insertar
    if (items.length) {
      const { error } = await sb.from("pay_items").insert(items);
      if (error) return res.status(500).json({ ok: false, message: error.message });
    }
    if (slips.length) {
      const { error } = await sb.from("pay_slips").insert(slips);
      if (error) return res.status(500).json({ ok: false, message: error.message });
    }
    if (summaries.length) {
      const { error } = await sb.from("pay_run_employee_summary").insert(summaries);
      if (error) return res.status(500).json({ ok: false, message: error.message });
    }

    await sb.from("pay_runs").update({ status: "CALCULATED" }).eq("id", payRunId);

    return res.json({ ok: true, payRunId, employees: summaries.length });
  } catch (e) {
    return res.status(500).json({ ok: false, message: e.message || String(e) });
  }
});

/* =========================================================
   REPORTS: summary + employee details
========================================================= */
app.get("/api/pay-runs/:id/summary", requireSupabase, async (req, res) => {
  const payRunId = req.params.id;
  const { data, error } = await sb
    .from("pay_run_employee_summary")
    .select("*")
    .eq("pay_run_id", payRunId)
    .order("full_name", { ascending: true });

  if (error) return res.status(500).json({ ok: false, message: error.message });
  res.json({ ok: true, rows: data || [] });
});

app.get("/api/pay-runs/:id/employee/:empId/shifts", requireSupabase, async (req, res) => {
  const payRunId = req.params.id;
  const empId = req.params.empId;

  const { data, error } = await sb
    .from("work_shifts")
    .select("*")
    .eq("pay_run_id", payRunId)
    .eq("emp_id", empId)
    .order("work_date", { ascending: true });

  if (error) return res.status(500).json({ ok: false, message: error.message });
  res.json({ ok: true, shifts: data || [] });
});

/* =========================================================
   Voucher PDF + Email (reusa lo tuyo; aquí solo lo dejo base)
========================================================= */
function buildSlipPdfBuffer({ companyName, periodLabel, empName, empCode, gross, deductions, net }) {
  return new Promise((resolve) => {
    const doc = new PDFDocument({ size: "A4", margin: 40 });
    const chunks = [];
    doc.on("data", (c) => chunks.push(c));
    doc.on("end", () => resolve(Buffer.concat(chunks)));

    doc.fontSize(16).text(companyName || "PRODIMA - Nómina", { align: "center" });
    doc.moveDown(0.2);
    doc.fontSize(11).text(`Comprobante de Pago: ${periodLabel}`, { align: "center" });
    doc.moveDown(1);

    doc.fontSize(11).text(`Empleado: ${empName}`);
    doc.text(`Código: ${empCode}`);
    doc.moveDown(1);

    doc.text(`Devengado: $${Number(gross).toFixed(2)}`);
    doc.text(`Deducciones: $${Number(deductions).toFixed(2)}`);
    doc.moveDown(0.5);
    doc.fontSize(13).text(`NETO: $${Number(net).toFixed(2)}`, { underline: true });

    doc.moveDown(2);
    doc.fontSize(9).text("Generado automáticamente.", { align: "left" });

    doc.end();
  });
}

app.post("/api/pay-runs/:id/generate-vouchers", requireSupabase, async (req, res) => {
  const payRunId = req.params.id;
  const { companyName = "PRODIMA" } = req.body || {};

  const { data: run, error: e0 } = await sb
    .from("pay_runs")
    .select("*, pay_periods(*)")
    .eq("id", payRunId)
    .single();
  if (e0) return res.status(500).json({ ok: false, message: e0.message });

  const periodLabel = run.pay_periods?.label || "Periodo";

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

  res.json({ ok: true, payRunId, generated });
});

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

    await resend.emails.send({
      from: MAIL_FROM,
      to: [emp.email],
      subject: `${subject} - ${periodLabel}`,
      html:
        "<p>Hola " + emp.full_name + ",</p>" +
        "<p>Tu voucher del período <b>" + periodLabel + "</b>:</p>" +
        '<p><a href="' + signed.signedUrl + '">Descargar voucher</a></p>' +
        "<p>— Nómina</p>",
    });

    await sb.from("pay_slips").update({ emailed_at: new Date().toISOString() }).eq("id", s.id);
    sent++;
  }

  await sb.from("pay_runs").update({ status: "SENT", sent_at: new Date().toISOString() }).eq("id", payRunId);

  res.json({ ok: true, payRunId, sent });
});

app.listen(Number(PORT), () => console.log(`Planilla server listening on :${PORT}`));
