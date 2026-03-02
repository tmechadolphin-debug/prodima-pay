# Planilla (MVP) - Prodima

Este módulo vive en `src/apps/planilla` y está pensado para desplegarse como **servicio separado** en Render (o donde uses).

## 1) Instalar
```bash
cd src/apps/planilla
npm install
cp .env.example .env
npm start
```

## 2) Variables de entorno
- `SUPABASE_URL` = Project URL (https://xxxxx.supabase.co)
- `SUPABASE_SERVICE_ROLE` = `sb_secret_...`
- `VOUCHER_BUCKET` = bucket en Supabase Storage (ej: `payroll-vouchers`)

## 3) Supabase SQL
Ejecuta `supabase_schema.sql` en Supabase → SQL Editor.

Crea un bucket en Storage: `payroll-vouchers` (privado).

## 4) Frontend
El frontend mínimo está en `public/index.html`.
El server lo sirve en `/`.

## 5) Endpoints principales
- `GET /api/health`
- `GET /api/employees`
- `POST /api/employees`
- `GET /api/pay-periods`
- `POST /api/pay-periods`
- `POST /api/time/import` (multipart file)
- `POST /api/pay-runs` (crea corrida)
- `POST /api/pay-runs/:id/calculate` (MVP: salario base)
- `POST /api/pay-runs/:id/generate-vouchers` (genera PDF y sube a Storage)
- `POST /api/pay-runs/:id/send-vouchers` (envía links por email si configuras Resend)

> Nota: Es un MVP. Luego se agregan: horas extra, tardanzas, feriados, deducciones legales, préstamos, etc.
