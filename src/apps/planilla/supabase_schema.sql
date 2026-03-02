-- Supabase SQL (MVP) - Planilla
-- Pega esto en Supabase -> SQL Editor

do $$ begin
  create type employee_type as enum ('CLOCKS_IN','NO_CLOCK');
exception when duplicate_object then null;
end $$;

do $$ begin
  create type pay_period_type as enum ('BIWEEKLY','MONTHLY');
exception when duplicate_object then null;
end $$;

do $$ begin
  create type payrun_status as enum ('DRAFT','CALCULATED','APPROVED','SENT');
exception when duplicate_object then null;
end $$;

create table if not exists employees (
  id uuid primary key default gen_random_uuid(),
  emp_code text unique not null,
  full_name text not null,
  email text,
  employee_type employee_type not null default 'CLOCKS_IN',
  is_active boolean not null default true,

  salary_type text not null default 'MONTHLY',
  base_salary numeric not null default 0,
  hourly_rate numeric not null default 0,

  department text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists time_imports (
  id uuid primary key default gen_random_uuid(),
  uploaded_by text,
  source_filename text,
  source_type text,
  period_start date,
  period_end date,
  created_at timestamptz not null default now()
);

create table if not exists time_raw (
  id bigserial primary key,
  import_id uuid references time_imports(id) on delete cascade,
  emp_code text not null,
  ts timestamptz not null,
  event_type text,
  raw_line text,
  created_at timestamptz not null default now()
);

create index if not exists time_raw_emp_ts_idx on time_raw(emp_code, ts);

create table if not exists pay_periods (
  id uuid primary key default gen_random_uuid(),
  period_type pay_period_type not null,
  start_date date not null,
  end_date date not null,
  label text not null,
  created_at timestamptz not null default now()
);

create unique index if not exists pay_period_unique_idx
on pay_periods(period_type, start_date, end_date);

create table if not exists pay_runs (
  id uuid primary key default gen_random_uuid(),
  period_id uuid references pay_periods(id) on delete restrict,
  status payrun_status not null default 'DRAFT',
  created_by text,
  created_at timestamptz not null default now(),
  approved_at timestamptz,
  sent_at timestamptz
);

create table if not exists pay_items (
  id bigserial primary key,
  pay_run_id uuid references pay_runs(id) on delete cascade,
  emp_id uuid references employees(id) on delete restrict,
  concept text not null,
  amount numeric not null default 0,
  meta jsonb not null default '{}'::jsonb
);

create index if not exists pay_items_run_idx on pay_items(pay_run_id);

create table if not exists pay_slips (
  id uuid primary key default gen_random_uuid(),
  pay_run_id uuid references pay_runs(id) on delete cascade,
  emp_id uuid references employees(id) on delete restrict,
  gross numeric not null default 0,
  deductions numeric not null default 0,
  net numeric not null default 0,
  pdf_path text,
  emailed_at timestamptz,
  created_at timestamptz not null default now()
);

create unique index if not exists pay_slips_unique_idx on pay_slips(pay_run_id, emp_id);
