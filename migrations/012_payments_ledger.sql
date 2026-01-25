-- Payments ledger (admin apply + status)
--
-- statuses: RECEIVED | APPLIED | ORPHANED | ERROR

create table if not exists payments (
  id bigserial primary key,
  user_id bigint not null references users(id) on delete cascade,
  kind text not null,
  invoice_payload text not null,
  currency text not null,
  total_amount int not null,
  telegram_payment_charge_id text not null,
  provider_payment_charge_id text,
  status text not null default 'RECEIVED',
  note text,
  raw jsonb,

  applying_by_user_id bigint references users(id) on delete set null,
  applying_at timestamptz,

  applied_by_user_id bigint references users(id) on delete set null,
  applied_at timestamptz,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists uq_payments_telegram_charge
  on payments (telegram_payment_charge_id);

create unique index if not exists uq_payments_invoice_payload
  on payments (invoice_payload);

create index if not exists idx_payments_status_time
  on payments (status, created_at desc);

create index if not exists idx_payments_user_time
  on payments (user_id, created_at desc);
