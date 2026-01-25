-- Stars payments ledger (idempotency + refunds)

create table if not exists stars_payments (
  id bigserial primary key,
  user_id bigint not null references users(id) on delete cascade,
  kind text not null,
  invoice_payload text not null,
  currency text not null,
  total_amount int not null,
  telegram_payment_charge_id text not null,
  provider_payment_charge_id text,
  raw jsonb,
  created_at timestamptz not null default now()
);

create unique index if not exists uq_stars_payments_telegram_charge
  on stars_payments (telegram_payment_charge_id);

-- extra safety: invoice payloads should not be applied twice
create unique index if not exists uq_stars_payments_invoice_payload
  on stars_payments (invoice_payload);

create index if not exists idx_stars_payments_user
  on stars_payments (user_id, created_at desc);
