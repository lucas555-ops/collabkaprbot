-- Intro Retry Credits (v1.3.2)
-- Fairness: if a brand sends a message and creator doesn't reply within N hours,
-- we grant 1 "Retry credit" (expires in M days). Used automatically before paid credits.

-- Track intro payment + first reply timing
alter table barter_threads
  add column if not exists intro_cost int not null default 0,
  add column if not exists intro_charge_source text, -- NONE | CREDITS | RETRY
  add column if not exists intro_charged_at timestamptz,
  add column if not exists buyer_first_msg_at timestamptz,
  add column if not exists seller_first_reply_at timestamptz,
  add column if not exists retry_issued_at timestamptz;

create index if not exists idx_barter_threads_intro_retry on barter_threads(intro_charge_source, retry_issued_at);
create index if not exists idx_barter_threads_first_msg on barter_threads(buyer_first_msg_at);

-- Retry credits ledger (separate from brand_credits; expires)
create table if not exists brand_retry_credits (
  id bigserial primary key,
  user_id bigint not null references users(id) on delete cascade,
  source_thread_id bigint not null references barter_threads(id) on delete cascade,
  status text not null default 'AVAILABLE', -- AVAILABLE | REDEEMED | EXPIRED
  reason text,
  created_at timestamptz not null default now(),
  expires_at timestamptz not null,
  redeemed_at timestamptz,
  redeemed_thread_id bigint references barter_threads(id) on delete set null,
  unique (source_thread_id)
);

create index if not exists idx_brand_retry_credits_user_status on brand_retry_credits(user_id, status, expires_at);
create index if not exists idx_brand_retry_credits_status_expires on brand_retry_credits(status, expires_at);
