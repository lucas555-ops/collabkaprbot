-- Smart Matching requests

create table if not exists matching_requests (
  id bigserial primary key,
  user_id bigint not null references users(id) on delete cascade,
  tier text not null,
  stars_paid int not null default 0,
  brief text,
  result_offer_ids jsonb,
  status text not null default 'PAID',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_matching_requests_user on matching_requests (user_id, created_at desc);
