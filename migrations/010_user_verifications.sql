-- user verifications (rolling upgrade)

create table if not exists user_verifications (
  user_id bigint primary key references users(id) on delete cascade,
  kind text not null default 'creator',
  status text not null default 'PENDING',
  submitted_text text,
  submitted_at timestamptz not null default now(),
  reviewed_at timestamptz,
  reviewed_by_user_id bigint references users(id),
  rejection_reason text,
  updated_at timestamptz not null default now()
);

create index if not exists idx_user_verifications_status on user_verifications(status);
create index if not exists idx_user_verifications_submitted_at on user_verifications(submitted_at desc);
