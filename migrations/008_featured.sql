-- Featured placements (sell attention in the feed)

create table if not exists featured_placements (
  id bigserial primary key,
  user_id bigint not null references users(id) on delete cascade,
  duration_days int not null,
  stars_paid int not null default 0,
  title text,
  body text,
  contact text,
  status text not null default 'DRAFT',
  starts_at timestamptz,
  ends_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_featured_active on featured_placements (status, ends_at desc);
create index if not exists idx_featured_user on featured_placements (user_id, created_at desc);
