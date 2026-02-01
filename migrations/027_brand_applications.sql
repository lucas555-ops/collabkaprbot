-- 027_brand_applications.sql
-- Creator → Brand applications (from brand directory card).
-- Stored and visible in Brand Inbox (Managers can read/reply).

create table if not exists brand_applications (
  id bigserial primary key,
  brand_user_id bigint not null references users(id) on delete cascade,
  creator_user_id bigint not null references users(id) on delete cascade,
  creator_tg_id bigint not null,
  creator_username text,
  message text not null,
  status text not null default 'new',
  reply_text text,
  replied_by_user_id bigint references users(id) on delete set null,
  replied_at timestamptz,
  meta jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_brand_applications_brand_status_created on brand_applications (brand_user_id, status, created_at desc);
create index if not exists idx_brand_applications_brand_created on brand_applications (brand_user_id, created_at desc);
create index if not exists idx_brand_applications_creator_created on brand_applications (creator_user_id, created_at desc);
