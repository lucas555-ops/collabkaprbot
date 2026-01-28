-- 024_brand_profiles.sql
-- Minimal brand profile for Brand Mode (gating for messaging + optional extended fields)

create table if not exists brand_profiles (
  user_id bigint primary key references users(id) on delete cascade,

  brand_name text,
  brand_link text,
  contact text,

  niche text,
  geo text,
  collab_types text,
  budget text,
  goals text,
  requirements text,

  meta jsonb,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_brand_profiles_updated_at on brand_profiles(updated_at desc);
