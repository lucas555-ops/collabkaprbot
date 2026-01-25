-- Brand Plan (tools subscription for brands)

alter table users
  add column if not exists brand_plan text;

alter table users
  add column if not exists brand_plan_until timestamptz;

alter table users
  add column if not exists brand_plan_updated_at timestamptz;

create index if not exists idx_users_brand_plan_until on users (brand_plan_until);
