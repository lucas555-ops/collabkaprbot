-- Brand Pass credits (minimal)

alter table users
  add column if not exists brand_credits int not null default 0;

alter table users
  add column if not exists brand_credits_spent int not null default 0;

alter table users
  add column if not exists brand_credits_updated_at timestamptz;

create index if not exists idx_users_brand_credits on users(brand_credits);
