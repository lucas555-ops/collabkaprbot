-- 026_brand_managers.sql
-- Brand Team (single role): allow a brand owner user to grant manager access.
-- This is intentionally minimal: one role (Brand Manager), no permissions table.

create table if not exists brand_managers (
  brand_user_id bigint not null references users(id) on delete cascade,
  manager_user_id bigint not null references users(id) on delete cascade,
  added_by_user_id bigint references users(id) on delete set null,
  created_at timestamptz not null default now(),
  primary key (brand_user_id, manager_user_id)
);

create index if not exists idx_brand_managers_brand_created on brand_managers (brand_user_id, created_at desc);
create index if not exists idx_brand_managers_manager_created on brand_managers (manager_user_id, created_at desc);
