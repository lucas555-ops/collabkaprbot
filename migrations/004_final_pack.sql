-- Final Pack v1.0.0 (Monetization + Moderation)

-- Workspace: plan, profile, pinned
alter table workspace_settings add column if not exists plan text not null default 'free';
alter table workspace_settings add column if not exists pro_until timestamptz;
alter table workspace_settings add column if not exists pro_pinned_offer_id bigint;

alter table workspace_settings add column if not exists profile_title text;
alter table workspace_settings add column if not exists profile_niche text;
alter table workspace_settings add column if not exists profile_contact text;
alter table workspace_settings add column if not exists profile_geo text;

-- Ensure enum-ish plan values
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname='workspace_settings_plan_chk'
  ) THEN
    ALTER TABLE workspace_settings
      ADD CONSTRAINT workspace_settings_plan_chk CHECK (plan in ('free','pro'));
  END IF;
END$$;

-- Network moderators ("curators of quality")
create table if not exists network_moderators (
  user_id bigint primary key references users(id) on delete cascade,
  added_by_user_id bigint not null references users(id) on delete cascade,
  created_at timestamptz not null default now()
);

-- Reports / disputes (queue for moderators)
create table if not exists barter_reports (
  id bigserial primary key,
  workspace_id bigint not null references workspaces(id) on delete cascade,
  offer_id bigint references barter_offers(id) on delete set null,
  thread_id bigint references barter_threads(id) on delete set null,
  reporter_user_id bigint references users(id) on delete set null,
  reason text not null,
  details text,
  status text not null default 'OPEN', -- OPEN | RESOLVED
  resolved_by_user_id bigint references users(id) on delete set null,
  resolved_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_barter_reports_status_time on barter_reports(status, created_at desc);
create index if not exists idx_barter_reports_ws_time on barter_reports(workspace_id, created_at desc);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname='barter_reports_status_chk'
  ) THEN
    ALTER TABLE barter_reports
      ADD CONSTRAINT barter_reports_status_chk CHECK (status in ('OPEN','RESOLVED'));
  END IF;
END$$;
