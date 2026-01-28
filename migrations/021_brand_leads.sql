-- Brand Leads v1 (IG leads -> TG deals)
-- Brands can send a request from a creator public profile (vitrina).
-- Owners / SUPER_ADMIN can process and reply.

create table if not exists brand_leads (
  id bigserial primary key,
  workspace_id bigint not null references workspaces(id) on delete cascade,
  owner_user_id bigint not null references users(id) on delete cascade,
  brand_user_id bigint not null references users(id) on delete cascade,
  brand_tg_id bigint not null,
  brand_username text,
  brand_name text,
  message text not null,
  status text not null default 'new',
  meta jsonb,
  reply_text text,
  replied_by_user_id bigint references users(id) on delete set null,
  replied_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='brand_leads_status_chk') THEN
    ALTER TABLE brand_leads
      ADD CONSTRAINT brand_leads_status_chk
      CHECK (status in ('new','in_progress','closed','spam'));
  END IF;
END$$;

create index if not exists idx_brand_leads_ws_status_created on brand_leads (workspace_id, status, created_at desc);
create index if not exists idx_brand_leads_owner_status_created on brand_leads (owner_user_id, status, created_at desc);
create index if not exists idx_brand_leads_brand_user_created on brand_leads (brand_user_id, created_at desc);
