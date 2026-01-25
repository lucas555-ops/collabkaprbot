-- Barters Marketplace (v0.9.1)

create table if not exists barter_offers (
  id bigserial primary key,
  workspace_id bigint not null references workspaces(id) on delete cascade,
  creator_user_id bigint references users(id) on delete set null,
  status text not null default 'ACTIVE', -- ACTIVE | PAUSED | CLOSED
  category text not null, -- cosmetics | skincare | accessories | other
  offer_type text not null, -- ad | review | giveaway | other
  compensation_type text not null, -- barter | cert | rub | mixed
  title text not null,
  description text not null,
  contact text,
  bump_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_barter_offers_feed on barter_offers(status, bump_at desc);
create index if not exists idx_barter_offers_ws on barter_offers(workspace_id, created_at desc);
create index if not exists idx_barter_offers_category on barter_offers(category, status, bump_at desc);

create table if not exists barter_offer_audit (
  id bigserial primary key,
  offer_id bigint not null references barter_offers(id) on delete cascade,
  workspace_id bigint not null references workspaces(id) on delete cascade,
  actor_user_id bigint references users(id) on delete set null,
  action text not null,
  payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_barter_offer_audit_offer_time on barter_offer_audit(offer_id, created_at desc);
