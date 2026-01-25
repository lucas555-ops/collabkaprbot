-- MicroGiveaways Bot schema (v0.9)

create table if not exists users (
  id bigserial primary key,
  tg_id bigint not null unique,
  tg_username text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists workspaces (
  id bigserial primary key,
  owner_user_id bigint not null references users(id) on delete cascade,
  title text not null,
  channel_id bigint not null,
  channel_username text,
  created_at timestamptz not null default now(),
  unique(owner_user_id, channel_id)
);

create table if not exists workspace_settings (
  workspace_id bigint primary key references workspaces(id) on delete cascade,
  network_enabled boolean not null default false,
  curator_enabled boolean not null default false,
  auto_draw_default boolean not null default false,
  auto_publish_default boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists workspace_curators (
  id bigserial primary key,
  workspace_id bigint not null references workspaces(id) on delete cascade,
  user_id bigint not null references users(id) on delete cascade,
  added_by_user_id bigint not null references users(id) on delete cascade,
  created_at timestamptz not null default now(),
  unique(workspace_id, user_id)
);

create table if not exists workspace_audit (
  id bigserial primary key,
  workspace_id bigint not null references workspaces(id) on delete cascade,
  actor_user_id bigint not null references users(id) on delete cascade,
  action text not null,
  payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_workspace_audit_ws_time on workspace_audit(workspace_id, created_at desc);
create index if not exists idx_workspace_audit_actor_time on workspace_audit(actor_user_id, created_at desc);

create table if not exists giveaways (
  id bigserial primary key,
  workspace_id bigint not null references workspaces(id) on delete cascade,
  status text not null default 'DRAFT',
  prize_value_text text,
  winners_count int not null default 1,
  ends_at timestamptz,
  auto_draw boolean not null default false,
  auto_publish boolean not null default false,
  published_chat_id bigint,
  published_message_id bigint,
  winners_drawn_at timestamptz,
  results_published_at timestamptz,
  results_message_id bigint,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_giveaways_workspace on giveaways(workspace_id, created_at desc);
create index if not exists idx_giveaways_status on giveaways(status);

create table if not exists giveaway_sponsors (
  id bigserial primary key,
  giveaway_id bigint not null references giveaways(id) on delete cascade,
  position int not null,
  sponsor_text text not null
);

create index if not exists idx_gw_sponsors_gw on giveaway_sponsors(giveaway_id, position);

create table if not exists giveaway_entries (
  giveaway_id bigint not null references giveaways(id) on delete cascade,
  user_id bigint not null references users(id) on delete cascade,
  joined_at timestamptz not null default now(),
  is_eligible boolean not null default false,
  last_checked_at timestamptz,
  primary key (giveaway_id, user_id)
);

create index if not exists idx_gw_entries_gw on giveaway_entries(giveaway_id);
create index if not exists idx_gw_entries_eligible on giveaway_entries(giveaway_id, is_eligible);

create table if not exists giveaway_winners (
  id bigserial primary key,
  giveaway_id bigint not null references giveaways(id) on delete cascade,
  user_id bigint not null references users(id) on delete cascade,
  place int not null,
  created_at timestamptz not null default now(),
  unique(giveaway_id, place),
  unique(giveaway_id, user_id)
);

create table if not exists giveaway_audit (
  id bigserial primary key,
  giveaway_id bigint not null references giveaways(id) on delete cascade,
  workspace_id bigint not null references workspaces(id) on delete cascade,
  actor_user_id bigint references users(id) on delete set null,
  action text not null,
  payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_giveaway_audit_gw_time on giveaway_audit(giveaway_id, created_at desc);

-- Optional: network sponsors (future barters marketplace)
create table if not exists network_sponsors (
  id bigserial primary key,
  category text,
  title text not null,
  contact text,
  created_by_user_id bigint references users(id) on delete set null,
  created_at timestamptz not null default now()
);
