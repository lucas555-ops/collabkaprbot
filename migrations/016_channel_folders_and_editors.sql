-- Channel folders (workspace-level reusable lists of @channels)
-- Use-case: joint contests / partner lists that can be reused in giveaways & barter offers.

create table if not exists channel_folders (
  id bigserial primary key,
  workspace_id bigint not null references workspaces(id) on delete cascade,
  created_by_user_id bigint not null references users(id),
  title text not null,
  created_at timestamptz not null default now()
);

create index if not exists idx_channel_folders_workspace_id on channel_folders(workspace_id);
create unique index if not exists uniq_channel_folders_workspace_title on channel_folders(workspace_id, lower(title));

create table if not exists channel_folder_items (
  id bigserial primary key,
  folder_id bigint not null references channel_folders(id) on delete cascade,
  channel_username text not null,
  created_at timestamptz not null default now()
);

create index if not exists idx_channel_folder_items_folder_id on channel_folder_items(folder_id);
create unique index if not exists uniq_channel_folder_items_folder_username on channel_folder_items(folder_id, lower(channel_username));

create table if not exists workspace_editors (
  workspace_id bigint not null references workspaces(id) on delete cascade,
  user_id bigint not null references users(id) on delete cascade,
  added_by_user_id bigint not null references users(id),
  created_at timestamptz not null default now(),
  primary key (workspace_id, user_id)
);

create index if not exists idx_workspace_editors_user_id on workspace_editors(user_id);

alter table barter_offers
  add column if not exists partner_folder_id bigint references channel_folders(id) on delete set null;

create index if not exists idx_barter_offers_partner_folder_id on barter_offers(partner_folder_id);
