-- Barters Marketplace "ideal" layer (v0.9.2)

-- bump counters
alter table barter_offers add column if not exists bump_count int not null default 0;

-- Inbox / mini-deals (brand <-> blogger)
create table if not exists barter_threads (
  id bigserial primary key,
  offer_id bigint not null references barter_offers(id) on delete cascade,
  workspace_id bigint not null references workspaces(id) on delete cascade,
  buyer_user_id bigint not null references users(id) on delete cascade,
  seller_user_id bigint not null references users(id) on delete cascade,
  status text not null default 'OPEN', -- OPEN | CLOSED
  last_message_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (offer_id, buyer_user_id)
);

create index if not exists idx_barter_threads_user_time on barter_threads(buyer_user_id, last_message_at desc);
create index if not exists idx_barter_threads_seller_time on barter_threads(seller_user_id, last_message_at desc);

create table if not exists barter_messages (
  id bigserial primary key,
  thread_id bigint not null references barter_threads(id) on delete cascade,
  sender_user_id bigint not null references users(id) on delete cascade,
  body text not null,
  created_at timestamptz not null default now()
);

create index if not exists idx_barter_messages_thread_time on barter_messages(thread_id, created_at desc);
