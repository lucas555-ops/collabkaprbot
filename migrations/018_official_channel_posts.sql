-- 018_official_channel_posts.sql
-- Official channel publishing for barter offers (paid or manual placements)

create table if not exists official_posts (
  id bigserial primary key,
  offer_id bigint not null references barter_offers(id) on delete cascade,
  channel_chat_id bigint not null,
  message_id bigint null,
  status text not null default 'PENDING',
  placement_type text not null default 'MANUAL',
  payment_id bigint null references payments(id) on delete set null,
  slot_days int null,
  slot_expires_at timestamptz null,
  published_by_user_id bigint null references users(id) on delete set null,
  last_error text null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists official_posts_offer_id_uniq on official_posts (offer_id);
create index if not exists official_posts_status_idx on official_posts (status, updated_at desc);
create index if not exists official_posts_expires_idx on official_posts (slot_expires_at) where slot_expires_at is not null;
