-- 017_analytics_events.sql
-- Minimal event log for product analytics / funnel tracking

create table if not exists events (
  id bigserial primary key,
  ts timestamptz not null default now(),
  user_id bigint null references users(id) on delete set null,
  ws_id bigint null references workspaces(id) on delete set null,
  name text not null,
  meta jsonb not null default '{}'::jsonb
);

create index if not exists events_name_ts_idx on events (name, ts desc);
create index if not exists events_ws_ts_idx on events (ws_id, ts desc);
create index if not exists events_user_ts_idx on events (user_id, ts desc);
