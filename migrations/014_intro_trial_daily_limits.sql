-- Intro pricing + trial credits + daily limits (v1.1.9)

-- One-time trial for brands (gift intro credits)
alter table users
  add column if not exists brand_trial_granted boolean not null default false;

alter table users
  add column if not exists brand_trial_granted_at timestamptz;

-- Daily intro usage (anti-spam / fairness)
create table if not exists intro_daily_usage (
  user_id bigint not null references users(id) on delete cascade,
  day date not null,
  used_count int not null default 0,
  updated_at timestamptz not null default now(),
  primary key (user_id, day)
);

create index if not exists idx_intro_daily_usage_day on intro_daily_usage(day desc);
