-- Profile Matrix v1 (IG leads -> TG deals)
-- Structured niches/formats + IG + portfolio on workspace profile.

alter table workspace_settings add column if not exists profile_mode text not null default 'both';
alter table workspace_settings add column if not exists profile_ig text;
alter table workspace_settings add column if not exists profile_verticals text[] not null default '{}'::text[];
alter table workspace_settings add column if not exists profile_formats text[] not null default '{}'::text[];
alter table workspace_settings add column if not exists profile_portfolio_urls text[] not null default '{}'::text[];
alter table workspace_settings add column if not exists profile_about text;

-- Enum-ish check for mode
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname='workspace_settings_profile_mode_chk'
  ) THEN
    ALTER TABLE workspace_settings
      ADD CONSTRAINT workspace_settings_profile_mode_chk
      CHECK (profile_mode in ('channel','ugc','both'));
  END IF;
END$$;

-- Helpful indexes
create index if not exists idx_ws_settings_profile_ig on workspace_settings (lower(profile_ig))
  where profile_ig is not null and profile_ig <> '';

create index if not exists gin_ws_settings_profile_verticals on workspace_settings using gin (profile_verticals);
create index if not exists gin_ws_settings_profile_formats on workspace_settings using gin (profile_formats);
create index if not exists gin_ws_settings_profile_portfolio_urls on workspace_settings using gin (profile_portfolio_urls);
