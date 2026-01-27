-- 022_profile_matrix_match_indexes.sql
-- Ускоряем overlap-запросы по text[] (profile_verticals / profile_formats)

create index if not exists idx_ws_settings_profile_verticals_gin
  on workspace_settings using gin (profile_verticals);

create index if not exists idx_ws_settings_profile_formats_gin
  on workspace_settings using gin (profile_formats);

create index if not exists idx_ws_settings_profile_mode_btree
  on workspace_settings (profile_mode);
