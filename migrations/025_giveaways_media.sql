-- 025_giveaways_media.sql
-- Adds optional media (photo/gif/video) to giveaways.

alter table if exists giveaways
  add column if not exists media_type text,
  add column if not exists media_file_id text;

create index if not exists idx_giveaways_media_type on giveaways(media_type);
