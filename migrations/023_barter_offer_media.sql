-- 023_barter_offer_media.sql
-- Add media fields for barter offers (photo / animation(gif) / video)

ALTER TABLE barter_offers
  ADD COLUMN IF NOT EXISTS media_type TEXT,
  ADD COLUMN IF NOT EXISTS media_file_id TEXT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'barter_offers_media_type_chk'
  ) THEN
    ALTER TABLE barter_offers
      ADD CONSTRAINT barter_offers_media_type_chk
      CHECK (media_type IS NULL OR media_type IN ('photo', 'animation', 'video'));
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_barter_offers_media_type ON barter_offers(media_type);
