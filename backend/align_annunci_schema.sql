ALTER TABLE annunci
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();

ALTER TABLE annunci
ALTER COLUMN updated_at SET DEFAULT NOW();

UPDATE annunci
SET updated_at = COALESCE(updated_at, created_at);

CREATE INDEX IF NOT EXISTS idx_annunci_created_at_feed
ON annunci (created_at DESC);
