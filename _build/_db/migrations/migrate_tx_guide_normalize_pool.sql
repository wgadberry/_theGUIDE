-- Migration: Normalize tx_guide pool columns
-- Replace dex, pool_address_id, pool_label with pool_id FK to tx_pool.id
-- Saves ~91MB data + index overhead per 1M rows
--
-- Prerequisites: tx_pool.dex must be populated (Step 1 backfills gaps)
-- Rollback: pool_address_id, dex, pool_label can be derived from tx_pool via pool_id

-- ============================================================
-- Step 1: Backfill tx_pool.dex from tx_guide where missing
-- ============================================================
UPDATE tx_pool p
JOIN (
    SELECT DISTINCT g.pool_address_id, g.dex
    FROM tx_guide g
    WHERE g.pool_address_id IS NOT NULL AND g.dex IS NOT NULL
) src ON src.pool_address_id = p.pool_address_id
SET p.dex = src.dex
WHERE p.dex IS NULL;
-- Expected: ~630 rows

-- ============================================================
-- Step 2: Add pool_id column to tx_guide
-- ============================================================
ALTER TABLE tx_guide
    ADD COLUMN pool_id BIGINT UNSIGNED DEFAULT NULL
    COMMENT 'FK to tx_pool.id — replaces pool_address_id, dex, pool_label'
    AFTER pool_address_id;

-- ============================================================
-- Step 3: Populate pool_id from existing pool_address_id
-- ============================================================
UPDATE tx_guide g
JOIN tx_pool p ON p.pool_address_id = g.pool_address_id
SET g.pool_id = p.id
WHERE g.pool_address_id IS NOT NULL;
-- Expected: ~579K rows

-- ============================================================
-- Step 4: Add index on pool_id
-- ============================================================
ALTER TABLE tx_guide ADD INDEX idx_pool_id (pool_id);

-- ============================================================
-- Step 5: Add FK constraint
-- ============================================================
ALTER TABLE tx_guide
    ADD CONSTRAINT tx_guide_ibfk_pool_id FOREIGN KEY (pool_id) REFERENCES tx_pool (id);

-- ============================================================
-- Step 6: Drop old columns (AFTER verifying Step 3 worked)
-- Run these ONLY after confirming pool_id is fully populated:
--   SELECT COUNT(*) FROM tx_guide WHERE pool_address_id IS NOT NULL AND pool_id IS NULL;
--   (should return 0)
-- ============================================================
-- ALTER TABLE tx_guide
--     DROP FOREIGN KEY tx_guide_ibfk_pool,
--     DROP INDEX idx_pool_address,
--     DROP COLUMN pool_address_id,
--     DROP COLUMN dex,
--     DROP COLUMN pool_label;
