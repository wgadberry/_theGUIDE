-- Migration: Add cached parsed dex/token_pair columns to tx_pool
-- Eliminates repeated fn_parse_dex()/fn_parse_token_pair() UDF calls in
-- sp_tx_pool_backfill Step 3 (was 4.9s per call on 10K rows)
--
-- Run once, then deploy updated sp_tx_pool_backfill

-- 1. Add columns
ALTER TABLE tx_pool
    ADD COLUMN dex        VARCHAR(100) DEFAULT NULL COMMENT 'Cached fn_parse_dex(pool_label) — DEX name' AFTER pool_label,
    ADD COLUMN token_pair VARCHAR(100) DEFAULT NULL COMMENT 'Cached fn_parse_token_pair(pool_label) — e.g. SOL-USDC' AFTER dex;

-- 2. Backfill from existing pool_label (one-time, ~36K rows)
UPDATE tx_pool
SET dex        = fn_parse_dex(pool_label),
    token_pair = fn_parse_token_pair(pool_label)
WHERE pool_label IS NOT NULL
  AND (dex IS NULL OR token_pair IS NULL);

-- 3. Index for Step 3 join (Market source → target by dex+pair)
ALTER TABLE tx_pool ADD INDEX idx_dex_pair (dex, token_pair);
