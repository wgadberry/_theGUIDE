-- ================================================================
-- Migration: Add block_time to balance change tables
--
-- Denormalizes tx.block_time into tx_sol_balance_change and
-- tx_token_balance_change so the guide loader's prior-tx LATERAL
-- lookups can filter + order on a local column without JOINing tx.
--
-- Before: LATERAL scans ALL sbc rows per address (up to 131K),
--         JOINs each to tx for block_time, sorts, takes top 1.
-- After:  Single index seek per guide row — O(log N), no JOIN.
-- ================================================================

-- Step 1: Add block_time column to both tables
ALTER TABLE tx_sol_balance_change
  ADD COLUMN block_time BIGINT UNSIGNED NULL AFTER tx_id;

ALTER TABLE tx_token_balance_change
  ADD COLUMN block_time BIGINT UNSIGNED NULL AFTER tx_id;

-- Step 2: Populate from tx.block_time
UPDATE tx_sol_balance_change sbc
JOIN tx t ON t.id = sbc.tx_id
SET sbc.block_time = t.block_time
WHERE sbc.block_time IS NULL;

UPDATE tx_token_balance_change tbc
JOIN tx t ON t.id = tbc.tx_id
SET tbc.block_time = t.block_time
WHERE tbc.block_time IS NULL;

-- Step 3: Add composite indexes for LATERAL prior-tx lookups
-- SOL: (address_id, block_time DESC) — covers the SOL prior LATERAL
ALTER TABLE tx_sol_balance_change
  ADD INDEX idx_sbc_addr_blocktime (address_id, block_time DESC, post_balance);

-- TOKEN: two indexes for the split-OR pattern
-- Pass 1: token_account match
ALTER TABLE tx_token_balance_change
  ADD INDEX idx_tbc_acct_token_blocktime (token_account_address_id, token_id, block_time DESC, post_balance);

-- Pass 2: owner fallback
ALTER TABLE tx_token_balance_change
  ADD INDEX idx_tbc_owner_token_blocktime (owner_address_id, token_id, block_time DESC, post_balance);
