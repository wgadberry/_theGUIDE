-- ============================================================================
-- migrate_balance_index_cleanup.sql
-- Date: 2026-02-14
-- Purpose: Drop redundant indexes and FK constraints from balance change tables
--
-- tx_sol_balance_change: Drop 2 indexes + 2 FKs  → saves ~474 MB
-- tx_token_balance_change: Drop 5 indexes + 4 FKs → saves ~441 MB
-- Total savings: ~915 MB
-- ============================================================================

-- ============================================================================
-- PART 1: tx_sol_balance_change
-- ============================================================================

-- Drop FK constraints first (they depend on indexes)
ALTER TABLE tx_sol_balance_change DROP FOREIGN KEY fk_sol_bal_tx;
ALTER TABLE tx_sol_balance_change DROP FOREIGN KEY fk_sol_bal_address;

-- idx_sbc_tx_addr (329 MB) — covering extension of idx_tx_address (UNIQUE)
-- Same leading columns (tx_id, address_id) + pre/post_balance for covering reads.
-- Table fetch after idx_tx_address lookup is fine for 3.7M rows.
ALTER TABLE tx_sol_balance_change DROP INDEX idx_sbc_tx_addr;

-- idx_sbc_addr_tx (145 MB) — (address_id, tx_id)
-- Redundant: idx_sbc_addr_blocktime already leads with address_id and covers
-- all LATERAL prior-tx and bmap lookups. No query needs (address_id, tx_id).
ALTER TABLE tx_sol_balance_change DROP INDEX idx_sbc_addr_tx;

-- REMAINING indexes (3):
--   PRIMARY (id)
--   idx_tx_address UNIQUE (tx_id, address_id)        — INSERT IGNORE dedup + same-tx joins
--   idx_sbc_addr_blocktime (address_id, block_time DESC, post_balance) — LATERAL prior-tx

-- ============================================================================
-- PART 2: tx_token_balance_change
-- ============================================================================

-- Drop FK constraints first
ALTER TABLE tx_token_balance_change DROP FOREIGN KEY fk_token_bal_tx;
ALTER TABLE tx_token_balance_change DROP FOREIGN KEY fk_token_bal_account;
ALTER TABLE tx_token_balance_change DROP FOREIGN KEY fk_token_bal_owner;
ALTER TABLE tx_token_balance_change DROP FOREIGN KEY fk_token_bal_token;

-- idx_tbc_tx_token_acct (142 MB) — (tx_id, token_id, token_account_address_id, pre_balance, post_balance)
-- Redundant: guide_loader same-tx JOIN is on (tx_id, token_id). idx_tx_owner_token
-- already starts with (tx_id, ...) and idx_tbc_tx_token_owner covers (tx_id, token_id).
ALTER TABLE tx_token_balance_change DROP INDEX idx_tbc_tx_token_acct;

-- idx_tbc_tx_token_owner (145 MB) — (tx_id, token_id, owner_address_id, pre_balance, post_balance)
-- Redundant: same-tx JOIN on (tx_id, token_id) covered by idx_tx_owner_token (tx_id, owner_address_id, token_id).
-- Owner fallback WHERE uses owner_address_id which is col 2 in idx_tx_owner_token.
ALTER TABLE tx_token_balance_change DROP INDEX idx_tbc_tx_token_owner;

-- idx_tbc_acct_token_tx (62 MB) — (token_account_address_id, token_id, tx_id)
-- Redundant: idx_tbc_acct_token_blocktime covers same leading columns
-- (token_account_address_id, token_id) with block_time + post_balance for LATERAL.
ALTER TABLE tx_token_balance_change DROP INDEX idx_tbc_acct_token_tx;

-- idx_tbc_owner_token_tx (62 MB) — (owner_address_id, token_id, tx_id)
-- Redundant: idx_tbc_owner_token_blocktime covers same leading columns
-- (owner_address_id, token_id) with block_time + post_balance for LATERAL.
ALTER TABLE tx_token_balance_change DROP INDEX idx_tbc_owner_token_tx;

-- idx_token (30 MB) — (token_id)
-- No query filters on token_id alone. FK fk_token_bal_token already dropped above.
ALTER TABLE tx_token_balance_change DROP INDEX idx_token;

-- REMAINING indexes (5):
--   PRIMARY (id)
--   idx_tx_token_account UNIQUE (tx_id, token_account_address_id)                              — INSERT IGNORE dedup
--   idx_tx_owner_token (tx_id, owner_address_id, token_id)                                     — same-tx owner fallback
--   idx_tbc_acct_token_blocktime (token_account_address_id, token_id, block_time DESC, post_balance) — LATERAL prior-tx
--   idx_tbc_owner_token_blocktime (owner_address_id, token_id, block_time DESC, post_balance)        — LATERAL prior-tx
