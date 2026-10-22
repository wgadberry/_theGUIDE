-- ============================================================================
-- migrate_bmap_indexes.sql
-- Date: 2026-02-14
-- Purpose: Add missing indexes to tx_guide for sp_tx_bmap_get performance
--
-- These indexes were never created but are referenced in SP comments.
-- SP also updated to use g.block_time instead of t.block_time so the
-- optimizer can use idx_token_blocktime for sliding window queries.
-- ============================================================================

-- Sliding window prev/next + tx resolution (ORDER BY g.block_time DESC/ASC LIMIT 5)
ALTER TABLE tx_guide ADD INDEX idx_token_blocktime (token_id, block_time);

-- Token balance FROM side (covering index includes from_token_post_balance)
ALTER TABLE tx_guide ADD INDEX idx_from_token_time (from_address_id, token_id, block_time, from_token_post_balance);

-- Token balance TO side (covering index includes to_token_post_balance)
ALTER TABLE tx_guide ADD INDEX idx_to_token_time (to_address_id, token_id, block_time, to_token_post_balance);
