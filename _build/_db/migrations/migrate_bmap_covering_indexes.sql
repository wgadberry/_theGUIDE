-- migrate_bmap_covering_indexes.sql
-- Extend idx_from_time and idx_to_time on tx_guide with sol_post_balance
-- as covering columns for sp_tx_bmap_get SOL balance lookups.
--
-- Before: GROUP BY + MAX(block_time) WHERE from_sol_post_balance IS NOT NULL
--         required row fetches for every index entry to check NULL filter (800ms)
-- After:  IS NOT NULL check resolved at index level (180ms)
--
-- Index size increase: ~59MB each → ~77MB each (+36MB total)

ALTER TABLE tx_guide
  DROP INDEX idx_from_time,
  ADD INDEX idx_from_time (from_address_id, block_time, from_sol_post_balance),
  DROP INDEX idx_to_time,
  ADD INDEX idx_to_time (to_address_id, block_time, to_sol_post_balance);
