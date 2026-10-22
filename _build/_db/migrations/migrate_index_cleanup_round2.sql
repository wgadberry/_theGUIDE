-- ============================================================================
-- migrate_index_cleanup_round2.sql
-- Date: 2026-02-14
-- Purpose: Drop redundant indexes and FK constraints from 4 large tables
--
-- tx_guide:    Drop 7 indexes + 9 FKs  → save ~202 MB
-- tx_transfer: Drop 6 indexes + 9 FKs  → save ~157 MB
-- tx_activity:  Drop 4 indexes + 4 FKs  → save ~112 MB
-- tx_swap:      Drop 13 indexes + 14 FKs → save ~83 MB
-- Total savings: ~554 MB (before tablespace rebuild)
-- ============================================================================

-- ============================================================================
-- PART 1: tx_guide (662 MB indexes → ~460 MB)
-- ============================================================================

-- Drop 9 FK constraints
ALTER TABLE tx_guide DROP FOREIGN KEY tx_guide_ibfk_tx;
ALTER TABLE tx_guide DROP FOREIGN KEY tx_guide_ibfk_from;
ALTER TABLE tx_guide DROP FOREIGN KEY tx_guide_ibfk_to;
ALTER TABLE tx_guide DROP FOREIGN KEY tx_guide_ibfk_token;
ALTER TABLE tx_guide DROP FOREIGN KEY tx_guide_ibfk_from_ata;
ALTER TABLE tx_guide DROP FOREIGN KEY tx_guide_ibfk_to_ata;
ALTER TABLE tx_guide DROP FOREIGN KEY tx_guide_ibfk_pool;
ALTER TABLE tx_guide DROP FOREIGN KEY tx_guide_ibfk_edge_type;
ALTER TABLE tx_guide DROP FOREIGN KEY tx_guide_ibfk_source;

-- idx_tx (32 MB) — prefix of uq_guide_edge (tx_id is leading column)
ALTER TABLE tx_guide DROP INDEX idx_tx;

-- idx_token (24 MB) — prefix of idx_token_blocktime (token_id, block_time)
ALTER TABLE tx_guide DROP INDEX idx_token;

-- idx_guide_txid_pool (44 MB) — no query uses (tx_id, pool_address_id) composite
ALTER TABLE tx_guide DROP INDEX idx_guide_txid_pool;

-- tx_guide_ibfk_pool (27 MB) — FK-only index on pool_address_id, bmap uses tmp tables
ALTER TABLE tx_guide DROP INDEX tx_guide_ibfk_pool;

-- idx_from_ata (29 MB) — FK-only index on from_token_account_id, no query filters on this
ALTER TABLE tx_guide DROP INDEX idx_from_ata;

-- idx_to_ata (27 MB) — FK-only index on to_token_account_id, no query filters on this
ALTER TABLE tx_guide DROP INDEX idx_to_ata;

-- idx_edge_type (19 MB) — FK-only index on edge_type_id, always filtered with tx_id first
ALTER TABLE tx_guide DROP INDEX idx_edge_type;

-- REMAINING indexes (10):
--   PRIMARY (id)
--   uq_guide_edge UNIQUE (tx_id, from_address_id, to_address_id, token_id, amount, edge_type_id, ins_index)
--   idx_from_token_time (from_address_id, token_id, block_time, from_token_post_balance)
--   idx_to_token_time (to_address_id, token_id, block_time, to_token_post_balance)
--   idx_token_blocktime (token_id, block_time)
--   idx_guide_txid_token (tx_id, token_id, source_id, edge_type_id)
--   idx_from_time (from_address_id, block_time)
--   idx_to_time (to_address_id, block_time)
--   idx_block_time (block_time)
--   idx_source (source_id, source_row_id)

-- ============================================================================
-- PART 2: tx_transfer (368 MB indexes → ~211 MB)
-- ============================================================================

-- Drop 9 FK constraints
ALTER TABLE tx_transfer DROP FOREIGN KEY tx_transfer_ibfk_tx;
ALTER TABLE tx_transfer DROP FOREIGN KEY tx_transfer_ibfk_source;
ALTER TABLE tx_transfer DROP FOREIGN KEY tx_transfer_ibfk_source_owner;
ALTER TABLE tx_transfer DROP FOREIGN KEY tx_transfer_ibfk_dest;
ALTER TABLE tx_transfer DROP FOREIGN KEY tx_transfer_ibfk_dest_owner;
ALTER TABLE tx_transfer DROP FOREIGN KEY tx_transfer_ibfk_token;
ALTER TABLE tx_transfer DROP FOREIGN KEY tx_transfer_ibfk_base_token;
ALTER TABLE tx_transfer DROP FOREIGN KEY tx_transfer_ibfk_program;
ALTER TABLE tx_transfer DROP FOREIGN KEY tx_transfer_ibfk_outer_program;

-- tx_transfer_ibfk_source (31 MB) — FK index on source_address_id; queries use source_owner_address_id
ALTER TABLE tx_transfer DROP INDEX tx_transfer_ibfk_source;

-- tx_transfer_ibfk_dest (29 MB) — FK index on destination_address_id; queries use dest_owner_address_id
ALTER TABLE tx_transfer DROP INDEX tx_transfer_ibfk_dest;

-- idx_token (26 MB) — always filtered with tx_id first, covered by idx_transfer_tx_type
ALTER TABLE tx_transfer DROP INDEX idx_token;

-- tx_transfer_ibfk_program (25 MB) — FK-only index on program_id
ALTER TABLE tx_transfer DROP INDEX tx_transfer_ibfk_program;

-- tx_transfer_ibfk_outer_program (25 MB) — FK-only index on outer_program_id
ALTER TABLE tx_transfer DROP INDEX tx_transfer_ibfk_outer_program;

-- tx_transfer_ibfk_base_token (21 MB) — FK-only index on base_token_id
ALTER TABLE tx_transfer DROP INDEX tx_transfer_ibfk_base_token;

-- REMAINING indexes (6):
--   PRIMARY (id)
--   idx_transfer_tx_type (tx_id, transfer_type, source_owner_address_id, dest_owner_address_id)
--   uk_tx_ins UNIQUE (tx_id, ins_index, outer_ins_index)
--   idx_source_owner (source_owner_address_id)
--   idx_dest_owner (destination_owner_address_id)
--   idx_transfer_activity (activity_id)

-- ============================================================================
-- PART 3: tx_activity (231 MB indexes → ~119 MB)
-- ============================================================================

-- Drop 4 FK constraints
ALTER TABLE tx_activity DROP FOREIGN KEY tx_activity_ibfk_tx;
ALTER TABLE tx_activity DROP FOREIGN KEY tx_activity_ibfk_account;
ALTER TABLE tx_activity DROP FOREIGN KEY tx_activity_ibfk_program;
ALTER TABLE tx_activity DROP FOREIGN KEY tx_activity_ibfk_outer_program;

-- idx_activity_type (42 MB) — always filtered with tx_id first via idx_activity_tx
ALTER TABLE tx_activity DROP INDEX idx_activity_type;

-- idx_program (25 MB) — FK-only index on program_id
ALTER TABLE tx_activity DROP INDEX idx_program;

-- idx_account (25 MB) — FK-only index on account_address_id
ALTER TABLE tx_activity DROP INDEX idx_account;

-- tx_activity_ibfk_outer_program (20 MB) — FK-only index on outer_program_id
ALTER TABLE tx_activity DROP INDEX tx_activity_ibfk_outer_program;

-- REMAINING indexes (3):
--   PRIMARY (id)
--   idx_activity_tx (tx_id, activity_type, account_address_id)
--   uk_tx_ins UNIQUE (tx_id, ins_index, outer_ins_index)

-- ============================================================================
-- PART 4: tx_swap (113 MB indexes → ~30 MB)
-- ============================================================================

-- Drop 14 FK constraints
ALTER TABLE tx_swap DROP FOREIGN KEY tx_swap_ibfk_tx;
ALTER TABLE tx_swap DROP FOREIGN KEY tx_swap_ibfk_account;
ALTER TABLE tx_swap DROP FOREIGN KEY tx_swap_ibfk_amm;
ALTER TABLE tx_swap DROP FOREIGN KEY tx_swap_ibfk_token_1;
ALTER TABLE tx_swap DROP FOREIGN KEY tx_swap_ibfk_token_2;
ALTER TABLE tx_swap DROP FOREIGN KEY tx_swap_ibfk_owner_1;
ALTER TABLE tx_swap DROP FOREIGN KEY tx_swap_ibfk_owner_2;
ALTER TABLE tx_swap DROP FOREIGN KEY tx_swap_ibfk_ta_1_1;
ALTER TABLE tx_swap DROP FOREIGN KEY tx_swap_ibfk_ta_1_2;
ALTER TABLE tx_swap DROP FOREIGN KEY tx_swap_ibfk_ta_2_1;
ALTER TABLE tx_swap DROP FOREIGN KEY tx_swap_ibfk_ta_2_2;
ALTER TABLE tx_swap DROP FOREIGN KEY tx_swap_ibfk_program;
ALTER TABLE tx_swap DROP FOREIGN KEY tx_swap_ibfk_outer_program;
ALTER TABLE tx_swap DROP FOREIGN KEY tx_swap_ibfk_fee_token;

-- idx_tx_token1_amt1 (9 MB) — exact duplicate of idx_swap_tx_token1
ALTER TABLE tx_swap DROP INDEX idx_tx_token1_amt1;

-- idx_account (7 MB) — FK-only index on account_address_id
ALTER TABLE tx_swap DROP INDEX idx_account;

-- idx_amm (7 MB) — FK-only index on amm_id
ALTER TABLE tx_swap DROP INDEX idx_amm;

-- idx_token_1 (6 MB) — FK-only, queries always filter tx_id first
ALTER TABLE tx_swap DROP INDEX idx_token_1;

-- idx_token_2 (6 MB) — FK-only, queries always filter tx_id first
ALTER TABLE tx_swap DROP INDEX idx_token_2;

-- tx_swap_ibfk_owner_1 (7 MB) — FK-only index on owner_1_address_id
ALTER TABLE tx_swap DROP INDEX tx_swap_ibfk_owner_1;

-- tx_swap_ibfk_owner_2 (5 MB) — FK-only index on owner_2_address_id
ALTER TABLE tx_swap DROP INDEX tx_swap_ibfk_owner_2;

-- tx_swap_ibfk_ta_1_1 (7 MB) — FK-only index on token_account_1_1_address_id
ALTER TABLE tx_swap DROP INDEX tx_swap_ibfk_ta_1_1;

-- tx_swap_ibfk_ta_1_2 (6 MB) — FK-only index on token_account_1_2_address_id
ALTER TABLE tx_swap DROP INDEX tx_swap_ibfk_ta_1_2;

-- tx_swap_ibfk_ta_2_1 (6 MB) — FK-only index on token_account_2_1_address_id
ALTER TABLE tx_swap DROP INDEX tx_swap_ibfk_ta_2_1;

-- tx_swap_ibfk_ta_2_2 (7 MB) — FK-only index on token_account_2_2_address_id
ALTER TABLE tx_swap DROP INDEX tx_swap_ibfk_ta_2_2;

-- tx_swap_ibfk_program (6 MB) — FK-only index on program_id
ALTER TABLE tx_swap DROP INDEX tx_swap_ibfk_program;

-- tx_swap_ibfk_outer_program (5 MB) — FK-only index on outer_program_id
ALTER TABLE tx_swap DROP INDEX tx_swap_ibfk_outer_program;

-- tx_swap_ibfk_fee_token (5 MB) — FK-only index on fee_token_id
ALTER TABLE tx_swap DROP INDEX tx_swap_ibfk_fee_token;

-- REMAINING indexes (5):
--   PRIMARY (id)
--   idx_swap_tx_token1 (tx_id, token_1_id, amount_1)
--   idx_swap_tx_token2 (tx_id, token_2_id, amount_2)
--   idx_swap_activity (activity_id)
--   uk_tx_ins UNIQUE (tx_id, ins_index, outer_ins_index)
