-- theGuide Database Build Script
-- Generated from t16o_db instance
--
-- This script creates all database objects in dependency order:
-- 1. Tables (base tables first, then dependent tables)
-- 2. Functions
-- 3. Stored Procedures
-- 4. Views
--
-- Usage:
--   mysql -h localhost -P 3396 -u root -p t16o_db < build-all.sql
--   OR run via 02-create-schema.py

-- =============================================================================
-- TABLES (23 total) - Order matters due to foreign key dependencies
-- =============================================================================

-- Base tables (no foreign keys)
SOURCE tables/config.sql;
SOURCE tables/tx_address.sql;
SOURCE tables/tx_guide_type.sql;
SOURCE tables/tx_guide_source.sql;

-- Tables with single FK dependency
SOURCE tables/tx_token.sql;
SOURCE tables/tx_program.sql;
SOURCE tables/tx_pool.sql;

-- Main transaction table
SOURCE tables/tx.sql;

-- Transaction detail tables
SOURCE tables/tx_account.sql;
SOURCE tables/tx_activity.sql;
SOURCE tables/tx_funding_edge.sql;
SOURCE tables/tx_guide.sql;
SOURCE tables/tx_instruction.sql;
SOURCE tables/tx_party.sql;
SOURCE tables/tx_signer.sql;
SOURCE tables/tx_sol_balance_change.sql;
SOURCE tables/tx_swap.sql;
SOURCE tables/tx_token_balance_change.sql;
SOURCE tables/tx_token_holder.sql;
SOURCE tables/tx_token_market.sql;
SOURCE tables/tx_token_participant.sql;
SOURCE tables/tx_token_price.sql;
SOURCE tables/tx_transfer.sql;

-- =============================================================================
-- FUNCTIONS (8 total)
-- =============================================================================

SOURCE functions/fn_tx_ensure_address.sql;
SOURCE functions/fn_tx_ensure_token.sql;
SOURCE functions/fn_tx_ensure_program.sql;
SOURCE functions/fn_tx_ensure_pool.sql;
SOURCE functions/fn_tx_extract_addresses.sql;
SOURCE functions/fn_tx_get_token_name.sql;
SOURCE functions/fn_get_guide_by_wallet.sql;
SOURCE functions/fn_get_guide_by_token.sql;

-- =============================================================================
-- STORED PROCEDURES (14 total)
-- =============================================================================

SOURCE procedures/sp_config_get.sql;
SOURCE procedures/sp_config_get_by_type.sql;
SOURCE procedures/sp_config_get_changes.sql;
SOURCE procedures/sp_config_set.sql;
SOURCE procedures/sp_detect_chart_clipping.sql;
SOURCE procedures/sp_tx_backfill_funding.sql;
SOURCE procedures/sp_tx_clear_tables.sql;
SOURCE procedures/sp_tx_detect_chart_clipping.sql;
SOURCE procedures/sp_tx_funding_chain.sql;
SOURCE procedures/sp_tx_hound_indexes.sql;
SOURCE procedures/sp_tx_prime.sql;
SOURCE procedures/sp_tx_prime_batch.sql;
SOURCE procedures/sp_tx_release_hound.sql;
SOURCE procedures/sp_tx_shred_batch.sql;

-- =============================================================================
-- VIEWS (13 total)
-- =============================================================================

SOURCE views/vw_tx_token_info.sql;
SOURCE views/vw_tx_funding_tree.sql;
SOURCE views/vw_tx_funding_chain.sql;
SOURCE views/vw_tx_common_funders.sql;
SOURCE views/vw_tx_flow_concentration.sql;
SOURCE views/vw_tx_address_risk_score.sql;
SOURCE views/vw_tx_high_freq_pairs.sql;
SOURCE views/vw_tx_high_freq_pairs2.sql;
SOURCE views/vw_tx_high_freq_pairs3.sql;
SOURCE views/vw_tx_rapid_fire.sql;
SOURCE views/vw_tx_sybil_clusters.sql;
SOURCE views/vw_tx_wash_roundtrip.sql;
SOURCE views/vw_tx_wash_triangle.sql;

-- =============================================================================
-- BUILD COMPLETE
-- =============================================================================
SELECT 'theGuide database build complete!' AS status;
SELECT
    (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 't16o_db' AND table_type = 'BASE TABLE') AS tables_count,
    (SELECT COUNT(*) FROM information_schema.routines WHERE routine_schema = 't16o_db' AND routine_type = 'FUNCTION') AS functions_count,
    (SELECT COUNT(*) FROM information_schema.routines WHERE routine_schema = 't16o_db' AND routine_type = 'PROCEDURE') AS procedures_count,
    (SELECT COUNT(*) FROM information_schema.views WHERE table_schema = 't16o_db') AS views_count;
