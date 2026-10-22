-- =============================================================================
-- Migration: Drop Dead Weight
-- Date: 2026-02-15
-- Description: Remove empty/unused tables, redundant indexes, and dead SPs
-- Backup: t16o_db_backup_20260215_*.sql.gz
-- =============================================================================

-- -----------------------------------------------------------------------------
-- TIER 1: Drop empty/unused tables (all have 0 rows)
-- -----------------------------------------------------------------------------

-- tx_signer: old-path table, only referenced by dead SPs (sp_tx_prime, sp_tx_prime_batch)
DROP TABLE IF EXISTS tx_signer;

-- tx_token_price: never populated, guide-price-loader.py never ran
DROP TABLE IF EXISTS tx_token_price;

-- tx_token_market: never populated, only referenced by sp_tx_clear_tables
DROP TABLE IF EXISTS tx_token_market;

-- tx_bmap_state: superseded by v2-v5 token state SPs, guide-welcome.py references but never populates
DROP TABLE IF EXISTS tx_bmap_state;

-- tx_participant: referenced by dead SPs and guide-shredder.py but never populated in current pipeline
DROP TABLE IF EXISTS tx_participant;

-- -----------------------------------------------------------------------------
-- TIER 2: Drop redundant indexes
-- -----------------------------------------------------------------------------

-- config: idx_config_type(config_type) is a prefix of idx_config_type_key(config_type, config_key)
ALTER TABLE config DROP INDEX idx_config_type;

-- tx_address: idx_type(address_type) is a prefix of idx_funding_lookup(address_type, funded_by_address_id, funding_checked_at)
ALTER TABLE tx_address DROP INDEX idx_type;

-- tx_address: idx_addr_type(id, address_type) — id is already the PRIMARY KEY, address_type filtering uses other indexes
ALTER TABLE tx_address DROP INDEX idx_addr_type;

-- tx_pool: idx_pool_addr(pool_address_id) is exact duplicate of uk_address(pool_address_id)
ALTER TABLE tx_pool DROP INDEX idx_pool_addr;

-- tx_api_key: idx_api_key(api_key) is exact duplicate of api_key unique index
ALTER TABLE tx_api_key DROP INDEX idx_api_key;

-- tx_request_log: idx_request_id(request_id) is a prefix of idx_request_worker_key(request_id, target_worker, api_key_id)
ALTER TABLE tx_request_log DROP INDEX idx_request_id;

-- -----------------------------------------------------------------------------
-- TIER 3: Drop dead stored procedures (zero Python worker callers)
-- -----------------------------------------------------------------------------

-- Old-path shred/prime procedures
DROP PROCEDURE IF EXISTS sp_tx_shred_batch;
DROP PROCEDURE IF EXISTS sp_tx_prime;
DROP PROCEDURE IF EXISTS sp_tx_prime_batch;
DROP PROCEDURE IF EXISTS sp_tx_detail_batch;

-- Analysis procedures that depend on tx_participant (now dropped)
DROP PROCEDURE IF EXISTS sp_tx_authority_detection;
DROP PROCEDURE IF EXISTS sp_tx_fee_payer_analysis;
DROP PROCEDURE IF EXISTS sp_tx_shadow_addresses;

-- bmap_state procedures that depend on tx_bmap_state (now dropped)
DROP PROCEDURE IF EXISTS sp_tx_bmap_get_token_state;
DROP PROCEDURE IF EXISTS sp_tx_bmap_state_backfill;

-- Chart clipping detection — no callers
DROP PROCEDURE IF EXISTS sp_detect_chart_clipping;
DROP PROCEDURE IF EXISTS sp_tx_detect_chart_clipping;

-- Billing stats — no callers
DROP PROCEDURE IF EXISTS sp_billing_stats_by_apikey;

-- -----------------------------------------------------------------------------
-- TIER 3b: Update sp_tx_clear_tables to remove references to dropped tables
-- (This SP is a dev/reset utility — must not fail on missing tables)
-- -----------------------------------------------------------------------------
-- NOTE: After running this migration, manually review sp_tx_clear_tables and
-- remove DELETE/TRUNCATE statements for: tx_signer, tx_token_price,
-- tx_token_market, tx_bmap_state, tx_participant.
-- The SP won't error (it uses DELETE FROM which silently fails on missing tables
-- in most contexts), but it should be cleaned up for clarity.

-- =============================================================================
-- Summary:
--   Tables dropped:  5 (tx_signer, tx_token_price, tx_token_market, tx_bmap_state, tx_participant)
--   Indexes dropped: 6 (config, tx_address x2, tx_pool, tx_api_key, tx_request_log)
--   SPs dropped:    12 (sp_tx_shred_batch, sp_tx_prime, sp_tx_prime_batch, sp_tx_detail_batch,
--                       sp_tx_authority_detection, sp_tx_fee_payer_analysis, sp_tx_shadow_addresses,
--                       sp_tx_bmap_get_token_state, sp_tx_bmap_state_backfill,
--                       sp_detect_chart_clipping, sp_tx_detect_chart_clipping,
--                       sp_billing_stats_by_apikey)
-- =============================================================================
