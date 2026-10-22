-- Migration: Add request_log_id to tx and staging tables for billing tracking
-- Created: 2026-01-30
--
-- This migration adds request_log_id columns to link tx records back to the
-- tx_request_log entry that triggered their creation, enabling client billing.
--
-- Run with: mysql -h 127.0.0.1 -P 3396 -u root -p t16o_db < migrate_add_request_log_id.sql

-- ============================================================================
-- STEP 1: Add request_log_id to tx table
-- ============================================================================
SELECT 'Adding request_log_id to tx table...' AS status;

ALTER TABLE tx
ADD COLUMN request_log_id BIGINT UNSIGNED NULL
COMMENT 'Links to tx_request_log.id for billing - the gateway record that triggered this tx';

ALTER TABLE tx
ADD INDEX idx_tx_request_log_id (request_log_id);

SELECT 'tx table updated.' AS status;

-- ============================================================================
-- STEP 2: Add request_log_id to staging.txs table
-- ============================================================================
SELECT 'Adding request_log_id to staging.txs table...' AS status;

ALTER TABLE t16o_db_staging.txs
ADD COLUMN request_log_id BIGINT UNSIGNED NULL
COMMENT 'Links to tx_request_log.id - passed from gateway through pipeline';

SELECT 'staging.txs table updated.' AS status;

-- ============================================================================
-- STEP 3: Add request_log_id to tx_address table (for funder discovery billing)
-- ============================================================================
SELECT 'Adding request_log_id to tx_address table...' AS status;

ALTER TABLE tx_address
ADD COLUMN request_log_id BIGINT UNSIGNED NULL
COMMENT 'Links to tx_request_log.id - the request that caused this address to be discovered';

ALTER TABLE tx_address
ADD INDEX idx_tx_address_request_log_id (request_log_id);

SELECT 'tx_address table updated.' AS status;

-- ============================================================================
-- STEP 3: Verify changes
-- ============================================================================
SELECT 'Verifying tx table columns...' AS status;
SHOW COLUMNS FROM tx WHERE Field = 'request_log_id';

SELECT 'Verifying staging.txs table columns...' AS status;
SHOW COLUMNS FROM t16o_db_staging.txs WHERE Field = 'request_log_id';

SELECT 'Migration complete!' AS status;
