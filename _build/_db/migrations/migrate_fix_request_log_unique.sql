-- Migration: Fix tx_request_log unique constraint for multi-worker tracking
-- Created: 2026-01-30
--
-- The current unique constraint on request_id alone prevents multiple workers
-- from creating records for the same request. We need (request_id, target_worker, api_key_id)
-- to allow gateway, producer, decoder, detailer to each have their own record.
--
-- Run with: mysql -h 127.0.0.1 -P 3396 -u root -p t16o_db < migrate_fix_request_log_unique.sql

-- ============================================================================
-- STEP 1: Check current indexes
-- ============================================================================
SELECT 'Current indexes on tx_request_log:' AS status;
SHOW INDEX FROM tx_request_log WHERE Column_name = 'request_id';

-- ============================================================================
-- STEP 2: Drop existing unique constraint on request_id
-- ============================================================================
SELECT 'Dropping old unique constraint...' AS status;

-- Check if the index exists before dropping
SET @index_exists = (
    SELECT COUNT(*) FROM information_schema.statistics
    WHERE table_schema = DATABASE()
    AND table_name = 'tx_request_log'
    AND index_name = 'request_id'
);

-- Drop if exists (using prepared statement for conditional execution)
SET @drop_sql = IF(@index_exists > 0,
    'ALTER TABLE tx_request_log DROP INDEX request_id',
    'SELECT "Index request_id does not exist" AS note');
PREPARE stmt FROM @drop_sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- ============================================================================
-- STEP 3: Create new composite unique constraint
-- ============================================================================
SELECT 'Creating new composite unique constraint...' AS status;

ALTER TABLE tx_request_log
ADD UNIQUE INDEX idx_request_worker_key (request_id, target_worker, api_key_id);

-- ============================================================================
-- STEP 4: Verify changes
-- ============================================================================
SELECT 'Verifying new indexes:' AS status;
SHOW INDEX FROM tx_request_log WHERE Key_name = 'idx_request_worker_key';

SELECT 'Migration complete!' AS status;
