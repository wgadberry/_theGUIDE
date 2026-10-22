-- Migration: Add indexes to improve sp_tx_bmap_get performance
-- These composite indexes help with balance lookup queries

-- Index for from_address balance lookups
SET @idx_exists = (SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'tx_guide' AND INDEX_NAME = 'idx_from_token_time');
SET @sql = IF(@idx_exists = 0,
    'ALTER TABLE tx_guide ADD INDEX idx_from_token_time (from_address_id, token_id, block_time DESC)',
    'SELECT "idx_from_token_time already exists"');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Index for to_address balance lookups
SET @idx_exists = (SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'tx_guide' AND INDEX_NAME = 'idx_to_token_time');
SET @sql = IF(@idx_exists = 0,
    'ALTER TABLE tx_guide ADD INDEX idx_to_token_time (to_address_id, token_id, block_time DESC)',
    'SELECT "idx_to_token_time already exists"');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Verify indexes
SELECT INDEX_NAME, GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS columns
FROM INFORMATION_SCHEMA.STATISTICS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'tx_guide'
  AND INDEX_NAME IN ('idx_from_token_time', 'idx_to_token_time', 'idx_token_blocktime')
GROUP BY INDEX_NAME;
