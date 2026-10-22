-- Migration: Add primed flag to tx_token
-- Prevents enricher from re-priming tokens that have already been queued

SET @col_exists = (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'tx_token' AND COLUMN_NAME = 'primed');

SET @sql = IF(@col_exists = 0,
    'ALTER TABLE tx_token ADD COLUMN primed TINYINT(1) NOT NULL DEFAULT 0 AFTER token_type',
    'SELECT ''column already exists'' AS result');

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
