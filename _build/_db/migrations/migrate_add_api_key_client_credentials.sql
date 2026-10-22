-- Migration: Add client_id and client_secret columns to tx_api_key
-- Safe to re-run (uses IF NOT EXISTS pattern via column check)

SET @col_exists = (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'tx_api_key' AND COLUMN_NAME = 'client_id');

SET @sql = IF(@col_exists = 0,
    'ALTER TABLE tx_api_key
        ADD COLUMN client_id VARCHAR(64) NOT NULL DEFAULT ''demo'' AFTER api_key,
        ADD COLUMN client_secret VARCHAR(64) NOT NULL DEFAULT ''a3f1b2c4-d5e6-7890-abcd-ef1234567890'' AFTER client_id,
        ADD INDEX idx_client (client_id, client_secret)',
    'SELECT ''columns already exist'' AS result');

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
