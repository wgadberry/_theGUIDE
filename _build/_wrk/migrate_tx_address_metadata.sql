-- Migration: Add metadata columns to tx_address for Solscan /account/metadata API
-- These columns store rich metadata from Solscan's account/metadata endpoint

-- account_tags: JSON array of tags (e.g., ["raydium", "pool"], ["pumpfun"])
-- active_age_days: Days since first activity on-chain

-- Check and add account_tags column
SET @col_exists = (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'tx_address' AND COLUMN_NAME = 'account_tags');
SET @sql = IF(@col_exists = 0,
    'ALTER TABLE tx_address ADD COLUMN account_tags JSON NULL AFTER label',
    'SELECT "account_tags already exists"');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Check and add active_age_days column
SET @col_exists = (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'tx_address' AND COLUMN_NAME = 'active_age_days');
SET @sql = IF(@col_exists = 0,
    'ALTER TABLE tx_address ADD COLUMN active_age_days INT UNSIGNED NULL AFTER first_seen_block_time',
    'SELECT "active_age_days already exists"');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Verify columns
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'tx_address'
  AND COLUMN_NAME IN ('account_tags', 'active_age_days', 'label', 'address_type');
