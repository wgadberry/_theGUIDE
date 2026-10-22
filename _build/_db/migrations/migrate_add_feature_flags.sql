-- Migration: Add feature flag constants and API key feature_mask
-- Created: 2026-01-31
--
-- Adds feature bitmask constants to config table for configurable data collection.
-- Each feature controls what data is collected during transaction processing.
--
-- Run with: mysql -h 127.0.0.1 -P 3396 -u root -p t16o_db < migrate_add_feature_flags.sql

SELECT 'Adding feature flag constants to config table...' AS status;

-- Feature constants (bitmask values)
-- Core features (always collected) have no flag - they're the default
-- These flags ENABLE additional data collection beyond core

INSERT INTO config (config_type, config_key, config_value, value_type, description) VALUES
('feature', 'balance_changes', '1', 'int', 'Collect all balance changes for all participants (not just searched address)'),
('feature', 'all_addresses', '2', 'int', 'Collect all addresses in transaction (ATAs, vaults, intermediate accounts)'),
('feature', 'swap_routing', '4', 'int', 'Collect full swap routing paths including intermediate hops'),
('feature', 'ata_mapping', '8', 'int', 'Collect associated token account mappings'),
('feature', 'funder_discovery', '16', 'int', 'Enable funder wallet discovery via Solscan API'),
('feature', 'token_metadata', '32', 'int', 'Enable token metadata enrichment (community service - typically free)'),
('feature', 'address_labels', '64', 'int', 'Enable address label/tag enrichment (community service - typically free)'),
('feature', 'program_details', '128', 'int', 'Collect detailed program invocation data')
ON DUPLICATE KEY UPDATE config_value = VALUES(config_value), description = VALUES(description);

SELECT 'Adding feature_mask column to tx_api_key...' AS status;

-- Add feature_mask column to tx_api_key if it doesn't exist
SET @col_exists = (
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 't16o_db'
    AND TABLE_NAME = 'tx_api_key'
    AND COLUMN_NAME = 'feature_mask'
);

SET @sql = IF(@col_exists = 0,
    'ALTER TABLE tx_api_key ADD COLUMN feature_mask INT UNSIGNED DEFAULT 0 AFTER active',
    'SELECT ''feature_mask column already exists'' AS status'
);

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT 'Adding features column to tx_request_log...' AS status;

-- Add features column to tx_request_log if it doesn't exist
SET @col_exists = (
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 't16o_db'
    AND TABLE_NAME = 'tx_request_log'
    AND COLUMN_NAME = 'features'
);

SET @sql = IF(@col_exists = 0,
    'ALTER TABLE tx_request_log ADD COLUMN features INT UNSIGNED DEFAULT 0 AFTER priority',
    'SELECT ''features column already exists'' AS status'
);

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT 'Verifying changes...' AS status;

-- Show inserted feature configs
SELECT config_key, config_value, description
FROM config
WHERE config_type = 'feature'
ORDER BY CAST(config_value AS UNSIGNED);

-- Show tx_api_key columns
SHOW COLUMNS FROM tx_api_key WHERE Field = 'feature_mask';

-- Show tx_request_log columns
SHOW COLUMNS FROM tx_request_log WHERE Field = 'features';

SELECT 'Migration complete!' AS status;
