-- Migration: Create dedicated tx_feature_config table
-- Created: 2026-01-31
--
-- Moves feature flags from generic config table to dedicated table.
--
-- Run with: mysql -h 127.0.0.1 -P 3396 -u root -p t16o_db < migrate_create_tx_feature_config.sql

SELECT 'Creating tx_feature_config table...' AS status;

CREATE TABLE IF NOT EXISTS tx_feature_config (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    feature_name VARCHAR(50) NOT NULL UNIQUE,
    feature_mask INT UNSIGNED NOT NULL,
    description VARCHAR(255),
    is_billable TINYINT(1) NOT NULL DEFAULT 1,
    created_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_feature_mask (feature_mask)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SELECT 'Inserting feature definitions...' AS status;

INSERT INTO tx_feature_config (feature_name, feature_mask, description, is_billable) VALUES
('balance_changes', 1, 'Collect all balance changes for all participants (not just searched address)', 1),
('all_addresses', 2, 'Collect all addresses in transaction (ATAs, vaults, intermediate accounts)', 1),
('swap_routing', 4, 'Collect full swap routing paths including intermediate hops', 1),
('ata_mapping', 8, 'Collect associated token account mappings', 1),
('funder_discovery', 16, 'Enable funder wallet discovery via Solscan API', 1),
('token_metadata', 32, 'Enable token metadata enrichment (community service)', 0),
('address_labels', 64, 'Enable address label/tag enrichment (community service)', 0),
('program_details', 128, 'Collect detailed program invocation data', 1)
ON DUPLICATE KEY UPDATE
    description = VALUES(description),
    is_billable = VALUES(is_billable);

SELECT 'Removing feature records from generic config table...' AS status;

DELETE FROM config WHERE config_type = 'feature' AND config_key IN (
    'balance_changes', 'all_addresses', 'swap_routing', 'ata_mapping',
    'funder_discovery', 'token_metadata', 'address_labels', 'program_details'
);

SELECT 'Verifying tx_feature_config...' AS status;

SELECT feature_name, feature_mask, is_billable, description
FROM tx_feature_config
ORDER BY feature_mask;

SELECT 'Migration complete!' AS status;
