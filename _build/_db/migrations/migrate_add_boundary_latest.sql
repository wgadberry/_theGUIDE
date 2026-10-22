-- Add latest_fetched_sig/block_time to tx_boundary for gap detection
-- latest_fetched = first (newest) sig in the fetch session
-- Gap exists when boundary_sig from row A has no matching latest_fetched_sig in any row for that address

ALTER TABLE tx_boundary
    ADD COLUMN `latest_fetched_sig` VARCHAR(88) DEFAULT NULL COMMENT 'First (newest) signature in the fetch session' AFTER `address_id`,
    ADD COLUMN `latest_fetched_block_time` BIGINT UNSIGNED DEFAULT NULL AFTER `latest_fetched_sig`,
    ADD INDEX `idx_address_latest` (`address_id`, `latest_fetched_sig`);
