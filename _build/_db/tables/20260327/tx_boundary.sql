-- tx_boundary: tracks fetch sessions and gap detection
-- Each row = one fetch session with two time dimensions:
--   REQUESTED: what range was asked for (intent — used for gap detection)
--   FETCHED:   where data actually landed (nearest sigs found by RPC)
--
-- Gap detection uses request_before/until_block_time to determine coverage.
-- A time range is "covered" if any row's requested range includes it,
-- even if no transactions exist in that range (RPC confirmed it's empty).

CREATE TABLE IF NOT EXISTS `tx_boundary` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `address_id` INT UNSIGNED NOT NULL COMMENT 'The address (mint/wallet) that was queried',
    `latest_fetched_sig` VARCHAR(88) DEFAULT NULL COMMENT 'First (newest) signature in the fetch session',
    `latest_fetched_block_time` BIGINT UNSIGNED DEFAULT NULL,
    `oldest_fetched_sig` VARCHAR(88) NOT NULL COMMENT 'Last (oldest) signature in the fetched batch',
    `oldest_fetched_block_time` BIGINT UNSIGNED DEFAULT NULL,
    `boundary_sig` VARCHAR(88) DEFAULT NULL COMMENT 'The N+1th sig (peeked, not processed). NULL = end of history',
    `boundary_block_time` BIGINT UNSIGNED DEFAULT NULL,
    `fetched_count` INT UNSIGNED DEFAULT 0 COMMENT 'How many sigs were in this fetch batch',
    `boundary_loaded` TINYINT UNSIGNED DEFAULT 0 COMMENT '1 if boundary_sig was later fetched/processed',
    `request_log_id` BIGINT UNSIGNED DEFAULT NULL COMMENT 'Links to tx_request_log for traceability',
    `request_before_block_time` BIGINT UNSIGNED DEFAULT NULL COMMENT 'Upper bound of the requested fetch range',
    `request_until_block_time` BIGINT UNSIGNED DEFAULT NULL COMMENT 'Lower bound of the requested fetch range',
    `created_utc` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    INDEX `idx_address` (`address_id`),
    INDEX `idx_boundary_sig` (`boundary_sig`),
    INDEX `idx_address_loaded` (`address_id`, `boundary_loaded`),
    INDEX `idx_address_latest` (`address_id`, `latest_fetched_sig`),
    INDEX `idx_request_log` (`request_log_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
