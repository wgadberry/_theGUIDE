-- Add request_before_block_time and request_until_block_time to tx_boundary
-- These store the REQUESTED time range (intent), not where data landed.
-- Gap detection uses these to determine if a time range was already queried,
-- regardless of whether actual transactions exist in that range.

ALTER TABLE tx_boundary
    ADD COLUMN `request_before_block_time` BIGINT UNSIGNED DEFAULT NULL
        COMMENT 'Upper bound of the requested fetch range (before this time)' AFTER `request_log_id`,
    ADD COLUMN `request_until_block_time` BIGINT UNSIGNED DEFAULT NULL
        COMMENT 'Lower bound of the requested fetch range (until/stop at this time)' AFTER `request_before_block_time`;
