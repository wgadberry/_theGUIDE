-- t16o_db_staging.txs_sync
-- Stores discovered on-chain signatures for gap-filling sync requests.
-- Each sync request (identified by sync_id) discovers signatures via Chainstack RPC,
-- then diffs against tx table to find and fill gaps.

CREATE TABLE IF NOT EXISTS t16o_db_staging.txs_sync (
  id              BIGINT AUTO_INCREMENT PRIMARY KEY,
  sync_id         VARCHAR(36)      NOT NULL,
  mint_address_id INT UNSIGNED     NOT NULL,
  signature       VARCHAR(88)      NOT NULL,
  block_time      BIGINT UNSIGNED  DEFAULT NULL,
  block_id        BIGINT UNSIGNED  DEFAULT NULL,
  status          ENUM('discovered','exists','queued','processed','failed') DEFAULT 'discovered',
  created_utc     DATETIME         DEFAULT (UTC_TIMESTAMP()),
  UNIQUE KEY uk_sync_sig    (sync_id, signature),
  KEY idx_sync_status       (sync_id, status),
  KEY idx_signature         (signature)
) ENGINE=InnoDB;
