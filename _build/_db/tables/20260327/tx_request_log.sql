-- tx_request_log: Request logging for theGuide gateway
-- Tracks all requests through the gateway for visibility and debugging
--
-- Usage:
--   SELECT * FROM tx_request_log WHERE target_worker = 'producer' ORDER BY created_utc DESC LIMIT 100;
--   SELECT * FROM tx_request_log WHERE status = 'failed' AND created_utc > NOW() - INTERVAL 1 HOUR;

DROP TABLE IF EXISTS tx_request_log;

CREATE TABLE tx_request_log (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    request_id      VARCHAR(36) NOT NULL UNIQUE,  -- UUID v4
    correlation_id  VARCHAR(36),  -- Original REST request ID (flows through entire cascade chain)
    api_key_id      INT UNSIGNED,
    source          ENUM('rest', 'queue', 'cascade', 'scheduler') NOT NULL,
    target_worker   VARCHAR(50) NOT NULL,
    action          VARCHAR(50) NOT NULL,
    priority        TINYINT UNSIGNED DEFAULT 5,
    payload_hash    VARCHAR(64),  -- SHA256 of request payload for deduplication
    payload_summary JSON,  -- Subset of payload for quick reference (batch size, filters, etc.)
    status          ENUM('queued', 'processing', 'completed', 'failed', 'timeout') DEFAULT 'queued',
    error_message   TEXT,
    created_utc     TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
    started_at      TIMESTAMP(3) NULL,
    completed_at    TIMESTAMP(3) NULL,
    duration_ms     INT UNSIGNED GENERATED ALWAYS AS (
        CASE
            WHEN completed_at IS NOT NULL AND started_at IS NOT NULL
            THEN TIMESTAMPDIFF(MICROSECOND, started_at, completed_at) / 1000
            ELSE NULL
        END
    ) STORED,
    result          JSON,  -- Worker response summary
    cascade_request_ids JSON,  -- Array of child request_ids if this spawned cascades
    FOREIGN KEY (api_key_id) REFERENCES tx_api_key(id),
    INDEX idx_request_id (request_id),
    INDEX idx_correlation_id (correlation_id),
    INDEX idx_status (status),
    INDEX idx_target_worker (target_worker),
    INDEX idx_created_utc (created_utc),
    INDEX idx_api_key_id (api_key_id),
    INDEX idx_source_status (source, status)
) ENGINE=InnoDB;

-- View for recent activity summary
CREATE OR REPLACE VIEW vw_request_log_summary AS
SELECT
    target_worker,
    status,
    COUNT(*) as request_count,
    AVG(duration_ms) as avg_duration_ms,
    MAX(created_utc) as last_request_at
FROM tx_request_log
WHERE created_utc > NOW() - INTERVAL 1 HOUR
GROUP BY target_worker, status;
