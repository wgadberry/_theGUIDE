-- tx_guide_stage: Persistent staging table for sp_tx_get
-- Replaces all temporary tables (tmp_guide_ids, tmp_final_ids, tmp_final_ids2)
-- Forward-only: monotonic seq, no deletes during SP execution
--
-- Benefits:
--   - No temp table reopen issue (real table, unlimited references per query)
--   - No per-call DDL overhead
--   - Replayable: client can re-read by seq without re-executing Phase A
--   - Zero contention: concurrent calls get different seq values
--   - Cleanup: background worker deletes old rows by age

DROP TABLE IF EXISTS tx_guide_stage;

CREATE TABLE tx_guide_stage (
    id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    seq         BIGINT UNSIGNED NOT NULL COMMENT 'Monotonic sequence from config table — groups rows from one SP call',
    guide_id    BIGINT UNSIGNED NOT NULL COMMENT 'FK to tx_guide.id — the surviving edge',
    tx_id       BIGINT UNSIGNED NOT NULL COMMENT 'FK to tx.id — denormalized for fast tx grouping',
    block_time  BIGINT UNSIGNED COMMENT 'Denormalized from tx.block_time',
    direction   VARCHAR(4) COMMENT 'in/out/swap (wallet mode) or NULL (graph mode)',
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_seq (seq),
    INDEX idx_seq_guide (seq, guide_id),
    INDEX idx_seq_tx (seq, tx_id),
    INDEX idx_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Sequence counter in config table
INSERT IGNORE INTO config (config_type, config_key, config_value, description)
VALUES ('system', 'stage_seq', '0', 'Monotonic sequence counter for tx_guide_stage');
