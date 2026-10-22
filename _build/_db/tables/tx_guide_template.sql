-- tx_guide_template
--
-- Stores canonical narrative templates and their pre-computed embeddings
-- for tx_guide edge patterns. Each row represents a distinct structural
-- pattern defined by 8 classification fields.
--
-- Used by qdrant_loader.py --mode template:
--   - Top patterns (use_template=1): reuse cached embedding vector
--   - Tail patterns (use_template=0): fall back to per-row OpenAI embedding
--
-- Pattern key: edge_type_code + edge_category + edge_direction +
--              from_address_type + to_address_type + has_pool + has_tax +
--              swap_direction
--
-- pattern_hash = SHA2(CONCAT_WS('|', key fields...), 256)

CREATE TABLE IF NOT EXISTS tx_guide_template (
    id                  INT UNSIGNED NOT NULL AUTO_INCREMENT,

    -- Pattern key fields
    pattern_hash        VARCHAR(64)  NOT NULL,
    edge_type_code      VARCHAR(32)  NOT NULL,
    edge_category       VARCHAR(32)  NOT NULL,
    edge_direction      VARCHAR(32)  NOT NULL DEFAULT 'neutral',
    from_address_type   VARCHAR(32)  NOT NULL DEFAULT 'unknown',
    to_address_type     VARCHAR(32)  NOT NULL DEFAULT 'unknown',
    has_pool            TINYINT UNSIGNED NOT NULL DEFAULT 0,
    has_tax             TINYINT UNSIGNED NOT NULL DEFAULT 0,
    swap_direction      VARCHAR(16)  NOT NULL DEFAULT 'none',
    balance_signal      VARCHAR(16)  NOT NULL DEFAULT 'normal', -- drained|dust|large|funded|normal
    amount_magnitude    VARCHAR(16)  NOT NULL DEFAULT 'medium', -- micro|small|medium|whale
    label_signal        VARCHAR(16)  NOT NULL DEFAULT 'unlabeled', -- dex_labeled|cex_labeled|burn_labeled|authority_labeled|unlabeled

    -- Template content
    narrative           TEXT         NULL,       -- canonical narrative text for this pattern
    embedding           JSON         NULL,       -- 384-float vector as JSON array

    -- Stats
    row_count           INT UNSIGNED NOT NULL DEFAULT 0,    -- tx_guide rows matching this pattern
    pct_of_total        DECIMAL(5,2) NOT NULL DEFAULT 0.00, -- percentage of total tx_guide rows
    cumulative_pct      DECIMAL(5,2) NOT NULL DEFAULT 0.00, -- running cumulative percentage

    -- Control
    use_template        TINYINT UNSIGNED NOT NULL DEFAULT 0, -- 1=use cached vector, 0=full embed
    min_row_threshold   INT UNSIGNED NOT NULL DEFAULT 100,   -- minimum rows to qualify for template

    -- Timestamps
    created_utc         DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_utc         DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    UNIQUE KEY uk_pattern_hash (pattern_hash),
    KEY ix_use_template (use_template),
    KEY ix_row_count (row_count DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
