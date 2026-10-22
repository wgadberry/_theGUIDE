-- ============================================================================
-- tx_bmap_state: Pre-computed token state per transaction (normalized)
-- Materialized running balances for fast BMap queries
-- ============================================================================

DROP TABLE IF EXISTS tx_bmap_state;

CREATE TABLE tx_bmap_state (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    token_id        BIGINT NOT NULL,              -- matches tx_token.id (signed)
    tx_id           BIGINT NOT NULL,              -- matches tx.id (signed)
    address_id      INT UNSIGNED NOT NULL,        -- matches tx_address.id

    -- Running balance AFTER this transaction
    balance         DECIMAL(30,9) NOT NULL DEFAULT 0,

    -- Delta for this transaction (positive = inflow, negative = outflow)
    delta           DECIMAL(30,9) NOT NULL DEFAULT 0,

    -- Denormalized timestamp for fast range queries (avoid JOIN to tx)
    block_time      BIGINT UNSIGNED NOT NULL,

    -- Foreign keys
    CONSTRAINT fk_bmap_token FOREIGN KEY (token_id) REFERENCES tx_token(id),
    CONSTRAINT fk_bmap_tx FOREIGN KEY (tx_id) REFERENCES tx(id),
    CONSTRAINT fk_bmap_address FOREIGN KEY (address_id) REFERENCES tx_address(id),

    -- Composite indexes for fast lookups
    UNIQUE KEY uq_token_tx_addr (token_id, tx_id, address_id),
    KEY ix_token_time (token_id, block_time DESC),
    KEY ix_token_addr_time (token_id, address_id, block_time DESC),
    KEY ix_tx (tx_id)
) ENGINE=InnoDB;
