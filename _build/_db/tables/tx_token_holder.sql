-- tx_token_holder — Cached token holder snapshots from Solscan
--
-- Stores top holders for tokens, refreshed on-demand via /api/token/<mint>/holders.
-- Each row = one holder of one token, with their current balance and rank.
--
-- Source: Solscan /v2.0/token/holders (2 pages × 40 = top 80 per token)
-- Owner = wallet address (resolved to tx_address.id)
-- Token account = ATA address (resolved to tx_address.id)
--
-- Usage:
--   - Upserted by sp_tx_token_holder_upsert (called from gateway)
--   - Queried by sp_tx_token_holder_edges (finds tx_guide connections between holders)
--   - updated_utc tracks freshness for cache invalidation

DROP TABLE IF EXISTS tx_token_holder;

CREATE TABLE tx_token_holder (
    id                BIGINT UNSIGNED    NOT NULL AUTO_INCREMENT,
    token_id          BIGINT UNSIGNED    NOT NULL,
    owner_address_id  INT UNSIGNED       NOT NULL COMMENT 'wallet owner → tx_address.id',
    token_account_id  INT UNSIGNED       DEFAULT NULL COMMENT 'ATA → tx_address.id',
    amount            DECIMAL(38,0)      NOT NULL DEFAULT 0,
    decimals          TINYINT UNSIGNED   NOT NULL DEFAULT 0,
    percentage        DECIMAL(10,6)      DEFAULT NULL COMMENT '% of total supply',
    `rank`            INT UNSIGNED       DEFAULT NULL,
    updated_utc       TIMESTAMP          NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_token_owner (token_id, owner_address_id),
    KEY idx_token_rank (token_id, `rank`),
    KEY idx_owner (owner_address_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
