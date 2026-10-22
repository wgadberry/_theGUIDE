-- sp_tx_bmap_get_wallet_txs: Wallet transaction history, optionally filtered by token
-- Returns flat result set of transactions involving a wallet (and optionally a specific token)
-- Optimized with UNION instead of OR for index utilization
--
-- Parameters:
--   p_wallet_address: Wallet address (required)
--   p_mint_address:   Token mint address (optional — NULL returns all tokens)
--   p_limit:          Max rows to return (default 50, max 200)
--
-- Usage:
--   CALL sp_tx_bmap_get_wallet_txs('BAoVYrCEjFk7bgP2fRp2ahLcH6ktYUVSgXWHT88jYymy', 'eFua55nxgpaMqdP65qKweyN8PqPqHqWFzdNuC6ppump', 50);
--   CALL sp_tx_bmap_get_wallet_txs('BAoVYrCEjFk7bgP2fRp2ahLcH6ktYUVSgXWHT88jYymy', NULL, 50);

DELIMITER //

DROP PROCEDURE IF EXISTS sp_tx_bmap_get_wallet_txs//

CREATE PROCEDURE sp_tx_bmap_get_wallet_txs(
    IN p_wallet_address VARCHAR(44),
    IN p_mint_address VARCHAR(44),
    IN p_limit SMALLINT UNSIGNED
)
BEGIN
    DECLARE v_wallet_id INT UNSIGNED;
    DECLARE v_token_id BIGINT DEFAULT NULL;

    SET p_limit = COALESCE(p_limit, 50);
    IF p_limit > 200 THEN SET p_limit = 200; END IF;

    -- Resolve wallet
    SELECT id INTO v_wallet_id FROM tx_address WHERE address = p_wallet_address LIMIT 1;

    IF v_wallet_id IS NULL THEN
        SELECT JSON_OBJECT('error', 'Wallet address not found', 'address', p_wallet_address) AS result;
    ELSE
        -- Resolve token (if mint provided)
        IF p_mint_address IS NOT NULL AND p_mint_address != '' THEN
            SELECT tk.id INTO v_token_id
            FROM tx_token tk
            JOIN tx_address mint ON mint.id = tk.mint_address_id
            WHERE mint.address = p_mint_address
            LIMIT 1;

            IF v_token_id IS NULL THEN
                SELECT JSON_OBJECT('error', 'Token not found', 'mint', p_mint_address) AS result;
                -- early exit via LEAVE not needed; the ELSE below handles it
            END IF;
        END IF;

        -- Only run main query if we didn't hit a token-not-found error
        IF v_token_id IS NOT NULL OR (p_mint_address IS NULL OR p_mint_address = '') THEN
            SELECT
                r.signature,
                r.block_time,
                FROM_UNIXTIME(r.block_time) AS block_time_utc,
                r.type_code,
                r.direction,
                r.amount,
                r.token_symbol,
                r.mint_address,
                r.counterparty,
                r.counterparty_label,
                r.post_balance,
                r.dex,
                r.pool_label
            FROM (
                -- FROM side: wallet is the sender (outflow)
                SELECT
                    t.signature,
                    t.block_time,
                    gt.type_code,
                    'out' AS direction,
                    ROUND(g.amount / POW(10, COALESCE(g.decimals, tk.decimals, 9)), 9) AS amount,
                    tk.token_symbol,
                    ma.address AS mint_address,
                    ca.address AS counterparty,
                    COALESCE(ca.label, ca.address_type) AS counterparty_label,
                    ROUND(g.from_token_post_balance / POW(10, COALESCE(g.decimals, tk.decimals, 9)), 9) AS post_balance,
                    pool_ref.dex,
                    pool_ref.pool_label
                FROM tx_guide g
                JOIN tx t ON t.id = g.tx_id
                JOIN tx_guide_type gt ON gt.id = g.edge_type_id
            LEFT JOIN tx_pool pool_ref ON pool_ref.id = g.pool_id
                JOIN tx_token tk ON tk.id = g.token_id
                JOIN tx_address ma ON ma.id = tk.mint_address_id
                LEFT JOIN tx_address ca ON ca.id = g.to_address_id
                WHERE g.from_address_id = v_wallet_id
                  AND (v_token_id IS NULL OR g.token_id = v_token_id)

                UNION ALL

                -- TO side: wallet is the receiver (inflow)
                SELECT
                    t.signature,
                    t.block_time,
                    gt.type_code,
                    'in' AS direction,
                    ROUND(g.amount / POW(10, COALESCE(g.decimals, tk.decimals, 9)), 9) AS amount,
                    tk.token_symbol,
                    ma.address AS mint_address,
                    fa.address AS counterparty,
                    COALESCE(fa.label, fa.address_type) AS counterparty_label,
                    ROUND(g.to_token_post_balance / POW(10, COALESCE(g.decimals, tk.decimals, 9)), 9) AS post_balance,
                    pool_ref.dex,
                    pool_ref.pool_label
                FROM tx_guide g
                JOIN tx t ON t.id = g.tx_id
                JOIN tx_guide_type gt ON gt.id = g.edge_type_id
            LEFT JOIN tx_pool pool_ref ON pool_ref.id = g.pool_id
                JOIN tx_token tk ON tk.id = g.token_id
                JOIN tx_address ma ON ma.id = tk.mint_address_id
                LEFT JOIN tx_address fa ON fa.id = g.from_address_id
                WHERE g.to_address_id = v_wallet_id
                  AND (v_token_id IS NULL OR g.token_id = v_token_id)

                UNION ALL

                -- POOL side: address is the pool (swap activity through this pool)
                SELECT
                    t.signature,
                    t.block_time,
                    gt.type_code,
                    CASE WHEN g.from_address_id = v_wallet_id THEN 'out'
                         WHEN g.to_address_id = v_wallet_id THEN 'in'
                         ELSE 'swap' END AS direction,
                    ROUND(g.amount / POW(10, COALESCE(g.decimals, tk.decimals, 9)), 9) AS amount,
                    tk.token_symbol,
                    ma.address AS mint_address,
                    COALESCE(fa.address, ta.address) AS counterparty,
                    COALESCE(fa.label, fa.address_type, ta.label, ta.address_type) AS counterparty_label,
                    NULL AS post_balance,
                    pool_ref.dex,
                    pool_ref.pool_label
                FROM tx_guide g
                JOIN tx t ON t.id = g.tx_id
                JOIN tx_guide_type gt ON gt.id = g.edge_type_id
            LEFT JOIN tx_pool pool_ref ON pool_ref.id = g.pool_id
                JOIN tx_token tk ON tk.id = g.token_id
                JOIN tx_address ma ON ma.id = tk.mint_address_id
                LEFT JOIN tx_address fa ON fa.id = g.from_address_id
                LEFT JOIN tx_address ta ON ta.id = g.to_address_id
                JOIN tx_pool wp ON wp.id = g.pool_id AND wp.pool_address_id = v_wallet_id
                WHERE 1=1
                  AND g.from_address_id != v_wallet_id
                  AND g.to_address_id != v_wallet_id
                  AND (v_token_id IS NULL OR g.token_id = v_token_id)
            ) r
            ORDER BY r.block_time DESC
            LIMIT p_limit;
        END IF;
    END IF;
END //

DELIMITER ;
