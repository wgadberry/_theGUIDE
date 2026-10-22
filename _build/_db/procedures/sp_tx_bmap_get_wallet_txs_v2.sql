-- sp_tx_bmap_get_wallet_txs_v2: Wallet transaction history with optional signature pivot
-- Returns flat result set of transactions involving a wallet (and optionally a specific token)
-- When p_signature is provided, returns transactions centered around that signature
-- (half before, half after) for contextual browsing.
--
-- Parameters:
--   p_wallet_address: Wallet address (required)
--   p_mint_address:   Token mint address (optional — NULL returns all tokens)
--   p_limit:          Max rows to return (default 50, max 200)
--   p_signature:      Anchor transaction signature (optional — NULL returns latest)
--
-- Usage:
--   CALL sp_tx_bmap_get_wallet_txs_v2('BAoV...', 'eFua...', 50, NULL);              -- latest 50
--   CALL sp_tx_bmap_get_wallet_txs_v2('BAoV...', 'eFua...', 50, '3dT9Aaa...');      -- 25 before + 25 after anchor

DELIMITER //

DROP PROCEDURE IF EXISTS sp_tx_bmap_get_wallet_txs_v2//

CREATE PROCEDURE sp_tx_bmap_get_wallet_txs_v2(
    IN p_wallet_address VARCHAR(44),
    IN p_mint_address VARCHAR(44),
    IN p_limit SMALLINT UNSIGNED,
    IN p_signature VARCHAR(88)
)
BEGIN
    DECLARE v_wallet_id INT UNSIGNED;
    DECLARE v_token_id BIGINT DEFAULT NULL;
    DECLARE v_anchor_block_time BIGINT UNSIGNED DEFAULT NULL;
    DECLARE v_half SMALLINT UNSIGNED;

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
            END IF;
        END IF;

        -- Only run main query if we didn't hit a token-not-found error
        IF v_token_id IS NOT NULL OR (p_mint_address IS NULL OR p_mint_address = '') THEN

            -- Resolve anchor signature
            IF p_signature IS NOT NULL AND p_signature != '' THEN
                SELECT block_time INTO v_anchor_block_time
                FROM tx WHERE signature = p_signature LIMIT 1;

                IF v_anchor_block_time IS NULL THEN
                    SELECT JSON_OBJECT('error', 'Signature not found', 'signature', p_signature) AS result;
                END IF;
            END IF;

            -- Proceed if no anchor was requested, or anchor was found
            IF p_signature IS NULL OR p_signature = '' OR v_anchor_block_time IS NOT NULL THEN

                -- Materialize wallet txs into temp table (single UNION, no duplication)
                DROP TEMPORARY TABLE IF EXISTS tmp_wallet_txs;
                CREATE TEMPORARY TABLE tmp_wallet_txs (
                    signature VARCHAR(88),
                    block_time BIGINT UNSIGNED,
                    block_time_utc DATETIME,
                    type_code VARCHAR(30),
                    direction VARCHAR(4),
                    amount DECIMAL(30,9),
                    token_symbol VARCHAR(30),
                    mint_address VARCHAR(44),
                    counterparty VARCHAR(44),
                    counterparty_label VARCHAR(255),
                    post_balance DECIMAL(30,9),
                    dex VARCHAR(50),
                    pool_label VARCHAR(255),
                    INDEX idx_bt (block_time)
                ) ENGINE=MEMORY;

                INSERT INTO tmp_wallet_txs
                -- FROM side: wallet is the sender (outflow)
                SELECT
                    t.signature,
                    t.block_time,
                    FROM_UNIXTIME(t.block_time),
                    gt.type_code,
                    'out' AS direction,
                    ROUND(g.amount / POW(10, COALESCE(g.decimals, tk.decimals, 9)), 9),
                    tk.token_symbol,
                    ma.address,
                    ca.address,
                    COALESCE(ca.label, ca.address_type),
                    ROUND(g.from_token_post_balance / POW(10, COALESCE(g.decimals, tk.decimals, 9)), 9),
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
                    FROM_UNIXTIME(t.block_time),
                    gt.type_code,
                    'in',
                    ROUND(g.amount / POW(10, COALESCE(g.decimals, tk.decimals, 9)), 9),
                    tk.token_symbol,
                    ma.address,
                    fa.address,
                    COALESCE(fa.label, fa.address_type),
                    ROUND(g.to_token_post_balance / POW(10, COALESCE(g.decimals, tk.decimals, 9)), 9),
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
                    FROM_UNIXTIME(t.block_time),
                    gt.type_code,
                    CASE WHEN g.from_address_id = v_wallet_id THEN 'out'
                         WHEN g.to_address_id = v_wallet_id THEN 'in'
                         ELSE 'swap' END,
                    ROUND(g.amount / POW(10, COALESCE(g.decimals, tk.decimals, 9)), 9),
                    tk.token_symbol,
                    ma.address,
                    COALESCE(fa.address, ta.address),
                    COALESCE(fa.label, fa.address_type, ta.label, ta.address_type),
                    NULL,
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
                  AND (v_token_id IS NULL OR g.token_id = v_token_id);

                -- Return results: pivot around anchor or just latest
                IF v_anchor_block_time IS NOT NULL THEN
                    SET v_half = CEIL(p_limit / 2);

                    -- MySQL can't reference the same temp table twice in one query,
                    -- so clone it for the UNION
                    DROP TEMPORARY TABLE IF EXISTS tmp_wallet_txs2;
                    CREATE TEMPORARY TABLE tmp_wallet_txs2 ENGINE=MEMORY AS
                    SELECT * FROM tmp_wallet_txs;

                    SELECT * FROM (
                        (SELECT * FROM tmp_wallet_txs
                         WHERE block_time <= v_anchor_block_time
                         ORDER BY block_time DESC
                         LIMIT v_half)
                        UNION ALL
                        (SELECT * FROM tmp_wallet_txs2
                         WHERE block_time > v_anchor_block_time
                         ORDER BY block_time ASC
                         LIMIT v_half)
                    ) combined
                    ORDER BY block_time DESC;

                    DROP TEMPORARY TABLE IF EXISTS tmp_wallet_txs2;
                ELSE
                    SELECT * FROM tmp_wallet_txs
                    ORDER BY block_time DESC
                    LIMIT p_limit;
                END IF;

                DROP TEMPORARY TABLE IF EXISTS tmp_wallet_txs;
            END IF;
        END IF;
    END IF;
END //

DELIMITER ;
