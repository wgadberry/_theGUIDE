-- sp_tx_bmap_get_wallet_state: Wallet-centric cluster map with funding tree
-- Shows wallet activity for a specific token, plus 5-level funding tree
--
-- Parameters:
--   p_wallet_address: Target wallet address (required)
--   p_token_symbol: Token symbol filter (optional but recommended)
--   p_mint_address: Token mint address filter (optional)
--   p_depth_limit: Funding tree depth (default 5)
--   p_tx_limit: Max transactions to include (default 50)

DROP PROCEDURE IF EXISTS sp_tx_bmap_get_wallet_state;

DELIMITER //

CREATE PROCEDURE sp_tx_bmap_get_wallet_state(
    IN p_wallet_address VARCHAR(44),
    IN p_token_symbol VARCHAR(128),
    IN p_mint_address VARCHAR(44),
    IN p_depth_limit TINYINT UNSIGNED,
    IN p_tx_limit SMALLINT UNSIGNED
)
BEGIN
    DECLARE v_wallet_id BIGINT UNSIGNED;
    DECLARE v_token_id BIGINT UNSIGNED;
    DECLARE v_mint_address VARCHAR(44);
    DECLARE v_token_symbol VARCHAR(128);

    DECLARE v_nodes_json JSON;
    DECLARE v_edges_json JSON;
    DECLARE v_stats_json JSON;

    -- Defaults
    SET p_depth_limit = COALESCE(p_depth_limit, 5);
    SET p_tx_limit = COALESCE(p_tx_limit, 50);
    IF p_tx_limit > 200 THEN SET p_tx_limit = 200; END IF;

    -- Resolve wallet address
    SELECT id INTO v_wallet_id FROM tx_address WHERE address = p_wallet_address LIMIT 1;

    IF v_wallet_id IS NULL THEN
        SELECT JSON_OBJECT('result', JSON_OBJECT('error', 'Wallet address not found', 'address', p_wallet_address)) AS guide;
    ELSE
        -- Resolve token (optional)
        IF p_mint_address IS NOT NULL THEN
            SELECT tk.id, mint.address, tk.token_symbol
            INTO v_token_id, v_mint_address, v_token_symbol
            FROM tx_token tk JOIN tx_address mint ON mint.id = tk.mint_address_id
            WHERE mint.address = p_mint_address LIMIT 1;
        ELSEIF p_token_symbol IS NOT NULL THEN
            SELECT tk.id, mint.address, tk.token_symbol
            INTO v_token_id, v_mint_address, v_token_symbol
            FROM tx_token tk JOIN tx_address mint ON mint.id = tk.mint_address_id
            LEFT JOIN (SELECT token_id, COUNT(*) AS cnt FROM tx_guide GROUP BY token_id) g ON g.token_id = tk.id
            WHERE tk.token_symbol = p_token_symbol
            ORDER BY COALESCE(g.cnt, 0) DESC LIMIT 1;
        END IF;

        -- Build funding tree UP using recursive CTE
        DROP TEMPORARY TABLE IF EXISTS tmp_funding_up;
        CREATE TEMPORARY TABLE tmp_funding_up AS
        WITH RECURSIVE funding_up AS (
            SELECT a.id as address_id, a.address, COALESCE(a.label, a.address_type) as label,
                   a.address_type, a.funded_by_address_id as funded_by_id, 0 as depth
            FROM tx_address a WHERE a.id = v_wallet_id

            UNION ALL

            SELECT a.id, a.address, COALESCE(a.label, a.address_type),
                   a.address_type, a.funded_by_address_id, fu.depth + 1
            FROM funding_up fu
            JOIN tx_address a ON a.id = fu.funded_by_id
            WHERE fu.depth < p_depth_limit
        )
        SELECT fu.*, f.address as funded_by_address,
               CASE WHEN fu.depth = p_depth_limit AND fu.funded_by_id IS NOT NULL THEN 1 ELSE 0 END as has_more
        FROM funding_up fu
        LEFT JOIN tx_address f ON f.id = fu.funded_by_id;

        -- Build funding tree DOWN using recursive CTE
        DROP TEMPORARY TABLE IF EXISTS tmp_funding_down;
        CREATE TEMPORARY TABLE tmp_funding_down AS
        WITH RECURSIVE funding_down AS (
            SELECT a.id as address_id, a.address, COALESCE(a.label, a.address_type) as label,
                   a.address_type, 0 as depth
            FROM tx_address a WHERE a.id = v_wallet_id

            UNION ALL

            SELECT a.id, a.address, COALESCE(a.label, a.address_type), a.address_type, fd.depth + 1
            FROM funding_down fd
            JOIN tx_address a ON a.funded_by_address_id = fd.address_id
            WHERE fd.depth < p_depth_limit
        )
        SELECT fd.*,
               CASE WHEN fd.depth = p_depth_limit
                    AND EXISTS (SELECT 1 FROM tx_address x WHERE x.funded_by_address_id = fd.address_id)
                    THEN 1 ELSE 0 END as has_more
        FROM funding_down fd;

        -- Get wallet's transactions
        DROP TEMPORARY TABLE IF EXISTS tmp_wallet_txs;
        CREATE TEMPORARY TABLE tmp_wallet_txs AS
        SELECT DISTINCT t.id as tx_id, t.signature, t.block_time
        FROM tx_guide g JOIN tx t ON t.id = g.tx_id
        WHERE (g.from_address_id = v_wallet_id OR g.to_address_id = v_wallet_id)
          AND (v_token_id IS NULL OR g.token_id = v_token_id OR g.token_id IS NULL)
        ORDER BY t.block_time DESC LIMIT p_tx_limit;

        -- Build nodes
        DROP TEMPORARY TABLE IF EXISTS tmp_nodes;
        CREATE TEMPORARY TABLE tmp_nodes (
            address_id BIGINT UNSIGNED PRIMARY KEY,
            address VARCHAR(44),
            label VARCHAR(200),
            address_type VARCHAR(30),
            pool_label VARCHAR(255) DEFAULT NULL,
            token_name VARCHAR(255) DEFAULT NULL,
            balance DECIMAL(30,9) DEFAULT 0,
            funded_by VARCHAR(44) DEFAULT NULL,
            is_target TINYINT DEFAULT 0,
            funding_depth TINYINT DEFAULT NULL,
            funding_direction VARCHAR(4) DEFAULT NULL,
            has_more_funding TINYINT DEFAULT 0
        );

        -- Target wallet
        INSERT INTO tmp_nodes (address_id, address, label, address_type, is_target, funding_depth, funding_direction, funded_by)
        SELECT a.id, a.address, COALESCE(a.label, a.address_type), a.address_type, 1, 0, 'self', f.address
        FROM tx_address a LEFT JOIN tx_address f ON f.id = a.funded_by_address_id
        WHERE a.id = v_wallet_id;

        -- Funders (up)
        INSERT IGNORE INTO tmp_nodes (address_id, address, label, address_type, funding_depth, funding_direction, has_more_funding, funded_by)
        SELECT address_id, address, label, address_type, depth, 'up', has_more, funded_by_address
        FROM tmp_funding_up WHERE depth > 0;

        -- Funded (down)
        INSERT IGNORE INTO tmp_nodes (address_id, address, label, address_type, funding_depth, funding_direction, has_more_funding)
        SELECT address_id, address, label, address_type, depth, 'down', has_more
        FROM tmp_funding_down WHERE depth > 0;

        -- Tx counterparties
        INSERT IGNORE INTO tmp_nodes (address_id, address, label, address_type, funded_by)
        SELECT DISTINCT a.id, a.address, COALESCE(a.label, a.address_type), a.address_type, f.address
        FROM tx_guide g
        JOIN tmp_wallet_txs w ON w.tx_id = g.tx_id
        JOIN tx_address a ON a.id = g.from_address_id
        LEFT JOIN tx_address f ON f.id = a.funded_by_address_id
        WHERE v_token_id IS NULL OR g.token_id = v_token_id OR g.token_id IS NULL;

        INSERT IGNORE INTO tmp_nodes (address_id, address, label, address_type, funded_by)
        SELECT DISTINCT a.id, a.address, COALESCE(a.label, a.address_type), a.address_type, f.address
        FROM tx_guide g
        JOIN tmp_wallet_txs w ON w.tx_id = g.tx_id
        JOIN tx_address a ON a.id = g.to_address_id
        LEFT JOIN tx_address f ON f.id = a.funded_by_address_id
        WHERE v_token_id IS NULL OR g.token_id = v_token_id OR g.token_id IS NULL;

        -- Enrich
        UPDATE tmp_nodes n JOIN tx_pool p ON p.pool_address_id = n.address_id SET n.pool_label = p.pool_label;
        UPDATE tmp_nodes n JOIN tx_token tk ON tk.mint_address_id = n.address_id SET n.token_name = tk.token_name;

        -- Token balance if filtered
        IF v_token_id IS NOT NULL THEN
            UPDATE tmp_nodes n SET balance = COALESCE((
                SELECT SUM(g.amount / POW(10, g.decimals)) FROM tx_guide g
                WHERE g.token_id = v_token_id AND g.to_address_id = n.address_id
            ), 0) - COALESCE((
                SELECT SUM(g.amount / POW(10, g.decimals)) FROM tx_guide g
                WHERE g.token_id = v_token_id AND g.from_address_id = n.address_id
            ), 0);
        END IF;

        -- Nodes JSON
        SELECT JSON_ARRAYAGG(JSON_OBJECT(
            'address', n.address, 'label', n.label, 'address_type', n.address_type,
            'pool_label', n.pool_label, 'token_name', n.token_name,
            'balance', ROUND(n.balance, 6), 'funded_by', n.funded_by,
            'is_target', n.is_target, 'funding_depth', n.funding_depth,
            'funding_direction', n.funding_direction, 'has_more_funding', n.has_more_funding
        )) INTO v_nodes_json FROM tmp_nodes n;

        -- Calculate stats FIRST using simple queries (avoids temp table reopen issues)
        SET @v_total_txs = (SELECT COUNT(*) FROM tmp_wallet_txs);
        SET @v_funding_up_levels = (SELECT MAX(depth) FROM tmp_funding_up);
        SET @v_funding_down_levels = (SELECT MAX(depth) FROM tmp_funding_down);
        SET @v_wallets_funded = (SELECT COUNT(*) FROM tmp_funding_down WHERE depth > 0);
        SET @v_has_more_up = (SELECT COALESCE(MAX(has_more), 0) FROM tmp_funding_up);
        SET @v_has_more_down = (SELECT COALESCE(MAX(has_more), 0) FROM tmp_funding_down);

        SET v_stats_json = JSON_OBJECT(
            'total_txs', @v_total_txs,
            'funding_up_levels', @v_funding_up_levels,
            'funding_down_levels', @v_funding_down_levels,
            'wallets_funded', @v_wallets_funded,
            'has_more_up', @v_has_more_up,
            'has_more_down', @v_has_more_down
        );

        -- Edges JSON (copies needed for UNION subqueries)
        DROP TEMPORARY TABLE IF EXISTS tmp_wallet_txs2;
        CREATE TEMPORARY TABLE tmp_wallet_txs2 AS SELECT * FROM tmp_wallet_txs;
        DROP TEMPORARY TABLE IF EXISTS tmp_funding_up2;
        CREATE TEMPORARY TABLE tmp_funding_up2 AS SELECT * FROM tmp_funding_up;
        DROP TEMPORARY TABLE IF EXISTS tmp_funding_down2;
        CREATE TEMPORARY TABLE tmp_funding_down2 AS SELECT * FROM tmp_funding_down;

        SELECT JSON_ARRAYAGG(edge_data) INTO v_edges_json FROM (
            -- Tx edges
            SELECT JSON_OBJECT(
                'source', fa.address, 'target', ta.address,
                'amount', ROUND(g.amount / POW(10, COALESCE(g.decimals, 9)), 6),
                'type', gt.type_code, 'category', gt.category,
                'token_symbol', COALESCE(tk.token_symbol, CASE WHEN g.token_id IS NULL THEN 'SOL' ELSE 'Unknown' END),
                'dex', s.name, 'pool_label', pool.pool_label,
                'signature', t.signature, 'block_time', t.block_time,
                'block_time_utc', FROM_UNIXTIME(t.block_time)
            ) AS edge_data
            FROM tx_guide g
            JOIN tmp_wallet_txs2 w ON w.tx_id = g.tx_id
            JOIN tx t ON t.id = g.tx_id
            JOIN tx_address fa ON fa.id = g.from_address_id
            JOIN tx_address ta ON ta.id = g.to_address_id
            JOIN tx_guide_type gt ON gt.id = g.edge_type_id
            LEFT JOIN tx_token tk ON tk.id = g.token_id
            LEFT JOIN tx_swap s ON s.tx_id = g.tx_id AND s.ins_index = g.ins_index
            LEFT JOIN tx_pool pool ON pool.id = s.amm_id
            WHERE v_token_id IS NULL OR g.token_id = v_token_id OR g.token_id IS NULL

            UNION ALL

            -- Funding edges UP
            SELECT JSON_OBJECT(
                'source', u.funded_by_address, 'target', u.address,
                'amount', 0, 'type', 'funded_by', 'category', 'funding',
                'token_symbol', 'SOL', 'dex', NULL, 'pool_label', NULL,
                'signature', NULL, 'block_time', NULL, 'block_time_utc', NULL
            ) FROM tmp_funding_up2 u WHERE u.funded_by_address IS NOT NULL

            UNION ALL

            -- Funding edges DOWN
            SELECT JSON_OBJECT(
                'source', n.address, 'target', d.address,
                'amount', 0, 'type', 'funded_by', 'category', 'funding',
                'token_symbol', 'SOL', 'dex', NULL, 'pool_label', NULL,
                'signature', NULL, 'block_time', NULL, 'block_time_utc', NULL
            ) FROM tmp_funding_down2 d
            JOIN tx_address a ON a.id = d.address_id
            JOIN tx_address n ON n.id = a.funded_by_address_id
            WHERE d.depth > 0
        ) edges;

        -- Return
        SELECT JSON_OBJECT('result', JSON_OBJECT(
            'wallet', JSON_OBJECT('address', p_wallet_address,
                'label', (SELECT COALESCE(label, address_type) FROM tx_address WHERE id = v_wallet_id)),
            'token', CASE WHEN v_token_id IS NOT NULL THEN
                JSON_OBJECT('mint', v_mint_address, 'symbol', v_token_symbol) ELSE NULL END,
            'stats', v_stats_json,
            'nodes', COALESCE(v_nodes_json, JSON_ARRAY()),
            'edges', COALESCE(v_edges_json, JSON_ARRAY())
        )) AS guide;

        -- Cleanup
        DROP TEMPORARY TABLE IF EXISTS tmp_funding_up;
        DROP TEMPORARY TABLE IF EXISTS tmp_funding_up2;
        DROP TEMPORARY TABLE IF EXISTS tmp_funding_down;
        DROP TEMPORARY TABLE IF EXISTS tmp_funding_down2;
        DROP TEMPORARY TABLE IF EXISTS tmp_wallet_txs;
        DROP TEMPORARY TABLE IF EXISTS tmp_wallet_txs2;
        DROP TEMPORARY TABLE IF EXISTS tmp_nodes;
    END IF;
END //

DELIMITER ;
