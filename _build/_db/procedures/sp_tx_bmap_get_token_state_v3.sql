-- sp_tx_bmap_get_token_state_v3: Time-navigable cluster map for token (V2)
-- Identical to v1 - exists for rollback safety with v2 viewer
--
-- Parameters:
--   p_token_name: Token name (optional)
--   p_token_symbol: Token symbol (optional)
--   p_mint_address: Token mint address (preferred)
--   p_signature: Transaction signature (optional - if NULL, uses p_block_time or most recent)
--   p_block_time: Unix timestamp (optional - find nearest tx; ignored if p_signature provided)
--
-- Token Resolution:
--   1. If p_mint_address provided, use it
--   2. Else if p_token_symbol provided, use it
--   3. Else if p_token_name provided, use it
--   4. Else if p_signature provided (only), derive token from tx_guide activity
--
-- Transaction Resolution:
--   1. If p_signature provided, use that tx (p_block_time ignored)
--   2. Else if p_block_time provided, find nearest tx to that time
--   3. Else use most recent tx
--
-- Usage:
--   CALL sp_tx_bmap_get_token_state_v3(NULL, 'BONK', NULL, NULL, NULL, NULL);
--   CALL sp_tx_bmap_get_token_state_v3(NULL, NULL, 'DezXAZ...', '5KtP...abc', NULL, NULL);
--   CALL sp_tx_bmap_get_token_state_v3(NULL, NULL, NULL, '5KtP...abc', NULL, NULL);  -- derive token from sig
--   CALL sp_tx_bmap_get_token_state_v3(NULL, 'BONK', NULL, NULL, 1703000000, 20);  -- 20 tx window

DROP PROCEDURE IF EXISTS sp_tx_bmap_get_token_state_v3;

DELIMITER //

CREATE PROCEDURE sp_tx_bmap_get_token_state_v3(
    IN p_token_name VARCHAR(128),
    IN p_token_symbol VARCHAR(128),
    IN p_mint_address VARCHAR(44),
    IN p_signature VARCHAR(88),
    IN p_block_time BIGINT UNSIGNED,
    IN p_tx_limit TINYINT UNSIGNED  -- 10, 20, 50, 100 (divided by 2 for prev/next)
)
BEGIN
    DECLARE v_token_id BIGINT;
    DECLARE v_mint_address VARCHAR(44);
    DECLARE v_token_symbol VARCHAR(128);
    DECLARE v_decimals TINYINT UNSIGNED;

    DECLARE v_signature VARCHAR(88);
    DECLARE v_block_time BIGINT UNSIGNED;
    DECLARE v_tx_id BIGINT;

    DECLARE v_half_limit TINYINT UNSIGNED;

    DECLARE v_nodes_json JSON;
    DECLARE v_edges_json JSON;
    DECLARE v_prev_json JSON;
    DECLARE v_next_json JSON;
    DECLARE v_current_edge_types JSON;
    DECLARE v_related_tokens_json JSON;
    DECLARE v_signature_only_mode TINYINT DEFAULT 0;  -- 1 = show all tokens in tx

    -- Default tx_limit to 10, validate, and calculate half
    -- Valid values: 1 (single tx), 10, 20, 50, 100
    SET p_tx_limit = COALESCE(p_tx_limit, 10);
    IF p_tx_limit NOT IN (1, 10, 20, 50, 100) THEN
        SET p_tx_limit = 10;
    END IF;
    SET v_half_limit = p_tx_limit DIV 2;

    -- ==========================================================================
    -- STEP 1: Resolve token
    -- ==========================================================================
    IF p_mint_address IS NOT NULL THEN
        SELECT tk.id, mint.address, tk.token_symbol, tk.decimals
        INTO v_token_id, v_mint_address, v_token_symbol, v_decimals
        FROM tx_token tk
        JOIN tx_address mint ON mint.id = tk.mint_address_id
        WHERE mint.address = p_mint_address
        LIMIT 1;
    ELSEIF p_token_symbol IS NOT NULL THEN
        -- Prefer tokens with: 1) address_type='mint', 2) actual tx_guide activity
        SELECT tk.id, mint.address, tk.token_symbol, tk.decimals
        INTO v_token_id, v_mint_address, v_token_symbol, v_decimals
        FROM tx_token tk
        JOIN tx_address mint ON mint.id = tk.mint_address_id
        LEFT JOIN (SELECT token_id, COUNT(*) AS cnt FROM tx_guide GROUP BY token_id) g ON g.token_id = tk.id
        WHERE tk.token_symbol = p_token_symbol
        ORDER BY COALESCE(g.cnt, 0) DESC, (mint.address_type = 'mint') DESC, tk.id
        LIMIT 1;
    ELSEIF p_token_name IS NOT NULL THEN
        -- Prefer tokens with: 1) address_type='mint', 2) actual tx_guide activity
        SELECT tk.id, mint.address, tk.token_symbol, tk.decimals
        INTO v_token_id, v_mint_address, v_token_symbol, v_decimals
        FROM tx_token tk
        JOIN tx_address mint ON mint.id = tk.mint_address_id
        LEFT JOIN (SELECT token_id, COUNT(*) AS cnt FROM tx_guide GROUP BY token_id) g ON g.token_id = tk.id
        WHERE tk.token_name = p_token_name
        ORDER BY COALESCE(g.cnt, 0) DESC, (mint.address_type = 'mint') DESC, tk.id
        LIMIT 1;
    ELSEIF p_signature IS NOT NULL THEN
        -- Signature-only mode: show ALL tokens in the transaction
        SET v_signature_only_mode = 1;

        -- Derive primary token from signature's tx_guide activity (for display only)
        -- Prefer "interesting" tokens over base currencies (SOL, WSOL, stablecoins)
        SELECT t.id, t.signature, t.block_time, tk.id, mint.address, tk.token_symbol, tk.decimals
        INTO v_tx_id, v_signature, v_block_time, v_token_id, v_mint_address, v_token_symbol, v_decimals
        FROM tx t
        JOIN tx_guide g ON g.tx_id = t.id
        JOIN tx_token tk ON tk.id = g.token_id
        JOIN tx_address mint ON mint.id = tk.mint_address_id
        WHERE t.signature = p_signature
          AND g.token_id IS NOT NULL
          AND UPPER(COALESCE(tk.token_symbol, '')) NOT IN (
              'SOL', 'WSOL',                          -- Native/Wrapped SOL
              'USDC', 'USDT', 'PYUSD', 'USDH', 'UXD', -- Stablecoins
              'MSOL', 'JITOSOL', 'BSOL', 'LSTSOL'    -- Liquid staking tokens
          )
        LIMIT 1;

        -- Fallback: if no interesting token found, take any token
        IF v_token_id IS NULL THEN
            SELECT t.id, t.signature, t.block_time, tk.id, mint.address, tk.token_symbol, tk.decimals
            INTO v_tx_id, v_signature, v_block_time, v_token_id, v_mint_address, v_token_symbol, v_decimals
            FROM tx t
            JOIN tx_guide g ON g.tx_id = t.id
            JOIN tx_token tk ON tk.id = g.token_id
            JOIN tx_address mint ON mint.id = tk.mint_address_id
            WHERE t.signature = p_signature
              AND g.token_id IS NOT NULL
            LIMIT 1;
        END IF;
    END IF;

    IF v_token_id IS NULL THEN
        SELECT JSON_OBJECT('result', JSON_OBJECT('error', 'Token not found')) AS guide;
    ELSE
        -- ==========================================================================
        -- STEP 2: Resolve current signature and block_time
        -- ==========================================================================
        IF v_tx_id IS NOT NULL THEN
            -- Already resolved from signature-only call in STEP 1
            SET v_tx_id = v_tx_id;  -- no-op, already set
        ELSEIF p_signature IS NOT NULL THEN
            -- Verify signature exists AND relates to this token (p_block_time ignored)
            SELECT t.id, t.signature, t.block_time
            INTO v_tx_id, v_signature, v_block_time
            FROM tx_guide g
            JOIN tx t ON t.id = g.tx_id
            WHERE t.signature = p_signature
              AND g.token_id = v_token_id
            LIMIT 1;

            IF v_tx_id IS NULL THEN
                -- Check if signature exists at all (just not for this token)
                IF EXISTS (SELECT 1 FROM tx WHERE signature = p_signature) THEN
                    SELECT JSON_OBJECT('result', JSON_OBJECT(
                        'error', 'Signature not found for this token',
                        'signature', p_signature,
                        'mint', v_mint_address
                    )) AS guide;
                ELSE
                    SELECT JSON_OBJECT('result', JSON_OBJECT(
                        'error', 'Signature not found',
                        'signature', p_signature
                    )) AS guide;
                END IF;
                -- Exit early
                SET v_token_id = NULL;
            END IF;
        ELSEIF p_block_time IS NOT NULL THEN
            -- Find nearest tx to provided block_time for this token
            SELECT t.id, t.signature, t.block_time
            INTO v_tx_id, v_signature, v_block_time
            FROM tx_guide g
            JOIN tx t ON t.id = g.tx_id
            WHERE g.token_id = v_token_id
            ORDER BY ABS(CAST(t.block_time AS SIGNED) - CAST(p_block_time AS SIGNED))
            LIMIT 1;

            IF v_tx_id IS NULL THEN
                SELECT JSON_OBJECT('result', JSON_OBJECT(
                    'error', 'No transactions found for this token',
                    'mint', v_mint_address
                )) AS guide;
                -- Exit early
                SET v_token_id = NULL;
            END IF;
        ELSE
            -- Most recent tx for this token
            SELECT t.id, t.signature, t.block_time
            INTO v_tx_id, v_signature, v_block_time
            FROM tx_guide g
            JOIN tx t ON t.id = g.tx_id
            WHERE g.token_id = v_token_id
            ORDER BY t.block_time DESC
            LIMIT 1;

            IF v_tx_id IS NULL THEN
                SELECT JSON_OBJECT('result', JSON_OBJECT(
                    'error', 'No transactions found for this token',
                    'mint', v_mint_address
                )) AS guide;
                -- Exit early
                SET v_token_id = NULL;
            END IF;
        END IF;
    END IF;

    IF v_token_id IS NOT NULL AND v_tx_id IS NOT NULL THEN
        -- Always show all tokens in the transaction (not just the searched token)
        -- The token_id is used for navigation (finding prev/next), but display is unfiltered
        SET v_signature_only_mode = 1;

        -- ==========================================================================
        -- STEP 3: Build sliding window of tx_ids (prev N + current + next N)
        -- Always fetch 5 prev + 5 next for NAVIGATION, but filter nodes/edges by v_half_limit
        -- ==========================================================================
        DROP TEMPORARY TABLE IF EXISTS tmp_window;
        CREATE TEMPORARY TABLE tmp_window (
            tx_id BIGINT PRIMARY KEY,
            signature VARCHAR(88),
            block_time BIGINT UNSIGNED,
            window_pos TINYINT  -- negative = prev, 0 = current, positive = next
        );

        -- Current tx
        INSERT INTO tmp_window (tx_id, signature, block_time, window_pos)
        VALUES (v_tx_id, v_signature, v_block_time, 0);

        -- Previous 5 (always fetch 5 for navigation, use v_half_limit for display filtering)
        SET @sql_prev = CONCAT('
            INSERT INTO tmp_window (tx_id, signature, block_time, window_pos)
            SELECT t.id, t.signature, t.block_time,
                   -1 * (@prev_row := @prev_row + 1)
            FROM (SELECT @prev_row := 0) init,
                 (
                    SELECT DISTINCT t.id, t.signature, t.block_time
                    FROM tx_guide g
                    JOIN tx t ON t.id = g.tx_id
                    WHERE g.token_id = ', v_token_id, '
                      AND t.block_time < ', v_block_time, '
                    ORDER BY t.block_time DESC
                    LIMIT 5
                 ) t
            ORDER BY t.block_time DESC
        ');
        PREPARE stmt_prev FROM @sql_prev;
        EXECUTE stmt_prev;
        DEALLOCATE PREPARE stmt_prev;

        -- Next 5 (always fetch 5 for navigation, use v_half_limit for display filtering)
        SET @sql_next = CONCAT('
            INSERT INTO tmp_window (tx_id, signature, block_time, window_pos)
            SELECT t.id, t.signature, t.block_time,
                   @next_row := @next_row + 1
            FROM (SELECT @next_row := 0) init,
                 (
                    SELECT DISTINCT t.id, t.signature, t.block_time
                    FROM tx_guide g
                    JOIN tx t ON t.id = g.tx_id
                    WHERE g.token_id = ', v_token_id, '
                      AND t.block_time > ', v_block_time, '
                    ORDER BY t.block_time ASC
                    LIMIT 5
                 ) t
            ORDER BY t.block_time ASC
        ');
        PREPARE stmt_next FROM @sql_next;
        EXECUTE stmt_next;
        DEALLOCATE PREPARE stmt_next;

        -- ==========================================================================
        -- STEP 3b: Create display window (filtered by v_half_limit for nodes/edges)
        -- Navigation uses full tmp_window (5 prev + 5 next)
        -- Display uses tmp_display_window (v_half_limit prev + v_half_limit next)
        -- ==========================================================================
        DROP TEMPORARY TABLE IF EXISTS tmp_display_window;
        CREATE TEMPORARY TABLE tmp_display_window AS
        SELECT * FROM tmp_window
        WHERE window_pos = 0
           OR (window_pos < 0 AND window_pos >= -v_half_limit)
           OR (window_pos > 0 AND window_pos <= v_half_limit);

        -- ==========================================================================
        -- STEP 4: Build prev/next navigation JSON from window
        -- ==========================================================================

        -- Previous 5 with edge_types
        SELECT JSON_ARRAYAGG(nav_data) INTO v_prev_json
        FROM (
            SELECT JSON_OBJECT(
                'signature', w.signature,
                'block_time', w.block_time,
                'block_time_utc', FROM_UNIXTIME(w.block_time),
                'edge_types', (
                    SELECT JSON_ARRAYAGG(dt.type_code)
                    FROM (
                        SELECT DISTINCT gt.type_code
                        FROM tx_guide g
                        JOIN tx_guide_type gt ON gt.id = g.edge_type_id
                        WHERE g.token_id = v_token_id
                          AND g.tx_id = w.tx_id
                    ) dt
                )
            ) AS nav_data
            FROM tmp_window w
            WHERE w.window_pos < 0
            ORDER BY w.block_time DESC
        ) nav_outer;

        -- Next 5 with edge_types
        SELECT JSON_ARRAYAGG(nav_data) INTO v_next_json
        FROM (
            SELECT JSON_OBJECT(
                'signature', w.signature,
                'block_time', w.block_time,
                'block_time_utc', FROM_UNIXTIME(w.block_time),
                'edge_types', (
                    SELECT JSON_ARRAYAGG(dt.type_code)
                    FROM (
                        SELECT DISTINCT gt.type_code
                        FROM tx_guide g
                        JOIN tx_guide_type gt ON gt.id = g.edge_type_id
                        WHERE g.token_id = v_token_id
                          AND g.tx_id = w.tx_id
                    ) dt
                )
            ) AS nav_data
            FROM tmp_window w
            WHERE w.window_pos > 0
            ORDER BY w.block_time ASC
        ) nav_outer;

        -- Current frame edge_types
        SELECT JSON_ARRAYAGG(dt.type_code) INTO v_current_edge_types
        FROM (
            SELECT DISTINCT gt.type_code
            FROM tx_guide g
            JOIN tx_guide_type gt ON gt.id = g.edge_type_id
            WHERE g.token_id = v_token_id
              AND g.tx_id = v_tx_id
        ) dt;

        -- ==========================================================================
        -- STEP 5: Build nodes (all addresses in sliding window, with their balance)
        -- ==========================================================================
        DROP TEMPORARY TABLE IF EXISTS tmp_nodes;
        CREATE TEMPORARY TABLE tmp_nodes (
            address_id INT UNSIGNED PRIMARY KEY,
            address VARCHAR(44),
            label VARCHAR(200),
            address_type VARCHAR(30),
            pool_label VARCHAR(255) DEFAULT NULL,
            token_name VARCHAR(255) DEFAULT NULL,
            balance DECIMAL(30,9) DEFAULT 0,
            sol_balance DECIMAL(20,9) DEFAULT NULL,
            funded_by VARCHAR(44) DEFAULT NULL
        );

        -- First: Insert all addresses that appear in the DISPLAY window (from OR to)
        -- Uses tmp_display_window (filtered by tx_limit) not tmp_window (full nav window)
        INSERT IGNORE INTO tmp_nodes (address_id, address, label, address_type, balance, funded_by)
        SELECT DISTINCT
            a.id,
            a.address,
            COALESCE(a.label, a.address_type),
            a.address_type,
            0,
            f.address
        FROM tx_guide g
        JOIN tmp_display_window w ON w.tx_id = g.tx_id
        JOIN tx_address a ON a.id = g.from_address_id
        LEFT JOIN tx_address f ON f.id = a.funded_by_address_id
        WHERE (v_signature_only_mode = 1 OR g.token_id = v_token_id);

        INSERT IGNORE INTO tmp_nodes (address_id, address, label, address_type, balance, funded_by)
        SELECT DISTINCT
            a.id,
            a.address,
            COALESCE(a.label, a.address_type),
            a.address_type,
            0,
            f.address
        FROM tx_guide g
        JOIN tmp_display_window w ON w.tx_id = g.tx_id
        JOIN tx_address a ON a.id = g.to_address_id
        LEFT JOIN tx_address f ON f.id = a.funded_by_address_id
        WHERE (v_signature_only_mode = 1 OR g.token_id = v_token_id);

        -- Enrich pool nodes with pool_label
        UPDATE tmp_nodes n
        JOIN tx_pool p ON p.pool_address_id = n.address_id
        SET n.pool_label = p.pool_label
        WHERE p.pool_label IS NOT NULL;

        -- Enrich mint nodes with token_name
        UPDATE tmp_nodes n
        JOIN tx_token tk ON tk.mint_address_id = n.address_id
        SET n.token_name = tk.token_name
        WHERE tk.token_name IS NOT NULL;

        -- Now calculate historical balance for each node: inflows - outflows up to current block_time
        -- Inflows (to_address receives tokens)
        UPDATE tmp_nodes n
        SET balance = balance + COALESCE((
            SELECT SUM(g.amount / POW(10, g.decimals))
            FROM tx_guide g
            JOIN tx t ON t.id = g.tx_id
            WHERE g.token_id = v_token_id
              AND g.to_address_id = n.address_id
              AND t.block_time <= v_block_time
        ), 0);

        -- Outflows (from_address sends tokens) - subtract from balance
        UPDATE tmp_nodes n
        SET balance = balance - COALESCE((
            SELECT SUM(g.amount / POW(10, g.decimals))
            FROM tx_guide g
            JOIN tx t ON t.id = g.tx_id
            WHERE g.token_id = v_token_id
              AND g.from_address_id = n.address_id
              AND t.block_time <= v_block_time
        ), 0);

        -- Update SOL balance for each node (latest balance up to current block_time)
        UPDATE tmp_nodes n
        SET sol_balance = (
            SELECT sbc.post_balance / 1e9
            FROM tx_sol_balance_change sbc
            JOIN tx t ON t.id = sbc.tx_id
            WHERE sbc.address_id = n.address_id
              AND t.block_time <= v_block_time
            ORDER BY t.block_time DESC, sbc.id DESC
            LIMIT 1
        );

        -- Build JSON for ALL addresses in sliding window (not just balance > 0)
        SELECT JSON_ARRAYAGG(
            JSON_OBJECT(
                'address', n.address,
                'label', n.label,
                'address_type', n.address_type,
                'pool_label', n.pool_label,
                'token_name', n.token_name,
                'balance', ROUND(n.balance, 6),
                'sol_balance', ROUND(COALESCE(n.sol_balance, 0), 9),
                'funded_by', n.funded_by
            )
        ) INTO v_nodes_json
        FROM tmp_nodes n;

        -- ==========================================================================
        -- STEP 6: Build edges (enriched with swap/transfer details)
        -- Uses tmp_display_window (filtered by tx_limit) for edges
        -- ==========================================================================

        -- Copy temp tables to avoid MySQL "Can't reopen table" limitation
        DROP TEMPORARY TABLE IF EXISTS tmp_window2;
        CREATE TEMPORARY TABLE tmp_window2 AS SELECT * FROM tmp_display_window;

        DROP TEMPORARY TABLE IF EXISTS tmp_window3;
        CREATE TEMPORARY TABLE tmp_window3 AS SELECT * FROM tmp_display_window;

        DROP TEMPORARY TABLE IF EXISTS tmp_nodes_from;
        CREATE TEMPORARY TABLE tmp_nodes_from AS SELECT address_id FROM tmp_nodes;

        DROP TEMPORARY TABLE IF EXISTS tmp_nodes_to;
        CREATE TEMPORARY TABLE tmp_nodes_to AS SELECT address_id FROM tmp_nodes;

        SELECT JSON_ARRAYAGG(edge_data) INTO v_edges_json
        FROM (
            -- Swap edges (swap_in, swap_out) - enriched with DEX name, pool, program
            SELECT JSON_OBJECT(
                'source', fa.address,
                'source_label', COALESCE(fa.label, fa.address_type),
                'target', ta.address,
                'target_label', COALESCE(ta.label, ta.address_type),
                'amount', ROUND(g.amount / POW(10, COALESCE(g.decimals, 9)), 6),
                'type', gt.type_code,
                'category', gt.category,
                'token_symbol', COALESCE(tk.token_symbol, CASE WHEN g.token_id IS NULL THEN 'SOL' ELSE 'Unknown' END),
                'dex', s.name,
                'pool', pool_addr.address,
                'pool_label', pool.pool_label,
                'program', prog_addr.address,
                'ins_index', g.ins_index,
                'signature', t.signature,
                'block_time', t.block_time,
                'block_time_utc', FROM_UNIXTIME(t.block_time)
            ) AS edge_data
            FROM tx_guide g
            JOIN tmp_display_window w ON w.tx_id = g.tx_id
            JOIN tx t ON t.id = g.tx_id
            JOIN tx_address fa ON fa.id = g.from_address_id
            JOIN tx_address ta ON ta.id = g.to_address_id
            JOIN tx_guide_type gt ON gt.id = g.edge_type_id
            LEFT JOIN tx_token tk ON tk.id = g.token_id
            LEFT JOIN tx_swap s ON s.tx_id = g.tx_id AND s.ins_index = g.ins_index
            LEFT JOIN tx_pool pool ON pool.id = s.amm_id
            LEFT JOIN tx_address pool_addr ON pool_addr.id = pool.pool_address_id
            LEFT JOIN tx_program prog ON prog.id = s.program_id
            LEFT JOIN tx_address prog_addr ON prog_addr.id = prog.program_address_id
            WHERE (v_signature_only_mode = 1 OR g.token_id = v_token_id)
              AND gt.type_code IN ('swap_in', 'swap_out')

            UNION ALL

            -- ALL other edge types (transfers, burns, mints, stake, liquidity, etc.)
            SELECT JSON_OBJECT(
                'source', fa.address,
                'source_label', COALESCE(fa.label, fa.address_type),
                'target', ta.address,
                'target_label', COALESCE(ta.label, ta.address_type),
                'amount', ROUND(g.amount / POW(10, COALESCE(g.decimals, 9)), 6),
                'type', gt.type_code,
                'category', gt.category,
                'token_symbol', COALESCE(tk.token_symbol, CASE WHEN g.token_id IS NULL THEN 'SOL' ELSE 'Unknown' END),
                'dex', NULL,
                'pool', NULL,
                'pool_label', NULL,
                'program', prog_addr.address,
                'ins_index', g.ins_index,
                'signature', t.signature,
                'block_time', t.block_time,
                'block_time_utc', FROM_UNIXTIME(t.block_time)
            ) AS edge_data
            FROM tx_guide g
            JOIN tmp_window2 w ON w.tx_id = g.tx_id
            JOIN tx t ON t.id = g.tx_id
            JOIN tx_address fa ON fa.id = g.from_address_id
            JOIN tx_address ta ON ta.id = g.to_address_id
            JOIN tx_guide_type gt ON gt.id = g.edge_type_id
            LEFT JOIN tx_token tk ON tk.id = g.token_id
            LEFT JOIN tx_transfer xf ON xf.tx_id = g.tx_id AND xf.ins_index = g.ins_index
            LEFT JOIN tx_program prog ON prog.id = xf.program_id
            LEFT JOIN tx_address prog_addr ON prog_addr.id = prog.program_address_id
            WHERE (v_signature_only_mode = 1 OR g.token_id = v_token_id OR g.token_id IS NULL)
              AND gt.type_code NOT IN ('swap_in', 'swap_out')
        ) edges;

        DROP TEMPORARY TABLE IF EXISTS tmp_window2;
        DROP TEMPORARY TABLE IF EXISTS tmp_window3;
        DROP TEMPORARY TABLE IF EXISTS tmp_nodes_from;
        DROP TEMPORARY TABLE IF EXISTS tmp_nodes_to;

        -- ==========================================================================
        -- STEP 7: Find related tokens (other tokens swapped in same transactions)
        -- ==========================================================================

        -- Get tx_ids from display window where our target token has swap activity
        DROP TEMPORARY TABLE IF EXISTS tmp_swap_txs;
        CREATE TEMPORARY TABLE tmp_swap_txs AS
        SELECT DISTINCT g.tx_id
        FROM tx_guide g
        JOIN tmp_display_window w ON w.tx_id = g.tx_id
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        WHERE g.token_id = v_token_id
          AND gt.type_code IN ('swap_in', 'swap_out');

        -- Find other tokens in those same transactions (with swap edges)
        SELECT JSON_ARRAYAGG(related_data) INTO v_related_tokens_json
        FROM (
            SELECT JSON_OBJECT(
                'mint', mint_addr.address,
                'symbol', COALESCE(tk.token_symbol, 'Unknown'),
                'name', tk.token_name,
                'swap_count', COUNT(DISTINCT g.tx_id),
                'total_volume', ROUND(SUM(g.amount / POW(10, COALESCE(g.decimals, 9))), 6)
            ) AS related_data
            FROM tx_guide g
            JOIN tmp_swap_txs stx ON stx.tx_id = g.tx_id
            JOIN tx_guide_type gt ON gt.id = g.edge_type_id
            JOIN tx_token tk ON tk.id = g.token_id
            JOIN tx_address mint_addr ON mint_addr.id = tk.mint_address_id
            WHERE g.token_id IS NOT NULL
              AND g.token_id != v_token_id
              AND gt.type_code IN ('swap_in', 'swap_out')
            GROUP BY tk.id, mint_addr.address, tk.token_symbol, tk.token_name
            ORDER BY COUNT(DISTINCT g.tx_id) DESC, SUM(g.amount) DESC
            LIMIT 20
        ) related_outer;

        DROP TEMPORARY TABLE IF EXISTS tmp_swap_txs;

        -- ==========================================================================
        -- STEP 8: Return JSON
        -- ==========================================================================
        SELECT JSON_OBJECT(
            'result', JSON_OBJECT(
                'txs', JSON_OBJECT(
                    'signature', v_signature,
                    'block_time', v_block_time,
                    'block_time_utc', FROM_UNIXTIME(v_block_time),
                    'edge_types', COALESCE(v_current_edge_types, JSON_ARRAY()),
                    'prev', COALESCE(v_prev_json, JSON_ARRAY()),
                    'next', COALESCE(v_next_json, JSON_ARRAY())
                ),
                'token', JSON_OBJECT(
                    'mint', v_mint_address,
                    'symbol', v_token_symbol
                ),
                'nodes', COALESCE(v_nodes_json, JSON_ARRAY()),
                'edges', COALESCE(v_edges_json, JSON_ARRAY()),
                'related_tokens', COALESCE(v_related_tokens_json, JSON_ARRAY())
            )
        ) AS guide;

        -- Cleanup
        DROP TEMPORARY TABLE IF EXISTS tmp_nodes;
        DROP TEMPORARY TABLE IF EXISTS tmp_window;
        DROP TEMPORARY TABLE IF EXISTS tmp_display_window;
    END IF;
END //

DELIMITER ;
