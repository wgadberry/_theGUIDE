DELIMITER //

DROP PROCEDURE IF EXISTS sp_tx_bmap_get_token_state//

CREATE DEFINER=`root`@`%` PROCEDURE `sp_tx_bmap_get_token_state`(
    IN p_token_name VARCHAR(128),
    IN p_token_symbol VARCHAR(128),
    IN p_mint_address VARCHAR(44),
    IN p_signature VARCHAR(88),
    IN p_block_time BIGINT UNSIGNED,
    IN p_tx_limit TINYINT UNSIGNED  
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
    DECLARE v_signature_only_mode TINYINT DEFAULT 0;  

    
    
    SET p_tx_limit = COALESCE(p_tx_limit, 10);
    IF p_tx_limit NOT IN (1, 10, 20, 50, 100) THEN
        SET p_tx_limit = 10;
    END IF;
    SET v_half_limit = p_tx_limit DIV 2;

    
    
    
    IF p_mint_address IS NOT NULL THEN
        SELECT tk.id, mint.address, tk.token_symbol, tk.decimals
        INTO v_token_id, v_mint_address, v_token_symbol, v_decimals
        FROM tx_token tk
        JOIN tx_address mint ON mint.id = tk.mint_address_id
        WHERE mint.address = p_mint_address
        LIMIT 1;
    ELSEIF p_token_symbol IS NOT NULL THEN
        
        SELECT tk.id, mint.address, tk.token_symbol, tk.decimals
        INTO v_token_id, v_mint_address, v_token_symbol, v_decimals
        FROM tx_token tk
        JOIN tx_address mint ON mint.id = tk.mint_address_id
        LEFT JOIN (SELECT token_id, COUNT(*) AS cnt FROM tx_guide GROUP BY token_id) g ON g.token_id = tk.id
        WHERE tk.token_symbol = p_token_symbol
        ORDER BY COALESCE(g.cnt, 0) DESC, (mint.address_type = 'mint') DESC, tk.id
        LIMIT 1;
    ELSEIF p_token_name IS NOT NULL THEN
        
        SELECT tk.id, mint.address, tk.token_symbol, tk.decimals
        INTO v_token_id, v_mint_address, v_token_symbol, v_decimals
        FROM tx_token tk
        JOIN tx_address mint ON mint.id = tk.mint_address_id
        LEFT JOIN (SELECT token_id, COUNT(*) AS cnt FROM tx_guide GROUP BY token_id) g ON g.token_id = tk.id
        WHERE tk.token_name = p_token_name
        ORDER BY COALESCE(g.cnt, 0) DESC, (mint.address_type = 'mint') DESC, tk.id
        LIMIT 1;
    ELSEIF p_signature IS NOT NULL THEN
        
        SET v_signature_only_mode = 1;

        
        
        SELECT t.id, t.signature, t.block_time, tk.id, mint.address, tk.token_symbol, tk.decimals
        INTO v_tx_id, v_signature, v_block_time, v_token_id, v_mint_address, v_token_symbol, v_decimals
        FROM tx t
        JOIN tx_guide g ON g.tx_id = t.id
        JOIN tx_token tk ON tk.id = g.token_id
        JOIN tx_address mint ON mint.id = tk.mint_address_id
        WHERE t.signature = p_signature
          AND g.token_id IS NOT NULL
          AND UPPER(COALESCE(tk.token_symbol, '')) NOT IN (
              'SOL', 'WSOL',                          
              'USDC', 'USDT', 'PYUSD', 'USDH', 'UXD', 
              'MSOL', 'JITOSOL', 'BSOL', 'LSTSOL'    
          )
        LIMIT 1;

        
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
        
        
        
        IF v_tx_id IS NOT NULL THEN
            
            SET v_tx_id = v_tx_id;  
        ELSEIF p_signature IS NOT NULL THEN
            
            SELECT t.id, t.signature, t.block_time
            INTO v_tx_id, v_signature, v_block_time
            FROM tx_guide g
            JOIN tx t ON t.id = g.tx_id
            WHERE t.signature = p_signature
              AND g.token_id = v_token_id
            LIMIT 1;

            IF v_tx_id IS NULL THEN
                
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
                
                SET v_token_id = NULL;
            END IF;
        ELSEIF p_block_time IS NOT NULL THEN
            
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
                
                SET v_token_id = NULL;
            END IF;
        ELSE
            
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
                
                SET v_token_id = NULL;
            END IF;
        END IF;
    END IF;

    IF v_token_id IS NOT NULL AND v_tx_id IS NOT NULL THEN
        
        
        SET v_signature_only_mode = 1;

        
        
        
        
        DROP TEMPORARY TABLE IF EXISTS tmp_window;
        CREATE TEMPORARY TABLE tmp_window (
            tx_id BIGINT PRIMARY KEY,
            signature VARCHAR(88),
            block_time BIGINT UNSIGNED,
            window_pos TINYINT  
        );

        
        INSERT INTO tmp_window (tx_id, signature, block_time, window_pos)
        VALUES (v_tx_id, v_signature, v_block_time, 0);

        
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

        
        
        
        
        
        DROP TEMPORARY TABLE IF EXISTS tmp_display_window;
        CREATE TEMPORARY TABLE tmp_display_window AS
        SELECT * FROM tmp_window
        WHERE window_pos = 0
           OR (window_pos < 0 AND window_pos >= -v_half_limit)
           OR (window_pos > 0 AND window_pos <= v_half_limit);

        
        
        

        
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

        
        SELECT JSON_ARRAYAGG(dt.type_code) INTO v_current_edge_types
        FROM (
            SELECT DISTINCT gt.type_code
            FROM tx_guide g
            JOIN tx_guide_type gt ON gt.id = g.edge_type_id
            WHERE g.token_id = v_token_id
              AND g.tx_id = v_tx_id
        ) dt;

        
        
        
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
            funded_by VARCHAR(44) DEFAULT NULL,
            
            interactions_pools JSON DEFAULT NULL,
            interactions_programs JSON DEFAULT NULL,
            interactions_dexes JSON DEFAULT NULL
        );

        
        
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

        
        INSERT IGNORE INTO tmp_nodes (address_id, address, label, address_type, balance, funded_by)
        SELECT DISTINCT
            pool_addr.id,
            pool_addr.address,
            COALESCE(pool_addr.label, 'pool'),
            COALESCE(pool_addr.address_type, 'pool'),
            0,
            NULL
        FROM tx_swap s
        JOIN tmp_display_window w ON w.tx_id = s.tx_id
        JOIN tx_pool p ON p.id = s.amm_id
        JOIN tx_address pool_addr ON pool_addr.id = p.pool_address_id
        WHERE s.amm_id IS NOT NULL;

        
        UPDATE tmp_nodes n
        JOIN tx_pool p ON p.pool_address_id = n.address_id
        SET n.pool_label = p.pool_label
        WHERE p.pool_label IS NOT NULL;

        
        UPDATE tmp_nodes n
        JOIN tx_token tk ON tk.mint_address_id = n.address_id
        SET n.token_name = tk.token_name
        WHERE tk.token_name IS NOT NULL;

        -- ============================================================
        -- BALANCE LOOKUPS: Read directly from tx_guide (deprecates tx_bmap_state)
        -- ============================================================

        -- Token balance: Read from tx_guide's post_balance columns
        UPDATE tmp_nodes n
        SET balance = COALESCE((
            SELECT CASE
                WHEN g.from_address_id = n.address_id AND g.from_token_post_balance IS NOT NULL
                    THEN g.from_token_post_balance / POW(10, COALESCE(g.decimals, 9))
                WHEN g.to_address_id = n.address_id AND g.to_token_post_balance IS NOT NULL
                    THEN g.to_token_post_balance / POW(10, COALESCE(g.decimals, 9))
                ELSE NULL
            END
            FROM tx_guide g
            WHERE g.token_id = v_token_id
              AND (g.from_address_id = n.address_id OR g.to_address_id = n.address_id)
              AND g.block_time <= v_block_time
              AND (g.from_token_post_balance IS NOT NULL OR g.to_token_post_balance IS NOT NULL)
            ORDER BY g.block_time DESC, g.id DESC
            LIMIT 1
        ), 0);

        -- SOL balance: Read from tx_guide's sol_post_balance columns
        UPDATE tmp_nodes n
        SET sol_balance = COALESCE((
            SELECT CASE
                WHEN g.from_address_id = n.address_id AND g.from_sol_post_balance IS NOT NULL
                    THEN g.from_sol_post_balance / 1e9
                WHEN g.to_address_id = n.address_id AND g.to_sol_post_balance IS NOT NULL
                    THEN g.to_sol_post_balance / 1e9
                ELSE NULL
            END
            FROM tx_guide g
            WHERE (g.from_address_id = n.address_id OR g.to_address_id = n.address_id)
              AND g.block_time <= v_block_time
              AND (g.from_sol_post_balance IS NOT NULL OR g.to_sol_post_balance IS NOT NULL)
            ORDER BY g.block_time DESC, g.id DESC
            LIMIT 1
        ), 0);

        
        
        
        

        
        DROP TEMPORARY TABLE IF EXISTS tmp_display_window_int;
        CREATE TEMPORARY TABLE tmp_display_window_int AS SELECT * FROM tmp_display_window;

        
        
        UPDATE tmp_nodes n
        SET interactions_pools = (
            SELECT JSON_ARRAYAGG(pool_info)
            FROM (
                SELECT DISTINCT JSON_OBJECT(
                    'address', pool_addr.address,
                    'label', COALESCE(p.pool_label, pool_addr.address)
                ) AS pool_info
                FROM tx_guide g
                JOIN tmp_display_window_int w ON w.tx_id = g.tx_id
                JOIN tx_swap s ON s.tx_id = g.tx_id AND s.ins_index = g.ins_index
                JOIN tx_pool p ON p.id = s.amm_id
                JOIN tx_address pool_addr ON pool_addr.id = p.pool_address_id
                WHERE (g.from_address_id = n.address_id OR g.to_address_id = n.address_id)
                  AND s.amm_id IS NOT NULL
                GROUP BY pool_addr.address, p.pool_label
            ) pools
        )
        WHERE n.address_type IN ('wallet', 'ata', 'unknown') OR n.address_type IS NULL;

        DROP TEMPORARY TABLE IF EXISTS tmp_display_window_int;
        CREATE TEMPORARY TABLE tmp_display_window_int AS SELECT * FROM tmp_display_window;

        
        UPDATE tmp_nodes n
        SET interactions_programs = (
            SELECT JSON_ARRAYAGG(prog_address)
            FROM (
                SELECT DISTINCT prog_addr.address AS prog_address
                FROM tx_guide g
                JOIN tmp_display_window_int w ON w.tx_id = g.tx_id
                LEFT JOIN tx_swap s ON s.tx_id = g.tx_id AND s.ins_index = g.ins_index
                LEFT JOIN tx_transfer xf ON xf.tx_id = g.tx_id AND xf.ins_index = g.ins_index
                LEFT JOIN tx_program prog ON prog.id = COALESCE(s.program_id, xf.program_id)
                LEFT JOIN tx_address prog_addr ON prog_addr.id = prog.program_address_id
                WHERE (g.from_address_id = n.address_id OR g.to_address_id = n.address_id)
                  AND prog_addr.address IS NOT NULL
            ) progs
        )
        WHERE n.address_type IN ('wallet', 'ata', 'unknown') OR n.address_type IS NULL;

        DROP TEMPORARY TABLE IF EXISTS tmp_display_window_int;
        CREATE TEMPORARY TABLE tmp_display_window_int AS SELECT * FROM tmp_display_window;

        
        UPDATE tmp_nodes n
        SET interactions_dexes = (
            SELECT JSON_ARRAYAGG(dex_name)
            FROM (
                SELECT DISTINCT s.name AS dex_name
                FROM tx_guide g
                JOIN tmp_display_window_int w ON w.tx_id = g.tx_id
                JOIN tx_swap s ON s.tx_id = g.tx_id AND s.ins_index = g.ins_index
                WHERE (g.from_address_id = n.address_id OR g.to_address_id = n.address_id)
                  AND s.name IS NOT NULL
            ) dexes
        )
        WHERE n.address_type IN ('wallet', 'ata', 'unknown') OR n.address_type IS NULL;

        DROP TEMPORARY TABLE IF EXISTS tmp_display_window_int;

        
        SELECT JSON_ARRAYAGG(
            JSON_OBJECT(
                'address', n.address,
                'label', n.label,
                'address_type', n.address_type,
                'pool_label', n.pool_label,
                'token_name', n.token_name,
                'balance', ROUND(n.balance, 6),
                'sol_balance', ROUND(COALESCE(n.sol_balance, 0), 9),
                'funded_by', n.funded_by,
                'interactions', JSON_OBJECT(
                    'pools', COALESCE(n.interactions_pools, JSON_ARRAY()),
                    'programs', COALESCE(n.interactions_programs, JSON_ARRAY()),
                    'dexes', COALESCE(n.interactions_dexes, JSON_ARRAY())
                )
            )
        ) INTO v_nodes_json
        FROM tmp_nodes n;

        
        
        
        

        
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
            
            
            
            SELECT JSON_OBJECT(
                'source', CASE
                    WHEN gt.type_code = 'swap_in' THEN fa.address  
                    ELSE COALESCE(pool_addr.address, fa.address)   
                END,
                'source_label', CASE
                    WHEN gt.type_code = 'swap_in' THEN COALESCE(fa.label, fa.address_type)
                    ELSE COALESCE(pool.pool_label, pool_addr.address, fa.label, fa.address_type)
                END,
                'target', CASE
                    WHEN gt.type_code = 'swap_in' THEN COALESCE(pool_addr.address, ta.address)  
                    ELSE ta.address  
                END,
                'target_label', CASE
                    WHEN gt.type_code = 'swap_in' THEN COALESCE(pool.pool_label, pool_addr.address, ta.label, ta.address_type)
                    ELSE COALESCE(ta.label, ta.address_type)
                END,
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

        
        
        

        
        DROP TEMPORARY TABLE IF EXISTS tmp_swap_txs;
        CREATE TEMPORARY TABLE tmp_swap_txs AS
        SELECT DISTINCT g.tx_id
        FROM tx_guide g
        JOIN tmp_display_window w ON w.tx_id = g.tx_id
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        WHERE g.token_id = v_token_id
          AND gt.type_code IN ('swap_in', 'swap_out');

        
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

        
        DROP TEMPORARY TABLE IF EXISTS tmp_nodes;
        DROP TEMPORARY TABLE IF EXISTS tmp_window;
        DROP TEMPORARY TABLE IF EXISTS tmp_display_window;
    END IF;
END
//

DELIMITER ;
