-- sp_tx_bmap_get: Time-navigable cluster map for token
-- Returns sliding window of activity with balances sourced from tx_guide
--
-- NO DEPENDENCY on tx_bmap_state - uses tx_guide post-balance columns directly
--
-- Parameters:
--   p_token_name: Token name (optional)
--   p_token_symbol: Token symbol (optional)
--   p_mint_address: Token mint address (preferred)
--   p_signature: Transaction signature (optional - if NULL, uses p_block_time or most recent)
--   p_block_time: Unix timestamp (optional - find nearest tx; ignored if p_signature provided)
--   p_tx_limit: Window size (1, 10, 20, 50, 100) - divided by 2 for prev/next
--
-- Token Resolution:
--   1. If p_mint_address provided, use it
--   2. Else if p_token_symbol provided, use it (ranked by activity)
--   3. Else if p_token_name provided, use it (ranked by activity)
--   4. Else if p_signature provided (only), derive token from tx_guide activity
--
-- Transaction Resolution:
--   1. If p_signature provided, use that tx (p_block_time ignored)
--   2. Else if p_block_time provided, find nearest tx to that time
--   3. Else use most recent tx
--
-- Usage:
--   CALL sp_tx_bmap_get(NULL, 'BONK', NULL, NULL, NULL, NULL);
--   CALL sp_tx_bmap_get(NULL, NULL, 'DezXAZ...', '5KtP...abc', NULL, NULL);
--   CALL sp_tx_bmap_get(NULL, NULL, NULL, '5KtP...abc', NULL, NULL);  -- derive token from sig
--   CALL sp_tx_bmap_get(NULL, 'BONK', NULL, NULL, 1703000000, 20);  -- 20 tx window

DELIMITER //

DROP PROCEDURE IF EXISTS sp_tx_bmap_get//

CREATE PROCEDURE sp_tx_bmap_get(
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
    DECLARE v_token_type VARCHAR(20);
    DECLARE v_decimals TINYINT UNSIGNED;

    DECLARE v_signature VARCHAR(88);
    DECLARE v_block_time BIGINT UNSIGNED;
    DECLARE v_tx_id BIGINT;
    DECLARE v_signer_address VARCHAR(44);

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
        SELECT tk.id, mint.address, tk.token_symbol, tk.token_type, tk.decimals
        INTO v_token_id, v_mint_address, v_token_symbol, v_token_type, v_decimals
        FROM tx_token tk
        JOIN tx_address mint ON mint.id = tk.mint_address_id
        WHERE mint.address = p_mint_address
        LIMIT 1;
    ELSEIF p_token_symbol IS NOT NULL THEN
        
        SELECT tk.id, mint.address, tk.token_symbol, tk.token_type, tk.decimals
        INTO v_token_id, v_mint_address, v_token_symbol, v_token_type, v_decimals
        FROM tx_token tk
        JOIN tx_address mint ON mint.id = tk.mint_address_id
        LEFT JOIN (SELECT token_id, COUNT(*) AS cnt FROM tx_guide GROUP BY token_id) g ON g.token_id = tk.id
        WHERE tk.token_symbol = p_token_symbol
        ORDER BY COALESCE(g.cnt, 0) DESC, (mint.address_type = 'mint') DESC, tk.id
        LIMIT 1;
    ELSEIF p_token_name IS NOT NULL THEN
        
        SELECT tk.id, mint.address, tk.token_symbol, tk.token_type, tk.decimals
        INTO v_token_id, v_mint_address, v_token_symbol, v_token_type, v_decimals
        FROM tx_token tk
        JOIN tx_address mint ON mint.id = tk.mint_address_id
        LEFT JOIN (SELECT token_id, COUNT(*) AS cnt FROM tx_guide GROUP BY token_id) g ON g.token_id = tk.id
        WHERE tk.token_name = p_token_name
        ORDER BY COALESCE(g.cnt, 0) DESC, (mint.address_type = 'mint') DESC, tk.id
        LIMIT 1;
    ELSEIF p_signature IS NOT NULL THEN
        
        SET v_signature_only_mode = 1;

        
        
        SELECT t.id, t.signature, t.block_time, tk.id, mint.address, tk.token_symbol, tk.token_type, tk.decimals
        INTO v_tx_id, v_signature, v_block_time, v_token_id, v_mint_address, v_token_symbol, v_token_type, v_decimals
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
            SELECT t.id, t.signature, t.block_time, tk.id, mint.address, tk.token_symbol, tk.token_type, tk.decimals
            INTO v_tx_id, v_signature, v_block_time, v_token_id, v_mint_address, v_token_symbol, v_token_type, v_decimals
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
        SELECT JSON_OBJECT('result', JSON_OBJECT('error', 'Token not found')) AS result;
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
                    )) AS result;
                ELSE
                    SELECT JSON_OBJECT('result', JSON_OBJECT(
                        'error', 'Signature not found',
                        'signature', p_signature
                    )) AS result;
                END IF;
                
                SET v_token_id = NULL;
            END IF;
        ELSEIF p_block_time IS NOT NULL THEN
            
            
            SELECT t.id, t.signature, t.block_time
            INTO v_tx_id, v_signature, v_block_time
            FROM tx_guide g
            JOIN tx t ON t.id = g.tx_id
            WHERE g.token_id = v_token_id
            ORDER BY ABS(CAST(g.block_time AS SIGNED) - CAST(p_block_time AS SIGNED))
            LIMIT 1;

            IF v_tx_id IS NULL THEN
                SELECT JSON_OBJECT('result', JSON_OBJECT(
                    'error', 'No transactions found for this token',
                    'mint', v_mint_address
                )) AS result;
                
                SET v_token_id = NULL;
            END IF;
        ELSE
            
            SELECT t.id, t.signature, t.block_time
            INTO v_tx_id, v_signature, v_block_time
            FROM tx_guide g
            JOIN tx t ON t.id = g.tx_id
            WHERE g.token_id = v_token_id
            ORDER BY g.block_time DESC
            LIMIT 1;

            IF v_tx_id IS NULL THEN
                SELECT JSON_OBJECT('result', JSON_OBJECT(
                    'error', 'No transactions found for this token',
                    'mint', v_mint_address
                )) AS result;
                
                SET v_token_id = NULL;
            END IF;
        END IF;
    END IF;

    IF v_token_id IS NOT NULL AND v_tx_id IS NOT NULL THEN
        
        
        SET v_signature_only_mode = 1;

        
        SELECT a.address INTO v_signer_address
        FROM tx t
        JOIN tx_address a ON a.id = t.signer_address_id
        WHERE t.id = v_tx_id;

        
        
        
        
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
            SELECT sub.tx_id, t.signature, sub.block_time,
                   -1 * (@prev_row := @prev_row + 1)
            FROM (SELECT @prev_row := 0) init,
                 (
                    SELECT DISTINCT g.tx_id, g.block_time
                    FROM tx_guide g
                    WHERE g.token_id = ', v_token_id, '
                      AND (g.block_time, g.tx_id) < (', v_block_time, ', ', v_tx_id, ')
                    ORDER BY g.block_time DESC, g.tx_id DESC
                    LIMIT 5
                 ) sub
            JOIN tx t ON t.id = sub.tx_id
            ORDER BY sub.block_time DESC, sub.tx_id DESC
        ');
        PREPARE stmt_prev FROM @sql_prev;
        EXECUTE stmt_prev;
        DEALLOCATE PREPARE stmt_prev;

        
        
        SET @sql_next = CONCAT('
            INSERT INTO tmp_window (tx_id, signature, block_time, window_pos)
            SELECT sub.tx_id, t.signature, sub.block_time,
                   @next_row := @next_row + 1
            FROM (SELECT @next_row := 0) init,
                 (
                    SELECT DISTINCT g.tx_id, g.block_time
                    FROM tx_guide g
                    WHERE g.token_id = ', v_token_id, '
                      AND (g.block_time, g.tx_id) > (', v_block_time, ', ', v_tx_id, ')
                    ORDER BY g.block_time ASC, g.tx_id ASC
                    LIMIT 5
                 ) sub
            JOIN tx t ON t.id = sub.tx_id
            ORDER BY sub.block_time ASC, sub.tx_id ASC
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
            ORDER BY w.block_time DESC, w.tx_id DESC
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
            ORDER BY w.block_time ASC, w.tx_id ASC
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
            pa.id,
            pa.address,
            COALESCE(pa.label, 'pool'),
            COALESCE(pa.address_type, 'pool'),
            0,
            NULL
        FROM tx_guide g
        JOIN tmp_display_window w ON w.tx_id = g.tx_id
        JOIN tx_pool pool ON pool.id = g.pool_id JOIN tx_address pa ON pa.id = pool.pool_address_id
        WHERE g.pool_id IS NOT NULL;     
        
        DELETE n FROM tmp_nodes n
        WHERE n.address_type = 'vault'
          AND NOT EXISTS (
              SELECT 1 FROM tx_guide g
              JOIN tmp_display_window w ON w.tx_id = g.tx_id
              JOIN tx_address fa ON fa.id = g.from_address_id
              JOIN tx_address ta ON ta.id = g.to_address_id
              JOIN tx_guide_type gt ON gt.id = g.edge_type_id
              WHERE (v_signature_only_mode = 1 OR g.token_id = v_token_id OR g.token_id IS NULL)
                AND gt.type_code NOT IN ('swap_in', 'swap_out')
                AND NOT (fa.address_type = 'vault' AND ta.address_type = 'vault')
                AND (g.from_address_id = n.address_id OR g.to_address_id = n.address_id)
          );

        
        UPDATE tmp_nodes n
        JOIN tx_pool p ON p.pool_address_id = n.address_id
        SET n.pool_label = p.pool_label
        WHERE n.pool_label IS NULL AND n.address_type = 'pool' AND p.pool_label IS NOT NULL;

        
        UPDATE tmp_nodes n
        JOIN tx_token tk ON tk.mint_address_id = n.address_id
        SET n.token_name = tk.token_name
        WHERE tk.token_name IS NOT NULL;    
        
        DROP TEMPORARY TABLE IF EXISTS tmp_token_bal;
        CREATE TEMPORARY TABLE tmp_token_bal (
            address_id INT UNSIGNED PRIMARY KEY,
            balance DECIMAL(30,9),
            block_time BIGINT UNSIGNED,
            guide_id BIGINT UNSIGNED
        ) ENGINE=MEMORY;

        
        INSERT INTO tmp_token_bal (address_id, balance, block_time, guide_id)
        SELECT g.from_address_id,
               ROUND(g.from_token_post_balance / POW(10, COALESCE(g.decimals, v_decimals, 9)), 9),
               g.block_time, g.id
        FROM tx_guide g
        WHERE g.from_address_id IN (SELECT address_id FROM tmp_nodes)
          AND g.token_id = v_token_id
          AND g.block_time <= v_block_time
          AND g.from_token_post_balance IS NOT NULL
        ON DUPLICATE KEY UPDATE
            balance = IF(VALUES(block_time) >= tmp_token_bal.block_time, VALUES(balance), tmp_token_bal.balance),
            block_time = GREATEST(VALUES(block_time), tmp_token_bal.block_time),
            guide_id = IF(VALUES(block_time) >= tmp_token_bal.block_time, VALUES(guide_id), tmp_token_bal.guide_id);

        
        INSERT INTO tmp_token_bal (address_id, balance, block_time, guide_id)
        SELECT g.to_address_id,
               ROUND(g.to_token_post_balance / POW(10, COALESCE(g.decimals, v_decimals, 9)), 9),
               g.block_time, g.id
        FROM tx_guide g
        WHERE g.to_address_id IN (SELECT address_id FROM tmp_nodes)
          AND g.token_id = v_token_id
          AND g.block_time <= v_block_time
          AND g.to_token_post_balance IS NOT NULL
        ON DUPLICATE KEY UPDATE
            balance = IF(VALUES(block_time) >= tmp_token_bal.block_time, VALUES(balance), tmp_token_bal.balance),
            block_time = GREATEST(VALUES(block_time), tmp_token_bal.block_time),
            guide_id = IF(VALUES(block_time) >= tmp_token_bal.block_time, VALUES(guide_id), tmp_token_bal.guide_id);

        
        UPDATE tmp_nodes n
        JOIN tmp_token_bal tb ON tb.address_id = n.address_id
        SET n.balance = COALESCE(tb.balance, 0);

        DROP TEMPORARY TABLE IF EXISTS tmp_token_bal;
        
        DROP TEMPORARY TABLE IF EXISTS tmp_sol_max;
        CREATE TEMPORARY TABLE tmp_sol_max (
            address_id INT UNSIGNED PRIMARY KEY,
            max_bt BIGINT UNSIGNED
        ) ENGINE=MEMORY;

        INSERT INTO tmp_sol_max (address_id, max_bt)
        SELECT g.from_address_id, MAX(g.block_time)
        FROM tx_guide g
        WHERE g.from_address_id IN (SELECT address_id FROM tmp_nodes)
          AND g.block_time <= v_block_time
          AND g.from_sol_post_balance IS NOT NULL
        GROUP BY g.from_address_id;

        
        DROP TEMPORARY TABLE IF EXISTS tmp_sol_bal;
        CREATE TEMPORARY TABLE tmp_sol_bal (
            address_id INT UNSIGNED PRIMARY KEY,
            sol_balance DECIMAL(20,9),
            block_time BIGINT UNSIGNED,
            guide_id BIGINT UNSIGNED
        ) ENGINE=MEMORY;

        INSERT IGNORE INTO tmp_sol_bal (address_id, sol_balance, block_time, guide_id)
        SELECT m.address_id,
               ROUND(g.from_sol_post_balance / 1e9, 9),
               g.block_time, g.id
        FROM tmp_sol_max m
        JOIN tx_guide g ON g.from_address_id = m.address_id
          AND g.block_time = m.max_bt
          AND g.from_sol_post_balance IS NOT NULL;

        DROP TEMPORARY TABLE IF EXISTS tmp_sol_max;

        
        DROP TEMPORARY TABLE IF EXISTS tmp_sol_max;
        CREATE TEMPORARY TABLE tmp_sol_max (
            address_id INT UNSIGNED PRIMARY KEY,
            max_bt BIGINT UNSIGNED
        ) ENGINE=MEMORY;

        INSERT INTO tmp_sol_max (address_id, max_bt)
        SELECT g.to_address_id, MAX(g.block_time)
        FROM tx_guide g
        WHERE g.to_address_id IN (SELECT address_id FROM tmp_nodes)
          AND g.block_time <= v_block_time
          AND g.to_sol_post_balance IS NOT NULL
        GROUP BY g.to_address_id;

        
        INSERT INTO tmp_sol_bal (address_id, sol_balance, block_time, guide_id)
        SELECT m.address_id,
               ROUND(g.to_sol_post_balance / 1e9, 9),
               g.block_time, g.id
        FROM tmp_sol_max m
        JOIN tx_guide g ON g.to_address_id = m.address_id
          AND g.block_time = m.max_bt
          AND g.to_sol_post_balance IS NOT NULL
        ON DUPLICATE KEY UPDATE
            sol_balance = IF(VALUES(block_time) >= tmp_sol_bal.block_time, VALUES(sol_balance), tmp_sol_bal.sol_balance),
            block_time = GREATEST(VALUES(block_time), tmp_sol_bal.block_time),
            guide_id = IF(VALUES(block_time) >= tmp_sol_bal.block_time, VALUES(guide_id), tmp_sol_bal.guide_id);

        DROP TEMPORARY TABLE IF EXISTS tmp_sol_max;

        
        UPDATE tmp_nodes n
        JOIN tmp_sol_bal sb ON sb.address_id = n.address_id
        SET n.sol_balance = COALESCE(sb.sol_balance, 0);

        DROP TEMPORARY TABLE IF EXISTS tmp_sol_bal;
        
        DROP TEMPORARY TABLE IF EXISTS tmp_display_window_int;
        CREATE TEMPORARY TABLE tmp_display_window_int AS SELECT * FROM tmp_display_window;

        
        UPDATE tmp_nodes n
        SET interactions_pools = (
            SELECT JSON_ARRAYAGG(pool_info)
            FROM (
                SELECT DISTINCT JSON_OBJECT(
                    'address', pa.address,
                    'label', COALESCE(pool.pool_label, pa.address)
                ) AS pool_info
                FROM tx_guide g
                JOIN tmp_display_window_int w ON w.tx_id = g.tx_id
                JOIN tx_pool pool ON pool.id = g.pool_id JOIN tx_address pa ON pa.id = pool.pool_address_id
                WHERE (g.from_address_id = n.address_id OR g.to_address_id = n.address_id)
                  AND g.pool_id IS NOT NULL
                GROUP BY pa.address, pool.pool_label
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
                SELECT DISTINCT pool.dex AS dex_name
                FROM tx_guide g
                JOIN tmp_display_window_int w ON w.tx_id = g.tx_id
                JOIN tx_pool pool ON pool.id = g.pool_id
                WHERE (g.from_address_id = n.address_id OR g.to_address_id = n.address_id)
                  AND pool.dex IS NOT NULL
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

        SELECT JSON_ARRAYAGG(edge_data) INTO v_edges_json
        FROM (
            
            
            
            
            SELECT JSON_OBJECT(
                'source', CASE
                    WHEN gt.type_code = 'swap_in' THEN fa.address
                    ELSE COALESCE(pa.address, fa.address)
                END,
                'source_label', CASE
                    WHEN gt.type_code = 'swap_in' THEN COALESCE(fa.label, fa.address_type)
                    ELSE COALESCE(pool.pool_label, pa.address, fa.label, fa.address_type)
                END,
                'target', CASE
                    WHEN gt.type_code = 'swap_in' THEN COALESCE(pa.address, ta.address)
                    ELSE ta.address
                END,
                'target_label', CASE
                    WHEN gt.type_code = 'swap_in' THEN COALESCE(pool.pool_label, pa.address, ta.label, ta.address_type)
                    ELSE COALESCE(ta.label, ta.address_type)
                END,
                'amount', ROUND(g.amount / POW(10, COALESCE(g.decimals, 9)), 9),
                'type', gt.type_code,
                'token_symbol', COALESCE(tk.token_symbol, CASE WHEN g.token_id IS NULL THEN 'SOL' ELSE 'Unknown' END),
                'dex', pool.dex,
                'pool_label', pool.pool_label,
                'ins_index', g.ins_index,
                'signature', t.signature,
                'block_time_utc', FROM_UNIXTIME(t.block_time),
                'tax_bps', g.tax_bps,
                'tax_amount', ROUND(g.tax_amount / POW(10, COALESCE(g.decimals, 9)), 9)
            ) AS edge_data
            FROM tx_guide g
            JOIN tmp_display_window w ON w.tx_id = g.tx_id
            JOIN tx t ON t.id = g.tx_id
            JOIN tx_address fa ON fa.id = g.from_address_id
            JOIN tx_address ta ON ta.id = g.to_address_id
            JOIN tx_guide_type gt ON gt.id = g.edge_type_id
            LEFT JOIN tx_token tk ON tk.id = g.token_id
            LEFT JOIN tx_pool pool ON pool.id = g.pool_id LEFT JOIN tx_address pa ON pa.id = pool.pool_address_id
            WHERE (v_signature_only_mode = 1 OR g.token_id = v_token_id)
              AND gt.type_code IN ('swap_in', 'swap_out')

            UNION ALL

            
            SELECT JSON_OBJECT(
                'source', fa.address,
                'source_label', COALESCE(fa.label, fa.address_type),
                'target', ta.address,
                'target_label', COALESCE(ta.label, ta.address_type),
                'amount', ROUND(g.amount / POW(10, COALESCE(g.decimals, 9)), 9),
                'type', gt.type_code,
                'token_symbol', COALESCE(tk.token_symbol, CASE WHEN g.token_id IS NULL THEN 'SOL' ELSE 'Unknown' END),
                'ins_index', g.ins_index,
                'signature', t.signature,
                'block_time_utc', FROM_UNIXTIME(t.block_time),
                'tax_bps', g.tax_bps,
                'tax_amount', ROUND(g.tax_amount / POW(10, COALESCE(g.decimals, 9)), 9)
            ) AS edge_data
            FROM tx_guide g
            JOIN tmp_window2 w ON w.tx_id = g.tx_id
            JOIN tx t ON t.id = g.tx_id
            JOIN tx_address fa ON fa.id = g.from_address_id
            JOIN tx_address ta ON ta.id = g.to_address_id
            JOIN tx_guide_type gt ON gt.id = g.edge_type_id
            LEFT JOIN tx_token tk ON tk.id = g.token_id
            WHERE (v_signature_only_mode = 1 OR g.token_id = v_token_id OR g.token_id IS NULL)
              AND gt.type_code NOT IN ('swap_in', 'swap_out')
              
              AND NOT (fa.address_type = 'vault' AND ta.address_type = 'vault')
        ) edges;

        DROP TEMPORARY TABLE IF EXISTS tmp_window2;
        
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
                'token_type', tk.token_type,
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
            GROUP BY tk.id, mint_addr.address, tk.token_symbol, tk.token_name, tk.token_type
            ORDER BY COUNT(DISTINCT g.tx_id) DESC, SUM(g.amount) DESC
            LIMIT 20
        ) related_outer;

        DROP TEMPORARY TABLE IF EXISTS tmp_swap_txs;
        
        SELECT JSON_OBJECT(
            'result', JSON_OBJECT(
                'token', JSON_OBJECT(
                    'mint', v_mint_address,
                    'symbol', v_token_symbol,
                    'token_type', v_token_type
                ),
                'txs', JSON_OBJECT(
                    'signature', v_signature,
                    'signer', v_signer_address,
                    'block_time', v_block_time,
                    'block_time_utc', CONVERT_TZ(FROM_UNIXTIME(v_block_time), @@session.time_zone, '+00:00'),
                    'edge_types', COALESCE(v_current_edge_types, JSON_ARRAY()),
                    'prev', COALESCE(v_prev_json, JSON_ARRAY()),
                    'next', COALESCE(v_next_json, JSON_ARRAY())
                ),
                'nodes', COALESCE(v_nodes_json, JSON_ARRAY()),
                'edges', COALESCE(v_edges_json, JSON_ARRAY()),
                'related_tokens', COALESCE(v_related_tokens_json, JSON_ARRAY())
            )
        ) AS result;

        
        DROP TEMPORARY TABLE IF EXISTS tmp_nodes;
        DROP TEMPORARY TABLE IF EXISTS tmp_window;
        DROP TEMPORARY TABLE IF EXISTS tmp_display_window;
    END IF;
END //

DELIMITER ;
