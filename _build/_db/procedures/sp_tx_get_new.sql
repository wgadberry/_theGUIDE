-- sp_tx_get2: Simplified transaction query — no modes, additive filters
--
-- At least one filter required:
--   p_address:       Wallet/pool address — filter edges involving this address
--   p_mint_address:  Token mint — resolve to token_id, filter by token
--   p_token_symbol:  Token symbol — resolve to token_id (fallback if no mint)
--   p_signature:     Tx signature — center window on this tx
--
-- Other params:
--   p_limit:         Max transactions in window (default 10, max 100)
--   p_after:         Unix timestamp — only edges after this time
--   p_before:        Unix timestamp — only edges before this time
--   p_type_mask:     Bitmask filtering edge types (tx_guide_type.indicator)
--   p_result_mask:   Bitmask controlling which sections to return (default 255 = all)
--
-- Result mask bits:
--     1  EDGES          Edge list (source/target/amount/type/token)
--     2  NODES          Address nodes with labels/types
--     4  BALANCES       Token + SOL post-balances on nodes
--     8  INTERACTIONS   Pool/dex interactions per node
--    16  (reserved)
--    32  (reserved)
--    64  NAVIGATION     Prev/next transaction lists
--   128  RELATED        Other tokens from swap edges
--
-- Filters are additive:
--   address + mint   = that wallet's activity in that token
--   mint + signature = that token centered on that tx
--   signature alone  = all edges for that single tx (limit forced to 1)
--   address alone    = all activity for that wallet
--
-- No token derivation from signature. No wallet_mode vs graph_mode.

DELIMITER //
DROP PROCEDURE IF EXISTS sp_tx_get_new//
CREATE PROCEDURE sp_tx_get_new(
    IN p_address       VARCHAR(44),
    IN p_mint_address   VARCHAR(44),
    IN p_token_symbol   VARCHAR(128),
    IN p_signature      VARCHAR(88),
    IN p_limit          SMALLINT UNSIGNED,
    IN p_after          BIGINT UNSIGNED,
    IN p_before         BIGINT UNSIGNED,
    IN p_type_mask      BIGINT UNSIGNED,
    IN p_result_mask    INT UNSIGNED
)
proc_body: BEGIN
    DECLARE SECT_EDGES INT UNSIGNED DEFAULT 1;
    DECLARE SECT_NODES INT UNSIGNED DEFAULT 2;
    DECLARE SECT_BALANCES INT UNSIGNED DEFAULT 4;
    DECLARE SECT_INTERACTIONS INT UNSIGNED DEFAULT 8;
    DECLARE SECT_NAVIGATION INT UNSIGNED DEFAULT 64;
    DECLARE SECT_RELATED INT UNSIGNED DEFAULT 128;

    DECLARE v_address_id INT UNSIGNED DEFAULT NULL;
    DECLARE v_token_id BIGINT DEFAULT NULL;
    DECLARE v_mint_address VARCHAR(44) DEFAULT NULL;
    DECLARE v_token_symbol VARCHAR(128) DEFAULT NULL;
    DECLARE v_token_type VARCHAR(20) DEFAULT NULL;
    DECLARE v_decimals TINYINT UNSIGNED DEFAULT NULL;
    DECLARE v_tx_id BIGINT DEFAULT NULL;
    DECLARE v_signature VARCHAR(88) DEFAULT NULL;
    DECLARE v_block_time BIGINT UNSIGNED DEFAULT NULL;
    DECLARE v_signer_address VARCHAR(44) DEFAULT NULL;
    DECLARE v_signer_address_id INT UNSIGNED DEFAULT NULL;
    DECLARE v_half SMALLINT UNSIGNED;
    DECLARE v_tx_count INT UNSIGNED DEFAULT 0;
    DECLARE v_edge_count INT UNSIGNED DEFAULT 0;

    DECLARE v_edges_json JSON DEFAULT NULL;
    DECLARE v_nodes_json JSON DEFAULT NULL;
    DECLARE v_prev_json JSON DEFAULT NULL;
    DECLARE v_next_json JSON DEFAULT NULL;
    DECLARE v_current_edge_types JSON DEFAULT NULL;
    DECLARE v_related_tokens_json JSON DEFAULT NULL;
    DECLARE v_boundary_json JSON DEFAULT NULL;

    -- ==================== SETUP ====================
    SET p_result_mask = COALESCE(p_result_mask, 255);
    SET p_limit = COALESCE(p_limit, 10);
    IF p_limit > 100 THEN SET p_limit = 100; END IF;
    SET v_half = CEIL(p_limit / 2);
    SET p_type_mask = COALESCE(p_type_mask, 18446744073709551615);  -- all bits set = no filter

    -- Normalize empty strings to NULL
    IF p_address = '' THEN SET p_address = NULL; END IF;
    IF p_mint_address = '' THEN SET p_mint_address = NULL; END IF;
    IF p_token_symbol = '' THEN SET p_token_symbol = NULL; END IF;
    IF p_signature = '' THEN SET p_signature = NULL; END IF;

    -- Must have at least one filter
    IF p_address IS NULL AND p_mint_address IS NULL AND p_token_symbol IS NULL AND p_signature IS NULL THEN
        SELECT JSON_OBJECT('error', 'At least one filter required: address, mint, symbol, or signature') AS result;
        LEAVE proc_body;
    END IF;

    -- ==================== RESOLVE FILTERS ====================

    -- Address
    IF p_address IS NOT NULL THEN
        SELECT id INTO v_address_id FROM tx_address WHERE address = p_address LIMIT 1;
        IF v_address_id IS NULL THEN
            SELECT JSON_OBJECT('error', 'Address not found', 'address', p_address) AS result;
            LEAVE proc_body;
        END IF;
    END IF;

    -- Token (mint takes priority over symbol)
    IF p_mint_address IS NOT NULL THEN
        SELECT tk.id, mint.address, tk.token_symbol, tk.token_type, tk.decimals
        INTO v_token_id, v_mint_address, v_token_symbol, v_token_type, v_decimals
        FROM tx_token tk JOIN tx_address mint ON mint.id = tk.mint_address_id
        WHERE mint.address = p_mint_address LIMIT 1;
    ELSEIF p_token_symbol IS NOT NULL THEN
        SELECT tk.id, mint.address, tk.token_symbol, tk.token_type, tk.decimals
        INTO v_token_id, v_mint_address, v_token_symbol, v_token_type, v_decimals
        FROM tx_token tk JOIN tx_address mint ON mint.id = tk.mint_address_id
        LEFT JOIN (SELECT token_id, COUNT(*) AS cnt FROM tx_guide GROUP BY token_id) gc ON gc.token_id = tk.id
        WHERE tk.token_symbol = p_token_symbol
        ORDER BY COALESCE(gc.cnt, 0) DESC, (mint.address_type = 'mint') DESC, tk.id LIMIT 1;
    END IF;

    IF (p_mint_address IS NOT NULL OR p_token_symbol IS NOT NULL) AND v_token_id IS NULL THEN
        SELECT JSON_OBJECT('error', 'Token not found') AS result;
        LEAVE proc_body;
    END IF;

    -- Signature
    IF p_signature IS NOT NULL THEN
        SELECT id, signature, block_time INTO v_tx_id, v_signature, v_block_time
        FROM tx WHERE signature = p_signature LIMIT 1;
        IF v_tx_id IS NULL THEN
            SELECT JSON_OBJECT('error', 'Signature not found', 'signature', p_signature) AS result;
            LEAVE proc_body;
        END IF;
        -- Resolve signer
        SELECT t.signer_address_id, a.address INTO v_signer_address_id, v_signer_address
        FROM tx t JOIN tx_address a ON a.id = t.signer_address_id WHERE t.id = v_tx_id;
    END IF;

	/*
    -- Sig-only (no token, no address): force limit=1 to get all edges for that tx
    IF p_signature IS NOT NULL AND v_token_id IS NULL AND v_address_id IS NULL THEN
        SET p_limit = 1;
        SET v_half = 1;
    END IF;
	*/
    -- ==================== COLLECT EDGES ====================
    DROP TEMPORARY TABLE IF EXISTS tmp_guide_ids;
    CREATE TEMPORARY TABLE tmp_guide_ids (
        guide_id BIGINT UNSIGNED, tx_id BIGINT UNSIGNED,
        block_time BIGINT UNSIGNED,
        INDEX idx_tx (tx_id)
    ) ENGINE=MEMORY;

    IF v_tx_id IS NOT NULL AND p_limit = 1 THEN
        -- Single tx: all edges (filtered by token/address/type if provided)
        INSERT INTO tmp_guide_ids
        SELECT g.id, g.tx_id, v_block_time
        FROM tx_guide g
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id AND (gt.indicator & p_type_mask) != 0
        WHERE g.tx_id = v_tx_id
          AND (v_token_id IS NULL OR g.token_id = v_token_id)
          AND (v_address_id IS NULL OR g.from_address_id = v_address_id OR g.to_address_id = v_address_id
               OR EXISTS (SELECT 1 FROM tx_pool p WHERE p.id = g.pool_id AND p.pool_address_id = v_address_id));

    ELSEIF v_tx_id IS NOT NULL THEN
        -- Windowed: center on signature, get half before + half after
        -- Before/at anchor
        INSERT INTO tmp_guide_ids
        SELECT g.id, g.tx_id, t.block_time
        FROM tx_guide g JOIN tx t ON t.id = g.tx_id
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id AND (gt.indicator & p_type_mask) != 0
        WHERE t.block_time <= v_block_time
          AND (p_after IS NULL OR t.block_time > p_after)
          AND (v_token_id IS NULL OR g.token_id = v_token_id)
          AND (v_address_id IS NULL OR g.from_address_id = v_address_id OR g.to_address_id = v_address_id
               OR EXISTS (SELECT 1 FROM tx_pool p WHERE p.id = g.pool_id AND p.pool_address_id = v_address_id))
        ORDER BY t.block_time DESC LIMIT 10000;

        -- After anchor
        INSERT INTO tmp_guide_ids
        SELECT g.id, g.tx_id, t.block_time
        FROM tx_guide g JOIN tx t ON t.id = g.tx_id
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id AND (gt.indicator & p_type_mask) != 0
        WHERE t.block_time > v_block_time
          AND (p_before IS NULL OR t.block_time < p_before)
          AND (v_token_id IS NULL OR g.token_id = v_token_id)
          AND (v_address_id IS NULL OR g.from_address_id = v_address_id OR g.to_address_id = v_address_id
               OR EXISTS (SELECT 1 FROM tx_pool p WHERE p.id = g.pool_id AND p.pool_address_id = v_address_id))
        ORDER BY t.block_time ASC LIMIT 10000;

    ELSE
        -- No signature: most recent edges matching filters
        INSERT INTO tmp_guide_ids
        SELECT g.id, g.tx_id, t.block_time
        FROM tx_guide g JOIN tx t ON t.id = g.tx_id
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id AND (gt.indicator & p_type_mask) != 0
        WHERE (p_after IS NULL OR t.block_time > p_after)
          AND (p_before IS NULL OR t.block_time < p_before)
          AND (v_token_id IS NULL OR g.token_id = v_token_id)
          AND (v_address_id IS NULL OR g.from_address_id = v_address_id OR g.to_address_id = v_address_id
               OR EXISTS (SELECT 1 FROM tx_pool p WHERE p.id = g.pool_id AND p.pool_address_id = v_address_id))
        ORDER BY t.block_time DESC LIMIT 10000;
    END IF;

    -- ==================== LIMIT TO p_limit TRANSACTIONS ====================
    DROP TEMPORARY TABLE IF EXISTS tmp_limit_txs;
    IF v_block_time IS NOT NULL AND p_limit > 1 THEN
        CREATE TEMPORARY TABLE tmp_limit_txs ENGINE=MEMORY AS
        SELECT DISTINCT tx_id, block_time FROM tmp_guide_ids
        WHERE block_time <= v_block_time ORDER BY block_time DESC LIMIT v_half;
        INSERT IGNORE INTO tmp_limit_txs
        SELECT DISTINCT tx_id, block_time FROM tmp_guide_ids
        WHERE block_time > v_block_time ORDER BY block_time ASC LIMIT v_half;
    ELSE
        CREATE TEMPORARY TABLE tmp_limit_txs (
            tx_id BIGINT UNSIGNED PRIMARY KEY, block_time BIGINT UNSIGNED
        ) ENGINE=MEMORY AS
        SELECT DISTINCT tx_id, block_time FROM tmp_guide_ids ORDER BY block_time DESC LIMIT p_limit;
    END IF;

    -- If no anchor tx yet, use most recent from window
    IF v_tx_id IS NULL THEN
        SELECT lt.tx_id, t.signature, lt.block_time INTO v_tx_id, v_signature, v_block_time
        FROM tmp_limit_txs lt JOIN tx t ON t.id = lt.tx_id
        ORDER BY lt.block_time DESC LIMIT 1;
        IF v_tx_id IS NOT NULL THEN
            SELECT a.address INTO v_signer_address
            FROM tx t JOIN tx_address a ON a.id = t.signer_address_id WHERE t.id = v_tx_id;
        END IF;
    END IF;

    -- ==================== MATERIALIZE FINAL EDGES ====================
    DROP TEMPORARY TABLE IF EXISTS tmp_final_ids;
    CREATE TEMPORARY TABLE tmp_final_ids (
        guide_id BIGINT UNSIGNED, tx_id BIGINT UNSIGNED,
        block_time BIGINT UNSIGNED,
        INDEX idx_guide (guide_id), INDEX idx_tx (tx_id)
    ) ENGINE=MEMORY AS
    SELECT DISTINCT g.guide_id, g.tx_id, g.block_time
    FROM tmp_guide_ids g JOIN tmp_limit_txs lt ON lt.tx_id = g.tx_id;

    -- Clone for MySQL temp table reopen workaround
    DROP TEMPORARY TABLE IF EXISTS tmp_final_ids2;
    CREATE TEMPORARY TABLE tmp_final_ids2 ENGINE=MEMORY AS SELECT * FROM tmp_final_ids;

    SELECT COUNT(DISTINCT tx_id) INTO v_tx_count FROM tmp_limit_txs;
    SELECT COUNT(*) INTO v_edge_count FROM tmp_final_ids;

    -- ==================== EDGES (bit 1) ====================
    IF (p_result_mask & SECT_EDGES) != 0 THEN
        SELECT JSON_ARRAYAGG(edge_data) INTO v_edges_json FROM (
            -- Swap edges: routed through pool
            SELECT JSON_OBJECT(
                'source', CASE WHEN gt.type_code = 'swap_in' THEN fa.address ELSE COALESCE(pa.address, fa.address) END,
                'source_label', CASE WHEN gt.type_code = 'swap_in' THEN COALESCE(fa.label, fa.address_type)
                    ELSE COALESCE(pool.pool_label, pa.address, fa.label, fa.address_type) END,
                'target', CASE WHEN gt.type_code = 'swap_in' THEN COALESCE(pa.address, ta.address) ELSE ta.address END,
                'target_label', CASE WHEN gt.type_code = 'swap_in' THEN COALESCE(pool.pool_label, pa.address, ta.label, ta.address_type)
                    ELSE COALESCE(ta.label, ta.address_type) END,
                'amount', fn_format_decimal(g.amount, COALESCE(g.decimals, v_decimals, 9)),
                'type', gt.type_code,
                'token_symbol', COALESCE(tk.token_symbol, IF(g.token_id IS NULL, 'SOL', 'Unknown')),
                'mint_address', ma.address,
                'dex', pool.dex, 'pool_label', pool.pool_label,
                'ins_index', g.ins_index, 'signature', t.signature, 'block_time', t.block_time,
                'block_time_utc', DATE_FORMAT(FROM_UNIXTIME(t.block_time), '%Y-%m-%dT%H:%i:%s'),
                'tax_bps', g.tax_bps,
                'tax_amount', fn_format_decimal(g.tax_amount, COALESCE(g.decimals, v_decimals, 9)),
                'source_funded_by', faf.address,
                'target_funded_by', taf.address,
                'mint_funded_by', mf.address
            ) AS edge_data
            FROM tmp_final_ids f JOIN tx_guide g ON g.id = f.guide_id
            JOIN tx t ON t.id = g.tx_id
            JOIN tx_address fa ON fa.id = g.from_address_id
            JOIN tx_address ta ON ta.id = g.to_address_id
            JOIN tx_guide_type gt ON gt.id = g.edge_type_id
            LEFT JOIN tx_token tk ON tk.id = g.token_id
            LEFT JOIN tx_pool pool ON pool.id = g.pool_id
            LEFT JOIN tx_address pa ON pa.id = pool.pool_address_id
            LEFT JOIN tx_address ma ON ma.id = tk.mint_address_id
            LEFT JOIN tx_address faf ON faf.id = fa.funded_by_address_id
            LEFT JOIN tx_address taf ON taf.id = ta.funded_by_address_id
            LEFT JOIN tx_address mf ON mf.id = ma.funded_by_address_id
            WHERE gt.type_code IN ('swap_in', 'swap_out')
              AND NOT (fa.address_type IN ('vault', 'dex_wallet') AND ta.address_type IN ('vault', 'dex_wallet'))

            UNION ALL

            -- Non-swap edges
            SELECT JSON_OBJECT(
                'source', fa.address, 'source_label', COALESCE(fa.label, fa.address_type),
                'target', ta.address, 'target_label', COALESCE(ta.label, ta.address_type),
                'amount', fn_format_decimal(g.amount, COALESCE(g.decimals, v_decimals, 9)),
                'type', gt.type_code,
                'token_symbol', COALESCE(tk.token_symbol, IF(g.token_id IS NULL, 'SOL', 'Unknown')),
                'mint_address', ma.address,
                'dex', NULL, 'pool_label', NULL,
                'ins_index', g.ins_index, 'signature', t.signature, 'block_time', t.block_time,
                'block_time_utc', DATE_FORMAT(FROM_UNIXTIME(t.block_time), '%Y-%m-%dT%H:%i:%s'),
                'tax_bps', g.tax_bps,
                'tax_amount', fn_format_decimal(g.tax_amount, COALESCE(g.decimals, v_decimals, 9)),
                'source_funded_by', faf.address,
                'target_funded_by', taf.address,
                'mint_funded_by', mf.address
            ) AS edge_data
            FROM tmp_final_ids2 f JOIN tx_guide g ON g.id = f.guide_id
            JOIN tx t ON t.id = g.tx_id
            JOIN tx_address fa ON fa.id = g.from_address_id
            JOIN tx_address ta ON ta.id = g.to_address_id
            JOIN tx_guide_type gt ON gt.id = g.edge_type_id
            LEFT JOIN tx_token tk ON tk.id = g.token_id
            LEFT JOIN tx_address ma ON ma.id = tk.mint_address_id
            LEFT JOIN tx_address faf ON faf.id = fa.funded_by_address_id
            LEFT JOIN tx_address taf ON taf.id = ta.funded_by_address_id
            LEFT JOIN tx_address mf ON mf.id = ma.funded_by_address_id
            WHERE gt.type_code NOT IN ('swap_in', 'swap_out')
              AND NOT (fa.address_type = 'vault' AND ta.address_type = 'vault')
        ) edges;
    END IF;

    -- ==================== NODES (bit 2) ====================
    IF (p_result_mask & SECT_NODES) != 0 THEN
        DROP TEMPORARY TABLE IF EXISTS tmp_node_ids;
        CREATE TEMPORARY TABLE tmp_node_ids (address_id INT UNSIGNED PRIMARY KEY) ENGINE=MEMORY;
        INSERT IGNORE INTO tmp_node_ids SELECT g.from_address_id FROM tmp_final_ids f JOIN tx_guide g ON g.id = f.guide_id;
        INSERT IGNORE INTO tmp_node_ids SELECT g.to_address_id FROM tmp_final_ids f JOIN tx_guide g ON g.id = f.guide_id;
        INSERT IGNORE INTO tmp_node_ids SELECT pool.pool_address_id FROM tmp_final_ids f JOIN tx_guide g ON g.id = f.guide_id JOIN tx_pool pool ON pool.id = g.pool_id WHERE g.pool_id IS NOT NULL;

        -- Include mint as hub node when filtering by token
        IF v_token_id IS NOT NULL THEN
            INSERT IGNORE INTO tmp_node_ids SELECT tk.mint_address_id FROM tx_token tk WHERE tk.id = v_token_id;
        END IF;

        -- Prune vaults/dex_wallets with no visible non-swap edges
        DELETE n FROM tmp_node_ids n
        JOIN tx_address a ON a.id = n.address_id
        WHERE a.address_type IN ('vault', 'dex_wallet')
          AND NOT EXISTS (
            SELECT 1 FROM tmp_final_ids2 s
            JOIN tx_guide g ON g.id = s.guide_id
            JOIN tx_guide_type gt ON gt.id = g.edge_type_id
            WHERE (g.from_address_id = n.address_id OR g.to_address_id = n.address_id)
              AND gt.type_code NOT IN ('swap_in','swap_out')
        );

        -- Balance temp tables
        DROP TEMPORARY TABLE IF EXISTS tmp_token_bal;
        CREATE TEMPORARY TABLE tmp_token_bal (
            address_id INT UNSIGNED PRIMARY KEY, balance DECIMAL(30,9),
            block_time BIGINT UNSIGNED, guide_id BIGINT UNSIGNED
        ) ENGINE=MEMORY;
        DROP TEMPORARY TABLE IF EXISTS tmp_sol_bal;
        CREATE TEMPORARY TABLE tmp_sol_bal (
            address_id INT UNSIGNED PRIMARY KEY, sol_balance DECIMAL(20,9),
            block_time BIGINT UNSIGNED, guide_id BIGINT UNSIGNED
        ) ENGINE=MEMORY;

        IF (p_result_mask & SECT_BALANCES) != 0 THEN
            -- Token balances from surviving edges
            INSERT INTO tmp_token_bal
            SELECT g.from_address_id,
                ROUND(g.from_token_post_balance / POW(10, COALESCE(g.decimals, v_decimals, 9)), 9),
                f.block_time, f.guide_id
            FROM tmp_final_ids f JOIN tx_guide g ON g.id = f.guide_id
            WHERE g.from_token_post_balance IS NOT NULL
              AND g.from_address_id IN (SELECT address_id FROM tmp_node_ids)
            ON DUPLICATE KEY UPDATE
                balance = IF(VALUES(block_time) >= tmp_token_bal.block_time, VALUES(balance), tmp_token_bal.balance),
                block_time = GREATEST(VALUES(block_time), tmp_token_bal.block_time),
                guide_id = IF(VALUES(block_time) >= tmp_token_bal.block_time, VALUES(guide_id), tmp_token_bal.guide_id);

            INSERT INTO tmp_token_bal
            SELECT g.to_address_id,
                ROUND(g.to_token_post_balance / POW(10, COALESCE(g.decimals, v_decimals, 9)), 9),
                f.block_time, f.guide_id
            FROM tmp_final_ids f JOIN tx_guide g ON g.id = f.guide_id
            WHERE g.to_token_post_balance IS NOT NULL
              AND g.to_address_id IN (SELECT address_id FROM tmp_node_ids)
            ON DUPLICATE KEY UPDATE
                balance = IF(VALUES(block_time) >= tmp_token_bal.block_time, VALUES(balance), tmp_token_bal.balance),
                block_time = GREATEST(VALUES(block_time), tmp_token_bal.block_time),
                guide_id = IF(VALUES(block_time) >= tmp_token_bal.block_time, VALUES(guide_id), tmp_token_bal.guide_id);

            -- SOL balances
            INSERT IGNORE INTO tmp_sol_bal
            SELECT sub.address_id, sub.sol_balance, sub.block_time, sub.guide_id FROM (
                SELECT g.from_address_id AS address_id,
                    ROUND(g.from_sol_post_balance / 1e9, 9) AS sol_balance,
                    f.block_time, f.guide_id,
                    ROW_NUMBER() OVER (PARTITION BY g.from_address_id ORDER BY f.block_time DESC) AS rn
                FROM tmp_final_ids f JOIN tx_guide g ON g.id = f.guide_id
                WHERE g.from_sol_post_balance IS NOT NULL
                  AND g.from_address_id IN (SELECT address_id FROM tmp_node_ids)
            ) sub WHERE sub.rn = 1;

            INSERT INTO tmp_sol_bal
            SELECT sub.address_id, sub.sol_balance, sub.block_time, sub.guide_id FROM (
                SELECT g.to_address_id AS address_id,
                    ROUND(g.to_sol_post_balance / 1e9, 9) AS sol_balance,
                    f.block_time, f.guide_id,
                    ROW_NUMBER() OVER (PARTITION BY g.to_address_id ORDER BY f.block_time DESC) AS rn
                FROM tmp_final_ids f JOIN tx_guide g ON g.id = f.guide_id
                WHERE g.to_sol_post_balance IS NOT NULL
                  AND g.to_address_id IN (SELECT address_id FROM tmp_node_ids)
            ) sub WHERE sub.rn = 1
            ON DUPLICATE KEY UPDATE
                sol_balance = IF(VALUES(block_time) >= tmp_sol_bal.block_time, VALUES(sol_balance), tmp_sol_bal.sol_balance),
                block_time = GREATEST(VALUES(block_time), tmp_sol_bal.block_time),
                guide_id = IF(VALUES(block_time) >= tmp_sol_bal.block_time, VALUES(guide_id), tmp_sol_bal.guide_id);
        END IF;

        -- Interactions
        DROP TEMPORARY TABLE IF EXISTS tmp_ix_pools;
        DROP TEMPORARY TABLE IF EXISTS tmp_ix_dexes;
        CREATE TEMPORARY TABLE tmp_ix_pools (address_id INT UNSIGNED PRIMARY KEY, pools JSON);
        CREATE TEMPORARY TABLE tmp_ix_dexes (address_id INT UNSIGNED PRIMARY KEY, dexes JSON);

        IF (p_result_mask & SECT_INTERACTIONS) != 0 THEN
            INSERT INTO tmp_ix_pools
            SELECT address_id, JSON_ARRAYAGG(JSON_OBJECT('address', pool_addr, 'label', pool_lbl)) FROM (
                SELECT DISTINCT n.address_id, pa.address AS pool_addr, COALESCE(pool.pool_label, pa.address) AS pool_lbl
                FROM tmp_node_ids n
                JOIN tx_guide g ON (g.from_address_id = n.address_id OR g.to_address_id = n.address_id) AND g.pool_id IS NOT NULL
                JOIN tmp_limit_txs lt ON lt.tx_id = g.tx_id
                JOIN tx_pool pool ON pool.id = g.pool_id
                JOIN tx_address pa ON pa.id = pool.pool_address_id
            ) distinct_pools GROUP BY address_id;

            INSERT INTO tmp_ix_dexes
            SELECT address_id, JSON_ARRAYAGG(dex_name) FROM (
                SELECT DISTINCT n.address_id, pool.dex AS dex_name
                FROM tmp_node_ids n
                JOIN tx_guide g ON (g.from_address_id = n.address_id OR g.to_address_id = n.address_id) AND g.pool_id IS NOT NULL
                JOIN tmp_limit_txs lt ON lt.tx_id = g.tx_id
                JOIN tx_pool pool ON pool.id = g.pool_id AND pool.dex IS NOT NULL
            ) dex_distinct GROUP BY address_id;
        END IF;

        SELECT JSON_ARRAYAGG(node_data) INTO v_nodes_json FROM (
            SELECT JSON_OBJECT(
                'address', a.address, 'label', COALESCE(a.label, a.address_type),
                'address_type', a.address_type, 'pool_label', p.pool_label,
                'token_name', tk.token_name,
                'balance', ROUND(COALESCE(tb.balance, 0), 6),
                'sol_balance', ROUND(COALESCE(sb.sol_balance, 0), 9),
                'funded_by', fund.address,
                'interactions', IF((p_result_mask & SECT_INTERACTIONS) != 0,
                    JSON_OBJECT(
                        'pools', COALESCE(ixp.pools, JSON_ARRAY()),
                        'programs', JSON_ARRAY(),
                        'dexes', COALESCE(ixd.dexes, JSON_ARRAY())
                    ), NULL)
            ) AS node_data
            FROM tmp_node_ids n JOIN tx_address a ON a.id = n.address_id
            LEFT JOIN tx_address fund ON fund.id = a.funded_by_address_id
            LEFT JOIN tx_pool p ON p.pool_address_id = n.address_id
            LEFT JOIN tx_token tk ON tk.mint_address_id = n.address_id
            LEFT JOIN tmp_token_bal tb ON tb.address_id = n.address_id
            LEFT JOIN tmp_sol_bal sb ON sb.address_id = n.address_id
            LEFT JOIN tmp_ix_pools ixp ON ixp.address_id = n.address_id
            LEFT JOIN tmp_ix_dexes ixd ON ixd.address_id = n.address_id
        ) nodes;

        DROP TEMPORARY TABLE IF EXISTS tmp_ix_pools, tmp_ix_dexes;
        DROP TEMPORARY TABLE IF EXISTS tmp_node_ids, tmp_token_bal, tmp_sol_bal;
    END IF;

    -- ==================== NAVIGATION (bit 64) ====================
    IF (p_result_mask & SECT_NAVIGATION) != 0 AND v_tx_id IS NOT NULL THEN
        -- Build prev/next using the same filters that collected edges
        -- This ensures navigation follows the same scope (token, address, or both)

        IF v_token_id IS NOT NULL THEN
            -- Token-scoped navigation (most common: browsing a token's activity)
            SET @sql_prev = CONCAT(
                'SELECT JSON_ARRAYAGG(JSON_OBJECT(',
                '''signature'',t.signature,''block_time'',sub.block_time,',
                '''block_time_utc'',DATE_FORMAT(FROM_UNIXTIME(sub.block_time),''%Y-%m-%dT%H:%i:%s''),',
                '''edge_types'',(SELECT JSON_ARRAYAGG(dt.type_code) FROM (',
                'SELECT DISTINCT gt.type_code FROM tx_guide g JOIN tx_guide_type gt ON gt.id=g.edge_type_id ',
                'WHERE g.token_id=',v_token_id,' AND g.tx_id=sub.tx_id) dt)',
                ')) INTO @prev_json FROM (',
                'SELECT DISTINCT g.tx_id,g.block_time FROM tx_guide g WHERE g.token_id=',v_token_id,
                ' AND (g.block_time,g.tx_id)<(',v_block_time,',',v_tx_id,') ORDER BY g.block_time DESC,g.tx_id DESC LIMIT 5',
                ') sub JOIN tx t ON t.id=sub.tx_id ORDER BY sub.block_time DESC');
            PREPARE stmt FROM @sql_prev; EXECUTE stmt; DEALLOCATE PREPARE stmt;
            SET v_prev_json = @prev_json;

            SET @sql_next = CONCAT(
                'SELECT JSON_ARRAYAGG(JSON_OBJECT(',
                '''signature'',t.signature,''block_time'',sub.block_time,',
                '''block_time_utc'',DATE_FORMAT(FROM_UNIXTIME(sub.block_time),''%Y-%m-%dT%H:%i:%s''),',
                '''edge_types'',(SELECT JSON_ARRAYAGG(dt.type_code) FROM (',
                'SELECT DISTINCT gt.type_code FROM tx_guide g JOIN tx_guide_type gt ON gt.id=g.edge_type_id ',
                'WHERE g.token_id=',v_token_id,' AND g.tx_id=sub.tx_id) dt)',
                ')) INTO @next_json FROM (',
                'SELECT DISTINCT g.tx_id,g.block_time FROM tx_guide g WHERE g.token_id=',v_token_id,
                ' AND (g.block_time,g.tx_id)>(',v_block_time,',',v_tx_id,') ORDER BY g.block_time ASC,g.tx_id ASC LIMIT 5',
                ') sub JOIN tx t ON t.id=sub.tx_id ORDER BY sub.block_time ASC');
            PREPARE stmt FROM @sql_next; EXECUTE stmt; DEALLOCATE PREPARE stmt;
            SET v_next_json = @next_json;

        ELSEIF v_address_id IS NOT NULL THEN
            -- Address-scoped navigation
            SET @sql_prev = CONCAT(
                'SELECT JSON_ARRAYAGG(JSON_OBJECT(',
                '''signature'',t.signature,''block_time'',sub.block_time,',
                '''block_time_utc'',DATE_FORMAT(FROM_UNIXTIME(sub.block_time),''%Y-%m-%dT%H:%i:%s''),',
                '''edge_types'',(SELECT JSON_ARRAYAGG(dt.type_code) FROM (',
                'SELECT DISTINCT gt.type_code FROM tx_guide g JOIN tx_guide_type gt ON gt.id=g.edge_type_id ',
                'WHERE (g.from_address_id=',v_address_id,' OR g.to_address_id=',v_address_id,') AND g.tx_id=sub.tx_id) dt)',
                ')) INTO @prev_json FROM (',
                'SELECT DISTINCT g.tx_id, MIN(t.block_time) AS block_time FROM tx_guide g JOIN tx t ON t.id=g.tx_id',
                ' WHERE (g.from_address_id=',v_address_id,' OR g.to_address_id=',v_address_id,')',
                ' AND t.block_time<',v_block_time,' GROUP BY g.tx_id ORDER BY block_time DESC LIMIT 5',
                ') sub JOIN tx t ON t.id=sub.tx_id ORDER BY sub.block_time DESC');
            PREPARE stmt FROM @sql_prev; EXECUTE stmt; DEALLOCATE PREPARE stmt;
            SET v_prev_json = @prev_json;

            SET @sql_next = CONCAT(
                'SELECT JSON_ARRAYAGG(JSON_OBJECT(',
                '''signature'',t.signature,''block_time'',sub.block_time,',
                '''block_time_utc'',DATE_FORMAT(FROM_UNIXTIME(sub.block_time),''%Y-%m-%dT%H:%i:%s''),',
                '''edge_types'',(SELECT JSON_ARRAYAGG(dt.type_code) FROM (',
                'SELECT DISTINCT gt.type_code FROM tx_guide g JOIN tx_guide_type gt ON gt.id=g.edge_type_id ',
                'WHERE (g.from_address_id=',v_address_id,' OR g.to_address_id=',v_address_id,') AND g.tx_id=sub.tx_id) dt)',
                ')) INTO @next_json FROM (',
                'SELECT DISTINCT g.tx_id, MIN(t.block_time) AS block_time FROM tx_guide g JOIN tx t ON t.id=g.tx_id',
                ' WHERE (g.from_address_id=',v_address_id,' OR g.to_address_id=',v_address_id,')',
                ' AND t.block_time>',v_block_time,' GROUP BY g.tx_id ORDER BY block_time ASC LIMIT 5',
                ') sub JOIN tx t ON t.id=sub.tx_id ORDER BY sub.block_time ASC');
            PREPARE stmt FROM @sql_next; EXECUTE stmt; DEALLOCATE PREPARE stmt;
            SET v_next_json = @next_json;
        ELSEIF v_signer_address_id IS NOT NULL THEN
            -- Sig-only: navigate by signer address (what else did this wallet do?)
            SET @sql_prev = CONCAT(
                'SELECT JSON_ARRAYAGG(JSON_OBJECT(',
                '''signature'',t.signature,''block_time'',sub.block_time,',
                '''block_time_utc'',DATE_FORMAT(FROM_UNIXTIME(sub.block_time),''%Y-%m-%dT%H:%i:%s''),',
                '''edge_types'',(SELECT JSON_ARRAYAGG(dt.type_code) FROM (',
                'SELECT DISTINCT gt.type_code FROM tx_guide g JOIN tx_guide_type gt ON gt.id=g.edge_type_id ',
                'WHERE (g.from_address_id=',v_signer_address_id,' OR g.to_address_id=',v_signer_address_id,') AND g.tx_id=sub.tx_id) dt)',
                ')) INTO @prev_json FROM (',
                'SELECT DISTINCT g.tx_id, MIN(t.block_time) AS block_time FROM tx_guide g JOIN tx t ON t.id=g.tx_id',
                ' WHERE (g.from_address_id=',v_signer_address_id,' OR g.to_address_id=',v_signer_address_id,')',
                ' AND t.block_time<',v_block_time,' GROUP BY g.tx_id ORDER BY block_time DESC LIMIT 5',
                ') sub JOIN tx t ON t.id=sub.tx_id ORDER BY sub.block_time DESC');
            PREPARE stmt FROM @sql_prev; EXECUTE stmt; DEALLOCATE PREPARE stmt;
            SET v_prev_json = @prev_json;

            SET @sql_next = CONCAT(
                'SELECT JSON_ARRAYAGG(JSON_OBJECT(',
                '''signature'',t.signature,''block_time'',sub.block_time,',
                '''block_time_utc'',DATE_FORMAT(FROM_UNIXTIME(sub.block_time),''%Y-%m-%dT%H:%i:%s''),',
                '''edge_types'',(SELECT JSON_ARRAYAGG(dt.type_code) FROM (',
                'SELECT DISTINCT gt.type_code FROM tx_guide g JOIN tx_guide_type gt ON gt.id=g.edge_type_id ',
                'WHERE (g.from_address_id=',v_signer_address_id,' OR g.to_address_id=',v_signer_address_id,') AND g.tx_id=sub.tx_id) dt)',
                ')) INTO @next_json FROM (',
                'SELECT DISTINCT g.tx_id, MIN(t.block_time) AS block_time FROM tx_guide g JOIN tx t ON t.id=g.tx_id',
                ' WHERE (g.from_address_id=',v_signer_address_id,' OR g.to_address_id=',v_signer_address_id,')',
                ' AND t.block_time>',v_block_time,' GROUP BY g.tx_id ORDER BY block_time ASC LIMIT 5',
                ') sub JOIN tx t ON t.id=sub.tx_id ORDER BY sub.block_time ASC');
            PREPARE stmt FROM @sql_next; EXECUTE stmt; DEALLOCATE PREPARE stmt;
            SET v_next_json = @next_json;
        END IF;

        -- Current tx edge types
        IF v_tx_id IS NOT NULL THEN
            SELECT JSON_ARRAYAGG(dt.type_code) INTO v_current_edge_types FROM (
                SELECT DISTINCT gt.type_code FROM tmp_final_ids2 s
                JOIN tx_guide g ON g.id = s.guide_id JOIN tx_guide_type gt ON gt.id = g.edge_type_id
                WHERE s.tx_id = v_tx_id) dt;
        END IF;
    END IF;

    -- ==================== RELATED TOKENS (bit 128) ====================
    IF (p_result_mask & SECT_RELATED) != 0 THEN
        -- Show ALL tokens from edges in the window (grouped by token)
        SELECT JSON_ARRAYAGG(related_data) INTO v_related_tokens_json FROM (
            SELECT JSON_OBJECT(
                'mint', mint_addr.address,
                'symbol', COALESCE(tk.token_symbol, 'Unknown'),
                'name', tk.token_name,
                'token_type', tk.token_type,
                'edge_count', COUNT(DISTINCT s.guide_id),
                'swap_count', SUM(IF(gt.type_code IN ('swap_in','swap_out'), 1, 0)),
                'total_volume', ROUND(SUM(g.amount / POW(10, COALESCE(g.decimals, 9))), 6)
            ) AS related_data
            FROM tmp_final_ids2 s
            JOIN tx_guide g ON g.id = s.guide_id
            JOIN tx_guide_type gt ON gt.id = g.edge_type_id
            JOIN tx_token tk ON tk.id = g.token_id
            JOIN tx_address mint_addr ON mint_addr.id = tk.mint_address_id
            WHERE g.token_id IS NOT NULL
            GROUP BY tk.id, mint_addr.address, tk.token_symbol, tk.token_name, tk.token_type
            ORDER BY COUNT(DISTINCT s.guide_id) DESC LIMIT 20
        ) related_outer;
    END IF;

    -- ==================== BOUNDARY ====================
    IF v_token_id IS NOT NULL THEN
        SELECT JSON_OBJECT(
            'boundary_sig', b.boundary_sig, 'boundary_block_time', b.boundary_block_time,
            'oldest_fetched_sig', b.oldest_fetched_sig, 'oldest_fetched_block_time', b.oldest_fetched_block_time,
            'fetched_count', b.fetched_count, 'end_of_history', IF(b.boundary_sig IS NULL, TRUE, FALSE)
        ) INTO v_boundary_json
        FROM tx_boundary b JOIN tx_token tk ON tk.mint_address_id = b.address_id
        WHERE tk.id = v_token_id ORDER BY b.id DESC LIMIT 1;
    ELSEIF v_address_id IS NOT NULL THEN
        SELECT JSON_OBJECT(
            'boundary_sig', b.boundary_sig, 'boundary_block_time', b.boundary_block_time,
            'oldest_fetched_sig', b.oldest_fetched_sig, 'oldest_fetched_block_time', b.oldest_fetched_block_time,
            'fetched_count', b.fetched_count, 'end_of_history', IF(b.boundary_sig IS NULL, TRUE, FALSE)
        ) INTO v_boundary_json
        FROM tx_boundary b WHERE b.address_id = v_address_id ORDER BY b.id DESC LIMIT 1;
    END IF;

    -- ==================== JSON ENVELOPE ====================
    SELECT JSON_OBJECT(
        'token', IF(v_token_id IS NOT NULL, JSON_OBJECT(
            'mint', v_mint_address, 'symbol', v_token_symbol,
            'token_type', v_token_type, 'decimals', v_decimals), NULL),
        'address', p_address,
        'address_funded_by', IF(v_address_id IS NOT NULL, (
            SELECT fund.address FROM tx_address wa JOIN tx_address fund ON fund.id = wa.funded_by_address_id
            WHERE wa.id = v_address_id), NULL),
        'count', v_tx_count, 'edge_count', v_edge_count,
        'current_tx', IF(v_tx_id IS NOT NULL, JSON_OBJECT(
            'signature', v_signature, 'block_time', v_block_time,
            'block_time_utc', DATE_FORMAT(FROM_UNIXTIME(v_block_time), '%Y-%m-%dT%H:%i:%s'),
            'signer', v_signer_address,
            'edge_types', COALESCE(v_current_edge_types, JSON_ARRAY())), NULL),
        'prev', COALESCE(v_prev_json, JSON_ARRAY()),
        'next', COALESCE(v_next_json, JSON_ARRAY()),
        'nodes', COALESCE(v_nodes_json, JSON_ARRAY()),
        'edges', COALESCE(v_edges_json, JSON_ARRAY()),
        'related_tokens', COALESCE(v_related_tokens_json, JSON_ARRAY()),
        'boundary', v_boundary_json
    ) AS result;

    -- Cleanup
    DROP TEMPORARY TABLE IF EXISTS tmp_guide_ids, tmp_limit_txs;
    DROP TEMPORARY TABLE IF EXISTS tmp_final_ids, tmp_final_ids2;

END //
DELIMITER ;
