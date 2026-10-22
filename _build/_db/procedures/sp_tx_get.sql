-- sp_tx_get: Unified transaction query
--
-- Data section bits (0-7):
--     1  SECT_EDGES         Core edge list (source/target/amount/type)
--     2  SECT_NODES         Distinct address nodes with labels/types
--     4  SECT_BALANCES      Token + SOL balance on nodes (requires bit 1)
--     8  SECT_INTERACTIONS  Pool/program/dex per node (requires bit 1)
--    16  SECT_WALLET        direction/party/post_balance on edges
--    32  SECT_TRANSACTIONS  Flat wallet tx rows
--    64  SECT_NAVIGATION    Prev/next transaction lists with edge_types
--   128  SECT_RELATED       Other tokens from swap edges
-- 256-2048 reserved for future data sections
--
-- View preset bits (12-13):
--  4096  VIEW_GRAPH         Expands to 207 (graph visualization preset)
--  8192  VIEW_LIST          Expands to 112 (transaction table preset: WALLET+TRANSACTIONS+NAV)
--
-- Presets:  4096=graph, 8192=list, 12288=both, 255=all sections, 1=edges only
-- Combined: 4096|8192=12288 (both views), 8192|2=8194 (list + nodes)
--
-- Uses persistent tx_guide_stage table instead of temp tables for edge IDs.
-- Sequence number (v_seq) isolates each call's rows; returned in JSON envelope
-- for follow-up pagination. Background worker cleans old rows by created_at.

DELIMITER //
DROP PROCEDURE IF EXISTS sp_tx_get//
CREATE PROCEDURE sp_tx_get(
    IN p_address      VARCHAR(44),
    IN p_mint_address VARCHAR(44),
    IN p_token_symbol VARCHAR(128),
    IN p_signature    VARCHAR(88),
    IN p_limit        SMALLINT UNSIGNED,
    IN p_after        BIGINT UNSIGNED,
    IN p_before       BIGINT UNSIGNED,
    IN p_type_mask    BIGINT UNSIGNED,
    IN p_result_mask  INT UNSIGNED
)
proc_body: BEGIN
    -- Data section bits (0-7)
    DECLARE SECT_EDGES INT UNSIGNED DEFAULT 1;          -- bit 0
    DECLARE SECT_NODES INT UNSIGNED DEFAULT 2;          -- bit 1
    DECLARE SECT_BALANCES INT UNSIGNED DEFAULT 4;       -- bit 2
    DECLARE SECT_INTERACTIONS INT UNSIGNED DEFAULT 8;   -- bit 3
    DECLARE SECT_WALLET INT UNSIGNED DEFAULT 16;        -- bit 4
    DECLARE SECT_TRANSACTIONS INT UNSIGNED DEFAULT 32;  -- bit 5
    DECLARE SECT_NAVIGATION INT UNSIGNED DEFAULT 64;    -- bit 6
    DECLARE SECT_RELATED INT UNSIGNED DEFAULT 128;      -- bit 7
    -- bits 8-11 (256-2048) reserved for future data sections
    -- View preset bits (12-13)
    DECLARE VIEW_GRAPH INT UNSIGNED DEFAULT 4096;       -- bit 12: expands to 207 (EDGES+NODES+BALANCES+INTERACTIONS+NAV+RELATED)
    DECLARE VIEW_LIST INT UNSIGNED DEFAULT 8192;        -- bit 13: expands to 113 (EDGES+WALLET+TRANSACTIONS+NAV)

    DECLARE v_wallet_mode TINYINT DEFAULT 0;
    DECLARE v_wallet_id INT UNSIGNED DEFAULT NULL;
    DECLARE v_token_id BIGINT DEFAULT NULL;
    DECLARE v_mint_address VARCHAR(44) DEFAULT NULL;
    DECLARE v_token_symbol VARCHAR(128) DEFAULT NULL;
    DECLARE v_token_type VARCHAR(20) DEFAULT NULL;
    DECLARE v_decimals TINYINT UNSIGNED DEFAULT NULL;
    DECLARE v_anchor_block_time BIGINT UNSIGNED DEFAULT NULL;
    DECLARE v_tx_id BIGINT DEFAULT NULL;
    DECLARE v_signature VARCHAR(88) DEFAULT NULL;
    DECLARE v_block_time BIGINT UNSIGNED DEFAULT NULL;
    DECLARE v_signer_address VARCHAR(44) DEFAULT NULL;
    DECLARE v_tx_count INT UNSIGNED DEFAULT 0;
    DECLARE v_edge_count INT UNSIGNED DEFAULT 0;
    DECLARE v_half SMALLINT UNSIGNED;
    DECLARE v_seq BIGINT UNSIGNED;
    DECLARE v_edges_json JSON DEFAULT NULL;
    DECLARE v_nodes_json JSON DEFAULT NULL;
    DECLARE v_transactions_json JSON DEFAULT NULL;
    DECLARE v_prev_json JSON DEFAULT NULL;
    DECLARE v_next_json JSON DEFAULT NULL;
    DECLARE v_current_edge_types JSON DEFAULT NULL;
    DECLARE v_related_tokens_json JSON DEFAULT NULL;
    DECLARE v_boundary_json JSON DEFAULT NULL;

    -- ====================== A1: SETUP & DEFAULTS ======================
    SET v_wallet_mode = IF(p_address IS NOT NULL AND p_address != '', 1, 0);
    SET p_result_mask = COALESCE(p_result_mask, 255);
    SET p_limit = COALESCE(p_limit, IF(v_wallet_mode = 1, 50, 10));
    IF v_wallet_mode = 1 AND p_limit > 200 THEN SET p_limit = 200; END IF;
    IF v_wallet_mode = 0 AND p_limit > 100 THEN SET p_limit = 100; END IF;
    SET v_half = CEIL(p_limit / 2);

    -- Expand view presets into data section bits
    IF (p_result_mask & VIEW_GRAPH) != 0 THEN
        SET p_result_mask = p_result_mask | 207;  -- EDGES+NODES+BALANCES+INTERACTIONS+NAV+RELATED
    END IF;
    IF (p_result_mask & VIEW_LIST) != 0 THEN
        SET p_result_mask = p_result_mask | 112;  -- WALLET+TRANSACTIONS+NAV (no EDGES)
    END IF;

    -- Type mask: coalesce to all-bits-set so the bitmask check always works
    SET p_type_mask = COALESCE(p_type_mask, 18446744073709551615);

    -- Allocate a unique sequence number (atomic increment)
    UPDATE config SET config_value = config_value + 1
    WHERE config_type = 'system' AND config_key = 'stage_seq';
    SELECT CAST(config_value AS UNSIGNED) INTO v_seq
    FROM config WHERE config_type = 'system' AND config_key = 'stage_seq';

    -- ====================== A2: VALIDATION & RESOLUTION ======================
    IF v_wallet_mode = 1 THEN
        SELECT id INTO v_wallet_id FROM tx_address WHERE address = p_address LIMIT 1;
        IF v_wallet_id IS NULL THEN
            SELECT JSON_OBJECT('error', 'Wallet address not found', 'address', p_address) AS result;
            LEAVE proc_body;
        END IF;
    END IF;

    -- Token resolution (4-tier)
    IF p_mint_address IS NOT NULL AND p_mint_address != '' THEN
        SELECT tk.id, mint.address, tk.token_symbol, tk.token_type, tk.decimals
        INTO v_token_id, v_mint_address, v_token_symbol, v_token_type, v_decimals
        FROM tx_token tk JOIN tx_address mint ON mint.id = tk.mint_address_id
        WHERE mint.address = p_mint_address LIMIT 1;
    ELSEIF p_token_symbol IS NOT NULL AND p_token_symbol != '' THEN
        SELECT tk.id, mint.address, tk.token_symbol, tk.token_type, tk.decimals
        INTO v_token_id, v_mint_address, v_token_symbol, v_token_type, v_decimals
        FROM tx_token tk JOIN tx_address mint ON mint.id = tk.mint_address_id
        LEFT JOIN (SELECT token_id, COUNT(*) AS cnt FROM tx_guide GROUP BY token_id) gc ON gc.token_id = tk.id
        WHERE tk.token_symbol = p_token_symbol
        ORDER BY COALESCE(gc.cnt, 0) DESC, (mint.address_type = 'mint') DESC, tk.id LIMIT 1;
    ELSEIF p_signature IS NOT NULL AND p_signature != '' AND v_wallet_mode = 0 THEN
        -- Derive token from signature (graph mode only — wallet mode shows all tokens)
        SELECT t.id, t.signature, t.block_time, tk.id, mint.address, tk.token_symbol, tk.token_type, tk.decimals
        INTO v_tx_id, v_signature, v_block_time, v_token_id, v_mint_address, v_token_symbol, v_token_type, v_decimals
        FROM tx t JOIN tx_guide g ON g.tx_id = t.id
        JOIN tx_token tk ON tk.id = g.token_id JOIN tx_address mint ON mint.id = tk.mint_address_id
        WHERE t.signature = p_signature AND g.token_id IS NOT NULL
          AND UPPER(COALESCE(tk.token_symbol, '')) NOT IN ('SOL','WSOL','USDC','USDT','PYUSD','USDH','UXD','MSOL','JITOSOL','BSOL','LSTSOL')
        LIMIT 1;
        IF v_token_id IS NULL THEN
            SELECT t.id, t.signature, t.block_time, tk.id, mint.address, tk.token_symbol, tk.token_type, tk.decimals
            INTO v_tx_id, v_signature, v_block_time, v_token_id, v_mint_address, v_token_symbol, v_token_type, v_decimals
            FROM tx t JOIN tx_guide g ON g.tx_id = t.id
            JOIN tx_token tk ON tk.id = g.token_id JOIN tx_address mint ON mint.id = tk.mint_address_id
            WHERE t.signature = p_signature AND g.token_id IS NOT NULL LIMIT 1;
        END IF;
    ELSEIF p_signature IS NOT NULL AND p_signature != '' AND v_wallet_mode = 1 THEN
        -- Wallet mode with signature: resolve tx_id and block_time but don't lock to a token
        SELECT id, signature, block_time INTO v_tx_id, v_signature, v_block_time
        FROM tx WHERE signature = p_signature LIMIT 1;
    END IF;

    -- Graph mode requires token
    IF v_wallet_mode = 0 AND v_token_id IS NULL THEN
        SELECT JSON_OBJECT('error', 'Token not found') AS result;
        LEAVE proc_body;
    END IF;

    -- Anchor resolution (if not already resolved from sig-only path)
    IF v_tx_id IS NULL AND p_signature IS NOT NULL AND p_signature != '' THEN
        IF v_token_id IS NOT NULL THEN
            SELECT t.id, t.signature, t.block_time INTO v_tx_id, v_signature, v_block_time
            FROM tx_guide g JOIN tx t ON t.id = g.tx_id
            WHERE t.signature = p_signature AND g.token_id = v_token_id LIMIT 1;
            IF v_tx_id IS NULL AND EXISTS (SELECT 1 FROM tx WHERE signature = p_signature) THEN
                SELECT JSON_OBJECT('error', 'Signature not found for this token',
                    'signature', p_signature, 'mint', v_mint_address) AS result;
                LEAVE proc_body;
            ELSEIF v_tx_id IS NULL THEN
                SELECT JSON_OBJECT('error', 'Signature not found', 'signature', p_signature) AS result;
                LEAVE proc_body;
            END IF;
        ELSE
            SELECT id, signature, block_time INTO v_tx_id, v_signature, v_block_time
            FROM tx WHERE signature = p_signature LIMIT 1;
            IF v_tx_id IS NULL THEN
                SELECT JSON_OBJECT('error', 'Signature not found', 'signature', p_signature) AS result;
                LEAVE proc_body;
            END IF;
        END IF;
    ELSEIF v_tx_id IS NULL AND v_wallet_mode = 0 THEN
        -- Graph mode, no signature: most recent tx for token
        SELECT t.id, t.signature, t.block_time INTO v_tx_id, v_signature, v_block_time
        FROM tx_guide g JOIN tx t ON t.id = g.tx_id
        WHERE g.token_id = v_token_id ORDER BY g.block_time DESC LIMIT 1;
        IF v_tx_id IS NULL THEN
            SELECT JSON_OBJECT('error', 'No transactions found for this token', 'mint', v_mint_address) AS result;
            LEAVE proc_body;
        END IF;
    END IF;

    -- Only use anchor splitting when caller explicitly provided a signature to center on.
    -- Graph mode's auto-resolved "most recent tx" should NOT trigger splitting (it's the edge, so half-after=0).
    IF p_signature IS NOT NULL AND p_signature != '' THEN
        SET v_anchor_block_time = v_block_time;
        IF v_anchor_block_time IS NULL THEN
            SELECT block_time INTO v_anchor_block_time FROM tx WHERE signature = p_signature LIMIT 1;
        END IF;
    END IF;

    -- Graph mode: resolve signer for anchor
    IF v_wallet_mode = 0 AND v_tx_id IS NOT NULL THEN
        SELECT a.address INTO v_signer_address
        FROM tx t JOIN tx_address a ON a.id = t.signer_address_id WHERE t.id = v_tx_id;
    END IF;

    -- ====================== PHASE A: FILTER & ID GATHERING ======================
    -- Phase A uses MEMORY temp tables for fast filtering (no InnoDB write overhead).
    -- Only the small surviving set gets written to tx_guide_stage for Phase B.

    -- A3: Collect all matching guide_ids into MEMORY temp table
    DROP TEMPORARY TABLE IF EXISTS tmp_guide_ids;
    CREATE TEMPORARY TABLE tmp_guide_ids (
        guide_id BIGINT UNSIGNED, tx_id BIGINT UNSIGNED,
        block_time BIGINT UNSIGNED, direction VARCHAR(4),
        INDEX idx_bt (block_time), INDEX idx_tx (tx_id)
    ) ENGINE=MEMORY;

    -- FAST PATH: signature-based lookup — skip full token scan
    -- When we already resolved a tx_id from the signature, get edges by tx_id directly.
    -- For limit=1: just this tx. For limit>1 (context): this tx + neighbors by block_time.
    IF v_tx_id IS NOT NULL AND p_signature IS NOT NULL AND p_signature != '' AND v_wallet_mode = 0 THEN
        IF p_limit = 1 THEN
            -- Single tx: point lookup
            INSERT INTO tmp_guide_ids
            SELECT g.id, g.tx_id, v_block_time, NULL
            FROM tx_guide g WHERE g.tx_id = v_tx_id;
        ELSE
            -- Context: this tx + neighbors via token_id + block_time index
            -- Get half before, half after by block_time, scoped to this token
            SET v_half = CEIL(p_limit / 2);

            -- Before/at anchor (including anchor itself)
            INSERT INTO tmp_guide_ids
            SELECT g.id, g.tx_id, t.block_time, NULL
            FROM tx_guide g JOIN tx t ON t.id = g.tx_id
            WHERE g.token_id = v_token_id AND t.block_time <= v_block_time
              AND (p_type_mask IS NULL OR EXISTS (
                  SELECT 1 FROM tx_guide_type gt WHERE gt.id = g.edge_type_id AND (gt.indicator & p_type_mask) != 0))
            ORDER BY t.block_time DESC LIMIT 10000;
            -- Note: we collect more than v_half edges here because multiple edges per tx.
            -- The limiting step (A4) will narrow to v_half distinct tx_ids.

            -- After anchor
            INSERT INTO tmp_guide_ids
            SELECT g.id, g.tx_id, t.block_time, NULL
            FROM tx_guide g JOIN tx t ON t.id = g.tx_id
            WHERE g.token_id = v_token_id AND t.block_time > v_block_time
              AND (p_type_mask IS NULL OR EXISTS (
                  SELECT 1 FROM tx_guide_type gt WHERE gt.id = g.edge_type_id AND (gt.indicator & p_type_mask) != 0))
            ORDER BY t.block_time ASC LIMIT 10000;
        END IF;

    ELSEIF v_wallet_mode = 1 THEN
        INSERT INTO tmp_guide_ids
        SELECT g.id, g.tx_id, t.block_time, 'out'
        FROM tx_guide g JOIN tx t ON t.id = g.tx_id
            AND (p_after IS NULL OR t.block_time > p_after) AND (p_before IS NULL OR t.block_time < p_before)
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id AND (gt.indicator & p_type_mask) != 0
        WHERE g.from_address_id = v_wallet_id AND (v_token_id IS NULL OR g.token_id = v_token_id)
        UNION ALL
        SELECT g.id, g.tx_id, t.block_time, 'in'
        FROM tx_guide g JOIN tx t ON t.id = g.tx_id
            AND (p_after IS NULL OR t.block_time > p_after) AND (p_before IS NULL OR t.block_time < p_before)
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id AND (gt.indicator & p_type_mask) != 0
        WHERE g.to_address_id = v_wallet_id AND (v_token_id IS NULL OR g.token_id = v_token_id)
        UNION ALL
        SELECT g.id, g.tx_id, t.block_time,
            CASE WHEN g.from_address_id = v_wallet_id THEN 'out'
                 WHEN g.to_address_id = v_wallet_id THEN 'in' ELSE 'swap' END
        FROM tx_guide g JOIN tx t ON t.id = g.tx_id
            AND (p_after IS NULL OR t.block_time > p_after) AND (p_before IS NULL OR t.block_time < p_before)
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id AND (gt.indicator & p_type_mask) != 0
        JOIN tx_pool wp ON wp.id = g.pool_id AND wp.pool_address_id = v_wallet_id
        WHERE g.from_address_id != v_wallet_id
          AND g.to_address_id != v_wallet_id AND (v_token_id IS NULL OR g.token_id = v_token_id);
    ELSE
        INSERT INTO tmp_guide_ids
        SELECT g.id, g.tx_id, t.block_time, NULL
        FROM tx_guide g JOIN tx t ON t.id = g.tx_id
            AND (p_after IS NULL OR t.block_time > p_after) AND (p_before IS NULL OR t.block_time < p_before)
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id AND (gt.indicator & p_type_mask) != 0
        WHERE g.token_id = v_token_id;
    END IF;

    -- A4: Transaction limiting (small MEMORY temp table)
    DROP TEMPORARY TABLE IF EXISTS tmp_limit_txs;
    IF v_anchor_block_time IS NOT NULL THEN
        IF p_limit = 1 THEN
            CREATE TEMPORARY TABLE tmp_limit_txs ENGINE=MEMORY AS
            SELECT DISTINCT tx_id, block_time FROM tmp_guide_ids
            WHERE block_time = v_anchor_block_time LIMIT 1;
        ELSE
            CREATE TEMPORARY TABLE tmp_limit_txs ENGINE=MEMORY AS
            SELECT DISTINCT tx_id, block_time FROM tmp_guide_ids
            WHERE block_time <= v_anchor_block_time ORDER BY block_time DESC LIMIT v_half;
            INSERT IGNORE INTO tmp_limit_txs
            SELECT DISTINCT tx_id, block_time FROM tmp_guide_ids
            WHERE block_time > v_anchor_block_time ORDER BY block_time ASC LIMIT v_half;
        END IF;
    ELSE
        CREATE TEMPORARY TABLE tmp_limit_txs (
            tx_id BIGINT UNSIGNED PRIMARY KEY, block_time BIGINT UNSIGNED
        ) ENGINE=MEMORY AS
        SELECT DISTINCT tx_id, block_time FROM tmp_guide_ids ORDER BY block_time DESC LIMIT p_limit;
    END IF;

    -- Set anchor for wallet navigation when no signature was provided
    -- (uses most recent tx in window as anchor so prev/next can work)
    IF v_wallet_mode = 1 AND v_anchor_block_time IS NULL THEN
        SELECT MAX(block_time) INTO v_anchor_block_time FROM tmp_limit_txs;
    END IF;

    -- A5: Materialize surviving edges into MEMORY temp tables
    DROP TEMPORARY TABLE IF EXISTS tmp_final_ids;
    CREATE TEMPORARY TABLE tmp_final_ids (
        guide_id BIGINT UNSIGNED, tx_id BIGINT UNSIGNED,
        block_time BIGINT UNSIGNED, direction VARCHAR(4),
        INDEX idx_guide (guide_id), INDEX idx_tx (tx_id)
    ) ENGINE=MEMORY AS
    SELECT DISTINCT g.guide_id, g.tx_id, g.block_time, g.direction
    FROM tmp_guide_ids g JOIN tmp_limit_txs lt ON lt.tx_id = g.tx_id;

    -- Clones for UNION/subquery reopen workaround (MySQL can't reopen same temp table)
    DROP TEMPORARY TABLE IF EXISTS tmp_final_ids2;
    CREATE TEMPORARY TABLE tmp_final_ids2 (
        guide_id BIGINT UNSIGNED, tx_id BIGINT UNSIGNED,
        block_time BIGINT UNSIGNED, direction VARCHAR(4),
        INDEX idx_guide (guide_id), INDEX idx_tx (tx_id)
    ) ENGINE=MEMORY AS SELECT * FROM tmp_final_ids;

    DROP TEMPORARY TABLE IF EXISTS tmp_final_ids3;
    CREATE TEMPORARY TABLE tmp_final_ids3 (
        guide_id BIGINT UNSIGNED, tx_id BIGINT UNSIGNED,
        block_time BIGINT UNSIGNED, direction VARCHAR(4),
        INDEX idx_guide (guide_id), INDEX idx_tx (tx_id)
    ) ENGINE=MEMORY AS SELECT * FROM tmp_final_ids;

    DROP TEMPORARY TABLE IF EXISTS tmp_final_ids4;
    CREATE TEMPORARY TABLE tmp_final_ids4 (
        guide_id BIGINT UNSIGNED, tx_id BIGINT UNSIGNED,
        block_time BIGINT UNSIGNED, direction VARCHAR(4),
        INDEX idx_guide (guide_id), INDEX idx_tx (tx_id)
    ) ENGINE=MEMORY AS SELECT * FROM tmp_final_ids;

    DROP TEMPORARY TABLE IF EXISTS tmp_final_ids5;
    CREATE TEMPORARY TABLE tmp_final_ids5 (
        guide_id BIGINT UNSIGNED, tx_id BIGINT UNSIGNED,
        block_time BIGINT UNSIGNED, direction VARCHAR(4),
        INDEX idx_guide (guide_id), INDEX idx_tx (tx_id)
    ) ENGINE=MEMORY AS SELECT * FROM tmp_final_ids;

    SELECT COUNT(DISTINCT tx_id) INTO v_tx_count FROM tmp_limit_txs;
    SELECT COUNT(*) INTO v_edge_count FROM tmp_final_ids;

    -- Also persist to staging table for replayability (forward-only, seq-based)
    INSERT INTO tx_guide_stage (seq, guide_id, tx_id, block_time, direction)
    SELECT v_seq, guide_id, tx_id, block_time, direction FROM tmp_final_ids;

    DROP TEMPORARY TABLE tmp_guide_ids;

    -- ====================== PHASE B: RENDER ======================

    -- B1: SECT_EDGES (bit 1)
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
                'mint_address', ma.address, 'dex', pool.dex, 'pool_label', pool.pool_label,
                'ins_index', g.ins_index, 'signature', t.signature, 'block_time', t.block_time,
                'block_time_utc', DATE_FORMAT(FROM_UNIXTIME(t.block_time), '%Y-%m-%dT%H:%i:%s'),
                'tax_bps', g.tax_bps, 'tax_amount', fn_format_decimal(g.tax_amount, COALESCE(g.decimals, v_decimals, 9)),
                'direction', IF((p_result_mask & SECT_WALLET) != 0, f.direction, NULL),
                'party', IF((p_result_mask & SECT_WALLET) != 0,
                    CASE f.direction WHEN 'out' THEN ta.address WHEN 'in' THEN fa.address
                        ELSE COALESCE(fa.address, ta.address) END, NULL),
                'party_label', IF((p_result_mask & SECT_WALLET) != 0,
                    CASE WHEN f.direction='out' AND g.to_address_id=t.signer_address_id THEN 'Signer'
                         WHEN f.direction='in' AND g.from_address_id=t.signer_address_id THEN 'Signer'
                         WHEN f.direction='swap' AND COALESCE(g.from_address_id,g.to_address_id)=t.signer_address_id THEN 'Signer'
                         WHEN f.direction='out' THEN COALESCE(ta.label, ta.address_type)
                         WHEN f.direction='in' THEN COALESCE(fa.label, fa.address_type)
                         ELSE COALESCE(fa.label, fa.address_type, ta.label, ta.address_type) END, NULL),
                'source_funded_by', faf.address,
                'target_funded_by', taf.address,
                'mint_funded_by', mf.address,
                'post_balance', IF((p_result_mask & SECT_WALLET) != 0,
                    fn_format_decimal(COALESCE(
                        CASE f.direction WHEN 'out' THEN g.from_token_post_balance WHEN 'in' THEN g.to_token_post_balance END,
                        CASE f.direction WHEN 'out' THEN g.from_sol_post_balance WHEN 'in' THEN g.to_sol_post_balance END
                    ), COALESCE(g.decimals, tk.decimals, 9)), NULL)
            ) AS edge_data
            FROM tmp_final_ids f JOIN tx_guide g ON g.id = f.guide_id
            JOIN tx t ON t.id = g.tx_id JOIN tx_address fa ON fa.id = g.from_address_id
            JOIN tx_address ta ON ta.id = g.to_address_id JOIN tx_guide_type gt ON gt.id = g.edge_type_id
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
            -- Non-swap edges: direct from→to
            SELECT JSON_OBJECT(
                'source', fa.address, 'source_label', COALESCE(fa.label, fa.address_type),
                'target', ta.address, 'target_label', COALESCE(ta.label, ta.address_type),
                'amount', fn_format_decimal(g.amount, COALESCE(g.decimals, v_decimals, 9)),
                'type', gt.type_code,
                'token_symbol', COALESCE(tk.token_symbol, IF(g.token_id IS NULL, 'SOL', 'Unknown')),
                'mint_address', ma.address, 'dex', NULL, 'pool_label', NULL,
                'ins_index', g.ins_index, 'signature', t.signature, 'block_time', t.block_time,
                'block_time_utc', DATE_FORMAT(FROM_UNIXTIME(t.block_time), '%Y-%m-%dT%H:%i:%s'),
                'tax_bps', g.tax_bps, 'tax_amount', fn_format_decimal(g.tax_amount, COALESCE(g.decimals, v_decimals, 9)),
                'direction', IF((p_result_mask & SECT_WALLET) != 0, f.direction, NULL),
                'party', IF((p_result_mask & SECT_WALLET) != 0,
                    CASE f.direction WHEN 'out' THEN ta.address WHEN 'in' THEN fa.address
                        ELSE COALESCE(fa.address, ta.address) END, NULL),
                'party_label', IF((p_result_mask & SECT_WALLET) != 0,
                    CASE WHEN f.direction='out' AND g.to_address_id=t.signer_address_id THEN 'Signer'
                         WHEN f.direction='in' AND g.from_address_id=t.signer_address_id THEN 'Signer'
                         WHEN f.direction='swap' AND COALESCE(g.from_address_id,g.to_address_id)=t.signer_address_id THEN 'Signer'
                         WHEN f.direction='out' THEN COALESCE(ta.label, ta.address_type)
                         WHEN f.direction='in' THEN COALESCE(fa.label, fa.address_type)
                         ELSE COALESCE(fa.label, fa.address_type, ta.label, ta.address_type) END, NULL),
                'source_funded_by', faf.address,
                'target_funded_by', taf.address,
                'mint_funded_by', mf.address,
                'post_balance', IF((p_result_mask & SECT_WALLET) != 0,
                    fn_format_decimal(COALESCE(
                        CASE f.direction WHEN 'out' THEN g.from_token_post_balance WHEN 'in' THEN g.to_token_post_balance END,
                        CASE f.direction WHEN 'out' THEN g.from_sol_post_balance WHEN 'in' THEN g.to_sol_post_balance END
                    ), COALESCE(g.decimals, tk.decimals, 9)), NULL)
            ) AS edge_data
            FROM tmp_final_ids2 f JOIN tx_guide g ON g.id = f.guide_id
            JOIN tx t ON t.id = g.tx_id JOIN tx_address fa ON fa.id = g.from_address_id
            JOIN tx_address ta ON ta.id = g.to_address_id JOIN tx_guide_type gt ON gt.id = g.edge_type_id
            LEFT JOIN tx_token tk ON tk.id = g.token_id LEFT JOIN tx_address ma ON ma.id = tk.mint_address_id
            LEFT JOIN tx_address faf ON faf.id = fa.funded_by_address_id
            LEFT JOIN tx_address taf ON taf.id = ta.funded_by_address_id
            LEFT JOIN tx_address mf ON mf.id = ma.funded_by_address_id
            WHERE gt.type_code NOT IN ('swap_in', 'swap_out')
              AND NOT (fa.address_type = 'vault' AND ta.address_type = 'vault')
            UNION ALL
            -- Synthetic mint-hub edges: connect each pool to the mint token node
            -- (pools trade this token but swap routing hides the mint — this reconnects disconnected clusters)
            SELECT JSON_OBJECT(
                'source', pa.address, 'source_label', COALESCE(pool.pool_label, pa.label, pa.address_type),
                'target', mint.address, 'target_label', COALESCE(mint.label, v_token_symbol),
                'amount', NULL, 'type', 'token_hub',
                'token_symbol', v_token_symbol, 'mint_address', mint.address,
                'dex', pool.dex, 'pool_label', pool.pool_label,
                'ins_index', NULL, 'signature', NULL, 'block_time', NULL,
                'block_time_utc', NULL, 'tax_bps', NULL, 'tax_amount', NULL,
                'direction', NULL, 'party', NULL, 'party_label', NULL,
                'source_funded_by', NULL, 'target_funded_by', NULL,
                'mint_funded_by', NULL, 'post_balance', NULL
            ) AS edge_data
            FROM (
                SELECT DISTINCT g.pool_id
                FROM tmp_final_ids3 f JOIN tx_guide g ON g.id = f.guide_id
                WHERE g.pool_id IS NOT NULL AND g.token_id = v_token_id
            ) dp
            JOIN tx_pool pool ON pool.id = dp.pool_id
            JOIN tx_address pa ON pa.id = pool.pool_address_id
            JOIN tx_token tk ON tk.id = v_token_id
            JOIN tx_address mint ON mint.id = tk.mint_address_id
            WHERE v_token_id IS NOT NULL
            UNION ALL
            -- Synthetic mint-hub edges for non-swap participants (transfers, burns, etc.)
            -- Connects wallets doing spl_transfer of this token to the mint hub
            SELECT JSON_OBJECT(
                'source', wa.address, 'source_label', COALESCE(wa.label, wa.address_type),
                'target', mint.address, 'target_label', COALESCE(mint.label, v_token_symbol),
                'amount', NULL, 'type', 'token_hub',
                'token_symbol', v_token_symbol, 'mint_address', mint.address,
                'dex', NULL, 'pool_label', NULL,
                'ins_index', NULL, 'signature', NULL, 'block_time', NULL,
                'block_time_utc', NULL, 'tax_bps', NULL, 'tax_amount', NULL,
                'direction', NULL, 'party', NULL, 'party_label', NULL,
                'source_funded_by', NULL, 'target_funded_by', NULL,
                'mint_funded_by', NULL, 'post_balance', NULL
            ) AS edge_data
            FROM (
                SELECT DISTINCT addr_id FROM (
                    SELECT g.from_address_id AS addr_id
                    FROM tmp_final_ids4 f JOIN tx_guide g ON g.id = f.guide_id
                    JOIN tx_guide_type gt ON gt.id = g.edge_type_id
                    WHERE g.token_id = v_token_id AND g.pool_id IS NULL
                      AND gt.type_code NOT IN ('swap_in','swap_out')
                ) from_addrs
                UNION
                SELECT DISTINCT g.to_address_id AS addr_id
                FROM tmp_final_ids5 f JOIN tx_guide g ON g.id = f.guide_id
                JOIN tx_guide_type gt ON gt.id = g.edge_type_id
                WHERE g.token_id = v_token_id AND g.pool_id IS NULL
                  AND gt.type_code NOT IN ('swap_in','swap_out')
            ) da
            JOIN tx_address wa ON wa.id = da.addr_id
            JOIN tx_token tk ON tk.id = v_token_id
            JOIN tx_address mint ON mint.id = tk.mint_address_id
            WHERE v_token_id IS NOT NULL AND da.addr_id != tk.mint_address_id
        ) edges;
    END IF;

    -- B2: SECT_NODES (bit 2)
    IF (p_result_mask & SECT_NODES) != 0 THEN
        DROP TEMPORARY TABLE IF EXISTS tmp_node_ids;
        CREATE TEMPORARY TABLE tmp_node_ids (address_id INT UNSIGNED PRIMARY KEY) ENGINE=MEMORY;
        INSERT IGNORE INTO tmp_node_ids SELECT g.from_address_id FROM tmp_final_ids f JOIN tx_guide g ON g.id = f.guide_id;
        INSERT IGNORE INTO tmp_node_ids SELECT g.to_address_id FROM tmp_final_ids f JOIN tx_guide g ON g.id = f.guide_id;
        INSERT IGNORE INTO tmp_node_ids SELECT pool.pool_address_id FROM tmp_final_ids f JOIN tx_guide g ON g.id = f.guide_id JOIN tx_pool pool ON pool.id = g.pool_id;

        -- Include mint token as hub node when filtering by token (connects disconnected swap clusters)
        IF v_token_id IS NOT NULL THEN
            INSERT IGNORE INTO tmp_node_ids SELECT tk.mint_address_id FROM tx_token tk WHERE tk.id = v_token_id;
        END IF;

        -- Prune nodes with zero visible edges (vaults/dex_wallets whose swap edges are routed through pools)
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

        -- Balance temp tables (created even if empty — needed for LEFT JOINs below)
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

        -- SECT_BALANCES (bit 4): populate balances from surviving edges only
        IF (p_result_mask & SECT_BALANCES) != 0 THEN
            -- Token balance: most recent post_balance per node from edges we already have
            INSERT INTO tmp_token_bal (address_id, balance, block_time, guide_id)
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

            INSERT INTO tmp_token_bal (address_id, balance, block_time, guide_id)
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

            -- SOL balance: most recent from surviving edges
            INSERT IGNORE INTO tmp_sol_bal (address_id, sol_balance, block_time, guide_id)
            SELECT sub.address_id, sub.sol_balance, sub.block_time, sub.guide_id FROM (
                SELECT g.from_address_id AS address_id,
                    ROUND(g.from_sol_post_balance / 1e9, 9) AS sol_balance,
                    f.block_time, f.guide_id,
                    ROW_NUMBER() OVER (PARTITION BY g.from_address_id ORDER BY f.block_time DESC) AS rn
                FROM tmp_final_ids f JOIN tx_guide g ON g.id = f.guide_id
                WHERE g.from_sol_post_balance IS NOT NULL
                  AND g.from_address_id IN (SELECT address_id FROM tmp_node_ids)
            ) sub WHERE sub.rn = 1;

            INSERT INTO tmp_sol_bal (address_id, sol_balance, block_time, guide_id)
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

        -- Pre-compute interactions into temp tables
        DROP TEMPORARY TABLE IF EXISTS tmp_ix_pools;
        DROP TEMPORARY TABLE IF EXISTS tmp_ix_dexes;
        CREATE TEMPORARY TABLE tmp_ix_pools (address_id INT UNSIGNED PRIMARY KEY, pools JSON);
        CREATE TEMPORARY TABLE tmp_ix_dexes (address_id INT UNSIGNED PRIMARY KEY, dexes JSON);

        IF (p_result_mask & SECT_INTERACTIONS) != 0 THEN
            -- Pools: find distinct pools each node interacted with via tx_guide
            INSERT INTO tmp_ix_pools
            SELECT address_id, JSON_ARRAYAGG(JSON_OBJECT('address', pool_addr, 'label', pool_lbl)) FROM (
                SELECT DISTINCT n.address_id, pa.address AS pool_addr, COALESCE(pool.pool_label, pa.address) AS pool_lbl
                FROM tmp_node_ids n
                JOIN tx_guide g ON (g.from_address_id = n.address_id OR g.to_address_id = n.address_id)
                    AND g.pool_id IS NOT NULL
                JOIN tmp_limit_txs lt ON lt.tx_id = g.tx_id
                JOIN tx_pool pool ON pool.id = g.pool_id
                JOIN tx_address pa ON pa.id = pool.pool_address_id
            ) distinct_pools GROUP BY address_id;

            -- Dexes: find distinct DEX names each node interacted with
            INSERT INTO tmp_ix_dexes
            SELECT address_id, JSON_ARRAYAGG(dex_name) FROM (
                SELECT DISTINCT n.address_id, pool.dex AS dex_name
                FROM tmp_node_ids n
                JOIN tx_guide g ON (g.from_address_id = n.address_id OR g.to_address_id = n.address_id)
                    AND g.pool_id IS NOT NULL
                JOIN tmp_limit_txs lt ON lt.tx_id = g.tx_id
                JOIN tx_pool pool ON pool.id = g.pool_id AND pool.dex IS NOT NULL
            ) dex_distinct GROUP BY address_id;
        END IF;

        -- Build nodes JSON
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
            LEFT JOIN tx_pool p ON p.pool_address_id = n.address_id  -- pool_label for pool-type nodes
            LEFT JOIN tx_token tk ON tk.mint_address_id = n.address_id
            LEFT JOIN tmp_token_bal tb ON tb.address_id = n.address_id
            LEFT JOIN tmp_sol_bal sb ON sb.address_id = n.address_id
            LEFT JOIN tmp_ix_pools ixp ON ixp.address_id = n.address_id
            LEFT JOIN tmp_ix_dexes ixd ON ixd.address_id = n.address_id
        ) nodes;

        DROP TEMPORARY TABLE IF EXISTS tmp_ix_pools;
        DROP TEMPORARY TABLE IF EXISTS tmp_ix_dexes;

        DROP TEMPORARY TABLE IF EXISTS tmp_node_ids;
        DROP TEMPORARY TABLE IF EXISTS tmp_token_bal;
        DROP TEMPORARY TABLE IF EXISTS tmp_sol_bal;
    END IF;

    -- B3: SECT_TRANSACTIONS (bit 32) — flat wallet rows (v3.1 compat)
    IF (p_result_mask & SECT_TRANSACTIONS) != 0 AND v_wallet_mode = 1 THEN
        SELECT COALESCE(JSON_ARRAYAGG(JSON_OBJECT(
            'signature', t.signature, 'mint_address', ma.address,
            'block_time', t.block_time,
            'block_time_utc', DATE_FORMAT(FROM_UNIXTIME(t.block_time), '%Y-%m-%dT%H:%i:%s'),
            'type_code', gt.type_code, 'token_symbol', tk.token_symbol,
            'direction', f.direction,
            'amount', fn_format_decimal(g.amount, COALESCE(g.decimals, tk.decimals, 9)),
            'party', CASE f.direction WHEN 'out' THEN ca.address WHEN 'in' THEN fa.address
                ELSE COALESCE(fa.address, ca.address) END,
            'party_label', CASE
                WHEN f.direction='out' AND g.to_address_id=t.signer_address_id THEN 'Signer'
                WHEN f.direction='in' AND g.from_address_id=t.signer_address_id THEN 'Signer'
                WHEN f.direction='swap' AND COALESCE(g.from_address_id,g.to_address_id)=t.signer_address_id THEN 'Signer'
                WHEN f.direction='out' THEN COALESCE(ca.label, ca.address_type)
                WHEN f.direction='in' THEN COALESCE(fa.label, fa.address_type)
                ELSE COALESCE(fa.label, fa.address_type, ca.label, ca.address_type) END,
            'counterparty_funded_by', CASE f.direction WHEN 'out' THEN caf.address WHEN 'in' THEN faf.address
                ELSE COALESCE(faf.address, caf.address) END,
            'mint_funded_by', mf.address, 'dex', pool.dex,
            'post_balance', fn_format_decimal(COALESCE(
                CASE f.direction WHEN 'out' THEN g.from_token_post_balance WHEN 'in' THEN g.to_token_post_balance END,
                CASE f.direction WHEN 'out' THEN g.from_sol_post_balance WHEN 'in' THEN g.to_sol_post_balance END
            ), COALESCE(g.decimals, tk.decimals, 9))
        )), CAST('[]' AS JSON)) INTO v_transactions_json
        FROM tmp_final_ids f JOIN tx_guide g ON g.id = f.guide_id
        JOIN tx t ON t.id = g.tx_id JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        JOIN tx_token tk ON tk.id = g.token_id JOIN tx_address ma ON ma.id = tk.mint_address_id
        LEFT JOIN tx_pool pool ON pool.id = g.pool_id
        LEFT JOIN tx_address ca ON ca.id = g.to_address_id LEFT JOIN tx_address fa ON fa.id = g.from_address_id
        LEFT JOIN tx_address mf ON mf.id = ma.funded_by_address_id
        LEFT JOIN tx_address caf ON caf.id = ca.funded_by_address_id
        LEFT JOIN tx_address faf ON faf.id = fa.funded_by_address_id
        ORDER BY t.block_time DESC;
    END IF;

    -- B4: SECT_NAVIGATION (bit 64)
    IF (p_result_mask & SECT_NAVIGATION) != 0 THEN
        IF v_wallet_mode = 0 AND v_tx_id IS NOT NULL AND v_token_id IS NOT NULL THEN
            -- Graph mode: prepared statements for prev/next 5
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

        ELSEIF v_wallet_mode = 1 AND v_anchor_block_time IS NOT NULL THEN
            -- Wallet mode: prev/next from tmp_limit_txs
            SELECT JSON_ARRAYAGG(nav_data) INTO v_prev_json FROM (
                SELECT JSON_OBJECT('signature', t.signature, 'block_time', lt.block_time,
                    'block_time_utc', DATE_FORMAT(FROM_UNIXTIME(lt.block_time), '%Y-%m-%dT%H:%i:%s'),
                    'edge_types', (SELECT JSON_ARRAYAGG(dt.type_code) FROM (
                        SELECT DISTINCT gt.type_code FROM tmp_final_ids2 s
                        JOIN tx_guide g ON g.id = s.guide_id JOIN tx_guide_type gt ON gt.id = g.edge_type_id
                        WHERE s.tx_id = lt.tx_id) dt)
                ) AS nav_data FROM tmp_limit_txs lt JOIN tx t ON t.id = lt.tx_id
                WHERE lt.block_time < v_anchor_block_time ORDER BY lt.block_time DESC LIMIT 5
            ) nav_outer;

            SELECT JSON_ARRAYAGG(nav_data) INTO v_next_json FROM (
                SELECT JSON_OBJECT('signature', t.signature, 'block_time', lt.block_time,
                    'block_time_utc', DATE_FORMAT(FROM_UNIXTIME(lt.block_time), '%Y-%m-%dT%H:%i:%s'),
                    'edge_types', (SELECT JSON_ARRAYAGG(dt.type_code) FROM (
                        SELECT DISTINCT gt.type_code FROM tmp_final_ids2 s
                        JOIN tx_guide g ON g.id = s.guide_id JOIN tx_guide_type gt ON gt.id = g.edge_type_id
                        WHERE s.tx_id = lt.tx_id) dt)
                ) AS nav_data FROM tmp_limit_txs lt JOIN tx t ON t.id = lt.tx_id
                WHERE lt.block_time > v_anchor_block_time ORDER BY lt.block_time ASC LIMIT 5
            ) nav_outer;
        END IF;

        -- Current anchor edge types
        IF v_tx_id IS NOT NULL THEN
            SELECT JSON_ARRAYAGG(dt.type_code) INTO v_current_edge_types FROM (
                SELECT DISTINCT gt.type_code FROM tmp_final_ids2 s
                JOIN tx_guide g ON g.id = s.guide_id JOIN tx_guide_type gt ON gt.id = g.edge_type_id
                WHERE s.tx_id = v_tx_id) dt;
        END IF;
    END IF;

    -- B5: SECT_RELATED (bit 128)
    IF (p_result_mask & SECT_RELATED) != 0 THEN
        IF v_token_id IS NOT NULL THEN
            -- Graph/filtered mode: show OTHER tokens from swap edges (exclude the main token)
            DROP TEMPORARY TABLE IF EXISTS tmp_swap_txs;
            CREATE TEMPORARY TABLE tmp_swap_txs ENGINE=MEMORY AS
            SELECT DISTINCT s.tx_id FROM tmp_final_ids2 s
            JOIN tx_guide g ON g.id = s.guide_id JOIN tx_guide_type gt ON gt.id = g.edge_type_id
            WHERE g.token_id = v_token_id AND gt.type_code IN ('swap_in', 'swap_out');

            SELECT JSON_ARRAYAGG(related_data) INTO v_related_tokens_json FROM (
                SELECT JSON_OBJECT('mint', mint_addr.address,
                    'symbol', COALESCE(tk.token_symbol, 'Unknown'), 'name', tk.token_name,
                    'token_type', tk.token_type, 'swap_count', COUNT(DISTINCT g.tx_id),
                    'total_volume', ROUND(SUM(g.amount / POW(10, COALESCE(g.decimals, 9))), 6)
                ) AS related_data
                FROM tx_guide g JOIN tmp_swap_txs stx ON stx.tx_id = g.tx_id
                JOIN tx_guide_type gt ON gt.id = g.edge_type_id
                JOIN tx_token tk ON tk.id = g.token_id JOIN tx_address mint_addr ON mint_addr.id = tk.mint_address_id
                WHERE g.token_id IS NOT NULL AND g.token_id != v_token_id AND gt.type_code IN ('swap_in','swap_out')
                GROUP BY tk.id, mint_addr.address, tk.token_symbol, tk.token_name, tk.token_type
                ORDER BY COUNT(DISTINCT g.tx_id) DESC, SUM(g.amount) DESC LIMIT 20
            ) related_outer;
            DROP TEMPORARY TABLE IF EXISTS tmp_swap_txs;
        ELSE
            -- Wallet mode, no mint filter: show ALL tokens this wallet interacted with
            SELECT JSON_ARRAYAGG(related_data) INTO v_related_tokens_json FROM (
                SELECT JSON_OBJECT('mint', mint_addr.address,
                    'symbol', COALESCE(tk.token_symbol, 'Unknown'), 'name', tk.token_name,
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
    END IF;

    -- ====================== B5b: BOUNDARY (gap detection) ======================
    -- Look up the most recent boundary record for the current mint/wallet
    IF v_token_id IS NOT NULL THEN
        SELECT JSON_OBJECT(
            'boundary_sig', b.boundary_sig,
            'boundary_block_time', b.boundary_block_time,
            'oldest_fetched_sig', b.oldest_fetched_sig,
            'oldest_fetched_block_time', b.oldest_fetched_block_time,
            'fetched_count', b.fetched_count,
            'end_of_history', IF(b.boundary_sig IS NULL, TRUE, FALSE)
        ) INTO v_boundary_json
        FROM tx_boundary b
        JOIN tx_token tk ON tk.mint_address_id = b.address_id
        WHERE tk.id = v_token_id
        ORDER BY b.id DESC LIMIT 1;
    ELSEIF v_wallet_id IS NOT NULL THEN
        SELECT JSON_OBJECT(
            'boundary_sig', b.boundary_sig,
            'boundary_block_time', b.boundary_block_time,
            'oldest_fetched_sig', b.oldest_fetched_sig,
            'oldest_fetched_block_time', b.oldest_fetched_block_time,
            'fetched_count', b.fetched_count,
            'end_of_history', IF(b.boundary_sig IS NULL, TRUE, FALSE)
        ) INTO v_boundary_json
        FROM tx_boundary b
        WHERE b.address_id = v_wallet_id
        ORDER BY b.id DESC LIMIT 1;
    END IF;

    -- ====================== B6: FINAL JSON ENVELOPE ======================
    SELECT JSON_OBJECT(
        'stage_seq', v_seq,
        'token', IF(v_token_id IS NOT NULL, JSON_OBJECT(
            'mint', v_mint_address, 'symbol', v_token_symbol,
            'token_type', v_token_type, 'decimals', v_decimals), NULL),
        'address', p_address,
        'address_funded_by', IF(v_wallet_mode = 1, (
            SELECT fund.address FROM tx_address wa JOIN tx_address fund ON fund.id = wa.funded_by_address_id
            WHERE wa.id = v_wallet_id), NULL),
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
        'transactions', IF(v_wallet_mode = 1, COALESCE(v_transactions_json, JSON_ARRAY()), NULL),
        'boundary', v_boundary_json
    ) AS result;

    -- B7: Cleanup — only drop remaining MEMORY temp tables
    -- tx_guide_stage rows are NOT cleaned here; background worker handles by created_at
    DROP TEMPORARY TABLE IF EXISTS tmp_final_ids;
    DROP TEMPORARY TABLE IF EXISTS tmp_final_ids2;
    DROP TEMPORARY TABLE IF EXISTS tmp_final_ids3;
    DROP TEMPORARY TABLE IF EXISTS tmp_final_ids4;
    DROP TEMPORARY TABLE IF EXISTS tmp_final_ids5;
    DROP TEMPORARY TABLE IF EXISTS tmp_limit_txs;
    DROP TEMPORARY TABLE IF EXISTS tmp_node_ids;
    DROP TEMPORARY TABLE IF EXISTS tmp_token_bal;
    DROP TEMPORARY TABLE IF EXISTS tmp_sol_bal;
    DROP TEMPORARY TABLE IF EXISTS tmp_swap_txs;

END //
DELIMITER ;
