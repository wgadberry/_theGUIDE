DELIMITER //

DROP PROCEDURE IF EXISTS sp_tx_guide_loader//

CREATE PROCEDURE sp_tx_guide_loader(
    IN p_batch_size INT,
    OUT p_rows_loaded INT,
    OUT p_last_tx_id BIGINT
)
BEGIN
    DECLARE v_start BIGINT;
    DECLARE v_end BIGINT;
    DECLARE v_transfer_count INT DEFAULT 0;
    DECLARE v_swap_count INT DEFAULT 0;
    DECLARE v_other_count INT DEFAULT 0;


    -- Cache guide_type IDs once instead of subquerying per UPDATE
    DECLARE v_gt_spl_transfer INT;
    DECLARE v_gt_swap_in INT;
    DECLARE v_gt_swap_out INT;
    DECLARE v_gt_burn INT;
    DECLARE v_gt_mint INT;
    DECLARE v_gt_close_ata INT;

    DECLARE v_unknown_mint_id BIGINT DEFAULT 751438;
    DECLARE v_sol_token_id BIGINT DEFAULT 25993;
    DECLARE v_create_sink_id INT UNSIGNED DEFAULT 6;

    SET p_batch_size = COALESCE(p_batch_size, 100);
    SET p_rows_loaded = 0;

    -- ============================================================
    -- Cache guide type IDs
    -- ============================================================
    SELECT MAX(CASE WHEN type_code = 'spl_transfer' THEN id END),
           MAX(CASE WHEN type_code = 'swap_in'      THEN id END),
           MAX(CASE WHEN type_code = 'swap_out'     THEN id END),
           MAX(CASE WHEN type_code = 'burn'         THEN id END),
           MAX(CASE WHEN type_code = 'mint'         THEN id END),
           MAX(CASE WHEN type_code = 'close_ata'    THEN id END)
    INTO v_gt_spl_transfer, v_gt_swap_in, v_gt_swap_out,
         v_gt_burn, v_gt_mint, v_gt_close_ata
    FROM tx_guide_type
    WHERE type_code IN ('spl_transfer','swap_in','swap_out','burn','mint','close_ata');

    -- ============================================================
    -- Pull next batch
    -- ============================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_batch;
    CREATE TEMPORARY TABLE tmp_batch (
        tx_id BIGINT PRIMARY KEY,
        block_time BIGINT UNSIGNED,
        fee BIGINT UNSIGNED,
        priority_fee BIGINT UNSIGNED
    ) ENGINE=MEMORY;

    -- Require both decode (bit 4) and detail (bit 16) before loading guide edges.
    INSERT INTO tmp_batch (tx_id, block_time, fee, priority_fee)
    SELECT id, block_time, fee, priority_fee
    FROM tx
    WHERE tx_state & 32 = 0
      AND tx_state & 4 != 0
      AND tx_state & 16 != 0
    ORDER BY id
    LIMIT p_batch_size;

    SELECT MIN(tx_id), MAX(tx_id) + 1, MAX(tx_id)
    INTO v_start, v_end, p_last_tx_id
    FROM tmp_batch;

    IF v_start IS NULL THEN
        SET p_last_tx_id = 0;
        SET p_rows_loaded = 0;
        DROP TEMPORARY TABLE IF EXISTS tmp_batch;
    ELSE

    -- ============================================================
    -- TRANSFERS
    -- ============================================================
    INSERT IGNORE INTO tx_guide (
        tx_id, block_time,
        from_address_id, to_address_id,
        from_token_account_id, to_token_account_id,
        token_id, amount, decimals,
        edge_type_id, source_id, source_row_id, ins_index,
        fee, priority_fee
    )
    SELECT
        b.tx_id, b.block_time,
        CASE WHEN t.transfer_type = 'ACTIVITY_SPL_MINT'
             THEN COALESCE(tk.mint_address_id, v_unknown_mint_id)
             ELSE t.source_owner_address_id END,
        CASE WHEN t.transfer_type = 'ACTIVITY_SPL_BURN'
             THEN COALESCE(tk.mint_address_id, v_unknown_mint_id)
             ELSE t.destination_owner_address_id END,
        t.source_address_id,
        t.destination_address_id,
        t.token_id, t.amount, t.decimals,
        CASE t.transfer_type
            WHEN 'ACTIVITY_SPL_TRANSFER'     THEN v_gt_spl_transfer
            WHEN 'ACTIVITY_SPL_BURN'         THEN v_gt_burn
            WHEN 'ACTIVITY_SPL_MINT'         THEN v_gt_mint
            WHEN 'ACTIVITY_SPL_CLOSE_ACCOUNT' THEN v_gt_close_ata
        END,
        1, t.id, t.ins_index,
        b.fee, b.priority_fee
    FROM tmp_batch b
    JOIN tx_transfer t ON t.tx_id = b.tx_id
    LEFT JOIN tx_token tk ON tk.id = t.token_id
    WHERE t.transfer_type IN (
        'ACTIVITY_SPL_TRANSFER','ACTIVITY_SPL_BURN',
        'ACTIVITY_SPL_MINT','ACTIVITY_SPL_CLOSE_ACCOUNT'
    )
    AND (t.source_owner_address_id IS NOT NULL OR t.transfer_type = 'ACTIVITY_SPL_MINT')
    AND (t.destination_owner_address_id IS NOT NULL OR t.transfer_type = 'ACTIVITY_SPL_BURN');

    SET v_transfer_count = ROW_COUNT();

    -- ============================================================
    -- SWAP ENRICHMENT (swap_in + swap_out in one pass)
    -- ============================================================
    -- Build a temp of all swap matches to avoid two separate UPDATE scans
    DROP TEMPORARY TABLE IF EXISTS tmp_swap_match;
    CREATE TEMPORARY TABLE tmp_swap_match (
        guide_id BIGINT PRIMARY KEY,
        pool_id BIGINT UNSIGNED,
        swap_direction CHAR(3),
        edge_type_id INT
    ) ENGINE=MEMORY;

    -- swap_in (token_1)
    INSERT IGNORE INTO tmp_swap_match
    SELECT g.id, s.amm_id, 'in', v_gt_swap_in
    FROM tx_guide g
    JOIN tx_swap s ON s.tx_id = g.tx_id
      AND s.token_1_id = g.token_id AND s.amount_1 = g.amount
    WHERE g.tx_id >= v_start AND g.tx_id < v_end
      AND g.source_id = 1
      AND g.edge_type_id = v_gt_spl_transfer;

    -- swap_out (token_2)
    INSERT IGNORE INTO tmp_swap_match
    SELECT g.id, s.amm_id, 'out', v_gt_swap_out
    FROM tx_guide g
    JOIN tx_swap s ON s.tx_id = g.tx_id
      AND s.token_2_id = g.token_id AND s.amount_2 = g.amount
    WHERE g.tx_id >= v_start AND g.tx_id < v_end
      AND g.source_id = 1
      AND g.edge_type_id = v_gt_spl_transfer;

    -- Single UPDATE from temp (IGNORE: edge_type_id is in uq_guide_edge)
    UPDATE IGNORE tx_guide g
    JOIN tmp_swap_match sm ON sm.guide_id = g.id
    SET g.pool_id          = sm.pool_id,
        g.swap_direction   = sm.swap_direction,
        g.edge_type_id     = sm.edge_type_id;

    SET v_swap_count = ROW_COUNT();
    DROP TEMPORARY TABLE tmp_swap_match;

    -- ============================================================
    -- POOL ENRICHMENT (combine both directions into one pass)
    -- ============================================================
    UPDATE tx_guide g
    LEFT JOIN tx_pool pf ON pf.pool_address_id = g.from_address_id
    LEFT JOIN tx_pool pt ON pt.pool_address_id = g.to_address_id
    SET g.pool_id = COALESCE(pf.id, pt.id)
    WHERE g.tx_id >= v_start AND g.tx_id < v_end
      AND g.pool_id IS NULL
      AND (pf.id IS NOT NULL OR pt.id IS NOT NULL);

    -- ============================================================
    -- OTHER ACTIVITY EDGES
    -- ============================================================
    INSERT IGNORE INTO tx_guide (
        tx_id, block_time, from_address_id, to_address_id,
        token_id, edge_type_id, source_id, source_row_id, ins_index,
        fee, priority_fee
    )
    SELECT a.tx_id, b.block_time,
           a.account_address_id, a.account_address_id,
           NULL, atm.guide_type_id,
           5, a.id, a.ins_index,
           b.fee, b.priority_fee
    FROM tmp_batch b
    JOIN tx_activity a ON a.tx_id = b.tx_id
    JOIN tx_activity_type_map atm ON atm.activity_type = a.activity_type
    LEFT JOIN tx_swap s ON s.activity_id = a.id
    LEFT JOIN tx_transfer t ON t.activity_id = a.id
    WHERE atm.creates_edge = 1
      AND atm.guide_type_id IS NOT NULL
      AND s.id IS NULL AND t.id IS NULL
      AND a.account_address_id IS NOT NULL
      AND a.activity_type NOT LIKE 'ACTIVITY_%SWAP'
      AND a.activity_type NOT LIKE '%TRANSFER%';

    SET v_other_count = ROW_COUNT();

    -- ============================================================
    -- BALANCE POPULATION (replaces the entire chunk loop)
    --
    -- Strategy: materialize balance lookups into temp tables using
    -- window functions, then apply with simple JOINs.
    -- No more correlated subqueries. No more chunk loop.
    -- ============================================================

    -- ----------------------------------------------------------
    -- Collect the guide rows that need balances
    -- ----------------------------------------------------------
    DROP TEMPORARY TABLE IF EXISTS tmp_guide_keys;
    CREATE TEMPORARY TABLE tmp_guide_keys (
        id          BIGINT PRIMARY KEY,
        tx_id       BIGINT,
        block_time  BIGINT UNSIGNED,
        from_address_id      INT UNSIGNED,
        to_address_id        INT UNSIGNED,
        from_token_account_id INT UNSIGNED,
        to_token_account_id  INT UNSIGNED,
        token_id             BIGINT,
        from_token_filled    TINYINT NOT NULL DEFAULT 0,
        to_token_filled      TINYINT NOT NULL DEFAULT 0,
        from_sol_filled      TINYINT NOT NULL DEFAULT 0,
        to_sol_filled        TINYINT NOT NULL DEFAULT 0,
        INDEX idx_txid (tx_id),
        INDEX idx_from_ta (from_token_account_id, token_id),
        INDEX idx_to_ta   (to_token_account_id, token_id),
        INDEX idx_from_addr (from_address_id, token_id),
        INDEX idx_to_addr   (to_address_id, token_id)
    ) ENGINE=MEMORY;

    INSERT INTO tmp_guide_keys (id, tx_id, block_time, from_address_id, to_address_id,
           from_token_account_id, to_token_account_id, token_id,
           from_token_filled, to_token_filled, from_sol_filled, to_sol_filled)
    SELECT id, tx_id, block_time,
           from_address_id, to_address_id,
           from_token_account_id, to_token_account_id,
           token_id,
           (from_token_pre_balance IS NOT NULL AND from_token_post_balance IS NOT NULL),
           (to_token_pre_balance IS NOT NULL AND to_token_post_balance IS NOT NULL),
           (from_sol_post_balance IS NOT NULL),
           (to_sol_post_balance IS NOT NULL)
    FROM tx_guide
    WHERE tx_id >= v_start AND tx_id < v_end;

    -- ----------------------------------------------------------
    -- TOKEN BALANCE: same-tx matches (pre + post in one shot)
    -- Priority: token_account_address_id match first, owner match second
    -- ----------------------------------------------------------

    -- === FROM token balance (same tx) ===
    DROP TEMPORARY TABLE IF EXISTS tmp_from_token_same;
    CREATE TEMPORARY TABLE tmp_from_token_same (
        guide_id BIGINT PRIMARY KEY,
        pre_balance DECIMAL(38,0),
        post_balance DECIMAL(38,0)
    ) ENGINE=MEMORY;

    INSERT INTO tmp_from_token_same
    SELECT sub.guide_id, sub.pre_balance, sub.post_balance
    FROM (
        SELECT g.id AS guide_id,
               tbc.pre_balance, tbc.post_balance,
               ROW_NUMBER() OVER (
                   PARTITION BY g.id
                   ORDER BY CASE WHEN tbc.token_account_address_id = g.from_token_account_id
                                 THEN 0 ELSE 1 END,
                            tbc.id
               ) AS rn
        FROM tmp_guide_keys g
        JOIN tx_token_balance_change tbc
             ON tbc.tx_id = g.tx_id AND tbc.token_id = g.token_id
        WHERE g.token_id IS NOT NULL
          AND (tbc.token_account_address_id = g.from_token_account_id
               OR tbc.owner_address_id = g.from_address_id)
    ) sub WHERE sub.rn = 1;

    -- === TO token balance (same tx) ===
    DROP TEMPORARY TABLE IF EXISTS tmp_to_token_same;
    CREATE TEMPORARY TABLE tmp_to_token_same (
        guide_id BIGINT PRIMARY KEY,
        pre_balance DECIMAL(38,0),
        post_balance DECIMAL(38,0)
    ) ENGINE=MEMORY;

    INSERT INTO tmp_to_token_same
    SELECT sub.guide_id, sub.pre_balance, sub.post_balance
    FROM (
        SELECT g.id AS guide_id,
               tbc.pre_balance, tbc.post_balance,
               ROW_NUMBER() OVER (
                   PARTITION BY g.id
                   ORDER BY CASE WHEN tbc.token_account_address_id = g.to_token_account_id
                                 THEN 0 ELSE 1 END,
                            tbc.id
               ) AS rn
        FROM tmp_guide_keys g
        JOIN tx_token_balance_change tbc
             ON tbc.tx_id = g.tx_id AND tbc.token_id = g.token_id
        WHERE g.token_id IS NOT NULL
          AND (tbc.token_account_address_id = g.to_token_account_id
               OR tbc.owner_address_id = g.to_address_id)
    ) sub WHERE sub.rn = 1;

    -- Apply same-tx token balances
    UPDATE tx_guide g
    JOIN tmp_from_token_same f ON f.guide_id = g.id
    SET g.from_token_pre_balance  = COALESCE(g.from_token_pre_balance, f.pre_balance),
        g.from_token_post_balance = COALESCE(g.from_token_post_balance, f.post_balance)
    WHERE g.token_id IS NOT NULL;

    -- Mark filled in temp table so LATERAL pass skips these
    UPDATE tmp_guide_keys g
    JOIN tmp_from_token_same f ON f.guide_id = g.id
    SET g.from_token_filled = 1;

    UPDATE tx_guide g
    JOIN tmp_to_token_same t ON t.guide_id = g.id
    SET g.to_token_pre_balance  = COALESCE(g.to_token_pre_balance, t.pre_balance),
        g.to_token_post_balance = COALESCE(g.to_token_post_balance, t.post_balance)
    WHERE g.token_id IS NOT NULL;

    UPDATE tmp_guide_keys g
    JOIN tmp_to_token_same t ON t.guide_id = g.id
    SET g.to_token_filled = 1;

    DROP TEMPORARY TABLE tmp_from_token_same;
    DROP TEMPORARY TABLE tmp_to_token_same;

    -- ----------------------------------------------------------
    -- TOKEN BALANCE: prior-tx fallback (only rows still NULL)
    -- Dedup: one LATERAL seek per unique (account/owner, token, block_time)
    -- instead of per guide row — typically 3-5x fewer I/O seeks
    --
    -- READ COMMITTED: avoid gap locks on balance change indexes
    -- that deadlock with the shredder's concurrent INSERTs
    -- ----------------------------------------------------------
    SET @saved_isolation = @@SESSION.transaction_isolation;
    SET SESSION transaction_isolation = 'READ-COMMITTED';

    -- === FROM token prior ===
    DROP TEMPORARY TABLE IF EXISTS tmp_from_token_prior;
    CREATE TEMPORARY TABLE tmp_from_token_prior (
        guide_id BIGINT PRIMARY KEY,
        post_balance DECIMAL(38,0)
    ) ENGINE=MEMORY;

    -- Pass 1: match by token_account (uses idx_tbc_acct_token_blocktime)
    -- Dedup: one lookup per unique (token_account, token_id, block_time)
    DROP TEMPORARY TABLE IF EXISTS tmp_dedup_from_tok_acct;
    CREATE TEMPORARY TABLE tmp_dedup_from_tok_acct (
        token_account_id INT UNSIGNED,
        token_id BIGINT,
        block_time BIGINT UNSIGNED,
        post_balance DECIMAL(38,0),
        PRIMARY KEY (token_account_id, token_id, block_time)
    ) ENGINE=MEMORY;

    INSERT IGNORE INTO tmp_dedup_from_tok_acct (token_account_id, token_id, block_time)
    SELECT from_token_account_id, token_id, block_time
    FROM tmp_guide_keys
    WHERE token_id IS NOT NULL AND from_token_account_id IS NOT NULL AND from_token_filled = 0;

    INSERT INTO tmp_from_token_prior
    SELECT g.id, d.post_balance
    FROM tmp_guide_keys g
    JOIN (
        SELECT u.token_account_id, u.token_id, u.block_time, lat.post_balance
        FROM tmp_dedup_from_tok_acct u
        CROSS JOIN LATERAL (
            SELECT tbc.post_balance
            FROM tx_token_balance_change tbc
            WHERE tbc.token_account_address_id = u.token_account_id
              AND tbc.token_id = u.token_id
              AND tbc.block_time < u.block_time
            ORDER BY tbc.block_time DESC
            LIMIT 1
        ) lat
    ) d ON d.token_account_id = g.from_token_account_id
        AND d.token_id = g.token_id AND d.block_time = g.block_time
    WHERE g.token_id IS NOT NULL AND g.from_token_account_id IS NOT NULL AND g.from_token_filled = 0;

    DROP TEMPORARY TABLE tmp_dedup_from_tok_acct;

    -- Pass 2: fallback by owner (uses idx_tbc_owner_token_blocktime)
    -- Dedup: one lookup per unique (owner, token_id, block_time)
    DROP TEMPORARY TABLE IF EXISTS tmp_dedup_from_tok_own;
    CREATE TEMPORARY TABLE tmp_dedup_from_tok_own (
        owner_id INT UNSIGNED,
        token_id BIGINT,
        block_time BIGINT UNSIGNED,
        post_balance DECIMAL(38,0),
        PRIMARY KEY (owner_id, token_id, block_time)
    ) ENGINE=MEMORY;

    INSERT IGNORE INTO tmp_dedup_from_tok_own (owner_id, token_id, block_time)
    SELECT from_address_id, token_id, block_time
    FROM tmp_guide_keys
    WHERE token_id IS NOT NULL AND from_token_filled = 0;

    INSERT IGNORE INTO tmp_from_token_prior
    SELECT g.id, d.post_balance
    FROM tmp_guide_keys g
    JOIN (
        SELECT u.owner_id, u.token_id, u.block_time, lat.post_balance
        FROM tmp_dedup_from_tok_own u
        CROSS JOIN LATERAL (
            SELECT tbc.post_balance
            FROM tx_token_balance_change tbc
            WHERE tbc.owner_address_id = u.owner_id
              AND tbc.token_id = u.token_id
              AND tbc.block_time < u.block_time
            ORDER BY tbc.block_time DESC
            LIMIT 1
        ) lat
    ) d ON d.owner_id = g.from_address_id
        AND d.token_id = g.token_id AND d.block_time = g.block_time
    WHERE g.token_id IS NOT NULL AND g.from_token_filled = 0;

    DROP TEMPORARY TABLE tmp_dedup_from_tok_own;

    UPDATE tx_guide g
    JOIN tmp_from_token_prior fp ON fp.guide_id = g.id
    SET g.from_token_pre_balance  = COALESCE(g.from_token_pre_balance, fp.post_balance),
        g.from_token_post_balance = COALESCE(g.from_token_post_balance, fp.post_balance);

    DROP TEMPORARY TABLE tmp_from_token_prior;

    -- === TO token prior ===
    DROP TEMPORARY TABLE IF EXISTS tmp_to_token_prior;
    CREATE TEMPORARY TABLE tmp_to_token_prior (
        guide_id BIGINT PRIMARY KEY,
        post_balance DECIMAL(38,0)
    ) ENGINE=MEMORY;

    -- Pass 1: match by token_account (uses idx_tbc_acct_token_blocktime)
    DROP TEMPORARY TABLE IF EXISTS tmp_dedup_to_tok_acct;
    CREATE TEMPORARY TABLE tmp_dedup_to_tok_acct (
        token_account_id INT UNSIGNED,
        token_id BIGINT,
        block_time BIGINT UNSIGNED,
        post_balance DECIMAL(38,0),
        PRIMARY KEY (token_account_id, token_id, block_time)
    ) ENGINE=MEMORY;

    INSERT IGNORE INTO tmp_dedup_to_tok_acct (token_account_id, token_id, block_time)
    SELECT to_token_account_id, token_id, block_time
    FROM tmp_guide_keys
    WHERE token_id IS NOT NULL AND to_token_account_id IS NOT NULL AND to_token_filled = 0;

    INSERT INTO tmp_to_token_prior
    SELECT g.id, d.post_balance
    FROM tmp_guide_keys g
    JOIN (
        SELECT u.token_account_id, u.token_id, u.block_time, lat.post_balance
        FROM tmp_dedup_to_tok_acct u
        CROSS JOIN LATERAL (
            SELECT tbc.post_balance
            FROM tx_token_balance_change tbc
            WHERE tbc.token_account_address_id = u.token_account_id
              AND tbc.token_id = u.token_id
              AND tbc.block_time < u.block_time
            ORDER BY tbc.block_time DESC
            LIMIT 1
        ) lat
    ) d ON d.token_account_id = g.to_token_account_id
        AND d.token_id = g.token_id AND d.block_time = g.block_time
    WHERE g.token_id IS NOT NULL AND g.to_token_account_id IS NOT NULL AND g.to_token_filled = 0;

    DROP TEMPORARY TABLE tmp_dedup_to_tok_acct;

    -- Pass 2: fallback by owner (uses idx_tbc_owner_token_blocktime)
    DROP TEMPORARY TABLE IF EXISTS tmp_dedup_to_tok_own;
    CREATE TEMPORARY TABLE tmp_dedup_to_tok_own (
        owner_id INT UNSIGNED,
        token_id BIGINT,
        block_time BIGINT UNSIGNED,
        post_balance DECIMAL(38,0),
        PRIMARY KEY (owner_id, token_id, block_time)
    ) ENGINE=MEMORY;

    INSERT IGNORE INTO tmp_dedup_to_tok_own (owner_id, token_id, block_time)
    SELECT to_address_id, token_id, block_time
    FROM tmp_guide_keys
    WHERE token_id IS NOT NULL AND to_token_filled = 0;

    INSERT IGNORE INTO tmp_to_token_prior
    SELECT g.id, d.post_balance
    FROM tmp_guide_keys g
    JOIN (
        SELECT u.owner_id, u.token_id, u.block_time, lat.post_balance
        FROM tmp_dedup_to_tok_own u
        CROSS JOIN LATERAL (
            SELECT tbc.post_balance
            FROM tx_token_balance_change tbc
            WHERE tbc.owner_address_id = u.owner_id
              AND tbc.token_id = u.token_id
              AND tbc.block_time < u.block_time
            ORDER BY tbc.block_time DESC
            LIMIT 1
        ) lat
    ) d ON d.owner_id = g.to_address_id
        AND d.token_id = g.token_id AND d.block_time = g.block_time
    WHERE g.token_id IS NOT NULL AND g.to_token_filled = 0;

    DROP TEMPORARY TABLE tmp_dedup_to_tok_own;

    UPDATE tx_guide g
    JOIN tmp_to_token_prior tp ON tp.guide_id = g.id
    SET g.to_token_pre_balance  = COALESCE(g.to_token_pre_balance, tp.post_balance),
        g.to_token_post_balance = COALESCE(g.to_token_post_balance, tp.post_balance);

    DROP TEMPORARY TABLE tmp_to_token_prior;

    -- ----------------------------------------------------------
    -- SOL BALANCE: same-tx matches
    -- ----------------------------------------------------------

    -- === FROM sol (same tx) ===
    DROP TEMPORARY TABLE IF EXISTS tmp_from_sol_same;
    CREATE TEMPORARY TABLE tmp_from_sol_same (
        guide_id BIGINT PRIMARY KEY,
        pre_balance DECIMAL(38,0),
        post_balance DECIMAL(38,0)
    ) ENGINE=MEMORY;

    INSERT INTO tmp_from_sol_same
    SELECT sub.guide_id, sub.pre_balance, sub.post_balance
    FROM (
        SELECT g.id AS guide_id, sbc.pre_balance, sbc.post_balance,
               ROW_NUMBER() OVER (PARTITION BY g.id ORDER BY sbc.id) AS rn
        FROM tmp_guide_keys g
        JOIN tx_sol_balance_change sbc
             ON sbc.tx_id = g.tx_id AND sbc.address_id = g.from_address_id
    ) sub WHERE sub.rn = 1;

    -- === TO sol (same tx) ===
    DROP TEMPORARY TABLE IF EXISTS tmp_to_sol_same;
    CREATE TEMPORARY TABLE tmp_to_sol_same (
        guide_id BIGINT PRIMARY KEY,
        pre_balance DECIMAL(38,0),
        post_balance DECIMAL(38,0)
    ) ENGINE=MEMORY;

    INSERT INTO tmp_to_sol_same
    SELECT sub.guide_id, sub.pre_balance, sub.post_balance
    FROM (
        SELECT g.id AS guide_id, sbc.pre_balance, sbc.post_balance,
               ROW_NUMBER() OVER (PARTITION BY g.id ORDER BY sbc.id) AS rn
        FROM tmp_guide_keys g
        JOIN tx_sol_balance_change sbc
             ON sbc.tx_id = g.tx_id AND sbc.address_id = g.to_address_id
    ) sub WHERE sub.rn = 1;

    -- Apply same-tx SOL balances
    UPDATE tx_guide g
    JOIN tmp_from_sol_same f ON f.guide_id = g.id
    SET g.from_sol_pre_balance  = COALESCE(g.from_sol_pre_balance, f.pre_balance),
        g.from_sol_post_balance = COALESCE(g.from_sol_post_balance, f.post_balance);

    UPDATE tmp_guide_keys g
    JOIN tmp_from_sol_same f ON f.guide_id = g.id
    SET g.from_sol_filled = 1;

    UPDATE tx_guide g
    JOIN tmp_to_sol_same t ON t.guide_id = g.id
    SET g.to_sol_pre_balance  = COALESCE(g.to_sol_pre_balance, t.pre_balance),
        g.to_sol_post_balance = COALESCE(g.to_sol_post_balance, t.post_balance);

    UPDATE tmp_guide_keys g
    JOIN tmp_to_sol_same t ON t.guide_id = g.id
    SET g.to_sol_filled = 1;

    DROP TEMPORARY TABLE tmp_from_sol_same;
    DROP TEMPORARY TABLE tmp_to_sol_same;

    -- ----------------------------------------------------------
    -- SOL BALANCE: prior-tx fallback
    -- Dedup: one LATERAL seek per unique (address_id, block_time)
    -- ----------------------------------------------------------

    -- === FROM sol prior ===
    DROP TEMPORARY TABLE IF EXISTS tmp_from_sol_prior;
    CREATE TEMPORARY TABLE tmp_from_sol_prior (
        guide_id BIGINT PRIMARY KEY,
        post_balance DECIMAL(38,0)
    ) ENGINE=MEMORY;

    DROP TEMPORARY TABLE IF EXISTS tmp_dedup_from_sol;
    CREATE TEMPORARY TABLE tmp_dedup_from_sol (
        address_id INT UNSIGNED,
        block_time BIGINT UNSIGNED,
        post_balance DECIMAL(38,0),
        PRIMARY KEY (address_id, block_time)
    ) ENGINE=MEMORY;

    INSERT IGNORE INTO tmp_dedup_from_sol (address_id, block_time)
    SELECT from_address_id, block_time FROM tmp_guide_keys WHERE from_sol_filled = 0;

    INSERT INTO tmp_from_sol_prior
    SELECT g.id, d.post_balance
    FROM tmp_guide_keys g
    JOIN (
        SELECT u.address_id, u.block_time, lat.post_balance
        FROM tmp_dedup_from_sol u
        CROSS JOIN LATERAL (
            SELECT sbc.post_balance
            FROM tx_sol_balance_change sbc
            WHERE sbc.address_id = u.address_id
              AND sbc.block_time < u.block_time
            ORDER BY sbc.block_time DESC
            LIMIT 1
        ) lat
    ) d ON d.address_id = g.from_address_id AND d.block_time = g.block_time
    WHERE g.from_sol_filled = 0;

    DROP TEMPORARY TABLE tmp_dedup_from_sol;

    UPDATE tx_guide g
    JOIN tmp_from_sol_prior fp ON fp.guide_id = g.id
    SET g.from_sol_pre_balance  = COALESCE(g.from_sol_pre_balance, fp.post_balance),
        g.from_sol_post_balance = COALESCE(g.from_sol_post_balance, fp.post_balance);

    DROP TEMPORARY TABLE tmp_from_sol_prior;

    -- === TO sol prior ===
    DROP TEMPORARY TABLE IF EXISTS tmp_to_sol_prior;
    CREATE TEMPORARY TABLE tmp_to_sol_prior (
        guide_id BIGINT PRIMARY KEY,
        post_balance DECIMAL(38,0)
    ) ENGINE=MEMORY;

    DROP TEMPORARY TABLE IF EXISTS tmp_dedup_to_sol;
    CREATE TEMPORARY TABLE tmp_dedup_to_sol (
        address_id INT UNSIGNED,
        block_time BIGINT UNSIGNED,
        post_balance DECIMAL(38,0),
        PRIMARY KEY (address_id, block_time)
    ) ENGINE=MEMORY;

    INSERT IGNORE INTO tmp_dedup_to_sol (address_id, block_time)
    SELECT to_address_id, block_time FROM tmp_guide_keys WHERE to_sol_filled = 0;

    INSERT INTO tmp_to_sol_prior
    SELECT g.id, d.post_balance
    FROM tmp_guide_keys g
    JOIN (
        SELECT u.address_id, u.block_time, lat.post_balance
        FROM tmp_dedup_to_sol u
        CROSS JOIN LATERAL (
            SELECT sbc.post_balance
            FROM tx_sol_balance_change sbc
            WHERE sbc.address_id = u.address_id
              AND sbc.block_time < u.block_time
            ORDER BY sbc.block_time DESC
            LIMIT 1
        ) lat
    ) d ON d.address_id = g.to_address_id AND d.block_time = g.block_time
    WHERE g.to_sol_filled = 0;

    DROP TEMPORARY TABLE tmp_dedup_to_sol;

    UPDATE tx_guide g
    JOIN tmp_to_sol_prior tp ON tp.guide_id = g.id
    SET g.to_sol_pre_balance  = COALESCE(g.to_sol_pre_balance, tp.post_balance),
        g.to_sol_post_balance = COALESCE(g.to_sol_post_balance, tp.post_balance);

    DROP TEMPORARY TABLE tmp_to_sol_prior;

    -- Restore default isolation level
    SET SESSION transaction_isolation = @saved_isolation;

    -- ----------------------------------------------------------
    -- Default mint addresses to 0
    -- ----------------------------------------------------------
    UPDATE tx_guide g
    JOIN tx_address a ON a.id = g.from_address_id
    SET g.from_token_pre_balance  = COALESCE(g.from_token_pre_balance, 0),
        g.from_token_post_balance = COALESCE(g.from_token_post_balance, 0),
        g.from_sol_pre_balance    = COALESCE(g.from_sol_pre_balance, 0),
        g.from_sol_post_balance   = COALESCE(g.from_sol_post_balance, 0)
    WHERE g.tx_id >= v_start AND g.tx_id < v_end
      AND a.address_type = 'mint'
      AND (g.from_token_pre_balance IS NULL OR g.from_token_post_balance IS NULL
           OR g.from_sol_pre_balance IS NULL OR g.from_sol_post_balance IS NULL);

    UPDATE tx_guide g
    JOIN tx_address a ON a.id = g.to_address_id
    SET g.to_token_pre_balance  = COALESCE(g.to_token_pre_balance, 0),
        g.to_token_post_balance = COALESCE(g.to_token_post_balance, 0),
        g.to_sol_pre_balance    = COALESCE(g.to_sol_pre_balance, 0),
        g.to_sol_post_balance   = COALESCE(g.to_sol_post_balance, 0)
    WHERE g.tx_id >= v_start AND g.tx_id < v_end
      AND a.address_type = 'mint'
      AND (g.to_token_pre_balance IS NULL OR g.to_token_post_balance IS NULL
           OR g.to_sol_pre_balance IS NULL OR g.to_sol_post_balance IS NULL);

    -- Default remaining NULLs for pre-balances to 0
    UPDATE tx_guide
    SET from_token_pre_balance = COALESCE(from_token_pre_balance, 0),
        to_token_pre_balance   = COALESCE(to_token_pre_balance, 0),
        from_sol_pre_balance   = COALESCE(from_sol_pre_balance, 0),
        to_sol_pre_balance     = COALESCE(to_sol_pre_balance, 0)
    WHERE tx_id >= v_start AND tx_id < v_end
      AND (from_token_pre_balance IS NULL OR to_token_pre_balance IS NULL
           OR from_sol_pre_balance IS NULL OR to_sol_pre_balance IS NULL);

    DROP TEMPORARY TABLE tmp_guide_keys;

    -- ============================================================
    -- Token tax detection
    -- ============================================================
    UPDATE tx_guide g
    SET
        g.tax_amount = CAST(g.amount AS DECIMAL(38,0))
                     - (g.to_token_post_balance - g.to_token_pre_balance),
        g.tax_bps = ROUND(
            (CAST(g.amount AS DECIMAL(38,0))
             - (g.to_token_post_balance - g.to_token_pre_balance))
            / CAST(g.amount AS DECIMAL(38,10)) * 10000
        )
    WHERE g.tx_id >= v_start AND g.tx_id < v_end
      AND g.token_id IS NOT NULL AND g.amount > 0
      AND g.to_token_pre_balance IS NOT NULL
      AND g.to_token_post_balance IS NOT NULL
      AND g.to_token_post_balance > g.to_token_pre_balance
      AND CAST(g.amount AS DECIMAL(38,0)) >
          (g.to_token_post_balance - g.to_token_pre_balance) * 1.005
      AND ROUND(
          (CAST(g.amount AS DECIMAL(38,0))
           - (g.to_token_post_balance - g.to_token_pre_balance))
          / CAST(g.amount AS DECIMAL(38,10)) * 10000
      ) BETWEEN 100 AND 4999;

    -- ============================================================
    -- Stamp tx_state bit 32
    -- ============================================================
    UPDATE tx t
    JOIN tmp_batch b ON b.tx_id = t.id
    SET t.tx_state = t.tx_state | 32;

    -- Stamp in_guide on any tokens that appeared in this batch.
    -- Also set attempt_cnt = 3 on already-enriched tokens so the enricher
    -- claim query (attempt_cnt < 3) skips them immediately.
    UPDATE tx_token tk
    JOIN (
        SELECT DISTINCT g.token_id FROM tx_guide g
        JOIN tmp_batch b ON b.tx_id = g.tx_id
        WHERE g.token_id IS NOT NULL
    ) batch_tokens ON batch_tokens.token_id = tk.id
    SET tk.in_guide = 1,
        tk.attempt_cnt = CASE
            WHEN tk.token_symbol IS NOT NULL AND tk.token_symbol != ''
             AND tk.token_name IS NOT NULL AND tk.token_name != ''
             AND tk.decimals IS NOT NULL
             AND NOT (tk.decimals = 0 AND tk.token_type IS NULL)
            THEN 3
            ELSE tk.attempt_cnt
        END
    WHERE tk.in_guide = 0;

    SET p_rows_loaded = v_transfer_count + v_swap_count + v_other_count;
    DROP TEMPORARY TABLE IF EXISTS tmp_batch;

    END IF;
END //

DELIMITER ;