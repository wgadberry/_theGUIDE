DROP PROCEDURE IF EXISTS sp_tx_guide_balance_backfill;

DELIMITER $$

CREATE PROCEDURE sp_tx_guide_balance_backfill(
    IN p_batch_size INT UNSIGNED
)
BEGIN
    -- ============================================================
    -- sp_tx_guide_balance_backfill
    --
    -- Re-runs balance population logic from sp_tx_guide_loader for
    -- tx_guide rows that have NULL from_token_post_balance or
    -- to_token_post_balance but where matching tbc data now exists.
    --
    -- Parameter:
    --   p_batch_size  — max guide rows to process per call (default 1000)
    -- ============================================================

    DECLARE v_batch_size INT UNSIGNED;
    DECLARE v_rows_updated BIGINT DEFAULT 0;
    DECLARE v_gt_close_ata INT;

    SET v_batch_size = COALESCE(p_batch_size, 1000);

    -- Look up close_ata edge type id
    SELECT id INTO v_gt_close_ata
    FROM tx_guide_type
    WHERE type_code = 'close_ata'
    LIMIT 1;

    -- ----------------------------------------------------------
    -- Collect the guide rows that need balance backfill
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
        edge_type_id         INT UNSIGNED,
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
           from_token_account_id, to_token_account_id, token_id, edge_type_id,
           from_token_filled, to_token_filled, from_sol_filled, to_sol_filled)
    SELECT id, tx_id, block_time,
           from_address_id, to_address_id,
           from_token_account_id, to_token_account_id,
           token_id, edge_type_id,
           (from_token_pre_balance IS NOT NULL AND from_token_post_balance IS NOT NULL),
           (to_token_pre_balance IS NOT NULL AND to_token_post_balance IS NOT NULL),
           (from_sol_post_balance IS NOT NULL),
           (to_sol_post_balance IS NOT NULL)
    FROM tx_guide
    WHERE (from_token_post_balance IS NULL OR to_token_post_balance IS NULL)
      AND token_id IS NOT NULL
    ORDER BY id
    LIMIT v_batch_size;

    -- ----------------------------------------------------------
    -- Phase 1: TOKEN BALANCE — same-tx matches (pre + post)
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
          AND g.from_token_filled = 0
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
          AND g.to_token_filled = 0
          AND (tbc.token_account_address_id = g.to_token_account_id
               OR tbc.owner_address_id = g.to_address_id)
    ) sub WHERE sub.rn = 1;

    -- Apply same-tx token balances
    UPDATE tx_guide g
    JOIN tmp_from_token_same f ON f.guide_id = g.id
    SET g.from_token_pre_balance  = COALESCE(g.from_token_pre_balance, f.pre_balance),
        g.from_token_post_balance = COALESCE(g.from_token_post_balance, f.post_balance)
    WHERE g.token_id IS NOT NULL;

    SET v_rows_updated = v_rows_updated + ROW_COUNT();

    -- Mark filled in temp table so LATERAL pass skips these
    UPDATE tmp_guide_keys g
    JOIN tmp_from_token_same f ON f.guide_id = g.id
    SET g.from_token_filled = 1;

    UPDATE tx_guide g
    JOIN tmp_to_token_same t ON t.guide_id = g.id
    SET g.to_token_pre_balance  = COALESCE(g.to_token_pre_balance, t.pre_balance),
        g.to_token_post_balance = COALESCE(g.to_token_post_balance, t.post_balance)
    WHERE g.token_id IS NOT NULL;

    SET v_rows_updated = v_rows_updated + ROW_COUNT();

    UPDATE tmp_guide_keys g
    JOIN tmp_to_token_same t ON t.guide_id = g.id
    SET g.to_token_filled = 1;

    DROP TEMPORARY TABLE tmp_from_token_same;
    DROP TEMPORARY TABLE tmp_to_token_same;

    -- ----------------------------------------------------------
    -- Phase 2: TOKEN BALANCE — prior-tx LATERAL fallback
    -- Dedup: one LATERAL seek per unique (account/owner, token, block_time)
    -- READ COMMITTED: avoid gap locks on balance change indexes
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

    SET v_rows_updated = v_rows_updated + ROW_COUNT();

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

    SET v_rows_updated = v_rows_updated + ROW_COUNT();

    DROP TEMPORARY TABLE tmp_to_token_prior;

    -- ----------------------------------------------------------
    -- Phase 3: SOL BALANCE — same-tx matches
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
        WHERE g.from_sol_filled = 0
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
        WHERE g.to_sol_filled = 0
    ) sub WHERE sub.rn = 1;

    -- Apply same-tx SOL balances
    UPDATE tx_guide g
    JOIN tmp_from_sol_same f ON f.guide_id = g.id
    SET g.from_sol_pre_balance  = COALESCE(g.from_sol_pre_balance, f.pre_balance),
        g.from_sol_post_balance = COALESCE(g.from_sol_post_balance, f.post_balance);

    SET v_rows_updated = v_rows_updated + ROW_COUNT();

    UPDATE tmp_guide_keys g
    JOIN tmp_from_sol_same f ON f.guide_id = g.id
    SET g.from_sol_filled = 1;

    UPDATE tx_guide g
    JOIN tmp_to_sol_same t ON t.guide_id = g.id
    SET g.to_sol_pre_balance  = COALESCE(g.to_sol_pre_balance, t.pre_balance),
        g.to_sol_post_balance = COALESCE(g.to_sol_post_balance, t.post_balance);

    SET v_rows_updated = v_rows_updated + ROW_COUNT();

    UPDATE tmp_guide_keys g
    JOIN tmp_to_sol_same t ON t.guide_id = g.id
    SET g.to_sol_filled = 1;

    DROP TEMPORARY TABLE tmp_from_sol_same;
    DROP TEMPORARY TABLE tmp_to_sol_same;

    -- ----------------------------------------------------------
    -- SOL BALANCE: prior-tx LATERAL fallback
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

    SET v_rows_updated = v_rows_updated + ROW_COUNT();

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

    SET v_rows_updated = v_rows_updated + ROW_COUNT();

    DROP TEMPORARY TABLE tmp_to_sol_prior;

    -- Restore default isolation level
    SET SESSION transaction_isolation = @saved_isolation;

    -- ----------------------------------------------------------
    -- Phase 4: Default mint address balances to 0
    -- ----------------------------------------------------------
    UPDATE tx_guide g
    JOIN tmp_guide_keys gk ON gk.id = g.id
    JOIN tx_address a ON a.id = g.from_address_id
    SET g.from_token_pre_balance  = COALESCE(g.from_token_pre_balance, 0),
        g.from_token_post_balance = COALESCE(g.from_token_post_balance, 0),
        g.from_sol_pre_balance    = COALESCE(g.from_sol_pre_balance, 0),
        g.from_sol_post_balance   = COALESCE(g.from_sol_post_balance, 0)
    WHERE a.address_type = 'mint'
      AND (g.from_token_pre_balance IS NULL OR g.from_token_post_balance IS NULL
           OR g.from_sol_pre_balance IS NULL OR g.from_sol_post_balance IS NULL);

    SET v_rows_updated = v_rows_updated + ROW_COUNT();

    UPDATE tx_guide g
    JOIN tmp_guide_keys gk ON gk.id = g.id
    JOIN tx_address a ON a.id = g.to_address_id
    SET g.to_token_pre_balance  = COALESCE(g.to_token_pre_balance, 0),
        g.to_token_post_balance = COALESCE(g.to_token_post_balance, 0),
        g.to_sol_pre_balance    = COALESCE(g.to_sol_pre_balance, 0),
        g.to_sol_post_balance   = COALESCE(g.to_sol_post_balance, 0)
    WHERE a.address_type = 'mint'
      AND (g.to_token_pre_balance IS NULL OR g.to_token_post_balance IS NULL
           OR g.to_sol_pre_balance IS NULL OR g.to_sol_post_balance IS NULL);

    SET v_rows_updated = v_rows_updated + ROW_COUNT();

    -- ----------------------------------------------------------
    -- Phase 4b: Default close_ata edges post_balance to 0
    -- (ATA is closed, so token balance after is always 0)
    -- ----------------------------------------------------------
    IF v_gt_close_ata IS NOT NULL THEN
        UPDATE tx_guide g
        JOIN tmp_guide_keys gk ON gk.id = g.id
        SET g.from_token_post_balance = COALESCE(g.from_token_post_balance, 0),
            g.to_token_post_balance   = COALESCE(g.to_token_post_balance, 0)
        WHERE gk.edge_type_id = v_gt_close_ata
          AND (g.from_token_post_balance IS NULL OR g.to_token_post_balance IS NULL);

        SET v_rows_updated = v_rows_updated + ROW_COUNT();
    END IF;

    -- ----------------------------------------------------------
    -- Phase 5: Default remaining NULL pre-balances to 0
    -- ----------------------------------------------------------
    UPDATE tx_guide g
    JOIN tmp_guide_keys gk ON gk.id = g.id
    SET g.from_token_pre_balance = COALESCE(g.from_token_pre_balance, 0),
        g.to_token_pre_balance   = COALESCE(g.to_token_pre_balance, 0),
        g.from_sol_pre_balance   = COALESCE(g.from_sol_pre_balance, 0),
        g.to_sol_pre_balance     = COALESCE(g.to_sol_pre_balance, 0)
    WHERE g.from_token_pre_balance IS NULL OR g.to_token_pre_balance IS NULL
       OR g.from_sol_pre_balance IS NULL OR g.to_sol_pre_balance IS NULL;

    SET v_rows_updated = v_rows_updated + ROW_COUNT();

    DROP TEMPORARY TABLE tmp_guide_keys;

    -- ----------------------------------------------------------
    -- Report
    -- ----------------------------------------------------------
    SELECT v_rows_updated AS rows_updated;

END$$

DELIMITER ;
