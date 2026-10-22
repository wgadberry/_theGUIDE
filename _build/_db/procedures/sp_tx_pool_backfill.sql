DELIMITER //

DROP PROCEDURE IF EXISTS sp_tx_pool_backfill//

CREATE PROCEDURE sp_tx_pool_backfill(
    OUT p_created INT,
    OUT p_labels_updated INT,
    OUT p_accounts_copied INT
)
BEGIN
    -- ================================================================
    -- Backfill tx_pool from tx_address where address_type in (pool, lp_token)
    -- Uses fn_parse_dex, fn_parse_token_a, fn_parse_token_b to parse labels
    -- 1. Creates missing tx_pool records with program + token lookups
    --    (also caches dex + token_pair from label)
    -- 2. Updates pool_label/dex/token_pair + token IDs on existing records
    -- 3. Copies token_account/program from "Market" siblings
    --    (uses cached dex + token_pair columns — no UDF calls)
    -- ================================================================

    SET p_created = 0;
    SET p_labels_updated = 0;
    SET p_accounts_copied = 0;

    -- Step 1: Insert missing tx_pool records from pool/lp_token addresses
    IF EXISTS (
        SELECT 1 FROM tx_address a
        LEFT JOIN tx_pool p ON p.pool_address_id = a.id
        WHERE a.address_type IN ('pool', 'lp_token') AND p.id IS NULL
        LIMIT 1
    ) THEN
        DROP TEMPORARY TABLE IF EXISTS tmp_missing_pools;
        CREATE TEMPORARY TABLE tmp_missing_pools (
            address_id INT UNSIGNED PRIMARY KEY,
            label      VARCHAR(255)
        ) ENGINE=MEMORY AS
        SELECT a.id AS address_id, a.label
        FROM tx_address a
        LEFT JOIN tx_pool p ON p.pool_address_id = a.id
        WHERE a.address_type IN ('pool', 'lp_token')
          AND p.id IS NULL;

        INSERT IGNORE INTO tx_pool (pool_address_id, program_id, token1_id, token2_id,
                                    pool_label, dex, token_pair, attempt_cnt)
        SELECT
            m.address_id,
            pr.id,
            t1.id,
            t2.id,
            m.label,
            fn_parse_dex(m.label),
            fn_parse_token_pair(m.label),
            1
        FROM tmp_missing_pools m
        LEFT JOIN tx_program pr ON pr.name LIKE CONCAT(fn_parse_dex(m.label), '%')
        LEFT JOIN tx_token t1 ON t1.token_symbol = fn_parse_token_a(m.label)
        LEFT JOIN tx_token t2 ON t2.token_symbol = fn_parse_token_b(m.label);

        SET p_created = ROW_COUNT();
        DROP TEMPORARY TABLE IF EXISTS tmp_missing_pools;
    END IF;

    -- Step 2: Backfill token1/token2/program/dex/token_pair on existing records
    -- Pre-compute UDF results in temp table to avoid per-row evaluation in JOIN
    IF EXISTS (
        SELECT 1 FROM tx_pool p
        JOIN tx_address a ON a.id = p.pool_address_id
        WHERE (p.token1_id IS NULL OR p.token2_id IS NULL
               OR p.pool_label IS NULL OR p.dex IS NULL OR p.token_pair IS NULL)
          AND a.label IS NOT NULL AND a.label LIKE '%(%-%)%'
        LIMIT 1
    ) THEN
        DROP TEMPORARY TABLE IF EXISTS tmp_backfill_pools;
        CREATE TEMPORARY TABLE tmp_backfill_pools (
            pool_id      BIGINT UNSIGNED PRIMARY KEY,
            address_id   INT UNSIGNED,
            label        VARCHAR(255),
            parsed_dex   VARCHAR(100),
            parsed_pair  VARCHAR(100),
            parsed_tok_a VARCHAR(100),
            parsed_tok_b VARCHAR(100),
            INDEX idx_addr (address_id)
        ) ENGINE=MEMORY AS
        SELECT p.id AS pool_id, p.pool_address_id AS address_id, a.label,
               fn_parse_dex(a.label)        AS parsed_dex,
               fn_parse_token_pair(a.label)  AS parsed_pair,
               fn_parse_token_a(a.label)     AS parsed_tok_a,
               fn_parse_token_b(a.label)     AS parsed_tok_b
        FROM tx_pool p
        JOIN tx_address a ON a.id = p.pool_address_id
        WHERE (p.token1_id IS NULL OR p.token2_id IS NULL
               OR p.pool_label IS NULL OR p.dex IS NULL OR p.token_pair IS NULL)
          AND a.label IS NOT NULL AND a.label LIKE '%(%-%)%';

        UPDATE tx_pool p
        JOIN tmp_backfill_pools b ON b.pool_id = p.id
        LEFT JOIN tx_program pr ON pr.name LIKE CONCAT(b.parsed_dex, '%')
        LEFT JOIN tx_token t1 ON t1.token_symbol = b.parsed_tok_a
        LEFT JOIN tx_token t2 ON t2.token_symbol = b.parsed_tok_b
        SET p.program_id  = COALESCE(p.program_id, pr.id),
            p.token1_id   = COALESCE(p.token1_id, t1.id),
            p.token2_id   = COALESCE(p.token2_id, t2.id),
            p.pool_label  = COALESCE(p.pool_label, b.label),
            p.dex         = COALESCE(p.dex, b.parsed_dex),
            p.token_pair  = COALESCE(p.token_pair, b.parsed_pair);

        SET p_labels_updated = ROW_COUNT();
        DROP TEMPORARY TABLE IF EXISTS tmp_backfill_pools;
    END IF;

    -- Step 3: Copy token_account/program from "Market" siblings
    -- Uses cached dex + token_pair columns — NO UDF calls, just indexed joins
    IF EXISTS (
        SELECT 1 FROM tx_pool
        WHERE token_account1_id IS NULL
          AND pool_label IS NOT NULL
          AND pool_label NOT LIKE '%Market%'
        LIMIT 1
    ) THEN
        UPDATE tx_pool target
        JOIN (
            SELECT dex, token_pair,
                   token_account1_id, token_account2_id, lp_token_id, program_id
            FROM tx_pool
            WHERE pool_label LIKE '%Market%'
              AND token_account1_id IS NOT NULL
              AND dex IS NOT NULL
              AND token_pair IS NOT NULL
        ) src ON src.dex = target.dex AND src.token_pair = target.token_pair
        SET target.token_account1_id = COALESCE(target.token_account1_id, src.token_account1_id),
            target.token_account2_id = COALESCE(target.token_account2_id, src.token_account2_id),
            target.lp_token_id       = COALESCE(target.lp_token_id,       src.lp_token_id),
            target.program_id        = COALESCE(target.program_id,         src.program_id)
        WHERE target.token_account1_id IS NULL
          AND target.pool_label IS NOT NULL
          AND target.pool_label NOT LIKE '%Market%';

        SET p_accounts_copied = ROW_COUNT();
    END IF;

END //

DELIMITER ;
