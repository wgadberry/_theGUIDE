-- ============================================================================
-- sp_tx_bmap_state_backfill: Populate tx_bmap_state from tx_guide history
--
-- Calculates running balances per (token, address) ordered by block_time
--
-- Parameters:
--   p_token_id: Specific token to backfill (NULL = all tokens)
--   p_batch_size: Tokens per batch (default 100)
--   p_truncate: 1 = truncate table first, 0 = incremental (default 0)
--
-- Usage:
--   CALL sp_tx_bmap_state_backfill(NULL, 100, 1);  -- Full rebuild
--   CALL sp_tx_bmap_state_backfill(12345, NULL, 0); -- Single token
-- ============================================================================

DROP PROCEDURE IF EXISTS sp_tx_bmap_state_backfill;

DELIMITER //

CREATE PROCEDURE sp_tx_bmap_state_backfill(
    IN p_token_id BIGINT UNSIGNED,
    IN p_batch_size INT UNSIGNED,
    IN p_truncate TINYINT UNSIGNED
)
BEGIN
    DECLARE v_token_id BIGINT UNSIGNED;
    DECLARE v_token_count INT DEFAULT 0;
    DECLARE v_processed INT DEFAULT 0;
    DECLARE v_rows_inserted BIGINT DEFAULT 0;
    DECLARE v_done INT DEFAULT 0;
    DECLARE v_start_time DATETIME DEFAULT NOW();

    DECLARE cur_tokens CURSOR FOR
        SELECT DISTINCT token_id
        FROM tx_guide
        WHERE token_id IS NOT NULL
          AND (p_token_id IS NULL OR token_id = p_token_id)
        ORDER BY token_id;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;

    -- Default batch size
    SET p_batch_size = COALESCE(p_batch_size, 100);
    SET p_truncate = COALESCE(p_truncate, 0);

    -- Truncate if requested
    IF p_truncate = 1 THEN
        TRUNCATE TABLE tx_bmap_state;
        SELECT 'Table truncated' AS status;
    END IF;

    -- Count tokens to process
    SELECT COUNT(DISTINCT token_id) INTO v_token_count
    FROM tx_guide
    WHERE token_id IS NOT NULL
      AND (p_token_id IS NULL OR token_id = p_token_id);

    SELECT CONCAT('Processing ', v_token_count, ' tokens...') AS status;

    -- Process each token
    OPEN cur_tokens;

    token_loop: LOOP
        FETCH cur_tokens INTO v_token_id;
        IF v_done THEN
            LEAVE token_loop;
        END IF;

        -- Delete existing state for this token (for incremental updates)
        IF p_truncate = 0 THEN
            DELETE FROM tx_bmap_state WHERE token_id = v_token_id;
        END IF;

        -- =======================================================================
        -- Build running balances using window function approach
        -- For each (token, address), calculate cumulative balance over time
        -- =======================================================================

        INSERT INTO tx_bmap_state (token_id, tx_id, address_id, delta, balance, block_time)
        SELECT
            token_id,
            tx_id,
            address_id,
            delta,
            ROUND(SUM(delta) OVER (
                PARTITION BY token_id, address_id
                ORDER BY block_time, tx_id
            ), 9) AS balance,
            block_time
        FROM (
            -- Aggregate deltas per (token, tx, address)
            -- Inflows are positive, outflows are negative
            SELECT
                g.token_id,
                g.tx_id,
                g.address_id,
                ROUND(SUM(g.delta), 9) AS delta,
                t.block_time
            FROM (
                -- Inflows (to_address receives)
                SELECT
                    token_id,
                    tx_id,
                    to_address_id AS address_id,
                    ROUND(amount / POW(10, COALESCE(decimals, 9)), 9) AS delta
                FROM tx_guide
                WHERE token_id = v_token_id
                  AND to_address_id IS NOT NULL

                UNION ALL

                -- Outflows (from_address sends) - negative
                SELECT
                    token_id,
                    tx_id,
                    from_address_id AS address_id,
                    -ROUND(amount / POW(10, COALESCE(decimals, 9)), 9) AS delta
                FROM tx_guide
                WHERE token_id = v_token_id
                  AND from_address_id IS NOT NULL
            ) g
            JOIN tx t ON t.id = g.tx_id
            GROUP BY g.token_id, g.tx_id, g.address_id, t.block_time
        ) deltas
        ORDER BY token_id, address_id, block_time, tx_id;

        SET v_rows_inserted = v_rows_inserted + ROW_COUNT();
        SET v_processed = v_processed + 1;

        -- Progress update every batch_size tokens
        IF v_processed % p_batch_size = 0 THEN
            SELECT CONCAT(
                'Progress: ', v_processed, '/', v_token_count,
                ' tokens (', ROUND(v_processed/v_token_count*100, 1), '%)',
                ' | Rows: ', FORMAT(v_rows_inserted, 0),
                ' | Elapsed: ', TIMEDIFF(NOW(), v_start_time)
            ) AS progress;
        END IF;

    END LOOP;

    CLOSE cur_tokens;

    -- Final summary
    SELECT
        v_processed AS tokens_processed,
        v_rows_inserted AS rows_inserted,
        TIMEDIFF(NOW(), v_start_time) AS elapsed,
        ROUND(v_rows_inserted / TIMESTAMPDIFF(SECOND, v_start_time, NOW()), 0) AS rows_per_sec;

END //

DELIMITER ;
