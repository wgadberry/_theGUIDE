DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_tx_guide_backfill`;;

CREATE DEFINER=`root`@`%` PROCEDURE `sp_tx_guide_backfill`(
    IN p_batch_size INT,
    IN p_start_tx_id BIGINT,
    IN p_end_tx_id BIGINT,
    OUT p_transfer_edges INT,
    OUT p_swap_edges INT,
    OUT p_last_tx_id BIGINT
)
BEGIN
    /*
    Backfill tx_guide edges from existing tx_transfer and tx_swap records.

    Parameters:
        p_batch_size    - Number of transactions to process per call (default 10000)
        p_start_tx_id   - Start from this tx_id (NULL = start from beginning)
        p_end_tx_id     - Stop at this tx_id (NULL = no upper limit)

    Output:
        p_transfer_edges - Number of transfer edges created/updated
        p_swap_edges     - Number of swap edges created/updated
        p_last_tx_id     - Last tx_id processed (use as p_start_tx_id for next batch)

    Usage:
        -- Process first batch of 10000 transactions
        CALL sp_tx_guide_backfill(10000, NULL, NULL, @te, @se, @last);
        SELECT @te, @se, @last;

        -- Continue from where we left off
        CALL sp_tx_guide_backfill(10000, @last, NULL, @te, @se, @last);

        -- Process specific range
        CALL sp_tx_guide_backfill(5000, 1000, 50000, @te, @se, @last);
    */

    DECLARE v_sol_token_id BIGINT DEFAULT 25993;  -- So11111111111111111111111111111111111111112
    DECLARE v_actual_start BIGINT;
    DECLARE v_actual_end BIGINT;

    SET p_transfer_edges = 0, p_swap_edges = 0, p_last_tx_id = NULL;
    SET p_batch_size = COALESCE(p_batch_size, 10000);

    -- Determine actual start
    IF p_start_tx_id IS NULL THEN
        SELECT COALESCE(MIN(id), 0) INTO v_actual_start FROM tx;
    ELSE
        SET v_actual_start = p_start_tx_id;
    END IF;

    -- Determine actual end (batch_size transactions from start, capped by p_end_tx_id)
    SELECT MIN(id) INTO v_actual_end
    FROM (
        SELECT id FROM tx WHERE id >= v_actual_start ORDER BY id LIMIT p_batch_size, 1
    ) sub;

    IF v_actual_end IS NULL THEN
        -- Less than batch_size remaining, get max
        SELECT MAX(id) + 1 INTO v_actual_end FROM tx WHERE id >= v_actual_start;
    END IF;

    IF p_end_tx_id IS NOT NULL AND v_actual_end > p_end_tx_id THEN
        SET v_actual_end = p_end_tx_id + 1;
    END IF;

    -- Exit if nothing to process
    IF v_actual_end IS NULL OR v_actual_start >= v_actual_end THEN
        SET p_last_tx_id = v_actual_start;
        SIGNAL SQLSTATE '01000' SET MESSAGE_TEXT = 'No transactions in range';
    END IF;

    -- =========================================================================
    -- 1. STAGE: Get batch of transactions with their block_time and fees
    -- =========================================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_tx_batch;
    CREATE TEMPORARY TABLE tmp_tx_batch (
        tx_id BIGINT PRIMARY KEY,
        block_time BIGINT UNSIGNED,
        fee BIGINT UNSIGNED,
        priority_fee BIGINT UNSIGNED
    ) ENGINE=MEMORY;

    INSERT INTO tmp_tx_batch (tx_id, block_time, fee, priority_fee)
    SELECT id, block_time, fee, priority_fee
    FROM tx
    WHERE id >= v_actual_start AND id < v_actual_end;

    -- Record last tx_id for continuation
    SELECT MAX(tx_id) INTO p_last_tx_id FROM tmp_tx_batch;

    IF p_last_tx_id IS NULL THEN
        DROP TEMPORARY TABLE IF EXISTS tmp_tx_batch;
        SIGNAL SQLSTATE '01000' SET MESSAGE_TEXT = 'No transactions found in batch';
    END IF;

    -- =========================================================================
    -- 2. TRANSFER EDGES → tx_guide
    -- =========================================================================

    -- Standard transfers (source → destination)
    INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                          from_token_account_id, to_token_account_id,
                          token_id, amount, decimals, edge_type_id,
                          source_id, source_row_id, ins_index, fee, priority_fee)
    SELECT t.tx_id, b.block_time, t.source_owner_address_id, t.destination_owner_address_id,
           t.source_address_id, t.destination_address_id,
           t.token_id, t.amount, t.decimals,
           CASE WHEN t.token_id = v_sol_token_id THEN 1 ELSE 2 END,  -- sol_transfer=1, spl_transfer=2
           1, t.id, t.ins_index, b.fee, b.priority_fee
    FROM tmp_tx_batch b
    JOIN tx_transfer t ON t.tx_id = b.tx_id
    WHERE t.source_owner_address_id IS NOT NULL
      AND t.destination_owner_address_id IS NOT NULL
      AND t.transfer_type NOT IN ('ACTIVITY_SPL_BURN', 'ACTIVITY_SPL_MINT', 'ACTIVITY_SPL_CREATE_ACCOUNT')
    ON DUPLICATE KEY UPDATE
        amount = VALUES(amount),
        fee = VALUES(fee),
        priority_fee = VALUES(priority_fee);

    SET p_transfer_edges = ROW_COUNT();

    -- BURN edges (wallet → BURN sink id=3)
    INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                          from_token_account_id, to_token_account_id,
                          token_id, amount, decimals, edge_type_id,
                          source_id, source_row_id, ins_index, fee, priority_fee)
    SELECT t.tx_id, b.block_time, t.source_owner_address_id, 3,
           t.source_address_id, NULL,
           t.token_id, t.amount, t.decimals, 39,  -- burn
           1, t.id, t.ins_index, b.fee, b.priority_fee
    FROM tmp_tx_batch b
    JOIN tx_transfer t ON t.tx_id = b.tx_id
    WHERE t.transfer_type = 'ACTIVITY_SPL_BURN'
      AND t.source_owner_address_id IS NOT NULL
    ON DUPLICATE KEY UPDATE amount = VALUES(amount);

    SET p_transfer_edges = p_transfer_edges + ROW_COUNT();

    -- MINT edges (MINT source id=4 → wallet)
    INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                          from_token_account_id, to_token_account_id,
                          token_id, amount, decimals, edge_type_id,
                          source_id, source_row_id, ins_index, fee, priority_fee)
    SELECT t.tx_id, b.block_time, 4, t.destination_owner_address_id,
           NULL, t.destination_address_id,
           t.token_id, t.amount, t.decimals, 38,  -- mint
           1, t.id, t.ins_index, b.fee, b.priority_fee
    FROM tmp_tx_batch b
    JOIN tx_transfer t ON t.tx_id = b.tx_id
    WHERE t.transfer_type = 'ACTIVITY_SPL_MINT'
      AND t.destination_owner_address_id IS NOT NULL
    ON DUPLICATE KEY UPDATE amount = VALUES(amount);

    SET p_transfer_edges = p_transfer_edges + ROW_COUNT();

    -- CREATE_ACCOUNT edges (wallet → CREATE sink id=6)
    INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                          from_token_account_id, to_token_account_id,
                          token_id, amount, decimals, edge_type_id,
                          source_id, source_row_id, ins_index, fee, priority_fee)
    SELECT t.tx_id, b.block_time, t.source_owner_address_id, 6,
           t.source_address_id, t.destination_address_id,
           t.token_id, t.amount, t.decimals, 8,  -- create_account
           1, t.id, t.ins_index, b.fee, b.priority_fee
    FROM tmp_tx_batch b
    JOIN tx_transfer t ON t.tx_id = b.tx_id
    WHERE t.transfer_type = 'ACTIVITY_SPL_CREATE_ACCOUNT'
      AND t.source_owner_address_id IS NOT NULL
    ON DUPLICATE KEY UPDATE amount = VALUES(amount);

    SET p_transfer_edges = p_transfer_edges + ROW_COUNT();

    -- =========================================================================
    -- 3. SWAP EDGES → tx_guide
    -- =========================================================================

    -- swap_in: account sends token_1 to pool (edge_type=3)
    INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                          token_id, amount, decimals, edge_type_id,
                          source_id, source_row_id, ins_index, fee, priority_fee,
                          dex, pool_address_id, pool_label, swap_direction)
    SELECT s.tx_id, b.block_time, s.account_address_id, tk.mint_address_id,
           s.token_1_id, s.amount_1, tk.decimals, 3,  -- swap_in
           2, s.id, s.ins_index, b.fee, b.priority_fee,
           p.name, pool.pool_address_id, pool.pool_label, 'in'
    FROM tmp_tx_batch b
    JOIN tx_swap s ON s.tx_id = b.tx_id
    JOIN tx_token tk ON tk.id = s.token_1_id
    LEFT JOIN tx_program p ON p.id = s.program_id
    LEFT JOIN tx_pool pool ON pool.id = s.amm_id
    WHERE s.account_address_id IS NOT NULL
      AND s.token_1_id IS NOT NULL
    ON DUPLICATE KEY UPDATE
        amount = VALUES(amount),
        fee = VALUES(fee),
        priority_fee = VALUES(priority_fee),
        dex = COALESCE(VALUES(dex), dex),
        pool_address_id = COALESCE(VALUES(pool_address_id), pool_address_id),
        pool_label = COALESCE(VALUES(pool_label), pool_label),
        swap_direction = COALESCE(VALUES(swap_direction), swap_direction);

    SET p_swap_edges = ROW_COUNT();

    -- swap_out: account receives token_2 from pool (edge_type=4)
    INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                          token_id, amount, decimals, edge_type_id,
                          source_id, source_row_id, ins_index, fee, priority_fee,
                          dex, pool_address_id, pool_label, swap_direction)
    SELECT s.tx_id, b.block_time, tk.mint_address_id, s.account_address_id,
           s.token_2_id, s.amount_2, tk.decimals, 4,  -- swap_out
           2, s.id, s.ins_index, b.fee, b.priority_fee,
           p.name, pool.pool_address_id, pool.pool_label, 'out'
    FROM tmp_tx_batch b
    JOIN tx_swap s ON s.tx_id = b.tx_id
    JOIN tx_token tk ON tk.id = s.token_2_id
    LEFT JOIN tx_program p ON p.id = s.program_id
    LEFT JOIN tx_pool pool ON pool.id = s.amm_id
    WHERE s.account_address_id IS NOT NULL
      AND s.token_2_id IS NOT NULL
    ON DUPLICATE KEY UPDATE
        amount = VALUES(amount),
        fee = VALUES(fee),
        priority_fee = VALUES(priority_fee),
        dex = COALESCE(VALUES(dex), dex),
        pool_address_id = COALESCE(VALUES(pool_address_id), pool_address_id),
        pool_label = COALESCE(VALUES(pool_label), pool_label),
        swap_direction = COALESCE(VALUES(swap_direction), swap_direction);

    SET p_swap_edges = p_swap_edges + ROW_COUNT();

    -- =========================================================================
    -- CLEANUP
    -- =========================================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_tx_batch;

END;;

DELIMITER ;
