*************************** 1. row ***************************
           Procedure: sp_tx_guide_backfill
            sql_mode: ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION
    Create Procedure: CREATE DEFINER=`root`@`%` PROCEDURE `sp_tx_guide_backfill`(
    IN p_batch_size INT,
    IN p_start_tx_id BIGINT,
    IN p_end_tx_id BIGINT,
    OUT p_transfer_edges INT,
    OUT p_swap_edges INT,
    OUT p_last_tx_id BIGINT
)
BEGIN
    

    DECLARE v_sol_token_id BIGINT DEFAULT 25993;  
    DECLARE v_actual_start BIGINT;
    DECLARE v_actual_end BIGINT;

    SET p_transfer_edges = 0, p_swap_edges = 0, p_last_tx_id = NULL;
    SET p_batch_size = COALESCE(p_batch_size, 10000);

    
    IF p_start_tx_id IS NULL THEN
        SELECT COALESCE(MIN(id), 0) INTO v_actual_start FROM tx;
    ELSE
        SET v_actual_start = p_start_tx_id;
    END IF;

    
    SELECT MIN(id) INTO v_actual_end
    FROM (
        SELECT id FROM tx WHERE id >= v_actual_start ORDER BY id LIMIT p_batch_size, 1
    ) sub;

    IF v_actual_end IS NULL THEN
        
        SELECT MAX(id) + 1 INTO v_actual_end FROM tx WHERE id >= v_actual_start;
    END IF;

    IF p_end_tx_id IS NOT NULL AND v_actual_end > p_end_tx_id THEN
        SET v_actual_end = p_end_tx_id + 1;
    END IF;

    
    IF v_actual_end IS NULL OR v_actual_start >= v_actual_end THEN
        SET p_last_tx_id = v_actual_start;
        SIGNAL SQLSTATE '01000' SET MESSAGE_TEXT = 'No transactions in range';
    END IF;

    
    
    
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

    
    SELECT MAX(tx_id) INTO p_last_tx_id FROM tmp_tx_batch;

    IF p_last_tx_id IS NULL THEN
        DROP TEMPORARY TABLE IF EXISTS tmp_tx_batch;
        SIGNAL SQLSTATE '01000' SET MESSAGE_TEXT = 'No transactions found in batch';
    END IF;

    
    
    

    
    INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                          from_token_account_id, to_token_account_id,
                          token_id, amount, decimals, edge_type_id,
                          source_id, source_row_id, ins_index, fee, priority_fee)
    SELECT t.tx_id, b.block_time, t.source_owner_address_id, t.destination_owner_address_id,
           t.source_address_id, t.destination_address_id,
           t.token_id, t.amount, t.decimals,
           CASE WHEN t.token_id = v_sol_token_id THEN 1 ELSE 2 END,  
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

    
    INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                          from_token_account_id, to_token_account_id,
                          token_id, amount, decimals, edge_type_id,
                          source_id, source_row_id, ins_index, fee, priority_fee)
    SELECT t.tx_id, b.block_time, t.source_owner_address_id, 742702,
           t.source_address_id, NULL,
           t.token_id, t.amount, t.decimals, 39,  
           1, t.id, t.ins_index, b.fee, b.priority_fee
    FROM tmp_tx_batch b
    JOIN tx_transfer t ON t.tx_id = b.tx_id
    WHERE t.transfer_type = 'ACTIVITY_SPL_BURN'
      AND t.source_owner_address_id IS NOT NULL
    ON DUPLICATE KEY UPDATE amount = VALUES(amount);

    SET p_transfer_edges = p_transfer_edges + ROW_COUNT();

    
    INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                          from_token_account_id, to_token_account_id,
                          token_id, amount, decimals, edge_type_id,
                          source_id, source_row_id, ins_index, fee, priority_fee)
    SELECT t.tx_id, b.block_time, 742703, t.destination_owner_address_id,
           NULL, t.destination_address_id,
           t.token_id, t.amount, t.decimals, 38,  
           1, t.id, t.ins_index, b.fee, b.priority_fee
    FROM tmp_tx_batch b
    JOIN tx_transfer t ON t.tx_id = b.tx_id
    WHERE t.transfer_type = 'ACTIVITY_SPL_MINT'
      AND t.destination_owner_address_id IS NOT NULL
    ON DUPLICATE KEY UPDATE amount = VALUES(amount);

    SET p_transfer_edges = p_transfer_edges + ROW_COUNT();

    
    INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                          from_token_account_id, to_token_account_id,
                          token_id, amount, decimals, edge_type_id,
                          source_id, source_row_id, ins_index, fee, priority_fee)
    SELECT t.tx_id, b.block_time, t.source_owner_address_id, 742705,
           t.source_address_id, t.destination_address_id,
           t.token_id, t.amount, t.decimals, 8,  
           1, t.id, t.ins_index, b.fee, b.priority_fee
    FROM tmp_tx_batch b
    JOIN tx_transfer t ON t.tx_id = b.tx_id
    WHERE t.transfer_type = 'ACTIVITY_SPL_CREATE_ACCOUNT'
      AND t.source_owner_address_id IS NOT NULL
    ON DUPLICATE KEY UPDATE amount = VALUES(amount);

    SET p_transfer_edges = p_transfer_edges + ROW_COUNT();

    
    
    

    
    INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                          token_id, amount, decimals, edge_type_id,
                          source_id, source_row_id, ins_index, fee, priority_fee)
    SELECT s.tx_id, b.block_time, s.account_address_id, tk.mint_address_id,
           s.token_1_id, s.amount_1, tk.decimals, 3,  
           2, s.id, s.ins_index, b.fee, b.priority_fee
    FROM tmp_tx_batch b
    JOIN tx_swap s ON s.tx_id = b.tx_id
    JOIN tx_token tk ON tk.id = s.token_1_id
    WHERE s.account_address_id IS NOT NULL
      AND s.token_1_id IS NOT NULL
    ON DUPLICATE KEY UPDATE
        amount = VALUES(amount),
        fee = VALUES(fee),
        priority_fee = VALUES(priority_fee);

    SET p_swap_edges = ROW_COUNT();

    
    INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                          token_id, amount, decimals, edge_type_id,
                          source_id, source_row_id, ins_index, fee, priority_fee)
    SELECT s.tx_id, b.block_time, tk.mint_address_id, s.account_address_id,
           s.token_2_id, s.amount_2, tk.decimals, 4,  
           2, s.id, s.ins_index, b.fee, b.priority_fee
    FROM tmp_tx_batch b
    JOIN tx_swap s ON s.tx_id = b.tx_id
    JOIN tx_token tk ON tk.id = s.token_2_id
    WHERE s.account_address_id IS NOT NULL
      AND s.token_2_id IS NOT NULL
    ON DUPLICATE KEY UPDATE
        amount = VALUES(amount),
        fee = VALUES(fee),
        priority_fee = VALUES(priority_fee);

    SET p_swap_edges = p_swap_edges + ROW_COUNT();

    
    
    
    DROP TEMPORARY TABLE IF EXISTS tmp_tx_batch;

END
character_set_client: cp850
collation_connection: cp850_general_ci
  Database Collation: utf8mb4_0900_ai_ci
