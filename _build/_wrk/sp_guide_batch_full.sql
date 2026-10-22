*************************** 1. row ***************************
           Procedure: sp_tx_guide_batch
            sql_mode: ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION
    Create Procedure: CREATE DEFINER=`root`@`%` PROCEDURE `sp_tx_guide_batch`(
    IN p_batch_size INT,
    OUT p_guide_count INT,
    OUT p_funding_count INT,
    OUT p_participant_count INT
)
BEGIN
    DECLARE v_sol_token_id BIGINT DEFAULT 25993;  

    SET p_guide_count = 0, p_funding_count = 0, p_participant_count = 0;

    
    DROP TEMPORARY TABLE IF EXISTS tmp_batch;
    CREATE TEMPORARY TABLE tmp_batch (
        activity_id BIGINT PRIMARY KEY,
        tx_id BIGINT,
        ins_index SMALLINT,
        block_time BIGINT UNSIGNED,
        activity_type VARCHAR(50),
        guide_type_id TINYINT UNSIGNED,
        creates_edge TINYINT,
        edge_direction VARCHAR(10),
        fee BIGINT UNSIGNED,
        priority_fee BIGINT UNSIGNED,
        INDEX idx_tx (tx_id)
    ) ENGINE=MEMORY;

    INSERT INTO tmp_batch (activity_id, tx_id, ins_index, block_time, activity_type,
                           guide_type_id, creates_edge, edge_direction, fee, priority_fee)
    SELECT a.id, a.tx_id, a.ins_index, t.block_time, a.activity_type,
           atm.guide_type_id, COALESCE(atm.creates_edge, 0), COALESCE(atm.edge_direction, 'none'),
           t.fee, t.priority_fee
    FROM tx_activity a
    JOIN tx t ON t.id = a.tx_id
    LEFT JOIN tx_activity_type_map atm ON atm.activity_type = a.activity_type
    WHERE a.guide_loaded = 0
    ORDER BY a.id
    LIMIT p_batch_size;

    
    IF NOT EXISTS (SELECT 1 FROM tmp_batch) THEN
        DROP TEMPORARY TABLE IF EXISTS tmp_batch;
    ELSE

    
    
    INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                          token_id, amount, decimals, edge_type_id,
                          source_id, source_row_id, ins_index, fee, priority_fee)
    SELECT s.tx_id, b.block_time, s.account_address_id, tk.mint_address_id,
           s.token_1_id, s.amount_1, tk.decimals, 3, 2, s.id, s.ins_index, b.fee, b.priority_fee
    FROM tmp_batch b
    JOIN tx_swap s ON s.activity_id = b.activity_id
    JOIN tx_token tk ON tk.id = s.token_1_id
    WHERE b.creates_edge = 1 AND b.edge_direction IN ('both', 'out')
      AND s.account_address_id IS NOT NULL AND tk.mint_address_id IS NOT NULL
    ON DUPLICATE KEY UPDATE amount = VALUES(amount), fee = VALUES(fee), priority_fee = VALUES(priority_fee);
    SET p_guide_count = ROW_COUNT();

    INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                          token_id, amount, decimals, edge_type_id,
                          source_id, source_row_id, ins_index, fee, priority_fee)
    SELECT s.tx_id, b.block_time, tk.mint_address_id, s.account_address_id,
           s.token_2_id, s.amount_2, tk.decimals, 4, 2, s.id, s.ins_index, b.fee, b.priority_fee
    FROM tmp_batch b
    JOIN tx_swap s ON s.activity_id = b.activity_id
    JOIN tx_token tk ON tk.id = s.token_2_id
    WHERE b.creates_edge = 1 AND b.edge_direction IN ('both', 'in')
      AND s.account_address_id IS NOT NULL AND tk.mint_address_id IS NOT NULL
    ON DUPLICATE KEY UPDATE amount = VALUES(amount), fee = VALUES(fee), priority_fee = VALUES(priority_fee);
    SET p_guide_count = p_guide_count + ROW_COUNT();

    
    INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                          from_token_account_id, to_token_account_id,
                          token_id, amount, decimals, edge_type_id,
                          source_id, source_row_id, ins_index, fee, priority_fee)
    SELECT t.tx_id, b.block_time, t.source_owner_address_id, t.destination_owner_address_id,
           t.source_address_id, t.destination_address_id,
           t.token_id, t.amount, t.decimals,
           CASE WHEN t.token_id = v_sol_token_id THEN 1 ELSE COALESCE(b.guide_type_id, 2) END,
           1, t.id, t.ins_index, b.fee, b.priority_fee
    FROM tmp_batch b
    JOIN tx_transfer t ON t.activity_id = b.activity_id
    WHERE b.creates_edge = 1
      AND t.source_owner_address_id IS NOT NULL
      AND t.destination_owner_address_id IS NOT NULL
    ON DUPLICATE KEY UPDATE amount = VALUES(amount), fee = VALUES(fee), priority_fee = VALUES(priority_fee);
    SET p_guide_count = p_guide_count + ROW_COUNT();

    
    
    
    INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                          from_token_account_id, to_token_account_id,
                          token_id, amount, decimals, edge_type_id,
                          source_id, source_row_id, ins_index, fee, priority_fee)
    SELECT t.tx_id, b.block_time, t.source_owner_address_id, 742702,  
           t.source_address_id, NULL,
           t.token_id, t.amount, t.decimals, 39,  
           1, t.id, t.ins_index, b.fee, b.priority_fee
    FROM tmp_batch b
    JOIN tx_transfer t ON t.activity_id = b.activity_id
    WHERE t.transfer_type = 'ACTIVITY_SPL_BURN'
      AND t.source_owner_address_id IS NOT NULL
      AND t.destination_owner_address_id IS NULL
    ON DUPLICATE KEY UPDATE amount = VALUES(amount), fee = VALUES(fee), priority_fee = VALUES(priority_fee);
    SET p_guide_count = p_guide_count + ROW_COUNT();

    
    
    
    INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                          from_token_account_id, to_token_account_id,
                          token_id, amount, decimals, edge_type_id,
                          source_id, source_row_id, ins_index, fee, priority_fee)
    SELECT t.tx_id, b.block_time, 742703, t.destination_owner_address_id,  
           NULL, t.destination_address_id,
           t.token_id, t.amount, t.decimals, 38,  
           1, t.id, t.ins_index, b.fee, b.priority_fee
    FROM tmp_batch b
    JOIN tx_transfer t ON t.activity_id = b.activity_id
    WHERE t.transfer_type = 'ACTIVITY_SPL_MINT'
      AND t.destination_owner_address_id IS NOT NULL
      AND t.source_owner_address_id IS NULL
    ON DUPLICATE KEY UPDATE amount = VALUES(amount), fee = VALUES(fee), priority_fee = VALUES(priority_fee);
    SET p_guide_count = p_guide_count + ROW_COUNT();

    
    
    
    INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                          from_token_account_id, to_token_account_id,
                          token_id, amount, decimals, edge_type_id,
                          source_id, source_row_id, ins_index, fee, priority_fee)
    SELECT t.tx_id, b.block_time, t.source_owner_address_id, 742705,  
           t.source_address_id, t.destination_address_id,
           t.token_id, t.amount, t.decimals, 8,  
           1, t.id, t.ins_index, b.fee, b.priority_fee
    FROM tmp_batch b
    JOIN tx_transfer t ON t.activity_id = b.activity_id
    WHERE t.transfer_type = 'ACTIVITY_SPL_CREATE_ACCOUNT'
      AND t.source_owner_address_id IS NOT NULL
      AND t.destination_owner_address_id IS NULL
    ON DUPLICATE KEY UPDATE amount = VALUES(amount), fee = VALUES(fee), priority_fee = VALUES(priority_fee);
    SET p_guide_count = p_guide_count + ROW_COUNT();

    
    INSERT INTO tx_funding_edge (from_address_id, to_address_id, total_sol, transfer_count,
                                  first_transfer_time, last_transfer_time)
    SELECT t.source_owner_address_id, t.destination_owner_address_id,
           SUM(t.amount) / 1e9, COUNT(*), MIN(b.block_time), MAX(b.block_time)
    FROM tmp_batch b
    JOIN tx_transfer t ON t.activity_id = b.activity_id
    JOIN tx_address a ON a.id = t.destination_owner_address_id
    WHERE t.token_id = v_sol_token_id
      AND a.funded_by_address_id = t.source_owner_address_id
      AND t.source_owner_address_id IS NOT NULL
      AND t.destination_owner_address_id IS NOT NULL
    GROUP BY t.source_owner_address_id, t.destination_owner_address_id
    ON DUPLICATE KEY UPDATE
        total_sol = total_sol + VALUES(total_sol),
        transfer_count = transfer_count + VALUES(transfer_count),
        last_transfer_time = GREATEST(last_transfer_time, VALUES(last_transfer_time));
    SET p_funding_count = ROW_COUNT();

    
    
    
    DROP TEMPORARY TABLE IF EXISTS tmp_part_stage;
    CREATE TEMPORARY TABLE tmp_part_stage (
        tid BIGINT, aid INT UNSIGNED, bt BIGINT UNSIGNED,
        ib TINYINT, isel TINYINT, iti TINYINT, ito TINYINT,
        vb DECIMAL(30,9), vs DECIMAL(30,9)
    ) ENGINE=MEMORY;

    INSERT INTO tmp_part_stage
    SELECT s.token_2_id, s.account_address_id, b.block_time, 1, 0, 0, 0, s.amount_2/POW(10, COALESCE(tk.decimals,9)), 0
    FROM tmp_batch b JOIN tx_swap s ON s.activity_id = b.activity_id JOIN tx_token tk ON tk.id = s.token_2_id
    WHERE s.token_2_id IS NOT NULL AND s.account_address_id IS NOT NULL;

    INSERT INTO tmp_part_stage
    SELECT s.token_1_id, s.account_address_id, b.block_time, 0, 1, 0, 0, 0, s.amount_1/POW(10, COALESCE(tk.decimals,9))
    FROM tmp_batch b JOIN tx_swap s ON s.activity_id = b.activity_id JOIN tx_token tk ON tk.id = s.token_1_id
    WHERE s.token_1_id IS NOT NULL AND s.account_address_id IS NOT NULL;

    INSERT INTO tmp_part_stage
    SELECT t.token_id, t.destination_owner_address_id, b.block_time, 0, 0, 1, 0, 0, 0
    FROM tmp_batch b JOIN tx_transfer t ON t.activity_id = b.activity_id
    WHERE t.token_id IS NOT NULL AND t.destination_owner_address_id IS NOT NULL;

    INSERT INTO tmp_part_stage
    SELECT t.token_id, t.source_owner_address_id, b.block_time, 0, 0, 0, 1, 0, 0
    FROM tmp_batch b JOIN tx_transfer t ON t.activity_id = b.activity_id
    WHERE t.token_id IS NOT NULL AND t.source_owner_address_id IS NOT NULL;

    INSERT INTO tx_token_participant (token_id, address_id, first_seen, last_seen,
                                       buy_count, sell_count, transfer_in_count, transfer_out_count,
                                       buy_volume, sell_volume, net_position)
    SELECT tid, aid, MIN(bt), MAX(bt), SUM(ib), SUM(isel), SUM(iti), SUM(ito), SUM(vb), SUM(vs), SUM(vb-vs)
    FROM tmp_part_stage GROUP BY tid, aid
    ON DUPLICATE KEY UPDATE
        last_seen = GREATEST(last_seen, VALUES(last_seen)),
        buy_count = buy_count + VALUES(buy_count),
        sell_count = sell_count + VALUES(sell_count),
        transfer_in_count = transfer_in_count + VALUES(transfer_in_count),
        transfer_out_count = transfer_out_count + VALUES(transfer_out_count),
        buy_volume = buy_volume + VALUES(buy_volume),
        sell_volume = sell_volume + VALUES(sell_volume),
        net_position = net_position + VALUES(net_position);
    SET p_participant_count = ROW_COUNT();

    
    UPDATE tx_activity a JOIN tmp_batch b ON b.activity_id = a.id SET a.guide_loaded = 1;
    
    DROP TEMPORARY TABLE IF EXISTS tmp_batch;
    DROP TEMPORARY TABLE IF EXISTS tmp_part_stage;

    END IF;
END
character_set_client: cp850
collation_connection: cp850_general_ci
  Database Collation: utf8mb4_0900_ai_ci
