DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_tx_guide_batch`;;

CREATE DEFINER=`root`@`%` PROCEDURE `sp_tx_guide_batch`(
    IN p_batch_size INT,
    OUT p_guide_count INT,
    OUT p_funding_count INT,
    OUT p_participant_count INT
)
BEGIN
    DECLARE v_sol_token_id BIGINT DEFAULT 25993;  -- So1111...

    SET p_guide_count = 0, p_funding_count = 0, p_participant_count = 0;

    -- 1. STAGE: Get current batch
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

    -- Exit early if empty
    IF NOT EXISTS (SELECT 1 FROM tmp_batch) THEN
        DROP TEMPORARY TABLE IF EXISTS tmp_batch;
    ELSE

    -- 2. SWAP EDGES
    -- Broken into two steps to avoid CROSS JOIN issues with temp tables
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

    -- 3. TRANSFER EDGES
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

    -- 3b. BURN EDGES (wallet → BURN sink)
    -- For burns, destination is NULL - use synthetic BURN address
    -- Note: Check t.transfer_type since activity_type may differ
    INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                          from_token_account_id, to_token_account_id,
                          token_id, amount, decimals, edge_type_id,
                          source_id, source_row_id, ins_index, fee, priority_fee)
    SELECT t.tx_id, b.block_time, t.source_owner_address_id, 3,  -- BURN sink
           t.source_address_id, NULL,
           t.token_id, t.amount, t.decimals, 39,  -- burn edge_type
           1, t.id, t.ins_index, b.fee, b.priority_fee
    FROM tmp_batch b
    JOIN tx_transfer t ON t.activity_id = b.activity_id
    WHERE t.transfer_type = 'ACTIVITY_SPL_BURN'
      AND t.source_owner_address_id IS NOT NULL
      AND t.destination_owner_address_id IS NULL
    ON DUPLICATE KEY UPDATE amount = VALUES(amount), fee = VALUES(fee), priority_fee = VALUES(priority_fee);
    SET p_guide_count = p_guide_count + ROW_COUNT();

    -- 3c. MINT EDGES (MINT source → wallet)
    -- For mints, source is NULL - use synthetic MINT address
    -- Note: Check t.transfer_type since activity_type may differ
    INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                          from_token_account_id, to_token_account_id,
                          token_id, amount, decimals, edge_type_id,
                          source_id, source_row_id, ins_index, fee, priority_fee)
    SELECT t.tx_id, b.block_time, 4, t.destination_owner_address_id,  -- MINT source
           NULL, t.destination_address_id,
           t.token_id, t.amount, t.decimals, 38,  -- mint edge_type
           1, t.id, t.ins_index, b.fee, b.priority_fee
    FROM tmp_batch b
    JOIN tx_transfer t ON t.activity_id = b.activity_id
    WHERE t.transfer_type = 'ACTIVITY_SPL_MINT'
      AND t.destination_owner_address_id IS NOT NULL
      AND t.source_owner_address_id IS NULL
    ON DUPLICATE KEY UPDATE amount = VALUES(amount), fee = VALUES(fee), priority_fee = VALUES(priority_fee);
    SET p_guide_count = p_guide_count + ROW_COUNT();

    -- 3d. CREATE_ACCOUNT EDGES (wallet → CREATE sink)
    -- For account creation where destination is NULL
    -- Note: Check t.transfer_type since activity_type may differ
    INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                          from_token_account_id, to_token_account_id,
                          token_id, amount, decimals, edge_type_id,
                          source_id, source_row_id, ins_index, fee, priority_fee)
    SELECT t.tx_id, b.block_time, t.source_owner_address_id, 6,  -- CREATE sink
           t.source_address_id, t.destination_address_id,
           t.token_id, t.amount, t.decimals, 8,  -- create_account edge_type
           1, t.id, t.ins_index, b.fee, b.priority_fee
    FROM tmp_batch b
    JOIN tx_transfer t ON t.activity_id = b.activity_id
    WHERE t.transfer_type = 'ACTIVITY_SPL_CREATE_ACCOUNT'
      AND t.source_owner_address_id IS NOT NULL
      AND t.destination_owner_address_id IS NULL
    ON DUPLICATE KEY UPDATE amount = VALUES(amount), fee = VALUES(fee), priority_fee = VALUES(priority_fee);
    SET p_guide_count = p_guide_count + ROW_COUNT();

    -- 4. TOKEN PARTICIPANTS
    -- To avoid "Can't reopen table", we use a second temporary table
    -- to flatten the data before the final aggregation.
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

    -- 5. FINALIZE
    UPDATE tx_activity a JOIN tmp_batch b ON b.activity_id = a.id SET a.guide_loaded = 1;
    
    DROP TEMPORARY TABLE IF EXISTS tmp_batch;
    DROP TEMPORARY TABLE IF EXISTS tmp_part_stage;

    END IF;
END;;

/*

DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_tx_guide_batch`;;

CREATE DEFINER=`root`@`%` PROCEDURE `sp_tx_guide_batch`(
    IN p_batch_size INT,
    OUT p_guide_count INT,
    OUT p_funding_count INT,
    OUT p_participant_count INT
)
BEGIN
    DECLARE v_sol_token_id BIGINT DEFAULT 25993;  -- So11111111111111111111111111111111111111112

    SET p_guide_count = 0, p_funding_count = 0, p_participant_count = 0;

    -- =========================================================================
    -- 1. STAGE: Batch of unprocessed activities with type mapping
    -- =========================================================================
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
        INDEX idx_tx (tx_id),
        INDEX idx_type (activity_type)
    ) ENGINE=MEMORY;

    INSERT INTO tmp_batch (activity_id, tx_id, ins_index, block_time, activity_type,
                           guide_type_id, creates_edge, edge_direction)
    SELECT a.id, a.tx_id, a.ins_index, t.block_time, a.activity_type,
           atm.guide_type_id, COALESCE(atm.creates_edge, 0), COALESCE(atm.edge_direction, 'none')
    FROM tx_activity a
    JOIN tx t ON t.id = a.tx_id
    LEFT JOIN tx_activity_type_map atm ON atm.activity_type = a.activity_type
    WHERE a.guide_loaded = 0
    ORDER BY a.id
    LIMIT p_batch_size;

    -- Exit early if nothing to process
    SELECT COUNT(*) INTO @batch_count FROM tmp_batch;
    IF @batch_count = 0 THEN
        DROP TEMPORARY TABLE IF EXISTS tmp_batch;
        SET p_guide_count = 0, p_funding_count = 0, p_participant_count = 0;
    ELSE

    -- =========================================================================
    -- 2. SWAP edges → tx_guide (swap_in=3 for token_1, swap_out=4 for token_2)
    --    Uses tx_activity_type_map for edge_type lookup
    -- =========================================================================

    -- swap_in: account sends token_1 into swap
    INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                          token_id, amount, decimals, edge_type_id,
                          source_id, source_row_id, ins_index,
                          pool_id, swap_direction)
    SELECT s.tx_id, b.block_time, s.account_address_id, tk1.mint_address_id,
           s.token_1_id, s.amount_1, tk1.decimals, 3,  -- swap_in
           2, s.id, s.ins_index,
           s.amm_id, 'in'
    FROM tmp_batch b
    JOIN tx_swap s ON s.activity_id = b.activity_id
    JOIN tx_token tk1 ON tk1.id = s.token_1_id
    WHERE b.edge_direction IN ('both', 'out')
      AND b.creates_edge = 1
      AND s.token_1_id IS NOT NULL
      AND s.account_address_id IS NOT NULL
    ON DUPLICATE KEY UPDATE amount = VALUES(amount),
      pool_id = COALESCE(VALUES(pool_id), pool_id),
      swap_direction = COALESCE(VALUES(swap_direction), swap_direction);

    SET p_guide_count = ROW_COUNT();

    -- swap_out: account receives token_2 from swap
    INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                          token_id, amount, decimals, edge_type_id,
                          source_id, source_row_id, ins_index,
                          pool_id, swap_direction)
    SELECT s.tx_id, b.block_time, tk2.mint_address_id, s.account_address_id,
           s.token_2_id, s.amount_2, tk2.decimals, 4,  -- swap_out
           2, s.id, s.ins_index,
           s.amm_id, 'out'
    FROM tmp_batch b
    JOIN tx_swap s ON s.activity_id = b.activity_id
    JOIN tx_token tk2 ON tk2.id = s.token_2_id
    WHERE b.edge_direction IN ('both', 'in')
      AND b.creates_edge = 1
      AND s.token_2_id IS NOT NULL
      AND s.account_address_id IS NOT NULL
    ON DUPLICATE KEY UPDATE amount = VALUES(amount),
      pool_id = COALESCE(VALUES(pool_id), pool_id),
      swap_direction = COALESCE(VALUES(swap_direction), swap_direction);

    SET p_guide_count = p_guide_count + ROW_COUNT();

    -- =========================================================================
    -- 3. TRANSFER edges → tx_guide
    --    Uses tx_activity_type_map for edge_type (sol_transfer=1, spl_transfer=2)
    -- =========================================================================
    INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                          from_token_account_id, to_token_account_id,
                          token_id, amount, decimals, edge_type_id,
                          source_id, source_row_id, ins_index)
    SELECT t.tx_id, b.block_time, t.source_owner_address_id, t.destination_owner_address_id,
           t.source_address_id, t.destination_address_id,
           t.token_id, t.amount, t.decimals,
           CASE WHEN t.token_id = v_sol_token_id THEN 1 ELSE COALESCE(b.guide_type_id, 2) END,
           1, t.id, t.ins_index
    FROM tmp_batch b
    JOIN tx_transfer t ON t.activity_id = b.activity_id
    WHERE b.creates_edge = 1
      AND t.source_owner_address_id IS NOT NULL
      AND t.destination_owner_address_id IS NOT NULL
    ON DUPLICATE KEY UPDATE amount = VALUES(amount);

    SET p_guide_count = p_guide_count + ROW_COUNT();

    -- =========================================================================
    -- 4. OTHER ACTIVITY edges → tx_guide (burn, mint, liquidity, etc.)
    --    Creates edges for activities that have guide_type but no swap/transfer
    -- =========================================================================
    INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                          token_id, edge_type_id, source_id, source_row_id, ins_index)
    SELECT b.tx_id, b.block_time,
           a.account_address_id,  -- from
           a.account_address_id,  -- to (self-referential for burns, mints, etc.)
           NULL,  -- token_id would need to come from activity data
           b.guide_type_id,
           5,  -- source_id = 5 (manual/other)
           b.activity_id,
           b.ins_index
    FROM tmp_batch b
    JOIN tx_activity a ON a.id = b.activity_id
    LEFT JOIN tx_swap s ON s.activity_id = b.activity_id
    LEFT JOIN tx_transfer t ON t.activity_id = b.activity_id
    WHERE b.creates_edge = 1
      AND b.guide_type_id IS NOT NULL
      AND s.id IS NULL  -- Not already handled as swap
      AND t.id IS NULL  -- Not already handled as transfer
      AND a.account_address_id IS NOT NULL
      AND b.activity_type NOT LIKE 'ACTIVITY_%SWAP'  -- Extra safety
      AND b.activity_type NOT LIKE '%TRANSFER%'      -- Extra safety
    ON DUPLICATE KEY UPDATE edge_type_id = VALUES(edge_type_id);

    SET p_guide_count = p_guide_count + ROW_COUNT();

    -- =========================================================================
    -- 5. FUNDING edges → tx_funding_edge (aggregate by funder→funded pair)
    -- =========================================================================

    -- Collect distinct addresses involved in this batch that have funders
    -- (Use separate INSERTs to avoid MySQL temp table reopen limitation)
    DROP TEMPORARY TABLE IF EXISTS tmp_funded_addresses;
    CREATE TEMPORARY TABLE tmp_funded_addresses (
        address_id INT UNSIGNED PRIMARY KEY,
        funder_id INT UNSIGNED,
        INDEX idx_funder (funder_id)
    ) ENGINE=MEMORY;

    -- From swap accounts
    INSERT IGNORE INTO tmp_funded_addresses (address_id, funder_id)
    SELECT DISTINCT s.account_address_id, a.funded_by_address_id
    FROM tmp_batch b
    JOIN tx_swap s ON s.activity_id = b.activity_id
    JOIN tx_address a ON a.id = s.account_address_id
    WHERE a.funded_by_address_id IS NOT NULL AND s.account_address_id IS NOT NULL;

    -- From transfer source owners
    INSERT IGNORE INTO tmp_funded_addresses (address_id, funder_id)
    SELECT DISTINCT t.source_owner_address_id, a.funded_by_address_id
    FROM tmp_batch b
    JOIN tx_transfer t ON t.activity_id = b.activity_id
    JOIN tx_address a ON a.id = t.source_owner_address_id
    WHERE a.funded_by_address_id IS NOT NULL AND t.source_owner_address_id IS NOT NULL;

    -- From transfer destination owners
    INSERT IGNORE INTO tmp_funded_addresses (address_id, funder_id)
    SELECT DISTINCT t.destination_owner_address_id, a.funded_by_address_id
    FROM tmp_batch b
    JOIN tx_transfer t ON t.activity_id = b.activity_id
    JOIN tx_address a ON a.id = t.destination_owner_address_id
    WHERE a.funded_by_address_id IS NOT NULL AND t.destination_owner_address_id IS NOT NULL;

    -- Aggregate funding from transfers in this batch where funder→funded relationship exists
    INSERT INTO tx_funding_edge (from_address_id, to_address_id, total_sol, transfer_count,
                                  first_transfer_time, last_transfer_time)
    SELECT tf.funder_id, tf.address_id,
           SUM(CASE WHEN t.token_id = v_sol_token_id THEN t.amount ELSE 0 END) / 1e9,
           COUNT(*),
           MIN(b.block_time),
           MAX(b.block_time)
    FROM tmp_funded_addresses tf
    JOIN tx_transfer t ON t.source_owner_address_id = tf.funder_id
                      AND t.destination_owner_address_id = tf.address_id
    JOIN tmp_batch b ON b.activity_id = t.activity_id
    GROUP BY tf.funder_id, tf.address_id
    ON DUPLICATE KEY UPDATE
        total_sol = total_sol + VALUES(total_sol),
        transfer_count = transfer_count + VALUES(transfer_count),
        last_transfer_time = GREATEST(last_transfer_time, VALUES(last_transfer_time));

    SET p_funding_count = ROW_COUNT();

    DROP TEMPORARY TABLE IF EXISTS tmp_funded_addresses;

    -- =========================================================================
    -- 6. TOKEN PARTICIPANTS → tx_token_participant
    -- =========================================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_participants;
    CREATE TEMPORARY TABLE tmp_participants (
        token_id BIGINT,
        address_id INT UNSIGNED,
        block_time BIGINT UNSIGNED,
        is_buy TINYINT DEFAULT 0,
        is_sell TINYINT DEFAULT 0,
        is_transfer_in TINYINT DEFAULT 0,
        is_transfer_out TINYINT DEFAULT 0,
        buy_amount DECIMAL(30,9) DEFAULT 0,
        sell_amount DECIMAL(30,9) DEFAULT 0,
        INDEX idx_token_addr (token_id, address_id)
    ) ENGINE=MEMORY;

    -- Swap buys: receiving token_2
    INSERT INTO tmp_participants (token_id, address_id, block_time, is_buy, buy_amount)
    SELECT s.token_2_id, s.account_address_id, b.block_time, 1,
           s.amount_2 / POW(10, COALESCE(tk.decimals, 9))
    FROM tmp_batch b
    JOIN tx_swap s ON s.activity_id = b.activity_id
    LEFT JOIN tx_token tk ON tk.id = s.token_2_id
    WHERE s.token_2_id IS NOT NULL AND s.account_address_id IS NOT NULL;

    -- Swap sells: sending token_1
    INSERT INTO tmp_participants (token_id, address_id, block_time, is_sell, sell_amount)
    SELECT s.token_1_id, s.account_address_id, b.block_time, 1,
           s.amount_1 / POW(10, COALESCE(tk.decimals, 9))
    FROM tmp_batch b
    JOIN tx_swap s ON s.activity_id = b.activity_id
    LEFT JOIN tx_token tk ON tk.id = s.token_1_id
    WHERE s.token_1_id IS NOT NULL AND s.account_address_id IS NOT NULL;

    -- Transfer in: destination receives
    INSERT INTO tmp_participants (token_id, address_id, block_time, is_transfer_in)
    SELECT t.token_id, t.destination_owner_address_id, b.block_time, 1
    FROM tmp_batch b
    JOIN tx_transfer t ON t.activity_id = b.activity_id
    WHERE t.token_id IS NOT NULL AND t.destination_owner_address_id IS NOT NULL;

    -- Transfer out: source sends
    INSERT INTO tmp_participants (token_id, address_id, block_time, is_transfer_out)
    SELECT t.token_id, t.source_owner_address_id, b.block_time, 1
    FROM tmp_batch b
    JOIN tx_transfer t ON t.activity_id = b.activity_id
    WHERE t.token_id IS NOT NULL AND t.source_owner_address_id IS NOT NULL;

    -- Aggregate and upsert
    INSERT INTO tx_token_participant (token_id, address_id, first_seen, last_seen,
                                       buy_count, sell_count, transfer_in_count, transfer_out_count,
                                       buy_volume, sell_volume, net_position)
    SELECT token_id, address_id,
           MIN(block_time), MAX(block_time),
           SUM(is_buy), SUM(is_sell), SUM(is_transfer_in), SUM(is_transfer_out),
           SUM(buy_amount), SUM(sell_amount), SUM(buy_amount) - SUM(sell_amount)
    FROM tmp_participants
    WHERE token_id IS NOT NULL AND address_id IS NOT NULL
    GROUP BY token_id, address_id
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

    DROP TEMPORARY TABLE IF EXISTS tmp_participants;

    -- =========================================================================
    -- 7. Mark activities as processed
    -- =========================================================================
    UPDATE tx_activity a
    JOIN tmp_batch b ON b.activity_id = a.id
    SET a.guide_loaded = 1;

    -- =========================================================================
    -- Stamp in_guide on tokens from this batch.
    -- Also set attempt_cnt = 3 on already-enriched tokens so the enricher
    -- claim query (attempt_cnt < 3) skips them immediately.
    -- =========================================================================
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

    -- =========================================================================
    -- CLEANUP
    -- =========================================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_batch;

    END IF;  -- End of IF @batch_count > 0
END;;

DELIMITER ;
*/


DELIMITER ;














