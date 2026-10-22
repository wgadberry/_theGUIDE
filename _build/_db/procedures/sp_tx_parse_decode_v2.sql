-- sp_tx_parse_decode_v2 stored procedure
-- V2: Reads from 4 pre-populated temporary tables instead of parsing JSON.
-- No JSON_TABLE calls. Python creates and populates:
--   tmp_decode_txs, tmp_decode_transfers, tmp_decode_swaps, tmp_decode_activities
-- All ID columns are pre-resolved by Python (addresses, tokens, programs, pools).
-- Python must also have inserted all lookup rows before calling this SP.

DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_tx_parse_decode_v2`;;

CREATE DEFINER=`root`@`%` PROCEDURE `sp_tx_parse_decode_v2`(
    IN p_request_log_id  BIGINT UNSIGNED,
    IN p_tx_origin       TINYINT UNSIGNED,
    OUT p_tx_count       INT,
    OUT p_transfer_count INT,
    OUT p_swap_count     INT,
    OUT p_activity_count INT,
    OUT p_skipped_count  INT
)
BEGIN
    DECLARE v_transfer_count INT DEFAULT 0;
    DECLARE v_swap_count INT DEFAULT 0;
    DECLARE v_activity_count INT DEFAULT 0;

    SET p_tx_count = 0;
    SET p_transfer_count = 0;
    SET p_swap_count = 0;
    SET p_activity_count = 0;
    SET p_skipped_count = 0;

    -- ---------------------------------------------------------------
    -- Step 1: Create tracking temp table from tmp_decode_txs
    -- ---------------------------------------------------------------
    DROP TEMPORARY TABLE IF EXISTS tmp_batch_tx_signatures;
    CREATE TEMPORARY TABLE tmp_batch_tx_signatures (
        idx       INT,
        signature VARCHAR(88) PRIMARY KEY,
        tx_id     BIGINT,
        is_new    TINYINT DEFAULT 0
    ) ENGINE=MEMORY;

    INSERT INTO tmp_batch_tx_signatures (idx, signature)
    SELECT idx, tx_hash FROM tmp_decode_txs;

    -- ---------------------------------------------------------------
    -- Step 2: Insert/update transactions from tmp_decode_txs
    -- ---------------------------------------------------------------
    -- Column mapping: tmp_decode_txs → tx table
    --   token_1_id  → agg_token_in_id
    --   token_2_id  → agg_token_out_id
    --   amount_1    → agg_amount_in
    --   amount_2    → agg_amount_out
    --   decimal_1   → agg_decimals_in
    --   decimal_2   → agg_decimals_out
    INSERT INTO tx (signature, block_id, block_time, block_time_utc, fee, priority_fee,
        signer_address_id, agg_program_id, agg_account_address_id,
        agg_token_in_id, agg_token_out_id, agg_amount_in, agg_amount_out,
        agg_decimals_in, agg_decimals_out, agg_fee_amount, agg_fee_token_id,
        tx_state, request_log_id, tx_origin)
    SELECT tx_hash, block_id, block_time, FROM_UNIXTIME(block_time), fee, priority_fee,
        signer_id,
        CASE WHEN activity_type = 'ACTIVITY_TOKEN_SWAP' THEN agg_program_id END,
        CASE WHEN activity_type = 'ACTIVITY_TOKEN_SWAP' THEN signer_id END,
        CASE WHEN activity_type = 'ACTIVITY_TOKEN_SWAP' THEN token_1_id END,
        CASE WHEN activity_type = 'ACTIVITY_TOKEN_SWAP' THEN token_2_id END,
        CASE WHEN activity_type = 'ACTIVITY_TOKEN_SWAP' THEN amount_1 END,
        CASE WHEN activity_type = 'ACTIVITY_TOKEN_SWAP' THEN amount_2 END,
        CASE WHEN activity_type = 'ACTIVITY_TOKEN_SWAP' THEN decimal_1 END,
        CASE WHEN activity_type = 'ACTIVITY_TOKEN_SWAP' THEN decimal_2 END,
        CASE WHEN activity_type = 'ACTIVITY_TOKEN_SWAP' THEN fee_amount END,
        CASE WHEN activity_type = 'ACTIVITY_TOKEN_SWAP' THEN fee_token_id END,
        8, p_request_log_id, IFNULL(p_tx_origin, 0)
    FROM tmp_decode_txs
    ON DUPLICATE KEY UPDATE
        fee = COALESCE(tx.fee, VALUES(fee)),
        priority_fee = COALESCE(tx.priority_fee, VALUES(priority_fee)),
        signer_address_id = COALESCE(tx.signer_address_id, VALUES(signer_address_id)),
        agg_program_id = COALESCE(tx.agg_program_id, VALUES(agg_program_id)),
        agg_account_address_id = COALESCE(tx.agg_account_address_id, VALUES(agg_account_address_id)),
        agg_token_in_id = COALESCE(tx.agg_token_in_id, VALUES(agg_token_in_id)),
        agg_token_out_id = COALESCE(tx.agg_token_out_id, VALUES(agg_token_out_id)),
        agg_amount_in = COALESCE(tx.agg_amount_in, VALUES(agg_amount_in)),
        agg_amount_out = COALESCE(tx.agg_amount_out, VALUES(agg_amount_out)),
        agg_decimals_in = COALESCE(tx.agg_decimals_in, VALUES(agg_decimals_in)),
        agg_decimals_out = COALESCE(tx.agg_decimals_out, VALUES(agg_decimals_out)),
        agg_fee_amount = COALESCE(tx.agg_fee_amount, VALUES(agg_fee_amount)),
        agg_fee_token_id = COALESCE(tx.agg_fee_token_id, VALUES(agg_fee_token_id)),
        request_log_id = COALESCE(tx.request_log_id, VALUES(request_log_id)),
        tx_origin = COALESCE(tx.tx_origin, VALUES(tx_origin)),
        tx_state = tx.tx_state | 8;

    -- Update tracking table with tx IDs and new/existing status
    UPDATE tmp_batch_tx_signatures s
    JOIN tx ON tx.signature = s.signature
    SET s.tx_id = tx.id, s.is_new = (tx.tx_state & 4 = 0);

    -- ---------------------------------------------------------------
    -- Step 3: Insert transfers from tmp_decode_transfers
    -- ---------------------------------------------------------------
    INSERT IGNORE INTO tx_transfer (tx_id, ins_index, outer_ins_index, transfer_type,
        program_id, outer_program_id, token_id, decimals, amount,
        source_address_id, source_owner_address_id,
        destination_address_id, destination_owner_address_id,
        base_token_id, base_decimals, base_amount)
    SELECT sig.tx_id, t.ins_index, t.outer_ins_index, t.transfer_type,
        t.program_id, t.outer_program_id, t.token_id, t.decimals, t.amount,
        t.source_id, t.source_owner_id, t.destination_id, t.destination_owner_id,
        t.base_token_id, t.base_decimals, t.base_amount
    FROM tmp_decode_transfers t
    JOIN tmp_batch_tx_signatures sig ON sig.signature = t.tx_hash AND sig.is_new = 1;

    SET v_transfer_count = ROW_COUNT();

    -- ---------------------------------------------------------------
    -- Step 4: Insert swaps from tmp_decode_swaps
    -- ---------------------------------------------------------------
    INSERT IGNORE INTO tx_swap (tx_id, ins_index, outer_ins_index, name, activity_type,
        program_id, outer_program_id, amm_id, account_address_id,
        token_1_id, token_2_id, amount_1, amount_2, decimals_1, decimals_2,
        token_account_1_1_address_id, token_account_1_2_address_id,
        token_account_2_1_address_id, token_account_2_2_address_id,
        owner_1_address_id, owner_2_address_id, fee_amount, fee_token_id)
    SELECT sig.tx_id, s.ins_index, s.outer_ins_index, s.name, s.activity_type,
        s.program_id, s.outer_program_id, s.amm_id, s.account_id,
        s.token_1_id, s.token_2_id, s.amount_1, s.amount_2, s.decimal_1, s.decimal_2,
        s.ta_1_1_id, s.ta_1_2_id, s.ta_2_1_id, s.ta_2_2_id,
        s.owner_1_id, s.owner_2_id, s.fee_amount, s.fee_token_id
    FROM tmp_decode_swaps s
    JOIN tmp_batch_tx_signatures sig ON sig.signature = s.tx_hash AND sig.is_new = 1;

    SET v_swap_count = ROW_COUNT();

    -- ---------------------------------------------------------------
    -- Step 5: Insert activities from tmp_decode_activities
    -- ---------------------------------------------------------------
    INSERT IGNORE INTO tx_activity (tx_id, ins_index, outer_ins_index, name, activity_type,
        program_id, outer_program_id, account_address_id, guide_loaded)
    SELECT sig.tx_id, a.ins_index, a.outer_ins_index, a.name, a.activity_type,
        a.program_id, a.outer_program_id, a.account_id, 0
    FROM tmp_decode_activities a
    JOIN tmp_batch_tx_signatures sig ON sig.signature = a.tx_hash AND sig.is_new = 1;

    SET v_activity_count = ROW_COUNT();

    -- ---------------------------------------------------------------
    -- Step 6: Cross-link activity_id on transfers and swaps
    -- ---------------------------------------------------------------
    UPDATE tx_transfer t
    JOIN tmp_batch_tx_signatures b ON b.tx_id = t.tx_id AND b.is_new = 1
    JOIN tx_activity a ON a.tx_id = t.tx_id
                       AND a.ins_index = t.ins_index
                       AND a.outer_ins_index = t.outer_ins_index
    SET t.activity_id = a.id
    WHERE t.activity_id IS NULL;

    UPDATE tx_swap s
    JOIN tmp_batch_tx_signatures b ON b.tx_id = s.tx_id AND b.is_new = 1
    JOIN tx_activity a ON a.tx_id = s.tx_id
                       AND a.ins_index = s.ins_index
                       AND a.outer_ins_index = s.outer_ins_index
    SET s.activity_id = a.id
    WHERE s.activity_id IS NULL;

    -- ---------------------------------------------------------------
    -- Step 7: Stamp tx_state bit 4 (decode complete)
    -- ---------------------------------------------------------------
    UPDATE tx t
    JOIN tmp_batch_tx_signatures b ON b.tx_id = t.id AND b.is_new = 1
    SET t.tx_state = t.tx_state | 4;

    -- ---------------------------------------------------------------
    -- Step 8: Set output counts and cleanup
    -- ---------------------------------------------------------------
    SET p_tx_count = (SELECT COUNT(*) FROM tmp_batch_tx_signatures);
    SET p_transfer_count = v_transfer_count;
    SET p_swap_count = v_swap_count;
    SET p_activity_count = v_activity_count;
    SET p_skipped_count = (SELECT COUNT(*) FROM tmp_batch_tx_signatures WHERE is_new = 0);

    -- Clean up all temp tables (Python-created + SP-created)
    DROP TEMPORARY TABLE IF EXISTS tmp_decode_txs;
    DROP TEMPORARY TABLE IF EXISTS tmp_decode_transfers;
    DROP TEMPORARY TABLE IF EXISTS tmp_decode_swaps;
    DROP TEMPORARY TABLE IF EXISTS tmp_decode_activities;
    DROP TEMPORARY TABLE IF EXISTS tmp_batch_tx_signatures;
END;;

DELIMITER ;
