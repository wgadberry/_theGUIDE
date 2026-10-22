-- sp_tx_insert_txs_batch: Bulk insert transactions using JSON_TABLE
-- Uses JOINs only (no fn_tx_ensure_* calls) - requires sp_tx_prepopulate_lookups first
-- p_request_log_id links new tx records to the gateway request for billing

DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_tx_insert_txs_batch`;;

CREATE DEFINER=`root`@`%` PROCEDURE `sp_tx_insert_txs_batch`(
    IN p_txs_json JSON,
    IN p_request_log_id BIGINT UNSIGNED,
    IN p_tx_origin TINYINT UNSIGNED,
    OUT p_inserted_count INT,
    OUT p_skipped_count INT
)
BEGIN
    DECLARE v_total_count INT;

    -- Create temp table to track signatures
    DROP TEMPORARY TABLE IF EXISTS tmp_batch_tx_signatures;
    CREATE TEMPORARY TABLE tmp_batch_tx_signatures (
        idx INT,
        signature VARCHAR(88) PRIMARY KEY,
        tx_id BIGINT DEFAULT NULL,
        is_new TINYINT DEFAULT 0
    );

    -- Extract signatures for tracking
    INSERT INTO tmp_batch_tx_signatures (idx, signature)
    SELECT t.idx, t.tx_hash
    FROM JSON_TABLE(p_txs_json, '$.data[*]' COLUMNS (
        idx FOR ORDINALITY,
        tx_hash VARCHAR(88) PATH '$.tx_hash'
    )) t;

    SET v_total_count = ROW_COUNT();

    -- Main tx insert with JOINs (addresses already populated by sp_tx_prepopulate_lookups)
    -- Uses ON DUPLICATE KEY UPDATE so decoder fills in agg fields even when
    -- detailer created a skeleton tx record first (race condition).
    -- request_log_id is set for billing - only new records get this value
    INSERT INTO tx (
        signature,
        block_id,
        block_time,
        block_time_utc,
        fee,
        priority_fee,
        signer_address_id,
        agg_program_id,
        agg_account_address_id,
        agg_token_in_id,
        agg_token_out_id,
        agg_amount_in,
        agg_amount_out,
        agg_decimals_in,
        agg_decimals_out,
        agg_fee_amount,
        agg_fee_token_id,
        -- tx_json,
        tx_state,
        request_log_id,
        tx_origin
    )
    SELECT
        t.tx_hash,
        t.block_id,
        t.block_time,
        FROM_UNIXTIME(t.block_time),
        t.fee,
        t.priority_fee,
        -- Signer: prefer account, fallback to first transfer source_owner
        COALESCE(signer_addr.id, fallback_addr.id),
        -- Aggregator fields (only for swaps)
        CASE WHEN t.activity_type = 'ACTIVITY_TOKEN_SWAP' THEN agg_prog.id ELSE NULL END,
        CASE WHEN t.activity_type = 'ACTIVITY_TOKEN_SWAP' THEN signer_addr.id ELSE NULL END,
        CASE WHEN t.activity_type = 'ACTIVITY_TOKEN_SWAP' THEN tok1.id ELSE NULL END,
        CASE WHEN t.activity_type = 'ACTIVITY_TOKEN_SWAP' THEN tok2.id ELSE NULL END,
        CASE WHEN t.activity_type = 'ACTIVITY_TOKEN_SWAP' THEN t.amount_1 ELSE NULL END,
        CASE WHEN t.activity_type = 'ACTIVITY_TOKEN_SWAP' THEN t.amount_2 ELSE NULL END,
        CASE WHEN t.activity_type = 'ACTIVITY_TOKEN_SWAP' THEN t.decimal_1 ELSE NULL END,
        CASE WHEN t.activity_type = 'ACTIVITY_TOKEN_SWAP' THEN t.decimal_2 ELSE NULL END,
        CASE WHEN t.activity_type = 'ACTIVITY_TOKEN_SWAP' THEN t.fee_amount ELSE NULL END,
        CASE WHEN t.activity_type = 'ACTIVITY_TOKEN_SWAP' THEN fee_tok.id ELSE NULL END,
        -- t.tx_json,
        8,
        p_request_log_id,
        IFNULL(p_tx_origin, 0)
    FROM JSON_TABLE(
        p_txs_json,
        '$.data[*]' COLUMNS (
            tx_hash VARCHAR(88) PATH '$.tx_hash',
            block_id BIGINT UNSIGNED PATH '$.block_id',
            block_time BIGINT UNSIGNED PATH '$.block_time',
            fee BIGINT UNSIGNED PATH '$.fee',
            priority_fee BIGINT UNSIGNED PATH '$.priority_fee',
            signer_account VARCHAR(44) PATH '$.one_line_summary.data.account',
            fallback_signer VARCHAR(44) PATH '$.transfers[0].source_owner',
            activity_type VARCHAR(50) PATH '$.one_line_summary.activity_type',
            agg_program VARCHAR(44) PATH '$.one_line_summary.program_id',
            token_1 VARCHAR(44) PATH '$.one_line_summary.data.token_1',
            token_2 VARCHAR(44) PATH '$.one_line_summary.data.token_2',
            amount_1 BIGINT UNSIGNED PATH '$.one_line_summary.data.amount_1',
            amount_2 BIGINT UNSIGNED PATH '$.one_line_summary.data.amount_2',
            decimal_1 TINYINT UNSIGNED PATH '$.one_line_summary.data.token_decimal_1',
            decimal_2 TINYINT UNSIGNED PATH '$.one_line_summary.data.token_decimal_2',
            fee_amount BIGINT UNSIGNED PATH '$.one_line_summary.data.fee_ammount',
            fee_token VARCHAR(44) PATH '$.one_line_summary.data.fee_token' -- ,
            -- tx_json JSON PATH '$'
        )
    ) AS t
    -- JOINs for lookups (addresses already populated by sp_tx_prepopulate_lookups)
    LEFT JOIN tx_address signer_addr ON signer_addr.address = t.signer_account
    LEFT JOIN tx_address fallback_addr ON fallback_addr.address = t.fallback_signer
    LEFT JOIN tx_address agg_prog_addr ON agg_prog_addr.address = t.agg_program
    LEFT JOIN tx_program agg_prog ON agg_prog.program_address_id = agg_prog_addr.id
    LEFT JOIN tx_address tok1_addr ON tok1_addr.address = t.token_1
    LEFT JOIN tx_token tok1 ON tok1.mint_address_id = tok1_addr.id
    LEFT JOIN tx_address tok2_addr ON tok2_addr.address = t.token_2
    LEFT JOIN tx_token tok2 ON tok2.mint_address_id = tok2_addr.id
    LEFT JOIN tx_address fee_addr ON fee_addr.address = t.fee_token
    LEFT JOIN tx_token fee_tok ON fee_tok.mint_address_id = fee_addr.id
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

    SET p_inserted_count = ROW_COUNT();
    SET p_skipped_count = v_total_count - p_inserted_count;

    -- Update temp table with tx_ids
    -- is_new = 1 when decode hasn't run yet (bit 4 not set), so decoder
    -- still processes skeleton rows created by detailer
    UPDATE tmp_batch_tx_signatures s
    JOIN tx ON tx.signature = s.signature
    SET s.tx_id = tx.id,
        s.is_new = (tx.tx_state & 4 = 0);

    -- Note: tmp_batch_tx_signatures available for caller
END;;

DELIMITER ;
