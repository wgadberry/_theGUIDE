-- sp_tx_insert_transfers: Batch insert ALL transfers from full staging JSON
-- Uses JSON_TABLE with NESTED PATH to process all txs at once (no loop)

DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_tx_insert_transfers`;;

CREATE DEFINER=`root`@`%` PROCEDURE `sp_tx_insert_transfers`(
    IN p_txs_json JSON,
    OUT p_count INT
)
BEGIN
    INSERT IGNORE INTO tx_transfer (
        tx_id,
        ins_index,
        outer_ins_index,
        transfer_type,
        program_id,
        outer_program_id,
        token_id,
        decimals,
        amount,
        source_address_id,
        source_owner_address_id,
        destination_address_id,
        destination_owner_address_id,
        base_token_id,
        base_decimals,
        base_amount
    )
    SELECT
        tx.id,
        t.ins_index,
        t.outer_ins_index,
        t.transfer_type,
        prog.id,
        outer_prog.id,
        tok.id,
        t.decimals,
        t.amount,
        src_addr.id,
        src_owner.id,
        dst_addr.id,
        dst_owner.id,
        base_tok.id,
        t.base_decimals,
        t.base_amount
    FROM JSON_TABLE(
        p_txs_json,
        '$.data[*]' COLUMNS (
            tx_hash VARCHAR(88) PATH '$.tx_hash',
            NESTED PATH '$.transfers[*]' COLUMNS (
                ins_index SMALLINT PATH '$.ins_index',
                outer_ins_index SMALLINT PATH '$.outer_ins_index',
                transfer_type VARCHAR(50) PATH '$.transfer_type',
                program_id VARCHAR(44) PATH '$.program_id',
                outer_program_id VARCHAR(44) PATH '$.outer_program_id',
                token_address VARCHAR(44) PATH '$.token_address',
                decimals TINYINT UNSIGNED PATH '$.decimals',
                amount BIGINT UNSIGNED PATH '$.amount',
                source VARCHAR(44) PATH '$.source',
                source_owner VARCHAR(44) PATH '$.source_owner',
                destination VARCHAR(44) PATH '$.destination',
                destination_owner VARCHAR(44) PATH '$.destination_owner',
                base_token_address VARCHAR(44) PATH '$.base_value.token_address',
                base_decimals TINYINT UNSIGNED PATH '$.base_value.decimals',
                base_amount BIGINT UNSIGNED PATH '$.base_value.amount'
            )
        )
    ) AS t
    -- Join to tx table to get tx_id from signature
    JOIN tx ON tx.signature = t.tx_hash
    -- Only process NEW transactions (tx_state=8 means just inserted by decode)
    JOIN tmp_batch_tx_signatures sig ON sig.signature = t.tx_hash AND sig.is_new = 1
    -- JOINs for lookups
    LEFT JOIN tx_address prog_addr ON prog_addr.address = t.program_id
    LEFT JOIN tx_program prog ON prog.program_address_id = prog_addr.id
    LEFT JOIN tx_address outer_prog_addr ON outer_prog_addr.address = t.outer_program_id
    LEFT JOIN tx_program outer_prog ON outer_prog.program_address_id = outer_prog_addr.id
    LEFT JOIN tx_address tok_addr ON tok_addr.address = t.token_address
    LEFT JOIN tx_token tok ON tok.mint_address_id = tok_addr.id
    LEFT JOIN tx_address src_addr ON src_addr.address = t.source
    LEFT JOIN tx_address src_owner ON src_owner.address = t.source_owner
    LEFT JOIN tx_address dst_addr ON dst_addr.address = t.destination
    LEFT JOIN tx_address dst_owner ON dst_owner.address = t.destination_owner
    LEFT JOIN tx_address base_tok_addr ON base_tok_addr.address = t.base_token_address
    LEFT JOIN tx_token base_tok ON base_tok.mint_address_id = base_tok_addr.id
    WHERE t.ins_index IS NOT NULL;  -- Filter out txs with no transfers

    SET p_count = ROW_COUNT();
END;;

DELIMITER ;
