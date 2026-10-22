-- sp_tx_insert_swaps: Batch insert swaps from full staging JSON
-- Uses JSON_TABLE with NESTED PATH to process all txs at once (no loop)
--
-- Feature flags:
--   FEATURE_SWAP_ROUTING (4): When set, collect ALL swap hops
--                             When not set, only collect top-level swaps (ins_index = outer_ins_index)

DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_tx_insert_swaps`;;

CREATE DEFINER=`root`@`%` PROCEDURE `sp_tx_insert_swaps`(
    IN p_txs_json JSON,
    IN p_features INT UNSIGNED,
    OUT p_count INT
)
BEGIN
    -- Feature flag constant
    DECLARE FEATURE_SWAP_ROUTING INT UNSIGNED DEFAULT 4;
    DECLARE v_collect_all_hops BOOLEAN;

    SET v_collect_all_hops = (p_features & FEATURE_SWAP_ROUTING) = FEATURE_SWAP_ROUTING;

    INSERT IGNORE INTO tx_swap (
        tx_id,
        ins_index,
        outer_ins_index,
        name,
        activity_type,
        program_id,
        outer_program_id,
        amm_id,
        account_address_id,
        token_1_id,
        token_2_id,
        amount_1,
        amount_2,
        decimals_1,
        decimals_2,
        token_account_1_1_address_id,
        token_account_1_2_address_id,
        token_account_2_1_address_id,
        token_account_2_2_address_id,
        owner_1_address_id,
        owner_2_address_id,
        fee_amount,
        fee_token_id
    )
    SELECT
        tx.id,
        a.ins_index,
        a.outer_ins_index,
        a.name,
        a.activity_type,
        prog.id,
        outer_prog.id,
        pool.id,
        account_addr.id,
        tok1.id,
        tok2.id,
        a.amount_1,
        a.amount_2,
        a.token_decimal_1,
        a.token_decimal_2,
        ta_1_1.id,
        ta_1_2.id,
        ta_2_1.id,
        ta_2_2.id,
        owner1.id,
        owner2.id,
        a.fee_amount,
        fee_tok.id
    FROM JSON_TABLE(
        p_txs_json,
        '$.data[*]' COLUMNS (
            tx_hash VARCHAR(88) PATH '$.tx_hash',
            NESTED PATH '$.activities[*]' COLUMNS (
                ins_index SMALLINT PATH '$.ins_index',
                outer_ins_index SMALLINT PATH '$.outer_ins_index',
                name VARCHAR(50) PATH '$.name',
                activity_type VARCHAR(50) PATH '$.activity_type',
                program_id VARCHAR(44) PATH '$.program_id',
                outer_program_id VARCHAR(44) PATH '$.outer_program_id',
                amm_id VARCHAR(44) PATH '$.data.amm_id',
                account VARCHAR(44) PATH '$.data.account',
                token_1 VARCHAR(44) PATH '$.data.token_1',
                token_2 VARCHAR(44) PATH '$.data.token_2',
                amount_1 BIGINT UNSIGNED PATH '$.data.amount_1',
                amount_2 BIGINT UNSIGNED PATH '$.data.amount_2',
                token_decimal_1 TINYINT UNSIGNED PATH '$.data.token_decimal_1',
                token_decimal_2 TINYINT UNSIGNED PATH '$.data.token_decimal_2',
                token_account_1_1 VARCHAR(44) PATH '$.data.token_account_1_1',
                token_account_1_2 VARCHAR(44) PATH '$.data.token_account_1_2',
                token_account_2_1 VARCHAR(44) PATH '$.data.token_account_2_1',
                token_account_2_2 VARCHAR(44) PATH '$.data.token_account_2_2',
                owner_1 VARCHAR(44) PATH '$.data.owner_1',
                owner_2 VARCHAR(44) PATH '$.data.owner_2',
                fee_amount BIGINT UNSIGNED PATH '$.data.fee_ammount',
                fee_token VARCHAR(44) PATH '$.data.fee_token'
            )
        )
    ) AS a
    -- Join to tx table to get tx_id from signature
    JOIN tx ON tx.signature = a.tx_hash
    -- Only process NEW transactions
    JOIN tmp_batch_tx_signatures sig ON sig.signature = a.tx_hash AND sig.is_new = 1
    -- JOINs for lookups
    LEFT JOIN tx_address prog_addr ON prog_addr.address = a.program_id
    LEFT JOIN tx_program prog ON prog.program_address_id = prog_addr.id
    LEFT JOIN tx_address outer_prog_addr ON outer_prog_addr.address = a.outer_program_id
    LEFT JOIN tx_program outer_prog ON outer_prog.program_address_id = outer_prog_addr.id
    LEFT JOIN tx_address pool_addr ON pool_addr.address = a.amm_id
    LEFT JOIN tx_pool pool ON pool.pool_address_id = pool_addr.id
    LEFT JOIN tx_address account_addr ON account_addr.address = a.account
    LEFT JOIN tx_address tok1_addr ON tok1_addr.address = a.token_1
    LEFT JOIN tx_token tok1 ON tok1.mint_address_id = tok1_addr.id
    LEFT JOIN tx_address tok2_addr ON tok2_addr.address = a.token_2
    LEFT JOIN tx_token tok2 ON tok2.mint_address_id = tok2_addr.id
    LEFT JOIN tx_address ta_1_1 ON ta_1_1.address = a.token_account_1_1
    LEFT JOIN tx_address ta_1_2 ON ta_1_2.address = a.token_account_1_2
    LEFT JOIN tx_address ta_2_1 ON ta_2_1.address = a.token_account_2_1
    LEFT JOIN tx_address ta_2_2 ON ta_2_2.address = a.token_account_2_2
    LEFT JOIN tx_address owner1 ON owner1.address = a.owner_1
    LEFT JOIN tx_address owner2 ON owner2.address = a.owner_2
    LEFT JOIN tx_address fee_tok_addr ON fee_tok_addr.address = a.fee_token
    LEFT JOIN tx_token fee_tok ON fee_tok.mint_address_id = fee_tok_addr.id
    WHERE a.activity_type = 'ACTIVITY_TOKEN_SWAP'
      -- Filter: collect all hops OR only top-level swaps (not nested)
      AND (v_collect_all_hops OR a.ins_index = a.outer_ins_index);

    SET p_count = ROW_COUNT();
END;;

DELIMITER ;
