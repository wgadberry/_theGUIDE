-- sp_tx_insert_activities: Batch insert activities from full staging JSON
-- Uses JSON_TABLE with NESTED PATH to process all txs at once (no loop)
--
-- Feature flags:
--   FEATURE_SWAP_ROUTING (4): When set, collect ALL activity records
--                             When not set, only collect top-level activities (ins_index = outer_ins_index)

DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_tx_insert_activities`;;

CREATE DEFINER=`root`@`%` PROCEDURE `sp_tx_insert_activities`(
    IN p_txs_json JSON,
    IN p_features INT UNSIGNED,
    OUT p_count INT
)
BEGIN
    -- Feature flag constant
    DECLARE FEATURE_SWAP_ROUTING INT UNSIGNED DEFAULT 4;
    DECLARE v_collect_all_activities BOOLEAN;

    SET v_collect_all_activities = (p_features & FEATURE_SWAP_ROUTING) = FEATURE_SWAP_ROUTING;

    INSERT IGNORE INTO tx_activity (
        tx_id,
        ins_index,
        outer_ins_index,
        name,
        activity_type,
        program_id,
        outer_program_id,
        account_address_id,
        guide_loaded
    )
    SELECT
        tx.id,
        a.ins_index,
        a.outer_ins_index,
        a.name,
        a.activity_type,
        prog.id,
        outer_prog.id,
        account_addr.id,
        0
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
                account VARCHAR(44) PATH '$.data.account',
                source VARCHAR(44) PATH '$.data.source',
                new_account VARCHAR(44) PATH '$.data.new_account',
                init_account VARCHAR(44) PATH '$.data.init_account'
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
    LEFT JOIN tx_address account_addr ON account_addr.address = COALESCE(a.account, a.source, a.new_account, a.init_account)
    WHERE a.ins_index IS NOT NULL  -- Filter out txs with no activities
      -- Filter: collect all activities OR only top-level (not nested)
      AND (v_collect_all_activities OR a.ins_index = a.outer_ins_index);

    SET p_count = ROW_COUNT();
END;;

DELIMITER ;
