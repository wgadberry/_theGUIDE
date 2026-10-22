-- sp_tx_insert_sol_balance: Batch insert SOL balance changes from full staging JSON
-- Uses JSON_TABLE with NESTED PATH to process all txs at once (no loop)
-- Requires caller to create tmp_detail_tx and tmp_detail_addr MEMORY temp tables
--
-- Feature flags:
--   FEATURE_BALANCE_CHANGES (1): When set, collect ALL balance changes
--                                When not set, only collect for searched addresses

DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_tx_insert_sol_balance`;;

CREATE DEFINER=`root`@`%` PROCEDURE `sp_tx_insert_sol_balance`(
    IN p_txs_json JSON,
    IN p_features INT UNSIGNED,
    IN p_searched_addresses JSON,
    OUT p_count INT
)
BEGIN
    -- Feature flag constant
    DECLARE FEATURE_BALANCE_CHANGES INT UNSIGNED DEFAULT 1;
    DECLARE v_collect_all_balances BOOLEAN;

    SET v_collect_all_balances = (p_features & FEATURE_BALANCE_CHANGES) = FEATURE_BALANCE_CHANGES;

    INSERT IGNORE INTO tx_sol_balance_change (
        tx_id,
        block_time,
        address_id,
        pre_balance,
        post_balance,
        change_amount
    )
    SELECT
        dt.tx_id,
        b.block_time,
        da.address_id,
        CAST(b.pre_balance AS UNSIGNED),
        CAST(b.post_balance AS UNSIGNED),
        CAST(b.change_amount AS SIGNED)
    FROM JSON_TABLE(
        p_txs_json,
        '$.data[*]' COLUMNS (
            tx_hash VARCHAR(88) PATH '$.tx_hash',
            block_time BIGINT PATH '$.block_time',
            NESTED PATH '$.sol_bal_change[*]' COLUMNS (
                address VARCHAR(44) PATH '$.address',
                pre_balance VARCHAR(30) PATH '$.pre_balance',
                post_balance VARCHAR(30) PATH '$.post_balance',
                change_amount VARCHAR(30) PATH '$.change_amount'
            )
        )
    ) AS b
    JOIN tmp_detail_tx dt ON dt.tx_hash = b.tx_hash
    JOIN tmp_detail_addr da ON da.address = b.address
    WHERE b.address IS NOT NULL
      AND (v_collect_all_balances
           OR p_searched_addresses IS NULL
           OR JSON_CONTAINS(p_searched_addresses, JSON_QUOTE(b.address)));

    SET p_count = ROW_COUNT();
END;;

DELIMITER ;
