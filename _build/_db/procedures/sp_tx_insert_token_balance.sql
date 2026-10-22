-- sp_tx_insert_token_balance: Batch insert token balance changes from full staging JSON
-- Uses JSON_TABLE with NESTED PATH to process all txs at once (no loop)
-- Requires caller to create tmp_detail_tx and tmp_detail_addr MEMORY temp tables
--
-- Feature flags:
--   FEATURE_BALANCE_CHANGES (1): When set, collect ALL balance changes
--                                When not set, only collect for searched addresses (by owner)

DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_tx_insert_token_balance`;;

CREATE DEFINER=`root`@`%` PROCEDURE `sp_tx_insert_token_balance`(
    IN p_txs_json JSON,
    IN p_features INT UNSIGNED,
    IN p_searched_addresses JSON,
    OUT p_count INT
)
BEGIN

    DECLARE FEATURE_BALANCE_CHANGES INT UNSIGNED DEFAULT 1;
    DECLARE v_collect_all_balances BOOLEAN;

    SET v_collect_all_balances = (p_features & FEATURE_BALANCE_CHANGES) = FEATURE_BALANCE_CHANGES;

    INSERT IGNORE INTO tx_token_balance_change (
        tx_id,
        block_time,
        token_account_address_id,
        owner_address_id,
        token_id,
        decimals,
        pre_balance,
        post_balance,
        change_amount,
        change_type
    )
    SELECT
        dt.tx_id,
        b.block_time,
        ata.address_id,
        owner.address_id,
        tok.id,
        COALESCE(b.decimals, 0),
        CAST(b.pre_balance AS DECIMAL(38,0)),
        CAST(b.post_balance AS DECIMAL(38,0)),
        CAST(b.change_amount AS DECIMAL(38,0)),
        CASE
            WHEN b.change_type IS NOT NULL THEN b.change_type
            WHEN CAST(b.change_amount AS DECIMAL(38,0)) >= 0 THEN 'inc'
            ELSE 'dec'
        END
    FROM JSON_TABLE(
        p_txs_json,
        '$.data[*]' COLUMNS (
            tx_hash VARCHAR(88) PATH '$.tx_hash',
            block_time BIGINT PATH '$.block_time',
            NESTED PATH '$.token_bal_change[*]' COLUMNS (
                address VARCHAR(44) PATH '$.address',
                owner VARCHAR(44) PATH '$.owner',
                token_address VARCHAR(44) PATH '$.token_address',
                decimals TINYINT UNSIGNED PATH '$.decimals',
                pre_balance VARCHAR(50) PATH '$.pre_balance',
                post_balance VARCHAR(50) PATH '$.post_balance',
                change_amount VARCHAR(50) PATH '$.change_amount',
                change_type VARCHAR(10) PATH '$.change_type'
            )
        )
    ) AS b
    JOIN tmp_detail_tx dt ON dt.tx_hash = b.tx_hash
    JOIN tmp_detail_addr ata ON ata.address = b.address
    JOIN tmp_detail_addr2 owner ON owner.address = b.owner
    JOIN tmp_detail_addr3 mint ON mint.address = b.token_address
    JOIN tx_token tok ON tok.mint_address_id = mint.address_id
    WHERE b.address IS NOT NULL
      AND b.owner IS NOT NULL
      AND b.token_address IS NOT NULL
      AND (v_collect_all_balances
           OR p_searched_addresses IS NULL
           OR JSON_CONTAINS(p_searched_addresses, JSON_QUOTE(b.owner)));

    SET p_count = ROW_COUNT();
END;;

DELIMITER ;
