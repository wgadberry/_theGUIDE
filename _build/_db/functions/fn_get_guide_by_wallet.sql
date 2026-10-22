-- fn_get_guide_by_wallet function
-- Generated from t16o_db instance

DELIMITER ;;

DROP FUNCTION IF EXISTS `fn_get_guide_by_wallet`;;

CREATE DEFINER=`root`@`%` FUNCTION `fn_get_guide_by_wallet`(p_wallet_address VARCHAR(44), p_min_tx_state BIGINT UNSIGNED) RETURNS json
    READS SQL DATA
    DETERMINISTIC
BEGIN
    RETURN (
        SELECT JSON_ARRAYAGG(
            JSON_OBJECT(
                'id', g.id,
                'tx_id', g.tx_id,
                'block_time', g.block_time,
                'from_address_id', g.from_address_id,
                'to_address_id', g.to_address_id,
                'from_token_account_id', g.from_token_account_id,
                'to_token_account_id', g.to_token_account_id,
                'token_id', g.token_id,
                'amount', g.amount,
                'decimals', g.decimals,
                'edge_type_id', g.edge_type_id,
                'source_id', g.source_id,
                'source_row_id', g.source_row_id,
                'ins_index', g.ins_index
            )
        )
        FROM tx_guide g
        JOIN tx t ON t.id = g.tx_id
        JOIN tx_address fa ON fa.id = g.from_address_id
        JOIN tx_address ta ON ta.id = g.to_address_id
        WHERE (fa.address = p_wallet_address OR ta.address = p_wallet_address)
          AND (p_min_tx_state = 0 OR CAST(t.tx_state AS UNSIGNED) >= p_min_tx_state)
          AND (p_min_tx_state = 0 OR CAST(t.tx_state AS UNSIGNED) & p_min_tx_state != 0)
    );
END;;

DELIMITER ;
