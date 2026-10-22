-- fn_tx_ensure_token function
-- Generated from t16o_db instance

DELIMITER ;;

DROP FUNCTION IF EXISTS `fn_tx_ensure_token`;;

CREATE DEFINER=`root`@`%` FUNCTION `fn_tx_ensure_token`(
    p_mint_address VARCHAR(44)
) RETURNS bigint
    DETERMINISTIC
BEGIN
    DECLARE v_address_id INT UNSIGNED;
    DECLARE v_token_id BIGINT;

    -- NULL/empty check
    IF p_mint_address IS NULL OR p_mint_address = '' THEN
        RETURN NULL;
    END IF;

    -- Get/create address
    SET v_address_id = fn_tx_ensure_address(p_mint_address, 'mint');

    -- Try insert first
    INSERT IGNORE INTO tx_token (mint_address_id)
    VALUES (v_address_id);

    IF ROW_COUNT() > 0 THEN
        RETURN LAST_INSERT_ID();
    END IF;

    SELECT id INTO v_token_id FROM tx_token WHERE mint_address_id = v_address_id LIMIT 1;
    RETURN v_token_id;
END;;

DELIMITER ;
