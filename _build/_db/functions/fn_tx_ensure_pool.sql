-- fn_tx_ensure_pool function
-- Generated from t16o_db instance

DELIMITER ;;

DROP FUNCTION IF EXISTS `fn_tx_ensure_pool`;;

CREATE DEFINER=`root`@`%` FUNCTION `fn_tx_ensure_pool`(
    p_pool_address VARCHAR(44)
) RETURNS bigint unsigned
    DETERMINISTIC
BEGIN
    DECLARE v_address_id INT UNSIGNED;
    DECLARE v_pool_id BIGINT UNSIGNED;

    -- NULL/empty check
    IF p_pool_address IS NULL OR p_pool_address = '' THEN
        RETURN NULL;
    END IF;

    -- Get/create pool address
    SET v_address_id = fn_tx_ensure_address(p_pool_address, 'pool');

    -- Try insert first
    INSERT IGNORE INTO tx_pool (pool_address_id)
    VALUES (v_address_id);

    IF ROW_COUNT() > 0 THEN
        RETURN LAST_INSERT_ID();
    END IF;

    SELECT id INTO v_pool_id FROM tx_pool WHERE pool_address_id = v_address_id LIMIT 1;
    RETURN v_pool_id;
END;;

DELIMITER ;
