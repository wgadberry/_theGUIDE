-- fn_tx_ensure_address function
-- Generated from t16o_db instance

DELIMITER ;;

DROP FUNCTION IF EXISTS `fn_tx_ensure_address`;;

CREATE DEFINER=`root`@`%` FUNCTION `fn_tx_ensure_address`(
    p_address VARCHAR(44),
    p_address_type ENUM('program','pool','mint','vault','wallet','ata','unknown')
) RETURNS int unsigned
    DETERMINISTIC
BEGIN
    DECLARE v_id INT UNSIGNED;

    -- NULL/empty check
    IF p_address IS NULL OR p_address = '' THEN
        RETURN NULL;
    END IF;

    -- Try insert first (releases lock quickly on duplicate)
    INSERT IGNORE INTO tx_address (address, address_type)
    VALUES (p_address, COALESCE(p_address_type, 'unknown'));

    -- If inserted, return new ID; if duplicate, fetch existing
    IF ROW_COUNT() > 0 THEN
        RETURN LAST_INSERT_ID();
    END IF;

    SELECT id INTO v_id FROM tx_address WHERE address = p_address LIMIT 1;
    RETURN v_id;
END;;

DELIMITER ;
