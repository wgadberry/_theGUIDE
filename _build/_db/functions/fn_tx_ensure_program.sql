-- fn_tx_ensure_program function
-- Generated from t16o_db instance

DELIMITER ;;

DROP FUNCTION IF EXISTS `fn_tx_ensure_program`;;

CREATE DEFINER=`root`@`%` FUNCTION `fn_tx_ensure_program`(
    p_program_address VARCHAR(44)
) RETURNS bigint unsigned
    DETERMINISTIC
BEGIN
    DECLARE v_address_id INT UNSIGNED;
    DECLARE v_program_id BIGINT UNSIGNED;

    -- NULL/empty check
    IF p_program_address IS NULL OR p_program_address = '' THEN
        RETURN NULL;
    END IF;

    -- Get/create address
    SET v_address_id = fn_tx_ensure_address(p_program_address, 'program');

    -- Try insert first
    INSERT IGNORE INTO tx_program (program_address_id)
    VALUES (v_address_id);

    IF ROW_COUNT() > 0 THEN
        RETURN LAST_INSERT_ID();
    END IF;

    SELECT id INTO v_program_id FROM tx_program WHERE program_address_id = v_address_id LIMIT 1;
    RETURN v_program_id;
END;;

DELIMITER ;
