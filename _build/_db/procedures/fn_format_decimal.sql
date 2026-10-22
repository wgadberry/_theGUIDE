-- fn_format_decimal: Format raw balance/amount to human-readable decimal string
-- Trims trailing zeros: 1.500000 → '1.5', 0.000000001 → '0.000000001'
-- Used by sp_tx_get to avoid repeating the 15-line CASE/TRIM pattern

DELIMITER ;;

DROP FUNCTION IF EXISTS fn_format_decimal;;

CREATE DEFINER=`root`@`%` FUNCTION fn_format_decimal(
    p_raw DECIMAL(38,0),
    p_decimals TINYINT UNSIGNED
) RETURNS VARCHAR(60)
DETERMINISTIC NO SQL
BEGIN
    DECLARE v_val VARCHAR(60);

    IF p_raw IS NULL THEN RETURN NULL; END IF;

    SET v_val = CAST(ROUND(p_raw / POW(10, COALESCE(p_decimals, 9)), 18) AS CHAR);

    IF LOCATE('.', v_val) > 0 THEN
        SET v_val = TRIM(TRAILING '0' FROM v_val);
        SET v_val = TRIM(TRAILING '.' FROM v_val);
    END IF;

    RETURN COALESCE(NULLIF(v_val, ''), '0');
END;;

DELIMITER ;
