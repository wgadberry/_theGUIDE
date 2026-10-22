-- fn_tx_extract_addresses function
-- Generated from t16o_db instance

DELIMITER ;;

DROP FUNCTION IF EXISTS `fn_tx_extract_addresses`;;

CREATE DEFINER=`root`@`%` FUNCTION `fn_tx_extract_addresses`(p_json LONGTEXT) RETURNS json
    DETERMINISTIC
BEGIN
    DECLARE v_result JSON;

    
    
    SELECT JSON_ARRAYAGG(address) INTO v_result
    FROM (
        SELECT DISTINCT address FROM (
        
        SELECT source_owner AS address
        FROM JSON_TABLE(p_json, '$.data[*].transfers[*]' COLUMNS (
            source_owner VARCHAR(44) PATH '$.source_owner'
        )) AS jt WHERE source_owner IS NOT NULL

        UNION

        
        SELECT destination_owner
        FROM JSON_TABLE(p_json, '$.data[*].transfers[*]' COLUMNS (
            destination_owner VARCHAR(44) PATH '$.destination_owner'
        )) AS jt WHERE destination_owner IS NOT NULL

        UNION

        
        SELECT token_address
        FROM JSON_TABLE(p_json, '$.data[*].transfers[*]' COLUMNS (
            token_address VARCHAR(44) PATH '$.token_address'
        )) AS jt
        WHERE token_address IS NOT NULL
          AND token_address != 'So11111111111111111111111111111111111111111'

        UNION

        
        SELECT account
        FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (
            account VARCHAR(44) PATH '$.data.account'
        )) AS jt WHERE account IS NOT NULL

        UNION

        
        SELECT token_1
        FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (
            token_1 VARCHAR(44) PATH '$.data.token_1'
        )) AS jt
        WHERE token_1 IS NOT NULL
          AND token_1 != 'So11111111111111111111111111111111111111111'

        UNION

        
        SELECT token_2
        FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (
            token_2 VARCHAR(44) PATH '$.data.token_2'
        )) AS jt
        WHERE token_2 IS NOT NULL
          AND token_2 != 'So11111111111111111111111111111111111111111'

        ) AS raw_addresses
    ) AS all_addresses;

    RETURN COALESCE(v_result, JSON_ARRAY());
END;;

DELIMITER ;
