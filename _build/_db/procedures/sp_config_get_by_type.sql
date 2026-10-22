-- sp_config_get_by_type stored procedure
-- Returns all config keys for a type as a single JSON object
-- Workers call once per supervisor cycle instead of N individual sp_config_get calls

DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_config_get_by_type`;;

CREATE DEFINER=`root`@`%` PROCEDURE `sp_config_get_by_type`(
    IN p_config_type VARCHAR(64)
)
BEGIN
    SELECT JSON_OBJECTAGG(config_key, config_value) AS config_json
    FROM config
    WHERE config_type = p_config_type;
END;;

DELIMITER ;
