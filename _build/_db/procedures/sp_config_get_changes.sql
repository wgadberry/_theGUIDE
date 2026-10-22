-- sp_config_get_changes stored procedure
-- Generated from t16o_db instance

DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_config_get_changes`;;

CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_config_get_changes`(
    IN p_config_type VARCHAR(64),
    IN p_since_version INT UNSIGNED
)
BEGIN
    SELECT config_key, config_value, value_type, version, updated_utc
    FROM config
    WHERE config_type = p_config_type AND version > p_since_version AND is_runtime_editable = 1
    ORDER BY version;
END;;

DELIMITER ;
