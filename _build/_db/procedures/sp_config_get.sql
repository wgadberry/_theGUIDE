-- sp_config_get stored procedure
-- Generated from t16o_db instance

DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_config_get`;;

CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_config_get`(
    IN p_config_type CHAR(64),
    IN p_config_key CHAR(64)
)
BEGIN
    SELECT config_value, value_type, default_value, version, updated_utc
    FROM config
    WHERE config_type = p_config_type AND config_key = p_config_key;
END;;

DELIMITER ;
