-- sp_config_set stored procedure
-- Generated from t16o_db instance

DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_config_set`;;

CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_config_set`(
    IN p_config_type VARCHAR(64),
    IN p_config_key VARCHAR(64),
    IN p_config_value VARCHAR(1024),
    IN p_updated_by VARCHAR(64)
)
BEGIN
    DECLARE v_is_runtime_editable TINYINT;
    DECLARE v_current_version INT UNSIGNED;

    SELECT is_runtime_editable, version
    INTO v_is_runtime_editable, v_current_version
    FROM config
    WHERE config_type = p_config_type AND config_key = p_config_key;

    IF v_is_runtime_editable IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Configuration key not found';
    ELSEIF v_is_runtime_editable = 0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Configuration is not runtime editable';
    ELSE
        UPDATE config
        SET config_value = p_config_value,
            version = v_current_version + 1,
            updated_by = p_updated_by
        WHERE config_type = p_config_type AND config_key = p_config_key;

        SELECT config_type, config_key, config_value, version, updated_utc
        FROM config
        WHERE config_type = p_config_type AND config_key = p_config_key;
    END IF;
END;;

DELIMITER ;
