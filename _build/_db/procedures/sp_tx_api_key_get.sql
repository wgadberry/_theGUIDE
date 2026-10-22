-- sp_tx_api_key_get: Retrieve API key record by api_key OR client credentials
--
-- Usage:
--   CALL sp_tx_api_key_get('my_api_key', NULL, NULL);          -- lookup by api_key
--   CALL sp_tx_api_key_get(NULL, 'demo', 'a3f1b2c4-...');     -- lookup by client_id + client_secret
--   CALL sp_tx_api_key_get('key', 'demo', 'secret');           -- api_key takes precedence

DROP PROCEDURE IF EXISTS sp_tx_api_key_get;

DELIMITER $$

CREATE PROCEDURE sp_tx_api_key_get(
    IN p_api_key        VARCHAR(64),
    IN p_client_id      VARCHAR(64),
    IN p_client_secret  VARCHAR(64)
)
BEGIN
    IF p_api_key IS NOT NULL THEN
        SELECT id, api_key, client_id, client_secret, name, description,
               permissions, rate_limit, active, created_utc, last_used_at
        FROM tx_api_key
        WHERE api_key = p_api_key AND active = 1;
    ELSEIF p_client_id IS NOT NULL AND p_client_secret IS NOT NULL THEN
        SELECT id, api_key, client_id, client_secret, name, description,
               permissions, rate_limit, active, created_utc, last_used_at
        FROM tx_api_key
        WHERE client_id = p_client_id AND client_secret = p_client_secret AND active = 1;
    END IF;
END$$

DELIMITER ;
