-- fn_tx_ensure_token_holder
--
-- Upsert a single token holder row. Returns the tx_token_holder.id.
-- Follows the fn_tx_ensure_* pattern: INSERT IGNORE, then update if exists.
--
-- Called per-holder from the gateway after resolving addresses via
-- fn_tx_ensure_address. Typically called 80 times per token refresh
-- (top 80 holders from Solscan).
--
-- Usage:
--   SELECT fn_tx_ensure_token_holder(42, 123, 456, '298459501016441', 6, 29.87, 1);

DELIMITER ;;

DROP FUNCTION IF EXISTS fn_tx_ensure_token_holder;;

CREATE FUNCTION fn_tx_ensure_token_holder(
    p_token_id          BIGINT UNSIGNED,
    p_owner_address_id  INT UNSIGNED,
    p_token_account_id  INT UNSIGNED,
    p_amount            DECIMAL(38,0),
    p_decimals          TINYINT UNSIGNED,
    p_percentage        DECIMAL(10,6),
    p_rank              INT UNSIGNED
) RETURNS BIGINT UNSIGNED
    DETERMINISTIC
BEGIN
    DECLARE v_id BIGINT UNSIGNED;

    IF p_token_id IS NULL OR p_owner_address_id IS NULL THEN
        RETURN NULL;
    END IF;

    -- Try insert first
    INSERT IGNORE INTO tx_token_holder
        (token_id, owner_address_id, token_account_id, amount, decimals, percentage, `rank`)
    VALUES
        (p_token_id, p_owner_address_id, p_token_account_id, p_amount, p_decimals, p_percentage, p_rank);

    IF ROW_COUNT() > 0 THEN
        RETURN LAST_INSERT_ID();
    END IF;

    -- Exists — update and return existing ID
    UPDATE tx_token_holder
    SET token_account_id = COALESCE(p_token_account_id, token_account_id),
        amount           = p_amount,
        decimals         = p_decimals,
        percentage       = p_percentage,
        `rank`           = p_rank,
        updated_utc      = CURRENT_TIMESTAMP
    WHERE token_id = p_token_id
      AND owner_address_id = p_owner_address_id;

    SELECT id INTO v_id
    FROM tx_token_holder
    WHERE token_id = p_token_id
      AND owner_address_id = p_owner_address_id
    LIMIT 1;

    RETURN v_id;
END;;

DELIMITER ;
