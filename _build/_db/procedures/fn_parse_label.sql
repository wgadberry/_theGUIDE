-- Label parsing functions for pool/lp_token labels
-- Format: "DEX (TOKEN1-TOKEN2) Pool N" or "DEX (TOKEN) LP Token"

DELIMITER ;;

-- =============================================================================
-- fn_parse_dex: Extract DEX name (everything before first '(')
-- =============================================================================
DROP FUNCTION IF EXISTS fn_parse_dex;;

CREATE DEFINER=`root`@`%` FUNCTION fn_parse_dex(label VARCHAR(500))
RETURNS VARCHAR(100) CHARSET utf8mb4
DETERMINISTIC
BEGIN
    DECLARE paren_pos INT;
    SET paren_pos = LOCATE('(', label);
    IF paren_pos = 0 THEN
        RETURN TRIM(label);
    END IF;
    RETURN TRIM(LEFT(label, paren_pos - 1));
END;;

-- =============================================================================
-- fn_parse_token_pair: Extract token pair (everything between '(' and ')')
-- =============================================================================
DROP FUNCTION IF EXISTS fn_parse_token_pair;;

CREATE DEFINER=`root`@`%` FUNCTION fn_parse_token_pair(label VARCHAR(500))
RETURNS VARCHAR(200) CHARSET utf8mb4
DETERMINISTIC
BEGIN
    DECLARE open_pos INT;
    DECLARE close_pos INT;
    SET open_pos  = LOCATE('(', label);
    SET close_pos = LOCATE(')', label);
    IF open_pos = 0 OR close_pos = 0 OR close_pos <= open_pos THEN
        RETURN NULL;
    END IF;
    RETURN TRIM(SUBSTRING(label, open_pos + 1, close_pos - open_pos - 1));
END;;

-- =============================================================================
-- fn_parse_token_a: Extract first token from pair (before '-')
-- =============================================================================
DROP FUNCTION IF EXISTS fn_parse_token_a;;

CREATE DEFINER=`root`@`%` FUNCTION fn_parse_token_a(label VARCHAR(500))
RETURNS VARCHAR(100) CHARSET utf8mb4
DETERMINISTIC
BEGIN
    DECLARE pair VARCHAR(200);
    DECLARE dash_pos INT;
    SET pair = fn_parse_token_pair(label);
    IF pair IS NULL THEN RETURN NULL; END IF;
    SET dash_pos = LOCATE('-', pair);
    IF dash_pos = 0 THEN
        -- Single token (e.g. "KOINZ")
        RETURN pair;
    END IF;
    RETURN TRIM(LEFT(pair, dash_pos - 1));
END;;

-- =============================================================================
-- fn_parse_token_b: Extract second token from pair (after '-')
-- =============================================================================
DROP FUNCTION IF EXISTS fn_parse_token_b;;

CREATE DEFINER=`root`@`%` FUNCTION fn_parse_token_b(label VARCHAR(500))
RETURNS VARCHAR(100) CHARSET utf8mb4
DETERMINISTIC
BEGIN
    DECLARE pair VARCHAR(200);
    DECLARE dash_pos INT;
    SET pair = fn_parse_token_pair(label);
    IF pair IS NULL THEN RETURN NULL; END IF;
    SET dash_pos = LOCATE('-', pair);
    IF dash_pos = 0 THEN
        -- Single token, no token B
        RETURN NULL;
    END IF;
    RETURN TRIM(SUBSTRING(pair, dash_pos + 1));
END;;

DELIMITER ;
