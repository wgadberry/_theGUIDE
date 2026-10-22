-- Helper functions for ensuring entities exist (address, program, pool, token)
-- These are used by sp_tx_insert_* procedures

DELIMITER ;;

-- =============================================================================
-- fn_tx_ensure_address: Ensures address exists, creates if not
-- =============================================================================
DROP FUNCTION IF EXISTS `fn_tx_ensure_address`;;

CREATE DEFINER=`root`@`%` FUNCTION `fn_tx_ensure_address`(
    p_address VARCHAR(44),
    p_type ENUM('program','pool','mint','vault','wallet','ata','unknown')
) RETURNS INT UNSIGNED
DETERMINISTIC
BEGIN
    DECLARE v_id INT UNSIGNED;

    IF p_address IS NULL OR p_address = '' THEN
        RETURN NULL;
    END IF;

    SELECT id INTO v_id FROM tx_address WHERE address = p_address LIMIT 1;

    IF v_id IS NOT NULL THEN
        RETURN v_id;
    END IF;

    INSERT IGNORE INTO tx_address (address, address_type)
    VALUES (p_address, COALESCE(p_type, 'unknown'));

    SELECT id INTO v_id FROM tx_address WHERE address = p_address LIMIT 1;

    RETURN v_id;
END;;

-- =============================================================================
-- fn_tx_ensure_program: Ensures program exists, creates if not
-- =============================================================================
DROP FUNCTION IF EXISTS `fn_tx_ensure_program`;;

CREATE DEFINER=`root`@`%` FUNCTION `fn_tx_ensure_program`(
    p_program_address VARCHAR(44)
) RETURNS BIGINT UNSIGNED
DETERMINISTIC
BEGIN
    DECLARE v_program_id BIGINT UNSIGNED;
    DECLARE v_address_id INT UNSIGNED;

    IF p_program_address IS NULL OR p_program_address = '' THEN
        RETURN NULL;
    END IF;

    SET v_address_id = fn_tx_ensure_address(p_program_address, 'program');

    SELECT id INTO v_program_id FROM tx_program WHERE program_address_id = v_address_id LIMIT 1;

    IF v_program_id IS NOT NULL THEN
        RETURN v_program_id;
    END IF;

    INSERT IGNORE INTO tx_program (program_address_id) VALUES (v_address_id);

    SELECT id INTO v_program_id FROM tx_program WHERE program_address_id = v_address_id LIMIT 1;

    RETURN v_program_id;
END;;

-- =============================================================================
-- fn_tx_ensure_pool: Ensures pool exists, creates if not
-- =============================================================================
DROP FUNCTION IF EXISTS `fn_tx_ensure_pool`;;

CREATE DEFINER=`root`@`%` FUNCTION `fn_tx_ensure_pool`(
    p_pool_address VARCHAR(44)
) RETURNS BIGINT UNSIGNED
DETERMINISTIC
BEGIN
    DECLARE v_pool_id BIGINT UNSIGNED;
    DECLARE v_address_id INT UNSIGNED;

    IF p_pool_address IS NULL OR p_pool_address = '' THEN
        RETURN NULL;
    END IF;

    SET v_address_id = fn_tx_ensure_address(p_pool_address, 'pool');

    SELECT id INTO v_pool_id FROM tx_pool WHERE pool_address_id = v_address_id LIMIT 1;

    IF v_pool_id IS NOT NULL THEN
        RETURN v_pool_id;
    END IF;

    INSERT IGNORE INTO tx_pool (pool_address_id) VALUES (v_address_id);

    SELECT id INTO v_pool_id FROM tx_pool WHERE pool_address_id = v_address_id LIMIT 1;

    RETURN v_pool_id;
END;;

-- =============================================================================
-- fn_tx_ensure_token: Ensures token exists, creates if not
-- =============================================================================
DROP FUNCTION IF EXISTS `fn_tx_ensure_token`;;

CREATE DEFINER=`root`@`%` FUNCTION `fn_tx_ensure_token`(
    p_mint_address VARCHAR(44)
) RETURNS BIGINT
DETERMINISTIC
BEGIN
    DECLARE v_token_id BIGINT;
    DECLARE v_address_id INT UNSIGNED;

    IF p_mint_address IS NULL OR p_mint_address = '' THEN
        RETURN NULL;
    END IF;

    SET v_address_id = fn_tx_ensure_address(p_mint_address, 'mint');

    SELECT id INTO v_token_id FROM tx_token WHERE mint_address_id = v_address_id LIMIT 1;

    IF v_token_id IS NOT NULL THEN
        RETURN v_token_id;
    END IF;

    INSERT IGNORE INTO tx_token (mint_address_id) VALUES (v_address_id);

    SELECT id INTO v_token_id FROM tx_token WHERE mint_address_id = v_address_id LIMIT 1;

    RETURN v_token_id;
END;;

DELIMITER ;
