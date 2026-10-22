-- fn_tx_state_helpers.sql: Helper functions for type_state bitmask operations
--
-- Functions:
--   fn_tx_state_set(current, phase_bit)   - Sets a bit, returns new value
--   fn_tx_state_clear(current, phase_bit) - Clears a bit, returns new value
--   fn_tx_state_has(current, phase_bit)   - Returns 1 if bit is set, 0 otherwise
--   fn_tx_state_missing(current, phase_bit) - Returns 1 if bit is NOT set
--   fn_tx_state_get_phase(phase_code)     - Returns bit_value for a phase code

-- =============================================================================
-- SET a phase bit
-- =============================================================================
DROP FUNCTION IF EXISTS fn_tx_state_set;
DELIMITER //
CREATE FUNCTION fn_tx_state_set(
    p_current BIGINT UNSIGNED,
    p_phase_bit BIGINT UNSIGNED
) RETURNS BIGINT UNSIGNED
DETERMINISTIC
BEGIN
    RETURN COALESCE(p_current, 0) | p_phase_bit;
END //
DELIMITER ;

-- =============================================================================
-- CLEAR a phase bit
-- =============================================================================
DROP FUNCTION IF EXISTS fn_tx_state_clear;
DELIMITER //
CREATE FUNCTION fn_tx_state_clear(
    p_current BIGINT UNSIGNED,
    p_phase_bit BIGINT UNSIGNED
) RETURNS BIGINT UNSIGNED
DETERMINISTIC
BEGIN
    RETURN COALESCE(p_current, 0) & ~p_phase_bit;
END //
DELIMITER ;

-- =============================================================================
-- CHECK if a phase bit is set (returns 1 or 0)
-- =============================================================================
DROP FUNCTION IF EXISTS fn_tx_state_has;
DELIMITER //
CREATE FUNCTION fn_tx_state_has(
    p_current BIGINT UNSIGNED,
    p_phase_bit BIGINT UNSIGNED
) RETURNS TINYINT
DETERMINISTIC
BEGIN
    RETURN IF((COALESCE(p_current, 0) & p_phase_bit) = p_phase_bit, 1, 0);
END //
DELIMITER ;

-- =============================================================================
-- CHECK if a phase bit is MISSING (returns 1 or 0)
-- =============================================================================
DROP FUNCTION IF EXISTS fn_tx_state_missing;
DELIMITER //
CREATE FUNCTION fn_tx_state_missing(
    p_current BIGINT UNSIGNED,
    p_phase_bit BIGINT UNSIGNED
) RETURNS TINYINT
DETERMINISTIC
BEGIN
    RETURN IF((COALESCE(p_current, 0) & p_phase_bit) = 0, 1, 0);
END //
DELIMITER ;

-- =============================================================================
-- GET phase bit value from phase code
-- =============================================================================
DROP FUNCTION IF EXISTS fn_tx_state_get_phase;
DELIMITER //
CREATE FUNCTION fn_tx_state_get_phase(
    p_phase_code VARCHAR(32)
) RETURNS BIGINT UNSIGNED
READS SQL DATA
BEGIN
    DECLARE v_bit_value BIGINT UNSIGNED;
    SELECT bit_value INTO v_bit_value
    FROM tx_state_phase
    WHERE phase_code = p_phase_code;
    RETURN COALESCE(v_bit_value, 0);
END //
DELIMITER ;

-- =============================================================================
-- SET multiple phases at once (pass sum of bit values)
-- =============================================================================
DROP FUNCTION IF EXISTS fn_tx_state_set_multi;
DELIMITER //
CREATE FUNCTION fn_tx_state_set_multi(
    p_current BIGINT UNSIGNED,
    p_phase_bits BIGINT UNSIGNED
) RETURNS BIGINT UNSIGNED
DETERMINISTIC
BEGIN
    RETURN COALESCE(p_current, 0) | COALESCE(p_phase_bits, 0);
END //
DELIMITER ;

-- =============================================================================
-- Constants for common composite states (for reference in code)
-- =============================================================================
-- SHREDDER_COMPLETE = 1+2+4+8+16+32 = 63
-- FULLY_PROCESSED = 2047 (all 11 bits set)
