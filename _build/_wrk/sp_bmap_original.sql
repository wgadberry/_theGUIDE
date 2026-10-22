    Create Procedure: CREATE DEFINER=`root`@`%` PROCEDURE `sp_tx_bmap_get_token_state`(
    IN p_token_name VARCHAR(128),
    IN p_token_symbol VARCHAR(128),
    IN p_mint_address VARCHAR(44),
    IN p_signature VARCHAR(88),
    IN p_block_time BIGINT UNSIGNED,
    IN p_tx_limit TINYINT UNSIGNED  
)
BEGIN
    DECLARE v_token_id BIGINT;
    DECLARE v_mint_address VARCHAR(44);
    DECLARE v_token_symbol VARCHAR(128);
    DECLARE v_decimals TINYINT UNSIGNED;

