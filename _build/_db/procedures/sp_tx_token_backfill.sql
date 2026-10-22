DELIMITER //

DROP PROCEDURE IF EXISTS sp_tx_token_backfill//

CREATE PROCEDURE sp_tx_token_backfill(
    OUT p_updated INT
)
BEGIN
    -- ================================================================
    -- Backfill tx_token symbol/name from tx_address.label
    --
    -- LP tokens and other tokens that Solscan API doesn't cover
    -- often have parseable labels in tx_address (e.g.,
    --   "Pump.fun AMM (MARS-WSOL) LP Token" → symbol "MARS-WSOL")
    --
    -- Called from guide-enricher.py Phase 0 (before Solscan API calls)
    -- ================================================================

    SET p_updated = 0;

    UPDATE tx_token t
    JOIN tx_address a ON a.id = t.mint_address_id
    SET t.token_symbol = TRIM(fn_parse_token_pair(a.label)),
        t.token_name   = TRIM(a.label)
    WHERE (t.token_symbol IS NULL OR t.token_symbol = '')
      AND a.label IS NOT NULL
      AND fn_parse_token_pair(a.label) IS NOT NULL;

    SET p_updated = ROW_COUNT();

END //

DELIMITER ;
