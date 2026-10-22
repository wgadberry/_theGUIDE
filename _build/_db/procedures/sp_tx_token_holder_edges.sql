-- sp_tx_token_holder_edges
--
-- Find transfer connections between top holders of a token from tx_guide.
-- Returns aggregated edges: (from_owner, to_owner, transfer_count, total_amount)
--
-- Used by the Bubblemaps feature to draw connection lines between holder bubbles.
-- Only returns edges where BOTH from_address and to_address are in the current
-- top holder list for this token.
--
-- Parameters:
--   p_token_id: resolved token ID
--
-- Usage:
--   CALL sp_tx_token_holder_edges(42);

DELIMITER $$

DROP PROCEDURE IF EXISTS sp_tx_token_holder_edges$$

CREATE PROCEDURE sp_tx_token_holder_edges(
    IN p_token_id BIGINT UNSIGNED
)
BEGIN
    -- Aggregated edges between holders
    -- Group by (from_owner, to_owner) to collapse multiple transfers into one edge
    SELECT
        fa.address                                  AS from_address,
        fa.address_type                             AS from_address_type,
        ta.address                                  AS to_address,
        ta.address_type                             AS to_address_type,
        COUNT(*)                                    AS transfer_count,
        SUM(g.amount)                               AS total_amount_raw,
        MIN(g.decimals)                             AS decimals,
        MIN(t.block_time)                           AS first_transfer,
        MAX(t.block_time)                           AS last_transfer,
        GROUP_CONCAT(DISTINCT gt.type_code)         AS edge_types
    FROM tx_guide g
    JOIN tx t                   ON t.id  = g.tx_id
    JOIN tx_address fa          ON fa.id = g.from_address_id
    JOIN tx_address ta          ON ta.id = g.to_address_id
    JOIN tx_guide_type gt       ON gt.id = g.edge_type_id
    -- Only edges where BOTH sides are current holders
    JOIN tx_token_holder hf     ON hf.token_id = p_token_id
                               AND hf.owner_address_id = g.from_address_id
    JOIN tx_token_holder ht     ON ht.token_id = p_token_id
                               AND ht.owner_address_id = g.to_address_id
    WHERE g.token_id = p_token_id
      AND g.from_address_id != g.to_address_id  -- no self-loops
    GROUP BY fa.address, fa.address_type, ta.address, ta.address_type
    ORDER BY transfer_count DESC;
END$$

DELIMITER ;
