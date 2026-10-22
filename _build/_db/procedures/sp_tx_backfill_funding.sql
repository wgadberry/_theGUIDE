-- sp_tx_backfill_funding stored procedure
-- Backfills funding wallet data from tx_guide SOL transfer edges.
-- Finds the first SOL transfer TO each wallet and records the funder.

DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_tx_backfill_funding`;;

CREATE DEFINER=`root`@`%` PROCEDURE `sp_tx_backfill_funding`(
    IN p_batch_size INT,
    OUT p_updated_count INT
)
BEGIN
    -- All SOL transfers in tx_guide use the wrapped SOL mint (So111...111).
    -- token_id is NOT NULL DEFAULT 0, so IS NULL never matches; we only
    -- need the explicit mint address check.
    UPDATE tx_address a
    INNER JOIN (
        SELECT
            g.to_address_id,
            g.from_address_id AS funder_id,
            g.tx_id,
            g.amount,
            g.block_time,
            ROW_NUMBER() OVER (PARTITION BY g.to_address_id ORDER BY g.block_time ASC, g.id ASC) as rn
        FROM tx_guide g
        INNER JOIN tx_token t ON g.token_id = t.id
        INNER JOIN tx_address mint ON t.mint_address_id = mint.id
        WHERE mint.address LIKE 'So1111111111111111111111111111111111111111%'
          AND g.amount > 0
    ) first_funding ON a.id = first_funding.to_address_id AND first_funding.rn = 1
    -- Ensure funder exists in tx_address (FK safety)
    INNER JOIN tx_address funder ON funder.id = first_funding.funder_id
    SET
        a.funded_by_address_id = first_funding.funder_id,
        a.funding_tx_id = first_funding.tx_id,
        a.funding_amount = first_funding.amount,
        a.first_seen_block_time = COALESCE(a.first_seen_block_time, first_funding.block_time)
    WHERE a.funded_by_address_id IS NULL
      AND (a.address_type IN ('wallet', 'unknown') OR a.address_type IS NULL);

    SET p_updated_count = ROW_COUNT();

    -- Also update first_seen_block_time for addresses that have outbound tx but no inbound
    UPDATE tx_address a
    INNER JOIN (
        SELECT
            g.from_address_id,
            MIN(g.block_time) as first_block_time
        FROM tx_guide g
        GROUP BY g.from_address_id
    ) first_out ON a.id = first_out.from_address_id
    SET a.first_seen_block_time = first_out.first_block_time
    WHERE a.first_seen_block_time IS NULL;

END;;

DELIMITER ;
