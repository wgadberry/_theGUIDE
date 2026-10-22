DROP PROCEDURE IF EXISTS sp_tx_guide_backfill_pool;
DELIMITER $$
CREATE PROCEDURE sp_tx_guide_backfill_pool(OUT p_updated INT)
BEGIN
    -- DEPRECATED: dex/pool_label no longer stored on tx_guide.
    -- sp_tx_get reads them from tx_pool via pool_id JOIN.
    -- This SP now only backfills pool_id where missing.

    UPDATE tx_guide g
    JOIN tx_pool pf ON pf.pool_address_id = g.from_address_id
    SET g.pool_id = pf.id
    WHERE g.pool_id IS NULL AND g.pool_address_id IS NOT NULL;

    SET p_updated = ROW_COUNT();

    UPDATE tx_guide g
    JOIN tx_pool pt ON pt.pool_address_id = g.to_address_id
    SET g.pool_id = pt.id
    WHERE g.pool_id IS NULL
      AND NOT EXISTS (SELECT 1 FROM tx_pool px WHERE px.pool_address_id = g.from_address_id);

    SET p_updated = p_updated + ROW_COUNT();
END$$
DELIMITER ;
