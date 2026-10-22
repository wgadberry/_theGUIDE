-- sp_tx_token_holder_upsert
--
-- Bulk upsert token holders from Solscan JSON in a single call.
-- Resolves owner/ATA addresses via fn_tx_ensure_address internally.
-- Replaces 80+ individual fn_tx_ensure_token_holder calls with one SP call.
--
-- Input: p_token_id + p_holders_json (raw Solscan holder array)
-- JSON shape per item:
--   {"owner":"FwawsT...","address":"J3vQo...","amount":298459501016441,
--    "decimals":6,"percentage":29.87,"rank":1}
--
-- Steps:
--   1. Parse JSON into temp table
--   2. Ensure all owner addresses exist (bulk INSERT IGNORE)
--   3. Ensure all ATA addresses exist (bulk INSERT IGNORE)
--   4. Resolve address IDs via JOIN
--   5. Bulk upsert into tx_token_holder
--   6. Clean stale holders not in this batch
--
-- Usage:
--   CALL sp_tx_token_holder_upsert(42, '[{"owner":"FwawsT...","address":"J3vQ...",...},...]');

DELIMITER $$

DROP PROCEDURE IF EXISTS sp_tx_token_holder_upsert$$

CREATE PROCEDURE sp_tx_token_holder_upsert(
    IN p_token_id       BIGINT UNSIGNED,
    IN p_holders_json   JSON
)
BEGIN
    DECLARE v_upserted INT DEFAULT 0;
    DECLARE v_cleaned  INT DEFAULT 0;

    -- ── Step 1: Parse JSON into temp table ───────────────────────────
    DROP TEMPORARY TABLE IF EXISTS tmp_holders;
    CREATE TEMPORARY TABLE tmp_holders (
        owner_address   VARCHAR(44) NOT NULL,
        ata_address     VARCHAR(44),
        amount          DECIMAL(38,0) NOT NULL DEFAULT 0,
        decimals        TINYINT UNSIGNED NOT NULL DEFAULT 0,
        percentage      DECIMAL(10,6) DEFAULT NULL,
        `rank`          INT UNSIGNED DEFAULT NULL,
        owner_id        INT UNSIGNED DEFAULT NULL,
        ata_id          INT UNSIGNED DEFAULT NULL,
        KEY idx_owner (owner_address),
        KEY idx_ata (ata_address)
    );

    INSERT INTO tmp_holders (owner_address, ata_address, amount, decimals, percentage, `rank`)
    SELECT
        h.owner_address,
        h.ata_address,
        h.amount,
        h.decimals,
        h.percentage,
        h.`rank`
    FROM JSON_TABLE(p_holders_json, '$[*]' COLUMNS (
        owner_address   VARCHAR(44)      PATH '$.owner',
        ata_address     VARCHAR(44)      PATH '$.address',
        amount          DECIMAL(38,0)    PATH '$.amount',
        decimals        TINYINT UNSIGNED PATH '$.decimals',
        percentage      DECIMAL(10,6)    PATH '$.percentage',
        `rank`          INT UNSIGNED     PATH '$.rank'
    )) AS h
    WHERE h.owner_address IS NOT NULL AND h.owner_address != '';

    -- ── Step 2: Bulk ensure owner addresses ──────────────────────────
    INSERT IGNORE INTO tx_address (address, address_type)
    SELECT DISTINCT owner_address, 'wallet'
    FROM tmp_holders;

    -- ── Step 3: Bulk ensure ATA addresses ────────────────────────────
    INSERT IGNORE INTO tx_address (address, address_type)
    SELECT DISTINCT ata_address, 'ata'
    FROM tmp_holders
    WHERE ata_address IS NOT NULL AND ata_address != '';

    -- ── Step 4: Resolve address IDs ──────────────────────────────────
    UPDATE tmp_holders h
    JOIN tx_address a ON a.address = h.owner_address
    SET h.owner_id = a.id;

    UPDATE tmp_holders h
    JOIN tx_address a ON a.address = h.ata_address
    SET h.ata_id = a.id
    WHERE h.ata_address IS NOT NULL;

    -- ── Step 5: Bulk upsert holders ──────────────────────────────────
    INSERT INTO tx_token_holder
        (token_id, owner_address_id, token_account_id, amount, decimals, percentage, `rank`)
    SELECT
        p_token_id, h.owner_id, h.ata_id, h.amount, h.decimals, h.percentage, h.`rank`
    FROM tmp_holders h
    WHERE h.owner_id IS NOT NULL
    ON DUPLICATE KEY UPDATE
        token_account_id = VALUES(token_account_id),
        amount           = VALUES(amount),
        decimals         = VALUES(decimals),
        percentage       = VALUES(percentage),
        `rank`           = VALUES(`rank`),
        updated_utc      = CURRENT_TIMESTAMP;

    SET v_upserted = ROW_COUNT();

    -- ── Step 6: Clean stale holders ──────────────────────────────────
    -- Remove holders for this token that weren't in this batch
    DELETE FROM tx_token_holder
    WHERE token_id = p_token_id
      AND updated_utc < DATE_SUB(NOW(), INTERVAL 1 MINUTE);

    SET v_cleaned = ROW_COUNT();

    -- Return stats
    SELECT v_upserted AS upserted, v_cleaned AS cleaned;

    DROP TEMPORARY TABLE IF EXISTS tmp_holders;
END$$

DELIMITER ;
