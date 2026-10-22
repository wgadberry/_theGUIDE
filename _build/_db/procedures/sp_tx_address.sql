-- sp_tx_address
--
-- Given a JSON array of addresses, returns all known metadata from tx_address
-- plus pool info (if applicable) and funded-by wallet address.
--
-- This is a broader enrichment SP than sp_tx_token_pool_lookup:
--   - Returns address_type, label, tags for ANY known address (not just pools)
--   - Joins tx_pool for pool_label/dex when available
--   - Resolves funded_by_address_id to the funder's address string
--
-- Input:  p_addresses — JSON array of address strings
-- Output: One row per matched address with full metadata
--
-- Usage:
--   CALL sp_tx_address('["FwawsT...","5Q544f..."]');

DELIMITER $$

DROP PROCEDURE IF EXISTS sp_tx_address$$

CREATE PROCEDURE sp_tx_address(
    IN p_addresses JSON
)
BEGIN

    -- Parse addresses into temp table
    DROP TEMPORARY TABLE IF EXISTS tmp_lookup_addrs;
    CREATE TEMPORARY TABLE tmp_lookup_addrs (
        address VARCHAR(44) NOT NULL,
        PRIMARY KEY (address)
    ) ENGINE=MEMORY;

    INSERT IGNORE INTO tmp_lookup_addrs (address)
    SELECT jt.addr
    FROM JSON_TABLE(
        p_addresses,
        '$[*]' COLUMNS (addr VARCHAR(44) PATH '$')
    ) AS jt
    WHERE jt.addr IS NOT NULL AND jt.addr != '';

    -- Main lookup: tx_address + optional tx_pool + funded-by address
    SELECT
        a.address,
        a.address_type,
        a.label,
        a.account_tags,
        a.active_age_days,
        -- Funded-by: resolve to address string
        fb.address              AS funded_by,
        -- Pool info (NULL when not a pool)
        p.pool_label,
        p.dex,
        p.token_pair,
        -- Program info
        prg.name                AS program_name,
        prg.program_type
    FROM tmp_lookup_addrs t
    JOIN tx_address a            ON a.address = t.address
    LEFT JOIN tx_pool p          ON p.pool_address_id = a.id
    LEFT JOIN tx_address fb      ON fb.id = a.funded_by_address_id
    LEFT JOIN tx_program prg     ON prg.id = COALESCE(p.program_id, a.program_id);

    DROP TEMPORARY TABLE IF EXISTS tmp_lookup_addrs;

END$$

DELIMITER ;
