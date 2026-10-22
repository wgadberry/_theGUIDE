-- sp_tx_token_pool_lookup
--
-- Given a JSON array of addresses, returns pool_label and dex information
-- for any addresses that match a pool in tx_pool (via pool_address_id).
--
-- Input:  p_addresses — JSON array of address strings
--         e.g. '["FwawsT...","5Q544f...","addr3"]'
--
-- Output: Result set with address, pool_label, dex, token_pair, program_name
--         Only rows where a matching pool exists are returned.
--
-- Usage:
--   CALL sp_tx_token_pool_lookup('["FwawsTFru93RHPankhcCo4WeB2tPoXSEsAGBuozwm2ze"]');

DELIMITER $$

DROP PROCEDURE IF EXISTS sp_tx_token_pool_lookup$$

CREATE PROCEDURE sp_tx_token_pool_lookup(
    IN p_addresses JSON
)
BEGIN

    -- Parse addresses into temp table
    DROP TEMPORARY TABLE IF EXISTS tmp_lookup_addrs;
    CREATE TEMPORARY TABLE tmp_lookup_addrs (
        address VARCHAR(128) NOT NULL,
        PRIMARY KEY (address)
    ) ENGINE=MEMORY;

    INSERT IGNORE INTO tmp_lookup_addrs (address)
    SELECT jt.addr
    FROM JSON_TABLE(
        p_addresses,
        '$[*]' COLUMNS (addr VARCHAR(128) PATH '$')
    ) AS jt
    WHERE jt.addr IS NOT NULL AND jt.addr != '';

    -- Join through tx_address → tx_pool → tx_program
    SELECT
        a.address,
        a.address_type,
        a.label                     AS address_label,
        p.pool_label,
        p.dex,
        p.token_pair,
        prg.name                    AS program_name,
        prg.program_type,
        pa.address                  AS program_address
    FROM tmp_lookup_addrs t
    JOIN tx_address a        ON a.address = t.address
    JOIN tx_pool p           ON p.pool_address_id = a.id
    LEFT JOIN tx_program prg ON prg.id = p.program_id
    LEFT JOIN tx_address pa  ON pa.id = prg.program_address_id;

    DROP TEMPORARY TABLE IF EXISTS tmp_lookup_addrs;

END$$

DELIMITER ;
