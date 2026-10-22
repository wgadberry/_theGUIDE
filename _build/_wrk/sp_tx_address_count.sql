DELIMITER //

DROP PROCEDURE IF EXISTS sp_tx_address_count//

CREATE PROCEDURE sp_tx_address_count(
    IN p_min_address_count INT
)
BEGIN
    -- Returns transactions with at least p_min_address_count unique addresses
    -- Counts both from_address_id and to_address_id as distinct participants

    SELECT
        a.tx_id,
        t.block_id,
        FROM_UNIXTIME(t.block_time) AS block_time_utc,
        t.signature,
        a.address_count
    FROM (
        SELECT
            tx_id,
            COUNT(DISTINCT address_id) AS address_count
        FROM (
            SELECT tx_id, from_address_id AS address_id FROM tx_guide WHERE from_address_id IS NOT NULL
            UNION ALL
            SELECT tx_id, to_address_id AS address_id FROM tx_guide WHERE to_address_id IS NOT NULL
        ) addresses
        GROUP BY tx_id
        HAVING COUNT(DISTINCT address_id) >= p_min_address_count
    ) a
    JOIN tx t ON t.id = a.tx_id
    ORDER BY a.address_count DESC;
END //

DELIMITER ;
