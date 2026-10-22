DELIMITER //

DROP PROCEDURE IF EXISTS sp_tx_token_count //

CREATE PROCEDURE sp_tx_token_count(
    IN p_limit INT
)
BEGIN
    /*
    Returns transaction count by token_name/token_symbol
    Only includes tokens where token_symbol is not null

    Parameters:
        p_limit - Max rows to return (0 or NULL = unlimited)

    Usage:
        CALL sp_tx_token_count(100);  -- Top 100 tokens by tx count
        CALL sp_tx_token_count(0);    -- All tokens

    Index: idx_token_cover(token_id, block_time, tx_id) on tx_guide
    */

	DROP TEMPORARY TABLE IF EXISTS tmp_token_id_filter;
    CREATE TEMPORARY TABLE tmp_token_id_filter (
        token_id BIGINT PRIMARY KEY
    ) ENGINE=MEMORY;
        
    SET p_limit = COALESCE(p_limit, 0);
    
    INSERT INTO tmp_token_id_filter
    SELECT t.id
    FROM tx_address a
    JOIN tx_token t on t.mint_address_id = a.id
    WHERE a.address NOT IN ('So11111111111111111111111111111111111111111', 'So11111111111111111111111111111111111111112','EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v')
	;
    
    IF p_limit > 0 THEN
        SELECT
            a.address AS mint_address,
            t.token_name,
            t.token_symbol,
            COUNT(g.id) AS tx_count,
            COUNT(DISTINCT g.tx_id) AS unique_tx_count,
            FROM_UNIXTIME(MIN(g.block_time)) AS first_seen,
            FROM_UNIXTIME(MAX(g.block_time)) AS last_seen
        FROM tx_guide g 
        INNER JOIN tmp_token_id_filter tf ON g.token_id = tf.token_id
        INNER JOIN tx_token t ON g.token_id = t.id         
        INNER JOIN tx_address a ON a.id = t.mint_address_id
        WHERE t.token_symbol IS NOT NULL AND t.token_symbol != ''
          AND g.token_id IS NOT NULL
        GROUP BY t.id, a.address -- , t.token_name, t.token_symbol
        ORDER BY unique_tx_count DESC
        LIMIT p_limit;
    ELSE
        SELECT
            a.address AS mint_address,
            t.token_name,
            t.token_symbol,
            COUNT(g.id) AS tx_count,
            COUNT(DISTINCT g.tx_id) AS unique_tx_count,
            FROM_UNIXTIME(MIN(g.block_time)) AS first_seen,
            FROM_UNIXTIME(MAX(g.block_time)) AS last_seen
        FROM tx_guide g 
        INNER JOIN tmp_token_id_filter tf ON g.token_id = tf.token_id
        INNER JOIN tx_token t ON g.token_id = t.id         
        INNER JOIN tx_address a ON a.id = t.mint_address_id
        WHERE t.token_symbol IS NOT NULL AND t.token_symbol != ''
          AND g.token_id IS NOT NULL
        GROUP BY t.id, a.address -- , t.token_name, t.token_symbol
        ORDER BY unique_tx_count DESC;
    END IF;

END //

DELIMITER ;
