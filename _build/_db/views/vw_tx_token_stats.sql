CREATE OR REPLACE VIEW `vw_tx_token_stats` AS
SELECT 
    t.id AS token_id,
    a.address AS mint_address,
    t.token_symbol,
    t.token_name,
    t.decimals,
    -- Aggregated Stats
    COUNT(DISTINCT g.tx_id) AS tx_count,
    COUNT(g.id) AS edge_count,
    SUM(g.amount) AS total_volume,
    COUNT(DISTINCT g.from_address_id) AS unique_senders,
    COUNT(DISTINCT g.to_address_id) AS unique_receivers,
    -- Time-based Stats
    MIN(g.block_time) AS first_guide_activity,
    MAX(g.block_time) AS last_guide_activity,
    FROM_UNIXTIME(MIN(g.block_time)) AS first_guide_activity_utc,
    FROM_UNIXTIME(MAX(g.block_time)) AS last_guide_activity_utc,
    -- Getting specific IDs (using a trick to avoid more subqueries)
    SUBSTRING_INDEX(GROUP_CONCAT(g.tx_id ORDER BY g.block_time ASC), ',', 1) AS first_tx_id,
    SUBSTRING_INDEX(GROUP_CONCAT(g.tx_id ORDER BY g.block_time DESC), ',', 1) AS last_tx_id
FROM tx_token t
JOIN tx_address a ON a.id = t.mint_address_id
LEFT JOIN tx_guide g ON g.token_id = t.id
GROUP BY t.id, a.address;