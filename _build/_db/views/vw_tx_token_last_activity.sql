CREATE OR REPLACE VIEW `vw_tx_token_last_activity` AS
SELECT 
    t.id AS token_id,
    a.address AS mint_address,
    t.token_symbol,
    t.token_name,
    t.decimals,
    FROM_UNIXTIME(g_stats.last_timestamp) AS last_guide_activity_utc
FROM tx_token t
JOIN tx_address a ON a.id = t.mint_address_id
JOIN (
    -- Single table aggregation is significantly faster
    SELECT 
        token_id, 
        MAX(block_time) AS last_timestamp
    FROM tx_guide
    WHERE token_id IS NOT NULL
    GROUP BY token_id
) g_stats ON g_stats.token_id = t.id;