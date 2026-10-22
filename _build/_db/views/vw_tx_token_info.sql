-- vw_tx_token_info view
-- Generated from t16o_db instance

DROP VIEW IF EXISTS `vw_tx_token_info`;

CREATE OR REPLACE VIEW `vw_tx_token_info` AS
WITH token_summary AS (
    -- Single pass to get stats and ID boundaries
    SELECT 
        token_id,
        COUNT(*) AS tx_count,
        SUM(amount) AS total_volume,
        SUBSTRING_INDEX(GROUP_CONCAT(tx_id ORDER BY block_time ASC), ',', 1) AS oldest_tx_id,
        SUBSTRING_INDEX(GROUP_CONCAT(tx_id ORDER BY block_time DESC), ',', 1) AS newest_tx_id
    FROM tx_guide
    WHERE token_id IS NOT NULL
    GROUP BY token_id
)
SELECT 
    t.id AS token_id,
    mint.address AS mint_address,
    t.token_symbol,
    t.token_name,
    t.decimals,
    ts.tx_count,
    ts.total_volume,
    -- Oldest TX details
    ts.oldest_tx_id,
    tx_old.signature AS oldest_signature,
    tx_old.block_id AS oldest_block_id,
    tx_old.block_time AS oldest_block_time,
    tx_old.block_time_utc AS oldest_block_time_utc,
    -- Newest TX details
    ts.newest_tx_id,
    tx_new.signature AS newest_signature,
    tx_new.block_id AS newest_block_id,
    tx_new.block_time AS newest_block_time,
    tx_new.block_time_utc AS newest_block_time_utc,
    -- Time Difference
    TIMESTAMPDIFF(DAY, tx_old.block_time_utc, tx_new.block_time_utc) AS active_days
FROM tx_token t
INNER JOIN tx_address mint ON mint.id = t.mint_address_id 
    AND mint.address_type = 'mint' -- This prevents the duplicates
LEFT JOIN token_summary ts ON ts.token_id = t.id
LEFT JOIN tx tx_old ON tx_old.id = ts.oldest_tx_id
LEFT JOIN tx tx_new ON tx_new.id = ts.newest_tx_id;