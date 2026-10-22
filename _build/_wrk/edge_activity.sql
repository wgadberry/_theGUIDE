-- Edge type activity over time between two addresses
-- Usage: mysql -u root -h 127.0.0.1 -P 3396 -p"rootpassword" t16o_db < edge_activity.sql

SET @addr1 = 'FwawsTFru93RHPankhcCo4WeB2tPoXSEsAGBuozwm2ze';
SET @addr2 = 'SHARKRdGLNYRZrhotqvZi3XAtT62CRGCFxmg5LJgSHC';

SELECT @id1 := id FROM tx_address WHERE address = @addr1;
SELECT @id2 := id FROM tx_address WHERE address = @addr2;

-- ============================================================
-- 1. Daily edge type summary (volume + count per day per type)
-- ============================================================
SELECT
    DATE(FROM_UNIXTIME(g.block_time))                           AS tx_date,
    gt.type_code                                                AS edge_type,
    CASE WHEN g.from_address_id = @id1 THEN @addr1 ELSE @addr2 END AS `from`,
    CASE WHEN g.to_address_id   = @id1 THEN @addr1 ELSE @addr2 END AS `to`,
    COALESCE(tk.token_symbol, 'SOL')                            AS token,
    COUNT(*)                                                    AS edge_count,
    ROUND(SUM(g.amount / POW(10, COALESCE(g.decimals, 9))), 6) AS total_volume,
    MIN(FROM_UNIXTIME(g.block_time))                            AS first_seen,
    MAX(FROM_UNIXTIME(g.block_time))                            AS last_seen
FROM tx_guide g
JOIN tx_guide_type gt ON gt.id = g.edge_type_id
LEFT JOIN tx_token tk ON tk.id = g.token_id
WHERE (g.from_address_id = @id1 AND g.to_address_id = @id2)
   OR (g.from_address_id = @id2 AND g.to_address_id = @id1)
GROUP BY tx_date, gt.type_code,
    CASE WHEN g.from_address_id = @id1 THEN @addr1 ELSE @addr2 END,
    CASE WHEN g.to_address_id   = @id1 THEN @addr1 ELSE @addr2 END,
    COALESCE(tk.token_symbol, 'SOL')
ORDER BY tx_date, gt.type_code, token;

-- ============================================================
-- 2. Full edge timeline (every individual edge, chronological)
-- ============================================================
SELECT
	g.tx_id,
    tx.signature,
    FROM_UNIXTIME(g.block_time)                                 AS block_time_utc,
    gt.type_code                                                AS edge_type,
    gt.category,
    CASE WHEN g.from_address_id = @id1 THEN @addr1 ELSE @addr2 END AS from_addr,
    CASE WHEN g.to_address_id   = @id1 THEN @addr1 ELSE @addr2 END AS to_addr,
    COALESCE(tk.token_symbol, 'SOL')                            AS token,
    ROUND(g.amount / POW(10, COALESCE(g.decimals, 9)), 6)      AS amount,
    t.signature,
    g.ins_index
FROM tx_guide g
join tx on tx.id = g.tx_id
JOIN tx_guide_type gt ON gt.id = g.edge_type_id
JOIN tx t ON t.id = g.tx_id
LEFT JOIN tx_token tk ON tk.id = g.token_id
WHERE (g.from_address_id = @id1 AND g.to_address_id = @id2)
   OR (g.from_address_id = @id2 AND g.to_address_id = @id1)
ORDER BY g.block_time, g.ins_index;
