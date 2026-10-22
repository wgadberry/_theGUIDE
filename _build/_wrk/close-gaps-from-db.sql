-- close-gaps-from-db.sql
-- Close tx_boundary gaps using data already in the DB (no RPC needed).
-- For each unchained boundary, check if tx_guide has coverage in the gap range.
-- If so, create a bridging boundary row from the existing data.

-- Step 1: Find gaps and check for existing DB coverage
DROP TEMPORARY TABLE IF EXISTS tmp_gaps;
CREATE TEMPORARY TABLE tmp_gaps (
    gap_id INT AUTO_INCREMENT PRIMARY KEY,
    boundary_id BIGINT UNSIGNED,
    address_id INT UNSIGNED,
    boundary_block_time BIGINT UNSIGNED,
    next_latest_block_time BIGINT UNSIGNED,
    -- Coverage from existing DB data in the gap range
    db_newest_sig VARCHAR(88),
    db_newest_bt BIGINT UNSIGNED,
    db_oldest_sig VARCHAR(88),
    db_oldest_bt BIGINT UNSIGNED,
    db_tx_count INT UNSIGNED DEFAULT 0
) ENGINE=MEMORY;

INSERT INTO tmp_gaps (boundary_id, address_id, boundary_block_time, next_latest_block_time)
SELECT
    b.id,
    b.address_id,
    b.boundary_block_time,
    COALESCE(next_session.latest_fetched_block_time, UNIX_TIMESTAMP())
FROM tx_boundary b
LEFT JOIN LATERAL (
    SELECT b2.latest_fetched_block_time
    FROM tx_boundary b2
    WHERE b2.address_id = b.address_id
      AND b2.latest_fetched_block_time > b.boundary_block_time
    ORDER BY b2.latest_fetched_block_time ASC
    LIMIT 1
) next_session ON TRUE
WHERE b.boundary_sig IS NOT NULL
  -- No exact sig chain
  AND NOT EXISTS (
    SELECT 1 FROM tx_boundary b2
    WHERE b2.address_id = b.address_id
      AND b2.latest_fetched_sig = b.boundary_sig
  )
  -- No time-based coverage (within 60s)
  AND NOT EXISTS (
    SELECT 1 FROM tx_boundary b2
    WHERE b2.address_id = b.address_id
      AND b2.id != b.id
      AND b2.oldest_fetched_block_time <= b.boundary_block_time + 60
      AND b2.latest_fetched_block_time >= b.boundary_block_time - 60
  )
  -- Only gaps <= 24 hours
  AND (COALESCE(next_session.latest_fetched_block_time, UNIX_TIMESTAMP()) - b.boundary_block_time) <= 86400;

-- Step 2: For each gap, find the newest tx in the gap range from tx_guide
UPDATE tmp_gaps g
JOIN LATERAL (
    SELECT t.signature, t.block_time
    FROM tx_guide tg
    JOIN tx t ON t.id = tg.tx_id
    JOIN tx_token tk ON tk.id = tg.token_id
    WHERE tk.mint_address_id = g.address_id
      AND t.block_time >= g.boundary_block_time - 60
      AND t.block_time <= g.next_latest_block_time + 60
    ORDER BY t.block_time DESC
    LIMIT 1
) newest ON TRUE
SET g.db_newest_sig = newest.signature,
    g.db_newest_bt = newest.block_time;

-- Step 3: Find the oldest tx in the gap range
UPDATE tmp_gaps g
JOIN LATERAL (
    SELECT t.signature, t.block_time
    FROM tx_guide tg
    JOIN tx t ON t.id = tg.tx_id
    JOIN tx_token tk ON tk.id = tg.token_id
    WHERE tk.mint_address_id = g.address_id
      AND t.block_time >= g.boundary_block_time - 60
      AND t.block_time <= g.next_latest_block_time + 60
    ORDER BY t.block_time ASC
    LIMIT 1
) oldest ON TRUE
SET g.db_oldest_sig = oldest.signature,
    g.db_oldest_bt = oldest.block_time;

-- Step 4: Count txs in gap range
UPDATE tmp_gaps g
SET g.db_tx_count = (
    SELECT COUNT(DISTINCT tg.tx_id)
    FROM tx_guide tg
    JOIN tx t ON t.id = tg.tx_id
    JOIN tx_token tk ON tk.id = tg.token_id
    WHERE tk.mint_address_id = g.address_id
      AND t.block_time >= g.boundary_block_time - 60
      AND t.block_time <= g.next_latest_block_time + 60
);

-- Report: show what we found
SELECT '=== GAP COVERAGE FROM EXISTING DB DATA ===' AS report;
SELECT
    a.address,
    g.boundary_id,
    FROM_UNIXTIME(g.boundary_block_time) AS gap_start,
    FROM_UNIXTIME(g.next_latest_block_time) AS gap_end,
    g.db_tx_count,
    LEFT(g.db_newest_sig, 20) AS db_newest_sig,
    FROM_UNIXTIME(g.db_newest_bt) AS db_newest_bt,
    LEFT(g.db_oldest_sig, 20) AS db_oldest_sig,
    FROM_UNIXTIME(g.db_oldest_bt) AS db_oldest_bt,
    IF(g.db_tx_count > 0, 'CLOSEABLE', 'REAL GAP') AS status
FROM tmp_gaps g
JOIN tx_address a ON a.id = g.address_id
ORDER BY g.db_tx_count DESC, a.address;

-- Step 5: Close gaps that have DB coverage by inserting bridging boundary rows
INSERT INTO tx_boundary
    (address_id, latest_fetched_sig, latest_fetched_block_time,
     oldest_fetched_sig, oldest_fetched_block_time,
     boundary_sig, boundary_block_time, fetched_count, boundary_loaded)
SELECT
    g.address_id,
    g.db_newest_sig,
    g.db_newest_bt,
    g.db_oldest_sig,
    g.db_oldest_bt,
    NULL,  -- no peek needed, this is a bridge
    NULL,
    g.db_tx_count,
    1      -- mark as loaded (it's from existing data)
FROM tmp_gaps g
WHERE g.db_tx_count > 0;

SELECT ROW_COUNT() AS gaps_closed;

-- Show remaining real gaps (no DB coverage)
SELECT '=== REMAINING REAL GAPS (no DB data) ===' AS report;
SELECT
    a.address,
    g.boundary_id,
    FROM_UNIXTIME(g.boundary_block_time) AS gap_start,
    FROM_UNIXTIME(g.next_latest_block_time) AS gap_end,
    ROUND((g.next_latest_block_time - g.boundary_block_time) / 3600.0, 1) AS gap_hours
FROM tmp_gaps g
JOIN tx_address a ON a.id = g.address_id
WHERE g.db_tx_count = 0
ORDER BY (g.next_latest_block_time - g.boundary_block_time) DESC;

DROP TEMPORARY TABLE IF EXISTS tmp_gaps;
