-- validate-boundaries.sql
-- Audit tx_boundary coverage against tx_guide data
-- Run: mysql -u root -prootpassword t16o_db < validate-boundaries.sql

-- ============================================================
-- 1. SUMMARY: boundary coverage per address
-- ============================================================
SELECT '=== BOUNDARY COVERAGE SUMMARY ===' AS report;

SELECT
    a.address,
    COUNT(b.id) AS boundary_sessions,
    MIN(b.oldest_fetched_block_time) AS earliest_coverage,
    MAX(b.latest_fetched_block_time) AS latest_coverage,
    SUM(b.fetched_count) AS total_fetched,
    SUM(b.boundary_sig IS NULL) AS end_of_history_cnt,
    FROM_UNIXTIME(MIN(b.oldest_fetched_block_time)) AS earliest_utc,
    FROM_UNIXTIME(MAX(b.latest_fetched_block_time)) AS latest_utc
FROM tx_boundary b
JOIN tx_address a ON a.id = b.address_id
GROUP BY b.address_id, a.address
ORDER BY boundary_sessions DESC
LIMIT 50;

-- ============================================================
-- 2. GAP DETECTION: boundary_sig not chained to any latest_fetched_sig
--    These represent gaps between fetch sessions
-- ============================================================
SELECT '=== GAPS: boundary_sig with no matching latest_fetched_sig ===' AS report;

SELECT
    b.id AS boundary_id,
    a.address,
    b.oldest_fetched_sig,
    FROM_UNIXTIME(b.oldest_fetched_block_time) AS oldest_utc,
    b.boundary_sig,
    FROM_UNIXTIME(b.boundary_block_time) AS boundary_utc,
    b.latest_fetched_sig,
    FROM_UNIXTIME(b.latest_fetched_block_time) AS latest_utc,
    b.fetched_count
FROM tx_boundary b
JOIN tx_address a ON a.id = b.address_id
WHERE b.boundary_sig IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM tx_boundary b2
    WHERE b2.address_id = b.address_id
      AND b2.latest_fetched_sig = b.boundary_sig
  )
ORDER BY a.address, b.oldest_fetched_block_time;

-- ============================================================
-- 3. ORPHANED GUIDE DATA: tx_guide edges for boundary-tracked addresses
--    that fall OUTSIDE all boundary coverage windows
-- ============================================================
SELECT '=== ORPHANED GUIDE DATA: edges outside boundary coverage ===' AS report;

SELECT
    a.address,
    COUNT(DISTINCT g.tx_id) AS orphan_tx_count,
    COUNT(*) AS orphan_edge_count,
    MIN(g.block_time) AS earliest_orphan_bt,
    MAX(g.block_time) AS latest_orphan_bt,
    FROM_UNIXTIME(MIN(g.block_time)) AS earliest_orphan_utc,
    FROM_UNIXTIME(MAX(g.block_time)) AS latest_orphan_utc
FROM tx_guide g
JOIN tx_token tk ON tk.id = g.token_id
JOIN tx_address a ON a.id = tk.mint_address_id
-- Only check addresses that HAVE boundary tracking
WHERE a.id IN (SELECT DISTINCT address_id FROM tx_boundary)
-- Edge falls outside ALL boundary windows for this address
  AND NOT EXISTS (
    SELECT 1 FROM tx_boundary b
    WHERE b.address_id = a.id
      AND g.block_time >= b.oldest_fetched_block_time
      AND g.block_time <= b.latest_fetched_block_time
  )
GROUP BY a.id, a.address
HAVING orphan_edge_count > 0
ORDER BY orphan_edge_count DESC
LIMIT 50;

-- ============================================================
-- 4. MISSING BOUNDARY: addresses with tx_guide data but NO tx_boundary rows
-- ============================================================
SELECT '=== MISSING BOUNDARY: guide data with no boundary tracking ===' AS report;

SELECT
    a.address,
    COUNT(DISTINCT g.tx_id) AS tx_count,
    COUNT(*) AS edge_count,
    FROM_UNIXTIME(MIN(g.block_time)) AS earliest_utc,
    FROM_UNIXTIME(MAX(g.block_time)) AS latest_utc
FROM tx_guide g
JOIN tx_token tk ON tk.id = g.token_id
JOIN tx_address a ON a.id = tk.mint_address_id
WHERE a.id NOT IN (SELECT DISTINCT address_id FROM tx_boundary)
GROUP BY a.id, a.address
HAVING edge_count > 0
ORDER BY edge_count DESC
LIMIT 50;

-- ============================================================
-- 5. BOUNDARY HEALTH: quick stats
-- ============================================================
SELECT '=== BOUNDARY HEALTH ===' AS report;

SELECT
    (SELECT COUNT(*) FROM tx_boundary) AS total_boundary_rows,
    (SELECT COUNT(DISTINCT address_id) FROM tx_boundary) AS tracked_addresses,
    (SELECT COUNT(*) FROM tx_boundary WHERE boundary_sig IS NULL) AS end_of_history,
    (SELECT COUNT(*) FROM tx_boundary WHERE latest_fetched_sig IS NULL) AS missing_latest,
    (SELECT COUNT(*) FROM tx_boundary WHERE boundary_sig IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM tx_boundary b2
            WHERE b2.address_id = tx_boundary.address_id
              AND b2.latest_fetched_sig = tx_boundary.boundary_sig
        )
    ) AS gap_count,
    (SELECT COUNT(DISTINCT tk.mint_address_id)
     FROM tx_guide g JOIN tx_token tk ON tk.id = g.token_id
     WHERE tk.mint_address_id NOT IN (SELECT DISTINCT address_id FROM tx_boundary)
    ) AS untracked_addresses;
