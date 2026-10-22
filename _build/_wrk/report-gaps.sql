-- report-gaps.sql
-- Shows gap addresses with time ranges for reprocessing via producer
--
-- A gap exists when boundary_sig from session A doesn't match any
-- latest_fetched_sig for that address. The gap spans from
-- boundary_block_time (where session A stopped) to the next session's
-- latest_fetched_block_time (where the next session picked up).

SELECT
    a.address,
    COUNT(*) AS gap_count,
    MIN(FROM_UNIXTIME(b.boundary_block_time)) AS gap_earliest_utc,
    MAX(
        COALESCE(
            FROM_UNIXTIME(next_session.latest_fetched_block_time),
            NOW()
        )
    ) AS gap_latest_utc,
    MIN(b.boundary_block_time) AS gap_earliest_bt,
    MAX(COALESCE(next_session.latest_fetched_block_time, UNIX_TIMESTAMP())) AS gap_latest_bt,
    GROUP_CONCAT(
        CONCAT(
            FROM_UNIXTIME(b.boundary_block_time), ' → ',
            COALESCE(FROM_UNIXTIME(next_session.latest_fetched_block_time), 'NOW')
        )
        ORDER BY b.boundary_block_time
        SEPARATOR ' | '
    ) AS gap_ranges
FROM tx_boundary b
JOIN tx_address a ON a.id = b.address_id
-- Find the next session that starts after this boundary (closest in time)
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
  -- No coverage by another session's REQUESTED range
  AND NOT EXISTS (
    SELECT 1 FROM tx_boundary b2
    WHERE b2.address_id = b.address_id
      AND b2.id != b.id
      AND COALESCE(b2.request_until_block_time, b2.oldest_fetched_block_time) <= b.boundary_block_time
      AND COALESCE(b2.request_before_block_time, b2.latest_fetched_block_time) >= b.boundary_block_time
  )
GROUP BY b.address_id, a.address
ORDER BY gap_count DESC, a.address;

-- Large gaps: any individual gap spanning more than 24 hours
-- These may indicate missed fetch sessions or long outages

SELECT '=== LARGE GAPS (> 24 hours) ===' AS report;

SELECT
    b.id AS boundary_id,
    a.address,
    FROM_UNIXTIME(b.boundary_block_time) AS gap_start_utc,
    COALESCE(
        FROM_UNIXTIME(next_session.latest_fetched_block_time),
        NOW()
    ) AS gap_end_utc,
    ROUND(
        (COALESCE(next_session.latest_fetched_block_time, UNIX_TIMESTAMP()) - b.boundary_block_time) / 86400.0,
        1
    ) AS gap_days,
    b.boundary_sig,
    next_session.latest_fetched_sig AS next_session_start_sig
FROM tx_boundary b
JOIN tx_address a ON a.id = b.address_id
LEFT JOIN LATERAL (
    SELECT b2.latest_fetched_sig, b2.latest_fetched_block_time
    FROM tx_boundary b2
    WHERE b2.address_id = b.address_id
      AND b2.latest_fetched_block_time > b.boundary_block_time
    ORDER BY b2.latest_fetched_block_time ASC
    LIMIT 1
) next_session ON TRUE
WHERE b.boundary_sig IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM tx_boundary b2
    WHERE b2.address_id = b.address_id
      AND b2.latest_fetched_sig = b.boundary_sig
  )
  AND NOT EXISTS (
    SELECT 1 FROM tx_boundary b2
    WHERE b2.address_id = b.address_id
      AND b2.id != b.id
      AND b2.oldest_fetched_block_time <= b.boundary_block_time
      AND b2.latest_fetched_block_time >= b.boundary_block_time
  )
  AND (COALESCE(next_session.latest_fetched_block_time, UNIX_TIMESTAMP()) - b.boundary_block_time) > 86400
ORDER BY a.address, gap_days DESC;
