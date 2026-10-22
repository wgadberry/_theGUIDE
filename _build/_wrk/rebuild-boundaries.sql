-- Backfill tx_boundary from tx + tx_request_log
-- Groups by request_log_id to identify fetch sessions,
-- gets the newest (latest) and oldest sig per session.
--
-- Two passes:
--   1. UPDATE existing boundary rows that have NULL latest_fetched_sig
--   2. INSERT new rows for request_log_ids not yet in tx_boundary

-- Pass 1: Update existing rows missing latest_fetched_sig
UPDATE tx_boundary b
JOIN tx_request_log rl ON rl.id = b.request_log_id
JOIN LATERAL (
    SELECT signature, block_time
    FROM tx tx2
    WHERE tx2.request_log_id = b.request_log_id
    ORDER BY tx2.block_time DESC
    LIMIT 1
) latest ON TRUE
JOIN LATERAL (
    SELECT signature, block_time
    FROM tx tx3
    WHERE tx3.request_log_id = b.request_log_id
    ORDER BY tx3.block_time ASC
    LIMIT 1
) oldest ON TRUE
SET b.latest_fetched_sig = latest.signature,
    b.latest_fetched_block_time = latest.block_time,
    b.oldest_fetched_sig = oldest.signature,
    b.oldest_fetched_block_time = oldest.block_time,
    b.boundary_sig = NULL,
    b.boundary_block_time = NULL
WHERE b.latest_fetched_sig IS NULL
  AND b.request_log_id IS NOT NULL;

-- Pass 2: Insert new rows for request_log_ids not yet in tx_boundary
INSERT INTO tx_boundary
    (address_id, latest_fetched_sig, latest_fetched_block_time,
     oldest_fetched_sig, oldest_fetched_block_time,
     fetched_count, boundary_sig, boundary_block_time, boundary_loaded, request_log_id)
SELECT
    addr.id AS address_id,
    latest.signature AS latest_fetched_sig,
    latest.block_time AS latest_fetched_block_time,
    oldest.signature AS oldest_fetched_sig,
    oldest.block_time AS oldest_fetched_block_time,
    cnt.fetched_count,
    NULL AS boundary_sig,
    NULL AS boundary_block_time,
    0 AS boundary_loaded,
    t.request_log_id
FROM (
    SELECT DISTINCT request_log_id
    FROM tx
    WHERE request_log_id IS NOT NULL
      AND request_log_id NOT IN (SELECT request_log_id FROM tx_boundary WHERE request_log_id IS NOT NULL)
) t
JOIN tx_request_log rl ON rl.id = t.request_log_id
JOIN tx_address addr ON addr.address = JSON_UNQUOTE(JSON_EXTRACT(rl.payload_summary, '$.filters.addresses[0]'))
JOIN LATERAL (
    SELECT signature, block_time
    FROM tx tx2
    WHERE tx2.request_log_id = t.request_log_id
    ORDER BY tx2.block_time DESC
    LIMIT 1
) latest ON TRUE
JOIN LATERAL (
    SELECT signature, block_time
    FROM tx tx3
    WHERE tx3.request_log_id = t.request_log_id
    ORDER BY tx3.block_time ASC
    LIMIT 1
) oldest ON TRUE
JOIN LATERAL (
    SELECT COUNT(*) AS fetched_count
    FROM tx tx4
    WHERE tx4.request_log_id = t.request_log_id
) cnt ON TRUE
ORDER BY t.request_log_id;
