-- sp_billing_stats_by_apikey: Gather billing statistics per API key
-- Returns dimension counts for correlation, requests, transactions, addresses,
-- balance changes, swaps, transfers, and activities.
-- Excludes daemon source requests.

DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_billing_stats_by_apikey`;;

CREATE DEFINER=`root`@`%` PROCEDURE `sp_billing_stats_by_apikey`(
    IN p_api_key_id INT UNSIGNED,      -- NULL for all API keys
    IN p_start_date DATETIME,          -- NULL for no start filter
    IN p_end_date DATETIME             -- NULL for no end filter
)
BEGIN
    -- ==========================================================================
    -- Billing Statistics by API Key
    -- Counts dimensions across the request chain (excluding daemon activity)
    -- ==========================================================================

    SELECT
        ak.id AS api_key_id,
        ak.name AS api_key_name,

        -- Request-level counts
        COUNT(DISTINCT rl.correlation_id) AS correlation_count,
        COUNT(rl.id) AS request_count,

        -- Request breakdown by worker
        SUM(CASE WHEN rl.target_worker = 'gateway' THEN 1 ELSE 0 END) AS gateway_requests,
        SUM(CASE WHEN rl.target_worker = 'producer' THEN 1 ELSE 0 END) AS producer_requests,
        SUM(CASE WHEN rl.target_worker = 'decoder' THEN 1 ELSE 0 END) AS decoder_requests,
        SUM(CASE WHEN rl.target_worker = 'detailer' THEN 1 ELSE 0 END) AS detailer_requests,
        SUM(CASE WHEN rl.target_worker = 'funder' THEN 1 ELSE 0 END) AS funder_requests,

        -- Request breakdown by status
        SUM(CASE WHEN rl.status = 'completed' THEN 1 ELSE 0 END) AS completed_requests,
        SUM(CASE WHEN rl.status = 'failed' THEN 1 ELSE 0 END) AS failed_requests,

        -- Resource counts (via subqueries to avoid multiplication from joins)
        (
            SELECT COUNT(*)
            FROM tx t
            WHERE t.request_log_id IN (
                SELECT rl2.id FROM tx_request_log rl2
                WHERE rl2.api_key_id = ak.id
                  AND rl2.source != 'daemon'
                  AND (p_start_date IS NULL OR rl2.created_utc >= p_start_date)
                  AND (p_end_date IS NULL OR rl2.created_utc <= p_end_date)
            )
        ) AS tx_count,

        (
            SELECT COUNT(*)
            FROM tx_address a
            WHERE a.request_log_id IN (
                SELECT rl2.id FROM tx_request_log rl2
                WHERE rl2.api_key_id = ak.id
                  AND rl2.source != 'daemon'
                  AND (p_start_date IS NULL OR rl2.created_utc >= p_start_date)
                  AND (p_end_date IS NULL OR rl2.created_utc <= p_end_date)
            )
        ) AS address_count,

        (
            SELECT COUNT(*)
            FROM tx_sol_balance_change sbc
            JOIN tx t ON t.id = sbc.tx_id
            WHERE t.request_log_id IN (
                SELECT rl2.id FROM tx_request_log rl2
                WHERE rl2.api_key_id = ak.id
                  AND rl2.source != 'daemon'
                  AND (p_start_date IS NULL OR rl2.created_utc >= p_start_date)
                  AND (p_end_date IS NULL OR rl2.created_utc <= p_end_date)
            )
        ) AS sol_balance_change_count,

        (
            SELECT COUNT(*)
            FROM tx_token_balance_change tbc
            JOIN tx t ON t.id = tbc.tx_id
            WHERE t.request_log_id IN (
                SELECT rl2.id FROM tx_request_log rl2
                WHERE rl2.api_key_id = ak.id
                  AND rl2.source != 'daemon'
                  AND (p_start_date IS NULL OR rl2.created_utc >= p_start_date)
                  AND (p_end_date IS NULL OR rl2.created_utc <= p_end_date)
            )
        ) AS token_balance_change_count,

        (
            SELECT COUNT(*)
            FROM tx_swap s
            JOIN tx t ON t.id = s.tx_id
            WHERE t.request_log_id IN (
                SELECT rl2.id FROM tx_request_log rl2
                WHERE rl2.api_key_id = ak.id
                  AND rl2.source != 'daemon'
                  AND (p_start_date IS NULL OR rl2.created_utc >= p_start_date)
                  AND (p_end_date IS NULL OR rl2.created_utc <= p_end_date)
            )
        ) AS swap_count,

        (
            SELECT COUNT(*)
            FROM tx_transfer tr
            JOIN tx t ON t.id = tr.tx_id
            WHERE t.request_log_id IN (
                SELECT rl2.id FROM tx_request_log rl2
                WHERE rl2.api_key_id = ak.id
                  AND rl2.source != 'daemon'
                  AND (p_start_date IS NULL OR rl2.created_utc >= p_start_date)
                  AND (p_end_date IS NULL OR rl2.created_utc <= p_end_date)
            )
        ) AS transfer_count,

        (
            SELECT COUNT(*)
            FROM tx_activity act
            JOIN tx t ON t.id = act.tx_id
            WHERE t.request_log_id IN (
                SELECT rl2.id FROM tx_request_log rl2
                WHERE rl2.api_key_id = ak.id
                  AND rl2.source != 'daemon'
                  AND (p_start_date IS NULL OR rl2.created_utc >= p_start_date)
                  AND (p_end_date IS NULL OR rl2.created_utc <= p_end_date)
            )
        ) AS activity_count,

        -- Time range info
        MIN(rl.created_utc) AS first_request_at,
        MAX(rl.created_utc) AS last_request_at

    FROM tx_api_key ak
    LEFT JOIN tx_request_log rl ON rl.api_key_id = ak.id
        AND rl.source != 'daemon'
        AND (p_start_date IS NULL OR rl.created_utc >= p_start_date)
        AND (p_end_date IS NULL OR rl.created_utc <= p_end_date)
    WHERE (p_api_key_id IS NULL OR ak.id = p_api_key_id)
    GROUP BY ak.id, ak.name
    HAVING request_count > 0
    ORDER BY request_count DESC;

END;;

DELIMITER ;
