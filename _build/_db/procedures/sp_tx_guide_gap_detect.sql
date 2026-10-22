-- sp_tx_guide_gap_detect: Find balance discontinuities in tx_guide for a token
--
-- Detects gaps where post_balance of tx N != pre_balance of tx N+1 for the same wallet.
-- Merges both from_ and to_ sides into a unified balance timeline per wallet.
-- Returns gap windows that a Python worker can use to fetch missing signatures from Solscan.
--
-- Parameters:
--   p_token_id:             Token to analyze
--   p_wallet_batch_size:    How many wallets to process per call (e.g. 50)
--   p_max_gaps_per_wallet:  Max gap windows returned per wallet (e.g. 20)
--   p_last_address_id:      Cursor for pagination (0 = start from beginning)
--
-- Usage:
--   CALL sp_tx_guide_gap_detect(156, 50, 20, 0);       -- first batch
--   CALL sp_tx_guide_gap_detect(156, 50, 20, 12345);   -- resume from address_id 12345
--   Empty result set = all wallets scanned

DELIMITER //

DROP PROCEDURE IF EXISTS sp_tx_guide_gap_detect//

CREATE PROCEDURE sp_tx_guide_gap_detect(
    IN p_token_id BIGINT,
    IN p_wallet_batch_size INT,
    IN p_max_gaps_per_wallet INT,
    IN p_last_address_id INT UNSIGNED
)
BEGIN
    -- Defaults
    SET p_wallet_batch_size = COALESCE(p_wallet_batch_size, 50);
    SET p_max_gaps_per_wallet = COALESCE(p_max_gaps_per_wallet, 20);
    SET p_last_address_id = COALESCE(p_last_address_id, 0);

    -- ==========================================================================
    -- STEP 1: Collect candidate wallet address_ids in this batch
    -- Union from_ and to_ sides, filter to wallets only, apply cursor
    -- ==========================================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_gap_wallets;
    CREATE TEMPORARY TABLE tmp_gap_wallets (
        address_id INT UNSIGNED PRIMARY KEY
    ) ENGINE=MEMORY;

    INSERT INTO tmp_gap_wallets (address_id)
    SELECT DISTINCT address_id
    FROM (
        SELECT g.from_address_id AS address_id
        FROM tx_guide g
        WHERE g.token_id = p_token_id
          AND g.from_address_id > p_last_address_id
          AND g.from_token_post_balance IS NOT NULL
        UNION
        SELECT g.to_address_id
        FROM tx_guide g
        WHERE g.token_id = p_token_id
          AND g.to_address_id > p_last_address_id
          AND g.to_token_post_balance IS NOT NULL
    ) combined
    JOIN tx_address a ON a.id = combined.address_id
    WHERE a.address_type = 'wallet'
    ORDER BY address_id
    LIMIT p_wallet_batch_size;

    -- ==========================================================================
    -- STEP 2: Build unified balance timeline for these wallets
    -- Each row = one balance observation (pre + post) for a wallet in a tx
    -- ==========================================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_balance_events;
    CREATE TEMPORARY TABLE tmp_balance_events (
        address_id INT UNSIGNED NOT NULL,
        block_time BIGINT UNSIGNED NOT NULL,
        tx_id BIGINT NOT NULL,
        pre_balance BIGINT UNSIGNED,
        post_balance BIGINT UNSIGNED,
        PRIMARY KEY (address_id, block_time, tx_id)
    ) ENGINE=MEMORY;

    -- From-side: wallet is the sender
    INSERT INTO tmp_balance_events (address_id, block_time, tx_id, pre_balance, post_balance)
    SELECT g.from_address_id, g.block_time, g.tx_id,
           g.from_token_pre_balance, g.from_token_post_balance
    FROM tx_guide g
    JOIN tmp_gap_wallets w ON w.address_id = g.from_address_id
    WHERE g.token_id = p_token_id
      AND g.from_token_post_balance IS NOT NULL
    ON DUPLICATE KEY UPDATE
        pre_balance = LEAST(tmp_balance_events.pre_balance, VALUES(pre_balance)),
        post_balance = GREATEST(tmp_balance_events.post_balance, VALUES(post_balance));

    -- To-side: wallet is the receiver
    INSERT INTO tmp_balance_events (address_id, block_time, tx_id, pre_balance, post_balance)
    SELECT g.to_address_id, g.block_time, g.tx_id,
           g.to_token_pre_balance, g.to_token_post_balance
    FROM tx_guide g
    JOIN tmp_gap_wallets w ON w.address_id = g.to_address_id
    WHERE g.token_id = p_token_id
      AND g.to_token_post_balance IS NOT NULL
    ON DUPLICATE KEY UPDATE
        pre_balance = LEAST(tmp_balance_events.pre_balance, VALUES(pre_balance)),
        post_balance = GREATEST(tmp_balance_events.post_balance, VALUES(post_balance));

    -- ==========================================================================
    -- STEP 3: Materialize ranked timeline (MySQL can't reopen temp table in same query)
    -- ==========================================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_ranked;
    CREATE TEMPORARY TABLE tmp_ranked (
        address_id INT UNSIGNED NOT NULL,
        block_time BIGINT UNSIGNED NOT NULL,
        tx_id BIGINT NOT NULL,
        pre_balance BIGINT UNSIGNED,
        post_balance BIGINT UNSIGNED,
        rn INT UNSIGNED NOT NULL,
        PRIMARY KEY (address_id, rn)
    ) ENGINE=MEMORY;

    INSERT INTO tmp_ranked (address_id, block_time, tx_id, pre_balance, post_balance, rn)
    SELECT address_id, block_time, tx_id, pre_balance, post_balance,
           ROW_NUMBER() OVER (PARTITION BY address_id ORDER BY block_time, tx_id) AS rn
    FROM tmp_balance_events;

    -- Duplicate for self-join (MySQL can't reference same temp table twice)
    DROP TEMPORARY TABLE IF EXISTS tmp_ranked_next;
    CREATE TEMPORARY TABLE tmp_ranked_next LIKE tmp_ranked;
    INSERT INTO tmp_ranked_next SELECT * FROM tmp_ranked;

    -- ==========================================================================
    -- STEP 4: Detect gaps — compare post_balance of rank N with pre_balance of rank N+1
    -- ==========================================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_gaps;
    CREATE TEMPORARY TABLE tmp_gaps (
        address_id INT UNSIGNED NOT NULL,
        gap_start_block_time BIGINT UNSIGNED NOT NULL,
        gap_end_block_time BIGINT UNSIGNED NOT NULL,
        post_balance BIGINT UNSIGNED,
        next_pre_balance BIGINT UNSIGNED,
        balance_delta BIGINT,
        gap_rank INT UNSIGNED,
        KEY (address_id, gap_rank)
    ) ENGINE=MEMORY;

    INSERT INTO tmp_gaps (address_id, gap_start_block_time, gap_end_block_time,
                          post_balance, next_pre_balance, balance_delta, gap_rank)
    SELECT
        address_id,
        gap_start_block_time,
        gap_end_block_time,
        post_balance,
        next_pre_balance,
        balance_delta,
        ROW_NUMBER() OVER (PARTITION BY address_id ORDER BY gap_start_block_time) AS gap_rank
    FROM (
        SELECT
            cur.address_id,
            cur.block_time AS gap_start_block_time,
            nxt.block_time AS gap_end_block_time,
            cur.post_balance,
            nxt.pre_balance AS next_pre_balance,
            CAST(nxt.pre_balance AS SIGNED) - CAST(cur.post_balance AS SIGNED) AS balance_delta
        FROM tmp_ranked cur
        JOIN tmp_ranked_next nxt ON nxt.address_id = cur.address_id AND nxt.rn = cur.rn + 1
        WHERE cur.post_balance != nxt.pre_balance
    ) detected;

    -- ==========================================================================
    -- STEP 5: Return result set — capped at max_gaps_per_wallet
    -- ==========================================================================
    SELECT
        a.address AS wallet_address,
        g.address_id,
        g.gap_start_block_time,
        g.gap_end_block_time,
        FROM_UNIXTIME(g.gap_start_block_time) AS gap_start_utc,
        FROM_UNIXTIME(g.gap_end_block_time) AS gap_end_utc,
        g.post_balance,
        g.next_pre_balance,
        g.balance_delta,
        g.gap_rank
    FROM tmp_gaps g
    JOIN tx_address a ON a.id = g.address_id
    WHERE g.gap_rank <= p_max_gaps_per_wallet
    ORDER BY g.address_id, g.gap_start_block_time;

    -- Cleanup
    DROP TEMPORARY TABLE IF EXISTS tmp_gap_wallets;
    DROP TEMPORARY TABLE IF EXISTS tmp_balance_events;
    DROP TEMPORARY TABLE IF EXISTS tmp_ranked;
    DROP TEMPORARY TABLE IF EXISTS tmp_ranked_next;
    DROP TEMPORARY TABLE IF EXISTS tmp_gaps;
END //

DELIMITER ;
