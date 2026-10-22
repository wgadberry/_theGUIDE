-- sp_cluster_score
-- Aggregates relationship signals (sybil, wash, funding, frequency) into
-- weighted edge scores between wallet pairs for a given set of wallets.
-- Returns wallet pairs with combined scores for cluster inference.

DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_cluster_score`;;

CREATE PROCEDURE `sp_cluster_score`(
    IN p_wallet_addresses TEXT,       -- Comma-separated wallet addresses
    IN p_score_threshold  INT         -- Minimum combined score to include (default 50)
)
BEGIN
    DECLARE v_threshold INT;
    SET v_threshold = COALESCE(p_score_threshold, 50);

    -- ─── Parse wallet list into temp table ───────────────────────
    DROP TEMPORARY TABLE IF EXISTS tmp_wallets;
    CREATE TEMPORARY TABLE tmp_wallets (
        address_id INT UNSIGNED,
        address    VARCHAR(44),
        PRIMARY KEY (address_id),
        UNIQUE KEY uk_addr (address)
    );

    -- Split CSV into rows and resolve to tx_address IDs
    INSERT IGNORE INTO tmp_wallets (address_id, address)
    SELECT a.id, a.address
    FROM tx_address a
    WHERE FIND_IN_SET(a.address, p_wallet_addresses) > 0;

    -- ─── Collect all scored edges into a result table ────────────
    DROP TEMPORARY TABLE IF EXISTS tmp_scores;
    CREATE TEMPORARY TABLE tmp_scores (
        wallet_a VARCHAR(44),
        wallet_b VARCHAR(44),
        signal_type VARCHAR(30),
        score INT,
        detail VARCHAR(200)
    );

    -- ─── Signal 1: Same funder (direct) — +40 ──────────────────
    INSERT INTO tmp_scores (wallet_a, wallet_b, signal_type, score, detail)
    SELECT
        LEAST(w1.address, w2.address),
        GREATEST(w1.address, w2.address),
        'same_funder',
        40,
        CONCAT('funder: ', f.address)
    FROM tx_address a1
    JOIN tx_address a2 ON a1.funded_by_address_id = a2.funded_by_address_id
                      AND a1.id < a2.id
    JOIN tmp_wallets w1 ON a1.id = w1.address_id
    JOIN tmp_wallets w2 ON a2.id = w2.address_id
    JOIN tx_address f ON a1.funded_by_address_id = f.id;

    -- ─── Signal 2: Same funder (2-hop) — +25 ────────────────────
    INSERT INTO tmp_scores (wallet_a, wallet_b, signal_type, score, detail)
    SELECT
        LEAST(w1.address, w2.address),
        GREATEST(w1.address, w2.address),
        'same_funder_2hop',
        25,
        CONCAT('funder chain via: ', f1.address)
    FROM tx_address a1
    JOIN tx_address f1 ON a1.funded_by_address_id = f1.id
    JOIN tx_address a2 ON f1.id = a2.funded_by_address_id
                      AND a1.id <> a2.id
    JOIN tmp_wallets w1 ON a1.id = w1.address_id
    JOIN tmp_wallets w2 ON a2.id = w2.address_id
    WHERE NOT EXISTS (
        -- Don't double-count if already caught by direct same_funder
        SELECT 1 FROM tmp_scores s
        WHERE s.signal_type = 'same_funder'
          AND LEAST(w1.address, w2.address) = s.wallet_a
          AND GREATEST(w1.address, w2.address) = s.wallet_b
    );

    -- ─── Signal 3: High-frequency transfer pairs (5+) — +25 ────
    INSERT INTO tmp_scores (wallet_a, wallet_b, signal_type, score, detail)
    SELECT
        LEAST(a1.address, a2.address),
        GREATEST(a1.address, a2.address),
        'high_freq_pair',
        25,
        CONCAT(COUNT(*), ' transfers')
    FROM tx_guide g
    JOIN tmp_wallets w1 ON g.from_address_id = w1.address_id
    JOIN tmp_wallets w2 ON g.to_address_id = w2.address_id
    JOIN tx_address a1 ON g.from_address_id = a1.id
    JOIN tx_address a2 ON g.to_address_id = a2.id
    WHERE g.edge_type_id IN (SELECT id FROM tx_guide_type WHERE type_code IN ('spl_transfer','sol_transfer'))
      AND g.from_address_id <> g.to_address_id
    GROUP BY LEAST(a1.address, a2.address), GREATEST(a1.address, a2.address)
    HAVING COUNT(*) >= 5;

    -- ─── Signal 4: Wash roundtrip (A→B→A <1hr) — +35 ───────────
    INSERT INTO tmp_scores (wallet_a, wallet_b, signal_type, score, detail)
    SELECT DISTINCT
        LEAST(a1.address, a2.address),
        GREATEST(a1.address, a2.address),
        'wash_roundtrip',
        35,
        CONCAT('roundtrip in ', MIN(g2.block_time - g1.block_time), 's')
    FROM tx_guide g1
    JOIN tx_guide g2 ON g1.from_address_id = g2.to_address_id
                    AND g1.to_address_id = g2.from_address_id
                    AND COALESCE(g1.token_id, 0) = COALESCE(g2.token_id, 0)
                    AND g2.block_time > g1.block_time
                    AND g2.block_time < (g1.block_time + 3600)
    JOIN tmp_wallets w1 ON g1.from_address_id = w1.address_id
    JOIN tmp_wallets w2 ON g1.to_address_id = w2.address_id
    JOIN tx_address a1 ON g1.from_address_id = a1.id
    JOIN tx_address a2 ON g1.to_address_id = a2.id
    WHERE g1.edge_type_id IN (SELECT id FROM tx_guide_type WHERE type_code IN ('spl_transfer','sol_transfer'))
      AND g1.amount > 0
    GROUP BY LEAST(a1.address, a2.address), GREATEST(a1.address, a2.address);

    -- ─── Signal 5: Direct token/SOL transfer — +15 ─────────────
    INSERT INTO tmp_scores (wallet_a, wallet_b, signal_type, score, detail)
    SELECT
        LEAST(a1.address, a2.address),
        GREATEST(a1.address, a2.address),
        'direct_transfer',
        15,
        CONCAT(COUNT(*), ' direct transfer(s)')
    FROM tx_guide g
    JOIN tmp_wallets w1 ON g.from_address_id = w1.address_id
    JOIN tmp_wallets w2 ON g.to_address_id = w2.address_id
    JOIN tx_address a1 ON g.from_address_id = a1.id
    JOIN tx_address a2 ON g.to_address_id = a2.id
    WHERE g.edge_type_id IN (SELECT id FROM tx_guide_type WHERE type_code IN ('spl_transfer','sol_transfer'))
      AND g.from_address_id <> g.to_address_id
    GROUP BY LEAST(a1.address, a2.address), GREATEST(a1.address, a2.address)
    HAVING COUNT(*) < 5;  -- Only if NOT already a high-freq pair

    -- ─── Signal 6: SOL funding transfer — +20 ──────────────────
    INSERT INTO tmp_scores (wallet_a, wallet_b, signal_type, score, detail)
    SELECT DISTINCT
        LEAST(a1.address, a2.address),
        GREATEST(a1.address, a2.address),
        'sol_funding',
        20,
        CONCAT(ROUND(g.amount / 1e9, 4), ' SOL')
    FROM tx_guide g
    JOIN tmp_wallets w1 ON g.from_address_id = w1.address_id
    JOIN tmp_wallets w2 ON g.to_address_id = w2.address_id
    JOIN tx_address a1 ON g.from_address_id = a1.id
    JOIN tx_address a2 ON g.to_address_id = a2.id
    WHERE g.edge_type_id IN (SELECT id FROM tx_guide_type WHERE type_code = 'wallet_funded')
      AND g.from_address_id <> g.to_address_id;

    -- ─── Signal 7: Same withdrawal destination — +30 ───────────
    INSERT INTO tmp_scores (wallet_a, wallet_b, signal_type, score, detail)
    SELECT
        LEAST(w1.address, w2.address),
        GREATEST(w1.address, w2.address),
        'same_destination',
        30,
        CONCAT('common dest: ', dest.address)
    FROM tx_funding_edge e1
    JOIN tx_funding_edge e2 ON e1.to_address_id = e2.to_address_id
                           AND e1.from_address_id < e2.from_address_id
    JOIN tmp_wallets w1 ON e1.from_address_id = w1.address_id
    JOIN tmp_wallets w2 ON e2.from_address_id = w2.address_id
    JOIN tx_address dest ON e1.to_address_id = dest.id
    WHERE NOT EXISTS (
        SELECT 1 FROM tmp_wallets tw WHERE tw.address_id = e1.to_address_id
    );  -- Destination should not be in our wallet set (it's an external consolidation point)

    -- ─── Aggregate: combine scores per wallet pair ──────────────
    SELECT
        wallet_a,
        wallet_b,
        SUM(score) AS combined_score,
        GROUP_CONCAT(
            CONCAT(signal_type, '(', score, '): ', detail)
            ORDER BY score DESC
            SEPARATOR ' | '
        ) AS signals
    FROM tmp_scores
    GROUP BY wallet_a, wallet_b
    HAVING SUM(score) >= v_threshold
    ORDER BY combined_score DESC;

    -- ─── Cleanup ────────────────────────────────────────────────
    DROP TEMPORARY TABLE IF EXISTS tmp_wallets;
    DROP TEMPORARY TABLE IF EXISTS tmp_scores;
END;;

DELIMITER ;
