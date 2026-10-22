-- sp_tx_forensics: Token scam/risk analysis
-- Returns JSON report with activity curve, edge breakdown, wallet analysis,
-- Sybil detection, wash trading, swap concentration, and composite risk score.
--
-- Usage:
--   CALL sp_tx_forensics('11mMsBwszvhEVxHUdPoNPyF7Gt7h6zorMiXfNB5ZmMr');

DELIMITER //

DROP PROCEDURE IF EXISTS sp_tx_forensics//

CREATE PROCEDURE sp_tx_forensics(
    IN p_mint_address VARCHAR(44)
)
BEGIN
    DECLARE v_token_id BIGINT;
    DECLARE v_mint_address VARCHAR(44);
    DECLARE v_token_symbol VARCHAR(128);
    DECLARE v_token_name VARCHAR(255);
    DECLARE v_decimals TINYINT UNSIGNED;

    DECLARE v_total_edges INT DEFAULT 0;
    DECLARE v_total_txs INT DEFAULT 0;
    DECLARE v_unique_wallets INT DEFAULT 0;
    DECLARE v_holders INT DEFAULT 0;
    DECLARE v_holder_supply DECIMAL(30,6) DEFAULT 0;
    DECLARE v_first_bt BIGINT UNSIGNED;
    DECLARE v_last_bt BIGINT UNSIGNED;
    DECLARE v_lifespan_days INT DEFAULT 0;

    DECLARE v_peak_day_txs INT DEFAULT 0;
    DECLARE v_last_day_txs INT DEFAULT 0;
    DECLARE v_decay_ratio DECIMAL(10,2) DEFAULT 0;

    DECLARE v_risk_score INT DEFAULT 0;
    DECLARE v_max_sybil_cluster INT DEFAULT 0;
    DECLARE v_wash_count INT DEFAULT 0;
    DECLARE v_max_top2_sell_pct DECIMAL(10,2) DEFAULT 0;
    DECLARE v_burn_count INT DEFAULT 0;
    DECLARE v_remove_liq_count INT DEFAULT 0;
    DECLARE v_unique_sell_wallets INT DEFAULT 0;

    DECLARE v_token_json JSON;
    DECLARE v_activity_json JSON;
    DECLARE v_edge_json JSON;
    DECLARE v_wallets_json JSON;
    DECLARE v_sybil_json JSON;
    DECLARE v_wash_json JSON;
    DECLARE v_swap_json JSON;
    DECLARE v_assoc_json JSON;

    -- ==========================================================================
    -- STEP 1: Resolve token (borrowed from sp_tx_bmap_get)
    -- ==========================================================================
    SELECT tk.id, mint.address, tk.token_symbol, tk.token_name, tk.decimals
    INTO v_token_id, v_mint_address, v_token_symbol, v_token_name, v_decimals
    FROM tx_token tk
    JOIN tx_address mint ON mint.id = tk.mint_address_id
    WHERE mint.address = p_mint_address
    LIMIT 1;

    IF v_token_id IS NULL THEN
        SELECT JSON_OBJECT('result', JSON_OBJECT('error', 'Token not found', 'mint', p_mint_address)) AS result;
    ELSE

        -- ==========================================================================
        -- STEP 2: Aggregate summary stats
        -- ==========================================================================
        SELECT COUNT(*), COUNT(DISTINCT g.tx_id), MIN(g.block_time), MAX(g.block_time)
        INTO v_total_edges, v_total_txs, v_first_bt, v_last_bt
        FROM tx_guide g
        WHERE g.token_id = v_token_id;

        -- Unique wallets (from + to deduplicated)
        DROP TEMPORARY TABLE IF EXISTS tmp_all_wallets;
        CREATE TEMPORARY TABLE tmp_all_wallets (
            address_id INT UNSIGNED PRIMARY KEY
        ) ENGINE=MEMORY;

        INSERT IGNORE INTO tmp_all_wallets (address_id)
        SELECT from_address_id FROM tx_guide WHERE token_id = v_token_id;
        INSERT IGNORE INTO tmp_all_wallets (address_id)
        SELECT to_address_id FROM tx_guide WHERE token_id = v_token_id;

        SELECT COUNT(*) INTO v_unique_wallets FROM tmp_all_wallets;
        DROP TEMPORARY TABLE IF EXISTS tmp_all_wallets;

        -- Current holders: wallets whose latest post_balance > 0
        -- Use ROW_NUMBER to pick latest balance per wallet (avoids GROUP_CONCAT string building)
        DROP TEMPORARY TABLE IF EXISTS tmp_latest_balance;
        CREATE TEMPORARY TABLE tmp_latest_balance (
            address_id INT UNSIGNED NOT NULL,
            post_balance BIGINT UNSIGNED DEFAULT 0,
            rn INT UNSIGNED NOT NULL,
            KEY (address_id, rn)
        ) ENGINE=MEMORY;

        INSERT INTO tmp_latest_balance (address_id, post_balance, rn)
        SELECT address_id, post_balance,
               ROW_NUMBER() OVER (PARTITION BY address_id ORDER BY block_time DESC, tx_id DESC) AS rn
        FROM (
            SELECT g.from_address_id AS address_id, g.block_time, g.tx_id, g.from_token_post_balance AS post_balance
            FROM tx_guide g
            JOIN tx_address a ON a.id = g.from_address_id AND a.address_type = 'wallet'
            WHERE g.token_id = v_token_id AND g.from_token_post_balance IS NOT NULL
            UNION ALL
            SELECT g.to_address_id, g.block_time, g.tx_id, g.to_token_post_balance
            FROM tx_guide g
            JOIN tx_address a ON a.id = g.to_address_id AND a.address_type = 'wallet'
            WHERE g.token_id = v_token_id AND g.to_token_post_balance IS NOT NULL
        ) combined;

        SELECT COUNT(*), COALESCE(ROUND(SUM(post_balance / POW(10, COALESCE(v_decimals, 9))), 6), 0)
        INTO v_holders, v_holder_supply
        FROM tmp_latest_balance
        WHERE rn = 1 AND post_balance > 0;

        DROP TEMPORARY TABLE IF EXISTS tmp_latest_balance;

        SET v_lifespan_days = GREATEST(1, FLOOR((CAST(v_last_bt AS SIGNED) - CAST(v_first_bt AS SIGNED)) / 86400));

        -- ==========================================================================
        -- STEP 3: Activity curve (daily)
        -- ==========================================================================
        DROP TEMPORARY TABLE IF EXISTS tmp_activity;
        CREATE TEMPORARY TABLE tmp_activity (
            day_date DATE,
            tx_count INT,
            unique_wallets INT
        );

        INSERT INTO tmp_activity (day_date, tx_count, unique_wallets)
        SELECT
            DATE(FROM_UNIXTIME(g.block_time)) AS day_date,
            COUNT(DISTINCT g.tx_id) AS tx_count,
            COUNT(DISTINCT g.from_address_id) + COUNT(DISTINCT g.to_address_id) AS unique_wallets
        FROM tx_guide g
        WHERE g.token_id = v_token_id
        GROUP BY day_date
        ORDER BY day_date;

        -- Peak and last day
        SELECT MAX(tx_count) INTO v_peak_day_txs FROM tmp_activity;
        SELECT tx_count INTO v_last_day_txs FROM tmp_activity ORDER BY day_date DESC LIMIT 1;
        SET v_decay_ratio = IF(v_last_day_txs > 0, ROUND(v_peak_day_txs / v_last_day_txs, 2), 9999);

        SELECT JSON_ARRAYAGG(
            JSON_OBJECT('date', day_date, 'tx_count', tx_count, 'unique_wallets', unique_wallets)
        ) INTO v_activity_json
        FROM (SELECT * FROM tmp_activity ORDER BY day_date) a;

        DROP TEMPORARY TABLE IF EXISTS tmp_activity;

        -- ==========================================================================
        -- STEP 4: Edge type breakdown
        -- ==========================================================================
        SELECT JSON_ARRAYAGG(edge_data) INTO v_edge_json
        FROM (
            SELECT JSON_OBJECT(
                'type_code', gt.type_code,
                'category', gt.category,
                'edge_count', COUNT(*),
                'unique_wallets', COUNT(DISTINCT g.from_address_id) + COUNT(DISTINCT g.to_address_id),
                'total_volume', ROUND(SUM(g.amount / POW(10, COALESCE(g.decimals, v_decimals, 9))), 6)
            ) AS edge_data
            FROM tx_guide g
            JOIN tx_guide_type gt ON gt.id = g.edge_type_id
            WHERE g.token_id = v_token_id
            GROUP BY gt.type_code, gt.category
            ORDER BY COUNT(*) DESC
        ) eb;

        -- Grab counts for risk scoring
        SELECT COUNT(*) INTO v_burn_count
        FROM tx_guide g JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        WHERE g.token_id = v_token_id AND gt.type_code = 'burn';

        SELECT COUNT(*) INTO v_remove_liq_count
        FROM tx_guide g JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        WHERE g.token_id = v_token_id AND gt.type_code = 'remove_liquidity';

        -- ==========================================================================
        -- STEP 5: Top 20 wallets by volume
        -- ==========================================================================
        DROP TEMPORARY TABLE IF EXISTS tmp_wallet_stats;
        CREATE TEMPORARY TABLE tmp_wallet_stats (
            address_id INT UNSIGNED PRIMARY KEY,
            address VARCHAR(44),
            label VARCHAR(200),
            address_type VARCHAR(30),
            funded_by VARCHAR(44),
            total_volume DECIMAL(30,6) DEFAULT 0,
            edge_count INT DEFAULT 0,
            buy_volume DECIMAL(30,6) DEFAULT 0,
            sell_volume DECIMAL(30,6) DEFAULT 0,
            swap_in_count INT DEFAULT 0,
            swap_out_count INT DEFAULT 0,
            transfer_in DECIMAL(30,6) DEFAULT 0,
            transfer_out DECIMAL(30,6) DEFAULT 0,
            edge_types VARCHAR(500) DEFAULT NULL
        );

        -- FROM side (sender/outflow)
        INSERT INTO tmp_wallet_stats (address_id, address, label, address_type, funded_by,
                                      total_volume, edge_count, sell_volume, swap_in_count, transfer_out)
        SELECT
            a.id, a.address, a.address_type, a.address_type,
            f.address,
            ROUND(SUM(g.amount / POW(10, COALESCE(g.decimals, v_decimals, 9))), 6),
            COUNT(*),
            ROUND(SUM(CASE WHEN gt.type_code = 'swap_in' THEN g.amount / POW(10, COALESCE(g.decimals, v_decimals, 9)) ELSE 0 END), 6),
            SUM(CASE WHEN gt.type_code = 'swap_in' THEN 1 ELSE 0 END),
            ROUND(SUM(CASE WHEN gt.type_code IN ('spl_transfer','sol_transfer') THEN g.amount / POW(10, COALESCE(g.decimals, v_decimals, 9)) ELSE 0 END), 6)
        FROM tx_guide g
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        JOIN tx_address a ON a.id = g.from_address_id
        LEFT JOIN tx_address f ON f.id = a.funded_by_address_id
        WHERE g.token_id = v_token_id
        GROUP BY a.id, a.address, a.address_type, f.address;

        -- TO side (receiver/inflow) — merge
        INSERT INTO tmp_wallet_stats (address_id, address, label, address_type, funded_by,
                                      total_volume, edge_count, buy_volume, swap_out_count, transfer_in)
        SELECT
            a.id, a.address, a.address_type, a.address_type,
            f.address,
            ROUND(SUM(g.amount / POW(10, COALESCE(g.decimals, v_decimals, 9))), 6),
            COUNT(*),
            ROUND(SUM(CASE WHEN gt.type_code = 'swap_out' THEN g.amount / POW(10, COALESCE(g.decimals, v_decimals, 9)) ELSE 0 END), 6),
            SUM(CASE WHEN gt.type_code = 'swap_out' THEN 1 ELSE 0 END),
            ROUND(SUM(CASE WHEN gt.type_code IN ('spl_transfer','sol_transfer') THEN g.amount / POW(10, COALESCE(g.decimals, v_decimals, 9)) ELSE 0 END), 6)
        FROM tx_guide g
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        JOIN tx_address a ON a.id = g.to_address_id
        LEFT JOIN tx_address f ON f.id = a.funded_by_address_id
        WHERE g.token_id = v_token_id
        GROUP BY a.id, a.address, a.address_type, f.address
        ON DUPLICATE KEY UPDATE
            total_volume = tmp_wallet_stats.total_volume + VALUES(total_volume),
            edge_count = tmp_wallet_stats.edge_count + VALUES(edge_count),
            buy_volume = tmp_wallet_stats.buy_volume + VALUES(buy_volume),
            swap_out_count = tmp_wallet_stats.swap_out_count + VALUES(swap_out_count),
            transfer_in = tmp_wallet_stats.transfer_in + VALUES(transfer_in);

        -- Populate distinct edge types per wallet (both sides merged)
        -- Pre-compute into temp table to avoid correlated OR subquery (N×M scan)
        DROP TEMPORARY TABLE IF EXISTS tmp_wallet_edge_types;
        CREATE TEMPORARY TABLE tmp_wallet_edge_types (
            address_id INT UNSIGNED NOT NULL,
            type_code VARCHAR(30) NOT NULL,
            PRIMARY KEY (address_id, type_code)
        ) ENGINE=MEMORY;

        INSERT IGNORE INTO tmp_wallet_edge_types (address_id, type_code)
        SELECT g.from_address_id, gt.type_code
        FROM tx_guide g
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        JOIN tmp_wallet_stats w ON w.address_id = g.from_address_id
        WHERE g.token_id = v_token_id;

        INSERT IGNORE INTO tmp_wallet_edge_types (address_id, type_code)
        SELECT g.to_address_id, gt.type_code
        FROM tx_guide g
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        JOIN tmp_wallet_stats w ON w.address_id = g.to_address_id
        WHERE g.token_id = v_token_id;

        UPDATE tmp_wallet_stats w
        SET w.edge_types = (
            SELECT GROUP_CONCAT(et.type_code ORDER BY et.type_code SEPARATOR ', ')
            FROM tmp_wallet_edge_types et
            WHERE et.address_id = w.address_id
        );

        DROP TEMPORARY TABLE IF EXISTS tmp_wallet_edge_types;

        -- -----------------------------------------------------------------
        -- STEP 5b: Enrich labels from related tables (per-token context)
        -- Priority: mint-edge "Token Creator" > pool_label > program name
        --           > token symbol > tx_address.label (if not misleading)
        --           > address_type
        -- -----------------------------------------------------------------

        -- 1) Addresses that have a mint edge (type_code=38) for THIS token = actual creator
        UPDATE tmp_wallet_stats w
        SET w.label = 'Token Creator'
        WHERE EXISTS (
            SELECT 1 FROM tx_guide g
            WHERE g.token_id = v_token_id
              AND g.edge_type_id = 38
              AND g.from_address_id = w.address_id
        );

        -- 2) Pool labels from tx_pool
        UPDATE tmp_wallet_stats w
        JOIN tx_pool p ON p.pool_address_id = w.address_id
        SET w.label = p.pool_label
        WHERE w.label = w.address_type
          AND p.pool_label IS NOT NULL AND p.pool_label != '';

        -- 3) Program names from tx_program
        UPDATE tmp_wallet_stats w
        JOIN tx_program pr ON pr.program_address_id = w.address_id
        SET w.label = pr.name
        WHERE w.label = w.address_type
          AND pr.name IS NOT NULL AND pr.name != '';

        -- 4) Token symbol/name for mint addresses
        UPDATE tmp_wallet_stats w
        JOIN tx_token tk ON tk.mint_address_id = w.address_id
        SET w.label = CONCAT(COALESCE(tk.token_symbol, ''), IF(tk.token_name IS NOT NULL AND tk.token_name != '', CONCAT(' (', tk.token_name, ')'), ''))
        WHERE w.label = w.address_type
          AND (tk.token_symbol IS NOT NULL OR tk.token_name IS NOT NULL);

        -- 5) Apply tx_address.label for remaining unlabeled, but skip misleading globals
        UPDATE tmp_wallet_stats w
        JOIN tx_address a ON a.id = w.address_id
        SET w.label = a.label
        WHERE w.label = w.address_type
          AND a.label IS NOT NULL AND a.label != ''
          AND a.label NOT IN ('Token Creator');

        SELECT JSON_ARRAYAGG(wallet_data) INTO v_wallets_json
        FROM (
            SELECT JSON_OBJECT(
                'address', w.address,
                'label', w.label,
                'address_type', w.address_type,
                'funded_by', w.funded_by,
                'total_volume', w.total_volume,
                'edge_count', w.edge_count,
                'buy_volume', w.buy_volume,
                'sell_volume', w.sell_volume,
                'swap_in_count', w.swap_in_count,
                'swap_out_count', w.swap_out_count,
                'transfer_in', w.transfer_in,
                'transfer_out', w.transfer_out,
                'edge_types', w.edge_types
            ) AS wallet_data
            FROM tmp_wallet_stats w
            ORDER BY w.total_volume DESC
            LIMIT 20
        ) tw;

        -- ==========================================================================
        -- STEP 6: Sybil clusters (wallets sharing same label, threshold 3+)
        -- ==========================================================================
        SELECT JSON_ARRAYAGG(cluster_data) INTO v_sybil_json
        FROM (
            SELECT JSON_OBJECT(
                'label', w.label,
                'wallet_count', COUNT(*),
                'addresses', JSON_ARRAYAGG(w.address)
            ) AS cluster_data
            FROM tmp_wallet_stats w
            WHERE w.label IS NOT NULL
              AND w.label != ''
              AND w.label NOT IN ('wallet', 'ata', 'unknown', 'pool', 'mint', 'vault', 'program', 'market', 'bot', 'dex_wallet', 'lp_token')
              AND w.address_type NOT IN ('program', 'mint', 'pool', 'vault', 'market')
            GROUP BY w.label
            HAVING COUNT(*) >= 3
            ORDER BY COUNT(*) DESC
            LIMIT 20
        ) sc;

        SELECT COALESCE(MAX(cnt), 0) INTO v_max_sybil_cluster
        FROM (
            SELECT COUNT(*) AS cnt
            FROM tmp_wallet_stats w
            WHERE w.label IS NOT NULL AND w.label != ''
              AND w.label NOT IN ('wallet', 'ata', 'unknown', 'pool', 'mint', 'vault', 'program', 'market', 'bot', 'dex_wallet', 'lp_token')
              AND w.address_type NOT IN ('program', 'mint', 'pool', 'vault', 'market')
            GROUP BY w.label
            HAVING COUNT(*) >= 3
        ) sybil_counts;

        -- ==========================================================================
        -- STEP 7: Wash trading detection
        -- ==========================================================================
        SELECT JSON_ARRAYAGG(wash_data) INTO v_wash_json
        FROM (
            SELECT JSON_OBJECT(
                'address', w.address,
                'label', w.label,
                'sent_volume', w.sell_volume + w.transfer_out,
                'received_volume', w.buy_volume + w.transfer_in,
                'overlap_pct', ROUND(
                    LEAST(w.sell_volume + w.transfer_out, w.buy_volume + w.transfer_in)
                    / GREATEST(w.sell_volume + w.transfer_out, w.buy_volume + w.transfer_in, 0.000001)
                    * 100, 2)
            ) AS wash_data
            FROM tmp_wallet_stats w
            WHERE (w.sell_volume + w.transfer_out) > 0
              AND (w.buy_volume + w.transfer_in) > 0
              AND w.address_type NOT IN ('program', 'mint', 'pool', 'vault', 'market')
              AND LEAST(w.sell_volume + w.transfer_out, w.buy_volume + w.transfer_in)
                  / GREATEST(w.sell_volume + w.transfer_out, w.buy_volume + w.transfer_in, 0.000001) > 0.80
            ORDER BY w.total_volume DESC
            LIMIT 20
        ) wt;

        SELECT COUNT(*) INTO v_wash_count
        FROM tmp_wallet_stats w
        WHERE (w.sell_volume + w.transfer_out) > 0
          AND (w.buy_volume + w.transfer_in) > 0
          AND w.address_type NOT IN ('program', 'mint', 'pool', 'vault', 'market')
          AND LEAST(w.sell_volume + w.transfer_out, w.buy_volume + w.transfer_in)
              / GREATEST(w.sell_volume + w.transfer_out, w.buy_volume + w.transfer_in, 0.000001) > 0.80;

        -- ==========================================================================
        -- STEP 8: Swap concentration per DEX
        -- ==========================================================================
        DROP TEMPORARY TABLE IF EXISTS tmp_dex_stats;
        CREATE TEMPORARY TABLE tmp_dex_stats (
            dex VARCHAR(50),
            buy_wallets INT,
            sell_wallets INT,
            buy_volume DECIMAL(30,6),
            sell_volume DECIMAL(30,6),
            top2_sell_pct DECIMAL(10,2) DEFAULT 0
        );

        INSERT INTO tmp_dex_stats (dex, buy_wallets, sell_wallets, buy_volume, sell_volume)
        SELECT
            COALESCE(g.dex, 'Unknown'),
            COUNT(DISTINCT CASE WHEN gt.type_code = 'swap_out' THEN g.to_address_id END),
            COUNT(DISTINCT CASE WHEN gt.type_code = 'swap_in' THEN g.from_address_id END),
            ROUND(SUM(CASE WHEN gt.type_code = 'swap_out' THEN g.amount / POW(10, COALESCE(g.decimals, v_decimals, 9)) ELSE 0 END), 6),
            ROUND(SUM(CASE WHEN gt.type_code = 'swap_in' THEN g.amount / POW(10, COALESCE(g.decimals, v_decimals, 9)) ELSE 0 END), 6)
        FROM tx_guide g
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        WHERE g.token_id = v_token_id
          AND gt.type_code IN ('swap_in', 'swap_out')
        GROUP BY COALESCE(g.dex, 'Unknown');

        -- Calculate top2 sell concentration per DEX
        -- Pre-materialize seller volumes, then rank to pick top 2 per DEX
        DROP TEMPORARY TABLE IF EXISTS tmp_seller_vols;
        CREATE TEMPORARY TABLE tmp_seller_vols (
            dex VARCHAR(50) NOT NULL,
            seller_id INT UNSIGNED NOT NULL,
            seller_vol DECIMAL(30,6) NOT NULL,
            rn INT UNSIGNED DEFAULT 0,
            KEY (dex, rn)
        ) ENGINE=MEMORY;

        INSERT INTO tmp_seller_vols (dex, seller_id, seller_vol)
        SELECT COALESCE(g.dex, 'Unknown'), g.from_address_id,
               ROUND(SUM(g.amount / POW(10, COALESCE(g.decimals, v_decimals, 9))), 6)
        FROM tx_guide g
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        WHERE g.token_id = v_token_id AND gt.type_code = 'swap_in'
        GROUP BY COALESCE(g.dex, 'Unknown'), g.from_address_id;

        -- Rank sellers per DEX (can't self-reference temp table, so use UPDATE with window)
        DROP TEMPORARY TABLE IF EXISTS tmp_seller_ranked;
        CREATE TEMPORARY TABLE tmp_seller_ranked (
            dex VARCHAR(50) NOT NULL,
            seller_vol DECIMAL(30,6) NOT NULL,
            rn INT UNSIGNED NOT NULL,
            KEY (dex, rn)
        ) ENGINE=MEMORY;

        INSERT INTO tmp_seller_ranked (dex, seller_vol, rn)
        SELECT dex, seller_vol,
               ROW_NUMBER() OVER (PARTITION BY dex ORDER BY seller_vol DESC) AS rn
        FROM tmp_seller_vols;

        UPDATE tmp_dex_stats d
        SET d.top2_sell_pct = COALESCE((
            SELECT ROUND(SUM(sr.seller_vol) / GREATEST(d.sell_volume, 0.000001) * 100, 2)
            FROM tmp_seller_ranked sr
            WHERE sr.dex = d.dex AND sr.rn <= 2
        ), 0)
        WHERE d.sell_volume > 0;

        DROP TEMPORARY TABLE IF EXISTS tmp_seller_vols;
        DROP TEMPORARY TABLE IF EXISTS tmp_seller_ranked;

        SELECT COALESCE(MAX(top2_sell_pct), 0) INTO v_max_top2_sell_pct FROM tmp_dex_stats;

        -- Unique sell wallets across all DEXes
        SELECT COUNT(DISTINCT g.from_address_id) INTO v_unique_sell_wallets
        FROM tx_guide g
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        WHERE g.token_id = v_token_id AND gt.type_code = 'swap_in';

        SELECT JSON_ARRAYAGG(dex_data) INTO v_swap_json
        FROM (
            SELECT JSON_OBJECT(
                'dex', d.dex,
                'buy_wallets', d.buy_wallets,
                'sell_wallets', d.sell_wallets,
                'buy_volume', d.buy_volume,
                'sell_volume', d.sell_volume,
                'top2_sell_pct', d.top2_sell_pct
            ) AS dex_data
            FROM tmp_dex_stats d
            ORDER BY d.sell_volume DESC
        ) ds;

        DROP TEMPORARY TABLE IF EXISTS tmp_dex_stats;
        DROP TEMPORARY TABLE IF EXISTS tmp_wallet_stats;

        -- ==========================================================================
        -- STEP 8b: Associations — pools, programs, dexes, paired tokens
        -- ==========================================================================
        SELECT JSON_OBJECT(
            'pools', COALESCE((
                SELECT JSON_ARRAYAGG(JSON_OBJECT(
                    'address', pa.address,
                    'label', p.pool_label,
                    'program', prg.name
                ))
                FROM tx_pool p
                JOIN tx_address pa ON pa.id = p.pool_address_id
                LEFT JOIN tx_program prg ON prg.id = p.program_id
                WHERE p.token1_id = v_token_id OR p.token2_id = v_token_id
            ), JSON_ARRAY()),
            'programs', COALESCE((
                SELECT JSON_ARRAYAGG(JSON_OBJECT(
                    'address', pra.address,
                    'name', prg.name,
                    'type', prg.program_type
                ))
                FROM (
                    SELECT DISTINCT prg2.id
                    FROM tx_pool p
                    JOIN tx_program prg2 ON prg2.id = p.program_id
                    WHERE p.token1_id = v_token_id OR p.token2_id = v_token_id
                ) dp
                JOIN tx_program prg ON prg.id = dp.id
                JOIN tx_address pra ON pra.id = prg.program_address_id
            ), JSON_ARRAY()),
            'dexes', COALESCE((
                SELECT JSON_ARRAYAGG(dex_name)
                FROM (
                    SELECT DISTINCT g.dex AS dex_name
                    FROM tx_guide g
                    WHERE g.token_id = v_token_id AND g.dex IS NOT NULL
                ) dd
            ), JSON_ARRAY()),
            'paired_tokens', COALESCE((
                SELECT JSON_ARRAYAGG(JSON_OBJECT(
                    'mint', ma.address,
                    'symbol', tk2.token_symbol,
                    'name', tk2.token_name
                ))
                FROM (
                    SELECT DISTINCT CASE WHEN p.token1_id = v_token_id THEN p.token2_id ELSE p.token1_id END AS other_token_id
                    FROM tx_pool p
                    WHERE (p.token1_id = v_token_id OR p.token2_id = v_token_id)
                      AND CASE WHEN p.token1_id = v_token_id THEN p.token2_id ELSE p.token1_id END IS NOT NULL
                ) ot
                JOIN tx_token tk2 ON tk2.id = ot.other_token_id
                JOIN tx_address ma ON ma.id = tk2.mint_address_id
            ), JSON_ARRAY())
        ) INTO v_assoc_json;

        -- ==========================================================================
        -- STEP 9: Risk score (0-100)
        -- ==========================================================================
        SET v_risk_score = 0;

        -- Activity decay: peak-to-last ratio > 10 means pump-and-dump curve
        IF v_decay_ratio > 100 THEN
            SET v_risk_score = v_risk_score + 25;
        ELSEIF v_decay_ratio > 10 THEN
            SET v_risk_score = v_risk_score + 15;
        ELSEIF v_decay_ratio > 5 THEN
            SET v_risk_score = v_risk_score + 8;
        END IF;

        -- Sybil: large cluster of same-label wallets
        IF v_max_sybil_cluster >= 50 THEN
            SET v_risk_score = v_risk_score + 20;
        ELSEIF v_max_sybil_cluster >= 10 THEN
            SET v_risk_score = v_risk_score + 15;
        ELSEIF v_max_sybil_cluster >= 5 THEN
            SET v_risk_score = v_risk_score + 8;
        END IF;

        -- Wash trading
        IF v_wash_count > 10 THEN
            SET v_risk_score = v_risk_score + 15;
        ELSEIF v_wash_count > 3 THEN
            SET v_risk_score = v_risk_score + 10;
        ELSEIF v_wash_count > 0 THEN
            SET v_risk_score = v_risk_score + 5;
        END IF;

        -- Swap sell concentration (top 2 wallets dominate sells)
        IF v_max_top2_sell_pct > 80 THEN
            SET v_risk_score = v_risk_score + 20;
        ELSEIF v_max_top2_sell_pct > 50 THEN
            SET v_risk_score = v_risk_score + 12;
        ELSEIF v_max_top2_sell_pct > 30 THEN
            SET v_risk_score = v_risk_score + 5;
        END IF;

        -- Burns without liquidity removal = rug-pull indicator
        IF v_burn_count > 0 AND v_remove_liq_count = 0 THEN
            SET v_risk_score = v_risk_score + 10;
        END IF;

        -- Very few unique sell wallets = concentrated exit
        IF v_unique_sell_wallets > 0 AND v_unique_sell_wallets < 5 THEN
            SET v_risk_score = v_risk_score + 10;
        ELSEIF v_unique_sell_wallets < 10 THEN
            SET v_risk_score = v_risk_score + 5;
        END IF;

        -- Cap at 100
        SET v_risk_score = LEAST(v_risk_score, 100);

        -- ==========================================================================
        -- STEP 10: Build token summary JSON
        -- ==========================================================================
        SET v_token_json = JSON_OBJECT(
            'mint', v_mint_address,
            'symbol', v_token_symbol,
            'name', v_token_name,
            'decimals', v_decimals,
            'total_edges', v_total_edges,
            'total_txs', v_total_txs,
            'unique_wallets', v_unique_wallets,
            'first_block_time', v_first_bt,
            'first_block_time_utc', FROM_UNIXTIME(v_first_bt),
            'last_block_time', v_last_bt,
            'last_block_time_utc', FROM_UNIXTIME(v_last_bt),
            'lifespan_days', v_lifespan_days,
            'holders', v_holders,
            'holder_supply', v_holder_supply
        );

        -- ==========================================================================
        -- FINAL: Return complete JSON report
        -- ==========================================================================
        SELECT JSON_OBJECT(
            'result', JSON_OBJECT(
                'token', v_token_json,
                'activity_curve', JSON_OBJECT(
                    'peak_day_txs', v_peak_day_txs,
                    'last_day_txs', v_last_day_txs,
                    'decay_ratio', v_decay_ratio,
                    'daily', COALESCE(v_activity_json, JSON_ARRAY())
                ),
                'edge_breakdown', COALESCE(v_edge_json, JSON_ARRAY()),
                'top_wallets', COALESCE(v_wallets_json, JSON_ARRAY()),
                'sybil_clusters', COALESCE(v_sybil_json, JSON_ARRAY()),
                'wash_trading', COALESCE(v_wash_json, JSON_ARRAY()),
                'swap_concentration', COALESCE(v_swap_json, JSON_ARRAY()),
                'associations', v_assoc_json,
                'risk_score', JSON_OBJECT(
                    'score', v_risk_score,
                    'rating', CASE
                        WHEN v_risk_score >= 75 THEN 'CRITICAL'
                        WHEN v_risk_score >= 50 THEN 'HIGH'
                        WHEN v_risk_score >= 25 THEN 'MEDIUM'
                        ELSE 'LOW'
                    END,
                    'signals', JSON_OBJECT(
                        'activity_decay_ratio', v_decay_ratio,
                        'max_sybil_cluster', v_max_sybil_cluster,
                        'wash_trade_wallets', v_wash_count,
                        'top2_sell_concentration_pct', v_max_top2_sell_pct,
                        'burn_without_liq_removal', v_burn_count > 0 AND v_remove_liq_count = 0,
                        'unique_sell_wallets', v_unique_sell_wallets
                    )
                )
            )
        ) AS result;

    END IF;
END //

DELIMITER ;
