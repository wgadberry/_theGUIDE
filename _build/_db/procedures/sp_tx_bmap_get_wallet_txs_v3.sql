-- sp_tx_bmap_get_wallet_txs_v3: Wallet transaction history with block_id time boundaries, JSON output
-- Enhances v2 with:
--   1. p_before / p_after (block_id) for time-boundary filtering pushed into tx_guide queries
--   2. JSON output grouped by transaction (deduplicates sig/block_time per edge)
--
-- Parameters:
--   p_wallet_address: Wallet address (required)
--   p_mint_address:   Token mint address (optional — NULL returns all tokens)
--   p_limit:          Max transactions to return (default 50, max 200)
--   p_signature:      Anchor transaction signature (optional — NULL returns latest)
--   p_after:          Only include tx with block_time > this value (optional, unix timestamp)
--   p_before:         Only include tx with block_time < this value (optional, unix timestamp)
--   p_type_mask:      Bitmask filter on tx_guide_type.indicator (optional — NULL returns all types)
--
-- JSON output structure (flat rows, same columns as v2):
--   {
--     "address": "...", "count": N, "mint": "...",
--     "transactions": [
--       { "signature": "...", "block_time": N, "block_time_utc": "...",
--         "type_code": "swap", "direction": "out", "amount": 1.23,
--         "token_symbol": "SOL", "mint_address": "So1...",
--         "party": "7xK...", "party_label": "Raydium",
--         "post_balance": 4.56, "dex": "Raydium" }
--     ]
--   }

DELIMITER //

DROP PROCEDURE IF EXISTS sp_tx_bmap_get_wallet_txs_v3//

CREATE PROCEDURE sp_tx_bmap_get_wallet_txs_v3(
    IN p_wallet_address VARCHAR(44),
    IN p_mint_address VARCHAR(44),
    IN p_limit SMALLINT UNSIGNED,
    IN p_signature VARCHAR(88),
	IN p_after BIGINT UNSIGNED,
    IN p_before BIGINT UNSIGNED,
    IN p_type_mask BIGINT UNSIGNED
)
BEGIN
    DECLARE v_wallet_id INT UNSIGNED;
    DECLARE v_token_id BIGINT DEFAULT NULL;
    DECLARE v_anchor_block_time BIGINT UNSIGNED DEFAULT NULL;
    DECLARE v_half SMALLINT UNSIGNED;
    DECLARE v_tx_count INT UNSIGNED DEFAULT 0;

    SET p_limit = COALESCE(p_limit, 50);
    IF p_limit > 10000 THEN SET p_limit = 10000; END IF;

    -- Validate wallet
    IF NOT EXISTS (SELECT 1 FROM tx_address WHERE address = p_wallet_address) THEN
        SELECT JSON_OBJECT('error', 'Wallet address not found', 'address', p_wallet_address) AS result;
    -- Validate token (if mint provided)
    ELSEIF p_mint_address IS NOT NULL AND p_mint_address != ''
       AND NOT EXISTS (
           SELECT 1 FROM tx_token tk
           JOIN tx_address mint ON mint.id = tk.mint_address_id
           WHERE mint.address = p_mint_address
       ) THEN
        SELECT JSON_OBJECT('error', 'Token not found', 'mint', p_mint_address) AS result;
    -- Validate anchor signature (if provided)
    ELSEIF p_signature IS NOT NULL AND p_signature != ''
       AND NOT EXISTS (SELECT 1 FROM tx WHERE signature = p_signature) THEN
        SELECT JSON_OBJECT('error', 'Signature not found', 'signature', p_signature) AS result;
    ELSE
        -- All validations passed — resolve IDs
        SELECT id INTO v_wallet_id FROM tx_address WHERE address = p_wallet_address LIMIT 1;

        IF p_mint_address IS NOT NULL AND p_mint_address != '' THEN
            SELECT tk.id INTO v_token_id
            FROM tx_token tk
            JOIN tx_address mint ON mint.id = tk.mint_address_id
            WHERE mint.address = p_mint_address
            LIMIT 1;
        END IF;

        IF p_signature IS NOT NULL AND p_signature != '' THEN
            SELECT block_time INTO v_anchor_block_time
            FROM tx WHERE signature = p_signature LIMIT 1;
        END IF;

                -- Phase 1: Lightweight ID collection (tx_guide + tx only, no token/address joins)
                -- Phase 1a: Collect all matching guide_ids with tx_id
                DROP TEMPORARY TABLE IF EXISTS tmp_guide_ids;
                CREATE TEMPORARY TABLE tmp_guide_ids (
                    guide_id BIGINT UNSIGNED,
                    tx_id BIGINT UNSIGNED,
                    block_time BIGINT UNSIGNED,
                    direction VARCHAR(4),
                    INDEX idx_bt (block_time),
                    INDEX idx_tx (tx_id)
                ) ENGINE=MEMORY;

                INSERT INTO tmp_guide_ids
                -- FROM side: wallet is the sender
                SELECT g.id, g.tx_id, t.block_time, 'out'
                FROM tx_guide g
                JOIN tx t ON t.id = g.tx_id
                    AND (p_before IS NULL OR t.block_time < p_before)
                    AND (p_after  IS NULL OR t.block_time > p_after)
                JOIN tx_guide_type gt ON gt.id = g.edge_type_id
                    AND (p_type_mask IS NULL OR (gt.indicator & p_type_mask) != 0)
                WHERE g.from_address_id = v_wallet_id
                  AND (v_token_id IS NULL OR g.token_id = v_token_id)

                UNION ALL

                -- TO side: wallet is the receiver
                SELECT g.id, g.tx_id, t.block_time, 'in'
                FROM tx_guide g
                JOIN tx t ON t.id = g.tx_id
                    AND (p_before IS NULL OR t.block_time < p_before)
                    AND (p_after  IS NULL OR t.block_time > p_after)
                JOIN tx_guide_type gt ON gt.id = g.edge_type_id
                    AND (p_type_mask IS NULL OR (gt.indicator & p_type_mask) != 0)
                WHERE g.to_address_id = v_wallet_id
                  AND (v_token_id IS NULL OR g.token_id = v_token_id)

                UNION ALL

                -- POOL side: swap activity through this pool
                SELECT g.id, g.tx_id, t.block_time,
                    CASE WHEN g.from_address_id = v_wallet_id THEN 'out'
                         WHEN g.to_address_id = v_wallet_id THEN 'in'
                         ELSE 'swap' END
                FROM tx_guide g
                JOIN tx t ON t.id = g.tx_id
                    AND (p_before IS NULL OR t.block_time < p_before)
                    AND (p_after  IS NULL OR t.block_time > p_after)
                JOIN tx_guide_type gt ON gt.id = g.edge_type_id
                    AND (p_type_mask IS NULL OR (gt.indicator & p_type_mask) != 0)
                JOIN tx_pool wp ON wp.id = g.pool_id AND wp.pool_address_id = v_wallet_id
                WHERE 1=1
                  AND g.from_address_id != v_wallet_id
                  AND g.to_address_id != v_wallet_id
                  AND (v_token_id IS NULL OR g.token_id = v_token_id);

                -- Phase 1b: Select top N distinct transactions, then keep all edges for those txs
                DROP TEMPORARY TABLE IF EXISTS tmp_limit_txs;

                IF v_anchor_block_time IS NOT NULL THEN
                    IF p_limit = 1 THEN
                        -- Exact match: return only the anchor transaction
                        CREATE TEMPORARY TABLE tmp_limit_txs ENGINE=MEMORY AS
                        SELECT DISTINCT tx_id, block_time FROM tmp_guide_ids
                        WHERE block_time = v_anchor_block_time
                        LIMIT 1;
                    ELSE
                        SET v_half = CEIL(p_limit / 2);

                        CREATE TEMPORARY TABLE tmp_limit_txs ENGINE=MEMORY AS
                        SELECT DISTINCT tx_id, block_time FROM tmp_guide_ids
                        WHERE block_time <= v_anchor_block_time
                        ORDER BY block_time DESC
                        LIMIT v_half;

                        INSERT IGNORE INTO tmp_limit_txs
                        SELECT DISTINCT tx_id, block_time FROM tmp_guide_ids
                        WHERE block_time > v_anchor_block_time
                        ORDER BY block_time ASC
                        LIMIT v_half;
                    END IF;
                ELSE
                    CREATE TEMPORARY TABLE tmp_limit_txs (
                        tx_id BIGINT UNSIGNED PRIMARY KEY,
                        block_time BIGINT UNSIGNED
                    ) ENGINE=MEMORY AS
                    SELECT DISTINCT tx_id, block_time FROM tmp_guide_ids
                    ORDER BY block_time DESC
                    LIMIT p_limit;
                END IF;

                -- Final IDs: all edges for the selected transactions
                DROP TEMPORARY TABLE IF EXISTS tmp_final_ids;
                CREATE TEMPORARY TABLE tmp_final_ids ENGINE=MEMORY AS
                SELECT g.guide_id, g.block_time, g.direction
                FROM tmp_guide_ids g
                JOIN tmp_limit_txs lt ON lt.tx_id = g.tx_id;

                SELECT COUNT(*) INTO v_tx_count FROM tmp_limit_txs;

                DROP TEMPORARY TABLE tmp_guide_ids;
                DROP TEMPORARY TABLE tmp_limit_txs;

                -- Phase 2: Full join only for surviving IDs — flat rows (one per edge)
                SELECT JSON_OBJECT(
                    'address', p_wallet_address,
                    'address_funded_by', (SELECT fa2.address FROM tx_address wa
                                          JOIN tx_address fa2 ON fa2.id = wa.funded_by_address_id
                                          WHERE wa.id = v_wallet_id),
                    'count',      v_tx_count,
                    'edge_count', JSON_LENGTH(txs_val),
                    'mint',    p_mint_address,
                    'transactions', txs_val
                ) AS result
                FROM (
                    SELECT
                        COALESCE((
                            SELECT JSON_ARRAYAGG(
                                JSON_OBJECT(
                                    'signature',        t.signature,
                                    'mint_address',     ma.address,							
                                    'block_time',       t.block_time,                                  
									'party',            CASE f.direction
                                                          WHEN 'out'  THEN ca.address
                                                          WHEN 'in'   THEN fa.address
                                                          ELSE COALESCE(fa.address, ca.address)
                                                        END,
                                    'type_code',        gt.type_code,
									'token_symbol',     tk.token_symbol,
									'direction',        f.direction,
                                    'amount',           COALESCE(NULLIF(
                                                          CASE WHEN LOCATE('.', CAST(ROUND(g.amount / POW(10, COALESCE(g.decimals, tk.decimals, 9)), 18) AS CHAR)) > 0
                                                            THEN TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM CAST(ROUND(g.amount / POW(10, COALESCE(g.decimals, tk.decimals, 9)), 18) AS CHAR)))
                                                            ELSE CAST(ROUND(g.amount / POW(10, COALESCE(g.decimals, tk.decimals, 9)), 18) AS CHAR)
                                                          END, ''), '0'),
									  'block_time_utc',   DATE_FORMAT(FROM_UNIXTIME(t.block_time), '%Y-%m-%dT%H:%i:%s'),
									 'party_label',      CASE
                                                          WHEN f.direction = 'out'  AND g.to_address_id   = t.signer_address_id THEN 'Signer'
                                                          WHEN f.direction = 'in'   AND g.from_address_id = t.signer_address_id THEN 'Signer'
                                                          WHEN f.direction = 'swap' AND COALESCE(g.from_address_id, g.to_address_id) = t.signer_address_id THEN 'Signer'
                                                          WHEN f.direction = 'out'  THEN COALESCE(ca.label, ca.address_type)
                                                          WHEN f.direction = 'in'   THEN COALESCE(fa.label, fa.address_type)
                                                          ELSE COALESCE(fa.label, fa.address_type, ca.label, ca.address_type)
                                                        END,
                                    'mint_funded_by',   mf.address,
                                    'party_funded_by',  CASE f.direction
                                                          WHEN 'out'  THEN caf.address
                                                          WHEN 'in'   THEN faf.address
                                                          ELSE COALESCE(faf.address, caf.address)
                                                        END,
                                    'dex',              pool_ref.dex,
                                    'post_balance',     CASE
                                                          WHEN COALESCE(
                                                                 CASE f.direction
                                                                   WHEN 'out' THEN g.from_token_post_balance
                                                                   WHEN 'in'  THEN g.to_token_post_balance
                                                                 END,
                                                                 CASE f.direction
                                                                   WHEN 'out' THEN g.from_sol_post_balance
                                                                   WHEN 'in'  THEN g.to_sol_post_balance
                                                                 END
                                                               ) IS NULL THEN NULL
                                                          ELSE COALESCE(NULLIF(
                                                            CASE WHEN LOCATE('.', CAST(ROUND(
                                                              COALESCE(
                                                                CASE f.direction
                                                                  WHEN 'out' THEN g.from_token_post_balance
                                                                  WHEN 'in'  THEN g.to_token_post_balance
                                                                END,
                                                                CASE f.direction
                                                                  WHEN 'out' THEN g.from_sol_post_balance
                                                                  WHEN 'in'  THEN g.to_sol_post_balance
                                                                END
                                                              ) / POW(10, COALESCE(g.decimals, tk.decimals, 9)), 18) AS CHAR)) > 0
                                                            THEN TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM CAST(ROUND(
                                                              COALESCE(
                                                                CASE f.direction
                                                                  WHEN 'out' THEN g.from_token_post_balance
                                                                  WHEN 'in'  THEN g.to_token_post_balance
                                                                END,
                                                                CASE f.direction
                                                                  WHEN 'out' THEN g.from_sol_post_balance
                                                                  WHEN 'in'  THEN g.to_sol_post_balance
                                                                END
                                                              ) / POW(10, COALESCE(g.decimals, tk.decimals, 9)), 18) AS CHAR)))
                                                            ELSE CAST(ROUND(
                                                              COALESCE(
                                                                CASE f.direction
                                                                  WHEN 'out' THEN g.from_token_post_balance
                                                                  WHEN 'in'  THEN g.to_token_post_balance
                                                                END,
                                                                CASE f.direction
                                                                  WHEN 'out' THEN g.from_sol_post_balance
                                                                  WHEN 'in'  THEN g.to_sol_post_balance
                                                                END
                                                              ) / POW(10, COALESCE(g.decimals, tk.decimals, 9)), 18) AS CHAR)
                                                            END, ''), '0')
                                                        END
                                )
                            )
                            FROM tmp_final_ids f
                            JOIN tx_guide g ON g.id = f.guide_id
                            JOIN tx t ON t.id = g.tx_id
                            JOIN tx_guide_type gt ON gt.id = g.edge_type_id
                            JOIN tx_token tk ON tk.id = g.token_id
                            JOIN tx_address ma ON ma.id = tk.mint_address_id
                            LEFT JOIN tx_address ca ON ca.id = g.to_address_id
                            LEFT JOIN tx_address fa ON fa.id = g.from_address_id
                            LEFT JOIN tx_pool pool_ref ON pool_ref.id = g.pool_id
                            LEFT JOIN tx_address mf ON mf.id = ma.funded_by_address_id
                            LEFT JOIN tx_address caf ON caf.id = ca.funded_by_address_id
                            LEFT JOIN tx_address faf ON faf.id = fa.funded_by_address_id
                            ORDER BY t.block_time DESC
                        ), CAST('[]' AS JSON)) AS txs_val
                ) sub;

		DROP TEMPORARY TABLE IF EXISTS tmp_final_ids;
    END IF;
END //

DELIMITER ;
