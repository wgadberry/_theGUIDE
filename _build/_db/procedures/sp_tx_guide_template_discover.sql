-- sp_tx_guide_template_discover
--
-- Scans tx_guide to discover all distinct edge patterns across 11 dimensions,
-- compute row counts, and upsert into tx_guide_template.
--
-- Dimensions:
--   1-8: edge_type_code, category, direction, from/to_address_type,
--         has_pool, has_tax, swap_direction
--   9:   balance_signal  (drained|funded|large|dust|normal)
--  10:   amount_magnitude (micro|small|medium|whale)
--  11:   label_signal    (dex_labeled|cex_labeled|burn_labeled|authority_labeled|unlabeled)
--
-- Usage:
--   CALL sp_tx_guide_template_discover(30);

DELIMITER $$

DROP PROCEDURE IF EXISTS sp_tx_guide_template_discover$$

CREATE PROCEDURE sp_tx_guide_template_discover(
    IN p_min_rows INT UNSIGNED
)
BEGIN
    DECLARE v_total BIGINT DEFAULT 0;

    SELECT COUNT(*) INTO v_total FROM tx_guide;

    INSERT INTO tx_guide_template (
        pattern_hash,
        edge_type_code, edge_category, edge_direction,
        from_address_type, to_address_type,
        has_pool, has_tax, swap_direction,
        balance_signal, amount_magnitude, label_signal,
        row_count, pct_of_total, use_template
    )
    SELECT
        SHA2(CONCAT_WS('|', p.etc, p.cat, p.dir, p.fat, p.tat,
                        p.hp, p.ht, p.sd, p.bs, p.am, p.ls), 256),
        p.etc, p.cat, p.dir, p.fat, p.tat, p.hp, p.ht, p.sd,
        p.bs, p.am, p.ls,
        p.cnt,
        ROUND(p.cnt * 100.0 / v_total, 2),
        CASE WHEN p.cnt >= p_min_rows THEN 1 ELSE 0 END
    FROM (
        SELECT
            gt.type_code                                            AS etc,
            gt.category                                             AS cat,
            IFNULL(gt.direction, 'neutral')                         AS dir,
            IFNULL(fa.address_type, 'unknown')                      AS fat,
            IFNULL(ta.address_type, 'unknown')                      AS tat,
            CASE WHEN g.pool_id IS NOT NULL THEN 1 ELSE 0 END      AS hp,
            CASE WHEN g.tax_bps IS NOT NULL THEN 1 ELSE 0 END      AS ht,
            IFNULL(g.swap_direction, 'none')                        AS sd,

            -- Balance signal
            CASE
                WHEN g.from_token_post_balance IS NOT NULL
                     AND g.from_token_post_balance = 0
                     AND g.from_token_pre_balance IS NOT NULL
                     AND g.from_token_pre_balance > 0
                THEN 'drained'
                WHEN g.to_token_pre_balance IS NOT NULL
                     AND g.to_token_pre_balance = 0
                     AND g.amount IS NOT NULL
                     AND g.amount > 0
                THEN 'funded'
                WHEN g.amount IS NOT NULL
                     AND g.amount < 10000
                THEN 'dust'
                WHEN g.amount IS NOT NULL
                     AND g.from_token_pre_balance IS NOT NULL
                     AND g.from_token_pre_balance > 0
                     AND g.amount > g.from_token_pre_balance * 0.9
                THEN 'large'
                ELSE 'normal'
            END                                                     AS bs,

            -- Amount magnitude
            CASE
                WHEN g.amount IS NULL THEN 'medium'
                WHEN g.amount < 1000000 THEN 'micro'
                WHEN g.amount < 1000000000 THEN 'small'
                WHEN g.amount < 1000000000000 THEN 'medium'
                ELSE 'whale'
            END                                                     AS am,

            -- Label signal (from either from_address or to_address)
            CASE
                WHEN LOWER(IFNULL(fa.label, '')) LIKE '%burn%'
                  OR LOWER(IFNULL(ta.label, '')) LIKE '%burn%'
                THEN 'burn_labeled'
                WHEN LOWER(IFNULL(fa.label, '')) LIKE '%authority%'
                  OR LOWER(IFNULL(ta.label, '')) LIKE '%authority%'
                THEN 'authority'
                WHEN LOWER(IFNULL(fa.label, '')) REGEXP 'raydium|orca|jupiter|meteora|pump\\.fun|openbook'
                  OR LOWER(IFNULL(ta.label, '')) REGEXP 'raydium|orca|jupiter|meteora|pump\\.fun|openbook'
                THEN 'dex_labeled'
                WHEN LOWER(IFNULL(fa.label, '')) REGEXP 'binance|coinbase|kraken|okx|bybit|ftx|kucoin'
                  OR LOWER(IFNULL(ta.label, '')) REGEXP 'binance|coinbase|kraken|okx|bybit|ftx|kucoin'
                THEN 'cex_labeled'
                WHEN fa.label IS NOT NULL OR ta.label IS NOT NULL
                THEN 'labeled'
                ELSE 'unlabeled'
            END                                                     AS ls,

            COUNT(*)                                                AS cnt
        FROM tx_guide g
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        JOIN tx_address fa ON fa.id = g.from_address_id
        JOIN tx_address ta ON ta.id = g.to_address_id
        GROUP BY etc, cat, dir, fat, tat, hp, ht, sd, bs, am, ls
    ) p
    ON DUPLICATE KEY UPDATE
        row_count    = VALUES(row_count),
        pct_of_total = VALUES(pct_of_total),
        use_template = CASE WHEN VALUES(row_count) >= p_min_rows THEN 1 ELSE use_template END;

    -- Compute cumulative percentage
    SET @cum = 0.0;
    UPDATE tx_guide_template t
    JOIN (
        SELECT id,
               (@cum := @cum + pct_of_total) AS cum_pct
        FROM tx_guide_template
        ORDER BY row_count DESC
    ) ranked ON ranked.id = t.id
    SET t.cumulative_pct = ranked.cum_pct;

    -- Summary
    SELECT
        COUNT(*)                                                     AS total_patterns,
        SUM(use_template)                                            AS template_patterns,
        SUM(CASE WHEN use_template = 0 THEN 1 ELSE 0 END)           AS embed_patterns,
        SUM(CASE WHEN use_template = 1 THEN row_count ELSE 0 END)   AS template_rows,
        SUM(CASE WHEN use_template = 0 THEN row_count ELSE 0 END)   AS embed_rows,
        v_total                                                      AS total_rows
    FROM tx_guide_template;

    -- Signal breakdown
    SELECT balance_signal, amount_magnitude, label_signal,
           COUNT(*) AS patterns, SUM(row_count) AS total_rows,
           SUM(use_template) AS templated
    FROM tx_guide_template
    GROUP BY balance_signal, amount_magnitude, label_signal
    ORDER BY total_rows DESC
    LIMIT 30;

END$$

DELIMITER ;
