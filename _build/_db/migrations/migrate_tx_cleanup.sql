-- ============================================================================
-- migrate_tx_cleanup.sql
-- Date: 2026-02-14
-- Purpose: Remove unused FK constraints, indexes, and dead columns from tx table
--          Update dependent SPs/functions/views to use tx_state instead of type_state
--
-- Before: 13 indexes (PRIMARY + 12 secondary)
-- After:   6 indexes (PRIMARY + 5 secondary)
-- Columns removed: type_state, is_guide_loaded
-- ============================================================================

-- ============================================================================
-- TIER 1: Drop redundant index
-- ============================================================================

-- idx_tx_blocktime (id, block_time) — PK covers id, idx_block_time covers time ordering
ALTER TABLE tx DROP INDEX idx_tx_blocktime;

-- ============================================================================
-- TIER 2: Drop unused FK constraints + their indexes
--         These agg_* and signer columns are populated but NEVER queried via
--         WHERE/JOIN in any SP, function, view, or Python worker.
-- ============================================================================

-- Drop FK constraints first (required before dropping their indexes)
ALTER TABLE tx DROP FOREIGN KEY tx_ibfk_signer;
ALTER TABLE tx DROP FOREIGN KEY tx_ibfk_agg_account;
ALTER TABLE tx DROP FOREIGN KEY tx_ibfk_agg_token_in;
ALTER TABLE tx DROP FOREIGN KEY tx_ibfk_agg_token_out;
ALTER TABLE tx DROP FOREIGN KEY tx_ibfk_agg_program;
ALTER TABLE tx DROP FOREIGN KEY tx_ibfk_agg_fee_token;

-- Drop the FK indexes
ALTER TABLE tx DROP INDEX idx_signer;
ALTER TABLE tx DROP INDEX idx_agg_account;
ALTER TABLE tx DROP INDEX idx_agg_token_in;
ALTER TABLE tx DROP INDEX idx_agg_token_out;
ALTER TABLE tx DROP INDEX tx_ibfk_agg_program;
ALTER TABLE tx DROP INDEX tx_ibfk_agg_fee_token;

-- ============================================================================
-- TIER 3: Drop dead columns + update dependent objects
--
-- type_state (BIGINT UNSIGNED):
--   - Never written to by any SP or worker (all pipeline SPs update tx_state)
--   - Always 0 for every row
--   - Referenced by 3 SPs/functions + 3 views (all reading 0 / no-op filters)
--   - tx_state.py defines SQL templates but no worker imports them
--
-- is_guide_loaded (generated stored column):
--   - Defined as (tx_state & 32) >> 5
--   - No SP, function, view, or worker queries this column
--   - Index was already dropped in prior migration
-- ============================================================================

-- Step 3a: Update SPs/functions that reference type_state → tx_state
-- --------------------------------------------------------------------------

-- sp_tx_funding_chain: reads type_state in SELECT and WHERE
-- Replace type_state with CAST(tx_state AS UNSIGNED) for bitmask compatibility
DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_tx_funding_chain`;;

CREATE DEFINER=`root`@`%` PROCEDURE `sp_tx_funding_chain`(
    IN p_wallet_address VARCHAR(44),
    IN p_min_tx_state BIGINT UNSIGNED,
    IN p_max_depth INT,
    IN p_direction VARCHAR(10)
)
BEGIN
    DECLARE v_start_id INT UNSIGNED;
    DECLARE v_depth INT DEFAULT 0;
    DECLARE v_found INT DEFAULT 1;

    SET p_max_depth = COALESCE(p_max_depth, 10);
    SET p_direction = COALESCE(p_direction, 'up');
    SET p_min_tx_state = COALESCE(p_min_tx_state, 0);

    SELECT id INTO v_start_id
    FROM tx_address
    WHERE address = p_wallet_address
    LIMIT 1;

    IF v_start_id IS NULL THEN
        SELECT 'Wallet not found' AS error, p_wallet_address AS wallet;
    ELSE
        DROP TEMPORARY TABLE IF EXISTS tmp_chain;
        DROP TEMPORARY TABLE IF EXISTS tmp_frontier;

        CREATE TEMPORARY TABLE tmp_chain (
            depth INT,
            direction VARCHAR(10),
            wallet_id INT UNSIGNED,
            wallet_address VARCHAR(44),
            wallet_label VARCHAR(100),
            funder_id INT UNSIGNED,
            funder_address VARCHAR(44),
            funder_label VARCHAR(100),
            funding_sol DECIMAL(20,9),
            funding_tx_signature VARCHAR(88),
            tx_state BIGINT UNSIGNED,
            first_seen_utc DATETIME,
            PRIMARY KEY (wallet_id)
        );

        CREATE TEMPORARY TABLE tmp_frontier (
            wallet_id INT UNSIGNED PRIMARY KEY,
            funder_id INT UNSIGNED
        );

        INSERT INTO tmp_chain
        SELECT
            0,
            'start',
            w.id,
            w.address,
            w.label,
            f.id,
            f.address,
            f.label,
            w.funding_amount / 1e9,
            t.signature,
            COALESCE(CAST(t.tx_state AS UNSIGNED), 0),
            FROM_UNIXTIME(w.first_seen_block_time)
        FROM tx_address w
        LEFT JOIN tx_address f ON w.funded_by_address_id = f.id
        LEFT JOIN tx t ON w.funding_tx_id = t.id
        WHERE w.id = v_start_id;

        WHILE v_found > 0 AND v_depth < p_max_depth DO
            SET v_depth = v_depth + 1;
            SET v_found = 0;

            IF p_direction IN ('up', 'both') THEN
                TRUNCATE TABLE tmp_frontier;
                INSERT INTO tmp_frontier
                SELECT wallet_id, funder_id FROM tmp_chain WHERE depth = v_depth - 1;

                INSERT IGNORE INTO tmp_chain
                SELECT
                    v_depth,
                    'up',
                    w.id,
                    w.address,
                    w.label,
                    f.id,
                    f.address,
                    f.label,
                    w.funding_amount / 1e9,
                    t.signature,
                    COALESCE(CAST(t.tx_state AS UNSIGNED), 0),
                    FROM_UNIXTIME(w.first_seen_block_time)
                FROM tx_address w
                LEFT JOIN tx_address f ON w.funded_by_address_id = f.id
                LEFT JOIN tx t ON w.funding_tx_id = t.id
                INNER JOIN tmp_frontier tf ON w.id = tf.funder_id
                WHERE (p_min_tx_state = 0 OR COALESCE(CAST(t.tx_state AS UNSIGNED), 0) >= p_min_tx_state);

                SET v_found = v_found + ROW_COUNT();
            END IF;

            IF p_direction IN ('down', 'both') THEN
                TRUNCATE TABLE tmp_frontier;
                INSERT INTO tmp_frontier
                SELECT wallet_id, funder_id FROM tmp_chain WHERE depth = v_depth - 1;

                INSERT IGNORE INTO tmp_chain
                SELECT
                    v_depth,
                    'down',
                    w.id,
                    w.address,
                    w.label,
                    f.id,
                    f.address,
                    f.label,
                    w.funding_amount / 1e9,
                    t.signature,
                    COALESCE(CAST(t.tx_state AS UNSIGNED), 0),
                    FROM_UNIXTIME(w.first_seen_block_time)
                FROM tx_address w
                LEFT JOIN tx_address f ON w.funded_by_address_id = f.id
                LEFT JOIN tx t ON w.funding_tx_id = t.id
                INNER JOIN tmp_frontier tf ON w.funded_by_address_id = tf.wallet_id
                WHERE (p_min_tx_state = 0 OR COALESCE(CAST(t.tx_state AS UNSIGNED), 0) >= p_min_tx_state);

                SET v_found = v_found + ROW_COUNT();
            END IF;
        END WHILE;

        SELECT
            depth,
            direction,
            wallet_address,
            wallet_label,
            funder_address,
            funder_label,
            funding_sol,
            funding_tx_signature,
            tx_state,
            first_seen_utc
        FROM tmp_chain
        ORDER BY depth, direction, wallet_address;

        DROP TEMPORARY TABLE IF EXISTS tmp_chain;
        DROP TEMPORARY TABLE IF EXISTS tmp_frontier;
    END IF;
END;;

DELIMITER ;

-- --------------------------------------------------------------------------
-- fn_get_guide_by_wallet: type_state param → tx_state filter
-- --------------------------------------------------------------------------

DELIMITER ;;

DROP FUNCTION IF EXISTS `fn_get_guide_by_wallet`;;

CREATE DEFINER=`root`@`%` FUNCTION `fn_get_guide_by_wallet`(p_wallet_address VARCHAR(44), p_min_tx_state BIGINT UNSIGNED) RETURNS json
    READS SQL DATA
    DETERMINISTIC
BEGIN
    RETURN (
        SELECT JSON_ARRAYAGG(
            JSON_OBJECT(
                'id', g.id,
                'tx_id', g.tx_id,
                'block_time', g.block_time,
                'from_address_id', g.from_address_id,
                'to_address_id', g.to_address_id,
                'from_token_account_id', g.from_token_account_id,
                'to_token_account_id', g.to_token_account_id,
                'token_id', g.token_id,
                'amount', g.amount,
                'decimals', g.decimals,
                'edge_type_id', g.edge_type_id,
                'source_id', g.source_id,
                'source_row_id', g.source_row_id,
                'ins_index', g.ins_index
            )
        )
        FROM tx_guide g
        JOIN tx t ON t.id = g.tx_id
        JOIN tx_address fa ON fa.id = g.from_address_id
        JOIN tx_address ta ON ta.id = g.to_address_id
        WHERE (fa.address = p_wallet_address OR ta.address = p_wallet_address)
          AND (p_min_tx_state = 0 OR CAST(t.tx_state AS UNSIGNED) >= p_min_tx_state)
          AND (p_min_tx_state = 0 OR CAST(t.tx_state AS UNSIGNED) & p_min_tx_state != 0)
    );
END;;

DELIMITER ;

-- --------------------------------------------------------------------------
-- fn_get_guide_by_token: type_state param → tx_state filter
-- --------------------------------------------------------------------------

DELIMITER ;;

DROP FUNCTION IF EXISTS `fn_get_guide_by_token`;;

CREATE DEFINER=`root`@`%` FUNCTION `fn_get_guide_by_token`(p_mint_address VARCHAR(44), p_min_tx_state BIGINT UNSIGNED) RETURNS json
    READS SQL DATA
    DETERMINISTIC
BEGIN
    RETURN (
        SELECT JSON_ARRAYAGG(
            JSON_OBJECT(
                'id', g.id,
                'tx_id', g.tx_id,
                'block_time', g.block_time,
                'from_address_id', g.from_address_id,
                'to_address_id', g.to_address_id,
                'from_token_account_id', g.from_token_account_id,
                'to_token_account_id', g.to_token_account_id,
                'token_id', g.token_id,
                'amount', g.amount,
                'decimals', g.decimals,
                'edge_type_id', g.edge_type_id,
                'source_id', g.source_id,
                'source_row_id', g.source_row_id,
                'ins_index', g.ins_index
            )
        )
        FROM tx_guide g
        JOIN tx t ON t.id = g.tx_id
        JOIN tx_token tk ON tk.id = g.token_id
        JOIN tx_address mint ON mint.id = tk.mint_address_id
        WHERE mint.address = p_mint_address
          AND (p_min_tx_state = 0 OR CAST(t.tx_state AS UNSIGNED) >= p_min_tx_state)
          AND (p_min_tx_state = 0 OR CAST(t.tx_state AS UNSIGNED) & p_min_tx_state != 0)
    );
END;;

DELIMITER ;

-- --------------------------------------------------------------------------
-- Step 3b: Recreate views that reference type_state → tx_state
-- --------------------------------------------------------------------------

DROP VIEW IF EXISTS `vw_tx_funding_chain`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `vw_tx_funding_chain` AS
SELECT
    `w`.`id` AS `wallet_id`,
    `w`.`address` AS `wallet_address`,
    `w`.`label` AS `wallet_label`,
    `f1`.`id` AS `funder_1_id`,
    `f1`.`address` AS `funder_1_address`,
    `f1`.`label` AS `funder_1_label`,
    `f2`.`id` AS `funder_2_id`,
    `f2`.`address` AS `funder_2_address`,
    `f2`.`label` AS `funder_2_label`,
    (`w`.`funding_amount` / 1e9) AS `funding_sol`,
    `t1`.`signature` AS `funding_tx_signature`,
    CAST(`t1`.`tx_state` AS UNSIGNED) AS `tx_state`,
    FROM_UNIXTIME(`w`.`first_seen_block_time`) AS `first_seen_utc`
FROM `tx_address` `w`
LEFT JOIN `tx_address` `f1` ON `w`.`funded_by_address_id` = `f1`.`id`
LEFT JOIN `tx_address` `f2` ON `f1`.`funded_by_address_id` = `f2`.`id`
LEFT JOIN `tx` `t1` ON `w`.`funding_tx_id` = `t1`.`id`
WHERE `w`.`address_type` IN ('wallet','unknown')
  AND `w`.`funded_by_address_id` IS NOT NULL;

-- --------------------------------------------------------------------------

DROP VIEW IF EXISTS `vw_tx_funding_tree`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `vw_tx_funding_tree` AS
SELECT
    `w`.`id` AS `wallet_id`,
    `w`.`address` AS `wallet_address`,
    `w`.`address_type` AS `wallet_type`,
    `w`.`label` AS `wallet_label`,
    `f`.`id` AS `funder_id`,
    `f`.`address` AS `funder_address`,
    `f`.`address_type` AS `funder_type`,
    `f`.`label` AS `funder_label`,
    (`w`.`funding_amount` / 1e9) AS `funding_sol`,
    `w`.`funding_tx_id` AS `funding_tx_id`,
    FROM_UNIXTIME(`w`.`first_seen_block_time`) AS `first_seen_utc`,
    `t`.`signature` AS `funding_tx_signature`,
    CAST(`t`.`tx_state` AS UNSIGNED) AS `tx_state`
FROM `tx_address` `w`
LEFT JOIN `tx_address` `f` ON `w`.`funded_by_address_id` = `f`.`id`
LEFT JOIN `tx` `t` ON `w`.`funding_tx_id` = `t`.`id`
WHERE `w`.`address_type` IN ('wallet','unknown');

-- --------------------------------------------------------------------------

DROP VIEW IF EXISTS `vw_tx_high_freq_pairs`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `vw_tx_high_freq_pairs` AS
SELECT
    LEAST(`a1`.`address`, `a2`.`address`) AS `wallet_1`,
    GREATEST(`a1`.`address`, `a2`.`address`) AS `wallet_2`,
    `tk`.`token_symbol` AS `token_symbol`,
    COUNT(*) AS `transfer_count`,
    SUM(CASE WHEN `g`.`from_address_id` = `a1`.`id` THEN 1 ELSE 0 END) AS `wallet1_to_wallet2`,
    SUM(CASE WHEN `g`.`from_address_id` = `a2`.`id` THEN 1 ELSE 0 END) AS `wallet2_to_wallet1`,
    (SUM(`g`.`amount`) / POW(10, MAX(`g`.`decimals`))) AS `total_volume`,
    MIN(`g`.`block_time`) AS `first_transfer`,
    MAX(`g`.`block_time`) AS `last_transfer`,
    ((MAX(`g`.`block_time`) - MIN(`g`.`block_time`)) / 3600) AS `hours_span`,
    BIT_OR(CAST(`t`.`tx_state` AS UNSIGNED)) AS `tx_state`
FROM `tx_guide` `g`
JOIN `tx_address` `a1` ON `g`.`from_address_id` = `a1`.`id`
JOIN `tx_address` `a2` ON `g`.`to_address_id` = `a2`.`id`
JOIN `tx` `t` ON `g`.`tx_id` = `t`.`id`
LEFT JOIN `tx_token` `tk` ON `g`.`token_id` = `tk`.`id`
WHERE `g`.`edge_type_id` IN (
    SELECT `id` FROM `tx_guide_type` WHERE `type_code` IN ('spl_transfer','sol_transfer')
)
AND `a1`.`id` <> `a2`.`id`
GROUP BY LEAST(`a1`.`address`, `a2`.`address`), GREATEST(`a1`.`address`, `a2`.`address`), `tk`.`token_symbol`
HAVING COUNT(*) >= 5;

-- --------------------------------------------------------------------------
-- Step 3c: Drop the dead columns
-- --------------------------------------------------------------------------

-- is_guide_loaded: generated stored column, no queries reference it
ALTER TABLE tx DROP COLUMN is_guide_loaded;

-- type_state: never written by any SP/worker, always 0
ALTER TABLE tx DROP COLUMN type_state;

-- --------------------------------------------------------------------------
-- Step 3d: Add functional index for guide_loader batch selection
-- --------------------------------------------------------------------------
-- tx_state is VARCHAR(16) used with bitwise AND — without this index,
-- WHERE tx_state & 32 = 0 ORDER BY id forces a full table scan.
-- Functional index pre-computes the bitmask so MySQL can do a ref lookup.
ALTER TABLE tx ADD INDEX idx_tx_guide_pending ((tx_state & 32), id);

-- ============================================================================
-- SUMMARY
-- ============================================================================
-- Tier 1: Dropped 1 redundant index (idx_tx_blocktime)
-- Tier 2: Dropped 6 FK constraints + 6 indexes (agg_*/signer)
-- Tier 3: Updated 1 SP, 2 functions, 3 views; dropped 2 columns;
--         added functional index for guide_loader batch selection
--
-- tx table after migration:
--   Indexes:  PRIMARY, uk_signature, idx_block_time, idx_tx_state,
--             idx_tx_request_log_id, idx_tx_guide_pending (functional)
--   Removed:  7 indexes, 6 FK constraints, 2 columns
-- ============================================================================
